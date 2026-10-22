#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
work="$tmp/work"
bin="$tmp/bin"
mkdir -p "$work/subscriptions" "$bin"

cat > "$bin/sing-box" <<'SINGBOX'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != "check" ]]; then
  echo "[mock] expected the check command" >&2
  exit 2
fi
config=""
while (($#)); do
  if [[ "$1" == "-c" ]]; then
    config="$2"
    shift 2
  else
    shift
  fi
done
if [[ -z "$config" ]]; then
  echo "[mock] missing config path" >&2
  exit 2
fi
if [[ "${MOCK_SINGBOX_FAIL:-false}" == "true" ]]; then
  echo "[mock] schema rejection" >&2
  exit 1
fi
jq --exit-status '
  type == "object"
  and (.outbounds | type == "array" and length > 0)
  and all(.outbounds[]; type == "object"
    and (.type | type == "string" and length > 0)
    and (.tag | type == "string" and length > 0))
' "$config" >/dev/null
SINGBOX
chmod +x "$bin/sing-box"

cat > "$bin/mihomo" <<'MIHOMO'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != "-t" || "${2:-}" != "-f" || -z "${3:-}" || ! -s "${3:-}" ]]; then
  echo "[mock] expected -t -f <config>" >&2
  exit 2
fi
if [[ "${MOCK_MIHOMO_FAIL:-false}" == "true" ]]; then
  echo "[mock] invalid Clash configuration" >&2
  exit 1
fi
MIHOMO
chmod +x "$bin/mihomo"

make_outputs() {
  local light_count="$1"
  local all_count="${2:-1}"
  local dir="$work/subscriptions"
  : > "$dir/light.txt"
  : > "$dir/all.txt"

  for ((i = 1; i <= light_count; i++)); do
    printf 'vless://test-%s@example.com:443\n' "$i" >> "$dir/light.txt"
  done
  for ((i = 1; i <= all_count; i++)); do
    printf 'socks5://test-%s@example.com:1080\n' "$i" >> "$dir/all.txt"
  done

  local light_entries=0 all_entries=0 name source_content entry_count index
  for name in light all; do
    source_content="$(cat "$dir/$name.txt")"
    printf '%s' "$source_content" | base64 --wrap=0 > "$dir/$name-base64.txt"
    printf '\n' >> "$dir/$name-base64.txt"

    entry_count="$(awk 'NF && $0 !~ /^#/ && index($0, "://") { n++ } END { print n + 0 }' "$dir/$name.txt")"
    jq -n --argjson count "$entry_count" \
      '{outbounds: [range(1; $count + 1) | {type: "direct", tag: ("test-" + tostring)}]}' \
      > "$dir/$name-singbox.json"

    {
      printf 'proxies:\n'
      for ((index = 1; index <= entry_count; index++)); do
        printf "  - name: 'test proxy %s'\\n    type: vless\\n    server: example.com\\n    port: 443\\n" "$index"
      done
    } > "$dir/$name-clash.yaml"

    if [[ "$name" == "light" ]]; then
      light_entries="$entry_count"
    else
      all_entries="$entry_count"
    fi
  done

  jq -n --argjson light "$light_entries" --argjson all "$all_entries" \
    '{light: {source: $light, clash: $light, singbox: $light}, all: {source: $all, clash: $all, singbox: $all}}' \
    > "$dir/.output-counts.json"

}

run_gate() {
  (cd "$work" && SINGBOX_BINARY="$bin/sing-box" MIHOMO_BINARY="$bin/mihomo" LIGHT_MIN_PUBLISH=50 ALL_MIN_PUBLISH=1 bash "$repo_root/scripts/update/prepare_published_outputs.sh")
}

run_gate_production_all_min() {
  (cd "$work" && SINGBOX_BINARY="$bin/sing-box" MIHOMO_BINARY="$bin/mihomo" LIGHT_MIN_PUBLISH=50 bash "$repo_root/scripts/update/prepare_published_outputs.sh")
}

make_outputs 50
run_gate >/dev/null
echo "[OK] Valid generated outputs pass the publication gate."

make_outputs 50
cat > "$work/subscriptions/light-clash.yaml" <<'YAML'
proxies: [{name: "inline proxy", type: vless, server: example.com, port: 443}]
YAML
jq '.light.clash = 1' "$work/subscriptions/.output-counts.json" > "$tmp/output-counts.json"
mv "$tmp/output-counts.json" "$work/subscriptions/.output-counts.json"
if ! run_gate >"$tmp/clash-inline.log" 2>&1; then
  echo "[ERROR] Publication gate rejected valid YAML because its formatting changed." >&2
  cat "$tmp/clash-inline.log" >&2
  exit 1
fi
grep -Fq "light source: 50 | Clash: 1 (omitted: 49)" "$tmp/clash-inline.log"
echo "[OK] Clash accounting uses parsed YAML entries, not text layout."

make_outputs 50
sed -i '/port: 443/d' "$work/subscriptions/light-clash.yaml"
if run_gate >"$tmp/clash-structure.log" 2>&1; then
  echo "[ERROR] Publication gate accepted a Clash proxy with a missing port." >&2
  exit 1
fi
grep --fixed-strings --quiet "proxies[0].port must be an integer from 1 to 65535" "$tmp/clash-structure.log"
echo "[OK] Clash proxies missing required fields are rejected."

make_outputs 50
if MOCK_SINGBOX_FAIL=true run_gate >"$tmp/singbox-check.log" 2>&1; then
  echo "[ERROR] Publication gate accepted a sing-box config rejected by the core validator." >&2
  exit 1
fi
grep --fixed-strings --quiet "sing-box rejected configuration" "$tmp/singbox-check.log"
echo "[OK] sing-box core validation failures block publication."

make_outputs 50
if MOCK_MIHOMO_FAIL=true run_gate >"$tmp/mihomo-check.log" 2>&1; then
  echo "[ERROR] Publication gate accepted a Clash config rejected by Mihomo." >&2
  exit 1
fi
grep --fixed-strings --quiet "Mihomo rejected Clash configuration" "$tmp/mihomo-check.log"
echo "[OK] Mihomo validation failures block publication."

make_outputs 201
if run_gate >"$tmp/light-max.log" 2>&1; then
  echo "[ERROR] Publication gate accepted Light above its hard maximum." >&2
  exit 1
fi
grep --quiet "maximum is 200" "$tmp/light-max.log"
echo "[OK] Light above the hard maximum is rejected."

make_outputs 50 2001
if run_gate >"$tmp/all-max.log" 2>&1; then
  echo "[ERROR] Publication gate accepted All above its hard maximum." >&2
  exit 1
fi
grep --quiet "maximum is 2000" "$tmp/all-max.log"
echo "[OK] All above the hard maximum is rejected."

make_outputs 50
printf 'wireguard://test@example.com:51820\n' >> "$work/subscriptions/all.txt"
source_content="$(cat "$work/subscriptions/all.txt")"
printf '%s' "$source_content" | base64 --wrap=0 > "$work/subscriptions/all-base64.txt"
printf '\n' >> "$work/subscriptions/all-base64.txt"
jq '.all.source += 1' "$work/subscriptions/.output-counts.json" > "$tmp/output-counts.json"
mv "$tmp/output-counts.json" "$work/subscriptions/.output-counts.json"
if ! run_gate >"$tmp/omissions.log" 2>&1; then
  echo "[ERROR] Publication gate rejected explicitly accounted unsupported-format omissions." >&2
  cat "$tmp/omissions.log" >&2
  exit 1
fi
grep --quiet "all source: 2 | Clash: 1 (omitted: 1) | sing-box: 1 (omitted: 1)" "$tmp/omissions.log"
echo "[OK] Accounted unsupported-format omissions are allowed and reported."

make_outputs 50
jq -n '{outbounds: [{type: "direct", tag: "unexpected-1"}, {type: "direct", tag: "unexpected-2"}]}' \
  > "$work/subscriptions/all-singbox.json"
if run_gate >"$tmp/count-parity.log" 2>&1; then
  echo "[ERROR] Publication gate accepted sing-box output with an unaccounted entry count." >&2
  exit 1
fi
grep --quiet "sing-box entry count does not match format-count manifest" "$tmp/count-parity.log"
echo "[OK] Unaccounted format count is rejected."

make_outputs 50
printf 'not-base64!\n' > "$work/subscriptions/light-base64.txt"
if run_gate >"$tmp/base64.log" 2>&1; then
  echo "[ERROR] Publication gate accepted a broken Base64 output." >&2
  exit 1
fi
grep --quiet "Base64 output cannot be decoded" "$tmp/base64.log"
echo "[OK] Broken Base64 output is rejected."

make_outputs 50
printf '{broken json\n' > "$work/subscriptions/light-singbox.json"
if run_gate >"$tmp/json.log" 2>&1; then
  echo "[ERROR] Publication gate accepted invalid sing-box JSON." >&2
  exit 1
fi
grep --quiet "Invalid sing-box JSON" "$tmp/json.log"
echo "[OK] Invalid sing-box JSON is rejected."

make_outputs 49
if run_gate >"$tmp/count.log" 2>&1; then
  echo "[ERROR] Publication gate accepted Light below its minimum." >&2
  exit 1
fi
grep --quiet "minimum is 50" "$tmp/count.log"
echo "[OK] Light below the minimum is rejected."

make_outputs 199
if (cd "$work" && SINGBOX_BINARY="$bin/sing-box" MIHOMO_BINARY="$bin/mihomo" LIGHT_MIN_PUBLISH=200 ALL_MIN_PUBLISH=1 bash "$repo_root/scripts/update/prepare_published_outputs.sh") >"$tmp/light-target.log" 2>&1; then
  echo "[ERROR] Publication gate accepted Light below the 200-config production target." >&2
  exit 1
fi
grep --quiet "minimum is 200" "$tmp/light-target.log"
echo "[OK] Light below the 200-config target is rejected."

make_outputs 200
(cd "$work" && SINGBOX_BINARY="$bin/sing-box" MIHOMO_BINARY="$bin/mihomo" LIGHT_MIN_PUBLISH=200 ALL_MIN_PUBLISH=1 bash "$repo_root/scripts/update/prepare_published_outputs.sh") >/dev/null
echo "[OK] Exactly 200 Light entries pass the production publication gate."

make_outputs 50 999
if run_gate_production_all_min >"$tmp/all-min.log" 2>&1; then
  echo "[ERROR] Publication gate accepted All below the 1000-config minimum." >&2
  exit 1
fi
grep --quiet "minimum is 1000" "$tmp/all-min.log"
echo "[OK] All below the 1000-config minimum is rejected."

make_outputs 50 1000
run_gate_production_all_min >/dev/null
echo "[OK] Exactly 1000 All entries pass the production publication gate."

