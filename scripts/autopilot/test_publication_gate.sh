#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
work="$tmp/work"
mkdir -p "$work/subscriptions"

make_outputs() {
  local light_count="$1"
  local dir="$work/subscriptions"
  : > "$dir/light.txt"
  : > "$dir/all.txt"

  for ((i = 1; i <= light_count; i++)); do
    printf 'vless://test-%s@example.com:443\n' "$i" >> "$dir/light.txt"
  done
  printf 'socks5://test@example.com:1080\n' >> "$dir/all.txt"

  for name in light all; do
    python3 - "$dir/$name.txt" "$dir/$name-base64.txt" <<'PY'
import base64
import pathlib
import sys

source = pathlib.Path(sys.argv[1]).read_bytes().rstrip(b"\r\n")
encoded = base64.b64encode(source).decode("ascii")
pathlib.Path(sys.argv[2]).write_text(encoded + "\n", encoding="ascii")
PY
    printf '{"outbounds":[{"type":"direct","tag":"direct"}]}\n' > "$dir/$name-singbox.json"
    cat > "$dir/$name-clash.yaml" <<'YAML'
proxies:
  - name: "test proxy"
    type: socks5
    server: example.com
    port: 1080
YAML
  done
}

run_gate() {
  (cd "$work" && LIGHT_MIN_PUBLISH=50 bash "$repo_root/scripts/update/prepare_published_outputs.sh")
}

make_outputs 50
run_gate >/dev/null
echo "[OK] Valid generated outputs pass the publication gate."

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
