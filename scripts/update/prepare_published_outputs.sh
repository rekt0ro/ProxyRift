#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

minimum_light="${LIGHT_MIN_PUBLISH:-50}"
minimum_all="${ALL_MIN_PUBLISH:-1000}"
maximum_light="${LIGHT_SELECTION_LIMIT:-200}"
maximum_all="${ALL_MAX_PUBLISH:-2000}"
counts_file="subscriptions/.output-counts.json"

for f in subscriptions/all.txt subscriptions/light.txt subscriptions/all-base64.txt subscriptions/light-base64.txt subscriptions/all-clash.yaml subscriptions/light-clash.yaml subscriptions/all-singbox.json subscriptions/light-singbox.json "$counts_file"; do
  if [ ! -s "$f" ]; then
    echo "[ERROR] Required output is missing or empty: $f" >&2
    exit 1
  fi
done

count_entries() {
  awk 'NF && $0 !~ /^#/ { n++ } END { print n + 0 }' "$1"
}

light_count="$(count_entries subscriptions/light.txt)"
all_count="$(count_entries subscriptions/all.txt)"

if (( light_count < minimum_light )); then
  echo "[ERROR] Refusing to publish Light with $light_count entries; minimum is $minimum_light." >&2
  exit 1
fi
if (( light_count > maximum_light )); then
  echo "[ERROR] Refusing to publish Light with $light_count entries; maximum is $maximum_light." >&2
  exit 1
fi
if (( all_count < minimum_all )); then
  echo "[ERROR] Refusing to publish All with $all_count entries; minimum is $minimum_all." >&2
  exit 1
fi
if (( all_count > maximum_all )); then
  echo "[ERROR] Refusing to publish All with $all_count entries; maximum is $maximum_all." >&2
  exit 1
fi

singbox_binary="${SINGBOX_BINARY:-$HOME/.local/bin/sing-box}"
if [[ ! -x "$singbox_binary" ]]; then
  echo "[ERROR] sing-box validator is missing or not executable: $singbox_binary" >&2
  exit 1
fi
mihomo_binary="${MIHOMO_BINARY:-$HOME/.local/bin/mihomo}"
if [[ ! -x "$mihomo_binary" ]]; then
  echo "[ERROR] Mihomo validator is missing or not executable: $mihomo_binary" >&2
  exit 1
fi

for name in light all; do
  decoded="$(mktemp)"
  if ! base64 --decode "subscriptions/$name-base64.txt" > "$decoded"; then
    rm -f "$decoded"
    echo "[ERROR] Base64 output cannot be decoded: subscriptions/$name-base64.txt" >&2
    exit 1
  fi
  if ! python3 - "subscriptions/$name.txt" "$decoded" <<'PY'
import pathlib
import sys

source = pathlib.Path(sys.argv[1]).read_bytes().rstrip(b"\r\n")
decoded = pathlib.Path(sys.argv[2]).read_bytes()
if source != decoded:
    raise SystemExit(1)
PY
  then
    rm -f "$decoded"
    echo "[ERROR] Base64 output does not mirror subscriptions/$name.txt." >&2
    exit 1
  fi
  rm -f "$decoded"

  if ! jq --exit-status '
    type == "object"
    and (.outbounds | type == "array" and length > 0)
    and all(.outbounds[]; type == "object"
      and (.type | type == "string" and length > 0)
      and (.tag | type == "string" and length > 0))
  ' "subscriptions/$name-singbox.json" >/dev/null; then
    echo "[ERROR] Invalid sing-box JSON or outbound missing a non-empty type/tag: subscriptions/$name-singbox.json" >&2
    exit 1
  fi

  if ! "$singbox_binary" check -c "subscriptions/$name-singbox.json"; then
    echo "[ERROR] sing-box rejected configuration: subscriptions/$name-singbox.json" >&2
    exit 1
  fi

  if ! clash_count="$(ruby "$repo_root/scripts/update/validate_clash_output.rb" --count "subscriptions/$name-clash.yaml")"; then
    echo "[ERROR] Clash/Mihomo output failed structural validation: subscriptions/$name-clash.yaml" >&2
    exit 1
  fi
  printf '[OK] [Clash/Mihomo] Structurally valid | subscriptions/%s-clash.yaml | Proxies: %s\n' "$name" "$clash_count"
  if ! "$mihomo_binary" -t -f "subscriptions/$name-clash.yaml"; then
    echo "[ERROR] Mihomo rejected Clash configuration: subscriptions/$name-clash.yaml" >&2
    exit 1
  fi

  source_count="$all_count"
  if [[ "$name" == "light" ]]; then
    source_count="$light_count"
  fi

  if ! jq --exit-status --arg name "$name" '
    def nonnegative_integer: if type == "number" then . >= 0 and floor == . else false end;
    .[$name] as $counts
    | ($counts | type == "object")
      and ($counts.source | nonnegative_integer)
      and ($counts.clash | nonnegative_integer)
      and ($counts.singbox | nonnegative_integer)
      and ($counts.clash <= $counts.source)
      and ($counts.singbox <= $counts.source)
  ' "$counts_file" >/dev/null; then
    echo "[ERROR] Invalid or inconsistent format-count manifest for $name: $counts_file" >&2
    exit 1
  fi

  manifest_source="$(jq --exit-status --arg name "$name" '.[$name].source' "$counts_file")"
  manifest_clash="$(jq --exit-status --arg name "$name" '.[$name].clash' "$counts_file")"
  manifest_singbox="$(jq --exit-status --arg name "$name" '.[$name].singbox' "$counts_file")"
  singbox_count="$(jq --exit-status '.outbounds | length' "subscriptions/$name-singbox.json")"

  if (( manifest_source != source_count )); then
    echo "[ERROR] Source count does not match format-count manifest for $name: manifest=$manifest_source, file=$source_count." >&2
    exit 1
  fi
  if (( manifest_clash != clash_count )); then
    echo "[ERROR] Clash entry count does not match format-count manifest for $name: manifest=$manifest_clash, file=$clash_count." >&2
    exit 1
  fi
  if (( manifest_singbox != singbox_count )); then
    echo "[ERROR] sing-box entry count does not match format-count manifest for $name: manifest=$manifest_singbox, file=$singbox_count." >&2
    exit 1
  fi

  printf '[INFO] Format accounting | %s source: %s | Clash: %s (omitted: %s) | sing-box: %s (omitted: %s)\n' \
    "$name" "$source_count" "$clash_count" "$((source_count - clash_count))" \
    "$singbox_count" "$((source_count - singbox_count))"
done

echo "[OK] Publication gate passed | Light: $light_count (minimum $minimum_light) | All: $all_count"
