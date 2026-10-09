#!/usr/bin/env bash
set -euo pipefail

minimum_light="\${LIGHT_MIN_PUBLISH:-50}"
minimum_all="\${ALL_MIN_PUBLISH:-1}"

for f in subscriptions/all.txt subscriptions/light.txt subscriptions/all-base64.txt subscriptions/light-base64.txt subscriptions/all-clash.yaml subscriptions/light-clash.yaml subscriptions/all-singbox.json subscriptions/light-singbox.json; do
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
if (( all_count < minimum_all )); then
  echo "[ERROR] Refusing to publish All with $all_count entries; minimum is $minimum_all." >&2
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

  if ! jq --exit-status \
    'type == "object" and (.outbounds | type == "array") and (.outbounds | length > 0)' \
    "subscriptions/$name-singbox.json" >/dev/null; then
    echo "[ERROR] Invalid sing-box JSON or empty outbounds: subscriptions/$name-singbox.json" >&2
    exit 1
  fi

  if ! grep --quiet --extended-regexp '^proxies:[[:space:]]*$' "subscriptions/$name-clash.yaml" \
      || ! grep --quiet --extended-regexp '^[[:space:]]+- name:' "subscriptions/$name-clash.yaml"; then
    echo "[ERROR] Clash output has no proxy entries: subscriptions/$name-clash.yaml" >&2
    exit 1
  fi
done

echo "[OK] Publication gate passed | Light: $light_count (minimum $minimum_light) | All: $all_count"
