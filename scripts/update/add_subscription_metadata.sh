#!/usr/bin/env bash
set -euo pipefail

for f in subscriptions/all.txt subscriptions/light.txt; do
  test -s "$f"
  tmp="${f}.tmp"
  {
    printf '%s\n' '#profile-update-interval: 1'
    awk '!/^#profile-update-interval:/' "$f"
  } > "$tmp"
  mv -f "$tmp" "$f"
  test "$(grep -Ec '^#profile-update-interval:' "$f")" -eq 1
done
