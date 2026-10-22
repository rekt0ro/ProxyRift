#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
script="$repo_root/scripts/update/build_light_subscription.sh"

for setting in "--precheck-timeout 3 \\" "--timeout 3 \\"; do
  if ! grep -Fq -- "$setting" "$script"; then
    echo "[test] Expected Light validation setting not found: $setting" >&2
    exit 1
  fi
done

if grep -Fq -- "--precheck-timeout 5 \\" "$script"; then
  echo "[test] Light HTTP precheck must not retain a 5-second timeout." >&2
  exit 1
fi

echo "[test] Light precheck and validation timeouts are both 3 seconds."
