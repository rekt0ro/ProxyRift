#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
pin="$tmp/.ruby-version"
printf '4.0.7\n' > "$pin"

RUBY_VERSION_PIN="4.0.8" python3 "$repo_root/scripts/update/pin_ruby_version.py" "$pin"
grep -Fxq "4.0.8" "$pin"

if RUBY_VERSION_PIN="4.1.0-preview1" python3 "$repo_root/scripts/update/pin_ruby_version.py" "$pin" >/dev/null 2>&1; then
  echo "[ERROR] Ruby pin updater accepted a prerelease version." >&2
  exit 1
fi
grep -Fxq "4.0.8" "$pin"

printf 'invalid\n' > "$pin"
if RUBY_VERSION_PIN="4.0.9" python3 "$repo_root/scripts/update/pin_ruby_version.py" "$pin" >/dev/null 2>&1; then
  echo "[ERROR] Ruby pin updater accepted a malformed existing pin." >&2
  exit 1
fi
grep -Fxq "invalid" "$pin"

echo "[OK] Ruby version pin updates only to a stable X.Y.Z release."
