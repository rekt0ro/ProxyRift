#!/usr/bin/env bash
set -euo pipefail

stable_channel="$(curl -fsSL \
  --retry 5 \
  --retry-delay 2 \
  --retry-max-time 45 \
  --retry-connrefused \
  https://static.rust-lang.org/dist/channel-rust-stable.toml)"

rust_version="$(awk -F'"' '/^\[pkg\.rustc\]$/ { in_rustc=1; next } /^\[/ { in_rustc=0 } in_rustc && /^[[:space:]]*version[[:space:]]*=/ { split($2, parts, " "); print parts[1]; exit }' <<< "$stable_channel")"
test -n "$rust_version"
[[ "$rust_version" =~ ^1\.[0-9]+\.[0-9]+$ ]]
echo "version=$rust_version" >> "$GITHUB_OUTPUT"
