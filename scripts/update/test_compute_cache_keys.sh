#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

write_keys() {
  local source_hash="$1"
  local output="$2"
  GITHUB_OUTPUT="$output" \
    RUNNER_OS_NAME="Linux" \
    RUNNER_ARCH_NAME="X64" \
    LOCK_HASH="lock-hash" \
    SOURCE_HASH="$source_hash" \
    RUST_VERSION="1.99.0" \
    XRAY_VERSION="1.2.3" \
    XRAY_SHA256="xray-hash" \
    SINGBOX_VERSION="4.5.6" \
    SINGBOX_SHA256="singbox-hash" \
    MIHOMO_VERSION="1.19.32" \
    MIHOMO_SHA256="mihomo-hash" \
    bash "$repo_root/scripts/update/compute_cache_keys.sh"
}

write_keys "source-a" "$tmp/first"
write_keys "source-b" "$tmp/second"

first_build="$(sed -n 's/^build=//p' "$tmp/first")"
second_build="$(sed -n 's/^build=//p' "$tmp/second")"
first_registry="$(sed -n 's/^registry=//p' "$tmp/first")"
second_registry="$(sed -n 's/^registry=//p' "$tmp/second")"

[[ -n "$first_build" && -n "$second_build" ]]
[[ "$first_build" != "$second_build" ]]
[[ "$first_registry" == "$second_registry" ]]
mihomo_key="$(sed -n 's/^mihomo=//p' "$tmp/first")"
[[ "$mihomo_key" == "Linux-mihomo-1.19.32-mihomo-hash-verified" ]]

printf '[OK] Rust and validation-core cache keys are computed correctly.\n'
