#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

valid_xray_sha="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
valid_singbox_sha="abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
valid_mihomo_sha="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

GITHUB_OUTPUT="$tmp_dir/valid-output" \
XRAY_VERSION_PIN="26.3.27" \
XRAY_SHA256_PIN="$valid_xray_sha" \
XRAY_ASSET_PIN="Xray-linux-64.zip" \
SINGBOX_VERSION_PIN="1.14.3" \
SINGBOX_SHA256_PIN="$valid_singbox_sha" \
SINGBOX_ASSET_PIN="sing-box-1.14.3-linux-amd64.tar.gz" \
MIHOMO_VERSION_PIN="1.19.32" \
MIHOMO_SHA256_PIN="$valid_mihomo_sha" \
MIHOMO_ASSET_PIN="mihomo-linux-amd64-v1.19.32.gz" \
  bash "$repo_root/scripts/update/resolve_validation_cores.sh"

grep -Fxq "xray_version=26.3.27" "$tmp_dir/valid-output"
grep -Fxq "singbox_version=1.14.3" "$tmp_dir/valid-output"
grep -Fxq "singbox_asset=sing-box-1.14.3-linux-amd64.tar.gz" "$tmp_dir/valid-output"
grep -Fxq "mihomo_version=1.19.32" "$tmp_dir/valid-output"
grep -Fxq "mihomo_sha256=$valid_mihomo_sha" "$tmp_dir/valid-output"
grep -Fxq "mihomo_asset=mihomo-linux-amd64-v1.19.32.gz" "$tmp_dir/valid-output"

if GITHUB_OUTPUT="$tmp_dir/invalid-output" \
  XRAY_VERSION_PIN="26.3.27" \
  XRAY_SHA256_PIN="$valid_xray_sha" \
  XRAY_ASSET_PIN="Xray-linux-64.zip" \
  SINGBOX_VERSION_PIN="1.14.3" \
  SINGBOX_SHA256_PIN="$valid_singbox_sha" \
  SINGBOX_ASSET_PIN="sing-box-1.14.2-linux-amd64.tar.gz" \
    MIHOMO_VERSION_PIN="1.19.32" \
    MIHOMO_SHA256_PIN="$valid_mihomo_sha" \
    MIHOMO_ASSET_PIN="mihomo-linux-amd64-v1.19.32.gz" \
    bash "$repo_root/scripts/update/resolve_validation_cores.sh" >/dev/null 2>&1; then
  echo "Expected mismatched sing-box asset to be rejected." >&2
  exit 1
fi

if GITHUB_OUTPUT="$tmp_dir/invalid-mihomo-output" \
  XRAY_VERSION_PIN="26.3.27" \
  XRAY_SHA256_PIN="$valid_xray_sha" \
  XRAY_ASSET_PIN="Xray-linux-64.zip" \
  SINGBOX_VERSION_PIN="1.14.3" \
  SINGBOX_SHA256_PIN="$valid_singbox_sha" \
  SINGBOX_ASSET_PIN="sing-box-1.14.3-linux-amd64.tar.gz" \
  MIHOMO_VERSION_PIN="1.19.32" \
  MIHOMO_SHA256_PIN="$valid_mihomo_sha" \
  MIHOMO_ASSET_PIN="mihomo-linux-amd64-v1.19.31.gz" \
    bash "$repo_root/scripts/update/resolve_validation_cores.sh" >/dev/null 2>&1; then
  echo "Expected mismatched Mihomo asset to be rejected." >&2
  exit 1
fi

echo "Pinned core validation tests passed."
