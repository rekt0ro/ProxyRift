#!/usr/bin/env bash
set -euo pipefail

[[ "$XRAY_VERSION_PIN" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
[[ "$XRAY_SHA256_PIN" =~ ^[0-9a-f]{64}$ ]]
[[ "$SINGBOX_VERSION_PIN" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
[[ "$SINGBOX_SHA256_PIN" =~ ^[0-9a-f]{64}$ ]]
test "$XRAY_ASSET_PIN" = "Xray-linux-64.zip"
test "$SINGBOX_ASSET_PIN" = "sing-box-1.14.2-linux-amd64.tar.gz"

{
  echo "xray_version=$XRAY_VERSION_PIN"
  echo "xray_sha256=$XRAY_SHA256_PIN"
  echo "xray_asset=$XRAY_ASSET_PIN"
  echo "singbox_version=$SINGBOX_VERSION_PIN"
  echo "singbox_sha256=$SINGBOX_SHA256_PIN"
  echo "singbox_asset=$SINGBOX_ASSET_PIN"
} >> "$GITHUB_OUTPUT"

echo "[INFO] 🧩 [Cores] Pinned releases | Xray $XRAY_VERSION_PIN | sing-box $SINGBOX_VERSION_PIN"
