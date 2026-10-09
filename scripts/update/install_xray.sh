#!/usr/bin/env bash
set -euo pipefail

mkdir -p "$HOME/.local/bin" /tmp/xray-download

if [ ! -x "$HOME/.local/bin/xray" ]; then
  if [[ ! "$XRAY_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "XRAY_SHA256 is not a valid SHA-256 from the release metadata." >&2
    exit 1
  fi

  asset_url="https://github.com/${XRAY_REPOSITORY}/releases/download/v${XRAY_VERSION}/${XRAY_ASSET}"
  curl -fsSL "$asset_url" -o /tmp/xray-download/xray.zip
  echo "${XRAY_SHA256}  /tmp/xray-download/xray.zip" | sha256sum -c -
  unzip -q /tmp/xray-download/xray.zip -d /tmp/xray-download/extracted
  install -m 0755 /tmp/xray-download/extracted/xray "$HOME/.local/bin/xray"
fi

echo "$HOME/.local/bin" >> "$GITHUB_PATH"
"$HOME/.local/bin/xray" version
