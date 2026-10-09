#!/usr/bin/env bash
set -euo pipefail

mkdir -p "$HOME/.local/bin" /tmp/singbox-download

if [ ! -x "$HOME/.local/bin/sing-box" ]; then
  if [[ ! "$SINGBOX_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "SINGBOX_SHA256 is not a valid SHA-256 from the release metadata." >&2
    exit 1
  fi

  asset_url="https://github.com/${SINGBOX_REPOSITORY}/releases/download/v${SINGBOX_VERSION}/${SINGBOX_ASSET}"
  curl -fsSL "$asset_url" -o /tmp/singbox-download/sing-box.tar.gz
  echo "${SINGBOX_SHA256}  /tmp/singbox-download/sing-box.tar.gz" | sha256sum -c -
  tar -xzf /tmp/singbox-download/sing-box.tar.gz -C /tmp/singbox-download
  install -m 0755 "/tmp/singbox-download/sing-box-${SINGBOX_VERSION}-linux-amd64/sing-box" "$HOME/.local/bin/sing-box"
fi

echo "$HOME/.local/bin" >> "$GITHUB_PATH"
singbox_version="$("$HOME/.local/bin/sing-box" version | head -n 1)"
echo "[INFO] 🧩 [Sing-Box] Version | ${singbox_version}"
grep -F "$SINGBOX_VERSION" <<< "$singbox_version" >/dev/null
