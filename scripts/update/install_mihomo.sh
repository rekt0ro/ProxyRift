#!/usr/bin/env bash
set -euo pipefail

mkdir -p "$HOME/.local/bin" /tmp/mihomo-download

if [ ! -x "$HOME/.local/bin/mihomo" ]; then
  if [[ ! "$MIHOMO_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "MIHOMO_SHA256 is not a valid SHA-256 from the release metadata." >&2
    exit 1
  fi

  asset_url="https://github.com/${MIHOMO_REPOSITORY}/releases/download/v${MIHOMO_VERSION}/${MIHOMO_ASSET}"
  curl -fsSL --retry 5 --retry-delay 2 --retry-max-time 45 --retry-connrefused "$asset_url" -o /tmp/mihomo-download/mihomo.gz
  echo "${MIHOMO_SHA256}  /tmp/mihomo-download/mihomo.gz" | sha256sum -c -
  gzip -dc /tmp/mihomo-download/mihomo.gz > /tmp/mihomo-download/mihomo
  install -m 0755 /tmp/mihomo-download/mihomo "$HOME/.local/bin/mihomo"
fi

echo "$HOME/.local/bin" >> "$GITHUB_PATH"
mihomo_version="$("$HOME/.local/bin/mihomo" -v 2>&1)"
echo "$mihomo_version"
grep -F "$MIHOMO_VERSION" <<< "$mihomo_version" >/dev/null
