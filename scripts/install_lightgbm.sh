#!/usr/bin/env bash
set -euo pipefail

release_json="$(curl -fsSL \
  --retry 5 \
  --retry-delay 2 \
  --retry-max-time 45 \
  --retry-connrefused \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/lightgbm-org/LightGBM/releases/latest)"

tag="$(jq -r '.tag_name // empty' <<< "$release_json")"
version="\${tag#v}"

[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]

asset_name="lib_lightgbm.so"
asset_url="$(jq -r --arg name "$asset_name" '.assets[] | select(.name == $name) | .browser_download_url' <<< "$release_json" | head -n 1)"
asset_digest="$(jq -r --arg name "$asset_name" '.assets[] | select(.name == $name) | .digest // empty' <<< "$release_json" | head -n 1)"

test -n "$asset_url"
[[ "$asset_digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]]

sha256="\${asset_digest#sha256:}"
install_dir="\${RUNNER_TEMP:-/tmp}/proxyrift/lightgbm/$version"
mkdir -p "$install_dir"

curl -fsSL \
  --retry 5 \
  --retry-delay 2 \
  --retry-max-time 45 \
  --retry-connrefused \
  "$asset_url" \
  -o "$install_dir/$asset_name"

echo "$sha256  $install_dir/$asset_name" | sha256sum -c -

{
  echo "LIGHTGBM_VERSION=$version"
  echo "LIGHTGBM_LIB_DIR=$install_dir"
  echo "LD_LIBRARY_PATH=$install_dir:\${LD_LIBRARY_PATH:-}"
} >> "$GITHUB_ENV"

echo "[INFO] [LightGBM] Native runtime | $version | $install_dir"
