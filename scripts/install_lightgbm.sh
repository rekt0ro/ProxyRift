#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] [LightGBM] Resolving latest stable release"

github_auth=()
if [[ -n "${GH_TOKEN:-}" ]]; then
  github_auth+=( -H "Authorization: Bearer ${GH_TOKEN}" )
fi

release_json="$(curl -fsSL \
  --retry 5 \
  --retry-delay 2 \
  --retry-max-time 45 \
  --retry-connrefused \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -H "User-Agent: ProxyRift" \
  "${github_auth[@]}" \
  https://api.github.com/repos/lightgbm-org/LightGBM/releases/latest)"

tag="$(jq -r '.tag_name // empty' <<< "$release_json")"
version="$tag"
version="${version#v}"

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "[ERROR] [LightGBM] Invalid latest release tag: $tag" >&2
  exit 1
fi

asset_name="lib_lightgbm.so"
asset_digest="$(jq -r --arg name "$asset_name" '.assets[] | select(.name == $name) | .digest // empty' <<< "$release_json" | head -n 1)"
asset_api_url="$(jq -r --arg name "$asset_name" '.assets[] | select(.name == $name) | .url // empty' <<< "$release_json" | head -n 1)"

if [[ ! "$asset_digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]]; then
  echo "[ERROR] [LightGBM] Missing SHA-256 digest for $asset_name" >&2
  exit 1
fi

if [[ ! "$asset_api_url" =~ ^https://api\.github\.com/repos/lightgbm-org/LightGBM/releases/assets/[0-9]+$ ]]; then
  echo "[ERROR] [LightGBM] Missing or invalid API URL for $asset_name: $asset_api_url" >&2
  exit 1
fi

sha256="${asset_digest#sha256:}"
install_dir="${RUNNER_TEMP:-/tmp}/proxyrift/lightgbm/${version}"
mkdir -p "$install_dir"

echo "[INFO] [LightGBM] Download | $asset_api_url"
curl -fL \
  --retry 5 \
  --retry-delay 2 \
  --retry-max-time 45 \
  --retry-connrefused \
  -H "Accept: application/octet-stream" \
  -H "User-Agent: ProxyRift" \
  "${github_auth[@]}" \
  "$asset_api_url" \
  -o "$install_dir/$asset_name"

echo "$sha256  $install_dir/$asset_name" | sha256sum -c -

existing_ld="${LD_LIBRARY_PATH:-}"
{
  echo "LIGHTGBM_VERSION=$version"
  echo "LIGHTGBM_LIB_DIR=$install_dir"
  echo "LD_LIBRARY_PATH=$install_dir:$existing_ld"
} >> "$GITHUB_ENV"

echo "[INFO] [LightGBM] Native runtime | $version | $install_dir"
