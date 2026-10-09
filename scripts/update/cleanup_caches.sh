#!/usr/bin/env bash
set -euo pipefail

for k in "$RUST_TOOLCHAIN_KEY" "$REGISTRY_KEY" "$BUILD_KEY" "$XRAY_KEY" "$SINGBOX_KEY"; do
  if [ -z "$k" ]; then
    echo "Refusing cache cleanup because a current cache key is empty." >&2
    exit 1
  fi
done

cleanup_prefix() {
  local prefix="$1"
  local current_key="$2"

  gh cache list \
    --repo "$GH_REPO" \
    --key "$prefix" \
    --ref refs/heads/main \
    --limit 100 \
    --json id,key \
    --jq '.[] | [.id, .key] | @tsv' |
  while IFS=$'\t' read -r cache_id cache_key; do
    [ -z "$cache_id" ] && continue
    if [ "$cache_key" = "$current_key" ]; then
      continue
    fi
    echo "Deleting obsolete cache $cache_id ($cache_key)"
    gh api --method DELETE "/repos/$GH_REPO/actions/caches/$cache_id"
  done
}

cleanup_prefix "${RUNNER_OS_NAME}-${RUNNER_ARCH_NAME}-rust-toolchain-" "$RUST_TOOLCHAIN_KEY"
cleanup_prefix "${RUNNER_OS_NAME}-cargo-registry-" "$REGISTRY_KEY"
cleanup_prefix "${RUNNER_OS_NAME}-cargo-build-rust-" "$BUILD_KEY"
cleanup_prefix "${RUNNER_OS_NAME}-xray-" "$XRAY_KEY"
cleanup_prefix "${RUNNER_OS_NAME}-sing-box-" "$SINGBOX_KEY"
