#!/usr/bin/env bash
set -euo pipefail

cleanup_prefix() {
  local prefix="$1"
  local current_key="$2"

  if [ -z "$current_key" ]; then
    echo "[WARN] Skipping obsolete-cache cleanup for prefix '$prefix': current key unavailable."
    return 0
  fi

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
cleanup_prefix "${RUNNER_OS_NAME}-mihomo-" "$MIHOMO_KEY"
