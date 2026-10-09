#!/usr/bin/env bash
set -euo pipefail

{
  echo "rust-toolchain=${RUNNER_OS_NAME}-$RUNNER_ARCH_NAME-rust-toolchain-${RUST_VERSION}"
  echo "registry=${RUNNER_OS_NAME}-cargo-registry-${LOCK_HASH}"
  echo "build=${RUNNER_OS_NAME}-cargo-build-rust-${RUST_VERSION}-${LOCK_HASH}"
  echo "xray=${RUNNER_OS_NAME}-xray-${XRAY_VERSION}-${XRAY_SHA256}-verified"
  echo "singbox=${RUNNER_OS_NAME}-sing-box-${SINGBOX_VERSION}-${SINGBOX_SHA256}-verified"
} >> "$GITHUB_OUTPUT"
