#!/usr/bin/env bash
set -euo pipefail

toolchain="$RUST_TOOLCHAIN_VERSION"
if ! rustup toolchain list | awk '{print $1}' | grep -E "^$toolchain(-|$)" >/dev/null; then
  rustup toolchain install "$toolchain" --profile minimal --component rustfmt
fi
rustup default "$toolchain"
