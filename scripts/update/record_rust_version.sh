#!/usr/bin/env bash
set -euo pipefail

actual_version="$(rustc --version | awk '{print $2}')"
test "$actual_version" = "$RUST_TOOLCHAIN_VERSION"
echo "version=$actual_version" >> "$GITHUB_OUTPUT"
