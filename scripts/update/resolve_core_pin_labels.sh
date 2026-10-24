#!/usr/bin/env bash
set -euo pipefail

core_labels=()
toolchain_labels=()

if ! git diff --quiet -- .python-version; then
  toolchain_labels+=("python")
fi
if ! git diff --quiet -- .ruby-version; then
  toolchain_labels+=("ruby")
fi
if ! git diff --quiet -- rust-toolchain.toml; then
  toolchain_labels+=("rust")
fi

pin_diff="$(git diff --unified=0 -- .github/actions/prepare-update/action.yml)"
if grep -Eq '^[+-][[:space:]]*XRAY_(VERSION|SHA256|ASSET)_PIN:' <<< "${pin_diff}"; then
  core_labels+=("xray")
fi
if grep -Eq '^[+-][[:space:]]*SINGBOX_(VERSION|SHA256|ASSET)_PIN:' <<< "${pin_diff}"; then
  core_labels+=("sing-box")
fi
if grep -Eq '^[+-][[:space:]]*MIHOMO_(VERSION|SHA256|ASSET)_PIN:' <<< "${pin_diff}"; then
  core_labels+=("mihomo")
fi

labels=()
if ((${#core_labels[@]} > 0)); then
  labels+=("dependencies" "${core_labels[@]}")
fi
if ((${#toolchain_labels[@]} > 0)); then
  labels+=("toolchain" "${toolchain_labels[@]}")
fi

printf '%s\n' "${labels[@]}"
