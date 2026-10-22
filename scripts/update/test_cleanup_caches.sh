#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"
cat > "$tmp/bin/gh" <<'GH'
#!/usr/bin/env bash
echo "Unexpected GitHub cache operation with a missing current key: $*" >> "$CLEANUP_GH_LOG"
exit 99
GH
chmod +x "$tmp/bin/gh"

if ! output="$(
  cd "$repo_root"
  PATH="$tmp/bin:$PATH" \
  CLEANUP_GH_LOG="$tmp/gh.log" \
  GH_TOKEN="test-token" GH_REPO="rekt0ro/ProxyRift" \
  RUNNER_OS_NAME="Linux" RUNNER_ARCH_NAME="X64" \
  RUST_TOOLCHAIN_KEY="" REGISTRY_KEY="" BUILD_KEY="" XRAY_KEY="" SINGBOX_KEY="" MIHOMO_KEY="" \
  bash scripts/update/cleanup_caches.sh 2>&1
)"; then
  printf '%s\n' "$output" >&2
  echo "[ERROR] Cache cleanup should skip prefixes whose current keys are unavailable." >&2
  exit 1
fi

skipped="$(grep -Foc '[WARN] Skipping obsolete-cache cleanup' <<< "$output" || true)"
if [[ "$skipped" -ne 6 ]]; then
  printf '%s\n' "$output" >&2
  echo "[ERROR] Expected all six cache prefixes to be safely skipped." >&2
  exit 1
fi
if [[ -e "$tmp/gh.log" ]]; then
  cat "$tmp/gh.log" >&2
  echo "[ERROR] Cache cleanup called GitHub despite missing current keys." >&2
  exit 1
fi

echo "[OK] Cache cleanup safely skips prefixes without current cache keys."
