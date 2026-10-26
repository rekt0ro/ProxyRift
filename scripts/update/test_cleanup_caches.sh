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
if [[ "$skipped" -ne 7 ]]; then
  printf '%s\n' "$output" >&2
  echo "[ERROR] Expected all seven cache prefixes to be safely skipped." >&2
  exit 1
fi
if [[ -e "$tmp/gh.log" ]]; then
  cat "$tmp/gh.log" >&2
  echo "[ERROR] Cache cleanup called GitHub despite missing current keys." >&2
  exit 1
fi

echo "[OK] Cache cleanup safely skips prefixes without current cache keys."

cat > "$tmp/bin/gh" <<'GH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "cache" && "$2" == "list" ]]; then
  prefix=""
  while (($#)); do
    if [[ "$1" == "--key" && $# -ge 2 ]]; then
      prefix="$2"
      shift 2
    else
      shift
    fi
  done
  printf '[cache-list] %s\n' "$prefix" >> "$CLEANUP_GH_LOG"
  case "$prefix" in
    Linux-cargo-build-rust-)
      printf '101\tLinux-cargo-build-rust-1.99.1-legacy-old\n'
      printf '102\tLinux-cargo-build-rust-1.99.1-legacy-previous\n'
      ;;
    Linux-X64-cargo-build-rust-)
      printf '201\tLinux-X64-cargo-build-rust-1.99.1-arch-old\n'
      printf '202\tLinux-X64-cargo-build-rust-1.99.1-lock-current\n'
      ;;
  esac
  exit 0
fi
if [[ "$1" == "api" && "$2" == "--method" && "$3" == "DELETE" ]]; then
  printf '%s\n' "$4" >> "$CLEANUP_DELETE_LOG"
  exit 0
fi
echo "Unexpected GitHub operation: $*" >&2
exit 99
GH
chmod +x "$tmp/bin/gh"

: > "$tmp/gh.log"
: > "$tmp/deletes.log"
output="$(
  cd "$repo_root"
  PATH="$tmp/bin:$PATH" \
  CLEANUP_GH_LOG="$tmp/gh.log" CLEANUP_DELETE_LOG="$tmp/deletes.log" \
  GH_TOKEN="test-token" GH_REPO="rekt0ro/ProxyRift" \
  RUNNER_OS_NAME="Linux" RUNNER_ARCH_NAME="X64" \
  RUST_TOOLCHAIN_KEY="Linux-X64-rust-toolchain-1.99.1" \
  REGISTRY_KEY="Linux-cargo-registry-current" \
  BUILD_KEY="Linux-X64-cargo-build-rust-1.99.1-lock-current" \
  XRAY_KEY="Linux-xray-current" SINGBOX_KEY="Linux-sing-box-current" MIHOMO_KEY="Linux-mihomo-current" \
  bash scripts/update/cleanup_caches.sh 2>&1
)"

if ! grep -Fq '[cache-list] Linux-cargo-build-rust-' "$tmp/gh.log" ||
   ! grep -Fq '[cache-list] Linux-X64-cargo-build-rust-' "$tmp/gh.log"; then
  cat "$tmp/gh.log" >&2
  echo "[ERROR] Cache cleanup must inspect both legacy and architecture-aware Rust build cache prefixes." >&2
  exit 1
fi

if grep -Fq '/actions/caches/202' "$tmp/deletes.log"; then
  cat "$tmp/deletes.log" >&2
  echo "[ERROR] Cache cleanup deleted the current architecture-aware build cache." >&2
  exit 1
fi

for cache_id in 101 102 201; do
  if ! grep -Fq "/actions/caches/$cache_id" "$tmp/deletes.log"; then
    cat "$tmp/deletes.log" >&2
    echo "[ERROR] Expected obsolete cache $cache_id to be deleted." >&2
    exit 1
  fi
done

echo "[OK] Cache cleanup removes stale legacy and architecture-aware build caches while preserving the current key."

