#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

current_branch="$(git branch --show-current)"
if [[ "$current_branch" != "main" ]]; then
  echo "[ERROR] Start this helper from local main, not '$current_branch'." >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "[ERROR] Working tree is not clean. Commit or stash local changes first." >&2
  git status --short
  exit 1
fi

git fetch origin main

if ! git merge-base --is-ancestor HEAD origin/main; then
  echo "[ERROR] Local main contains commits that are not on origin/main." >&2
  echo "[ERROR] Preserve them on a branch before using this helper." >&2
  exit 1
fi

git pull --ff-only origin main

for binary in xray sing-box; do
  if ! command -v "$binary" >/dev/null 2>&1; then
    echo "[ERROR] Required binary not found in PATH: $binary" >&2
    exit 1
  fi
done

mkdir -p /tmp/proxyrift
envfile=/tmp/proxyrift-github-env

cleanup() {
  git restore --quiet Cargo.lock 2>/dev/null || true
  git restore --quiet --     subscriptions/light-consumer-evidence.json     subscriptions/light-history.json     subscriptions/light-training-stats.json     subscriptions/light-training.jsonl     2>/dev/null || true

  rm -f /tmp/proxyrift-github-env         /tmp/proxyrift/light-local-next.txt         /tmp/proxyrift/light-local-stats.json
}

trap cleanup EXIT

env GITHUB_ENV="$envfile" bash scripts/install_lightgbm.sh

LIGHTGBM_LIB_DIR="$(grep '^LIGHTGBM_LIB_DIR=' "$envfile" | cut -d= -f2-)"
if [[ -z "$LIGHTGBM_LIB_DIR" ]]; then
  echo "[ERROR] LIGHTGBM_LIB_DIR was not exported by install_lightgbm.sh." >&2
  exit 1
fi
export LIGHTGBM_LIB_DIR
export LD_LIBRARY_PATH="$LIGHTGBM_LIB_DIR"

env LIGHTGBM_LIB_DIR="$LIGHTGBM_LIB_DIR" \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    cargo build --release --bin light_consumer_test --bin polish_light

git restore --quiet Cargo.lock

echo
echo "[1/2] Consumer test on this network"
echo "[INFO] Full pass/fail history is kept locally in ignored:"
echo "       subscriptions/light-consumer-results.json"

env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    ./target/release/light_consumer_test \
    --input subscriptions/light.txt \
    --history subscriptions/light-consumer-results.json \
    --adaptive \
    --max-candidates 96 \
    --deep-candidates 64 \
    --deep-rounds 3 \
    --workers 16 \
    --batch-size 64 \
    --timeout 6 \
    --xray-timeout 3

echo
echo "[2/2] Local Light funnel using the fresh consumer history"

env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    ./target/release/polish_light \
    --candidates subscriptions/light.txt \
    --output /tmp/proxyrift/light-local-next.txt \
    --workers 8 \
    --batch-size 24 \
    --selected-workers 8 \
    --selected-batch-size 32 \
    --timeout 15 \
    --selection-limit 200 \
    --max-per-endpoint 1 \
    --max-per-family 3 \
    --stats /tmp/proxyrift/light-local-stats.json \
    --record-performance-consumer-evidence

echo
echo "[INFO] Local artifacts produced by polish_light (before cleanup):"
if [[ -s /tmp/proxyrift/light-local-next.txt ]]; then
  light_count="$(awk 'NF { count++ } END { print count + 0 }' /tmp/proxyrift/light-local-next.txt)"
  echo "       Next Light candidate count: $light_count"
else
  echo "       No local Light output was produced."
fi

if [[ -s /tmp/proxyrift/light-local-stats.json ]]; then
  cat /tmp/proxyrift/light-local-stats.json
fi

echo
echo "[INFO] Git state before cleanup:"
git status --short

cleanup
trap - EXIT

echo
echo "[OK] Local test complete. Tracked generated state was restored."
echo "[OK] Persistent consumer history remains local:"
echo "     subscriptions/light-consumer-results.json"
echo
echo "[INFO] No branch, commit, push, or PR is created by this helper."
