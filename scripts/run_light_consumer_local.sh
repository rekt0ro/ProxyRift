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

run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
branch="consumer-evidence/$run_id"
git switch -c "$branch"

cleanup() {
  rm -f /tmp/proxyrift-github-env \
        /tmp/proxyrift/light-local-next.txt \
        /tmp/proxyrift/light-local-stats.json
}
trap cleanup EXIT

mkdir -p /tmp/proxyrift
envfile=/tmp/proxyrift-github-env

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
    cargo build --release --bin polish_light --bin light_consumer_test

git restore Cargo.lock

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

env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    ./target/release/light_consumer_test \
    --input subscriptions/light.txt \
    --rounds 3 \
    --timeout 15 \
    --xray-timeout 5

git status
git diff --stat

git restore -- \
  subscriptions/light-consumer-evidence.json \
  subscriptions/light-history.json \
  subscriptions/light-training-stats.json \
  subscriptions/light-training.jsonl

git add subscriptions/light-consumer-results.json

if git diff --cached --quiet; then
  echo "[INFO] No Light evidence changes were produced."
  exit 0
fi

git diff --cached --check
git commit -m "Update Light consumer test history"

git push -u origin HEAD

echo
echo "[OK] Consumer evidence pushed safely to:"
echo "     $branch"
echo
echo "Open a PR into main, or run:"
echo "     gh pr create --base main --head '$branch' --title 'Update Light consumer evidence' --body 'Local Light validation and consumer evidence update.'"
