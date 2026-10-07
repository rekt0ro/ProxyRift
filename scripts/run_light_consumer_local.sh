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

if ! command -v gh >/dev/null 2>&1; then
  echo "[ERROR] GitHub CLI (gh) is required to open the evidence PR." >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "[ERROR] GitHub CLI is not authenticated. Run: gh auth login" >&2
  exit 1
fi

mkdir -p /tmp/proxyrift
envfile=/tmp/proxyrift-github-env

cleanup_temp() {
  git restore --quiet Cargo.lock 2>/dev/null || true
  git restore --quiet -- \
    subscriptions/light-history.json \
    subscriptions/light-training-stats.json \
    subscriptions/light-training.jsonl \
    2>/dev/null || true

  rm -f "$envfile" \
        /tmp/proxyrift/light-local-next.txt \
        /tmp/proxyrift/light-local-stats.json
}

trap cleanup_temp EXIT

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

env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    ./target/release/light_consumer_test \
    --input subscriptions/light.txt \
    --history subscriptions/light-consumer-results.json \
    --write-evidence subscriptions/light-consumer-evidence.json \
    --adaptive \
    --max-candidates 96 \
    --deep-candidates 64 \
    --deep-rounds 3 \
    --workers 16 \
    --batch-size 64 \
    --timeout 6 \
    --xray-timeout 3

polish_status=0
if env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
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
then
  :
else
  polish_status=$?
  echo "[WARN] Local polish_light exited with status $polish_status; consumer evidence will still be published." >&2
fi

echo
echo "===== LOCAL LIGHT RESULT ====="
if [[ -s /tmp/proxyrift/light-local-next.txt ]]; then
  awk 'NF { count++ } END { print "Next Light candidates:", count + 0 }' /tmp/proxyrift/light-local-next.txt
else
  echo "No local Light output was produced."
fi

echo
echo "===== LIGHT STATS ====="
cat /tmp/proxyrift/light-local-stats.json 2>/dev/null || true

cleanup_temp
trap - EXIT

if [[ ! -s subscriptions/light-consumer-evidence.json ]]; then
  echo "[ERROR] No consumer evidence file was produced." >&2
  exit "${polish_status:-1}"
fi

if git diff --quiet -- subscriptions/light-consumer-evidence.json; then
  echo "[INFO] No new consumer evidence changes were produced. Nothing to push or open."
  exit 0
fi

run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
branch="consumer-evidence/$run_id"
git switch -c "$branch"

git add subscriptions/light-consumer-evidence.json
git diff --cached --check
git commit -m "Update Light consumer evidence"
git push -u origin HEAD

pr_url="$(gh pr create \\
  --base main \\
  --head "$branch" \\
  --title "Update Light consumer evidence" \\
  --body "Consumer-network validation results from the current main subscriptions/light.txt.\\n\\nThis PR updates only the privacy-safe structural consumer evidence. Private subscriptions/light-consumer-results.json remains local." )"

echo
echo "[OK] Consumer evidence pushed to:"
echo "     $branch"
echo "[OK] Evidence PR opened:"
echo "     $pr_url"
echo
echo "[INFO] Merge this PR into main. On the next Update Configs run, main will contain the new evidence; then rerun this same command against the refreshed main Light list."
echo "       Private results remain local in subscriptions/light-consumer-results.json"
exit 0
