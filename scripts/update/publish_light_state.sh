#!/usr/bin/env bash
set -euo pipefail

state_files=(
  light-history.json
  light-training.jsonl
  light-training-stats.json
  light-candidate-ledger.json
  source-registry.json
)
publish_snapshot="$(mktemp -d /tmp/proxyrift-light-state.XXXXXX)"
trap 'rm -rf "$publish_snapshot"' EXIT

mkdir -p "$publish_snapshot/subscriptions"
snapshot_files=()
for file in "${state_files[@]}"; do
  if [ -s "subscriptions/$file" ]; then
    cp -- "subscriptions/$file" "$publish_snapshot/subscriptions/$file"
    snapshot_files+=("$file")
  fi
done
if ((${#snapshot_files[@]} == 0)); then
  echo "[ERROR] [Light state] No learning-state files are available to persist" >&2
  exit 1
fi

git config user.name "rekt0ro"
git config user.email "104654981+rekt0ro@users.noreply.github.com"

for attempt in 1 2 3 4 5; do
  echo "[INFO] [Light state] Publish attempt $attempt/5"
  git fetch origin main
  git reset --hard origin/main

  staged_files=()
  for file in "${state_files[@]}"; do
    if [ -s "$publish_snapshot/subscriptions/$file" ]; then
      cp -- "$publish_snapshot/subscriptions/$file" "subscriptions/$file"
      staged_files+=("subscriptions/$file")
    fi
  done

  if ((${#staged_files[@]} == 0)); then
    echo "[INFO] [Light state] No learning state to persist"
    exit 0
  fi

  git add -- "${staged_files[@]}"
  if git diff --cached --quiet; then
    echo "[INFO] [Light state] No changes"
    exit 0
  fi

  git commit -m "Update Light validation state"

  push_output=""
  push_status=0
  push_output="$(git push origin HEAD:main 2>&1)" || push_status=$?
  printf '%s\n' "$push_output"

  if [ "$push_status" -eq 0 ]; then
    echo "[INFO] [Light state] Learning state persisted"
    exit 0
  fi

  if grep -Eiq 'non-fast-forward|fetch first|tip of your current branch is behind|updates were rejected because the tip' <<< "$push_output"; then
    echo "[WARN] [Light state] Main moved | Retrying"
    git reset --hard
    continue
  fi

  if grep -Eiq 'internal server error|http/2.*(500|502|503|504)| (500|502|503|504) |temporarily unavailable|service unavailable|bad gateway|gateway timeout' <<< "$push_output"; then
    if [ "$attempt" -lt 5 ]; then
      retry_delay=$((attempt * 2))
      echo "[WARN] [Light state] Transient GitHub server error | Retrying in ${retry_delay}s"
      git reset --hard
      sleep "$retry_delay"
      continue
    fi

    echo "[ERROR] [Light state] GitHub server error persisted after 5 attempts" >&2
    exit "$push_status"
  fi

  echo "[ERROR] [Light state] Push failed for a non-race reason" >&2
  exit "$push_status"
done

echo "[ERROR] [Light state] Failed after 5 attempts" >&2
exit 1
