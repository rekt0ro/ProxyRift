#!/usr/bin/env bash
set -euo pipefail

files="all.txt light.txt all-base64.txt light-base64.txt all-clash.yaml light-clash.yaml all-singbox.json light-singbox.json"
publish_snapshot="$(mktemp -d /tmp/proxyrift-publish.XXXXXX)"
trap 'rm -rf "$publish_snapshot"' EXIT

mkdir -p "$publish_snapshot/subscriptions"
for f in $files; do
  test -s "subscriptions/${f}"
  cp -- "subscriptions/${f}" "$publish_snapshot/subscriptions/${f}"
done

if [ -s "subscriptions/light-history.json" ]; then
  cp -- subscriptions/light-history.json "$publish_snapshot/subscriptions/light-history.json"
fi

if [ -s "subscriptions/light-training.jsonl" ]; then
  cp -- subscriptions/light-training.jsonl "$publish_snapshot/subscriptions/light-training.jsonl"
fi
if [ -s "subscriptions/light-training-stats.json" ]; then
  cp -- subscriptions/light-training-stats.json "$publish_snapshot/subscriptions/light-training-stats.json"
fi
if [ -s "subscriptions/light-ai.json" ]; then
  cp -- subscriptions/light-ai.json "$publish_snapshot/subscriptions/light-ai.json"
fi
if [ -s "subscriptions/source-registry.json" ]; then
  cp -- subscriptions/source-registry.json "$publish_snapshot/subscriptions/source-registry.json"
fi

git config user.name "rekt0ro"
git config user.email "104654981+rekt0ro@users.noreply.github.com"

for attempt in 1 2 3 4 5; do
  echo "[INFO] 📤 [Publish] Attempt ${attempt}/5"

  git fetch origin main
  git reset --hard origin/main

  for f in $files; do
    cp -- "$publish_snapshot/subscriptions/${f}" "subscriptions/${f}"
  done

  if [ -f "$publish_snapshot/subscriptions/light-history.json" ]; then
    cp -- "$publish_snapshot/subscriptions/light-history.json" subscriptions/light-history.json
  fi

  if [ -f "$publish_snapshot/subscriptions/light-training.jsonl" ]; then
    cp -- "$publish_snapshot/subscriptions/light-training.jsonl" subscriptions/light-training.jsonl
  fi
  if [ -f "$publish_snapshot/subscriptions/light-training-stats.json" ]; then
    cp -- "$publish_snapshot/subscriptions/light-training-stats.json" subscriptions/light-training-stats.json
  fi
  if [ -f "$publish_snapshot/subscriptions/source-registry.json" ]; then
    cp -- "$publish_snapshot/subscriptions/source-registry.json" subscriptions/source-registry.json
  fi


  git add -- \
    subscriptions/all.txt \
    subscriptions/light.txt \
    subscriptions/all-base64.txt \
    subscriptions/light-base64.txt \
    subscriptions/all-clash.yaml \
    subscriptions/light-clash.yaml \
    subscriptions/all-singbox.json \
    subscriptions/light-singbox.json

  if [ -f "subscriptions/light-history.json" ]; then
    git add -- subscriptions/light-history.json
  fi

  if [ -f "subscriptions/light-training.jsonl" ]; then
    git add -- subscriptions/light-training.jsonl
  fi
  if [ -f "subscriptions/light-training-stats.json" ]; then
    git add -- subscriptions/light-training-stats.json
  fi
  if [ -f "subscriptions/source-registry.json" ]; then
    git add -- subscriptions/source-registry.json
  fi


  if git diff --cached --quiet; then
    echo "[INFO] 📤 [Publish] No changes"
    exit 0
  fi

  git commit -m "Update subscriptions"

  push_output=""
  push_status=0
  push_output="$(git push origin HEAD:main 2>&1)" || push_status=$?
  printf '%s\n' "$push_output"

  if [ "$push_status" -eq 0 ]; then
    echo "[INFO] ✅ [Publish] Subscription update published"
    exit 0
  fi

  if grep -Eiq 'non-fast-forward|fetch first|tip of your current branch is behind|updates were rejected because the tip' <<< "$push_output"; then
    echo "[WARN] ⚠️ [Publish] Main moved | Retrying"
    git reset --hard
    continue
  fi

  if grep -Eiq 'internal server error|http/2.*(500|502|503|504)| (500|502|503|504) |temporarily unavailable|service unavailable|bad gateway|gateway timeout' <<< "$push_output"; then
    if [ "$attempt" -lt 5 ]; then
      retry_delay=$((attempt * 2))
      echo "[WARN] ⚠️ [Publish] Transient GitHub server error | Retrying in ${retry_delay}s"
      git reset --hard
      sleep "$retry_delay"
      continue
    fi

    echo "[ERROR] ❌ [Publish] GitHub server error persisted after 5 attempts" >&2
    exit "$push_status"
  fi

  echo "[ERROR] ❌ [Publish] Push failed for a non-race reason" >&2
  exit "$push_status"
done

echo "[ERROR] ❌ [Publish] Failed after 5 attempts" >&2
exit 1
