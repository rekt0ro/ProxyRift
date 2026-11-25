#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
gate="$repo_root/scripts/update/verify_dependabot_auto_merge_checks.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/ready.json" <<'JSON'
[
  {"name":"validate","workflow":"CI","bucket":"pass"},
  {"name":"CodeQL","workflow":"CodeQL","bucket":"fail"},
  {"name":"Analyze (rust)","workflow":"CodeQL","bucket":"pending"},
  {"name":"Analyze (ruby)","workflow":"CodeQL","bucket":"fail"},
  {"name":"Analyze (python)","workflow":"CodeQL","bucket":"cancel"}
]
JSON

if ! bash "$gate" < "$tmp/ready.json" > "$tmp/ready.log" 2>&1; then
  cat "$tmp/ready.log" >&2
  echo "[ERROR] A non-CI check incorrectly blocked auto-merge." >&2
  exit 1
fi
grep -Fq "All CI checks passed" "$tmp/ready.log"
echo "[OK] Failed, pending, and cancelled CodeQL checks are ignored."

jq 'map(select(.workflow != "CI"))' "$tmp/ready.json" > "$tmp/no-ci.json"
if bash "$gate" < "$tmp/no-ci.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a PR with no CI checks." >&2
  exit 1
fi
echo "[OK] Missing CI blocks auto-merge."

jq '. + [{"name":"build","workflow":"CI","bucket":"pending"}]' "$tmp/ready.json" > "$tmp/pending-ci.json"
if bash "$gate" < "$tmp/pending-ci.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a pending CI check." >&2
  exit 1
fi
echo "[OK] Pending CI blocks auto-merge."

jq '. + [{"name":"tests","workflow":"CI","bucket":"fail"}]' "$tmp/ready.json" > "$tmp/failed-ci.json"
if bash "$gate" < "$tmp/failed-ci.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a failed CI check." >&2
  exit 1
fi
echo "[OK] Failed CI blocks auto-merge."

jq '. + [{"name":"tests","workflow":"CI","bucket":"cancel"}]' "$tmp/ready.json" > "$tmp/cancelled-ci.json"
if bash "$gate" < "$tmp/cancelled-ci.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a cancelled CI check." >&2
  exit 1
fi
echo "[OK] Cancelled CI blocks auto-merge."

echo "[OK] Dependabot auto-merge check policy tests passed."
