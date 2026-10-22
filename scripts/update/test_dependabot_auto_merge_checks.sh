#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
gate="$repo_root/scripts/update/verify_dependabot_auto_merge_checks.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/ready.json" <<'JSON'
[
  {"name":"validate","workflow":"CI","bucket":"pass"},
  {"name":"Analyze (rust)","workflow":"CodeQL","bucket":"pass"},
  {"name":"Analyze (ruby)","workflow":"CodeQL","bucket":"pass"},
  {"name":"Analyze (python)","workflow":"CodeQL","bucket":"pass"},
  {"name":"CodeQL","workflow":"CodeQL","bucket":"fail"},
  {"name":"Analyze (actions)","workflow":"CodeQL","bucket":"fail"}
]
JSON

if ! bash "$gate" < "$tmp/ready.json" > "$tmp/ready.log" 2>&1; then
  cat "$tmp/ready.log" >&2
  echo "[ERROR] Aggregate CodeQL or Analyze (actions) incorrectly blocked auto-merge." >&2
  exit 1
fi
grep -Fq "CI and Rust, Ruby, Python checks passed" "$tmp/ready.log"
echo "[OK] Failed aggregate CodeQL and Analyze (actions) are ignored."

jq 'map(select(.name != "Analyze (python)"))' "$tmp/ready.json" > "$tmp/missing-python.json"
if bash "$gate" < "$tmp/missing-python.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted missing Python check." >&2
  exit 1
fi
echo "[OK] Missing required language check blocks auto-merge."

jq 'map(if .name == "Analyze (ruby)" then .bucket = "pending" else . end)' "$tmp/ready.json" > "$tmp/pending-ruby.json"
if bash "$gate" < "$tmp/pending-ruby.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a pending Ruby check." >&2
  exit 1
fi
echo "[OK] Pending required language check blocks auto-merge."

jq 'map(if .workflow == "CI" then .bucket = "fail" else . end)' "$tmp/ready.json" > "$tmp/failed-ci.json"
if bash "$gate" < "$tmp/failed-ci.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a failed CI check." >&2
  exit 1
fi
echo "[OK] Failed CI blocks auto-merge."

jq 'map(if .name == "Analyze (rust)" then .bucket = "fail" else . end)' "$tmp/ready.json" > "$tmp/failed-rust.json"
if bash "$gate" < "$tmp/failed-rust.json" > /dev/null 2>&1; then
  echo "[ERROR] Auto-merge accepted a failed Rust check." >&2
  exit 1
fi
echo "[OK] Failed required language check blocks auto-merge."

echo "[OK] Dependabot auto-merge check policy tests passed."
