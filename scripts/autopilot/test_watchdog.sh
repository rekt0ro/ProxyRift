#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fixtures="$tmp/fixtures"
bin="$tmp/bin"
mkdir -p "$fixtures" "$bin"

for ((i = 1; i <= 1000; i++)); do
  printf 'vless://all-%s@example.com:443\n' "$i"
done > "$fixtures/all.txt"

for ((i = 1; i <= 50; i++)); do
  printf 'vless://light-%s@example.net:443\n' "$i"
done > "$fixtures/light.txt"

python3 - "$fixtures" <<'PY'
import base64
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for name in ("all", "light"):
    source = (root / f"{name}.txt").read_bytes().rstrip(b"\r\n")
    (root / f"{name}-base64.txt").write_text(
        base64.b64encode(source).decode("ascii") + "\n", encoding="ascii"
    )
    (root / f"{name}-singbox.json").write_text(
        '{"outbounds":[{"type":"direct","tag":"direct"}]}\n', encoding="utf-8"
    )
    (root / f"{name}-clash.yaml").write_text(
        'proxies:\n  - name: "test proxy"\n    type: socks5\n'
        '    server: example.com\n    port: 1080\n', encoding="utf-8"
    )
PY

cat > "$bin/curl" <<'CURL'
#!/usr/bin/env bash
set -euo pipefail
url=""
output=""
while (($#)); do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    http://*|https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
file="${url##*/}"
file="${file%%\?*}"
printf '%s\n' "$url" >> "${WATCHDOG_CURL_LOG:?}"
cp "${WATCHDOG_FIXTURES:?}/$file" "$output"
CURL
chmod +x "$bin/curl"

cat > "$tmp/event.json" <<'JSON'
{"workflow_run":{"id":123,"name":"Update Configs","conclusion":"success","run_attempt":1}}
JSON

if ! WATCHDOG_FIXTURES="$fixtures" \
  PATH="$bin:$PATH" \
  GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" \
  EVENT_PATH="$tmp/event.json" \
  GITHUB_STEP_SUMMARY="$tmp/summary.md" \
  GH_TOKEN="test-token" \
  GITHUB_SHA="fixture-sha" \
  WATCHDOG_CURL_LOG="$tmp/curl.log" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" >"$tmp/watchdog.log" 2>&1; then
  cat "$tmp/watchdog.log" >&2
  echo "[ERROR] Watchdog rejected valid outputs or failed during cleanup." >&2
  exit 1
fi

grep --fixed-strings --quiet "[OK] Published outputs passed integrity checks (Light=50, All=1000)." "$tmp/watchdog.log"
if [[ "$(grep --count --fixed-strings '?ref=fixture-sha' "$tmp/curl.log")" -ne 8 ]]; then
  echo "[ERROR] Watchdog did not pin every published-output request to GITHUB_SHA." >&2
  exit 1
fi
if [[ "$(grep --count --fixed-strings 'https://api.github.com/repos/rekt0ro/ProxyRift/contents/subscriptions/' "$tmp/curl.log")" -ne 8 ]]; then
  echo "[ERROR] Watchdog did not fetch outputs through the GitHub Contents API." >&2
  exit 1
fi
if grep --quiet "tmp: unbound variable" "$tmp/watchdog.log"; then
  echo "[ERROR] Watchdog cleanup still references an unset temporary-directory variable." >&2
  exit 1
fi

for ((i = 1; i <= 201; i++)); do
  printf 'vless://light-%s@example.net:443\n' "$i"
done > "$fixtures/light.txt"

if WATCHDOG_FIXTURES="$fixtures" \
  PATH="$bin:$PATH" \
  GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" \
  EVENT_PATH="$tmp/event.json" \
  GITHUB_STEP_SUMMARY="$tmp/summary-max.md" \
  GH_TOKEN="test-token" \
  GITHUB_SHA="fixture-sha" \
  WATCHDOG_CURL_LOG="$tmp/curl-max.log" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" >"$tmp/watchdog-max.log" 2>&1; then
  echo "[ERROR] Watchdog accepted a published Light subscription above 200." >&2
  exit 1
fi
grep -Fq "maximum is 200" "$tmp/watchdog-max.log"

retry_bin="$tmp/retry-bin"
mkdir -p "$retry_bin"
cat > "$retry_bin/gh" <<'GH'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *"/actions/runs/123/jobs?per_page=100"*)
    if [[ "${WATCHDOG_PUBLISHED:-false}" == "true" ]]; then
      printf '{"jobs":[{"steps":[{"name":"🚀 Publish generated changes","conclusion":"success"}]}]}\n'
    elif [[ "${WATCHDOG_GATE_FAILURE:-false}" == "true" ]]; then
      if [[ "${WATCHDOG_STATE_PERSISTED:-false}" == "true" ]]; then
        printf '{"jobs":[{"steps":[{"name":"📦 Prepare published outputs","conclusion":"failure"},{"name":"📚 Persist Light learning state","conclusion":"success"},{"name":"🚀 Publish generated changes","conclusion":"skipped"}]}]}\n'
      else
        printf '{"jobs":[{"steps":[{"name":"📦 Prepare published outputs","conclusion":"failure"},{"name":"📚 Persist Light learning state","conclusion":"failure"},{"name":"🚀 Publish generated changes","conclusion":"skipped"}]}]}\n'
      fi
    else
      printf '{"jobs":[{"steps":[{"name":"🚀 Publish generated changes","conclusion":"skipped"}]}]}\n'
    fi
    ;;
  *"/rerun-failed-jobs"*)
    printf '%s\n' "$*" >> "${WATCHDOG_GH_LOG:?}"
    printf '{"message":"accepted"}\n'
    ;;
  *) printf '{}\n' ;;
esac
GH
chmod +x "$retry_bin/gh"
cat > "$tmp/failure-event.json" <<'JSON'
{"workflow_run":{"id":123,"name":"Update Configs","conclusion":"failure","run_attempt":1}}
JSON
: > "$tmp/gh-calls.log"

WATCHDOG_PUBLISHED=true WATCHDOG_GH_LOG="$tmp/gh-calls.log" \
  PATH="$retry_bin:$PATH" GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" EVENT_PATH="$tmp/failure-event.json" \
  GITHUB_STEP_SUMMARY="$tmp/post-publish-summary.md" GH_TOKEN="test-token" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" > "$tmp/post-publish.log" 2>&1
grep -Fq "failed after publishing outputs; not retrying" "$tmp/post-publish.log"
if grep -Fq "rerun-failed-jobs" "$tmp/gh-calls.log"; then
  echo "[ERROR] Watchdog retried a run after successful publication." >&2
  exit 1
fi

: > "$tmp/gate-gh-calls.log"
WATCHDOG_GATE_FAILURE=true WATCHDOG_STATE_PERSISTED=true WATCHDOG_GH_LOG="$tmp/gate-gh-calls.log" \
  PATH="$retry_bin:$PATH" GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" EVENT_PATH="$tmp/failure-event.json" \
  GITHUB_STEP_SUMMARY="$tmp/gate-summary.md" GH_TOKEN="test-token" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" > "$tmp/gate-failure.log" 2>&1
grep -Fq "failed the deterministic publication gate after persisting Light learning state; not retrying" "$tmp/gate-failure.log"
if grep -Fq "rerun-failed-jobs" "$tmp/gate-gh-calls.log"; then
  echo "[ERROR] Watchdog retried after the publication gate failed despite persisted learning state." >&2
  exit 1
fi

: > "$tmp/gate-state-failure-gh-calls.log"
WATCHDOG_GATE_FAILURE=true WATCHDOG_STATE_PERSISTED=false WATCHDOG_GH_LOG="$tmp/gate-state-failure-gh-calls.log" \
  PATH="$retry_bin:$PATH" GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" EVENT_PATH="$tmp/failure-event.json" \
  GITHUB_STEP_SUMMARY="$tmp/gate-state-failure-summary.md" GH_TOKEN="test-token" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" > "$tmp/gate-state-failure.log" 2>&1
grep -Fq "retrying its failed jobs once" "$tmp/gate-state-failure.log"
grep -Fq "rerun-failed-jobs" "$tmp/gate-state-failure-gh-calls.log"

WATCHDOG_PUBLISHED=false WATCHDOG_GH_LOG="$tmp/gh-calls.log" \
  PATH="$retry_bin:$PATH" GH_REPOSITORY="rekt0ro/ProxyRift" \
  EVENT_NAME="workflow_run" EVENT_PATH="$tmp/failure-event.json" \
  GITHUB_STEP_SUMMARY="$tmp/pre-publish-summary.md" GH_TOKEN="test-token" \
  bash "$repo_root/scripts/autopilot/watchdog.sh" > "$tmp/pre-publish.log" 2>&1
grep -Fq "retrying its failed jobs once" "$tmp/pre-publish.log"
grep -Fq "rerun-failed-jobs" "$tmp/gh-calls.log"

echo "[OK] Watchdog validates outputs, rejects excessive Light output, and only retries pre-publication failures."
