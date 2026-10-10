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
  bash "$repo_root/scripts/autopilot/watchdog.sh" >"$tmp/watchdog.log" 2>&1; then
  cat "$tmp/watchdog.log" >&2
  echo "[ERROR] Watchdog rejected valid outputs or failed during cleanup." >&2
  exit 1
fi

grep --fixed-strings --quiet "[OK] Published outputs passed integrity checks (Light=50, All=1000)." "$tmp/watchdog.log"
if grep --quiet "tmp: unbound variable" "$tmp/watchdog.log"; then
  echo "[ERROR] Watchdog cleanup still references an unset temporary-directory variable." >&2
  exit 1
fi

echo "[OK] Watchdog validates healthy outputs and exits cleanly."
