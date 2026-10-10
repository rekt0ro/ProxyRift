#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

work="$tmp/work"
mkdir -p "$work/subscriptions" "$work/proxyrift" "$tmp/bin"

printf 'vless://all@example.com:443\n' > "$work/subscriptions/all.txt"
printf 'vless://light@example.net:443\n' > "$work/subscriptions/light.txt"

cat > "$work/proxyrift/light-stats.json" <<'JSON'
{
  "input_candidates": 15000,
  "security_rejected": 0,
  "strict_verified": 140,
  "selection_target": 130,
  "strict_selectable": 130,
  "stability_selectable": 130,
  "transfer_selectable": 130,
  "stream_selectable": 143,
  "selection_shortfall": 0,
  "transfer_tested": 156,
  "transfer_passed": 143,
  "stream_continuity_tested": 143,
  "stream_continuity_passed": 143,
  "published": 143
}
JSON

cat > "$work/proxyrift/lightgbm-scores.json" <<'JSON'
{
  "trained": true,
  "training_rows": 50000,
  "feature_count": 12,
  "scores": {"candidate": 0.1},
  "model_report": {
    "targets": {
      "strict": {"accepted": true, "reason": "promoted_after_temporal_validation"},
      "transfer": {"accepted": false, "reason": "model_did_not_beat_temporal_baseline"},
      "stream": {"accepted": false, "reason": "top_20pct_below_random_selection_baseline"}
    }
  }
}
JSON

cat > "$tmp/bin/gh" <<'GH'
#!/usr/bin/env bash
case "$*" in
  *"actions/workflows/update.yml/runs"*) printf '{"workflow_runs":[]}\n' ;;
  *"actions/runs/"*"jobs"*) printf '{"jobs":[]}\n' ;;
  *) printf '{}\n' ;;
esac
GH
chmod +x "$tmp/bin/gh"

(
  cd "$work"
  PATH="$tmp/bin:$PATH" \
  PROXYRIFT_TMP_DIR="$work/proxyrift" \
  GH_REPOSITORY="test/repo" \
  GITHUB_RUN_ID="1" \
  GITHUB_RUN_NUMBER="1" \
  JOB_STATUS="success" \
  DISCOVERY_OUTCOME="success" \
  COLLECT_OUTCOME="success" \
  LIGHT_OUTCOME="success" \
  GENERATION_OUTCOME="success" \
  PREPARE_OUTCOME="success" \
  PUBLISH_OUTCOME="success" \
  XRAY_VERSION="test" \
  SINGBOX_VERSION="test" \
  GITHUB_STEP_SUMMARY="$tmp/summary.md" \
  bash "$repo_root/scripts/update/write_run_summary.sh"
)

grep -Fq '| All | 1 | TXT (.txt), Base64, Clash/Mihomo YAML, sing-box JSON |' "$tmp/summary.md"
grep -Fq '| Light | 1 | TXT (.txt), Base64, Clash/Mihomo YAML, sing-box JSON |' "$tmp/summary.md"
grep -Fq '| Strict validation | Promoted after temporal validation |' "$tmp/summary.md"
grep -Fq '| Transfer (10 MiB) | Did not beat temporal baseline |' "$tmp/summary.md"
grep -Fq '| Stream stability | Top-20% pass rate below random baseline |' "$tmp/summary.md"

if grep -Eq 'promoted_after_temporal_validation|model_did_not_beat_temporal_baseline|top_20pct_below_random_selection_baseline|\| Strict validation \| promoted \|' "$tmp/summary.md"; then
  echo "[ERROR] Summary contains a raw LightGBM reason or unnormalized status." >&2
  exit 1
fi

echo "[OK] Update summary reports TXT and human-readable temporal LightGBM statuses without metadata injection."
