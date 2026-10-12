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
  "input_candidates": 15000, "security_rejected": 0, "strict_verified": 300,
  "strict_attempts": 317, "selection_target": 200, "strict_selectable": 200,
  "stability_selectable": 200, "transfer_selectable": 200, "stream_selectable": 200,
  "selection_shortfall": 0, "transfer_tested": 232, "transfer_passed": 211,
  "stream_continuity_tested": 211, "stream_continuity_passed": 209, "published": 200,
  "historical_subscription_revisions": 8, "recent_family_keys": 400,
  "fresh_family_target_percent": 70, "score_independent_exploration_percent": 15,
  "published_from_fresh_families": 140, "published_from_recent_families": 60,
  "published_new_or_expired": 160, "published_revalidated": 40,
  "revalidated_candidate_max_percent": 20, "revalidation_window_seconds": 86400
}
JSON

cat > "$work/proxyrift/lightgbm-scores.json" <<'JSON'
{
  "version": 5, "trained": true, "training_rows": 1200, "feature_count": 61,
  "scores": {"vless://candidate@example.invalid:443": 0.8},
  "model_report": {
    "validation": "chronological_80_20_first_observation_of_unseen_candidates",
    "baseline": "training_median_log_stream_throughput",
    "ranking_strategy": "stream_speed_regression",
    "target": "log(stream_throughput_kbps)", "target_unit": "kbps",
    "accepted": true, "reason": "promoted_after_temporal_speed_validation",
    "cutoff_timestamp": 1800000000, "speed_samples": 1200,
    "unique_candidates": 1100, "unique_timestamps": 15,
    "train_rows": 960, "holdout_rows": 240,
    "model_log_mae": 0.1, "baseline_log_mae": 0.13,
    "relative_mae_improvement": 0.2307692, "top_20pct_speed_lift": 1.24,
    "minimum_relative_mae_improvement": 0.01, "minimum_top_20pct_speed_lift": 1.0
  }
}
JSON

cat > "$work/subscriptions/light-training-stats.json" <<'JSON'
{
  "schema_version": 3,
  "dataset": {
    "speed_samples": 1200, "new_speed_samples": 120, "unique_runs": 12,
    "unique_candidates": 1100, "strict_passes": 1200, "transfer_passes": 1200,
    "stream_passes": 1200
  },
  "readiness": {
    "speed_model_ready": true,
    "minimum_speed_samples": 40,
    "minimum_unique_runs": 3,
    "minimum_unique_candidates": 25
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

run_summary() {
  local output="$1"
  (
    cd "$work"
    PATH="$tmp/bin:$PATH" \
    PROXYRIFT_TMP_DIR="$work/proxyrift" \
    GH_REPOSITORY="test/repo" \
    GITHUB_RUN_ID="1" GITHUB_RUN_NUMBER="1" \
    JOB_STATUS="success" DISCOVERY_OUTCOME="success" COLLECT_OUTCOME="success" \
    LIGHT_OUTCOME="success" GENERATION_OUTCOME="success" PREPARE_OUTCOME="success" \
    PUBLISH_OUTCOME="success" XRAY_VERSION="test" SINGBOX_VERSION="test" \
    GITHUB_STEP_SUMMARY="$output" \
    bash "$repo_root/scripts/update/write_run_summary.sh"
  )
  cmp -s "$output" "$work/proxyrift/run-summary.md"
}

run_summary "$tmp/summary.md"
grep -Fq '| All | 1 |' "$tmp/summary.md"
grep -Fq '| Light | 1 |' "$tmp/summary.md"
grep -Fq '| Candidates considered | 15,000 |' "$tmp/summary.md"
grep -Fq '| Recent subscription revisions considered | 8 |' "$tmp/summary.md"
grep -Fq '| Recent family keys | 400 |' "$tmp/summary.md"
grep -Fq '| Configs from fresh families | 140 of 200 (70.0%; ranking preference: 70%) |' "$tmp/summary.md"
grep -Fq '| Score-independent discovery exploration | 15% |' "$tmp/summary.md"
grep -Fq '| Strict validation | 300 of 317 passed |' "$tmp/summary.md"
grep -Fq '| 10 MiB transfer | 211 of 232 passed |' "$tmp/summary.md"
grep -Fq '| Sustained stream | 209 of 211 passed |' "$tmp/summary.md"
grep -Fq '| Light configs published | 200 of 200 target |' "$tmp/summary.md"
grep -Fq '**Ranking:** Model-based ranking active — Stream-speed regression.' "$tmp/summary.md"
grep -Fq '**Training:** 1,200 speed samples (72-hour retention) · 61 features · Candidates scored: 1.' "$tmp/summary.md"
grep -Fq '**Evaluation:** Chronological 80/20 holdout on first observations of unseen candidates · Target: log stream throughput (kbps) · Baseline: Training median log stream throughput.' "$tmp/summary.md"
grep -Fq '| Holdout log-MAE (model / baseline) | 0.10000 / 0.13000 |' "$tmp/summary.md"
grep -Fq '| Relative log-MAE improvement | 23.1% |' "$tmp/summary.md"
grep -Fq '| Top-20% measured speed lift | 1.24x |' "$tmp/summary.md"
grep -Fq '| Train / unseen holdout rows | 960 / 240 |' "$tmp/summary.md"
grep -Fq '| Unique candidates / timestamps | 1,100 / 15 |' "$tmp/summary.md"
grep -Fq '**Model decision:** Accepted — promoted after temporal speed validation.' "$tmp/summary.md"
grep -Fq '**Training data:** 1,200 speed samples retained for 72 hours (120 new this run) · 1,100 unique candidates · 12 runs. Speed model: Ready.' "$tmp/summary.md"
grep -Fq '| Newly discovered / expired published candidates | 160 |' "$tmp/summary.md"
grep -Fq '| Revalidated candidates published | 40 of 200 (max 20%; validated within 86400s and still active) |' "$tmp/summary.md"

jq '
  .trained = false
  | .model_report.ranking_strategy = "historical_stream_speed_prior_fallback"
  | .model_report.accepted = false
  | .model_report.reason = "insufficient_speed_samples_or_temporal_coverage"
  | .model_report.baseline_log_mae = null
  | .model_report.model_log_mae = null
  | .model_report.relative_mae_improvement = null
  | .model_report.top_20pct_speed_lift = null
  | .model_report.train_rows = 0
  | .model_report.holdout_rows = 0
' "$work/proxyrift/lightgbm-scores.json" > "$tmp/fallback-scores.json"
cp "$tmp/fallback-scores.json" "$work/proxyrift/lightgbm-scores.json"
run_summary "$tmp/fallback-summary.md"
grep -Fq '**Ranking:** Historical speed-prior ranking active — Historical stream-speed priors.' "$tmp/fallback-summary.md"
grep -Fq '**Model decision:** Not promoted — insufficient speed samples or temporal coverage.' "$tmp/fallback-summary.md"

if grep -Eq 'training_prevalence_brier|Brier score \(model / baseline\)|Strict model:|End-to-end model:' "$tmp/summary.md"; then
  printf '[ERROR] Summary still contains legacy pass-fail model output.\n' >&2
  exit 1
fi
echo '[OK] Update summary reports stream-speed evaluation and candidate freshness.'
