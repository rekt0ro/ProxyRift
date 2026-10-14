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
  "input_candidates": 20000, "security_rejected": 0, "strict_verified": 300,
  "strict_attempts": 317, "selection_target": 200, "strict_selectable": 200,
  "stability_selectable": 200, "transfer_selectable": 200, "stream_selectable": 200,
  "selection_shortfall": 0, "transfer_tested": 232, "transfer_passed": 211,
  "stream_continuity_tested": 211, "stream_continuity_passed": 209, "published": 200,
  "historical_subscription_revisions": 8, "recent_family_keys": 400,
  "fresh_family_target_percent": 70, "score_independent_exploration_percent": 15,
  "published_from_fresh_families": 140, "published_from_recent_families": 60,
  "published_new_candidates": 100, "published_expired_candidates": 60, "published_revalidated": 40,
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
grep -Fq '| Candidates checked | 20,000 |' "$tmp/summary.md"
grep -Fq '| Strict validation | 300 / 317 |' "$tmp/summary.md"
grep -Fq '| 10 MiB transfer | 211 / 232 |' "$tmp/summary.md"
grep -Fq '| Sustained stream | 209 / 211 |' "$tmp/summary.md"
grep -Fq '| New candidates (not in retained history) | 100 |' "$tmp/summary.md"
grep -Fq '| Previously known; 24-hour revalidation expired | 60 |' "$tmp/summary.md"
grep -Fq '| Recently reused (validated within 24 hours) | 40 of 200 (maximum 20%) |' "$tmp/summary.md"
grep -Fq '| Configs from unseen families | 140 of 200 (70.0%; target 70%) |' "$tmp/summary.md"
grep -Fq '| Light published | 200 of 200 target |' "$tmp/summary.md"
grep -Fq '| Ranking mode | Speed model |' "$tmp/summary.md"
grep -Fq '| Recent speed data | 1,200 speed measurements in the last 72 hours; 120 new this run; 1,100 candidate identities across 15 timestamps |' "$tmp/summary.md"
grep -Fq '| Temporal validation split | 960 training rows / 240 holdout rows |' "$tmp/summary.md"
grep -Fq '| Evaluation | Evaluated — model MAE 0.100 vs baseline 0.130 (23.1% lower); top-20% holdout speed lift 1.24x. |' "$tmp/summary.md"
grep -Fq '| Decision | Active |' "$tmp/summary.md"
grep -Fq '| All | 1 |' "$tmp/summary.md"
grep -Fq '| Light | 1 |' "$tmp/summary.md"

jq '
  .trained = false
  | .model_report.ranking_strategy = "historical_stream_speed_prior_fallback"
  | .model_report.accepted = false
  | .model_report.reason = "insufficient_speed_samples_or_temporal_coverage"
  | .model_report.speed_samples = 20
  | .model_report.unique_candidates = 12
  | .model_report.unique_timestamps = 1
  | .model_report.baseline_log_mae = null
  | .model_report.model_log_mae = null
  | .model_report.relative_mae_improvement = null
  | .model_report.top_20pct_speed_lift = null
  | .model_report.train_rows = 0
  | .model_report.holdout_rows = 0
' "$work/proxyrift/lightgbm-scores.json" > "$tmp/fallback-scores.json"
cp "$tmp/fallback-scores.json" "$work/proxyrift/lightgbm-scores.json"
run_summary "$tmp/fallback-summary.md"
grep -Fq '| Ranking mode | Recent speed estimates |' "$tmp/fallback-summary.md"
grep -Fq '| Recent speed data | 20 speed measurements in the last 72 hours; 120 new this run; 12 candidate identities across 1 timestamps |' "$tmp/fallback-summary.md"
grep -Fq '| Evaluation | Not evaluated — insufficient speed samples or temporal coverage (samples: 20; candidates: 12; timestamps: 1). |' "$tmp/fallback-summary.md"
grep -Fq '| Decision | Not enabled — collecting more recent speed measurements |' "$tmp/fallback-summary.md"

jq '
  .trained = true
  | .model_report.ranking_strategy = "stream_speed_regression"
  | .model_report.accepted = false
  | .model_report.reason = "model_did_not_beat_temporal_speed_baseline"
  | .model_report.speed_samples = 100
  | .model_report.unique_candidates = 100
  | .model_report.unique_timestamps = 15
  | .model_report.train_rows = 80
  | .model_report.holdout_rows = 20
  | .model_report.baseline_log_mae = 0.12
  | .model_report.model_log_mae = 0.125
  | .model_report.relative_mae_improvement = -0.0416667
  | .model_report.top_20pct_speed_lift = 1.08
' "$work/proxyrift/lightgbm-scores.json" > "$tmp/rejected-scores.json"
cp "$tmp/rejected-scores.json" "$work/proxyrift/lightgbm-scores.json"
run_summary "$tmp/rejected-summary.md"
grep -Fq '| Evaluation | Evaluated — model MAE 0.125 vs baseline 0.120 (4.2% higher); top-20% holdout speed lift 1.08x. |' "$tmp/rejected-summary.md"
grep -Fq '| Decision | Not enabled — MAE did not improve on the temporal baseline by at least 1% |' "$tmp/rejected-summary.md"
if grep -Fq 'Not evaluated — insufficient speed samples' "$tmp/rejected-summary.md"; then
  printf '[ERROR] Rejected, measured model was incorrectly described as having insufficient data.\n' >&2
  exit 1
fi

if grep -Eq 'stream_speed_regression|training_prevalence_brier|Brier score|Strict model:|End-to-end model:|model_log_mae|selected_strategy' "$tmp/summary.md"; then
  printf '[ERROR] Summary still contains legacy pass-fail model output.\n' >&2
  exit 1
fi
echo '[OK] Update summary reports stream-speed evaluation and candidate freshness.'
