#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

score_file="$tmp/lightgbm-scores.json"
summary_file="$tmp/summary.md"
stats_file="$tmp/light-training-stats.json"
artifact_file="$tmp/lightgbm-evaluation.json"

cat > "$score_file" <<'JSON'
{
  "version": 5,
  "trained": true,
  "training_rows": 100,
  "feature_count": 61,
  "scores": {"vless://private-candidate@example.invalid:443": 0.8},
  "model_report": {
    "model": "LightGBM GBDT regression",
    "validation": "chronological_80_20_first_observation_of_unseen_candidates",
    "baseline": "training_median_log_stream_throughput",
    "ranking_strategy": "stream_speed_regression",
    "target": "log(stream_throughput_kbps)",
    "target_unit": "kbps",
    "accepted": true,
    "reason": "promoted_after_temporal_speed_validation",
    "cutoff_timestamp": 1800000000,
    "speed_samples": 100,
    "unique_candidates": 100,
    "candidate_count": 1,
    "scored_candidate_count": 1,
    "unique_timestamps": 15,
    "train_rows": 80,
    "holdout_rows": 20,
    "baseline_log_mae": 0.12,
    "model_log_mae": 0.1,
    "relative_mae_improvement": 0.1666667,
    "top_20pct_speed_lift": 1.25,
    "minimum_relative_mae_improvement": 0.01,
    "minimum_top_20pct_speed_lift": 1.05
  }
}
JSON

cat > "$stats_file" <<'JSON'
{"dataset":{"new_speed_samples":12}}
JSON

cat > "$summary_file" <<'MD'
| Ranking mode | Speed model |
| Candidate score coverage | 1 / 1 candidates scored (100.0%; complete) |
| Recent speed data | 100 speed measurements in the last 72 hours; 12 new this run; 100 candidate identities across 15 timestamps |
| Temporal validation split | 80 training rows / 20 holdout rows |
| Evaluation | Evaluated — model MAE 0.100 vs baseline 0.120 (16.7% lower); top-20% holdout speed lift 1.25x (minimum 1.05x). |
| Decision | Active |
MD

bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$summary_file" "$score_file" "$stats_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$artifact_file"
jq -e '.available == true and .trained == true and .model_report.accepted == true and .scored_candidates == 1 and (has("scores") | not) and .model_report.candidate_count == 1 and .model_report.scored_candidate_count == 1 and .model_report.minimum_top_20pct_speed_lift == 1.05 and .model_report.top_20pct_speed_lift == 1.25' "$artifact_file" >/dev/null
if grep -Fq 'private-candidate' "$artifact_file" || grep -Eq 'vless://|example.invalid' "$artifact_file"; then
  printf '[ERROR] Artifact contains a raw candidate string.\n' >&2
  exit 1
fi

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
' "$score_file" > "$tmp/history-fallback.json"
cat > "$tmp/history-fallback-summary.md" <<'MD'
| Ranking mode | Recent speed estimates |
| Candidate score coverage | 1 / 1 candidates scored (100.0%; complete) |
| Recent speed data | 20 speed measurements in the last 72 hours; 12 new this run; 12 candidate identities across 1 timestamps |
| Temporal validation split | Not available — validation data requirements were not met |
| Evaluation | Not evaluated — insufficient speed samples or temporal coverage (samples: 20; candidates: 12; timestamps: 1). |
| Decision | Not enabled — collecting more recent speed measurements |
MD
bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/history-fallback-summary.md" "$tmp/history-fallback.json" "$stats_file"

sed 's/model MAE 0.100 vs baseline 0.120/model MAE 9.990 vs baseline 0.120/' "$summary_file" > "$tmp/mismatch.md"
if bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/mismatch.md" "$score_file" "$stats_file" >/dev/null 2>&1; then
  printf '[ERROR] Summary validator accepted a mismatched model summary.\n' >&2
  exit 1
fi

jq '
  .model_report.accepted = false
  | .model_report.reason = "model_did_not_beat_temporal_speed_baseline"
  | .model_report.model_log_mae = 0.125
  | .model_report.baseline_log_mae = 0.12
  | .model_report.relative_mae_improvement = -0.0416667
  | .model_report.top_20pct_speed_lift = 1.08
' "$score_file" > "$tmp/rejected-score.json"
cp "$tmp/rejected-score.json" "$score_file"
cat > "$tmp/rejected-summary.md" <<'MD'
| Ranking mode | Speed model |
| Candidate score coverage | 1 / 1 candidates scored (100.0%; complete) |
| Recent speed data | 100 speed measurements in the last 72 hours; 12 new this run; 100 candidate identities across 15 timestamps |
| Temporal validation split | 80 training rows / 20 holdout rows |
| Evaluation | Evaluated — model MAE 0.125 vs baseline 0.120 (4.2% higher); top-20% holdout speed lift 1.08x (minimum 1.05x). |
| Decision | Not enabled — MAE did not improve on the temporal baseline by at least 1% |
MD
bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/rejected-summary.md" "$score_file" "$stats_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$tmp/rejected-artifact.json"
jq -e '.available == true and .model_report.accepted == false and .model_report.reason == "model_did_not_beat_temporal_speed_baseline" and .model_report.relative_mae_improvement < 0' "$tmp/rejected-artifact.json" >/dev/null

jq '
  .trained = false
  | .model_report.ranking_strategy = "historical_stream_speed_prior_fallback"
  | .model_report.accepted = false
  | .model_report.reason = "top_20pct_speed_lift_below_minimum"
  | .model_report.model_log_mae = 0.08
  | .model_report.baseline_log_mae = 0.12
  | .model_report.relative_mae_improvement = 0.3333333
  | .model_report.top_20pct_speed_lift = 1.02
' "$score_file" > "$tmp/low-lift-score.json"
cp "$tmp/low-lift-score.json" "$score_file"
cat > "$tmp/low-lift-summary.md" <<'MD'
| Ranking mode | Recent speed estimates |
| Candidate score coverage | 1 / 1 candidates scored (100.0%; complete) |
| Recent speed data | 100 speed measurements in the last 72 hours; 12 new this run; 100 candidate identities across 15 timestamps |
| Temporal validation split | 80 training rows / 20 holdout rows |
| Evaluation | Evaluated — model MAE 0.080 vs baseline 0.120 (33.3% lower); top-20% holdout speed lift 1.02x (minimum 1.05x). |
| Decision | Not enabled — top-20% holdout speed lift did not reach the required 1.05x minimum |
MD
bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/low-lift-summary.md" "$score_file" "$stats_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$tmp/low-lift-artifact.json"
jq -e '.available == true and .model_report.accepted == false and .model_report.reason == "top_20pct_speed_lift_below_minimum" and .model_report.minimum_top_20pct_speed_lift == 1.05' "$tmp/low-lift-artifact.json" >/dev/null

bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$tmp/missing.json" "$tmp/unavailable.json"
jq -e '.available == false and (has("scores") | not)' "$tmp/unavailable.json" >/dev/null
printf '[OK] Stream-speed report verification and artifact sanitization tests passed.\n'
