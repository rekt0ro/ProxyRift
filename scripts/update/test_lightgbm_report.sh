#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

score_file="$tmp/lightgbm-scores.json"
summary_file="$tmp/summary.md"
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
    "unique_timestamps": 15,
    "train_rows": 80,
    "holdout_rows": 20,
    "baseline_log_mae": 0.12,
    "model_log_mae": 0.1,
    "relative_mae_improvement": 0.1666667,
    "top_20pct_speed_lift": 1.25,
    "minimum_relative_mae_improvement": 0.01,
    "minimum_top_20pct_speed_lift": 1.0
  }
}
JSON

cat > "$summary_file" <<'MD'
**Ranking:** Model-based ranking active — Stream-speed regression.
**Training:** 100 speed samples (72-hour retention) · 61 features · Candidates scored: 1.
**Evaluation:** Chronological 80/20 holdout on first observations of unseen candidates · Target: log stream throughput (kbps) · Baseline: Training median log stream throughput.
| Holdout log-MAE (model / baseline) | 0.10000 / 0.12000 |
| Relative log-MAE improvement | 16.7% |
| Top-20% measured speed lift | 1.25x |
| Train / unseen holdout rows | 80 / 20 |
| Unique candidates / timestamps | 100 / 15 |
**Model decision:** Accepted — promoted after temporal speed validation.
MD

bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$summary_file" "$score_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$artifact_file"
jq -e '.available == true and .trained == true and .model_report.accepted == true and .scored_candidates == 1 and (has("scores") | not) and .model_report.top_20pct_speed_lift == 1.25' "$artifact_file" >/dev/null
if grep -Fq 'private-candidate' "$artifact_file" || grep -Eq 'vless://|example.invalid' "$artifact_file"; then
  printf '[ERROR] Artifact contains a raw candidate string.\n' >&2
  exit 1
fi

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
' "$score_file" > "$tmp/history-fallback.json"
cat > "$tmp/history-fallback-summary.md" <<'MD'
**Ranking:** Historical speed-prior ranking active — Historical stream-speed priors.
**Training:** 100 speed samples (72-hour retention) · 61 features · Candidates scored: 1.
**Evaluation:** Chronological 80/20 holdout on first observations of unseen candidates · Target: log stream throughput (kbps) · Baseline: Training median log stream throughput.
| Holdout log-MAE (model / baseline) | n/a / n/a |
| Relative log-MAE improvement | n/a |
| Top-20% measured speed lift | n/a |
| Train / unseen holdout rows | 0 / 0 |
| Unique candidates / timestamps | 100 / 15 |
**Model decision:** Not promoted — insufficient speed samples or temporal coverage.
MD
bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/history-fallback-summary.md" "$tmp/history-fallback.json"

sed 's/| Holdout log-MAE (model \/ baseline) | 0.10000 \/ 0.12000 |/| Holdout log-MAE (model \/ baseline) | 9.99000 \/ 0.12000 |/' "$summary_file" > "$tmp/mismatch.md"
if bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/mismatch.md" "$score_file" >/dev/null 2>&1; then
  printf '[ERROR] Summary validator accepted a mismatched speed metric.\n' >&2
  exit 1
fi

bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$tmp/missing.json" "$tmp/unavailable.json"
jq -e '.available == false and (has("scores") | not)' "$tmp/unavailable.json" >/dev/null
printf '[OK] Stream-speed report verification and artifact sanitization tests passed.\n'
