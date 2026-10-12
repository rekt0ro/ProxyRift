#!/usr/bin/env bash
set -euo pipefail

score_file="${1:-${PROXYRIFT_TMP_DIR:-/tmp/proxyrift}/lightgbm-scores.json}"
output_file="${2:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/lightgbm-evaluation.json}"
mkdir -p "$(dirname "$output_file")"

if [[ ! -s "$score_file" ]]; then
  jq -n '{schema_version: 2, available: false, reason: "LightGBM speed-regression report was not produced"}' > "$output_file"
  printf '[INFO] LightGBM report unavailable; wrote an availability marker.\n'
  exit 0
fi

if ! jq -e 'type == "object" and .version == 5 and (.model_report | type == "object") and .feature_count == 61 and .model_report.target == "log(stream_throughput_kbps)"' "$score_file" >/dev/null; then
  printf '[ERROR] LightGBM score report has an invalid stream-speed regression schema.\n' >&2
  exit 1
fi

tmp_file="$(mktemp "${output_file}.tmp.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT
jq -S '
  {
    schema_version: 2,
    available: true,
    model_version: .version,
    trained: (.trained // false),
    training_rows: (.training_rows // null),
    feature_count: .feature_count,
    scored_candidates: ((.scores // {}) | length),
    model_report: {
      model: (.model_report.model // null),
      validation: (.model_report.validation // null),
      baseline: (.model_report.baseline // null),
      ranking_strategy: (.model_report.ranking_strategy // null),
      target: (.model_report.target // null),
      target_unit: (.model_report.target_unit // null),
      accepted: (.model_report.accepted // false),
      reason: (.model_report.reason // null),
      cutoff_timestamp: (.model_report.cutoff_timestamp // null),
      speed_samples: (.model_report.speed_samples // null),
      unique_candidates: (.model_report.unique_candidates // null),
      unique_timestamps: (.model_report.unique_timestamps // null),
      train_rows: (.model_report.train_rows // null),
      holdout_rows: (.model_report.holdout_rows // null),
      baseline_log_mae: (.model_report.baseline_log_mae // null),
      model_log_mae: (.model_report.model_log_mae // null),
      relative_mae_improvement: (.model_report.relative_mae_improvement // null),
      top_20pct_speed_lift: (.model_report.top_20pct_speed_lift // null),
      minimum_relative_mae_improvement: (.model_report.minimum_relative_mae_improvement // null),
      minimum_top_20pct_speed_lift: (.model_report.minimum_top_20pct_speed_lift // null)
    }
  }
' "$score_file" > "$tmp_file"
mv "$tmp_file" "$output_file"
trap - EXIT

if jq -e 'has("scores") or any(.model_report | paths(scalars) as $p | ($p[-1] | tostring | test("candidate|config|uri|url"; "i")))' "$output_file" >/dev/null; then
  printf '[ERROR] Sanitized LightGBM artifact unexpectedly contains raw score or candidate fields.\n' >&2
  exit 1
fi

printf '[OK] Wrote sanitized LightGBM speed-regression report to %s\n' "$output_file"
