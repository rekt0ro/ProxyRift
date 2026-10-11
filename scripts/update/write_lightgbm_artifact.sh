#!/usr/bin/env bash
set -euo pipefail

score_file="${1:-${PROXYRIFT_TMP_DIR:-/tmp/proxyrift}/lightgbm-scores.json}"
output_file="${2:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/lightgbm-evaluation.json}"
mkdir -p "$(dirname "$output_file")"

if [[ ! -s "$score_file" ]]; then
  jq -n '{schema_version: 1, available: false, reason: "LightGBM score report was not produced"}' > "$output_file"
  printf '[INFO] LightGBM report unavailable; wrote an availability marker.\n'
  exit 0
fi

if ! jq -e 'type == "object" and (.model_report | type == "object")' "$score_file" >/dev/null; then
  printf '[ERROR] LightGBM score report has an invalid structure.\n' >&2
  exit 1
fi

tmp_file="$(mktemp "${output_file}.tmp.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT
jq -S '
  def recency_summary:
    if type == "object" then {
      half_life_days: (.half_life_days // null),
      minimum_sample_weight: (.minimum_sample_weight // null),
      unweighted_brier: (.unweighted_brier // null),
      weighted_brier: (.weighted_brier // null),
      unweighted_accepted: (.unweighted_accepted // null),
      weighted_accepted: (.weighted_accepted // null),
      unweighted_top_20pct_pass_rate: (.unweighted_top_20pct_pass_rate // null),
      weighted_top_20pct_pass_rate: (.weighted_top_20pct_pass_rate // null),
      selected_strategy: (.selected_strategy // null)
    } else null end;
  {
    schema_version: 1,
    available: true,
    version: (.version // null),
    trained: (.trained // false),
    training_rows: (.training_rows // null),
    feature_count: (.model_report.model_feature_count // .feature_count // null),
    scored_candidates: ((.scores // {}) | length),
    model_report: {
      validation: (.model_report.validation // null),
      baseline: (.model_report.baseline // null),
      ranking_strategy: (.model_report.ranking_strategy // null),
      composite_weights: (.model_report.composite_weights // null),
      promotion_minimum_relative_brier_improvement: (.model_report.promotion_minimum_relative_brier_improvement // null),
      promotion_top_20pct_lift_minimum: (.model_report.promotion_top_20pct_lift_minimum // null),
      history_features: (.model_report.history_features // null),
      model_feature_count: (.model_report.model_feature_count // .feature_count // null),
      targets: (
        (.model_report.targets // {}) | with_entries(
          .value |= {
            accepted: (.accepted // null),
            reason: (.reason // null),
            labelled_rows: (.labelled_rows // null),
            pass_rows: (.pass_rows // null),
            fail_rows: (.fail_rows // null),
            unique_candidates: (.unique_candidates // null),
            distinct_timestamps: (.distinct_timestamps // null),
            evaluation_strategy: (.evaluation_strategy // null),
            train_rows: (.train_rows // null),
            train_pass_rows: (.train_pass_rows // null),
            train_fail_rows: (.train_fail_rows // null),
            holdout_rows: (.holdout_rows // null),
            holdout_pass_rows: (.holdout_pass_rows // null),
            holdout_fail_rows: (.holdout_fail_rows // null),
            holdout_unique_candidates: (.holdout_unique_candidates // null),
            holdout_eligible_unseen_rows: (.holdout_eligible_unseen_rows // null),
            model_brier: (.model_brier // null),
            baseline_brier: (.baseline_brier // null),
            model_top_20pct_pass_rate: (.model_top_20pct_pass_rate // null),
            holdout_pass_rate: (.holdout_pass_rate // null),
            top_20pct_lift: (.top_20pct_lift // null),
            recency_weighting_experiment: (.recency_weighting_experiment | recency_summary)
          }
        )
      )
    }
  }
' "$score_file" > "$tmp_file"
mv "$tmp_file" "$output_file"
trap - EXIT

if jq -e 'has("scores") or any(.model_report.targets[]?; has("predictions") or has("scores"))' "$output_file" >/dev/null; then
  printf '[ERROR] Sanitized LightGBM artifact unexpectedly contains candidate scores.\n' >&2
  exit 1
fi

printf '[OK] Wrote sanitized LightGBM evaluation report to %s\n' "$output_file"
