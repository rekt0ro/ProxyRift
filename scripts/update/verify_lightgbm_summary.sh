#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  printf '[ERROR] Usage: %s SUMMARY_FILE SCORE_FILE TRAINING_STATS_FILE\n' "$0" >&2
  exit 2
fi

summary_file="$1"
score_file="$2"
stats_file="$3"
if [[ ! -s "$summary_file" ]]; then
  printf '[ERROR] LightGBM summary is missing or empty: %s\n' "$summary_file" >&2
  exit 1
fi
if [[ ! -s "$score_file" ]]; then
  printf '[ERROR] LightGBM score report is missing or empty: %s\n' "$score_file" >&2
  exit 1
fi
if ! jq -e 'type == "object" and .version == 5 and (.model_report | type == "object") and .model_report.target == "log(stream_throughput_kbps)" and .feature_count == 61' "$score_file" >/dev/null; then
  printf '[ERROR] LightGBM score report does not use the validated stream-speed schema.\n' >&2
  exit 1
fi

json_value() {
  local file="$1"
  local filter="$2"
  local value
  if [[ ! -s "$file" ]]; then
    printf 'n/a'
    return
  fi
  value="$(jq -r "$filter | if . == null then \"n/a\" else tostring end" "$file" 2>/dev/null)" || value='n/a'
  printf '%s' "${value:-n/a}"
}
format_count() {
  local value="$1"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf '%s' "$value"
    return
  fi
  awk -v value="$value" 'BEGIN {
    value = sprintf("%.0f", value)
    groups = ""
    while (length(value) > 3) {
      groups = "," substr(value, length(value) - 2) groups
      value = substr(value, 1, length(value) - 3)
    }
    printf "%s%s", value, groups
  }'
}
format_decimal() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { printf "%.3f", value }'
  else
    printf '%s' "$value"
  fi
}
format_percentage() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { printf "%.1f%%", value * 100 }'
  else
    printf '%s' "$value"
  fi
}
format_multiplier() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { printf "%.2fx", value }'
  else
    printf '%s' "$value"
  fi
}
format_decision() {
  local accepted="$1"
  local reason="$2"
  if [[ "$accepted" == "true" ]]; then
    printf 'Active'
    return
  fi
  case "$reason" in
    insufficient_speed_samples_or_temporal_coverage) printf 'Not enabled — collecting more recent speed measurements' ;;
    insufficient_distinct_timestamps) printf 'Not enabled — not enough separate test runs' ;;
    insufficient_unseen_candidate_holdout) printf 'Not enabled — not enough unseen configs to validate' ;;
    model_did_not_beat_temporal_speed_baseline) printf 'Not enabled — predictions did not beat the baseline' ;;
    top_20pct_speed_ranking_did_not_improve) printf 'Not enabled — the highest-ranked configs were not faster' ;;
    final_model_fit_failed|holdout_model_fit_failed) printf 'Not enabled — model training failed' ;;
    *)
      local readable
      readable="$(printf '%s' "$reason" | tr '_' ' ')"
      printf 'Not enabled — %s' "${readable:-not evaluated}"
      ;;
  esac
}

trained="$(jq -r '.trained // false' "$score_file")"
strategy_raw="$(jq -r '.model_report.ranking_strategy // "neutral_default"' "$score_file")"
if [[ "$trained" == "true" ]]; then
  mode='Speed model'
elif [[ "$strategy_raw" == "historical_stream_speed_prior_fallback" ]]; then
  mode='Recent speed estimates'
else
  mode='Neutral ranking'
fi

training_rows="$(format_count "$(json_value "$score_file" '.training_rows')")"
new_samples="$(format_count "$(json_value "$stats_file" '.dataset.new_speed_samples')")"
accepted="$(jq -r '.model_report.accepted // false' "$score_file")"
reason="$(jq -r '.model_report.reason // "not_evaluated"' "$score_file")"

expected_mode="**Mode:** $mode."
expected_training="**Training:** $training_rows samples from the last 72 hours ($new_samples new this run)."
if [[ "$accepted" == "true" ]]; then
  model_mae="$(format_decimal "$(json_value "$score_file" '.model_report.model_log_mae')")"
  baseline_mae="$(format_decimal "$(json_value "$score_file" '.model_report.baseline_log_mae')")"
  improvement="$(format_percentage "$(json_value "$score_file" '.model_report.relative_mae_improvement')")"
  speed_lift="$(format_multiplier "$(json_value "$score_file" '.model_report.top_20pct_speed_lift')")"
  expected_evaluation="**Evaluation:** Prediction error (model / baseline) $model_mae / $baseline_mae ($improvement lower); top 20% averaged $speed_lift holdout speed."
else
  expected_evaluation='**Evaluation:** Not enough recent measurements to validate a speed model.'
fi
decision="$(format_decision "$accepted" "$reason")"
expected_decision="**Decision:** $decision."

for expected in "$expected_mode" "$expected_training" "$expected_evaluation" "$expected_decision"; do
  if ! grep -Fqx -- "$expected" "$summary_file"; then
    printf '[ERROR] LightGBM summary line does not match the run report. Expected: %s\n' "$expected" >&2
    exit 1
  fi
done

printf '[OK] LightGBM summary matches the stream-speed report and current run-summary format.\n'
