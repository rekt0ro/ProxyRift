#!/usr/bin/env bash
set -euo pipefail

summary_file="${1:-${GITHUB_STEP_SUMMARY:-}}"
score_file="${2:-${PROXYRIFT_TMP_DIR:-/tmp/proxyrift}/lightgbm-scores.json}"
if [[ -z "$summary_file" || ! -s "$summary_file" ]]; then
  printf '[ERROR] LightGBM summary is missing or empty.\n' >&2
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
  local filter="$1" value
  value="$(jq -r "$filter | if . == null then \"n/a\" else tostring end" "$score_file" 2>/dev/null)" || value="n/a"
  printf '%s' "${value:-n/a}"
}
format_count() {
  local value="$1"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then printf '%s' "$value"; return; fi
  local grouped=""
  while (( ${#value} > 3 )); do
    grouped=",${value: -3}$grouped"
    value="${value:0:${#value}-3}"
  done
  printf '%s%s' "$value" "$grouped"
}
format_decimal() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { printf "%.5f", value }'; else printf '%s' "$value"; fi
}
format_percentage() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { printf "%.1f%%", value * 100 }'; else printf '%s' "$value"; fi
}
format_multiplier() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { printf "%.2fx", value }'; else printf '%s' "$value"; fi
}
format_strategy() {
  case "$1" in
    stream_speed_regression) printf 'Stream-speed regression' ;;
    historical_stream_speed_prior_fallback) printf 'Historical stream-speed priors' ;;
    neutral_default) printf 'Neutral fallback' ;;
    *) printf 'Not evaluated' ;;
  esac
}
format_validation() {
  case "$1" in
    chronological_80_20_first_observation_of_unseen_candidates) printf 'Chronological 80/20 holdout on first observations of unseen candidates' ;;
    *) local readable="${1//_/ }"; printf '%s' "${readable^}" ;;
  esac
}
format_baseline() {
  case "$1" in
    training_median_log_stream_throughput) printf 'Training median log stream throughput' ;;
    *) local readable="${1//_/ }"; printf '%s' "${readable^}" ;;
  esac
}
format_decision() {
  local accepted="$1" reason="$2"
  if [[ "$accepted" == "true" ]]; then
    printf 'Accepted — promoted after temporal speed validation'; return
  fi
  case "$reason" in
    insufficient_speed_samples_or_temporal_coverage) printf 'Not promoted — insufficient speed samples or temporal coverage' ;;
    insufficient_distinct_timestamps) printf 'Not promoted — insufficient distinct timestamps' ;;
    insufficient_unseen_candidate_holdout) printf 'Not promoted — insufficient unseen-candidate holdout' ;;
    model_did_not_beat_temporal_speed_baseline) printf 'Not promoted — did not beat temporal baseline' ;;
    top_20pct_speed_ranking_did_not_improve) printf 'Not promoted — top-20%% ranking did not improve speed' ;;
    final_model_fit_failed) printf 'Not promoted — final regression fit failed' ;;
    *) local readable="${reason//_/ }"; printf 'Not promoted — ${readable:-not evaluated}' ;;
  esac
}

trained="$(json_value '.trained')"
strategy_raw="$(json_value '.model_report.ranking_strategy')"
case "$trained:$strategy_raw" in
  true:*) state='Model-based ranking active' ;;
  false:historical_stream_speed_prior_fallback) state='Historical speed-prior ranking active' ;;
  false:*) state='Neutral fallback active' ;;
  *) state='Not evaluated' ;;
esac
strategy="$(format_strategy "$strategy_raw")"
training_rows="$(format_count "$(json_value '.training_rows')")"
features="$(format_count "$(json_value '.feature_count')")"
scored="$(format_count "$(json_value '(.scores // {}) | length')")"
validation="$(format_validation "$(json_value '.model_report.validation')")"
baseline="$(format_baseline "$(json_value '.model_report.baseline')")"
accepted="$(json_value '.model_report.accepted')"
reason="$(json_value '.model_report.reason')"

expected_ranking="**Ranking:** $state — $strategy."
expected_training="**Training:** $training_rows speed samples (72-hour retention) · $features features · Candidates scored: $scored."
expected_evaluation="**Evaluation:** $validation · Target: log stream throughput (kbps) · Baseline: $baseline."
expected_decision="**Model decision:** $(format_decision "$accepted" "$reason")."
for expected in "$expected_ranking" "$expected_training" "$expected_evaluation" "$expected_decision"; do
  if ! grep -Fqx -- "$expected" "$summary_file"; then
    printf '[ERROR] LightGBM summary line does not match the run report. Expected: %s\n' "$expected" >&2
    exit 1
  fi
done

expected_rows=(
  "| Holdout log-MAE (model / baseline) | $(format_decimal "$(json_value '.model_report.model_log_mae')") / $(format_decimal "$(json_value '.model_report.baseline_log_mae')") |"
  "| Relative log-MAE improvement | $(format_percentage "$(json_value '.model_report.relative_mae_improvement')") |"
  "| Top-20% measured speed lift | $(format_multiplier "$(json_value '.model_report.top_20pct_speed_lift')") |"
  "| Train / unseen holdout rows | $(format_count "$(json_value '.model_report.train_rows')") / $(format_count "$(json_value '.model_report.holdout_rows')") |"
  "| Unique candidates / timestamps | $(format_count "$(json_value '.model_report.unique_candidates')") / $(format_count "$(json_value '.model_report.unique_timestamps')") |"
)
for expected in "${expected_rows[@]}"; do
  if ! grep -Fqx -- "$expected" "$summary_file"; then
    printf '[ERROR] LightGBM metric row does not match the score report. Expected: %s\n' "$expected" >&2
    exit 1
  fi
done
printf '[OK] LightGBM summary matches the versioned stream-speed regression report.\n'
