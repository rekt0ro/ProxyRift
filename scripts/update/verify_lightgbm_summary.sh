#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 3 ]]; then
  printf '[ERROR] Usage: %s SUMMARY_FILE SCORE_FILE TRAINING_STATS_FILE\n' "$0" >&2; exit 2
fi
summary_file="$1"; score_file="$2"; stats_file="$3"
if [[ ! -s "$summary_file" || ! -s "$score_file" ]]; then
  printf '[ERROR] Summary or LightGBM score report is missing.\n' >&2; exit 1
fi
if ! jq -e 'type == "object" and .version == 5 and (.model_report | type == "object") and .model_report.target == "log(stream_throughput_kbps)" and .feature_count == 61' "$score_file" >/dev/null; then
  printf '[ERROR] LightGBM score report does not use the validated stream-speed schema.\n' >&2; exit 1
fi
json_value() {
  local file="$1" filter="$2" value
  if [[ ! -s "$file" ]]; then printf 'n/a'; return; fi
  value="$(jq -r "$filter | if . == null then \"n/a\" else tostring end" "$file" 2>/dev/null)" || value='n/a'
  printf '%s' "${value:-n/a}"
}
format_score_coverage() {
  local scored="$1" total="$2"
  if [[ ! "$scored" =~ ^[0-9]+$ || ! "$total" =~ ^[0-9]+$ ]] || (( total == 0 )); then
    printf 'Unavailable — candidate score coverage not recorded'
    return
  fi
  local details
  details="$(awk -v scored="$scored" -v total="$total" 'BEGIN {
    pct = 100 * scored / total
    missing = total - scored
    if (missing == 0) printf "%.1f%%; complete", pct
    else if (missing > 0) printf "%.1f%%; %d missing", pct, missing
    else printf "%.1f%%; %d over-reported", pct, -missing
  }')"
  printf '%s / %s candidates scored (%s)' "$(format_count "$scored")" "$(format_count "$total")" "$details"
}
format_count() {
  local value="$1"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then printf '%s' "$value"; return; fi
  awk -v value="$value" 'BEGIN {
    value = sprintf("%.0f", value); groups = ""
    while (length(value) > 3) { groups = "," substr(value, length(value) - 2) groups; value = substr(value, 1, length(value) - 3) }
    printf "%s%s", value, groups
  }'
}
format_decimal() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { printf "%.3f", value }'
  else printf '%s' "$value"; fi
}
format_mae_change() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { pct = value * 100; if (pct >= 0) printf "%.1f%% lower", pct; else printf "%.1f%% higher", -pct }'
  else printf '%s' "$value"; fi
}
format_multiplier() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { printf "%.2fx", value }'
  else printf '%s' "$value"; fi
}
format_decision() {
  local accepted="$1" reason="$2" minimum_lift="$3"
  if [[ "$accepted" == "true" ]]; then printf 'Active'; return; fi
  case "$reason" in
    insufficient_speed_samples_or_temporal_coverage) printf 'Not enabled — collecting more recent speed measurements' ;;
    insufficient_distinct_timestamps) printf 'Not enabled — not enough separate test runs' ;;
    insufficient_unseen_candidate_holdout) printf 'Not enabled — not enough unseen configs to validate' ;;
    model_did_not_beat_temporal_speed_baseline) printf 'Not enabled — MAE did not improve on the temporal baseline by at least 1%%' ;;
    top_20pct_speed_lift_below_minimum) printf 'Not enabled — top-20%% holdout speed lift did not reach the required %s minimum' "$(format_multiplier "$minimum_lift")" ;;
    top_20pct_speed_ranking_did_not_improve) printf 'Not enabled — top-20%% speed lift did not exceed the baseline' ;;
    final_model_fit_failed|holdout_model_fit_failed) printf 'Not enabled — model training failed' ;;
    *) local readable="${reason//_/ }"; printf 'Not enabled — %s' "${readable:-not evaluated}" ;;
  esac
}
format_evaluation() {
  local reason="$1" model_mae="$2" baseline_mae="$3" improvement="$4"
  local speed_lift="$5" speed_samples="$6" unique_candidates="$7"
  local unique_timestamps="$8" train_rows="$9" holdout_rows="${10}"
  local minimum_lift="${11}"
  if [[ "$model_mae" =~ ^-?[0-9]+([.][0-9]+)?$ \
    && "$baseline_mae" =~ ^-?[0-9]+([.][0-9]+)?$ \
    && "$improvement" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    printf 'Evaluated — model MAE %s vs baseline %s (%s); top-20%% holdout speed lift %s (minimum %s).' \
      "$(format_decimal "$model_mae")" "$(format_decimal "$baseline_mae")" \
      "$(format_mae_change "$improvement")" "$(format_multiplier "$speed_lift")" \
      "$(format_multiplier "$minimum_lift")"
    return
  fi
  case "$reason" in
    insufficient_speed_samples_or_temporal_coverage)
      printf 'Not evaluated — insufficient speed samples or temporal coverage (samples: %s; candidates: %s; timestamps: %s).' \
        "$(format_count "$speed_samples")" "$(format_count "$unique_candidates")" "$(format_count "$unique_timestamps")" ;;
    insufficient_distinct_timestamps)
      printf 'Not evaluated — fewer than two distinct measurement timestamps (found %s).' "$(format_count "$unique_timestamps")" ;;
    insufficient_unseen_candidate_holdout)
      printf 'Not evaluated — insufficient unseen-candidate holdout (train rows: %s; holdout rows: %s).' \
        "$(format_count "$train_rows")" "$(format_count "$holdout_rows")" ;;
    model_did_not_beat_temporal_speed_baseline)
      printf 'Metrics unavailable, but the artifact reports that the model did not beat the temporal baseline.' ;;
    top_20pct_speed_lift_below_minimum)
      printf 'Metrics unavailable, but the artifact reports that the top-20%% holdout speed lift did not reach the required minimum.' ;;
    top_20pct_speed_ranking_did_not_improve)
      printf 'Metrics unavailable, but the artifact reports that the top-20%% speed ranking did not improve.' ;;
    final_model_fit_failed|holdout_model_fit_failed) printf 'Not evaluated — model fitting failed.' ;;
    *) local readable="${reason//_/ }"; printf 'Not evaluated — %s.' "${readable:-no evaluation report}" ;;
  esac
}
trained="$(jq -r '.trained // false' "$score_file")"
strategy_raw="$(jq -r '.model_report.ranking_strategy // "neutral_default"' "$score_file")"
if [[ "$trained" == "true" ]]; then mode='Speed model'
elif [[ "$strategy_raw" == "historical_stream_speed_prior_fallback" ]]; then mode='Recent speed estimates'
else mode='Neutral ranking'; fi
speed_samples="$(format_count "$(json_value "$score_file" '.model_report.speed_samples')")"
unique_candidates="$(format_count "$(json_value "$score_file" '.model_report.unique_candidates')")"
unique_timestamps="$(format_count "$(json_value "$score_file" '.model_report.unique_timestamps')")"
train_rows="$(json_value "$score_file" '.model_report.train_rows')"
holdout_rows="$(json_value "$score_file" '.model_report.holdout_rows')"
model_mae="$(json_value "$score_file" '.model_report.model_log_mae')"
baseline_mae="$(json_value "$score_file" '.model_report.baseline_log_mae')"
improvement="$(json_value "$score_file" '.model_report.relative_mae_improvement')"
speed_lift="$(json_value "$score_file" '.model_report.top_20pct_speed_lift')"
new_samples="$(format_count "$(json_value "$stats_file" '.dataset.new_speed_samples')")"
candidate_count="$(json_value "$score_file" '.model_report.candidate_count')"
scored_candidate_count="$(json_value "$score_file" '.model_report.scored_candidate_count')"
minimum_lift="$(json_value "$score_file" '.model_report.minimum_top_20pct_speed_lift')"
accepted="$(jq -r '.model_report.accepted // false' "$score_file")"
reason="$(jq -r '.model_report.reason // "not_evaluated"' "$score_file")"
expected_mode="| Ranking mode | $mode |"
expected_data="| Recent speed data | $speed_samples speed measurements in the last 72 hours; $new_samples new this run; $unique_candidates candidate identities across $unique_timestamps timestamps |"
expected_coverage="| Candidate score coverage | $(format_score_coverage "$scored_candidate_count" "$candidate_count") |"
if [[ "$train_rows" =~ ^[0-9]+$ && "$holdout_rows" =~ ^[0-9]+$ ]] && (( train_rows > 0 || holdout_rows > 0 )); then
  expected_split="| Temporal validation split | $(format_count "$train_rows") training rows / $(format_count "$holdout_rows") holdout rows |"
else
  expected_split='| Temporal validation split | Not available — validation data requirements were not met |'
fi
expected_evaluation="| Evaluation | $(format_evaluation "$reason" "$model_mae" "$baseline_mae" "$improvement" "$speed_lift" "$(json_value "$score_file" '.model_report.speed_samples')" "$(json_value "$score_file" '.model_report.unique_candidates')" "$(json_value "$score_file" '.model_report.unique_timestamps')" "$train_rows" "$holdout_rows" "$minimum_lift") |"
expected_decision="| Decision | $(format_decision "$accepted" "$reason" "$minimum_lift") |"
for expected in "$expected_mode" "$expected_coverage" "$expected_data" "$expected_split" "$expected_evaluation" "$expected_decision"; do
  if ! grep -Fqx -- "$expected" "$summary_file"; then
    printf '[ERROR] LightGBM summary row does not match the score report. Expected: %s\n' "$expected" >&2; exit 1
  fi
done
printf '[OK] LightGBM summary matches the score report and artifact evaluation reason.\n'
