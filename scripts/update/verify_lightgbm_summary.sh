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
if ! jq -e 'type == "object" and (.model_report | type == "object")' "$score_file" >/dev/null; then
  printf '[ERROR] LightGBM score report has an invalid structure.\n' >&2
  exit 1
fi
json_value() {
  local filter="$1" value
  value="$(jq -r "$filter | if . == null then \"\" else tostring end" "$score_file" 2>/dev/null)" || value=""
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
format_compact_number() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then awk -v value="$value" 'BEGIN { if (value == int(value)) printf "%d", value; else printf "%g", value }'; else printf '%s' "$value"; fi
}
format_ranking_strategy() {
  case "$1" in
    end_to_end) printf 'End-to-end model' ;;
    stage_blend_fallback) printf 'Stage-model blend' ;;
    neutral_default) printf 'Neutral fallback' ;;
    *) printf 'Not evaluated' ;;
  esac
}
format_validation_scheme() {
  case "$1" in
    walk_forward_time_split) printf 'Walk-forward temporal split' ;;
    *) local readable="${1//_/ }"; printf '%s' "${readable^}" ;;
  esac
}
format_baseline() {
  case "$1" in
    training_prevalence_brier) printf 'Training-prevalence Brier score' ;;
    *) local readable="${1//_/ }"; printf '%s' "${readable^}" ;;
  esac
}
format_strategy() {
  case "$1" in
    recency_weighted) printf 'Recency-weighted' ;;
    unweighted) printf 'Unweighted' ;;
    none) printf 'Neither variant passed promotion gates' ;;
    *) printf 'Not evaluated' ;;
  esac
}
format_status() {
  local target="$1" accepted reason
  accepted="$(json_value ".model_report.targets.${target}.accepted")"
  reason="$(json_value ".model_report.targets.${target}.reason")"
  [[ "$reason" == "n/a" ]] && reason="not_evaluated"
  if [[ "$accepted" == "true" ]]; then
    if [[ "$reason" == "promoted_after_temporal_validation" ]]; then printf 'Promoted after temporal validation'; else printf 'Accepted: %s' "${reason//_/ }"; fi
    return
  fi
  if [[ "$accepted" != "false" ]]; then printf 'Not evaluated'; return; fi
  case "$reason" in
    model_did_not_beat_unseen_candidate_baseline) printf 'Did not beat unseen-candidate baseline' ;;
    top_20pct_below_unseen_candidate_baseline) printf 'Top-20%% pass rate below unseen-candidate baseline' ;;
    neither_weighting_strategy_passed_temporal_holdout) printf 'Neither weighting strategy passed temporal holdout' ;;
    "insufficient labelled data"*) printf 'Not promoted: insufficient labelled data' ;;
    "insufficient distinct observation timestamps"*) printf 'Not promoted: insufficient distinct timestamps' ;;
    "unseen-candidate temporal holdout lacks representative classes"*) printf 'Not promoted: unseen-candidate holdout lacks class balance' ;;
    not_evaluated|n/a) printf 'Not evaluated' ;;
    *) local readable="${reason//_/ }"; printf '%s' "${readable^}" ;;
  esac
}
trained="$(json_value '.trained')"
case "$trained" in
  true) model_state='Model-based ranking active' ;;
  false) model_state='Neutral fallback active' ;;
  *) model_state='Not evaluated' ;;
esac
ranking_strategy="$(format_ranking_strategy "$(json_value '.model_report.ranking_strategy')")"
training_rows="$(format_count "$(json_value '.training_rows')")"
features="$(json_value '.model_report.model_feature_count')"
if [[ "$features" == "n/a" ]]; then features="$(json_value '.feature_count')"; fi
features="$(format_count "$features")"
scored="$(format_count "$(json_value '(.scores // {}) | length')")"
promoted="$(format_count "$(json_value '[.model_report.targets[]? | select(.accepted == true)] | length')")"
expected_ranking="**Ranking:** $model_state — $ranking_strategy ($promoted of 4 targets accepted)."
expected_training="**Training:** $training_rows rows · $features features · Candidates scored: $scored."
expected_evaluation="**Evaluation:** $(format_validation_scheme "$(json_value '.model_report.validation')") · Baseline: $(format_baseline "$(json_value '.model_report.baseline')")."
for expected in "$expected_ranking" "$expected_training" "$expected_evaluation"; do
  if ! grep -Fqx -- "$expected" "$summary_file"; then
    printf '[ERROR] LightGBM summary line does not match the run report. Expected: %s\n' "$expected" >&2
    exit 1
  fi
done
for target in strict transfer stream end_to_end; do
  case "$target" in
    strict) label='Strict validation' ;;
    transfer) label='Transfer (10 MiB)' ;;
    stream) label='Stream stability' ;;
    end_to_end) label='End-to-end' ;;
  esac
  expected_row="| $label | $(format_status "$target") | $(format_decimal "$(json_value ".model_report.targets.${target}.model_brier")") / $(format_decimal "$(json_value ".model_report.targets.${target}.baseline_brier")") | $(format_percentage "$(json_value ".model_report.targets.${target}.model_top_20pct_pass_rate")") / $(format_percentage "$(json_value ".model_report.targets.${target}.holdout_pass_rate")") |"
  if ! grep -Fqx -- "$expected_row" "$summary_file"; then
    printf '[ERROR] LightGBM evaluation row for %s does not match the run report. Expected: %s\n' "$target" "$expected_row" >&2
    exit 1
  fi
done
strategy="$(format_strategy "$(json_value '.model_report.targets.strict.recency_weighting_experiment.selected_strategy')")"
if [[ "$strategy" != "Not evaluated" ]]; then
  unweighted_brier="$(format_decimal "$(json_value '.model_report.targets.strict.recency_weighting_experiment.unweighted_brier')")"
  weighted_brier="$(format_decimal "$(json_value '.model_report.targets.strict.recency_weighting_experiment.weighted_brier')")"
  unweighted_top20="$(format_percentage "$(json_value '.model_report.targets.strict.recency_weighting_experiment.unweighted_top_20pct_pass_rate')")"
  weighted_top20="$(format_percentage "$(json_value '.model_report.targets.strict.recency_weighting_experiment.weighted_top_20pct_pass_rate')")"
  half_life="$(format_compact_number "$(json_value '.model_report.targets.strict.recency_weighting_experiment.half_life_days')")"
  min_weight="$(json_value '.model_report.targets.strict.recency_weighting_experiment.minimum_sample_weight')"
  expected_recency="**Recency weighting:** Strict validation — $strategy · Brier $unweighted_brier → $weighted_brier · Top-20% pass rate $unweighted_top20 → $weighted_top20 · $half_life-day half-life, minimum weight $min_weight."
  if ! grep -Fqx -- "$expected_recency" "$summary_file"; then
    printf '[ERROR] LightGBM recency summary does not match the run report. Expected: %s\n' "$expected_recency" >&2
    exit 1
  fi
fi
printf '[OK] LightGBM Summary ranking and evaluation metrics match the score report from this run.\n'
