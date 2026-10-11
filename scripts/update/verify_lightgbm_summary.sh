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
  local filter="$1"
  local value
  value="$(jq -r "$filter | if . == null then \"\" else tostring end" "$score_file" 2>/dev/null)" || value=""
  printf '%s' "${value:-n/a}"
}

format_ranking_strategy() {
  case "$1" in
    end_to_end) printf 'End-to-end model' ;;
    stage_blend_fallback) printf 'Stage-model blend fallback' ;;
    neutral_default) printf 'Neutral 0.5-score fallback' ;;
    *) printf 'Not evaluated' ;;
  esac
}

trained="$(json_value '.trained')"
case "$trained" in
  true) model_state='Model-based ranking active' ;;
  false) model_state='Neutral fallback active' ;;
  *) model_state='Not evaluated' ;;
esac
ranking_strategy="$(format_ranking_strategy "$(json_value '.model_report.ranking_strategy')")"
training_rows="$(json_value '.training_rows')"
features="$(json_value '.model_report.model_feature_count')"
if [[ "$features" == 'n/a' ]]; then
  features="$(json_value '.feature_count')"
fi
scored="$(json_value '(.scores // {}) | length')"
promoted="$(json_value '[.model_report.targets[]? | select(.accepted == true)] | length')"
expected_model_row="| $model_state | $ranking_strategy | $training_rows | $features | $scored | $promoted / 4 |"
if ! grep -Fqx -- "$expected_model_row" "$summary_file"; then
  printf '[ERROR] LightGBM ranking row does not match the run report. Expected: %s\n' "$expected_model_row" >&2
  exit 1
fi

format_strategy() {
  case "$1" in
    recency_weighted) printf 'Recency-weighted' ;;
    unweighted) printf 'Unweighted' ;;
    none) printf 'Neither variant passed promotion gates' ;;
    *) printf 'Not evaluated' ;;
  esac
}

format_yes_no() {
  case "$1" in
    true) printf 'Yes' ;;
    false) printf 'No' ;;
    *) printf 'n/a' ;;
  esac
}

for target in strict transfer stream end_to_end; do
  case "$target" in
    strict) label='Strict validation' ;;
    transfer) label='Transfer' ;;
    stream) label='Stream stability' ;;
    end_to_end) label='End-to-end' ;;
  esac

  strategy="$(format_strategy "$(json_value ".model_report.targets.${target}.recency_weighting_experiment.selected_strategy")")"
  unweighted_accepted="$(format_yes_no "$(json_value ".model_report.targets.${target}.recency_weighting_experiment.unweighted_accepted")")"
  weighted_accepted="$(format_yes_no "$(json_value ".model_report.targets.${target}.recency_weighting_experiment.weighted_accepted")")"
  unweighted_brier="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.unweighted_brier")"
  weighted_brier="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.weighted_brier")"
  unweighted_top20="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.unweighted_top_20pct_pass_rate")"
  weighted_top20="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.weighted_top_20pct_pass_rate")"
  half_life="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.half_life_days")"
  minimum_weight="$(json_value ".model_report.targets.${target}.recency_weighting_experiment.minimum_sample_weight")"
  expected_row="| $label | $strategy | $unweighted_accepted | $weighted_accepted | $unweighted_brier | $weighted_brier | $unweighted_top20 | $weighted_top20 | $half_life | $minimum_weight |"

  if ! grep -Fqx -- "$expected_row" "$summary_file"; then
    printf '[ERROR] LightGBM recency row for %s does not match the run report. Expected: %s\n' "$target" "$expected_row" >&2
    exit 1
  fi
done

printf '[OK] LightGBM Summary ranking and recency metrics match the score report from this run.\n'
