#!/usr/bin/env bash
set +e

PROXYRIFT_TMP_DIR="${PROXYRIFT_TMP_DIR:-/tmp/proxyrift}"

stage_status() {
  local outcome="$1"
  case "$outcome" in
    success) printf "✅ Complete" ;;
    failure) printf "❌ Failed" ;;
    cancelled) printf "⏹️ Cancelled" ;;
    skipped) printf "⏭️ Skipped" ;;
    *) printf "⚪ Unknown" ;;
  esac
}

collection_status="$(stage_status "$DISCOVERY_OUTCOME")"
if [ "$collection_status" = "✅ Complete" ] && [ "$COLLECT_OUTCOME" != "success" ]; then
  collection_status="$(stage_status "$COLLECT_OUTCOME")"
fi
if [ "$collection_status" = "✅ Complete" ] && [ "$LIGHT_OUTCOME" != "success" ]; then
  collection_status="$(stage_status "$LIGHT_OUTCOME")"
fi

generation_status="$(stage_status "$GENERATION_OUTCOME")"
if [ "$generation_status" = "✅ Complete" ] && [ "$PREPARE_OUTCOME" != "success" ]; then
  generation_status="$(stage_status "$PREPARE_OUTCOME")"
fi

publish_status="$(stage_status "$PUBLISH_OUTCOME")"
overall_status="$(stage_status "$JOB_STATUS")"

xray_summary_version="${XRAY_VERSION:-unknown}"
singbox_summary_version="${SINGBOX_VERSION:-unknown}"

count_configs() {
  if [ -s "$1" ]; then
    awk 'NF && $0 !~ /^#/ { count++ } END { print count + 0 }' "$1"
  else
    printf '0'
  fi
}

read_json_value() {
  local file="$1"
  local filter="$2"
  local fallback="$3"
  local value

  if [ ! -s "$file" ]; then
    printf '%s' "$fallback"
    return
  fi

  value="$(jq -r "$filter | if . == null then \"\" else tostring end" "$file" 2>/dev/null)" || value=""
  if [ -n "$value" ]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

format_lightgbm_status() {
  local target="$1"
  local accepted
  local raw_status

  accepted="$(read_json_value "$PROXYRIFT_TMP_DIR/lightgbm-scores.json" ".model_report.targets.${target}.accepted" "unknown")"
  raw_status="$(read_json_value "$PROXYRIFT_TMP_DIR/lightgbm-scores.json" ".model_report.targets.${target}.reason" "not_evaluated")"

  if [ "$accepted" = "true" ]; then
    case "$raw_status" in
      promoted_after_temporal_validation) printf 'Promoted after temporal validation' ;;
      *) printf 'Accepted: %s' "${raw_status//_/ }" ;;
    esac
    return
  fi

  if [ "$accepted" != "false" ]; then
    printf 'Not evaluated'
    return
  fi

  case "$raw_status" in
    model_did_not_beat_unseen_candidate_baseline)
      printf 'Did not beat unseen-candidate baseline'
      ;;
    top_20pct_below_unseen_candidate_baseline)
      printf 'Top-20%% pass rate below unseen-candidate baseline'
      ;;
    neither_weighting_strategy_passed_temporal_holdout)
      printf 'Neither weighting strategy passed temporal holdout'
      ;;
    "insufficient labelled data"*)
      printf 'Not promoted: insufficient labelled data'
      ;;
    "insufficient distinct observation timestamps"*)
      printf 'Not promoted: insufficient distinct timestamps'
      ;;
    "unseen-candidate temporal holdout lacks representative classes"*)
      printf 'Not promoted: unseen-candidate holdout lacks class balance'
      ;;
    not_evaluated)
      printf 'Not evaluated'
      ;;
    *)
      local readable="${raw_status//_/ }"
      printf '%s' "${readable^}"
      ;;
  esac
}

format_recency_strategy() {
  local target="$1"
  local strategy
  strategy="$(read_json_value "$PROXYRIFT_TMP_DIR/lightgbm-scores.json" ".model_report.targets.${target}.recency_weighting_experiment.selected_strategy" "not_evaluated")"
  case "$strategy" in
    recency_weighted) printf 'Recency-weighted' ;;
    unweighted) printf 'Unweighted' ;;
    unweighted_default_insufficient_selection_data) printf 'Unweighted default (selection data insufficient)' ;;
    unweighted_default_no_variant_passed_selection_gate) printf 'Unweighted default (neither variant passed inner selection gate)' ;;
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
    single_temporal_holdout_unseen_candidates_first_observation) printf 'Single temporal holdout on unseen candidates (first observations)' ;;
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

format_decimal() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { printf "%.5f", value }'
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

format_compact_number() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { if (value == int(value)) printf "%d", value; else printf "%g", value }'
  else
    printf '%s' "$value"
  fi
}

format_count() {
  local value="$1"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf '%s' "$value"
    return
  fi
  local grouped=""
  while (( ${#value} > 3 )); do
    grouped=",${value: -3}$grouped"
    value="${value:0:${#value}-3}"
  done
  printf '%s%s' "$value" "$grouped"
}

format_readiness() {
  case "$1" in
    true) printf 'Ready' ;;
    false) printf 'Not ready' ;;
    *) printf 'Not reported' ;;
  esac
}

all_count="$(count_configs subscriptions/all.txt)"
light_count="$(count_configs subscriptions/light.txt)"

input_candidates="unknown"
security_rejected="unknown"
strict_verified="unknown"
strict_attempts="unknown"
selection_target="unknown"
strict_selectable="unknown"
stability_selectable="unknown"
transfer_selectable="unknown"
stream_selectable="unknown"
selection_shortfall="unknown"
transfer_tested="unknown"
transfer_passed="unknown"
stream_tested="unknown"
stream_passed="unknown"
published="unknown"

score_file="$PROXYRIFT_TMP_DIR/lightgbm-scores.json"
lightgbm_model_state="Not evaluated"
lightgbm_ranking_strategy="Not evaluated"
lightgbm_training_rows="n/a"
lightgbm_model_feature_count="n/a"
lightgbm_scored="n/a"
lightgbm_promoted_targets="n/a"
lightgbm_validation_scheme="n/a"
lightgbm_baseline_type="n/a"
lightgbm_min_brier_improvement="n/a"
lightgbm_min_top20_lift="n/a"

declare -A lightgbm_target_status=()
declare -A lightgbm_target_brier=()
declare -A lightgbm_target_baseline=()
declare -A lightgbm_target_holdout=()
declare -A lightgbm_target_top20_rate=()
declare -A lightgbm_target_holdout_rate=()
declare -A lightgbm_target_labelled=()
declare -A lightgbm_target_pass_rows=()
declare -A lightgbm_target_fail_rows=()
declare -A lightgbm_target_train=()
declare -A lightgbm_target_train_pass=()
declare -A lightgbm_target_train_fail=()
declare -A lightgbm_target_holdout_pass=()
declare -A lightgbm_target_holdout_fail=()
declare -A lightgbm_target_recency_strategy=()
declare -A lightgbm_target_selection_available=()
declare -A lightgbm_target_unweighted_accepted=()
declare -A lightgbm_target_weighted_accepted=()
declare -A lightgbm_target_unweighted_brier=()
declare -A lightgbm_target_weighted_brier=()
declare -A lightgbm_target_unweighted_top20=()
declare -A lightgbm_target_weighted_top20=()
declare -A lightgbm_target_half_life=()
declare -A lightgbm_target_min_weight=()

if [ -s "$score_file" ]; then
  trained="$(read_json_value "$score_file" '.trained' 'unknown')"
  if [ "$trained" = "true" ]; then
    lightgbm_model_state="Model-based ranking active"
  elif [ "$trained" = "false" ]; then
    lightgbm_model_state="Neutral fallback active"
  fi

  lightgbm_training_rows="$(read_json_value "$score_file" '.training_rows' 'n/a')"
  lightgbm_model_feature_count="$(read_json_value "$score_file" '.model_report.model_feature_count' "$(read_json_value "$score_file" '.feature_count' 'n/a')")"
  lightgbm_scored="$(read_json_value "$score_file" '(.scores // {}) | length' 'n/a')"
  lightgbm_promoted_targets="$(read_json_value "$score_file" '[.model_report.targets[]? | select(.accepted == true)] | length' 'n/a')"
  lightgbm_validation_scheme="$(read_json_value "$score_file" '.model_report.validation' 'n/a')"
  lightgbm_baseline_type="$(read_json_value "$score_file" '.model_report.baseline' 'n/a')"
  lightgbm_min_brier_improvement="$(read_json_value "$score_file" '.model_report.promotion_minimum_relative_brier_improvement' 'n/a')"
  lightgbm_min_top20_lift="$(read_json_value "$score_file" '.model_report.promotion_top_20pct_lift_minimum' 'n/a')"
  lightgbm_ranking_strategy="$(format_ranking_strategy "$(read_json_value "$score_file" '.model_report.ranking_strategy' 'not_evaluated')")"

  for target in strict transfer stream end_to_end; do
    lightgbm_target_status["$target"]="$(format_lightgbm_status "$target")"
    lightgbm_target_brier["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.model_brier" 'n/a')"
    lightgbm_target_baseline["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.baseline_brier" 'n/a')"
    lightgbm_target_holdout["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.holdout_rows" 'n/a')"
    lightgbm_target_top20_rate["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.model_top_20pct_pass_rate" 'n/a')"
    lightgbm_target_holdout_rate["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.holdout_pass_rate" 'n/a')"
    lightgbm_target_labelled["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.labelled_rows" 'n/a')"
    lightgbm_target_pass_rows["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.pass_rows" 'n/a')"
    lightgbm_target_fail_rows["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.fail_rows" 'n/a')"
    lightgbm_target_train["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.train_rows" 'n/a')"
    lightgbm_target_train_pass["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.train_pass_rows" 'n/a')"
    lightgbm_target_train_fail["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.train_fail_rows" 'n/a')"
    lightgbm_target_holdout_pass["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.holdout_pass_rows" 'n/a')"
    lightgbm_target_holdout_fail["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.holdout_fail_rows" 'n/a')"
    lightgbm_target_recency_strategy["$target"]="$(format_recency_strategy "$target")"
    lightgbm_target_selection_available["$target"]="$(format_yes_no "$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.selection_available" 'n/a')")"
    lightgbm_target_unweighted_accepted["$target"]="$(format_yes_no "$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.unweighted_accepted" 'n/a')")"
    lightgbm_target_weighted_accepted["$target"]="$(format_yes_no "$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.weighted_accepted" 'n/a')")"
    lightgbm_target_unweighted_brier["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.unweighted_brier" 'n/a')"
    lightgbm_target_weighted_brier["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.weighted_brier" 'n/a')"
    lightgbm_target_unweighted_top20["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.unweighted_top_20pct_pass_rate" 'n/a')"
    lightgbm_target_weighted_top20["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.weighted_top_20pct_pass_rate" 'n/a')"
    lightgbm_target_half_life["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.half_life_days" 'n/a')"
    lightgbm_target_min_weight["$target"]="$(read_json_value "$score_file" ".model_report.targets.${target}.recency_weighting_experiment.minimum_sample_weight" 'n/a')"
  done
fi

ml_rows="n/a"
ml_new_rows="n/a"
ml_unique_runs="n/a"
ml_unique_candidates="n/a"
ml_strict_passes="n/a"
ml_strict_failures="n/a"
ml_transfer_tests="n/a"
ml_transfer_passes="n/a"
ml_transfer_failures="n/a"
ml_stream_tests="n/a"
ml_stream_passes="n/a"
ml_stream_failures="n/a"
ml_strict_ready="n/a"
ml_end_to_end_ready="n/a"

if [ -s "subscriptions/light-training-stats.json" ]; then
  stats_file="subscriptions/light-training-stats.json"
  ml_rows="$(read_json_value "$stats_file" '.dataset.rows' 'n/a')"
  ml_new_rows="$(read_json_value "$stats_file" '.dataset.new_rows' 'n/a')"
  ml_unique_runs="$(read_json_value "$stats_file" '.dataset.unique_runs' 'n/a')"
  ml_unique_candidates="$(read_json_value "$stats_file" '.dataset.unique_candidates' 'n/a')"
  ml_strict_passes="$(read_json_value "$stats_file" '.dataset.strict_passes' 'n/a')"
  ml_strict_failures="$(read_json_value "$stats_file" '.dataset.strict_failures' 'n/a')"
  ml_transfer_tests="$(read_json_value "$stats_file" '.dataset.transfer_tests' 'n/a')"
  ml_transfer_passes="$(read_json_value "$stats_file" '.dataset.transfer_passes' 'n/a')"
  ml_transfer_failures="$(read_json_value "$stats_file" '.dataset.transfer_failures' 'n/a')"
  ml_stream_tests="$(read_json_value "$stats_file" '.dataset.stream_tests' 'n/a')"
  ml_stream_passes="$(read_json_value "$stats_file" '.dataset.stream_passes' 'n/a')"
  ml_stream_failures="$(read_json_value "$stats_file" '.dataset.stream_failures' 'n/a')"
  ml_strict_ready="$(read_json_value "$stats_file" '.readiness.strict_model_ready' 'n/a')"
  ml_end_to_end_ready="$(read_json_value "$stats_file" '.readiness.end_to_end_model_ready' 'n/a')"
fi


if [ -s "$PROXYRIFT_TMP_DIR/light-stats.json" ]; then
  input_candidates="$(jq -r '.input_candidates // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  security_rejected="$(jq -r '.security_rejected // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  strict_verified="$(jq -r '.strict_verified // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  strict_attempts="$(jq -r '.strict_attempts // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  selection_target="$(jq -r '.selection_target // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  strict_selectable="$(jq -r '.strict_selectable // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stability_selectable="$(jq -r '.stability_selectable // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  transfer_selectable="$(jq -r '.transfer_selectable // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stream_selectable="$(jq -r '.stream_selectable // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  selection_shortfall="$(jq -r '.selection_shortfall // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  transfer_tested="$(jq -r '.transfer_tested // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  transfer_passed="$(jq -r '.transfer_passed // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stream_tested="$(jq -r '.stream_continuity_tested // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stream_passed="$(jq -r '.stream_continuity_passed // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published="$(jq -r '.published // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
fi

current_jobs=""
if current_jobs="$(gh api --method GET "/repos/$GH_REPOSITORY/actions/runs/$GITHUB_RUN_ID/jobs?per_page=100" 2>/dev/null)"; then
  :
fi

step_elapsed() {
  local step_name="$1"
  if [ -z "$current_jobs" ]; then
    printf "%s" "unknown"
    return
  fi
  local elapsed
  elapsed="$(jq -r --arg step "$step_name" '.jobs[] | .steps[] | select(.name == $step and .started_at != null and .completed_at != null) | ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601)) | tostring' <<< "$current_jobs" | head -n 1)"
  [ -n "$elapsed" ] || elapsed="unknown"
  printf "%s" "$elapsed"
}

collect_elapsed="$(step_elapsed "🔎 Collect & transport-test configs")"
light_elapsed="$(step_elapsed "💡 Build Light subscription")"

current_job_started=""
current_publish_completed=""
if [ -n "$current_jobs" ]; then
  current_job_started="$(jq -r '.jobs[] | select(.name == "🔎 Collect & Validate") | .started_at // empty' <<< "$current_jobs" | head -n 1)"
  current_publish_completed="$(jq -r '.jobs[] | select(.name == "🔎 Collect & Validate") | .steps[] | select(.name == "🚀 Publish generated changes") | .completed_at // empty' <<< "$current_jobs" | head -n 1)"
fi

pipeline_elapsed="unknown"
if [[ "$current_job_started" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]; then
  job_started_epoch="$(date -d "$current_job_started" +%s 2>/dev/null || true)"
  if [[ "$job_started_epoch" =~ ^[0-9]+$ ]]; then
    pipeline_end_epoch="$(date +%s)"
    if [[ "$current_publish_completed" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]; then
      pipeline_end_epoch="$(date -d "$current_publish_completed" +%s 2>/dev/null || printf "%s" "$pipeline_end_epoch")"
    fi
    pipeline_elapsed="$(( pipeline_end_epoch - job_started_epoch ))"
  fi
fi

format_minutes() {
  local seconds="$1"
  if [[ "$seconds" =~ ^[0-9]+$ ]]; then
    awk -v seconds="$seconds" 'BEGIN { printf "%.1f min", seconds / 60 }'
  else
    printf '%s' "unknown"
  fi
}

format_delta() {
  local current="$1"
  local previous="$2"
  if [[ "$current" =~ ^[0-9]+$ && "$previous" =~ ^[0-9]+$ ]]; then
    awk -v current="$current" -v previous="$previous" '
      BEGIN {
        delta = current - previous
        pct = previous > 0 ? (delta / previous) * 100 : 0
        printf "%+.1f min (%+.1f%%)", delta / 60, pct
      }'
  else
    printf '%s' "unknown"
  fi
}

previous_run_id=""
previous_run_number=""
previous_total_elapsed="unknown"
previous_collect_elapsed="unknown"
previous_light_elapsed="unknown"

if previous_runs="$(
  gh api --method GET \
    "/repos/$GH_REPOSITORY/actions/workflows/update.yml/runs?per_page=30" \
    2>/dev/null
)"; then
  previous_run_id="$(
    jq -r --argjson current_run "$GITHUB_RUN_NUMBER" '
      [.workflow_runs[]
       | select(.status == "completed" and .run_number < $current_run)]
      | sort_by(.run_number)
      | last
      | .id // empty' <<< "$previous_runs"
  )"

  previous_run_number="$(
    jq -r --argjson current_run "$GITHUB_RUN_NUMBER" '
      [.workflow_runs[]
       | select(.status == "completed" and .run_number < $current_run)]
      | sort_by(.run_number)
      | last
      | .run_number // empty' <<< "$previous_runs"
  )"
fi

if [ -n "$previous_run_id" ]; then
  if previous_jobs="$(
    gh api --method GET \
      "/repos/$GH_REPOSITORY/actions/runs/$previous_run_id/jobs?per_page=100" \
      2>/dev/null
  )"; then
    previous_collect_elapsed="$(
      jq -r \
        --arg step "🔎 Collect & transport-test configs" \
        '.jobs[]
         | .steps[]
         | select(.name == $step and .started_at != null and .completed_at != null)
         | ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601))
         | tostring' <<< "$previous_jobs" | head -n 1
    )"

    previous_light_elapsed="$(
      jq -r \
        --arg step "💡 Build Light subscription" \
        '.jobs[]
         | .steps[]
         | select(.name == $step and .started_at != null and .completed_at != null)
         | ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601))
         | tostring' <<< "$previous_jobs" | head -n 1
    )"

    previous_summary_epoch="$(
      jq -r                   '.jobs[]
         | select(.name == "🔎 Collect & Validate")
         | .steps[]
         | select(.name == "🚀 Publish generated changes" and .completed_at != null)
         | (.completed_at | fromdateiso8601)
         | tostring' <<< "$previous_jobs" | head -n 1
    )"

    previous_job_started="$(
      jq -r \
        '.jobs[]
         | select(.name == "🔎 Collect & Validate")
         | .started_at
         | select(. != null)
         | fromdateiso8601
         | tostring' <<< "$previous_jobs" | head -n 1
    )"

    if [[ "$previous_summary_epoch" =~ ^[0-9]+$ && "$previous_job_started" =~ ^[0-9]+$ ]]; then
      previous_total_elapsed="$(( previous_summary_epoch - previous_job_started ))"
    fi
  fi
fi

current_total_display="$(format_minutes "$pipeline_elapsed")"
current_collect_display="$(format_minutes "$collect_elapsed")"
current_light_display="$(format_minutes "$light_elapsed")"
previous_total_display="$(format_minutes "$previous_total_elapsed")"
previous_collect_display="$(format_minutes "$previous_collect_elapsed")"
previous_light_display="$(format_minutes "$previous_light_elapsed")"
total_delta_display="$(format_delta "$pipeline_elapsed" "$previous_total_elapsed")"
collect_delta_display="$(format_delta "$collect_elapsed" "$previous_collect_elapsed")"
light_delta_display="$(format_delta "$light_elapsed" "$previous_light_elapsed")"

summary_output_file="$PROXYRIFT_TMP_DIR/run-summary.md"
mkdir -p "$(dirname "$summary_output_file")"
{
  echo "# ProxyRift Update"
  echo
  echo "**Overall status:** $overall_status"
  echo
  echo "## Pipeline status"
  echo
  echo "| Stage | Result |"
  echo "|---|---|"
  echo "| Discovery and validation | $collection_status |"
  echo "| Subscription generation | $generation_status |"
  echo "| Publishing | $publish_status |"
  echo
  echo "## Runtime"
  echo
  if [ -n "$previous_run_number" ]; then
    echo "| Stage | This run | Previous (#$previous_run_number) | Change |"
    echo "|---|---:|---:|---:|"
    echo "| Total pipeline | $current_total_display | $previous_total_display | $total_delta_display |"
    echo "| Collection and transport | $current_collect_display | $previous_collect_display | $collect_delta_display |"
    echo "| Light validation | $current_light_display | $previous_light_display | $light_delta_display |"
  else
    echo "| Stage | This run |"
    echo "|---|---:|"
    echo "| Total pipeline | $current_total_display |"
    echo "| Collection and transport | $current_collect_display |"
    echo "| Light validation | $current_light_display |"
    echo
    echo "_No previous completed update run was found for comparison._"
  fi
  echo
  echo "## Published subscriptions"
  echo
  echo "| Subscription | Configs published |"
  echo "|---|---:|"
  echo "| All | $(format_count "$all_count") |"
  echo "| Light | $(format_count "$light_count") |"
  echo
  echo "## Light validation"
  echo
  echo "| Check | Result |"
  echo "|---|---:|"
  echo "| Candidates considered | $(format_count "$input_candidates") |"
  echo "| Strict validation | $(format_count "$strict_verified") of $(format_count "$strict_attempts") passed |"
  echo "| 10 MiB transfer | $(format_count "$transfer_passed") of $(format_count "$transfer_tested") passed |"
  echo "| Sustained stream | $(format_count "$stream_passed") of $(format_count "$stream_tested") passed |"
  echo "| Light configs published | $(format_count "$published") of $(format_count "$selection_target") target |"
  echo
  echo "## LightGBM"
  echo
  echo "**Ranking:** $lightgbm_model_state — $lightgbm_ranking_strategy ($(format_count "$lightgbm_promoted_targets") of 4 targets accepted)."
  echo
  echo "**Training:** $(format_count "$lightgbm_training_rows") rows · $(format_count "$lightgbm_model_feature_count") features · Candidates scored: $(format_count "$lightgbm_scored")."
  echo
  echo "**Evaluation:** $(format_validation_scheme "$lightgbm_validation_scheme") · Baseline: $(format_baseline "$lightgbm_baseline_type")."
  echo
  echo "| Target | Decision | Brier score (model / baseline) | Top-20% pass / holdout rate |"
  echo "|---|---|---:|---:|"
  echo "| Strict validation | ${lightgbm_target_status[strict]} | $(format_decimal "${lightgbm_target_brier[strict]}") / $(format_decimal "${lightgbm_target_baseline[strict]}") | $(format_percentage "${lightgbm_target_top20_rate[strict]}") / $(format_percentage "${lightgbm_target_holdout_rate[strict]}") |"
  echo "| Transfer (10 MiB) | ${lightgbm_target_status[transfer]} | $(format_decimal "${lightgbm_target_brier[transfer]}") / $(format_decimal "${lightgbm_target_baseline[transfer]}") | $(format_percentage "${lightgbm_target_top20_rate[transfer]}") / $(format_percentage "${lightgbm_target_holdout_rate[transfer]}") |"
  echo "| Stream stability | ${lightgbm_target_status[stream]} | $(format_decimal "${lightgbm_target_brier[stream]}") / $(format_decimal "${lightgbm_target_baseline[stream]}") | $(format_percentage "${lightgbm_target_top20_rate[stream]}") / $(format_percentage "${lightgbm_target_holdout_rate[stream]}") |"
  echo "| End-to-end | ${lightgbm_target_status[end_to_end]} | $(format_decimal "${lightgbm_target_brier[end_to_end]}") / $(format_decimal "${lightgbm_target_baseline[end_to_end]}") | $(format_percentage "${lightgbm_target_top20_rate[end_to_end]}") / $(format_percentage "${lightgbm_target_holdout_rate[end_to_end]}") |"
  if [ "${lightgbm_target_recency_strategy[strict]}" != "Not evaluated" ]; then
    echo
    if [ "${lightgbm_target_selection_available[strict]}" = "Yes" ]; then
      echo "**Recency strategy selection:** Strict validation — ${lightgbm_target_recency_strategy[strict]} · Inner holdout Brier (unweighted → weighted) $(format_decimal "${lightgbm_target_unweighted_brier[strict]}") → $(format_decimal "${lightgbm_target_weighted_brier[strict]}") · Inner holdout Top-20% pass rate $(format_percentage "${lightgbm_target_unweighted_top20[strict]}") → $(format_percentage "${lightgbm_target_weighted_top20[strict]}") · $(format_compact_number "${lightgbm_target_half_life[strict]}")-day half-life, minimum weight ${lightgbm_target_min_weight[strict]}."
    else
      echo "**Recency strategy selection:** Strict validation — ${lightgbm_target_recency_strategy[strict]} · Comparison skipped because the inner temporal holdout lacked representative data."
    fi
  fi
  echo
  echo "**Training data:** $(format_count "$ml_rows") rows ($(format_count "$ml_new_rows") new this run) · $(format_count "$ml_unique_candidates") unique candidates · $(format_count "$ml_unique_runs") runs. Strict model: $(format_readiness "$ml_strict_ready") · End-to-end model: $(format_readiness "$ml_end_to_end_ready")."
  echo
  echo "<details>"
  echo "<summary>Validation tool versions</summary>"
  echo
  echo "| Xray | sing-box |"
  echo "|---|---|"
  echo "| $xray_summary_version | $singbox_summary_version |"
  echo "</details>"
  echo
  echo "_Generated by the ProxyRift update workflow._"
} | tee "$summary_output_file" >> "$GITHUB_STEP_SUMMARY"
