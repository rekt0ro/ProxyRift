#!/usr/bin/env bash
set +e

PROXYRIFT_TMP_DIR="${PROXYRIFT_TMP_DIR:-/tmp/proxyrift}"

stage_status() {
  local outcome="$1"
  case "$outcome" in
    success) printf "Complete" ;;
    failure) printf "Failed" ;;
    cancelled) printf "Cancelled" ;;
    skipped) printf "Skipped" ;;
    *) printf "Unknown" ;;
  esac
}

collection_status="$(stage_status "$DISCOVERY_OUTCOME")"
if [ "$collection_status" = "Complete" ] && [ "$COLLECT_OUTCOME" != "success" ]; then
  collection_status="$(stage_status "$COLLECT_OUTCOME")"
fi
if [ "$collection_status" = "Complete" ] && [ "$LIGHT_OUTCOME" != "success" ]; then
  collection_status="$(stage_status "$LIGHT_OUTCOME")"
fi

generation_status="$(stage_status "$GENERATION_OUTCOME")"
if [ "$generation_status" = "Complete" ] && [ "$PREPARE_OUTCOME" != "success" ]; then
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

format_ranking_strategy() {
  case "$1" in
    stream_speed_regression) printf 'Speed model' ;;
    historical_stream_speed_prior_fallback) printf 'Recent speed estimates' ;;
    *) printf 'Neutral ranking' ;;
  esac
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

format_mae_change() {
  local value="$1"
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    awk -v value="$value" 'BEGIN { pct = value * 100; if (pct >= 0) printf "%.1f%% lower", pct; else printf "%.1f%% higher", -pct }'
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

format_model_decision() {
  local accepted="$1" reason="$2"
  if [[ "$accepted" == "true" ]]; then
    printf 'Active'
    return
  fi
  case "$reason" in
    insufficient_speed_samples_or_temporal_coverage) printf 'Not enabled — collecting more recent speed measurements' ;;
    insufficient_distinct_timestamps) printf 'Not enabled — not enough separate test runs' ;;
    insufficient_unseen_candidate_holdout) printf 'Not enabled — not enough unseen configs to validate' ;;
    model_did_not_beat_temporal_speed_baseline) printf 'Not enabled — MAE did not improve on the temporal baseline by at least 1%' ;;
    top_20pct_speed_ranking_did_not_improve) printf 'Not enabled — top-20%% speed lift did not exceed the baseline' ;;
    final_model_fit_failed|holdout_model_fit_failed) printf 'Not enabled — model training failed' ;;
    *) local readable="${reason//_/ }"; printf 'Not evaluated — %s.' "${readable:-no evaluation report}" ;;
  esac
}

format_model_evaluation() {
  local reason="$1" model_mae="$2" baseline_mae="$3" improvement="$4"
  local speed_lift="$5" speed_samples="$6" unique_candidates="$7"
  local unique_timestamps="$8" train_rows="$9" holdout_rows="${10}"

  if [[ "$model_mae" =~ ^-?[0-9]+([.][0-9]+)?$ \
    && "$baseline_mae" =~ ^-?[0-9]+([.][0-9]+)?$ \
    && "$improvement" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    printf 'Evaluated — model MAE %s vs baseline %s (%s); top-20%% holdout speed lift %s.' \
      "$(format_decimal "$model_mae")" "$(format_decimal "$baseline_mae")" \
      "$(format_mae_change "$improvement")" "$(format_multiplier "$speed_lift")"
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
    top_20pct_speed_ranking_did_not_improve)
      printf 'Metrics unavailable, but the artifact reports that the top-20%% speed ranking did not improve.' ;;
    final_model_fit_failed|holdout_model_fit_failed) printf 'Not evaluated — model fitting failed.' ;;
    *) local readable="${reason//_/ }"; printf 'Not evaluated — %s.' "${readable:-no evaluation report}" ;;
  esac
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

all_count="$(count_configs subscriptions/all.txt)"
light_count="$(count_configs subscriptions/light.txt)"

input_candidates="unknown"
strict_verified="unknown"
strict_attempts="unknown"
selection_target="unknown"
transfer_tested="unknown"
transfer_passed="unknown"
stream_tested="unknown"
stream_passed="unknown"
published="unknown"
fresh_family_target_percent="unknown"
published_new_candidates="unknown"
published_expired_candidates="unknown"
published_revalidated="unknown"
revalidated_candidate_max_percent="unknown"
published_from_fresh_families="unknown"
fresh_family_actual_percent="n/a"

score_file="$PROXYRIFT_TMP_DIR/lightgbm-scores.json"
lightgbm_ranking_strategy="neutral_default"
lightgbm_training_rows="n/a"
lightgbm_model_accepted="false"
lightgbm_model_reason="not_evaluated"
lightgbm_baseline_mae="n/a"
lightgbm_model_mae="n/a"
lightgbm_relative_improvement="n/a"
lightgbm_top20_lift="n/a"
lightgbm_speed_samples="n/a"
lightgbm_unique_candidates="n/a"
lightgbm_unique_timestamps="n/a"
lightgbm_train_rows="n/a"
lightgbm_holdout_rows="n/a"

if [ -s "$score_file" ]; then
  trained="$(read_json_value "$score_file" '.trained' 'false')"
  lightgbm_ranking_strategy_raw="$(read_json_value "$score_file" '.model_report.ranking_strategy' 'neutral_default')"
  if [ "$trained" = "true" ]; then
    lightgbm_ranking_strategy="stream_speed_regression"
  else
    lightgbm_ranking_strategy="$lightgbm_ranking_strategy_raw"
  fi
  lightgbm_training_rows="$(read_json_value "$score_file" '.training_rows' 'n/a')"
  lightgbm_model_accepted="$(read_json_value "$score_file" '.model_report.accepted' 'false')"
  lightgbm_model_reason="$(read_json_value "$score_file" '.model_report.reason' 'not_evaluated')"
  lightgbm_baseline_mae="$(read_json_value "$score_file" '.model_report.baseline_log_mae' 'n/a')"
  lightgbm_model_mae="$(read_json_value "$score_file" '.model_report.model_log_mae' 'n/a')"
  lightgbm_relative_improvement="$(read_json_value "$score_file" '.model_report.relative_mae_improvement' 'n/a')"
  lightgbm_top20_lift="$(read_json_value "$score_file" '.model_report.top_20pct_speed_lift' 'n/a')"
  lightgbm_speed_samples="$(read_json_value "$score_file" '.model_report.speed_samples' 'n/a')"
  lightgbm_unique_candidates="$(read_json_value "$score_file" '.model_report.unique_candidates' 'n/a')"
  lightgbm_unique_timestamps="$(read_json_value "$score_file" '.model_report.unique_timestamps' 'n/a')"
  lightgbm_train_rows="$(read_json_value "$score_file" '.model_report.train_rows' 'n/a')"
  lightgbm_holdout_rows="$(read_json_value "$score_file" '.model_report.holdout_rows' 'n/a')"
fi

ml_rows="n/a"
ml_new_rows="n/a"
if [ -s "subscriptions/light-training-stats.json" ]; then
  stats_file="subscriptions/light-training-stats.json"
  ml_rows="$(read_json_value "$stats_file" '.dataset.speed_samples' 'n/a')"
  ml_new_rows="$(read_json_value "$stats_file" '.dataset.new_speed_samples' 'n/a')"
fi

if [ -s "$PROXYRIFT_TMP_DIR/light-stats.json" ]; then
  input_candidates="$(jq -r '.input_candidates // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  strict_verified="$(jq -r '.strict_verified // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  strict_attempts="$(jq -r '.strict_attempts // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  selection_target="$(jq -r '.selection_target // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  transfer_tested="$(jq -r '.transfer_tested // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  transfer_passed="$(jq -r '.transfer_passed // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stream_tested="$(jq -r '.stream_continuity_tested // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  stream_passed="$(jq -r '.stream_continuity_passed // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published="$(jq -r '.published // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  fresh_family_target_percent="$(jq -r '.fresh_family_target_percent // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published_new_candidates="$(jq -r '.published_new_candidates // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published_expired_candidates="$(jq -r '.published_expired_candidates // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published_revalidated="$(jq -r '.published_revalidated // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  revalidated_candidate_max_percent="$(jq -r '.revalidated_candidate_max_percent // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
  published_from_fresh_families="$(jq -r '.published_from_fresh_families // "unknown"' "$PROXYRIFT_TMP_DIR/light-stats.json")"
fi

if [[ "$published_from_fresh_families" =~ ^[0-9]+$ && "$published" =~ ^[0-9]+$ ]] && (( published > 0 )); then
  fresh_family_actual_percent="$(awk -v fresh="$published_from_fresh_families" -v total="$published" 'BEGIN { printf "%.1f%%", (fresh / total) * 100 }')"
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
  echo "## Published output"
  echo
  echo "| Subscription | Published configs |"
  echo "|---|---:|"
  echo "| All | $(format_count "$all_count") |"
  echo "| Light | $(format_count "$light_count") |"
  echo
  echo "## Light validation"
  echo
  echo "| Stage | Passed / tested |"
  echo "|---|---:|"
  echo "| Candidates received | $(format_count "$input_candidates") |"
  echo "| Strict validation | $(format_count "$strict_verified") / $(format_count "$strict_attempts") |"
  echo "| 10 MiB transfer | $(format_count "$transfer_passed") / $(format_count "$transfer_tested") |"
  echo "| Sustained stream | $(format_count "$stream_passed") / $(format_count "$stream_tested") |"
  echo
  echo "## Light selection mix"
  echo
  echo "| Category | Result |"
  echo "|---|---:|"
  echo "| New candidates (not in retained history) | $(format_count "$published_new_candidates") |"
  echo "| Previously known; 24-hour revalidation expired | $(format_count "$published_expired_candidates") |"
  echo "| Recently reused (validated within 24 hours) | $(format_count "$published_revalidated") of $(format_count "$published") (maximum ${revalidated_candidate_max_percent}%) |"
  echo "| Configs from unseen families | $(format_count "$published_from_fresh_families") of $(format_count "$published") ($fresh_family_actual_percent; target ${fresh_family_target_percent}%) |"
  echo "| Light published | $(format_count "$published") of $(format_count "$selection_target") target |"
  echo
  echo "_New means the candidate was not found in retained validation history or prior Light subscription revisions. Revalidation-expired means it was previously known but had not been fully validated within the last 24 hours. Unseen-family coverage measures diversity of published configs, not candidate age; it is a separate dimension._"
  echo
  echo "## Speed ranking (LightGBM)"
  echo
  echo "| Measure | Result |"
  echo "|---|---|"
  echo "| Ranking mode | $(format_ranking_strategy "$lightgbm_ranking_strategy") |"
  echo "| Recent speed data | $(format_count "$lightgbm_speed_samples") speed measurements in the last 72 hours; $(format_count "$ml_new_rows") new this run; $(format_count "$lightgbm_unique_candidates") candidate identities across $(format_count "$lightgbm_unique_timestamps") timestamps |"
  if [[ "$lightgbm_train_rows" =~ ^[0-9]+$ && "$lightgbm_holdout_rows" =~ ^[0-9]+$ ]] && { (( lightgbm_train_rows > 0 || lightgbm_holdout_rows > 0 )); }; then
    echo "| Temporal validation split | $(format_count "$lightgbm_train_rows") training rows / $(format_count "$lightgbm_holdout_rows") holdout rows |"
  else
    echo "| Temporal validation split | Not available — validation data requirements were not met |"
  fi
  echo "| Evaluation | $(format_model_evaluation "$lightgbm_model_reason" "$lightgbm_model_mae" "$lightgbm_baseline_mae" "$lightgbm_relative_improvement" "$lightgbm_top20_lift" "$lightgbm_speed_samples" "$lightgbm_unique_candidates" "$lightgbm_unique_timestamps" "$lightgbm_train_rows" "$lightgbm_holdout_rows") |"
  echo "| Decision | $(format_model_decision "$lightgbm_model_accepted" "$lightgbm_model_reason") |"
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
