#!/usr/bin/env bash
set +e

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

generation_status="$(stage_status "$METADATA_OUTCOME")"
if [ "$generation_status" = "✅ Complete" ] && [ "$GENERATION_OUTCOME" != "success" ]; then
  generation_status="$(stage_status "$GENERATION_OUTCOME")"
fi
if [ "$generation_status" = "✅ Complete" ] && [ "$PREPARE_OUTCOME" != "success" ]; then
  generation_status="$(stage_status "$PREPARE_OUTCOME")"
fi

publish_status="$(stage_status "$PUBLISH_OUTCOME")"
overall_status="$(stage_status "$JOB_STATUS")"

xray_summary_version="${XRAY_VERSION:-unknown}"
singbox_summary_version="${SINGBOX_VERSION:-unknown}"

count_configs() {
  if [ -s "$1" ]; then
    awk 'NF && $0 !~ /^#profile-update-interval:/ { count++ } END { print count + 0 }' "$1"
  else
    printf '0'
  fi
}

all_count="$(count_configs subscriptions/all.txt)"
light_count="$(count_configs subscriptions/light.txt)"

input_candidates="unknown"
security_rejected="unknown"
strict_verified="unknown"
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
lightgbm_trained="unknown"
lightgbm_training_rows="0"
lightgbm_feature_count="0"
lightgbm_scored="0"
ml_rows="0"
ml_strict_passes="0"
ml_transfer_tests="0"
ml_transfer_passes="0"
ml_strict_rate="0.0"
if [ -s "subscriptions/light-training.jsonl" ]; then
  ml_stats="$(jq -s '
    {
      rows: length,
      strict_passes: (map(select(.label.strict_pass == true)) | length),
      transfer_tests: (map(select(.label.transfer_tested == true)) | length),
      transfer_passes: (map(select(.label.transfer_tested == true and .label.transfer_pass == true)) | length)
    }' subscriptions/light-training.jsonl 2>/dev/null || true)"
  if [ -n "$ml_stats" ]; then
    ml_rows="$(jq -r '.rows // 0' <<< "$ml_stats")"
    ml_feature_count="$(head -n 1 subscriptions/light-training.jsonl | jq -r '.features | length' 2>/dev/null || printf '0')"
    ml_strict_passes="$(jq -r '.strict_passes // 0' <<< "$ml_stats")"
    ml_transfer_tests="$(jq -r '.transfer_tests // 0' <<< "$ml_stats")"
    ml_transfer_passes="$(jq -r '.transfer_passes // 0' <<< "$ml_stats")"
    if [ "$ml_rows" -gt 0 ]; then
      ml_strict_rate="$(awk -v passes="$ml_strict_passes" -v rows="$ml_rows" 'BEGIN { printf "%.1f", (passes / rows) * 100 }')"
    fi
  fi
fi
if [ -s /tmp/proxyrift/lightgbm-scores.json ]; then
  lightgbm_trained="$(jq -r 'if .trained == true then "✅ Trained" else "⚪ Not trained" end' /tmp/proxyrift/lightgbm-scores.json 2>/dev/null || printf 'unknown')"
  lightgbm_training_rows="$(jq -r '.training_rows // 0' /tmp/proxyrift/lightgbm-scores.json 2>/dev/null || printf '0')"
  lightgbm_feature_count="$(jq -r '.feature_count // 0' /tmp/proxyrift/lightgbm-scores.json 2>/dev/null || printf '0')"
  lightgbm_scored="$(jq -r '(.scores // {}) | length' /tmp/proxyrift/lightgbm-scores.json 2>/dev/null || printf '0')"
fi
if [ -s /tmp/proxyrift/light-stats.json ]; then
  input_candidates="$(jq -r '.input_candidates // "unknown"' /tmp/proxyrift/light-stats.json)"
  security_rejected="$(jq -r '.security_rejected // "unknown"' /tmp/proxyrift/light-stats.json)"
  strict_verified="$(jq -r '.strict_verified // "unknown"' /tmp/proxyrift/light-stats.json)"
  selection_target="$(jq -r '.selection_target // "unknown"' /tmp/proxyrift/light-stats.json)"
  strict_selectable="$(jq -r '.strict_selectable // "unknown"' /tmp/proxyrift/light-stats.json)"
  stability_selectable="$(jq -r '.stability_selectable // "unknown"' /tmp/proxyrift/light-stats.json)"
  transfer_selectable="$(jq -r '.transfer_selectable // "unknown"' /tmp/proxyrift/light-stats.json)"
  stream_selectable="$(jq -r '.stream_selectable // "unknown"' /tmp/proxyrift/light-stats.json)"
  selection_shortfall="$(jq -r '.selection_shortfall // "unknown"' /tmp/proxyrift/light-stats.json)"
  transfer_tested="$(jq -r '.transfer_tested // "unknown"' /tmp/proxyrift/light-stats.json)"
  transfer_passed="$(jq -r '.transfer_passed // "unknown"' /tmp/proxyrift/light-stats.json)"
  stream_tested="$(jq -r '.stream_continuity_tested // "unknown"' /tmp/proxyrift/light-stats.json)"
  stream_passed="$(jq -r '.stream_continuity_passed // "unknown"' /tmp/proxyrift/light-stats.json)"
  published="$(jq -r '.published // "unknown"' /tmp/proxyrift/light-stats.json)"
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

{
  echo "# ProxyRift Update"
  echo
  echo "## Run status"
  echo
  echo "| Workflow | Collection & validation | Generation | Publish |"
  echo "|---|---|---|---|"
  echo "| $overall_status | $collection_status | $generation_status | $publish_status |"
  echo
  echo "## Runtime"
  echo
  if [ -n "$previous_run_number" ]; then
    echo "| Stage | This run | Previous (#$previous_run_number) | Change |"
    echo "|---|---:|---:|---:|"
    echo "| Total pipeline | $current_total_display | $previous_total_display | $total_delta_display |"
    echo "| Collection & transport | $current_collect_display | $previous_collect_display | $collect_delta_display |"
    echo "| Light validation | $current_light_display | $previous_light_display | $light_delta_display |"
  else
    echo "| Stage | This run |"
    echo "|---|---:|"
    echo "| Total pipeline | $current_total_display |"
    echo "| Collection & transport | $current_collect_display |"
    echo "| Light validation | $current_light_display |"
    echo
    echo "_No preceding completed update run was found for comparison._"
  fi
  echo
  echo "## Published outputs"
  echo
  echo "| Subscription | Config entries | Formats |"
  echo "|---|---:|---|"
  echo "| All | $all_count | Base64, Clash/Mihomo YAML, sing-box JSON |"
  echo "| Light | $light_count | Base64, Clash/Mihomo YAML, sing-box JSON |"
  echo
  echo "## Light selection"
  echo
  echo "| Input candidates | Strictly verified | Selection target | Published by validator |"
  echo "|---:|---:|---:|---:|"
  echo "| $input_candidates | $strict_verified | $selection_target | $published |"
  echo
  echo "<details>"
  echo "<summary>Light validation and transport-test details</summary>"
  echo
  echo "| Metric | Count |"
  echo "|---|---:|"
  echo "| TLS bypass rejected | $security_rejected |"
  echo "| Strict selectable | $strict_selectable |"
  echo "| 1 MiB selectable | $stability_selectable |"
  echo "| 10 MiB selectable | $transfer_selectable |"
  echo "| Stream selectable | $stream_selectable |"
  echo "| Selection shortfall | $selection_shortfall |"
  echo "| 10 MiB tested | $transfer_tested |"
  echo "| 10 MiB passed | $transfer_passed |"
  echo "| Stream continuity tested | $stream_tested |"
  echo "| Stream continuity passed | $stream_passed |"
  echo "</details>"
  echo
  echo "## LightGBM ranking"
  echo
  echo "| Model | Training rows | Features | Candidates scored |"
  echo "|---|---:|---:|---:|"
  echo "| $lightgbm_trained | $lightgbm_training_rows | $lightgbm_feature_count | $lightgbm_scored |"
  echo
  echo "<details>"
  echo "<summary>Training dataset health</summary>"
  echo
  echo "| Metric | Result |"
  echo "|---|---:|"
  echo "| Dataset rows | $ml_rows |"
  echo "| Strict passes | $ml_strict_passes / $ml_rows ($ml_strict_rate%) |"
  echo "| Transfer labels passed | $ml_transfer_passes / $ml_transfer_tests |"
  echo "| Stream continuity passed | $stream_passed / $stream_tested |"
  echo "</details>"
  echo
  echo "## Validation cores"
  echo
  echo "| Xray | sing-box |"
  echo "|---|---|"
  echo "| $xray_summary_version | $singbox_summary_version |"
  echo
  echo "_Generated by the ProxyRift update workflow._"
} >> "$GITHUB_STEP_SUMMARY"
