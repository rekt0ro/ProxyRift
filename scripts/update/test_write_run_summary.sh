#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

work="$tmp/work"
mkdir -p "$work/subscriptions" "$work/proxyrift" "$tmp/bin"

printf 'vless://all@example.com:443\n' > "$work/subscriptions/all.txt"
printf 'vless://light@example.net:443\n' > "$work/subscriptions/light.txt"

cat > "$work/proxyrift/light-stats.json" <<'JSON'
{
  "input_candidates": 15000,
  "security_rejected": 0,
  "strict_verified": 300,
  "strict_attempts": 317,
  "selection_target": 200,
  "strict_selectable": 200,
  "stability_selectable": 200,
  "transfer_selectable": 200,
  "stream_selectable": 200,
  "selection_shortfall": 0,
  "transfer_tested": 232,
  "transfer_passed": 211,
  "stream_continuity_tested": 211,
  "stream_continuity_passed": 209,
  "published": 200
}
JSON

cat > "$work/proxyrift/lightgbm-scores.json" <<'JSON'
{
  "trained": true,
  "training_rows": 50000,
  "feature_count": 66,
  "scores": {"candidate": 0.1},
  "model_report": {
    "validation": "single_temporal_holdout_unseen_candidates_first_observation",
    "baseline": "training_prevalence_brier",
    "ranking_strategy": "stage_model_fallback",
    "model_feature_count": 66,
    "promotion_minimum_relative_brier_improvement": 0.005,
    "promotion_top_20pct_lift_minimum": 1.0,
    "targets": {
      "strict": {
        "accepted": true, "reason": "promoted_after_temporal_validation",
        "labelled_rows": 50000, "pass_rows": 47000, "fail_rows": 3000,
        "train_rows": 39500, "train_pass_rows": 37000, "train_fail_rows": 2500,
        "holdout_rows": 500, "holdout_pass_rows": 300, "holdout_fail_rows": 200,
        "model_brier": 0.10, "baseline_brier": 0.15,
        "model_top_20pct_pass_rate": 0.80, "holdout_pass_rate": 0.60,
        "recency_weighting_experiment": {
          "selection_evaluation_strategy": "inner_temporal_holdout_unseen_candidates_first_observation",
          "selection_available": true,
          "selection_reason": "recency_weighted_passed_inner_selection_gate",
          "selection_training_rows": 30000, "selection_holdout_rows": 600,
          "selection_holdout_pass_rows": 420, "selection_holdout_fail_rows": 180,
          "selection_baseline_brier": 0.15,
          "half_life_days": 3.0, "minimum_sample_weight": 0.1,
          "unweighted_brier": 0.13, "weighted_brier": 0.10,
          "unweighted_accepted": true, "weighted_accepted": true,
          "unweighted_top_20pct_pass_rate": 0.70, "weighted_top_20pct_pass_rate": 0.80,
          "selected_strategy": "recency_weighted"
        }
      },
      "transfer": {
        "accepted": false, "reason": "model_did_not_beat_unseen_candidate_baseline",
        "labelled_rows": 1000, "pass_rows": 850, "fail_rows": 150,
        "train_rows": 800, "train_pass_rows": 700, "train_fail_rows": 100,
        "holdout_rows": 200, "holdout_pass_rows": 150, "holdout_fail_rows": 50,
        "model_brier": 0.22, "baseline_brier": 0.20,
        "model_top_20pct_pass_rate": 0.70, "holdout_pass_rate": 0.75,
        "recency_weighting_experiment": {
          "half_life_days": 3.0, "minimum_sample_weight": 0.1,
          "unweighted_brier": 0.22, "weighted_brier": 0.22,
          "unweighted_accepted": false, "weighted_accepted": false,
          "unweighted_top_20pct_pass_rate": 0.70, "weighted_top_20pct_pass_rate": 0.70,
          "selected_strategy": "none"
        }
      },
      "stream": {
        "accepted": false, "reason": "top_20pct_below_unseen_candidate_baseline",
        "labelled_rows": 700, "pass_rows": 600, "fail_rows": 100,
        "train_rows": 550, "train_pass_rows": 500, "train_fail_rows": 50,
        "holdout_rows": 150, "holdout_pass_rows": 120, "holdout_fail_rows": 30,
        "model_brier": 0.20, "baseline_brier": 0.19,
        "model_top_20pct_pass_rate": 0.70, "holdout_pass_rate": 0.80,
        "recency_weighting_experiment": {
          "half_life_days": 3.0, "minimum_sample_weight": 0.1,
          "unweighted_brier": 0.20, "weighted_brier": 0.20,
          "unweighted_accepted": false, "weighted_accepted": false,
          "unweighted_top_20pct_pass_rate": 0.70, "weighted_top_20pct_pass_rate": 0.70,
          "selected_strategy": "none"
        }
      },
      "end_to_end": {
        "accepted": false,
        "reason": "insufficient labelled data (rows=6708, pass=0, fail=6708, unique_candidates=2281)",
        "labelled_rows": 6708, "pass_rows": 0, "fail_rows": 6708, "unique_candidates": 2281
      }
    }
  }
}
JSON

cat > "$work/subscriptions/light-training-stats.json" <<'JSON'
{
  "schema_version": 2,
  "dataset": {
    "rows": 50000, "new_rows": 278, "unique_runs": 177, "unique_candidates": 4044,
    "strict_passes": 47051, "strict_failures": 2949,
    "transfer_tests": 28313, "transfer_passes": 24568, "transfer_failures": 3745,
    "stream_tests": 18782, "stream_passes": 18058, "stream_failures": 724
  },
  "readiness": {"strict_model_ready": true, "end_to_end_model_ready": false}
}
JSON

cat > "$tmp/bin/gh" <<'GH'
#!/usr/bin/env bash
case "$*" in
  *"actions/workflows/update.yml/runs"*) printf '{"workflow_runs":[]}\n' ;;
  *"actions/runs/"*"jobs"*) printf '{"jobs":[]}\n' ;;
  *) printf '{}\n' ;;
esac
GH
chmod +x "$tmp/bin/gh"

(
  cd "$work"
  PATH="$tmp/bin:$PATH" \
  PROXYRIFT_TMP_DIR="$work/proxyrift" \
  GH_REPOSITORY="test/repo" \
  GITHUB_RUN_ID="1" \
  GITHUB_RUN_NUMBER="1" \
  JOB_STATUS="success" \
  DISCOVERY_OUTCOME="success" \
  COLLECT_OUTCOME="success" \
  LIGHT_OUTCOME="success" \
  GENERATION_OUTCOME="success" \
  PREPARE_OUTCOME="success" \
  PUBLISH_OUTCOME="success" \
  XRAY_VERSION="test" \
  SINGBOX_VERSION="test" \
  GITHUB_STEP_SUMMARY="$tmp/summary.md" \
  bash "$repo_root/scripts/update/write_run_summary.sh"
)

cmp -s "$tmp/summary.md" "$work/proxyrift/run-summary.md"


grep -Fq '| All | 1 |' "$tmp/summary.md"
grep -Fq '| Light | 1 |' "$tmp/summary.md"
grep -Fq '| Candidates considered | 15,000 |' "$tmp/summary.md"
grep -Fq '| Strict validation | 300 of 317 passed |' "$tmp/summary.md"
grep -Fq '| 10 MiB transfer | 211 of 232 passed |' "$tmp/summary.md"
grep -Fq '| Light configs published | 200 of 200 target |' "$tmp/summary.md"
grep -Fq '**Ranking:** Model-based ranking active — Stage-model fallback (1 of 4 targets accepted).' "$tmp/summary.md"
grep -Fq '**Training:** 50,000 rows · 66 features · Candidates scored: 1.' "$tmp/summary.md"
grep -Fq '**Evaluation:** Single temporal holdout on unseen candidates (first observations) · Baseline: Training-prevalence Brier score.' "$tmp/summary.md"
grep -Fq '| Strict validation | Promoted after temporal validation | 0.10000 / 0.15000 | 80.0% / 60.0% |' "$tmp/summary.md"
grep -Fq '| Transfer (10 MiB) | Did not beat unseen-candidate baseline | 0.22000 / 0.20000 | 70.0% / 75.0% |' "$tmp/summary.md"
grep -Fq '| Stream stability | Top-20% pass rate below unseen-candidate baseline | 0.20000 / 0.19000 | 70.0% / 80.0% |' "$tmp/summary.md"
grep -Fq '| End-to-end | Not promoted: insufficient labelled data | n/a / n/a | n/a / n/a |' "$tmp/summary.md"
grep -Fq '**Recency strategy selection:** Strict validation — Recency-weighted · Inner holdout Brier (unweighted → weighted) 0.13000 → 0.10000 · Inner holdout Top-20% pass rate 70.0% → 80.0% · 3-day half-life, minimum weight 0.1.' "$tmp/summary.md"
grep -Fq '**Training data:** 50,000 rows (278 new this run) · 4,044 unique candidates · 177 runs. Strict model: Ready · End-to-end model: Not ready.' "$tmp/summary.md"

if grep -Eq 'walk_forward_time_split|training_prevalence_brier|model_did_not_beat_temporal_baseline|top_20pct_below_random_selection_baseline|\| Transfer \(10 MiB total\)' "$tmp/summary.md"; then
  echo "[ERROR] Summary contains raw internal identifiers or a legacy target label." >&2
  exit 1
fi

echo "[OK] Update summary uses concise human-readable LightGBM and publication metrics."

