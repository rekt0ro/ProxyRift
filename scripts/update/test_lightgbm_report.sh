#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

score_file="$tmp/lightgbm-scores.json"
summary_file="$tmp/summary.md"
artifact_file="$tmp/lightgbm-evaluation.json"

cat > "$score_file" <<'JSON'
{
  "version": 4,
  "trained": true,
  "training_rows": 5000,
  "feature_count": 66,
  "scores": {"vless://private-candidate@example.invalid:443": 0.75},
  "model_report": {
    "validation": "walk_forward_time_split",
    "baseline": "training_prevalence_brier",
    "ranking_strategy": "stage_blend_fallback",
    "model_feature_count": 66,
    "targets": {
      "strict": {
        "accepted": true,
        "reason": "promoted_after_temporal_validation",
        "model_brier": 0.14,
        "baseline_brier": 0.15,
        "model_top_20pct_pass_rate": 0.9,
        "holdout_pass_rate": 0.8,
        "recency_weighting_experiment": {
          "half_life_days": 3,
          "minimum_sample_weight": 0.1,
          "unweighted_brier": 0.14,
          "weighted_brier": 0.13,
          "unweighted_accepted": true,
          "weighted_accepted": true,
          "unweighted_top_20pct_pass_rate": 0.9,
          "weighted_top_20pct_pass_rate": 0.91,
          "selected_strategy": "recency_weighted"
        }
      },
      "transfer": {"accepted": false, "reason": "model_did_not_beat_unseen_candidate_baseline", "model_brier": 0.22, "baseline_brier": 0.20, "model_top_20pct_pass_rate": 0.7, "holdout_pass_rate": 0.75},
      "stream": {
        "accepted": false,
        "reason": "top_20pct_below_unseen_candidate_baseline",
        "model_brier": 0.3,
        "baseline_brier": 0.31,
        "model_top_20pct_pass_rate": 0.5,
        "holdout_pass_rate": 0.5,
        "recency_weighting_experiment": {
          "half_life_days": 3,
          "minimum_sample_weight": 0.1,
          "unweighted_brier": 0.3,
          "weighted_brier": 0.31,
          "unweighted_accepted": false,
          "weighted_accepted": false,
          "unweighted_top_20pct_pass_rate": 0.5,
          "weighted_top_20pct_pass_rate": 0.5,
          "selected_strategy": "none"
        }
      },
      "end_to_end": {"accepted": false, "reason": "insufficient labelled data (rows=100, pass=0, fail=100)", "model_brier": null, "baseline_brier": null, "model_top_20pct_pass_rate": null, "holdout_pass_rate": null}
    }
  }
}
JSON

cat > "$summary_file" <<'MD'
**Ranking:** Model-based ranking active — Stage-model blend (1 of 4 targets accepted).
**Training:** 5,000 rows · 66 features · Candidates scored: 1.
**Evaluation:** Walk-forward temporal split · Baseline: Training-prevalence Brier score.
| Strict validation | Promoted after temporal validation | 0.14000 / 0.15000 | 90.0% / 80.0% |
| Transfer (10 MiB) | Did not beat unseen-candidate baseline | 0.22000 / 0.20000 | 70.0% / 75.0% |
| Stream stability | Top-20% pass rate below unseen-candidate baseline | 0.30000 / 0.31000 | 50.0% / 50.0% |
| End-to-end | Not promoted: insufficient labelled data | n/a / n/a | n/a / n/a |
**Recency weighting:** Strict validation — Recency-weighted · Brier 0.14000 → 0.13000 · Top-20% pass rate 90.0% → 91.0% · 3-day half-life, minimum weight 0.1.
MD

bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$summary_file" "$score_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$artifact_file"
jq -e '.available == true and .scored_candidates == 1 and (has("scores") | not) and .model_report.targets.strict.recency_weighting_experiment.selected_strategy == "recency_weighted"' "$artifact_file" >/dev/null
if grep -Fq 'private-candidate' "$artifact_file"; then
  printf '[ERROR] Artifact contains a private candidate string.\n' >&2
  exit 1
fi

sed 's/| Strict validation | Promoted after temporal validation | 0.14000 \/ 0.15000 |/| Strict validation | Promoted after temporal validation | 9.99000 \/ 0.15000 |/' "$summary_file" > "$tmp/mismatch.md"
if bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/mismatch.md" "$score_file" >/dev/null 2>&1; then
  printf '[ERROR] Summary validator accepted a mismatched metric.\n' >&2
  exit 1
fi

bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$tmp/missing.json" "$tmp/unavailable.json"
jq -e '.available == false and (has("scores") | not)' "$tmp/unavailable.json" >/dev/null
printf '[OK] LightGBM report verification and artifact sanitization tests passed.\n'
