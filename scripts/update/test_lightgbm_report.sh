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
      "transfer": {"accepted": false},
      "stream": {
        "accepted": false,
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
      "end_to_end": {"accepted": false}
    }
  }
}
JSON

cat > "$summary_file" <<'MD'
| Model-based ranking active | Stage-model blend fallback | 5000 | 66 | 1 | 1 / 4 |
| Strict validation | Recency-weighted | Yes | Yes | 0.14 | 0.13 | 0.9 | 0.91 | 3 | 0.1 |
| Transfer | Not evaluated | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| Stream stability | Neither variant passed promotion gates | No | No | 0.3 | 0.31 | 0.5 | 0.5 | 3 | 0.1 |
| End-to-end | Not evaluated | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
MD

bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$summary_file" "$score_file"
bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$score_file" "$artifact_file"
jq -e '.available == true and .scored_candidates == 1 and (has("scores") | not) and .model_report.targets.strict.recency_weighting_experiment.selected_strategy == "recency_weighted"' "$artifact_file" >/dev/null
if grep -Fq 'private-candidate' "$artifact_file"; then
  printf '[ERROR] Artifact contains a private candidate string.\n' >&2
  exit 1
fi

sed 's/| Strict validation | Recency-weighted | Yes | Yes | 0.14 |/| Strict validation | Recency-weighted | Yes | Yes | 9.99 |/' "$summary_file" > "$tmp/mismatch.md"
if bash "$repo_root/scripts/update/verify_lightgbm_summary.sh" "$tmp/mismatch.md" "$score_file" >/dev/null 2>&1; then
  printf '[ERROR] Summary validator accepted a mismatched metric.\n' >&2
  exit 1
fi

bash "$repo_root/scripts/update/write_lightgbm_artifact.sh" "$tmp/missing.json" "$tmp/unavailable.json"
jq -e '.available == false and (has("scores") | not)' "$tmp/unavailable.json" >/dev/null
printf '[OK] LightGBM report verification and artifact sanitization tests passed.\n'
