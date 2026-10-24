#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERIFY = ROOT / "scripts/update/verify_lightgbm_summary.py"
ARTIFACT = ROOT / "scripts/update/write_lightgbm_artifact.py"


def score_report() -> dict:
    return {
        "version": 5,
        "trained": True,
        "training_rows": 100,
        "feature_count": 61,
        "scores": {"vless://private-candidate@example.invalid:443": 0.8},
        "model_report": {
            "model": "LightGBM GBDT regression",
            "validation": "chronological_80_20_first_observation_of_unseen_candidates",
            "baseline": "training_median_log_stream_throughput",
            "ranking_strategy": "stream_speed_regression",
            "target": "log(stream_throughput_kbps)",
            "target_unit": "kbps",
            "accepted": True,
            "reason": "promoted_after_temporal_speed_validation",
            "cutoff_timestamp": 1800000000,
            "speed_samples": 100,
            "unique_candidates": 100,
            "candidate_count": 1,
            "scored_candidate_count": 1,
            "unique_timestamps": 15,
            "train_rows": 80,
            "holdout_rows": 20,
            "baseline_log_mae": 0.12,
            "model_log_mae": 0.1,
            "relative_mae_improvement": 0.1666667,
            "top_20pct_speed_lift": 1.25,
            "minimum_relative_mae_improvement": 0.01,
            "minimum_top_20pct_speed_lift": 1.05,
        },
    }


def summary_for(score: dict, new_samples: int = 12) -> str:
    report = score["model_report"]
    if score.get("trained", False):
        mode = "Speed model"
    elif report.get("ranking_strategy") == "historical_stream_speed_prior_fallback":
        mode = "Recent speed estimates"
    else:
        mode = "Neutral ranking"
    coverage = "1 / 1 candidates scored (100.0%; complete)"
    speed_samples = report.get("speed_samples", "n/a")
    candidates = report.get("unique_candidates", "n/a")
    timestamps = report.get("unique_timestamps", "n/a")
    train_rows = report.get("train_rows", 0)
    holdout_rows = report.get("holdout_rows", 0)
    split = f"{train_rows} training rows / {holdout_rows} holdout rows" if train_rows or holdout_rows else "Not available — validation data requirements were not met"
    reason = report.get("reason", "not_evaluated")
    numeric = all(isinstance(report.get(key), (int, float)) for key in ("model_log_mae", "baseline_log_mae", "relative_mae_improvement"))
    if numeric:
        change = report["relative_mae_improvement"] * 100
        change_text = f"{change:.1f}% lower" if change >= 0 else f"{-change:.1f}% higher"
        evaluation = (
            f"Evaluated — model MAE {report['model_log_mae']:.3f} vs baseline {report['baseline_log_mae']:.3f} "
            f"({change_text}); top-20% holdout speed lift {report.get('top_20pct_speed_lift', 'n/a'):.2f}x "
            f"(minimum {report.get('minimum_top_20pct_speed_lift', 1.05):.2f}x)."
        )
    elif reason == "insufficient_speed_samples_or_temporal_coverage":
        evaluation = f"Not evaluated — insufficient speed samples or temporal coverage (samples: {speed_samples}; candidates: {candidates}; timestamps: {timestamps})."
    else:
        evaluation = f"Not evaluated — {reason.replace('_', ' ')}."
    if report.get("accepted"):
        decision = "Active"
    elif reason == "insufficient_speed_samples_or_temporal_coverage":
        decision = "Not enabled — collecting more recent speed measurements"
    elif reason == "model_did_not_beat_temporal_speed_baseline":
        decision = "Not enabled — MAE did not improve on the temporal baseline by at least 1%"
    elif reason == "top_20pct_speed_lift_below_minimum":
        decision = "Not enabled — top-20% holdout speed lift did not reach the required 1.05x minimum"
    else:
        decision = "Not evaluated"
    return "\n".join([
        f"| Ranking mode | {mode} |",
        f"| Candidate score coverage | {coverage} |",
        f"| Recent speed data | {speed_samples} speed measurements in the last 72 hours; {new_samples} new this run; {candidates} candidate identities across {timestamps} timestamps |",
        f"| Temporal validation split | {split} |",
        f"| Evaluation | {evaluation} |",
        f"| Decision | {decision} |",
    ]) + "\n"


class LightGBMReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.score_path = self.directory / "scores.json"
        self.stats_path = self.directory / "stats.json"
        self.summary_path = self.directory / "summary.md"
        self.artifact_path = self.directory / "artifact.json"
        self.score = score_report()
        self.score_path.write_text(json.dumps(self.score), encoding="utf-8")
        self.stats_path.write_text(json.dumps({"dataset": {"new_speed_samples": 12}}), encoding="utf-8")
        self.summary_path.write_text(summary_for(self.score), encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_verify(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(VERIFY), str(self.summary_path), str(self.score_path), str(self.stats_path)],
            text=True, capture_output=True, check=False,
        )

    def run_artifact(self, score: Path | None = None, output: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(ARTIFACT), str(score or self.score_path), str(output or self.artifact_path)],
            text=True, capture_output=True, check=False,
        )

    def test_valid_summary_matches_report_and_artifact_is_sanitized(self) -> None:
        verified = self.run_verify()
        self.assertEqual(verified.returncode, 0, verified.stderr)
        produced = self.run_artifact()
        self.assertEqual(produced.returncode, 0, produced.stderr)
        artifact = json.loads(self.artifact_path.read_text(encoding="utf-8"))
        encoded = self.artifact_path.read_text(encoding="utf-8")
        self.assertTrue(artifact["available"])
        self.assertEqual(artifact["scored_candidates"], 1)
        self.assertNotIn("scores", artifact)
        self.assertNotIn("private-candidate", encoded)
        self.assertNotIn("example.invalid", encoded)
        self.assertEqual(artifact["model_report"]["candidate_count"], 1)

    def test_history_fallback_is_reported(self) -> None:
        self.score["trained"] = False
        report = self.score["model_report"]
        report.update({
            "ranking_strategy": "historical_stream_speed_prior_fallback",
            "accepted": False,
            "reason": "insufficient_speed_samples_or_temporal_coverage",
            "speed_samples": 20,
            "unique_candidates": 12,
            "unique_timestamps": 1,
            "baseline_log_mae": None,
            "model_log_mae": None,
            "relative_mae_improvement": None,
            "top_20pct_speed_lift": None,
            "train_rows": 0,
            "holdout_rows": 0,
        })
        self.score_path.write_text(json.dumps(self.score), encoding="utf-8")
        self.summary_path.write_text("\n".join([
            "| Ranking mode | Recent speed estimates |",
            "| Candidate score coverage | 1 / 1 candidates scored (100.0%; complete) |",
            "| Recent speed data | 20 speed measurements in the last 72 hours; 12 new this run; 12 candidate identities across 1 timestamps |",
            "| Temporal validation split | Not available — validation data requirements were not met |",
            "| Evaluation | Not evaluated — insufficient speed samples or temporal coverage (samples: 20; candidates: 12; timestamps: 1). |",
            "| Decision | Not enabled — collecting more recent speed measurements |",
        ]) + "\n", encoding="utf-8")
        verified = self.run_verify()
        self.assertEqual(verified.returncode, 0, verified.stderr)

    def test_mismatched_summary_is_rejected(self) -> None:
        self.summary_path.write_text(self.summary_path.read_text().replace("0.100 vs baseline 0.120", "9.990 vs baseline 0.120"), encoding="utf-8")
        verified = self.run_verify()
        self.assertNotEqual(verified.returncode, 0)
        self.assertIn("does not match", verified.stderr)

    def test_missing_score_file_produces_unavailable_artifact(self) -> None:
        missing = self.directory / "missing.json"
        produced = self.run_artifact(missing)
        self.assertEqual(produced.returncode, 0, produced.stderr)
        artifact = json.loads(self.artifact_path.read_text(encoding="utf-8"))
        self.assertFalse(artifact["available"])
        self.assertNotIn("scores", artifact)

    def test_invalid_score_schema_is_rejected(self) -> None:
        self.score["version"] = 4
        self.score_path.write_text(json.dumps(self.score), encoding="utf-8")
        produced = self.run_artifact()
        self.assertEqual(produced.returncode, 1)
        self.assertIn("invalid stream-speed regression schema", produced.stderr)


if __name__ == "__main__":
    unittest.main()
