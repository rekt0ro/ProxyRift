#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/update/write_run_summary.py"


class RunSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.work = self.root / "work"
        self.subscriptions = self.work / "subscriptions"
        self.temp_data = self.work / "proxyrift"
        self.bin = self.root / "bin"
        self.subscriptions.mkdir(parents=True)
        self.temp_data.mkdir()
        self.bin.mkdir()
        (self.subscriptions / "all.txt").write_text("vless://all@example.com:443\n", encoding="utf-8")
        (self.subscriptions / "light.txt").write_text("vless://light@example.net:443\n", encoding="utf-8")
        (self.temp_data / "light-stats.json").write_text(json.dumps({
            "input_candidates": 20000, "strict_verified": 300, "strict_attempts": 317,
            "selection_target": 200, "transfer_stability_tested": 232, "transfer_stability_passed": 220,
            "transfer_tested": 232, "transfer_passed": 211, "stream_continuity_tested": 232,
            "stream_continuity_passed": 211, "published": 200, "fresh_family_target_percent": 70,
            "published_from_fresh_families": 140, "published_new_candidates": 100,
            "published_expired_candidates": 60, "published_revalidated": 40,
            "revalidated_candidate_max_percent": 20,
        }), encoding="utf-8")
        (self.temp_data / "lightgbm-scores.json").write_text(json.dumps({
            "version": 5, "trained": True, "training_rows": 1200, "feature_count": 61,
            "scores": {"candidate-private-uri": 0.8},
            "model_report": {
                "ranking_strategy": "stream_speed_regression", "target": "log(stream_throughput_kbps)",
                "accepted": True, "reason": "promoted_after_temporal_speed_validation",
                "speed_samples": 1200, "unique_candidates": 1100, "candidate_count": 200,
                "scored_candidate_count": 200, "unique_timestamps": 15, "train_rows": 960, "holdout_rows": 240,
                "model_log_mae": 0.1, "baseline_log_mae": 0.13, "relative_mae_improvement": 0.2307692,
                "top_20pct_speed_lift": 1.24, "minimum_top_20pct_speed_lift": 1.05,
            },
        }), encoding="utf-8")
        (self.subscriptions / "light-training-stats.json").write_text(
            json.dumps({"dataset": {"speed_samples": 1200, "new_speed_samples": 120}}), encoding="utf-8"
        )
        gh_stub = self.bin / "gh"
        gh_stub.write_text(
            "#!/usr/bin/env python3\nimport sys\n"
            "print('{\"jobs\": []}' if 'jobs' in sys.argv[-1] else '{\"workflow_runs\": []}')\n",
            encoding="utf-8",
        )
        gh_stub.chmod(0o755)
        self.summary_file = self.root / "step-summary.md"
        self.env = os.environ.copy()
        self.env.update({
            "PATH": f"{self.bin}:{self.env.get('PATH', '')}",
            "PROXYRIFT_TMP_DIR": str(self.temp_data),
            "GH_REPOSITORY": "test/repo",
            "GITHUB_RUN_ID": "1", "GITHUB_RUN_NUMBER": "1",
            "JOB_STATUS": "success", "DISCOVERY_OUTCOME": "success", "COLLECT_OUTCOME": "success",
            "LIGHT_OUTCOME": "success", "GENERATION_OUTCOME": "success", "PREPARE_OUTCOME": "success",
            "PUBLISH_OUTCOME": "success", "XRAY_VERSION": "test", "SINGBOX_VERSION": "test",
            "MIHOMO_VERSION": "test", "GITHUB_STEP_SUMMARY": str(self.summary_file),
        })

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def generate(self) -> str:
        result = subprocess.run(
            [sys.executable, str(SCRIPT)], cwd=self.work, env=self.env,
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report_path = self.temp_data / "run-summary.md"
        self.assertTrue(report_path.is_file())
        self.assertEqual(self.summary_file.read_text(encoding="utf-8"), report_path.read_text(encoding="utf-8"))
        return report_path.read_text(encoding="utf-8")

    def test_summary_includes_validation_selection_and_model_details(self) -> None:
        summary = self.generate()
        for expected in (
            "| Candidates received | 20,000 |",
            "| Strict validation | 300 / 317 |",
            "| 1 MiB stability | 220 / 232 |",
            "| 9 MiB sustained stream | 211 / 232 |",
            "| Candidate score coverage | 200 / 200 candidates scored (100.0%; complete) |",
            "| New candidates (not in retained history) | 100 |",
            "| Previously known; 24-hour revalidation expired | 60 |",
            "| Recently reused (validated within 24 hours) | 40 of 200 (maximum 20%) |",
            "| Configs from unseen families | 140 of 200 (70.0%; target 70%) |",
            "| Light published | 200 of 200 target |",
            "| Ranking mode | Speed model |",
            "| Recent speed data | 1,200 speed measurements in the last 72 hours; 120 new this run; 1,100 candidate identities across 15 timestamps |",
            "| Temporal validation split | 960 training rows / 240 holdout rows |",
            "| Evaluation | Evaluated — model MAE 0.100 vs baseline 0.130 (23.1% lower); top-20% holdout speed lift 1.24x (minimum 1.05x). |",
            "| Decision | Active |",
            "| test | test | test |",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, summary)
        self.assertNotIn("| Sustained stream |", summary)
        self.assertNotIn("candidate-private-uri", summary)

    def test_fallback_model_is_not_described_as_evaluated(self) -> None:
        path = self.temp_data / "lightgbm-scores.json"
        score = json.loads(path.read_text(encoding="utf-8"))
        report = score["model_report"]
        score["trained"] = False
        report.update({
            "ranking_strategy": "historical_stream_speed_prior_fallback",
            "accepted": False,
            "reason": "insufficient_speed_samples_or_temporal_coverage",
            "speed_samples": 20, "unique_candidates": 12, "unique_timestamps": 1,
            "baseline_log_mae": None, "model_log_mae": None, "relative_mae_improvement": None,
            "top_20pct_speed_lift": None, "train_rows": 0, "holdout_rows": 0,
        })
        path.write_text(json.dumps(score), encoding="utf-8")
        stats = self.subscriptions / "light-training-stats.json"
        stats.write_text(json.dumps({"dataset": {"speed_samples": 20, "new_speed_samples": 8}}), encoding="utf-8")
        summary = self.generate()
        self.assertIn("| Ranking mode | Recent speed estimates |", summary)
        self.assertIn("| Temporal validation split | Not available — validation data requirements were not met |", summary)
        self.assertIn("| Evaluation | Not evaluated — insufficient speed samples or temporal coverage", summary)
        self.assertIn("| Decision | Not enabled — collecting more recent speed measurements |", summary)


if __name__ == "__main__":
    unittest.main()
