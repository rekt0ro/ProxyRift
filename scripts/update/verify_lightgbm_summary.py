#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

from lightgbm_report import (
    format_count,
    format_decision,
    format_evaluation,
    format_score_coverage,
    is_number,
    load_json,
    nested_value,
    validate_score_document,
)


def verify(summary_path: Path, score_path: Path, stats_path: Path) -> None:
    if not summary_path.is_file() or summary_path.stat().st_size == 0 or not score_path.is_file() or score_path.stat().st_size == 0:
        raise ValueError("Summary or LightGBM score report is missing.")
    score = load_json(score_path)
    if not validate_score_document(score):
        raise ValueError("LightGBM score report does not use the validated stream-speed schema.")

    report = score["model_report"]
    trained = str(score.get("trained", False)).lower()
    strategy = nested_value(score, "model_report.ranking_strategy", "neutral_default")
    mode = "Speed model" if trained == "true" else (
        "Recent speed estimates" if strategy == "historical_stream_speed_prior_fallback" else "Neutral ranking"
    )
    speed_samples = format_count(nested_value(score, "model_report.speed_samples"))
    unique_candidates = format_count(nested_value(score, "model_report.unique_candidates"))
    unique_timestamps = format_count(nested_value(score, "model_report.unique_timestamps"))
    train_rows = nested_value(score, "model_report.train_rows")
    holdout_rows = nested_value(score, "model_report.holdout_rows")
    model_mae = nested_value(score, "model_report.model_log_mae")
    baseline_mae = nested_value(score, "model_report.baseline_log_mae")
    improvement = nested_value(score, "model_report.relative_mae_improvement")
    speed_lift = nested_value(score, "model_report.top_20pct_speed_lift")
    stats = load_json(stats_path, {})
    new_samples = format_count(nested_value(stats, "dataset.new_speed_samples"))
    candidate_count = nested_value(score, "model_report.candidate_count")
    scored_count = nested_value(score, "model_report.scored_candidate_count")
    minimum_lift = nested_value(score, "model_report.minimum_top_20pct_speed_lift")
    if not is_number(minimum_lift):
        minimum_lift = "1.05"

    expected = [
        f"| Ranking mode | {mode} |",
        f"| Candidate score coverage | {format_score_coverage(scored_count, candidate_count)} |",
        (
            f"| Recent speed data | {speed_samples} speed measurements in the last 72 hours; {new_samples} new this run; "
            f"{unique_candidates} candidate identities across {unique_timestamps} timestamps |"
        ),
    ]
    if train_rows.isdigit() and holdout_rows.isdigit() and (int(train_rows) > 0 or int(holdout_rows) > 0):
        expected.append(f"| Temporal validation split | {format_count(train_rows)} training rows / {format_count(holdout_rows)} holdout rows |")
    else:
        expected.append("| Temporal validation split | Not available — validation data requirements were not met |")
    evaluation = format_evaluation(
        report.get("reason", "not_evaluated"), model_mae, baseline_mae, improvement,
        speed_lift, nested_value(score, "model_report.speed_samples"),
        nested_value(score, "model_report.unique_candidates"), nested_value(score, "model_report.unique_timestamps"),
        train_rows, holdout_rows, minimum_lift,
    )
    decision = format_decision(report.get("accepted", False), report.get("reason", "not_evaluated"), minimum_lift)
    expected.extend([f"| Evaluation | {evaluation} |", f"| Decision | {decision} |"])
    lines = summary_path.read_text(encoding="utf-8").splitlines()
    for row in expected:
        if row not in lines:
            raise ValueError(f"LightGBM summary row does not match the score report. Expected: {row}")


def main() -> int:
    if len(sys.argv) != 4:
        print(f"[ERROR] Usage: {Path(sys.argv[0]).name} SUMMARY_FILE SCORE_FILE TRAINING_STATS_FILE", file=sys.stderr)
        return 2
    try:
        verify(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]))
    except (OSError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print("[OK] LightGBM summary matches the score report and artifact evaluation reason.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
