#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from lightgbm_report import (
    format_count,
    format_decimal,
    format_mae_change,
    format_multiplier,
    format_score_coverage,
    nested_value,
)

UNKNOWN = "unknown"


def stage_status(outcome: str) -> str:
    return {
        "success": "Complete",
        "failure": "Failed",
        "cancelled": "Cancelled",
        "skipped": "Skipped",
    }.get(outcome, "Unknown")


def count_configs(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return sum(1 for line in handle if line.strip() and not line.startswith("#"))


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def value(data: Any, dotted_path: str, fallback: str = "n/a") -> str:
    return nested_value(data, dotted_path, fallback)


def format_ranking_strategy(strategy: str) -> str:
    return {
        "stream_speed_regression": "Speed model",
        "historical_stream_speed_prior_fallback": "Recent speed estimates",
    }.get(strategy, "Neutral ranking")


def format_percentage(value_text: str) -> str:
    if not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", value_text):
        return value_text
    return f"{float(value_text) * 100:.1f}%"


def format_compact_number(value_text: str) -> str:
    if not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", value_text):
        return value_text
    number = float(value_text)
    return str(int(number)) if number.is_integer() else f"{number:g}"


def format_model_decision(accepted: str, reason: str, minimum_lift: str) -> str:
    if accepted.lower() == "true":
        return "Active"
    messages = {
        "insufficient_speed_samples_or_temporal_coverage": "Not enabled — collecting more recent speed measurements",
        "insufficient_distinct_timestamps": "Not enabled — not enough separate test runs",
        "insufficient_unseen_candidate_holdout": "Not enabled — not enough unseen configs to validate",
        "model_did_not_beat_temporal_speed_baseline": "Not enabled — MAE did not improve on the temporal baseline by at least 1%",
        "top_20pct_speed_ranking_did_not_improve": "Not enabled — top-20% speed lift did not exceed the baseline",
        "final_model_fit_failed": "Not enabled — model training failed",
        "holdout_model_fit_failed": "Not enabled — model training failed",
    }
    if reason == "top_20pct_speed_lift_below_minimum":
        return f"Not enabled — top-20% holdout speed lift did not reach the required {format_multiplier(minimum_lift)} minimum"
    if reason in messages:
        return messages[reason]
    return f"Not evaluated — {reason.replace('_', ' ') or 'no evaluation report'}."


def format_model_evaluation(
    reason: str,
    model_mae: str,
    baseline_mae: str,
    improvement: str,
    speed_lift: str,
    speed_samples: str,
    unique_candidates: str,
    unique_timestamps: str,
    train_rows: str,
    holdout_rows: str,
    minimum_lift: str,
) -> str:
    number = re.compile(r"-?[0-9]+(?:\.[0-9]+)?")
    if all(number.fullmatch(item) for item in (model_mae, baseline_mae, improvement)):
        return (
            f"Evaluated — model MAE {format_decimal(model_mae)} vs baseline {format_decimal(baseline_mae)} "
            f"({format_mae_change(improvement)}); top-20% holdout speed lift {format_multiplier(speed_lift)} "
            f"(minimum {format_multiplier(minimum_lift)})."
        )
    if reason == "insufficient_speed_samples_or_temporal_coverage":
        return (
            "Not evaluated — insufficient speed samples or temporal coverage "
            f"(samples: {format_count(speed_samples)}; candidates: {format_count(unique_candidates)}; "
            f"timestamps: {format_count(unique_timestamps)})."
        )
    if reason == "insufficient_distinct_timestamps":
        return f"Not evaluated — fewer than two distinct measurement timestamps (found {format_count(unique_timestamps)})."
    if reason == "insufficient_unseen_candidate_holdout":
        return (
            "Not evaluated — insufficient unseen-candidate holdout "
            f"(train rows: {format_count(train_rows)}; holdout rows: {format_count(holdout_rows)})."
        )
    if reason == "model_did_not_beat_temporal_speed_baseline":
        return "Metrics unavailable, but the artifact reports that the model did not beat the temporal baseline."
    if reason == "top_20pct_speed_lift_below_minimum":
        return "Metrics unavailable, but the artifact reports that the top-20% holdout speed lift did not reach the required minimum."
    if reason == "top_20pct_speed_ranking_did_not_improve":
        return "Metrics unavailable, but the artifact reports that the top-20% speed ranking did not improve."
    if reason in ("final_model_fit_failed", "holdout_model_fit_failed"):
        return "Not evaluated — model fitting failed."
    return f"Not evaluated — {reason.replace('_', ' ') or 'no evaluation report'}."


def parse_timestamp(text: Any) -> int | None:
    if not isinstance(text, str) or not text:
        return None
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        return int(datetime.fromisoformat(normalized).timestamp())
    except (ValueError, OverflowError):
        return None


def gh_json(repository: str, *args: str) -> Any:
    try:
        result = subprocess.run(
            ["gh", "api", "--method", "GET", *args],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
            env=os.environ.copy(),
        )
    except OSError:
        return None
    if result.returncode:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def step_elapsed(jobs: Any, step_name: str) -> str:
    if not isinstance(jobs, dict):
        return UNKNOWN
    job_rows = jobs.get("jobs")
    if not isinstance(job_rows, list):
        return UNKNOWN
    for job in job_rows:
        if not isinstance(job, dict):
            continue
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict) or step.get("name") != step_name:
                continue
            start = parse_timestamp(step.get("started_at"))
            end = parse_timestamp(step.get("completed_at"))
            if start is not None and end is not None:
                return str(end - start)
    return UNKNOWN


def job_started_and_publish_completed(jobs: Any) -> tuple[Any, Any]:
    if not isinstance(jobs, dict) or not isinstance(jobs.get("jobs"), list):
        return "", ""
    for job in jobs["jobs"]:
        if not isinstance(job, dict) or job.get("name") != "Collect & Validate":
            continue
        started = job.get("started_at") or ""
        completed = ""
        for step in job.get("steps", []):
            if isinstance(step, dict) and step.get("name") == "Publish generated changes":
                completed = step.get("completed_at") or ""
                break
        return started, completed
    return "", ""


def format_minutes(seconds: str | int) -> str:
    text = str(seconds)
    if re.fullmatch(r"[0-9]+", text):
        return f"{int(text) / 60:.1f} min"
    return UNKNOWN


def format_delta(current: str | int, previous: str | int) -> str:
    current_text, previous_text = str(current), str(previous)
    if re.fullmatch(r"[0-9]+", current_text) and re.fullmatch(r"[0-9]+", previous_text):
        delta = int(current_text) - int(previous_text)
        pct = (delta / int(previous_text) * 100) if int(previous_text) > 0 else 0
        return f"{delta / 60:+.1f} min ({pct:+.1f}%)"
    return UNKNOWN


def make_summary(env: dict[str, str], workdir: Path) -> str:
    tmp_dir = Path(env.get("PROXYRIFT_TMP_DIR", "/tmp/proxyrift"))
    discovery = env.get("DISCOVERY_OUTCOME", "")
    collect = env.get("COLLECT_OUTCOME", "")
    light = env.get("LIGHT_OUTCOME", "")
    generation = env.get("GENERATION_OUTCOME", "")
    prepare = env.get("PREPARE_OUTCOME", "")
    publish = env.get("PUBLISH_OUTCOME", "")
    overall_status = stage_status(env.get("JOB_STATUS", ""))

    collection_status = stage_status(discovery)
    if collection_status == "Complete" and collect != "success":
        collection_status = stage_status(collect)
    if collection_status == "Complete" and light != "success":
        collection_status = stage_status(light)

    generation_status = stage_status(generation)
    if generation_status == "Complete" and prepare != "success":
        generation_status = stage_status(prepare)
    publish_status = stage_status(publish)

    tool_versions = (
        env.get("XRAY_VERSION", "unknown"),
        env.get("SINGBOX_VERSION", "unknown"),
        env.get("MIHOMO_VERSION", "unknown"),
    )
    subscriptions = workdir / "subscriptions"
    all_count = count_configs(subscriptions / "all.txt")
    light_count = count_configs(subscriptions / "light.txt")

    score_path = tmp_dir / "lightgbm-scores.json"
    score_doc = read_json(score_path)
    training_stats = read_json(subscriptions / "light-training-stats.json")
    model_report = score_doc.get("model_report", {}) if isinstance(score_doc, dict) else {}
    if not isinstance(model_report, dict):
        model_report = {}
    trained = str(score_doc.get("trained", False)).lower() if isinstance(score_doc, dict) else "false"
    raw_strategy = str(model_report.get("ranking_strategy", "neutral_default"))
    ranking_strategy = "stream_speed_regression" if trained == "true" else raw_strategy

    ml_rows = value(training_stats, "dataset.speed_samples") if training_stats else "n/a"
    ml_new_rows = value(training_stats, "dataset.new_speed_samples") if training_stats else "n/a"
    speed_samples = value(score_doc, "model_report.speed_samples", "n/a")
    unique_candidates = value(score_doc, "model_report.unique_candidates", "n/a")
    unique_timestamps = value(score_doc, "model_report.unique_timestamps", "n/a")
    train_rows = value(score_doc, "model_report.train_rows", "n/a")
    holdout_rows = value(score_doc, "model_report.holdout_rows", "n/a")
    candidate_count = value(score_doc, "model_report.candidate_count", "n/a")
    scored_candidate_count = value(score_doc, "model_report.scored_candidate_count", "n/a")
    model_mae = value(score_doc, "model_report.model_log_mae", "n/a")
    baseline_mae = value(score_doc, "model_report.baseline_log_mae", "n/a")
    improvement = value(score_doc, "model_report.relative_mae_improvement", "n/a")
    speed_lift = value(score_doc, "model_report.top_20pct_speed_lift", "n/a")
    accepted = value(score_doc, "model_report.accepted", "false")
    reason = value(score_doc, "model_report.reason", "not_evaluated")
    minimum_lift = value(score_doc, "model_report.minimum_top_20pct_speed_lift", "1.05")

    light_stats = read_json(tmp_dir / "light-stats.json")
    metric_names = (
        "input_candidates", "full_path_verified", "full_path_tested", "selection_target",
        "transfer_stability_tested", "transfer_stability_passed", "transfer_tested",
        "transfer_passed", "stream_continuity_tested", "stream_continuity_passed",
        "published", "fresh_family_target_percent", "published_new_candidates",
        "published_expired_candidates", "published_revalidated",
        "revalidated_candidate_max_percent", "published_from_fresh_families",
    )
    metrics = {key: value(light_stats, key, "unknown") for key in metric_names}
    fresh_actual = "n/a"
    if metrics["published_from_fresh_families"].isdigit() and metrics["published"].isdigit() and int(metrics["published"]) > 0:
        fresh_actual = f"{int(metrics['published_from_fresh_families']) / int(metrics['published']) * 100:.1f}%"

    repository = env.get("GH_REPOSITORY", "")
    run_id = env.get("GITHUB_RUN_ID", "")
    run_number_text = env.get("GITHUB_RUN_NUMBER", "")
    try:
        run_number = int(run_number_text)
    except ValueError:
        run_number = 0
    current_jobs = gh_json(repository, f"/repos/{repository}/actions/runs/{run_id}/jobs?per_page=100") if repository and run_id else None
    collect_elapsed = step_elapsed(current_jobs, "Collect & transport-test configs")
    light_elapsed = step_elapsed(current_jobs, "Build Light subscription")
    current_job_started, current_publish_completed = job_started_and_publish_completed(current_jobs)
    pipeline_elapsed = UNKNOWN
    start_epoch = parse_timestamp(current_job_started)
    if start_epoch is not None:
        end_epoch = parse_timestamp(current_publish_completed)
        if end_epoch is None:
            end_epoch = int(datetime.now().timestamp())
        pipeline_elapsed = str(end_epoch - start_epoch)

    previous_run_id = ""
    previous_run_number = ""
    previous_total_elapsed = UNKNOWN
    previous_collect_elapsed = UNKNOWN
    previous_light_elapsed = UNKNOWN
    runs = gh_json(repository, f"/repos/{repository}/actions/workflows/update.yml/runs?per_page=30") if repository and run_number else None
    if isinstance(runs, dict) and isinstance(runs.get("workflow_runs"), list):
        previous = [
            item for item in runs["workflow_runs"]
            if isinstance(item, dict)
            and item.get("status") == "completed"
            and isinstance(item.get("run_number"), int)
            and item["run_number"] < run_number
        ]
        previous.sort(key=lambda item: item["run_number"])
        if previous:
            previous_run_id = str(previous[-1].get("id", ""))
            previous_run_number = str(previous[-1].get("run_number", ""))
    if previous_run_id:
        previous_jobs = gh_json(repository, f"/repos/{repository}/actions/runs/{previous_run_id}/jobs?per_page=100")
        previous_collect_elapsed = step_elapsed(previous_jobs, "Collect & transport-test configs")
        previous_light_elapsed = step_elapsed(previous_jobs, "Build Light subscription")
        previous_started, previous_published = job_started_and_publish_completed(previous_jobs)
        previous_start_epoch = parse_timestamp(previous_started)
        previous_end_epoch = parse_timestamp(previous_published)
        if previous_start_epoch is not None and previous_end_epoch is not None:
            previous_total_elapsed = str(previous_end_epoch - previous_start_epoch)

    current_total_display = format_minutes(pipeline_elapsed)
    current_collect_display = format_minutes(collect_elapsed)
    current_light_display = format_minutes(light_elapsed)
    previous_total_display = format_minutes(previous_total_elapsed)
    previous_collect_display = format_minutes(previous_collect_elapsed)
    previous_light_display = format_minutes(previous_light_elapsed)
    total_delta_display = format_delta(pipeline_elapsed, previous_total_elapsed)
    collect_delta_display = format_delta(collect_elapsed, previous_collect_elapsed)
    light_delta_display = format_delta(light_elapsed, previous_light_elapsed)

    if train_rows.isdigit() and holdout_rows.isdigit() and (int(train_rows) > 0 or int(holdout_rows) > 0):
        validation_split = f"{format_count(train_rows)} training rows / {format_count(holdout_rows)} holdout rows"
    else:
        validation_split = "Not available — validation data requirements were not met"

    rows = [
        "# ProxyRift Update",
        "",
        f"**Overall status:** {overall_status}",
        "",
        "## Pipeline status",
        "",
        "| Stage | Result |",
        "|---|---|",
        f"| Discovery and validation | {collection_status} |",
        f"| Subscription generation | {generation_status} |",
        f"| Publishing | {publish_status} |",
        "",
        "## Runtime",
        "",
    ]
    if previous_run_number:
        rows.extend([
            f"| Stage | This run | Previous (#{previous_run_number}) | Change |",
            "|---|---:|---:|---:|",
            f"| Total pipeline | {current_total_display} | {previous_total_display} | {total_delta_display} |",
            f"| Collection and transport | {current_collect_display} | {previous_collect_display} | {collect_delta_display} |",
            f"| Light validation | {current_light_display} | {previous_light_display} | {light_delta_display} |",
        ])
    else:
        rows.extend([
            "| Stage | This run |",
            "|---|---:|",
            f"| Total pipeline | {current_total_display} |",
            f"| Collection and transport | {current_collect_display} |",
            f"| Light validation | {current_light_display} |",
            "",
            "_No previous completed update run was found for comparison._",
        ])
    rows.extend([
        "",
        "## Published output",
        "",
        "| Subscription | Published configs |",
        "|---|---:|",
        f"| All | {format_count(all_count)} |",
        f"| Light | {format_count(light_count)} |",
        "",
        "## Light validation",
        "",
        "| Stage | Passed / tested |",
        "|---|---:|",
        f"| Candidates received | {format_count(metrics['input_candidates'])} |",
        f"| Full-path validation (10 MiB) | {format_count(metrics['full_path_verified'])} / {format_count(metrics['full_path_tested'])} |",
        f"| 1 MiB stability | {format_count(metrics['transfer_stability_passed'])} / {format_count(metrics['transfer_stability_tested'])} |",
        f"| 9 MiB sustained stream | {format_count(metrics['transfer_passed'])} / {format_count(metrics['transfer_tested'])} |",
        "",
        "_The 1 MiB stability check and 9 MiB sustained transfer total 10 MiB per candidate. The internal Stream counters mirror the sustained-transfer result and are not a separate test._",
        "",
        "## Light selection mix",
        "",
        "| Category | Result |",
        "|---|---:|",
        f"| New candidates (not in retained history) | {format_count(metrics['published_new_candidates'])} |",
        f"| Previously known; 24-hour revalidation expired | {format_count(metrics['published_expired_candidates'])} |",
        f"| Recently reused (validated within 24 hours) | {format_count(metrics['published_revalidated'])} of {format_count(metrics['published'])} (maximum {metrics['revalidated_candidate_max_percent']}%) |",
        f"| Configs from unseen families | {format_count(metrics['published_from_fresh_families'])} of {format_count(metrics['published'])} ({fresh_actual}; target {metrics['fresh_family_target_percent']}%) |",
        f"| Light published | {format_count(metrics['published'])} of {format_count(metrics['selection_target'])} target |",
        "",
        "_New means the candidate was not found in retained validation history or prior Light subscription revisions. Revalidation-expired means it was previously known but had not been fully validated within the last 24 hours. Unseen-family coverage measures diversity of published configs, not candidate age; it is a separate dimension._",
        "",
        "## Speed ranking (LightGBM)",
        "",
        "| Measure | Result |",
        "|---|---|",
        f"| Ranking mode | {format_ranking_strategy(ranking_strategy)} |",
        f"| Candidate score coverage | {format_score_coverage(scored_candidate_count, candidate_count)} |",
        f"| Recent speed data | {format_count(speed_samples)} speed measurements in the last 72 hours; {format_count(ml_new_rows)} new this run; {format_count(unique_candidates)} candidate identities across {format_count(unique_timestamps)} timestamps |",
        f"| Temporal validation split | {validation_split} |",
        f"| Evaluation | {format_model_evaluation(reason, model_mae, baseline_mae, improvement, speed_lift, speed_samples, unique_candidates, unique_timestamps, train_rows, holdout_rows, minimum_lift)} |",
        f"| Decision | {format_model_decision(accepted, reason, minimum_lift)} |",
        "",
        "<details>",
        "<summary>Validation tool versions</summary>",
        "",
        "| Xray | sing-box | Mihomo |",
        "|---|---|---|",
        f"| {tool_versions[0]} | {tool_versions[1]} | {tool_versions[2]} |",
        "</details>",
        "",
        "_Generated by the ProxyRift update workflow._",
    ])
    return "\n".join(rows) + "\n"


def main() -> int:
    env = os.environ.copy()
    workdir = Path.cwd()
    tmp_dir = Path(env.get("PROXYRIFT_TMP_DIR", "/tmp/proxyrift"))
    output_path = tmp_dir / "run-summary.md"
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        summary = make_summary(env, workdir)
        output_path.write_text(summary, encoding="utf-8")
        step_summary = env.get("GITHUB_STEP_SUMMARY", "")
        if step_summary:
            with Path(step_summary).open("a", encoding="utf-8") as handle:
                handle.write(summary)
    except OSError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(summary, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
