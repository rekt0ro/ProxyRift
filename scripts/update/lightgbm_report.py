from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_NUMBER = re.compile(r"^-?[0-9]+(?:\.[0-9]+)?$")
_COUNT = re.compile(r"^[0-9]+$")
_SENSITIVE_KEYS = re.compile(r"^(candidate_id|config_id|config|uri|url)$", re.IGNORECASE)


def load_json(path: str | Path, default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def nested_value(data: Any, dotted_path: str, fallback: str = "n/a") -> str:
    value = data
    for key in dotted_path.split("."):
        if not isinstance(value, dict) or key not in value:
            return fallback
        value = value[key]
    if value is None:
        return fallback
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def is_number(value: Any) -> bool:
    return bool(_NUMBER.fullmatch(str(value)))


def format_count(value: Any) -> str:
    text = str(value)
    if not _COUNT.fullmatch(text):
        return text
    groups: list[str] = []
    while len(text) > 3:
        groups.insert(0, text[-3:])
        text = text[:-3]
    groups.insert(0, text)
    return ",".join(groups)


def format_decimal(value: Any) -> str:
    return f"{float(value):.3f}" if is_number(value) else str(value)


def format_mae_change(value: Any) -> str:
    if not is_number(value):
        return str(value)
    pct = float(value) * 100
    return f"{pct:.1f}% lower" if pct >= 0 else f"{-pct:.1f}% higher"


def format_multiplier(value: Any) -> str:
    return f"{float(value):.2f}x" if is_number(value) else str(value)


def format_score_coverage(scored: Any, total: Any) -> str:
    scored_text, total_text = str(scored), str(total)
    if not _COUNT.fullmatch(scored_text) or not _COUNT.fullmatch(total_text) or int(total_text) == 0:
        return "Unavailable — candidate score coverage not recorded"
    scored_int, total_int = int(scored_text), int(total_text)
    pct = f"{100 * scored_int / total_int:.1f}%"
    if scored_int == total_int:
        details = f"{pct}; complete"
    elif scored_int < total_int:
        details = f"{pct}; {format_count(total_int - scored_int)} missing"
    else:
        details = f"{pct}; {format_count(scored_int - total_int)} over-reported"
    return f"{format_count(scored_int)} / {format_count(total_int)} candidates scored ({details})"


def format_decision(accepted: Any, reason: Any, minimum_lift: Any) -> str:
    if str(accepted).lower() == "true":
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
    return messages.get(str(reason), f"Not evaluated — {str(reason).replace('_', ' ') or 'no evaluation report'}.")


def format_evaluation(
    reason: Any,
    model_mae: Any,
    baseline_mae: Any,
    improvement: Any,
    speed_lift: Any,
    speed_samples: Any,
    unique_candidates: Any,
    unique_timestamps: Any,
    train_rows: Any,
    holdout_rows: Any,
    minimum_lift: Any,
) -> str:
    if all(is_number(value) for value in (model_mae, baseline_mae, improvement)):
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
    return f"Not evaluated — {str(reason).replace('_', ' ') or 'no evaluation report'}."


def validate_score_document(score: Any) -> bool:
    return (
        isinstance(score, dict)
        and score.get("version") == 5
        and isinstance(score.get("model_report"), dict)
        and score.get("model_report", {}).get("target") == "log(stream_throughput_kbps)"
        and score.get("feature_count") == 61
    )


def artifact_document(score: dict[str, Any]) -> dict[str, Any]:
    report = score["model_report"]
    scores = score.get("scores") or {}
    fields = (
        "model", "validation", "baseline", "ranking_strategy", "target", "target_unit",
        "reason", "cutoff_timestamp", "speed_samples", "unique_candidates", "candidate_count",
        "scored_candidate_count", "unique_timestamps", "train_rows", "holdout_rows",
        "baseline_log_mae", "model_log_mae", "relative_mae_improvement", "top_20pct_speed_lift",
        "minimum_relative_mae_improvement", "minimum_top_20pct_speed_lift",
    )
    sanitized_report = {key: report.get(key) for key in fields}
    sanitized_report["accepted"] = report.get("accepted", False)
    return {
        "schema_version": 2,
        "available": True,
        "model_version": score["version"],
        "trained": score.get("trained", False),
        "training_rows": score.get("training_rows"),
        "feature_count": score["feature_count"],
        "scored_candidates": len(scores) if isinstance(scores, (dict, list, str)) else 0,
        "model_report": sanitized_report,
    }


def contains_sensitive_key(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_SENSITIVE_KEYS.fullmatch(str(key)) or contains_sensitive_key(item) for key, item in value.items())
    if isinstance(value, list):
        return any(contains_sensitive_key(item) for item in value)
    return False


def validate_artifact_document(artifact: Any) -> bool:
    return isinstance(artifact, dict) and not contains_sensitive_key(artifact) and "scores" not in artifact
