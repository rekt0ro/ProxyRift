#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


OUTPUT_FILES = (
    "all.txt", "light.txt", "all-base64.txt", "light-base64.txt",
    "all-clash.yaml", "light-clash.yaml", "all-singbox.json", "light-singbox.json",
)
CORE_OUTPUT_FILES = (
    "all.txt", "light.txt", "all-base64.txt", "light-base64.txt",
    "all-singbox.json", "light-singbox.json",
)
CLASH_OUTPUT_FILES = ("all-clash.yaml", "light-clash.yaml")
STATE_FILES = (
    "light-history.json", "light-training.jsonl", "light-training-stats.json",
    "light-candidate-ledger.json", "source-registry.json",
)
TRANSIENT_FETCH = re.compile(
    r"could not resolve host|temporary failure in name resolution|failed to connect|could not connect|"
    r"connection refused|operation timed out|connection timed out|connection reset|network is unreachable|"
    r"remote end hung up unexpectedly|http/2.*(500|502|503|504)| (500|502|503|504) |"
    r"temporarily unavailable|service unavailable|bad gateway|gateway timeout|tls handshake timeout",
    re.IGNORECASE,
)
TRANSIENT_PUSH = re.compile(
    r"internal server error|http/2.*(500|502|503|504)| (500|502|503|504) |"
    r"temporarily unavailable|service unavailable|bad gateway|gateway timeout",
    re.IGNORECASE,
)
NON_FAST_FORWARD = re.compile(
    r"non-fast-forward|fetch first|tip of your current branch is behind|updates were rejected because the tip",
    re.IGNORECASE,
)


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)


def emit(result: subprocess.CompletedProcess[str], *, stderr: bool = False) -> None:
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", file=sys.stderr if stderr else sys.stdout)


def copy_snapshot(snapshot: Path, root: Path, names: list[str]) -> None:
    for name in names:
        src = snapshot / "subscriptions" / name
        dst = root / "subscriptions" / name
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def publish(mode: str) -> int:
    root = Path.cwd()
    subscriptions = root / "subscriptions"
    if mode == "generated":
        publication_manifest_path = subscriptions / ".publication-manifest.json"
        try:
            publication_manifest = json.loads(publication_manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            print(f"[ERROR] [Publish] Missing or invalid publication manifest: {publication_manifest_path.relative_to(root)}", file=sys.stderr)
            return 1
        selected_files = publication_manifest.get("files") if isinstance(publication_manifest, dict) else None
        if (
            not isinstance(selected_files, list)
            or any(not isinstance(name, str) for name in selected_files)
            or len(selected_files) != len(set(selected_files))
            or not set(CORE_OUTPUT_FILES).issubset(selected_files)
            or not set(selected_files).issubset(OUTPUT_FILES)
        ):
            print(f"[ERROR] [Publish] Invalid output selection in {publication_manifest_path.relative_to(root)}", file=sys.stderr)
            return 1
        selected_clash = set(selected_files).intersection(CLASH_OUTPUT_FILES)
        if selected_clash and selected_clash != set(CLASH_OUTPUT_FILES):
            print("[ERROR] [Publish] Clash outputs must be selected or skipped together", file=sys.stderr)
            return 1
        skipped_formats = publication_manifest.get("skipped_formats", {})
        if not isinstance(skipped_formats, dict) or any(
            name != "clash" or not isinstance(reasons, list)
            or any(not isinstance(reason, str) for reason in reasons)
            for name, reasons in skipped_formats.items()
        ):
            print(f"[ERROR] [Publish] Invalid skipped-format details in {publication_manifest_path.relative_to(root)}", file=sys.stderr)
            return 1
        if selected_clash and "clash" in skipped_formats:
            print("[ERROR] [Publish] Clash cannot be both selected and skipped", file=sys.stderr)
            return 1
        if not selected_clash and "clash" not in skipped_formats:
            print("[ERROR] [Publish] Skipped Clash outputs require an explicit reason", file=sys.stderr)
            return 1
        for name, reasons in skipped_formats.items():
            detail = "; ".join(reasons) or "validation unavailable"
            print(f"[WARN] [Publish] Skipping {name} outputs; retaining last published versions | {detail}")
        required = list(selected_files)
        optional = list(STATE_FILES)
        snapshot_prefix = "proxyrift-publish."
        commit_message = "Update subscriptions"
        log_prefix = "Publish"
    elif mode == "state":
        required = []
        optional = list(STATE_FILES)
        snapshot_prefix = "proxyrift-light-state."
        commit_message = "Update Light validation state"
        log_prefix = "Light state"
    else:
        print(f"Unknown publication mode: {mode}", file=sys.stderr)
        return 2

    snapshot_names = list(required)
    for name in required:
        path = subscriptions / name
        if not path.is_file() or path.stat().st_size == 0:
            print(f"[ERROR] [{log_prefix}] Required output is missing or empty: {path.relative_to(root)}", file=sys.stderr)
            return 1

    for name in optional:
        path = subscriptions / name
        if path.is_file() and path.stat().st_size:
            snapshot_names.append(name)
    if mode == "state" and not snapshot_names:
        print("[ERROR] [Light state] No learning-state files are available to persist", file=sys.stderr)
        return 1

    tmpdir = Path(tempfile.mkdtemp(prefix=snapshot_prefix, dir="/tmp"))
    snapshot = tmpdir / "subscriptions"
    snapshot.mkdir(parents=True)
    try:
        for name in snapshot_names:
            shutil.copy2(subscriptions / name, snapshot / name)

        for key, value in (
            ("user.name", "rekt0ro"),
            ("user.email", "104654981+rekt0ro@users.noreply.github.com"),
        ):
            result = run(["git", "config", key, value])
            if result.returncode:
                emit(result, stderr=True)
                return result.returncode

        for attempt in range(1, 6):
            print(f"[INFO] [{log_prefix}] " + (f"Attempt {attempt}/5" if mode == "generated" else f"Publish attempt {attempt}/5"))
            fetch = run(["git", "fetch", "origin", "main"])
            if fetch.returncode:
                emit(fetch, stderr=True)
                if TRANSIENT_FETCH.search(fetch.stdout or "") and attempt < 5:
                    delay = attempt * 2
                    print(f"[WARN] [{log_prefix}] Fetch failed on attempt {attempt}/5; retrying in {delay}s", file=sys.stderr)
                    time.sleep(delay)
                    continue
                print(f"[ERROR] [{log_prefix}] Fetch failed on attempt {attempt}/5", file=sys.stderr)
                return fetch.returncode

            reset = run(["git", "reset", "--hard", "origin/main"])
            if reset.returncode:
                emit(reset, stderr=True)
                return reset.returncode

            copy_snapshot(tmpdir, root, snapshot_names)
            if mode == "state" and not snapshot_names:
                print("[INFO] [Light state] No learning state to persist")
                return 0

            stage_paths = [f"subscriptions/{name}" for name in snapshot_names]
            staged = run(["git", "add", "--", *stage_paths])
            if staged.returncode:
                emit(staged, stderr=True)
                return staged.returncode
            diff = run(["git", "diff", "--cached", "--quiet"])
            if diff.returncode == 0:
                print(f"[INFO] [{log_prefix}] No changes")
                return 0
            if diff.returncode != 1:
                emit(diff, stderr=True)
                return diff.returncode

            staged_paths = run(["git", "diff", "--cached", "--name-only"])
            if staged_paths.returncode:
                emit(staged_paths, stderr=True)
                return staged_paths.returncode
            changed_paths = set((staged_paths.stdout or "").splitlines())
            if mode == "generated":
                changed_outputs = changed_paths.intersection(
                    f"subscriptions/{name}" for name in OUTPUT_FILES
                )
                changed_state = changed_paths.intersection(
                    f"subscriptions/{name}" for name in STATE_FILES
                )
                if changed_outputs and changed_state:
                    commit_message = "Update subscriptions and validation state"
                elif changed_outputs:
                    commit_message = "Update subscriptions"
                else:
                    commit_message = "Update source and validation state"

            commit = run(["git", "commit", "-m", commit_message])
            if commit.returncode:
                emit(commit, stderr=True)
                return commit.returncode
            emit(commit)

            push = run(["git", "push", "origin", "HEAD:main"])
            emit(push)
            if push.returncode == 0:
                print(f"[INFO] [{log_prefix}] " + ("Subscription update published" if mode == "generated" else "Learning state persisted"))
                return 0

            message = push.stdout or ""
            if NON_FAST_FORWARD.search(message):
                print(f"[WARN] [{log_prefix}] Main moved | Retrying")
                reset = run(["git", "reset", "--hard"])
                if reset.returncode:
                    emit(reset, stderr=True)
                    return reset.returncode
                continue
            if TRANSIENT_PUSH.search(message):
                if attempt < 5:
                    delay = attempt * 2
                    print(f"[WARN] [{log_prefix}] Transient GitHub server error | Retrying in {delay}s")
                    reset = run(["git", "reset", "--hard"])
                    if reset.returncode:
                        emit(reset, stderr=True)
                        return reset.returncode
                    time.sleep(delay)
                    continue
                print(f"[ERROR] [{log_prefix}] GitHub server error persisted after 5 attempts", file=sys.stderr)
                return push.returncode
            print(f"[ERROR] [{log_prefix}] Push failed for a non-race reason", file=sys.stderr)
            return push.returncode

        print(f"[ERROR] [{log_prefix}] Failed after 5 attempts", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
