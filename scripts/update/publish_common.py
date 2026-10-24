#!/usr/bin/env python3
from __future__ import annotations

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
        required = list(OUTPUT_FILES)
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
