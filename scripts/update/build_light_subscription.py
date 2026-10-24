#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def count_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip())


def deferred_additions(deferred_file: Path, candidate_file: Path, existing_file: Path) -> list[str]:
    deferred = {
        line.split("#", 1)[0]
        for line in deferred_file.read_text(encoding="utf-8", errors="replace").splitlines()
    }
    candidates: dict[str, str] = {}
    order: list[str] = []
    for line in candidate_file.read_text(encoding="utf-8", errors="replace").splitlines():
        key = line.split("#", 1)[0]
        if key in deferred and key not in candidates:
            candidates[key] = line
            order.append(key)
    existing = {
        line.split("#", 1)[0]
        for line in existing_file.read_text(encoding="utf-8", errors="replace").splitlines()
    }
    return [candidates[key] for key in order if key not in existing]


def main() -> int:
    root = Path.cwd()
    subscriptions = root / "subscriptions"
    temp_root = Path("/tmp/proxyrift")
    temp_root.mkdir(parents=True, exist_ok=True)
    next_file = temp_root / "light.next.txt"
    stats_file = temp_root / "light-stats.json"
    started = time.monotonic()
    command = [
        "./target/release/polish_light",
        "--candidates", "subscriptions/.light-candidates.txt",
        "--prevalidated-http-metrics", "subscriptions/.light-candidates-http.json",
        "--precheck-timeout", "3",
        "--output", str(next_file),
        "--workers", "48",
        "--batch-size", "1000",
        "--timeout", "3",
        "--selected-recheck-limit", "350",
        "--primary-target", "http://cp.cloudflare.com/",
        "--singbox", str(Path.home() / ".local/bin/sing-box"),
        "--selection-limit", os.environ["LIGHT_SELECTION_LIMIT"],
        "--max-per-endpoint", "1",
        "--max-per-family", "3",
        "--stats", str(stats_file),
    ]
    result = subprocess.run(command, check=False)
    elapsed = f"{int(time.monotonic() - started)}s"
    if result.returncode:
        print(f"[WARN] [Light] Validation failed | Preserving previous subscription | Took: {elapsed}", file=sys.stderr)
        return result.returncode

    minimum = int(os.environ.get("LIGHT_MIN_PUBLISH", "50"))
    target = int(os.environ.get("LIGHT_SELECTION_LIMIT", "200"))
    if next_file.is_file() and next_file.stat().st_size:
        selected_count = count_lines(next_file)
        if selected_count >= minimum:
            staged = subscriptions / ".light.txt.next"
            shutil.copyfile(next_file, staged)
            os.replace(staged, subscriptions / "light.txt")

            deferred_file = subscriptions / ".deferred-transport-candidates.txt"
            all_file = subscriptions / "all.txt"
            if deferred_file.is_file() and deferred_file.stat().st_size and all_file.is_file():
                additions = deferred_additions(deferred_file, next_file, all_file)
                before = count_lines(all_file)
                capacity = 2000 - before
                if capacity > 0:
                    limited = additions[:capacity]
                    if limited:
                        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=subscriptions, prefix=".all-deferred-limited.", delete=False) as handle:
                            limited_file = Path(handle.name)
                            handle.write("\n".join(limited) + "\n")
                        try:
                            existing_text = all_file.read_bytes()
                            with tempfile.NamedTemporaryFile("wb", dir=subscriptions, prefix=".all.", suffix=".next", delete=False) as handle:
                                replacement = Path(handle.name)
                                handle.write(existing_text)
                                handle.write(limited_file.read_bytes())
                            os.replace(replacement, all_file)
                        finally:
                            limited_file.unlink(missing_ok=True)
                        print(f"[INFO] [All] Added {len(limited)} deferred configs verified by Light consumer validation")
            print(f"[INFO] [Light] Published {selected_count} configs | Target: {target} | Took: {elapsed}")
        else:
            print(f"[WARN] [Light] Pool too small | {selected_count}/{minimum} minimum | Preserving previous subscription | Took: {elapsed}")
    else:
        print(f"[WARN] [Light] No configs | Preserving previous subscription | Took: {elapsed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
