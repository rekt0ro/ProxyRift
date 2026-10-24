#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SUBSCRIPTIONS = Path("subscriptions")


def fail(message: str) -> int:
    print(f"[ERROR] {message}", file=sys.stderr)
    return 1


def count_entries(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        return sum(1 for line in handle if line.strip() and not line.startswith("#"))


def is_nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def run_validator(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, check=False)


def main() -> int:
    minimum_light = int(os.environ.get("LIGHT_MIN_PUBLISH", "50"))
    minimum_all = int(os.environ.get("ALL_MIN_PUBLISH", "1000"))
    maximum_light = int(os.environ.get("LIGHT_SELECTION_LIMIT", "200"))
    maximum_all = int(os.environ.get("ALL_MAX_PUBLISH", "2000"))
    counts_path = SUBSCRIPTIONS / ".output-counts.json"
    required = [
        SUBSCRIPTIONS / "all.txt", SUBSCRIPTIONS / "light.txt",
        SUBSCRIPTIONS / "all-base64.txt", SUBSCRIPTIONS / "light-base64.txt",
        SUBSCRIPTIONS / "all-clash.yaml", SUBSCRIPTIONS / "light-clash.yaml",
        SUBSCRIPTIONS / "all-singbox.json", SUBSCRIPTIONS / "light-singbox.json",
        counts_path,
    ]
    for path in required:
        if not path.is_file() or path.stat().st_size == 0:
            return fail(f"Required output is missing or empty: {path.as_posix()}")

    light_count = count_entries(SUBSCRIPTIONS / "light.txt")
    all_count = count_entries(SUBSCRIPTIONS / "all.txt")
    for name, count, minimum, maximum in (
        ("Light", light_count, minimum_light, maximum_light),
        ("All", all_count, minimum_all, maximum_all),
    ):
        if count < minimum:
            return fail(f"Refusing to publish {name} with {count} entries; minimum is {minimum}.")
        if count > maximum:
            return fail(f"Refusing to publish {name} with {count} entries; maximum is {maximum}.")

    home = Path.home()
    singbox = Path(os.environ.get("SINGBOX_BINARY", str(home / ".local/bin/sing-box")))
    mihomo = Path(os.environ.get("MIHOMO_BINARY", str(home / ".local/bin/mihomo")))
    if not singbox.is_file() or not os.access(singbox, os.X_OK):
        return fail(f"sing-box validator is missing or not executable: {singbox}")
    if not mihomo.is_file() or not os.access(mihomo, os.X_OK):
        return fail(f"Mihomo validator is missing or not executable: {mihomo}")

    try:
        manifest = json.loads(counts_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fail(f"Invalid or inconsistent format-count manifest for light: {counts_path.as_posix()}")
    if not isinstance(manifest, dict):
        return fail(f"Invalid or inconsistent format-count manifest for light: {counts_path.as_posix()}")

    for name in ("light", "all"):
        source_path = SUBSCRIPTIONS / f"{name}.txt"
        base64_path = SUBSCRIPTIONS / f"{name}-base64.txt"
        try:
            encoded = base64_path.read_bytes()
            compact = b"".join(encoded.split())
            decoded = base64.b64decode(compact, validate=True)
        except (OSError, ValueError):
            return fail(f"Base64 output cannot be decoded: {base64_path.as_posix()}")
        try:
            source = source_path.read_bytes().rstrip(b"\r\n")
        except OSError:
            return fail(f"Required output is missing or empty: {source_path.as_posix()}")
        if source != decoded:
            return fail(f"Base64 output does not mirror subscriptions/{name}.txt.")

        singbox_path = SUBSCRIPTIONS / f"{name}-singbox.json"
        try:
            singbox_doc = json.loads(singbox_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return fail(f"Invalid sing-box JSON or outbound missing a non-empty type/tag: {singbox_path.as_posix()}")
        outbounds = singbox_doc.get("outbounds") if isinstance(singbox_doc, dict) else None
        if not isinstance(outbounds, list) or not outbounds:
            return fail(f"Invalid sing-box JSON or outbound missing a non-empty type/tag: {singbox_path.as_posix()}")
        if any(
            not isinstance(item, dict)
            or not isinstance(item.get("type"), str) or not item["type"]
            or not isinstance(item.get("tag"), str) or not item["tag"]
            for item in outbounds
        ):
            return fail(f"Invalid sing-box JSON or outbound missing a non-empty type/tag: {singbox_path.as_posix()}")

        if run_validator([str(singbox), "check", "-c", singbox_path.as_posix()]).returncode != 0:
            return fail(f"sing-box rejected configuration: {singbox_path.as_posix()}")

        clash_path = SUBSCRIPTIONS / f"{name}-clash.yaml"
        validator = subprocess.run(
            ["ruby", str(ROOT / "scripts/update/validate_clash_output.rb"), "--count", clash_path.as_posix()],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if validator.returncode != 0:
            if validator.stderr:
                print(validator.stderr, end="", file=sys.stderr)
            return fail(f"Clash/Mihomo output failed structural validation: {clash_path.as_posix()}")
        try:
            clash_count = int(validator.stdout.strip())
        except ValueError:
            return fail(f"Clash/Mihomo output failed structural validation: {clash_path.as_posix()}")
        print(f"[OK] [Clash/Mihomo] Structurally valid | {clash_path.as_posix()} | Proxies: {clash_count}")

        if run_validator([str(mihomo), "-t", "-f", clash_path.as_posix()]).returncode != 0:
            return fail(f"Mihomo rejected Clash configuration: {clash_path.as_posix()}")

        source_count = light_count if name == "light" else all_count
        count_row = manifest.get(name)
        if not isinstance(count_row, dict):
            return fail(f"Invalid or inconsistent format-count manifest for {name}: {counts_path.as_posix()}")
        values = (count_row.get("source"), count_row.get("clash"), count_row.get("singbox"))
        if any(not is_nonnegative_integer(value) for value in values):
            return fail(f"Invalid or inconsistent format-count manifest for {name}: {counts_path.as_posix()}")
        manifest_source, manifest_clash, manifest_singbox = values
        if manifest_clash > manifest_source or manifest_singbox > manifest_source:
            return fail(f"Invalid or inconsistent format-count manifest for {name}: {counts_path.as_posix()}")
        singbox_count = len(outbounds)

        if manifest_source != source_count:
            return fail(f"Source count does not match format-count manifest for {name}: manifest={manifest_source}, file={source_count}.")
        if manifest_clash != clash_count:
            return fail(f"Clash entry count does not match format-count manifest for {name}: manifest={manifest_clash}, file={clash_count}.")
        if manifest_singbox != singbox_count:
            return fail(f"sing-box entry count does not match format-count manifest for {name}: manifest={manifest_singbox}, file={singbox_count}.")

        print(
            f"[INFO] Format accounting | {name} source: {source_count} | Clash: {clash_count} "
            f"(omitted: {source_count - clash_count}) | sing-box: {singbox_count} "
            f"(omitted: {source_count - singbox_count})"
        )

    print(f"[OK] Publication gate passed | Light: {light_count} (minimum {minimum_light}) | All: {all_count}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
