#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import shutil
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
    try:
        manifest = json.loads(counts_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fail(f"Invalid or inconsistent format-count manifest for light: {counts_path.as_posix()}")
    if not isinstance(manifest, dict):
        return fail(f"Invalid or inconsistent format-count manifest for light: {counts_path.as_posix()}")

    clash_failures: list[str] = []
    ruby = shutil.which("ruby")
    mihomo_available = mihomo.is_file() and os.access(mihomo, os.X_OK)

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

        source_count = light_count if name == "light" else all_count
        count_row = manifest.get(name)
        if not isinstance(count_row, dict):
            return fail(f"Invalid or inconsistent format-count manifest for {name}: {counts_path.as_posix()}")
        manifest_source = count_row.get("source")
        manifest_clash = count_row.get("clash")
        manifest_singbox = count_row.get("singbox")
        if not is_nonnegative_integer(manifest_source) or not is_nonnegative_integer(manifest_singbox):
            return fail(f"Invalid or inconsistent source/sing-box count manifest for {name}: {counts_path.as_posix()}")
        if manifest_singbox > manifest_source:
            return fail(f"Invalid or inconsistent source/sing-box count manifest for {name}: {counts_path.as_posix()}")
        singbox_count = len(outbounds)
        if manifest_source != source_count:
            return fail(f"Source count does not match format-count manifest for {name}: manifest={manifest_source}, file={source_count}.")
        if manifest_singbox != singbox_count:
            return fail(f"sing-box entry count does not match format-count manifest for {name}: manifest={manifest_singbox}, file={singbox_count}.")

        clash_path = SUBSCRIPTIONS / f"{name}-clash.yaml"
        clash_failure: str | None = None
        clash_count: int | None = None
        if not is_nonnegative_integer(manifest_clash) or manifest_clash > manifest_source:
            clash_failure = f"{name}: invalid or missing Clash entry count in {counts_path.as_posix()}"
        elif not clash_path.is_file() or clash_path.stat().st_size == 0:
            clash_failure = f"{name}: Clash output is missing or empty: {clash_path.as_posix()}"
        elif ruby is None:
            clash_failure = "Ruby is unavailable for Clash structural validation"
        else:
            try:
                validator = subprocess.run(
                    [ruby, str(ROOT / "scripts/update/validate_clash_output.rb"), "--count", clash_path.as_posix()],
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
                )
            except OSError as exc:
                validator = None
                clash_failure = f"{name}: could not run Clash structural validator: {exc}"
            if validator is not None and validator.returncode != 0:
                detail = validator.stderr.strip() or "validator rejected the YAML"
                clash_failure = f"{name}: structural validation failed for {clash_path.as_posix()}: {detail}"
            elif validator is not None:
                try:
                    clash_count = int(validator.stdout.strip())
                except ValueError:
                    clash_failure = f"{name}: structural validator returned an invalid proxy count for {clash_path.as_posix()}"
                if clash_failure is None and clash_count != manifest_clash:
                    clash_failure = f"{name}: Clash count does not match the format-count manifest: manifest={manifest_clash}, file={clash_count}"

        if clash_failure is None and not mihomo_available:
            clash_failure = f"Mihomo validator is missing or not executable: {mihomo}"
        if clash_failure is None:
            try:
                mihomo_result = subprocess.run(
                    [str(mihomo), "-t", "-f", clash_path.as_posix()],
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
                )
            except OSError as exc:
                mihomo_result = None
                clash_failure = f"{name}: could not run Mihomo validator: {exc}"
            if mihomo_result is not None and mihomo_result.returncode != 0:
                detail = (mihomo_result.stderr or mihomo_result.stdout).strip() or "Mihomo rejected the configuration"
                clash_failure = f"{name}: Mihomo rejected {clash_path.as_posix()}: {detail}"

        if clash_failure is not None:
            clash_failures.append(clash_failure)
            print(f"[WARN] [Clash/Mihomo] {clash_failure}", file=sys.stderr)
        else:
            print(f"[OK] [Clash/Mihomo] Structurally valid and accepted by Mihomo | {clash_path.as_posix()} | Proxies: {clash_count}")

        clash_display = str(clash_count) if clash_failure is None else "skipped"
        print(
            f"[INFO] Format accounting | {name} source: {source_count} | Clash: {clash_display} "
            f"| sing-box: {singbox_count} (omitted: {source_count - singbox_count})"
        )

    publication_files = [
        "all.txt", "light.txt", "all-base64.txt", "light-base64.txt",
        "all-singbox.json", "light-singbox.json",
    ]
    if clash_failures:
        for clash_file in ("all-clash.yaml", "light-clash.yaml"):
            print(
                f"[WARN] [Clash/Mihomo] Not publishing {clash_file}; retaining the last published version",
                file=sys.stderr,
            )
    else:
        publication_files.extend(["all-clash.yaml", "light-clash.yaml"])

    publication_manifest = {
        "files": publication_files,
        "skipped_formats": {"clash": clash_failures} if clash_failures else {},
    }
    (SUBSCRIPTIONS / ".publication-manifest.json").write_text(
        json.dumps(publication_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    clash_status = "skipped; previous exports retained" if clash_failures else "validated"
    print(
        f"[OK] Publication gate passed | Light: {light_count} (minimum {minimum_light}) "
        f"| All: {all_count} | Clash/Mihomo: {clash_status}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
