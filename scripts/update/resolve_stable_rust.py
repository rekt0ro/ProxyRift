#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sys
import time
import tomllib
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CHANNEL_URL = "https://static.rust-lang.org/dist/channel-rust-stable.toml"
RETRY_LIMIT = 5
RETRY_DELAY = 2
REQUEST_TIMEOUT = 45
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
VERSION_PATTERN = re.compile(r"^(1\.[0-9]+\.[0-9]+)(?:\s|$)")
PIN_PATH = Path(__file__).resolve().parents[2] / "rust-toolchain.toml"
PIN_PATTERN = re.compile(r"^1\.[0-9]+\.[0-9]+$")


def download_channel() -> bytes:
    for attempt in range(RETRY_LIMIT + 1):
        request = Request(CHANNEL_URL, headers={"User-Agent": "ProxyRift"})
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS_CODES or attempt == RETRY_LIMIT:
                raise RuntimeError(f"HTTP {exc.code} while requesting {CHANNEL_URL}") from exc
        except (TimeoutError, URLError, OSError) as exc:
            if attempt == RETRY_LIMIT:
                raise RuntimeError(f"Request failed for {CHANNEL_URL}: {exc}") from exc
        time.sleep(RETRY_DELAY)

    raise RuntimeError(f"Request failed for {CHANNEL_URL}")


def resolve_version(channel_text: str) -> str:
    try:
        document = tomllib.loads(channel_text)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError("Rust stable channel response is not valid TOML.") from exc

    rustc = document.get("pkg", {}).get("rustc", {})
    raw_version = rustc.get("version") if isinstance(rustc, dict) else None
    match = VERSION_PATTERN.match(raw_version) if isinstance(raw_version, str) else None
    if match is None:
        raise ValueError(f"Invalid stable Rust compiler version: {raw_version}")
    return match.group(1)


def resolve_pinned_version(toolchain_text: str) -> str:
    try:
        document = tomllib.loads(toolchain_text)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError("Rust toolchain pin is not valid TOML.") from exc

    toolchain = document.get("toolchain")
    channel = toolchain.get("channel") if isinstance(toolchain, dict) else None
    if not isinstance(channel, str) or PIN_PATTERN.fullmatch(channel) is None:
        raise ValueError(f"Invalid pinned Rust toolchain version: {channel}")
    return channel


def fetch_latest_stable_version() -> str:
    return resolve_version(download_channel().decode("utf-8"))


def main() -> int:
    try:
        github_output = os.environ.get("GITHUB_OUTPUT")
        if not github_output:
            raise ValueError("GITHUB_OUTPUT is required.")
        rust_version = resolve_pinned_version(PIN_PATH.read_text(encoding="utf-8"))
        with Path(github_output).open("a", encoding="utf-8") as output:
            output.write(f"version={rust_version}\n")
    except (OSError, RuntimeError, UnicodeDecodeError, ValueError) as exc:
        print(f"[ERROR] [Rust] {exc}", file=sys.stderr)
        return 1
    print(f"[INFO] [Rust] Pinned toolchain | {rust_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
