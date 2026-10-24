#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_URL = "https://api.github.com/repos/lightgbm-org/LightGBM/releases/latest"
ASSET_NAME = "lib_lightgbm.so"
RETRY_LIMIT = 5
RETRY_DELAY = 2
REQUEST_TIMEOUT = 45
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
TAG_PATTERN = re.compile(r"^v([0-9]+\.[0-9]+\.[0-9]+)$")
DIGEST_PATTERN = re.compile(r"^sha256:([0-9a-fA-F]{64})$")
ASSET_URL_PATTERN = re.compile(
    r"^https://github\.com/lightgbm-org/LightGBM/releases/download/v[0-9]+\.[0-9]+\.[0-9]+/[^/]+$"
)


def request_bytes(url: str, headers: dict[str, str], token: str = "") -> bytes:
    request_headers = dict(headers)
    if token:
        request_headers["Authorization"] = f"Bearer {token}"

    for attempt in range(RETRY_LIMIT + 1):
        request = Request(url, headers=request_headers)
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS_CODES or attempt == RETRY_LIMIT:
                raise RuntimeError(f"HTTP {exc.code} while requesting {url}") from exc
        except (TimeoutError, URLError, OSError) as exc:
            if attempt == RETRY_LIMIT:
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc
        time.sleep(RETRY_DELAY)

    raise RuntimeError(f"Request failed for {url}")


def resolve_release(document: object) -> tuple[str, str, str]:
    if not isinstance(document, dict):
        raise ValueError("Latest release response is not a JSON object.")

    tag = document.get("tag_name")
    match = TAG_PATTERN.fullmatch(tag) if isinstance(tag, str) else None
    if match is None:
        raise ValueError(f"Invalid latest release tag: {tag}")
    version = match.group(1)

    assets = document.get("assets")
    if not isinstance(assets, list):
        assets = []
    asset = next(
        (item for item in assets if isinstance(item, dict) and item.get("name") == ASSET_NAME),
        None,
    )
    if asset is None:
        raise ValueError(f"Missing release asset: {ASSET_NAME}")

    digest = asset.get("digest")
    digest_match = DIGEST_PATTERN.fullmatch(digest) if isinstance(digest, str) else None
    if digest_match is None:
        raise ValueError(f"Missing or invalid SHA-256 digest for {ASSET_NAME}")

    asset_url = asset.get("browser_download_url")
    if not isinstance(asset_url, str) or ASSET_URL_PATTERN.fullmatch(asset_url) is None:
        raise ValueError(f"Missing or invalid browser download URL for {ASSET_NAME}: {asset_url}")

    return version, asset_url, digest_match.group(1).lower()


def write_verified_asset(content: bytes, destination: Path, expected_sha256: str) -> None:
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if actual_sha256.lower() != expected_sha256.lower():
        raise ValueError(
            f"SHA-256 mismatch for {destination.name}: expected {expected_sha256}, got {actual_sha256}"
        )

    temporary = destination.with_name(f".{destination.name}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    try:
        github_env = os.environ.get("GITHUB_ENV")
        if not github_env:
            raise ValueError("GITHUB_ENV is required.")

        print("[INFO] [LightGBM] Resolving latest stable release")
        response = request_bytes(
            API_URL,
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "ProxyRift",
            },
            os.environ.get("GH_TOKEN", ""),
        )
        try:
            release_document = json.loads(response)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Latest release response is not valid JSON.") from exc

        version, asset_url, expected_sha256 = resolve_release(release_document)
        install_dir = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "proxyrift" / "lightgbm" / version
        install_dir.mkdir(parents=True, exist_ok=True)
        destination = install_dir / ASSET_NAME

        print(f"[INFO] [LightGBM] Download | {asset_url}")
        content = request_bytes(
            asset_url,
            {"Accept": "application/octet-stream", "User-Agent": "ProxyRift"},
        )
        write_verified_asset(content, destination, expected_sha256)

        existing_ld = os.environ.get("LD_LIBRARY_PATH", "")
        with Path(github_env).open("a", encoding="utf-8") as output:
            output.write(f"LIGHTGBM_VERSION={version}\n")
            output.write(f"LIGHTGBM_LIB_DIR={install_dir}\n")
            output.write(f"LD_LIBRARY_PATH={install_dir}:{existing_ld}\n")

        print(f"[INFO] [LightGBM] Native runtime | {version} | {install_dir}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[ERROR] [LightGBM] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
