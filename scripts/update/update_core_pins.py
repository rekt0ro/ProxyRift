#!/usr/bin/env python3
"""Resolve stable tool releases and update the repository's pinned versions."""

import gzip
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
import zlib
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
VERSION_PATTERN = re.compile(r"v([0-9]+\.[0-9]+\.[0-9]+)\Z")
PLAIN_VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\Z")
DIGEST_PATTERN = re.compile(r"sha256:([0-9a-fA-F]{64})\Z")
PYTHON_DOWNLOADS_URL = "https://www.python.org/downloads/"
RUBY_SETUP_MANIFEST_URL = "https://raw.githubusercontent.com/ruby/setup-ruby/master/ruby-builder-versions.json"


class PythonReleasePageParser(HTMLParser):
    """Read stable Python release links from the official downloads page."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.versions = []
        self._anchor_text = None

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href", "")
        path = urlparse(urljoin(PYTHON_DOWNLOADS_URL, href)).path
        if re.fullmatch(r"/downloads/release/python-[^/]+/?", path):
            self._anchor_text = []

    def handle_data(self, data):
        if self._anchor_text is not None:
            self._anchor_text.append(data)

    def handle_endtag(self, tag):
        if tag != "a" or self._anchor_text is None:
            return
        label = " ".join("".join(self._anchor_text).split())
        match = re.fullmatch(r"Python\s+([0-9]+\.[0-9]+\.[0-9]+)", label)
        if match:
            self.versions.append(match.group(1))
        self._anchor_text = None


def latest_python_version(html):
    """Return the first stable release listed on python.org's version-sorted page."""
    parser = PythonReleasePageParser()
    parser.feed(html)
    parser.close()
    if not parser.versions:
        raise ValueError("Could not find a stable Python X.Y.Z release on python.org")
    version = parser.versions[0]
    if PLAIN_VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError(f"Invalid stable Python version: {version!r}")
    return version


def fetch_latest_python_version():
    """Fetch Python's latest stable release from its official downloads page."""
    request = Request(
        PYTHON_DOWNLOADS_URL,
        headers={
            "Accept": "text/html",
            "Accept-Encoding": "identity",
            "User-Agent": "ProxyRift core-pin updater",
        },
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = response.read()
                content_encoding = response.headers.get("Content-Encoding", "").lower()
            if (
                "gzip" in {item.strip() for item in content_encoding.split(",")}
                or payload.startswith(b"\x1f\x8b")
            ):
                payload = gzip.decompress(payload)
            html = payload.decode("utf-8")
            return latest_python_version(html)
        except HTTPError as error:
            if error.code not in (403, 429) and 400 <= error.code < 500:
                raise RuntimeError(
                    f"python.org downloads page returned HTTP {error.code}"
                ) from error
            cause = error
        except (URLError, TimeoutError, UnicodeDecodeError, ValueError, OSError, EOFError, zlib.error) as error:
            cause = error

        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(
                f"Unable to resolve stable Python after {MAX_ATTEMPTS} attempts: {cause}"
            ) from cause
        print(
            f"[WARN] Python release lookup failed on attempt "
            f"{attempt}/{MAX_ATTEMPTS}: {cause}; retrying.",
            file=sys.stderr,
        )
        time.sleep(RETRY_DELAY_SECONDS)

    raise AssertionError("Retry loop exited unexpectedly")


MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 15
RETRY_DELAY_SECONDS = 2


def latest_supported_ruby_version(manifest):
    versions = manifest.get("ruby") if isinstance(manifest, dict) else None
    if not isinstance(versions, list):
        raise ValueError("ruby/setup-ruby manifest has no Ruby version list")
    stable_versions = [
        version for version in versions
        if isinstance(version, str) and PLAIN_VERSION_PATTERN.fullmatch(version)
    ]
    if not stable_versions:
        raise ValueError("ruby/setup-ruby manifest has no stable Ruby releases")
    return max(
        stable_versions,
        key=lambda version: tuple(map(int, version.split("."))),
    )


def fetch_latest_supported_ruby_version():
    request = Request(
        RUBY_SETUP_MANIFEST_URL,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "ProxyRift core-pin updater",
        },
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                manifest = json.load(response)
            return latest_supported_ruby_version(manifest)
        except HTTPError as error:
            if error.code not in (403, 429) and 400 <= error.code < 500:
                raise RuntimeError(
                    f"ruby/setup-ruby manifest returned HTTP {error.code}"
                ) from error
            cause = error
        except (URLError, TimeoutError, json.JSONDecodeError, ValueError, OSError) as error:
            cause = error

        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(
                f"Unable to resolve supported Ruby after {MAX_ATTEMPTS} attempts: {cause}"
            ) from cause
        print(
            f"[WARN] Ruby version lookup failed on attempt "
            f"{attempt}/{MAX_ATTEMPTS}: {cause}; retrying.",
            file=sys.stderr,
        )
        time.sleep(RETRY_DELAY_SECONDS)

    raise AssertionError("Retry loop exited unexpectedly")


def fetch_release(repository, token):
    url = f"https://api.github.com/repos/{repository}/releases/latest"
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ProxyRift",
        },
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                raise ValueError(f"GitHub returned an invalid release document for {repository}")
            return payload
        except HTTPError as error:
            if error.code not in (403, 429) and 400 <= error.code < 500:
                raise RuntimeError(
                    f"GitHub Releases API returned HTTP {error.code} for {repository}"
                ) from error
            cause = error
        except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as error:
            cause = error

        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(
                f"Unable to fetch stable release for {repository} after "
                f"{MAX_ATTEMPTS} attempts: {cause}"
            ) from cause
        print(
            f"[WARN] Release lookup for {repository} failed on attempt "
            f"{attempt}/{MAX_ATTEMPTS}: {cause}; retrying.",
            file=sys.stderr,
        )
        time.sleep(RETRY_DELAY_SECONDS)

    raise AssertionError("Retry loop exited unexpectedly")


def stable_version(release, project):
    tag = release.get("tag_name")
    match = VERSION_PATTERN.fullmatch(tag) if isinstance(tag, str) else None
    if match is None:
        raise ValueError(f"Invalid {project} stable release tag: {tag!r}")
    return match.group(1)


def asset_sha256(release, asset_name, project):
    assets = release.get("assets")
    if not isinstance(assets, list):
        raise ValueError(f"{project} release has no asset list")

    matches = [
        asset for asset in assets
        if isinstance(asset, dict) and asset.get("name") == asset_name
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one {project} release asset named {asset_name!r}; "
            f"found {len(matches)}"
        )

    digest = matches[0].get("digest")
    match = DIGEST_PATTERN.fullmatch(digest) if isinstance(digest, str) else None
    if match is None:
        raise ValueError(
            f"{project} release asset {asset_name!r} has no valid SHA-256 digest"
        )
    return match.group(1).lower()


RELEASE_REPOSITORIES = {
    "Ruby": "ruby/ruby",
    "Xray-core": "XTLS/Xray-core",
    "sing-box": "SagerNet/sing-box",
    "Mihomo": "MetaCubeX/mihomo",
}


def fetch_release_by_tag(repository, tag, token):
    url = f"https://api.github.com/repos/{repository}/releases/tags/{tag}"
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ProxyRift",
        },
    )
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
        return payload if isinstance(payload, dict) else {}
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError) as error:
        print(
            f"[WARN] Unable to fetch release notes for {repository} {tag}: {error}",
            file=sys.stderr,
        )
        return {}


def pinned_value(content, key, path):
    pattern = re.compile(
        rf'(?m)^[ \t]*{re.escape(key)}:[ \t]*"([^"\r\n]*)"[ \t]*$'
