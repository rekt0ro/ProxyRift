#!/usr/bin/env python3
"""Resolve stable tool releases and update the repository's pinned versions."""

import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
VERSION_PATTERN = re.compile(r"v([0-9]+\.[0-9]+\.[0-9]+)\Z")
DIGEST_PATTERN = re.compile(r"sha256:([0-9a-fA-F]{64})\Z")
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 15
RETRY_DELAY_SECONDS = 2


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


def replace_pins(content, values, path):
    for key, value in values.items():
        pattern = re.compile(
            rf'(?m)^([ \t]*{re.escape(key)}:[ \t]*)"[^"\r\n]*"[ \t]*$'
        )
        updated, count = pattern.subn(
            lambda match: f'{match.group(1)}"{value}"',
            content,
        )
        if count != 1:
            raise ValueError(
                f"Expected exactly one {key} pin in {path}, found {count}"
            )
        content = updated
    return content


def write_atomically(path, content):
    path = Path(path)
    current_mode = stat.S_IMODE(path.stat().st_mode)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_name = temporary.name
        os.chmod(temporary_name, current_mode)
        os.replace(temporary_name, path)
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def build_updates(releases):
    xray_version = stable_version(releases["xray"], "Xray")
    singbox_version = stable_version(releases["singbox"], "sing-box")
    mihomo_version = stable_version(releases["mihomo"], "Mihomo")
    ruby_version = stable_version(releases["ruby"], "Ruby")

    xray_asset = "Xray-linux-64.zip"
    singbox_asset = f"sing-box-{singbox_version}-linux-amd64.tar.gz"
    mihomo_asset = f"mihomo-linux-amd64-v{mihomo_version}.gz"

    action_values = {
        "XRAY_VERSION_PIN": xray_version,
        "XRAY_SHA256_PIN": asset_sha256(releases["xray"], xray_asset, "Xray"),
        "XRAY_ASSET_PIN": xray_asset,
        "SINGBOX_VERSION_PIN": singbox_version,
        "SINGBOX_SHA256_PIN": asset_sha256(
            releases["singbox"], singbox_asset, "sing-box"
        ),
        "SINGBOX_ASSET_PIN": singbox_asset,
        "MIHOMO_VERSION_PIN": mihomo_version,
        "MIHOMO_SHA256_PIN": asset_sha256(
            releases["mihomo"], mihomo_asset, "Mihomo"
        ),
        "MIHOMO_ASSET_PIN": mihomo_asset,
    }
    validation_values = {
        "MIHOMO_VERSION": mihomo_version,
        "MIHOMO_SHA256": action_values["MIHOMO_SHA256_PIN"],
        "MIHOMO_ASSET": mihomo_asset,
    }
    return ruby_version, action_values, validation_values


def main():
    token = os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("GH_TOKEN is required to query GitHub Releases")

    repositories = {
        "xray": "XTLS/Xray-core",
        "singbox": "SagerNet/sing-box",
        "mihomo": "MetaCubeX/mihomo",
        "ruby": "ruby/ruby",
    }
    releases = {
        name: fetch_release(repository, token)
        for name, repository in repositories.items()
    }
    ruby_version, action_values, validation_values = build_updates(releases)

    action_path = ROOT / ".github/actions/prepare-update/action.yml"
    validation_path = ROOT / ".github/workflows/validate.yml"
    ruby_path = ROOT / ".ruby-version"

    action_content = replace_pins(
        action_path.read_text(encoding="utf-8"), action_values, action_path
    )
    validation_content = replace_pins(
        validation_path.read_text(encoding="utf-8"),
        validation_values,
        validation_path,
    )
    current_ruby = ruby_path.read_text(encoding="utf-8")
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\n?", current_ruby) is None:
        raise ValueError(f"{ruby_path} must contain exactly one stable X.Y.Z version")

    pin_environment = os.environ.copy()
    pin_environment["RUBY_VERSION_PIN"] = ruby_version
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/update/pin_ruby_version.py"),
            str(ruby_path),
        ],
        cwd=ROOT,
        env=pin_environment,
        check=True,
    )
    write_atomically(action_path, action_content)
    write_atomically(validation_path, validation_content)

    print(
        f"Resolved Xray {action_values['XRAY_VERSION_PIN']}, "
        f"sing-box {action_values['SINGBOX_VERSION_PIN']}, "
        f"Mihomo {action_values['MIHOMO_VERSION_PIN']}, and Ruby {ruby_version}; "
        "required release asset SHA-256 digests were verified."
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"[ERROR] Core pin update failed: {error}") from error
