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

from resolve_stable_rust import fetch_latest_stable_version, resolve_pinned_version
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
    "Rust": "rust-lang/rust",
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
    )
    matches = pattern.findall(content)
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one {key} pin in {path}, found {len(matches)}")
    return matches[0]


def python_release_url(version):
    compact_version = version.replace(".", "")
    return f"https://www.python.org/downloads/release/python-{compact_version}/"


def release_notes_url(component, version, release):
    release_tag = release.get("tag_name") if isinstance(release, dict) else None
    release_url = release.get("html_url") if isinstance(release, dict) else None
    if (
        isinstance(release_url, str)
        and release_url.startswith("https://")
        and release_tag in (f"v{version}", version)
    ):
        return release_url
    if component == "Python":
        return python_release_url(version)
    repository = RELEASE_REPOSITORIES[component]
    tag = version if component == "Rust" else f"v{version}"
    return f"https://github.com/{repository}/releases/tag/{tag}"


def release_note_excerpt(body, limit=2400):
    if not isinstance(body, str):
        return ""
    body = body.strip()
    if len(body) <= limit:
        return body
    cutoff = body.rfind("\n", 0, limit)
    if cutoff < limit // 2:
        cutoff = limit
    suffix = "\n\n_Release notes excerpt shortened; see the official release link above for the complete notes._"
    return body[:cutoff].rstrip() + suffix


def build_pr_description(previous_versions, current_versions, release_metadata):
    components = ("Python", "Ruby", "Rust", "Xray-core", "sing-box", "Mihomo")
    changed = [
        (name, previous_versions[name], current_versions[name])
        for name in components
        if previous_versions[name] != current_versions[name]
    ]
    if len(changed) == 1:
        name, old, new = changed[0]
        component_slug = {
            "Python": "python",
            "Ruby": "ruby",
            "Xray-core": "xray-core",
            "sing-box": "sing-box",
            "Mihomo": "mihomo",
        }[name]
        title = f"Bump {component_slug} from {old} to {new}"
    elif changed:
        names = {name for name, _, _ in changed}
        toolchain = {"Python", "Ruby", "Rust"}
        cores = {"Xray-core", "sing-box", "Mihomo"}
        if names <= toolchain:
            group = "toolchain"
        elif names <= cores:
            group = "core"
        else:
            group = "core/runtime"
        title = f"Bump the {group} group with {len(changed)} updates"
    else:
        title = "Bump core/runtime pins"

    mark = chr(96)
    lines = [
        "## Updates",
        "",
        "| Component | Previous | Updated | Changelog |",
        "| --- | --- | --- | --- |",
    ]
    for name, old, new in changed:
        release = release_metadata.get(name, {})
        notes_url = release_notes_url(name, new, release)
        if name == "Python":
            old_link = python_release_url(old)
            changelog = f"[Release notes]({notes_url}) · [Previous release]({old_link})"
        else:
            repository = RELEASE_REPOSITORIES[name]
            old_tag = old if name == "Rust" else f"v{old}"
            new_tag = new if name == "Rust" else f"v{new}"
            compare_url = f"https://github.com/{repository}/compare/{old_tag}...{new_tag}"
            changelog = (
                f"[Release notes]({notes_url}) · "
                f"[Full changelog]({compare_url})"
            )
        lines.append(
            f"| {name} | {mark}{old}{mark} | {mark}{new}{mark} | {changelog} |"
        )

    if not changed:
        lines.extend(["", "No pinned versions changed."])
    else:
        lines.extend(["", "## Release notes"])
        for name, old, new in changed:
            release = release_metadata.get(name, {})
            notes_url = release_notes_url(name, new, release)
            lines.extend(
                [
                    "",
                    f"### {name} {new}",
                    "",
                    f"[Official release notes]({notes_url})",
                ]
            )
            excerpt = release_note_excerpt(release.get("body"))
            if excerpt:
                lines.extend(["", excerpt])

    return title, "\n".join(lines).rstrip() + "\n"


def write_pr_metadata(title, body):
    body_path = os.environ.get("CORE_PIN_PR_BODY_PATH")
    if body_path:
        target = Path(body_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"pr_title={title}\n")


def replace_rust_channel(content, version, path):
    if not isinstance(version, str) or re.fullmatch(r"1\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError(f"Invalid stable Rust version: {version!r}")
    pattern = re.compile(
        r'(?m)^([ \t]*channel[ \t]*=[ \t]*)"[^"\r\n]*"([ \t]*(?:#.*)?)$'
    )
    updated, count = pattern.subn(
        lambda match: f'{match.group(1)}"{version}"{match.group(2)}',
        content,
    )
    if count != 1:
        raise ValueError(f"Expected exactly one toolchain channel in {path}, found {count}")
    return updated


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


def build_updates(releases, python_version, ruby_version):
    if not isinstance(python_version, str) or PLAIN_VERSION_PATTERN.fullmatch(python_version) is None:
        raise ValueError(f"Invalid stable Python version: {python_version!r}")
    if not isinstance(ruby_version, str) or PLAIN_VERSION_PATTERN.fullmatch(ruby_version) is None:
        raise ValueError(f"Invalid supported Ruby version: {ruby_version!r}")
    xray_version = stable_version(releases["xray"], "Xray")
    singbox_version = stable_version(releases["singbox"], "sing-box")
    mihomo_version = stable_version(releases["mihomo"], "Mihomo")

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
    return python_version, ruby_version, action_values


def main():
    token = os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("GH_TOKEN is required to query GitHub Releases")

    repositories = {
        "xray": "XTLS/Xray-core",
        "singbox": "SagerNet/sing-box",
        "mihomo": "MetaCubeX/mihomo",
    }
    releases = {
        name: fetch_release(repository, token)
        for name, repository in repositories.items()
    }
    python_version = fetch_latest_python_version()
    ruby_version = fetch_latest_supported_ruby_version()
    python_version, ruby_version, action_values = build_updates(
        releases, python_version, ruby_version
    )

    action_path = ROOT / ".github/actions/prepare-update/action.yml"
    python_path = ROOT / ".python-version"
    ruby_path = ROOT / ".ruby-version"

    action_source = action_path.read_text(encoding="utf-8")
    action_content = replace_pins(action_source, action_values, action_path)
    current_python = python_path.read_text(encoding="utf-8")
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\n?", current_python) is None:
        raise ValueError(f"{python_path} must contain exactly one stable X.Y.Z version")
    current_ruby = ruby_path.read_text(encoding="utf-8")
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\n?", current_ruby) is None:
        raise ValueError(f"{ruby_path} must contain exactly one stable X.Y.Z version")

    previous_versions = {
        "Python": current_python.strip(),
        "Ruby": current_ruby.strip(),
        "Xray-core": pinned_value(action_source, "XRAY_VERSION_PIN", action_path),
        "sing-box": pinned_value(action_source, "SINGBOX_VERSION_PIN", action_path),
        "Mihomo": pinned_value(action_source, "MIHOMO_VERSION_PIN", action_path),
    }
    current_versions = {
        "Python": python_version,
        "Ruby": ruby_version,
        "Xray-core": action_values["XRAY_VERSION_PIN"],
        "sing-box": action_values["SINGBOX_VERSION_PIN"],
        "Mihomo": action_values["MIHOMO_VERSION_PIN"],
    }
    release_metadata = {
        "Xray-core": releases["xray"],
        "sing-box": releases["singbox"],
        "Mihomo": releases["mihomo"],
    }
    if previous_versions["Ruby"] != ruby_version:
        release_metadata["Ruby"] = fetch_release_by_tag(
            "ruby/ruby", f"v{ruby_version}", token
        )
    pr_title, pr_body = build_pr_description(
        previous_versions, current_versions, release_metadata
    )

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
    write_atomically(python_path, f"{python_version}\n")
    write_pr_metadata(pr_title, pr_body)

    print(
        f"Resolved Python {python_version}, Ruby {ruby_version}, "
        f"Xray {action_values['XRAY_VERSION_PIN']}, "
        f"sing-box {action_values['SINGBOX_VERSION_PIN']}, "
        f"and Mihomo {action_values['MIHOMO_VERSION_PIN']}; "
        "required release asset SHA-256 digests were verified."
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"[ERROR] Core pin update failed: {error}") from error
