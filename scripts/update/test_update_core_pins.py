#!/usr/bin/env python3
"""Unit tests for stable-release validation and pin replacement."""

import gzip
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import update_core_pins

from update_core_pins import (
    asset_sha256,
    build_pr_description,
    build_updates,
    latest_python_version,
    latest_supported_ruby_version,
    replace_pins,
    stable_version,
    write_atomically,
)


PYTHON_DOWNLOADS_HTML = b"""<!doctype html>
<html><body>
<a href="/downloads/release/python-3151/">Python 3.15.1</a>
<a href="/downloads/release/python-3149/">Python 3.14.9</a>
</body></html>
"""


class FakeResponse:
    def __init__(self, body, headers=None):
        self._body = body
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self._body


def release(tag, assets=()):
    return {
        "tag_name": tag,
        "assets": [{"name": name, "digest": digest} for name, digest in assets],
    }


class CorePinUpdaterTests(unittest.TestCase):

    def test_selects_latest_stable_ruby_release_supported_by_setup_ruby(self):
        manifest = {
            "ruby": [
                "3.4.11",
                "4.0.6",
                "4.1.0-preview1",
                "4.0.7",
                "head",
            ]
        }
        self.assertEqual(latest_supported_ruby_version(manifest), "4.0.7")

    def test_rejects_ruby_manifest_without_stable_releases(self):
        for manifest in ({}, {"ruby": ["4.1.0-preview1", "head"]}):
            with self.subTest(manifest=manifest):
                with self.assertRaises(ValueError):
                    latest_supported_ruby_version(manifest)

    def test_fetches_latest_supported_ruby_version(self):
        manifest = {"ruby": ["4.0.6", "4.1.0-preview1", "4.0.7"]}
        response = FakeResponse(json.dumps(manifest).encode("utf-8"))
        with patch.object(update_core_pins, "urlopen", return_value=response):
            self.assertEqual(
                update_core_pins.fetch_latest_supported_ruby_version(),
                "4.0.7",
            )

    def test_builds_release_note_pr_body_for_only_changed_versions(self):
        previous = {
            "Python": "3.15.1",
            "Ruby": "4.0.7",
            "Xray-core": "26.3.26",
            "sing-box": "1.14.3",
            "Mihomo": "1.19.32",
        }
        current = {
            "Python": "3.15.1",
            "Ruby": "4.0.8",
            "Xray-core": "26.3.26",
            "sing-box": "1.14.3",
            "Mihomo": "1.19.33",
        }
        releases = {
            "Ruby": {
                "tag_name": "v4.0.8",
                "html_url": "https://github.com/ruby/ruby/releases/tag/v4.0.8",
                "body": "## What's Changed\n\n* Fixed a runtime bug.",
            },
            "Mihomo": {
                "tag_name": "v1.19.33",
                "html_url": "https://github.com/MetaCubeX/mihomo/releases/tag/v1.19.33",
                "body": "## Bug fixes\n\n* Fixed a connection issue.",
            },
        }

        title, body = build_pr_description(previous, current, releases)

        self.assertEqual(
            title,
            "Bump the core/runtime group with 2 updates",
        )
        mark = chr(96)
        self.assertIn(f"| Ruby | {mark}4.0.7{mark} | {mark}4.0.8{mark} |", body)
        self.assertIn("https://github.com/ruby/ruby/compare/v4.0.7...v4.0.8", body)
        self.assertIn("https://github.com/MetaCubeX/mihomo/compare/v1.19.32...v1.19.33", body)
        self.assertIn("Fixed a runtime bug.", body)
        self.assertIn("Fixed a connection issue.", body)
        self.assertNotIn("| Python |", body)
        self.assertNotIn("Python 3.15.1", body)
        self.assertNotIn("Versions and release notes are collected", body)

    def test_pr_description_links_python_release_notes_and_previous_version(self):
        previous = {
            "Python": "3.14.9",
            "Ruby": "4.0.7",
            "Xray-core": "26.3.26",
            "sing-box": "1.14.3",
            "Mihomo": "1.19.32",
        }
        current = dict(previous, **{"Python": "3.15.1"})

        title, body = build_pr_description(previous, current, {})

        self.assertEqual(title, "Bump python from 3.14.9 to 3.15.1")
        self.assertIn(
            "https://www.python.org/downloads/release/python-3151/",
            body,
        )
        self.assertIn(
            "https://www.python.org/downloads/release/python-3149/",
            body,
        )

    def test_uses_dependabot_style_group_title_for_toolchain_updates(self):
        previous = {
            "Python": "3.14.9",
            "Ruby": "4.0.7",
            "Xray-core": "26.3.26",
            "sing-box": "1.14.3",
            "Mihomo": "1.19.32",
        }
        current = dict(previous, **{"Python": "3.15.1", "Ruby": "4.0.8"})

        title, _ = build_pr_description(previous, current, {})

        self.assertEqual(title, "Bump the toolchain group with 2 updates")

    def test_uses_dependabot_style_group_title_for_core_updates(self):
        previous = {
            "Python": "3.14.9",
            "Ruby": "4.0.7",
            "Xray-core": "26.3.26",
            "sing-box": "1.14.3",
            "Mihomo": "1.19.32",
        }
        current = dict(previous, **{"Xray-core": "26.3.27", "Mihomo": "1.19.33"})

        title, _ = build_pr_description(previous, current, {})

        self.assertEqual(title, "Bump the core group with 2 updates")

    def test_release_note_excerpt_is_bounded(self):
        excerpt = update_core_pins.release_note_excerpt("A" * 4000, limit=200)

        self.assertLess(len(excerpt), 300)
        self.assertIn("excerpt shortened", excerpt)

    def test_fetches_github_release_notes_by_tag(self):
        payload = {
            "tag_name": "v4.0.8",
            "html_url": "https://github.com/ruby/ruby/releases/tag/v4.0.8",
            "body": "Release notes",
        }
        response = FakeResponse(json.dumps(payload).encode("utf-8"))
        with patch.object(update_core_pins, "urlopen", return_value=response):
            self.assertEqual(
                update_core_pins.fetch_release_by_tag("ruby/ruby", "v4.0.8", "token"),
                payload,
            )

    def test_fetches_gzip_python_release_page_from_header(self):
        response = FakeResponse(
            gzip.compress(PYTHON_DOWNLOADS_HTML),
            {"Content-Encoding": "gzip"},
        )
        with patch.object(update_core_pins, "urlopen", return_value=response):
            self.assertEqual(
                update_core_pins.fetch_latest_python_version(),
                "3.15.1",
            )

    def test_detects_gzip_body_without_content_encoding_header(self):
        response = FakeResponse(gzip.compress(PYTHON_DOWNLOADS_HTML))
        with patch.object(update_core_pins, "urlopen", return_value=response):
            self.assertEqual(
                update_core_pins.fetch_latest_python_version(),
                "3.15.1",
            )

    def test_resolves_latest_python_stable_release_and_skips_pre_releases(self):
        page = """
        <html><body>
          <a href="/downloads/release/python-3151/">Python 3.15.1</a>
          <a href="/downloads/release/python-3149/">Python 3.14.9</a>
          <a href="/downloads/release/python-3160a1/">Python 3.16.0a1</a>
        </body></html>
        """
        self.assertEqual(latest_python_version(page), "3.15.1")

    def test_rejects_python_page_without_stable_release(self):
        with self.assertRaises(ValueError):
            latest_python_version('<a href="/downloads/release/python-3160a1/">Python 3.16.0a1</a>')

    def test_accepts_stable_version_tags(self):
        self.assertEqual(stable_version({"tag_name": "v1.2.3"}, "sample"), "1.2.3")

    def test_rejects_preview_and_malformed_tags(self):
        for tag in ("1.2.3", "v1.2", "v1.2.3-rc1", "nightly"):
            with self.subTest(tag=tag):
                with self.assertRaises(ValueError):
                    stable_version({"tag_name": tag}, "sample")

    def test_requires_one_asset_with_sha256_digest(self):
        valid = release("v1.2.3", [("sample.tar.gz", "sha256:" + "a" * 64)])
        self.assertEqual(asset_sha256(valid, "sample.tar.gz", "sample"), "a" * 64)
        with self.assertRaises(ValueError):
            asset_sha256(valid, "missing.tar.gz", "sample")
        invalid = release("v1.2.3", [("sample.tar.gz", "sha1:deadbeef")])
        with self.assertRaises(ValueError):
            asset_sha256(invalid, "sample.tar.gz", "sample")

    def test_builds_versioned_asset_names_and_matching_pins(self):
        releases = {
            "xray": release("v1.2.3", [("Xray-linux-64.zip", "sha256:" + "1" * 64)]),
            "singbox": release(
                "v2.3.4",
                [("sing-box-2.3.4-linux-amd64.tar.gz", "sha256:" + "2" * 64)],
            ),
            "mihomo": release(
                "v3.4.5",
                [("mihomo-linux-amd64-v3.4.5.gz", "sha256:" + "3" * 64)],
            ),
        }
        python_version, ruby_version, action = build_updates(releases, "3.15.1", "4.0.7")
        self.assertEqual(python_version, "3.15.1")
        self.assertEqual(ruby_version, "4.0.7")
        self.assertEqual(action["SINGBOX_ASSET_PIN"], "sing-box-2.3.4-linux-amd64.tar.gz")
        self.assertEqual(action["MIHOMO_SHA256_PIN"], "3" * 64)
        with self.assertRaises(ValueError):
            build_updates(releases, "3.15.1rc1", "4.0.7")
        with self.assertRaises(ValueError):
            build_updates(releases, "3.15.1", "4.1.0-preview1")

    def test_atomic_write_preserves_file_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pins.yml"
            path.write_text('pin: "old"\n', encoding="utf-8")
            path.chmod(0o640)

            write_atomically(path, 'pin: "new"\n')

            self.assertEqual(path.read_text(encoding="utf-8"), 'pin: "new"\n')
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)

    def test_replaces_exactly_one_pin_and_preserves_other_text(self):
        source = '  MIHOMO_VERSION: "1.0.0"\nnext: "unchanged"\n'
        self.assertEqual(
            replace_pins(source, {"MIHOMO_VERSION": "2.0.0"}, "sample.yml"),
            '  MIHOMO_VERSION: "2.0.0"\nnext: "unchanged"\n',
        )

    def test_rejects_missing_or_duplicate_pin_keys(self):
        for source in ('other: "1"\n', 'pin: "1"\npin: "2"\n'):
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    replace_pins(source, {"pin": "3"}, "sample.yml")


if __name__ == "__main__":
    unittest.main()
