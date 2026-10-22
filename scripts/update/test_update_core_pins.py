#!/usr/bin/env python3
"""Unit tests for stable-release validation and pin replacement."""

import stat
import tempfile
import unittest
from pathlib import Path

from update_core_pins import asset_sha256, build_updates, replace_pins, stable_version, write_atomically


def release(tag, assets=()):
    return {
        "tag_name": tag,
        "assets": [{"name": name, "digest": digest} for name, digest in assets],
    }


class CorePinUpdaterTests(unittest.TestCase):
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
            "ruby": release("v4.5.6"),
        }
        ruby_version, action, validation = build_updates(releases)
        self.assertEqual(ruby_version, "4.5.6")
        self.assertEqual(action["SINGBOX_ASSET_PIN"], "sing-box-2.3.4-linux-amd64.tar.gz")
        self.assertEqual(action["MIHOMO_SHA256_PIN"], "3" * 64)
        self.assertEqual(validation["MIHOMO_VERSION"], "3.4.5")
        self.assertEqual(validation["MIHOMO_SHA256"], action["MIHOMO_SHA256_PIN"])

    def test_atomic_write_preserves_file_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pins.yml"
            path.write_text('pin: "old"\\n', encoding="utf-8")
            path.chmod(0o640)

            write_atomically(path, 'pin: "new"\\n')

            self.assertEqual(path.read_text(encoding="utf-8"), 'pin: "new"\\n')
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
