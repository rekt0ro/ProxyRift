#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from install_lightgbm import resolve_release, write_verified_asset


class LightGBMReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = {
            "tag_name": "v4.6.0",
            "assets": [
                {
                    "name": "lib_lightgbm.so",
                    "digest": "sha256:" + "a" * 64,
                    "browser_download_url": "https://github.com/lightgbm-org/LightGBM/releases/download/v4.6.0/lib_lightgbm.so",
                }
            ],
        }

    def test_resolves_version_url_and_digest(self) -> None:
        self.assertEqual(
            resolve_release(self.release),
            (
                "4.6.0",
                "https://github.com/lightgbm-org/LightGBM/releases/download/v4.6.0/lib_lightgbm.so",
                "a" * 64,
            ),
        )

    def test_rejects_invalid_release_tag(self) -> None:
        self.release["tag_name"] = "latest"
        with self.assertRaisesRegex(ValueError, "Invalid latest release tag"):
            resolve_release(self.release)

    def test_rejects_missing_asset(self) -> None:
        self.release["assets"] = []
        with self.assertRaisesRegex(ValueError, "Missing release asset"):
            resolve_release(self.release)

    def test_rejects_invalid_digest(self) -> None:
        self.release["assets"][0]["digest"] = "sha256:bad"
        with self.assertRaisesRegex(ValueError, "SHA-256 digest"):
            resolve_release(self.release)

    def test_rejects_untrusted_download_url(self) -> None:
        self.release["assets"][0]["browser_download_url"] = "https://example.com/lib_lightgbm.so"
        with self.assertRaisesRegex(ValueError, "download URL"):
            resolve_release(self.release)

    def test_verifies_digest_before_replacing_destination(self) -> None:
        content = b"lightgbm"
        digest = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "lib_lightgbm.so"
            write_verified_asset(content, destination, digest)
            self.assertEqual(destination.read_bytes(), content)

    def test_does_not_replace_destination_on_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "lib_lightgbm.so"
            destination.write_bytes(b"old")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                write_verified_asset(b"new", destination, "0" * 64)
            self.assertEqual(destination.read_bytes(), b"old")


if __name__ == "__main__":
    unittest.main()
