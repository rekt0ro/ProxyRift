#!/usr/bin/env python3
from __future__ import annotations

import unittest

from unittest.mock import patch

from resolve_stable_rust import fetch_latest_stable_version, resolve_pinned_version, resolve_version


class StableRustVersionTests(unittest.TestCase):
    def test_extracts_semver_from_channel_metadata(self) -> None:
        channel = '''
[pkg.rustc]
version = "1.91.0 (abcdef123 2026-10-24)"
'''
        self.assertEqual(resolve_version(channel), "1.91.0")

    def test_rejects_non_stable_version(self) -> None:
        channel = '''
[pkg.rustc]
version = "2.0.0 (abcdef123 2026-10-24)"
'''
        with self.assertRaisesRegex(ValueError, "Invalid stable Rust compiler version"):
            resolve_version(channel)

    def test_rejects_missing_rustc_section(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid stable Rust compiler version"):
            resolve_version("[pkg.cargo]\nversion = \"1.91.0\"\n")

    def test_rejects_malformed_toml(self) -> None:
        with self.assertRaisesRegex(ValueError, "not valid TOML"):
            resolve_version("[pkg.rustc\nversion = \"1.91.0\"\n")


    def test_reads_exact_pinned_toolchain_version(self) -> None:
        self.assertEqual(
            resolve_pinned_version('[toolchain]\nchannel = "1.99.1"\nprofile = "minimal"\n'),
            "1.99.1",
        )

    def test_rejects_moving_rust_channels_as_pins(self) -> None:
        for channel in ("stable", "beta", "nightly", "1.99", "1.99.1-rc1"):
            with self.subTest(channel=channel):
                with self.assertRaisesRegex(ValueError, "Invalid pinned Rust toolchain version"):
                    resolve_pinned_version(f'[toolchain]\nchannel = "{channel}"\n')

    def test_rejects_invalid_toolchain_pin_toml(self) -> None:
        with self.assertRaisesRegex(ValueError, "not valid TOML"):
            resolve_pinned_version('[toolchain\nchannel = "1.99.1"\n')

    def test_fetches_latest_version_from_stable_channel_document(self) -> None:
        channel = b'[pkg.rustc]\nversion = "1.99.1 (abcdef123 2026-10-15)"\n'
        with patch("resolve_stable_rust.download_channel", return_value=channel):
            self.assertEqual(fetch_latest_stable_version(), "1.99.1")


if __name__ == "__main__":
    unittest.main()
