#!/usr/bin/env python3
from __future__ import annotations

import unittest

from resolve_stable_rust import resolve_version


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


if __name__ == "__main__":
    unittest.main()
