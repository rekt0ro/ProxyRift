#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/update/prepare_published_outputs.py"

SINGBOX_STUB = """#!/usr/bin/env python3
import json, os, sys
if sys.argv[1:2] != ["check"] or "-c" not in sys.argv:
    print("[mock] expected check -c <config>", file=sys.stderr); raise SystemExit(2)
if os.environ.get("MOCK_SINGBOX_FAIL") == "true":
    print("[mock] schema rejection", file=sys.stderr); raise SystemExit(1)
try:
    with open(sys.argv[sys.argv.index("-c") + 1], encoding="utf-8") as stream:
        doc = json.load(stream)
    assert isinstance(doc, dict) and isinstance(doc.get("outbounds"), list) and doc["outbounds"]
    assert all(isinstance(row, dict) and isinstance(row.get("type"), str) and row["type"] and isinstance(row.get("tag"), str) and row["tag"] for row in doc["outbounds"])
except Exception as exc:
    print(str(exc), file=sys.stderr); raise SystemExit(1)
"""

MIHOMO_STUB = """#!/usr/bin/env python3
import os, re, sys
if sys.argv[1:3] != ["-t", "-f"] or len(sys.argv) < 4 or not os.path.isfile(sys.argv[3]):
    print("[mock] expected -t -f <config>", file=sys.stderr); raise SystemExit(2)
if os.environ.get("MOCK_MIHOMO_FAIL") == "true":
    print("[mock] invalid Clash configuration", file=sys.stderr); raise SystemExit(1)
reject_name = os.environ.get("MOCK_MIHOMO_REJECT_NAME")
if reject_name:
    content = open(sys.argv[3], encoding="utf-8").read()
    names = [name.strip().strip("'\\\"") for name in re.findall(r"(?m)^  - name: (.*)$", content)]
    if reject_name in names:
        print(f"proxy {names.index(reject_name)}: mocked incompatible proxy", file=sys.stderr)
        raise SystemExit(1)
"""


class PublicationGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.work = self.root / "work"
        self.subscriptions = self.work / "subscriptions"
        self.bin = self.root / "bin"
        self.subscriptions.mkdir(parents=True)
        self.bin.mkdir()
        for name, content in (("sing-box", SINGBOX_STUB), ("mihomo", MIHOMO_STUB)):
            path = self.bin / name
            path.write_text(content, encoding="utf-8")
            path.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update({
            "PATH": f"{self.bin}:{self.env.get('PATH', '')}",
            "SINGBOX_BINARY": str(self.bin / "sing-box"),
            "MIHOMO_BINARY": str(self.bin / "mihomo"),
            "LIGHT_MIN_PUBLISH": "50",
            "ALL_MIN_PUBLISH": "1",
            "LIGHT_SELECTION_LIMIT": "200",
            "ALL_MAX_PUBLISH": "2000",
        })

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_outputs(self, light: int = 50, all_count: int = 1) -> None:
        counts: dict[str, dict[str, int]] = {}
        for name, number, scheme in (("light", light, "vless"), ("all", all_count, "socks5")):
            lines = [f"{scheme}://test-{index}@example.com:443" for index in range(1, number + 1)]
            source = ("\n".join(lines) + "\n").encode()
            (self.subscriptions / f"{name}.txt").write_bytes(source)
            (self.subscriptions / f"{name}-base64.txt").write_bytes(base64.b64encode(source.rstrip(b"\r\n")) + b"\n")
            outbounds = [{"type": "direct", "tag": f"test-{index}"} for index in range(1, number + 1)]
            (self.subscriptions / f"{name}-singbox.json").write_text(json.dumps({"outbounds": outbounds}), encoding="utf-8")
            yaml_lines = ["proxies:"]
            for index in range(1, number + 1):
                yaml_lines.extend([
                    f"  - name: 'test proxy {index}'",
                    "    type: socks5",
                    "    server: example.com",
                    "    port: 1080",
                ])
            (self.subscriptions / f"{name}-clash.yaml").write_text("\n".join(yaml_lines) + "\n", encoding="utf-8")
            counts[name] = {"source": number, "clash": number, "singbox": number}
        (self.subscriptions / ".output-counts.json").write_text(json.dumps(counts), encoding="utf-8")

    def run_gate(self, **updates: str) -> subprocess.CompletedProcess[str]:
        env = self.env.copy()
        env.update(updates)
        return subprocess.run(
            [sys.executable, str(SCRIPT)], cwd=self.work, env=env,
            text=True, capture_output=True, check=False,
        )

    def publication_manifest(self) -> dict[str, object]:
        return json.loads((self.subscriptions / ".publication-manifest.json").read_text(encoding="utf-8"))

    def test_valid_outputs_pass(self) -> None:
        self.make_outputs()
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("Clash/Mihomo: validated", result.stdout)
        self.assertEqual(
            set(self.publication_manifest()["files"]),
            {
                "all.txt", "light.txt", "all-base64.txt", "light-base64.txt",
                "all-clash.yaml", "light-clash.yaml", "all-singbox.json", "light-singbox.json",
            },
        )

    def test_yaml_structure_error_skips_clash_without_blocking_publication(self) -> None:
        self.make_outputs()
        path = self.subscriptions / "light-clash.yaml"
        path.write_text(path.read_text().replace("    port: 1080\n", ""), encoding="utf-8")
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("structural validation failed", result.stderr)
        selected = set(self.publication_manifest()["files"])
        self.assertNotIn("all-clash.yaml", selected)
        self.assertNotIn("light-clash.yaml", selected)
        self.assertIn("clash", self.publication_manifest()["skipped_formats"])

    def test_singbox_rejection_blocks_publication(self) -> None:
        self.make_outputs()
        result = self.run_gate(MOCK_SINGBOX_FAIL="true")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("sing-box rejected configuration", result.stderr)

    def test_mihomo_rejection_skips_clash_without_blocking_publication(self) -> None:
        self.make_outputs()
        result = self.run_gate(MOCK_MIHOMO_FAIL="true")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("Mihomo rejected", result.stderr)
        selected = set(self.publication_manifest()["files"])
        self.assertNotIn("all-clash.yaml", selected)
        self.assertNotIn("light-clash.yaml", selected)

    def test_mihomo_rejection_removes_only_the_invalid_proxy(self) -> None:
        self.make_outputs()
        result = self.run_gate(MOCK_MIHOMO_REJECT_NAME="test proxy 2")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("Removed rejected proxy 2", result.stderr)

        light = (self.subscriptions / "light-clash.yaml").read_text(encoding="utf-8")
        self.assertIn("test proxy 1", light)
        self.assertNotIn("- name: test proxy 2\n", light)
        self.assertIn("test proxy 3", light)

        all_output = (self.subscriptions / "all-clash.yaml").read_text(encoding="utf-8")
        self.assertIn("test proxy 1", all_output)
        self.assertEqual(
            set(self.publication_manifest()["files"]),
            {
                "all.txt", "light.txt", "all-base64.txt", "light-base64.txt",
                "all-clash.yaml", "light-clash.yaml", "all-singbox.json", "light-singbox.json",
            },
        )
        counts = json.loads((self.subscriptions / ".output-counts.json").read_text(encoding="utf-8"))
        self.assertEqual(counts["light"]["clash"], 49)
        self.assertEqual(counts["all"]["clash"], 1)

    def test_missing_mihomo_binary_skips_clash_without_blocking_publication(self) -> None:
        self.make_outputs()
        result = self.run_gate(MIHOMO_BINARY=str(self.bin / "missing-mihomo"))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("Mihomo validator is missing", result.stderr)
        selected = set(self.publication_manifest()["files"])
        self.assertNotIn("all-clash.yaml", selected)
        self.assertNotIn("light-clash.yaml", selected)

    def test_missing_clash_count_skips_both_clash_exports(self) -> None:
        self.make_outputs()
        path = self.subscriptions / ".output-counts.json"
        counts = json.loads(path.read_text(encoding="utf-8"))
        counts["light"]["clash"] = None
        path.write_text(json.dumps(counts), encoding="utf-8")
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("invalid or missing Clash entry count", result.stderr)
        selected = set(self.publication_manifest()["files"])
        self.assertNotIn("all-clash.yaml", selected)
        self.assertNotIn("light-clash.yaml", selected)

    def test_light_above_maximum_is_rejected(self) -> None:
        self.make_outputs(light=201)
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("maximum is 200", result.stderr)

    def test_all_below_minimum_is_rejected(self) -> None:
        self.make_outputs(all_count=999)
        result = self.run_gate(ALL_MIN_PUBLISH="1000")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("minimum is 1000", result.stderr)

    def test_invalid_base64_is_rejected(self) -> None:
        self.make_outputs()
        (self.subscriptions / "light-base64.txt").write_text("not-base64!\n", encoding="utf-8")
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Base64 output cannot be decoded", result.stderr)

    def test_invalid_singbox_json_is_rejected(self) -> None:
        self.make_outputs()
        (self.subscriptions / "light-singbox.json").write_text("{broken json\n", encoding="utf-8")
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid sing-box JSON", result.stderr)

    def test_unaccounted_manifest_count_is_rejected(self) -> None:
        self.make_outputs()
        path = self.subscriptions / ".output-counts.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["light"]["singbox"] = 1
        path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("sing-box entry count does not match format-count manifest", result.stderr)

    def test_light_exact_production_target_passes(self) -> None:
        self.make_outputs(light=200)
        result = self.run_gate(LIGHT_MIN_PUBLISH="200")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)


if __name__ == "__main__":
    unittest.main()
