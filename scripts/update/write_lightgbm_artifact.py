#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

from lightgbm_report import artifact_document, load_json, validate_artifact_document, validate_score_document


def main() -> int:
    temp_root = Path(os.environ.get("PROXYRIFT_TMP_DIR", "/tmp/proxyrift"))
    runner_temp = Path(os.environ.get("RUNNER_TEMP", os.environ.get("TMPDIR", "/tmp")))
    score_path = Path(sys.argv[1]) if len(sys.argv) > 1 else temp_root / "lightgbm-scores.json"
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else runner_temp / "lightgbm-evaluation.json"
    if len(sys.argv) > 3:
        print(f"[ERROR] Usage: {Path(sys.argv[0]).name} [SCORE_FILE [OUTPUT_FILE]]", file=sys.stderr)
        return 2
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if not score_path.is_file() or score_path.stat().st_size == 0:
            document = {"schema_version": 2, "available": False, "reason": "LightGBM speed-regression report was not produced"}
            print("[INFO] LightGBM report unavailable; wrote an availability marker.")
        else:
            score = load_json(score_path)
            if not validate_score_document(score):
                print("[ERROR] LightGBM score report has an invalid stream-speed regression schema.", file=sys.stderr)
                return 1
            document = artifact_document(score)
            if not validate_artifact_document(document):
                print("[ERROR] Sanitized LightGBM artifact unexpectedly contains raw score or candidate fields.", file=sys.stderr)
                return 1
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output_path.parent, prefix=f".{output_path.name}.", suffix=".tmp", delete=False) as handle:
            temp_path = Path(handle.name)
            json.dump(document, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
        os.replace(temp_path, output_path)
    except OSError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    if document.get("available") is True:
        print(f"[OK] Wrote sanitized LightGBM speed-regression report to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
