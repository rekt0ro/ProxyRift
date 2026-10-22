#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
from pathlib import Path

path = Path(".github/workflows/update-core-pins.yml")
content = path.read_text(encoding="utf-8")
if content.count("                  content, count = re.subn(") != 1:
    raise SystemExit("Core-pin workflow contains duplicated Python commands")
if content.rstrip().splitlines()[-1] != "            --ref main":
    raise SystemExit("Unexpected content after the final core-pin workflow step")
print("Core-pin workflow tail is valid.")
PY
