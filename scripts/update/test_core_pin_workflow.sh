#!/usr/bin/env bash
set -euo pipefail

ruby - <<'RUBY'
require "tempfile"
require "yaml"

path = ".github/workflows/update-core-pins.yml"
workflow = YAML.load_file(path)
step = workflow.dig("jobs", "update-pins", "steps").find do |entry|
  entry["name"] == "Resolve latest stable releases and update pins"
end
abort "Core-pin updater shell step is missing" unless step && step["run"].is_a?(String)

Tempfile.create(["core-pin-updater", ".sh"]) do |file|
  file.write(step.fetch("run"))
  file.flush
  abort "Core-pin updater shell script has invalid Bash syntax" unless system("bash", "-n", file.path)
end
RUBY

python3 - <<'PY'
from pathlib import Path

path = Path(".github/workflows/update-core-pins.yml")
content = path.read_text(encoding="utf-8")
if content.count("                  content, count = re.subn(") != 1:
    raise SystemExit("Core-pin workflow contains duplicated Python commands")
if content.rstrip().splitlines()[-1] != "            --ref main":
    raise SystemExit("Unexpected content after the final core-pin workflow step")
attributes = Path(".gitattributes").read_text(encoding="utf-8")
if "scripts/update/validate_clash_output.rb linguist-language=Ruby" not in attributes:
    raise SystemExit("Ruby language override is missing")
print("Core-pin workflow and Ruby language override are valid.")
PY
