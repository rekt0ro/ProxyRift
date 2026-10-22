#!/usr/bin/env ruby

require "tempfile"
require "yaml"

workflow_path = ".github/workflows/update-core-pins.yml"
workflow = YAML.load_file(workflow_path)
step = workflow.dig("jobs", "update-pins", "steps").find do |entry|
  entry["name"] == "Resolve latest stable releases and update pins"
end

abort "Core-pin updater shell step is missing" unless step && step["run"].is_a?(String)
abort "Core-pin updater must call the standalone Python module" unless
  step["run"].include?("python3 scripts/update/update_core_pins.py")
abort "Core-pin workflow must not embed Python source" if step["run"].include?("python3 - <<'PY'")

create_pr = workflow.dig("jobs", "update-pins", "steps").find do |entry|
  entry["name"] == "Create or update core-pin pull request"
end
abort "Core-pin updater must not stage workflow files without workflow permission" if
  create_pr.dig("with", "add-paths").lines.any? { |line| line.strip.start_with?(".github/workflows/") }
abort "Validation core-pin loader is missing" unless
  File.file?("scripts/update/load_validation_core_pins.rb")

Tempfile.create(["core-pin-updater", ".sh"]) do |file|
  file.write(step.fetch("run"))
  file.flush
  abort "Core-pin updater shell syntax is invalid" unless system("bash", "-n", file.path)
end

attributes = File.read(".gitattributes", encoding: "UTF-8")
abort "Ruby Linguist override is missing" unless
  attributes.include?("scripts/update/validate_clash_output.rb linguist-language=Ruby")

puts "Core-pin workflow shell syntax and Ruby language override are valid."
