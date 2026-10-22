#!/usr/bin/env ruby

require "tempfile"
require "tmpdir"
require "fileutils"
require "open3"
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

abort "Core-pin PR creation must support a token that does not require workflow approval" unless
  create_pr.dig("with", "token") == "${{ secrets.CORE_PIN_UPDATE_TOKEN || github.token }}"
abort "Core-pin updater must emit a PR title and changelog body from resolved versions" unless
  step["id"] == "resolve-pins" &&
    step.dig("env", "CORE_PIN_PR_BODY_PATH") == "${{ runner.temp }}/core-pin-pr-body.md" &&
    create_pr.dig("with", "title") == "${{ steps.resolve-pins.outputs.pr_title }}" &&
    create_pr.dig("with", "body-path") == "${{ runner.temp }}/core-pin-pr-body.md" &&
    step["run"].include?("python3 scripts/update/update_core_pins.py")

steps = workflow.dig("jobs", "update-pins", "steps")
label_setup = steps.find { |entry| entry["name"] == "Ensure selected update labels exist" }
label_step = steps.find { |entry| entry["name"] == "Determine labels for changed pins" }
label_sync = steps.find { |entry| entry["name"] == "Synchronize core-pin PR labels" }
expected_labels = %w[dependencies toolchain python ruby xray sing-box mihomo]

abort "Core-pin workflow must create only selected, recognized labels" unless
  label_setup && label_setup["run"].is_a?(String) &&
    label_setup["run"].include?('gh api --method POST') &&
    expected_labels.all? { |label| label_setup["run"].include?("#{label})") } &&
    label_setup.dig("env", "LABELS") == "${{ steps.labels.outputs.value }}"

abort "Core-pin workflow must delegate label classification to the tested resolver" unless
  label_step && label_step["run"].is_a?(String) &&
    label_step["run"].include?("bash scripts/update/resolve_core_pin_labels.sh")

abort "Core-pin PR labels must be synchronized to the exact calculated labels" unless
  label_sync && label_sync["run"].is_a?(String) &&
    label_sync["if"] == "steps.cpr.outputs.pull-request-number != ''" &&
    label_sync.dig("env", "SELECTED_LABELS") == "${{ steps.labels.outputs.value }}" &&
    label_sync["run"].include?("managed_labels=(dependencies toolchain python ruby xray sing-box mihomo)") &&
    label_sync["run"].include?("--add-label") &&
    label_sync["run"].include?("--remove-label") &&
    label_sync["run"].include?("gh pr view")

dispatch_step = steps.find { |entry| entry["name"] == "Dispatch validation CI for the generated PR" }
abort "Core-pin workflow must keep prior run IDs on one line for GITHUB_OUTPUT" unless
  dispatch_step && dispatch_step["run"].is_a?(String) &&
    dispatch_step["run"].include?("--json databaseId | jq -c '[.[].databaseId]'") &&
    dispatch_step["run"].include?('echo "prior-run-ids=$prior_run_ids" >> "$GITHUB_OUTPUT"')

abort "Core-pin updater must include the Python runtime pin" unless
  create_pr.dig("with", "add-paths").lines.any? { |line| line.strip == ".python-version" }
abort "Updater workflow must set up its pinned Python runtime" unless
  workflow.dig("jobs", "update-pins", "steps").any? do |entry|
    entry["uses"] == "actions/setup-python@v7" &&
      entry.dig("with", "python-version-file") == ".python-version"
  end
validation_workflow = YAML.load_file(".github/workflows/validate.yml")
abort "Validation workflow must use the pinned Python runtime" unless
  validation_workflow.dig("jobs", "validate", "steps").any? do |entry|
    entry["uses"] == "actions/setup-python@v7" &&
      entry.dig("with", "python-version-file") == ".python-version"
  end
abort "Both push and pull-request filters must include .python-version" unless
  File.read(".github/workflows/validate.yml").scan(/^\s*- "\.python-version"\s*$/).length == 2
abort "Validation core-pin loader is missing" unless
  File.file?("scripts/update/load_validation_core_pins.rb")

{
  "core-pin updater" => step.fetch("run"),
  "label creation" => label_setup.fetch("run"),
  "label calculation step" => label_step.fetch("run"),
  "label synchronization" => label_sync.fetch("run"),
  "label resolver" => File.read("scripts/update/resolve_core_pin_labels.sh", encoding: "UTF-8")
}.each do |name, script|
  Tempfile.create(["core-pin-workflow", ".sh"]) do |file|
    file.write(script)
    file.flush
    abort "#{name} shell syntax is invalid" unless system("bash", "-n", file.path)
  end
end

helper_path = File.expand_path("scripts/update/resolve_core_pin_labels.sh")
baseline_action = <<~YAML
  XRAY_VERSION_PIN: "1.0.0"
  XRAY_SHA256_PIN: "aaaa"
  XRAY_ASSET_PIN: "xray.zip"
  SINGBOX_VERSION_PIN: "1.0.0"
  SINGBOX_SHA256_PIN: "bbbb"
  SINGBOX_ASSET_PIN: "sing-box.tar.gz"
  MIHOMO_VERSION_PIN: "1.0.0"
  MIHOMO_SHA256_PIN: "cccc"
  MIHOMO_ASSET_PIN: "mihomo.gz"
YAML

resolve_labels = lambda do |changes|
  Dir.mktmpdir("proxyrift-label-test") do |directory|
    FileUtils.mkdir_p(File.join(directory, ".github/actions/prepare-update"))
    File.write(File.join(directory, ".python-version"), "3.14.0\n")
    File.write(File.join(directory, ".ruby-version"), "3.4.0\n")
    action_path = File.join(directory, ".github/actions/prepare-update/action.yml")
    File.write(action_path, baseline_action)

    abort "Could not initialize temporary Git repo" unless
      system("git", "-C", directory, "init", "--quiet")
    abort "Could not set temporary Git author" unless
      system("git", "-C", directory, "config", "user.name", "Workflow Test")
    abort "Could not set temporary Git email" unless
      system("git", "-C", directory, "config", "user.email", "workflow-test@example.invalid")
    abort "Could not stage baseline pin files" unless
      system("git", "-C", directory, "add", ".")
    abort "Could not commit baseline pin files" unless
      system("git", "-C", directory, "commit", "--quiet", "-m", "baseline")

    changes.each do |path, content|
      full_path = File.join(directory, path)
      FileUtils.mkdir_p(File.dirname(full_path))
      File.write(full_path, content)
    end

    stdout, stderr, status = Open3.capture3("bash", helper_path, chdir: directory)
    abort "Label resolver failed: #{stderr}" unless status.success?
    stdout.lines.map(&:strip).reject(&:empty?)
  end
end

mihomo_only = baseline_action.sub('MIHOMO_VERSION_PIN: "1.0.0"', 'MIHOMO_VERSION_PIN: "1.0.1"')
python_and_xray = baseline_action.sub('XRAY_VERSION_PIN: "1.0.0"', 'XRAY_VERSION_PIN: "1.0.1"')
ruby_and_singbox = baseline_action.sub('SINGBOX_VERSION_PIN: "1.0.0"', 'SINGBOX_VERSION_PIN: "1.0.1"')
all_cores = baseline_action
  .sub('XRAY_VERSION_PIN: "1.0.0"', 'XRAY_VERSION_PIN: "1.0.1"')
  .sub('SINGBOX_VERSION_PIN: "1.0.0"', 'SINGBOX_VERSION_PIN: "1.0.1"')
  .sub('MIHOMO_VERSION_PIN: "1.0.0"', 'MIHOMO_VERSION_PIN: "1.0.1"')

expected_scenarios = {
  "no changes" => [{}, []],
  "Mihomo only" => [{ ".github/actions/prepare-update/action.yml" => mihomo_only }, %w[dependencies mihomo]],
  "Python only" => [{ ".python-version" => "3.14.1\n" }, %w[toolchain python]],
  "Python plus Xray" => [
    { ".python-version" => "3.14.1\n", ".github/actions/prepare-update/action.yml" => python_and_xray },
    %w[dependencies xray toolchain python]
  ],
  "Ruby plus sing-box" => [
    { ".ruby-version" => "3.4.1\n", ".github/actions/prepare-update/action.yml" => ruby_and_singbox },
    %w[dependencies sing-box toolchain ruby]
  ],
  "all core and runtime pins" => [
    {
      ".python-version" => "3.14.1\n",
      ".ruby-version" => "3.4.1\n",
      ".github/actions/prepare-update/action.yml" => all_cores
    },
    %w[dependencies xray sing-box mihomo toolchain python ruby]
  ]
}
expected_scenarios.each do |name, (changes, expected)|
  actual = resolve_labels.call(changes)
  abort "Wrong labels for #{name}: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected
end

attributes = File.read(".gitattributes", encoding: "UTF-8")
abort "Ruby Linguist override is missing" unless
  attributes.include?("scripts/update/validate_clash_output.rb linguist-language=Ruby")

puts "Core-pin workflow shell syntax and Ruby language override are valid."
