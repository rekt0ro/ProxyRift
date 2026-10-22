#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

path = ".github/workflows/dependabot-auto-merge.yml"
workflow = YAML.load_file(path)
steps = workflow.dig("jobs", "merge", "steps")

checkout = steps.find { |step| step["name"] == "Checkout trusted merge-gate scripts" }
abort "Auto-merge workflow must checkout trusted scripts from the default branch" unless
  checkout &&
    checkout["uses"] == "actions/checkout@v7" &&
    checkout.dig("with", "ref") == "${{ github.event.repository.default_branch }}" &&
    checkout.dig("with", "persist-credentials") == false

merge = steps.find { |step| step["name"] == "Merge ready Dependabot tooling updates" }
abort "Auto-merge workflow step is missing" unless merge && merge["run"].is_a?(String)
abort "Auto-merge workflow must request named CI checks" unless
  merge["run"].include?("--json name,workflow,bucket")
abort "Auto-merge workflow must call the tested status gate" unless
  merge["run"].include?("bash scripts/update/verify_dependabot_auto_merge_checks.sh")

workflow_text = File.read(path, encoding: "UTF-8")
abort "Auto-merge must re-check PRs when CodeQL finishes, even though its aggregate check is not a gate" unless
  workflow_text.include?("      - CI\n      - CodeQL")

body = merge.fetch("run")
abort "Python pin must be restricted to the trusted core-pin updater PR" unless
  body.include?(".ruby-version|.python-version") &&
    body.include?('"${pr_author}" != "github-actions[bot]"') &&
    body.include?('"${head_ref}" != "automation/update-core-pins"')


dependabot_branch_rules =
  body.include?('(.head.ref | startswith("dependabot/github_actions/"))') &&
    body.include?('(.head.ref | startswith("dependabot/cargo/"))') &&
    body.include?('"automation/update-core-pins"')
abort "Auto-merge may only select configured Dependabot ecosystems or the core-pin update branch" unless
  dependabot_branch_rules

abort "GitHub Actions updates must be limited to workflow/composite-action files" unless
  body.include?('dependabot/github_actions/*)') &&
    body.include?('.github/workflows/*.yml|.github/workflows/*.yaml') &&
    body.include?('.github/actions/*/action.yml')

abort "Cargo updates must only modify Cargo manifests/lockfiles" unless
  body.include?('dependabot/cargo/*)') &&
    body.include?('Cargo.toml|Cargo.lock)')

abort "Core-pin updater must only modify its three managed pin files" unless
  body.include?('.github/actions/prepare-update/action.yml|.python-version|.ruby-version')
abort "Unconfigured requirements-file updates must not be auto-merged" if body.include?('requirements*.txt')
gate_path = "scripts/update/verify_dependabot_auto_merge_checks.sh"
gate = File.read(gate_path, encoding: "UTF-8")
%w[Analyze\ (rust) Analyze\ (ruby) Analyze\ (python)].each do |check_name|
  abort "Required check #{check_name} is missing from the status gate" unless
    gate.include?(check_name)
end
abort "Aggregate CodeQL must not be a required check" if gate.include?(".name == \"CodeQL\"")
abort "Analyze (actions) must not be a required check" if gate.include?(".name == \"Analyze (actions)\"")

puts "Dependabot auto-merge waits for CI plus Rust/Ruby/Python checks and ignores aggregate CodeQL."
