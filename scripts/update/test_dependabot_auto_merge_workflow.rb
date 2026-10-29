#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

path = ".github/workflows/dependabot-auto-merge.yml"
workflow = YAML.load_file(path)
steps = workflow.dig("jobs", "merge", "steps")

concurrency = workflow["concurrency"]
abort "Auto-merge workflow must serialize invocations that scan the same open PR queue" unless
  concurrency.is_a?(Hash) &&
    concurrency["group"] == "dependabot-action-auto-merge" &&
    concurrency["cancel-in-progress"] == false

merge_job_condition = workflow.dig("jobs", "merge", "if")
abort "PR-triggered auto-merge must ignore unrelated human PRs" unless
  merge_job_condition.is_a?(String) &&
    merge_job_condition.include?("github.event_name != 'pull_request_target'") &&
    merge_job_condition.include?("dependabot[bot]") &&
    merge_job_condition.include?("dependabot/github_actions/") &&
    merge_job_condition.include?("dependabot/cargo/") &&
    merge_job_condition.include?("automation/update-core-pins") &&
    merge_job_condition.include?("github-actions[bot]") &&
    merge_job_condition.include?("rekt0ro")

checkout = steps.find { |step| step["name"] == "Checkout trusted merge-gate scripts" }
abort "Auto-merge workflow must checkout trusted scripts from the default branch" unless
  checkout &&
    checkout["uses"] == "actions/checkout@v7" &&
    checkout.dig("with", "ref") == "${{ github.event.repository.default_branch }}" &&
    checkout.dig("with", "persist-credentials") == false

merge = steps.find { |step| step["name"] == "Merge ready automated dependency and core-pin updates" }
abort "Auto-merge workflow step is missing" unless merge && merge["run"].is_a?(String)
abort "Legacy bot PRs must use a successful workflow_dispatch run for the exact PR head SHA" unless
  merge["run"].include?("--event workflow_dispatch") &&
    merge["run"].include?("--json headSha,status,conclusion") &&
    merge["run"].include?(".headSha == $sha and .status == \"completed\" and .conclusion == \"success\"") &&
    merge["run"].include?('.head.sha // empty') &&
    merge["run"].include?('"name": "validate"') &&
    merge["run"].include?('"workflow": "CI"') &&
    merge["run"].include?('"bucket": "pass"') &&
    merge["run"].include?('select(.workflow != "CI")') &&
    merge["run"].include?("jq -s 'add'")
abort "Auto-merge workflow must request named CI checks" unless
  merge["run"].include?("--json name,workflow,bucket")
abort "Auto-merge workflow must call the tested status gate" unless
  merge["run"].include?("bash scripts/update/verify_dependabot_auto_merge_checks.sh")

workflow_text = File.read(path, encoding: "UTF-8")
abort "Auto-merge must re-check PRs when CodeQL finishes, even though its aggregate check is not a gate" unless
  workflow_text.include?("      - CI\n      - CodeQL")

body = merge.fetch("run")

wait_tokens = [
  "checks_deadline=$((SECONDS + 900))",
  "while (( SECONDS < checks_deadline )); do",
  "sleep 15",
  "checks_ready=true",
  'if [ "${checks_ready}" != "true" ]; then'
]
abort "Auto-merge must keep rechecking pending checks instead of giving up on the first pass" unless
  wait_tokens.all? { |token| body.include?(token) }

abort "Auto-merge polling must combine JSON arrays with a real newline" unless
  body.include?(%q{printf '%s\n'})
double_escaped_newline = "printf '%s" + ("\\" * 2) + "n'"
abort "Auto-merge polling must not append a literal backslash-n to check JSON" if
  body.include?(double_escaped_newline)

metadata_tokens = [
  "pr_data=\"$(printf",
  '--argjson pr "${pr}"',
  "select(.number == $pr)",
  "jq -r '.user.login // empty' <<< \"${pr_data}\"",
  "jq -r '.head.ref // empty' <<< \"${pr_data}\""
]
abort "Auto-merge PR metadata must use a real newline in the JSON formatter" unless
  body.match?(/pr_data=.*printf '%s\\n' "\$\{open_prs\}"/)
abort "Auto-merge PR metadata must not append a literal backslash-n to JSON" if
  body.match?(/pr_data=.*printf '%s\\\\n' "\$\{open_prs\}"/)
abort "Auto-merge must validate the author and branch from the same GitHub API PR record" unless
  metadata_tokens.all? { |token| body.include?(token) } &&
    !body.include?("--json author --jq") &&
    !body.include?("--json headRefName --jq")
abort "Runtime pins must be restricted to the trusted core-pin updater PR" unless
  body.include?(".python-version|.ruby-version|rust-toolchain.toml") &&
    body.include?('elif [[ ( "${pr_author}" == "github-actions[bot]" || "${pr_author}" == "rekt0ro" ) && "${head_ref}" == "automation/update-core-pins" ]]')


dependabot_branch_rules =
  body.include?('(.head.ref | startswith("dependabot/github_actions/"))') &&
    body.include?('(.head.ref | startswith("dependabot/cargo/"))') &&
    body.include?('"automation/update-core-pins"') &&
    body.include?('"rekt0ro"')
abort "Auto-merge may only select configured Dependabot ecosystems or the core-pin update branch" unless
  dependabot_branch_rules

abort "GitHub Actions updates must be limited to workflow/composite-action files" unless
  body.include?('dependabot/github_actions/*)') &&
    body.include?('.github/workflows/*.yml|.github/workflows/*.yaml') &&
    body.include?('.github/actions/*/action.yml')

abort "Cargo updates must only modify Cargo manifests/lockfiles" unless
  body.include?('dependabot/cargo/*)') &&
    body.include?('Cargo.toml|Cargo.lock)')

abort "Core-pin squash merges must include the current PR title and PR number in the commit subject" unless
  body.include?('merge_args+=(--subject "${pr_title} (#${pr})")') &&
    body.include?("--delete-branch") &&
    body.include?(%q{pr_title="$(jq -r '.title // empty' <<< "${pr_data}")"}) &&
    body.include?('if gh pr merge "${pr}" "${merge_args[@]}"; then') &&
    body.include?('PR #${pr} has no title; refusing to merge without a commit subject.')
merge_retry_tokens = [
  "merged_by_another_run=false",
  "for attempt in {1..10}; do",
  'if [ "${state}" = "MERGED" ]; then',
  "sleep 3",
  'if [ "${merged_by_another_run}" = "true" ]; then',
  "PR #${pr} was merged by another run."
]
abort "Auto-merge must re-check PR state after a concurrent merge attempt" unless
  merge_retry_tokens.all? { |token| body.include?(token) }

abort "Core-pin updater must only modify its three managed pin files" unless
  body.include?('.github/actions/prepare-update/action.yml|.python-version|.ruby-version|rust-toolchain.toml')
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
