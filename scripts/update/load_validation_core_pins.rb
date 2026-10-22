#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

root = File.expand_path("../..", __dir__)
action_path = File.join(root, ".github/actions/prepare-update/action.yml")
action = YAML.safe_load_file(action_path, aliases: true)
step = action.fetch("runs").fetch("steps").find do |entry|
  entry["id"] == "validation-cores"
end
abort "Pinned validation-core step is missing from #{action_path}" unless step

environment = step.fetch("env")
pins = {
  "SINGBOX_VERSION" => ["SINGBOX_VERSION_PIN", /\A\d+\.\d+\.\d+\z/],
  "SINGBOX_SHA256" => ["SINGBOX_SHA256_PIN", /\A[0-9a-f]{64}\z/],
  "SINGBOX_ASSET" => ["SINGBOX_ASSET_PIN", /\Asing-box-\d+\.\d+\.\d+-linux-amd64\.tar\.gz\z/],
  "MIHOMO_VERSION" => ["MIHOMO_VERSION_PIN", /\A\d+\.\d+\.\d+\z/],
  "MIHOMO_SHA256" => ["MIHOMO_SHA256_PIN", /\A[0-9a-f]{64}\z/],
  "MIHOMO_ASSET" => ["MIHOMO_ASSET_PIN", /\Amihomo-linux-amd64-v\d+\.\d+\.\d+\.gz\z/]
}

pins.each do |output_name, (source_name, pattern)|
  value = environment.fetch(source_name)
  abort "Invalid #{source_name} value in #{action_path}" unless
    value.is_a?(String) && pattern.match?(value)

  puts "#{output_name}=#{value}"
end
