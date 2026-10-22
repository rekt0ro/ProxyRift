#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

def reject(message)
  warn "[ERROR] [Clash/Mihomo] #{message}"
  exit 1
end

path = ARGV.fetch(0) { reject("usage: validate_clash_output.rb <output.yaml>") }

begin
  document = YAML.safe_load(File.read(path, encoding: "UTF-8"), aliases: false)
rescue Errno::ENOENT, Psych::Exception => error
  reject("cannot parse #{path}: #{error.message.lines.first.to_s.strip}")
end

unless document.is_a?(Hash)
  reject("#{path} must contain a YAML mapping")
end

proxies = document["proxies"]
unless proxies.is_a?(Array) && !proxies.empty?
  reject("#{path} must contain a non-empty proxies array")
end

proxies.each_with_index do |proxy, index|
  unless proxy.is_a?(Hash)
    reject("#{path} proxies[#{index}] must be a mapping")
  end

  %w[name type server].each do |field|
    value = proxy[field]
    unless value.is_a?(String) && !value.strip.empty?
      reject("#{path} proxies[#{index}].#{field} must be a non-empty string")
    end
  end

  port = proxy["port"]
  unless port.is_a?(Integer) && (1..65_535).cover?(port)
    reject("#{path} proxies[#{index}].port must be an integer from 1 to 65535")
  end
end

puts "[OK] [Clash/Mihomo] Structurally valid | #{path} | Proxies: #{proxies.length}"
