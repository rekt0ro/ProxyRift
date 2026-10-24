#!/usr/bin/env ruby

require "open3"
require "tempfile"

validator = File.expand_path("validate_clash_output.rb", __dir__)

def validate(validator, content, *arguments)
  Tempfile.create(["proxyrift-clash", ".yaml"]) do |file|
    file.write(content)
    file.flush
    Open3.capture3("ruby", validator, *arguments, file.path)
  end
end

valid = <<~YAML
  proxies:
    - name: "Validation proxy"
      type: socks5
      server: 127.0.0.1
      port: 1080
YAML

stdout, stderr, status = validate(validator, valid)
abort "Valid Clash output was rejected: #{stderr}" unless status.success?
abort "Unexpected success output: #{stdout}" unless stdout.include?("Proxies: 1")

stdout, stderr, status = validate(validator, valid, "--count")
abort "Valid proxy count was rejected: #{stderr}" unless status.success?
abort "Count mode did not return one proxy" unless stdout.strip == "1"

invalid_port = valid.sub("port: 1080", 'port: "1080"')
_, stderr, status = validate(validator, invalid_port)
abort "String proxy port was accepted" if status.success?
abort "Invalid port error was not clear" unless stderr.include?("port must be an integer")

_, stderr, status = validate(validator, "proxies: [\n")
abort "Malformed YAML was accepted" if status.success?
abort "Malformed YAML error was not clear" unless stderr.include?("cannot parse")

puts "Clash output validator tests passed."
