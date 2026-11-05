#!/usr/bin/env ruby

require "tempfile"
require "yaml"

def reject(message)
  warn "[ERROR] [Clash/Mihomo] #{message}"
  exit 1
end

unless ARGV.length == 2
  reject("usage: remove_clash_proxy.rb <output.yaml> <zero-based-proxy-index>")
end

path, raw_index = ARGV
begin
  index = Integer(raw_index, 10)
rescue ArgumentError
  reject("proxy index must be a positive integer")
end
reject("proxy index must be a positive integer") unless index >= 0

begin
  document = YAML.safe_load(File.read(path, encoding: "UTF-8"), aliases: false)
rescue Errno::ENOENT, Psych::Exception => error
  reject("cannot parse #{path}: #{error.message.lines.first.to_s.strip}")
end

unless document.is_a?(Hash) && document["proxies"].is_a?(Array)
  reject("#{path} must contain a proxies array")
end

proxies = document.fetch("proxies")
unless index < proxies.length
  reject("proxy index #{index} is outside the available range 0..#{proxies.length - 1}")
end
reject("cannot remove proxy index #{index}; at least one proxy must remain") if proxies.length <= 1

removed = proxies.delete_at(index)
name = removed.is_a?(Hash) ? removed["name"] : nil
begin
  Tempfile.create([".#{File.basename(path)}", ".tmp"], File.dirname(path)) do |file|
    file.write(YAML.dump(document))
    file.flush
    file.fsync
    File.rename(file.path, path)
  end
rescue SystemCallError => error
  reject("cannot write filtered Clash output #{path}: #{error.message}")
end

puts "Removed proxy index #{index}#{name ? " (#{name})" : ""} from #{path}"
