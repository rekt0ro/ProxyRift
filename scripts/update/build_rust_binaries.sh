#!/usr/bin/env bash
set -euo pipefail

cargo build --release --locked --bins --quiet
test -x ./target/release/ProxyRift
test -x ./target/release/check_proxies
test -x ./target/release/polish_light
test -x ./target/release/encode_subscriptions
test -x ./target/release/discover_sources
