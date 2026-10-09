#!/usr/bin/env bash
set -euo pipefail

for f in subscriptions/all.txt subscriptions/light.txt subscriptions/all-base64.txt subscriptions/light-base64.txt subscriptions/all-clash.yaml subscriptions/light-clash.yaml subscriptions/all-singbox.json subscriptions/light-singbox.json; do
  test -s "$f"
done
