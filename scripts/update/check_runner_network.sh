#!/usr/bin/env bash
set -euo pipefail

curl -fsS --max-time 10 "https://speed.cloudflare.com/__down?bytes=1" -o /dev/null
curl -fsS --max-time 10 "https://api.ipify.org?format=json" -o /dev/null
echo "[INFO] 🌍 [Network] Runner connectivity OK"
