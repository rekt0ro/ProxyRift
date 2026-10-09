#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/proxyrift
light_started_epoch="$(date +%s)"
set +e
./target/release/polish_light \
  --candidates subscriptions/.light-candidates.txt \
  --output /tmp/proxyrift/light.next.txt \
  --workers 48 \
  --batch-size 1000 \
  --timeout 3 \
  --selected-recheck-limit 350 \
  --max-candidates "$LIGHT_MAX_CANDIDATES" \
  --selected-workers 48 \
  --selected-batch-size 350 \
  --primary-target https://www.google.com/generate_204 \
  --singbox "$HOME/.local/bin/sing-box" \
  --selection-limit "$LIGHT_SELECTION_LIMIT" \
  --max-per-endpoint 1 \
  --max-per-family 3 \
  --stats /tmp/proxyrift/light-stats.json
status=$?
set -e
light_elapsed="$(( $(date +%s) - light_started_epoch ))s"
echo "[INFO] ✅ [Light] Validation complete | ${light_elapsed}"

if [ "$status" -ne 0 ]; then
  echo "[WARN] ⚠️ [Light] Validation failed | Preserving previous subscription" >&2
  exit "$status"
fi

if [ -s /tmp/proxyrift/light.next.txt ]; then
  light_count="$(awk 'NF { count++ } END { print count + 0 }' /tmp/proxyrift/light.next.txt)"
  echo "[INFO] 🎯 [Light] Output | ${light_count} Candidates | Target/max: ${LIGHT_SELECTION_LIMIT} | Min publish: ${LIGHT_MIN_PUBLISH}"
  if [ "$light_count" -ge "$LIGHT_MIN_PUBLISH" ]; then
    cp /tmp/proxyrift/light.next.txt subscriptions/.light.txt.next
    mv -f subscriptions/.light.txt.next subscriptions/light.txt
    echo "[INFO] ✅ [Light] Published refreshed subscription"
  else
    echo "[WARN] ⚠️ [Light] Pool too small | ${light_count}/${LIGHT_MIN_PUBLISH} minimum | Preserving previous subscription"
  fi
else
  echo "[WARN] ⚠️ [Light] No configs | Preserving previous subscription"
fi
