#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/proxyrift

# Remove the legacy interval header from a preserved subscription; do not add metadata.
if [ -f subscriptions/light.txt ] && grep -q '^#profile-update-interval:' subscriptions/light.txt; then
  awk '!/^#profile-update-interval:/' subscriptions/light.txt > subscriptions/.light.txt.clean
  mv -f subscriptions/.light.txt.clean subscriptions/light.txt
fi

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

    deferred_file="subscriptions/.deferred-transport-candidates.txt"
    if [ -s "$deferred_file" ] && [ -s /tmp/proxyrift/light.next.txt ] && [ -s subscriptions/all.txt ]; then
      additions="$(mktemp subscriptions/.all-deferred.XXXXXX)"
      awk '
        FILENAME == ARGV[1] {
          key = $0
          sub(/#.*/, "", key)
          deferred[key] = 1
          next
        }
        FILENAME == ARGV[2] {
          key = $0
          sub(/#.*/, "", key)
          if ((key in deferred) && !(key in candidate)) {
            candidate[key] = $0
            order[++count] = key
          }
          next
        }
        {
          key = $0
          sub(/#.*/, "", key)
          existing[key] = 1
        }
        END {
          for (i = 1; i <= count; i++) {
            key = order[i]
            if (!(key in existing)) {
              print candidate[key]
            }
          }
        }
      ' "$deferred_file" /tmp/proxyrift/light.next.txt subscriptions/all.txt > "$additions"

      added_count="$(awk 'END { print NR + 0 }' "$additions")"
      if [ "$added_count" -gt 0 ]; then
        cat subscriptions/all.txt "$additions" > subscriptions/.all.txt.next
        mv -f subscriptions/.all.txt.next subscriptions/all.txt
        echo "[INFO] ✅ [All] Added $added_count deferred configs verified by Light consumer validation"
      fi
      rm -f "$additions"
    fi

    echo "[INFO] ✅ [Light] Published refreshed subscription"
  else
    echo "[WARN] ⚠️ [Light] Pool too small | ${light_count}/${LIGHT_MIN_PUBLISH} minimum | Preserving previous subscription"
  fi
else
  echo "[WARN] ⚠️ [Light] No configs | Preserving previous subscription"
fi
