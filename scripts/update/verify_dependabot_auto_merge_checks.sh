#!/usr/bin/env bash
set -euo pipefail

checks="$(cat)"
if jq -e '
  if type != "array" then
    false
  else
    [ .[] | select(.workflow == "CI") ] as $ci
    | ($ci | length > 0)
      and all($ci[]; .bucket == "pass")
  end
' <<< "$checks" >/dev/null; then
  echo "[OK] All CI checks passed. CodeQL and other non-CI checks are ignored."
  exit 0
fi

echo "[WAIT] Every CI check must pass before auto-merge." >&2
jq -r '
  .[]
  | select(.workflow == "CI")
  | "\(.workflow // "unknown") / \(.name): \(.bucket)"
' <<< "$checks" >&2
exit 1
