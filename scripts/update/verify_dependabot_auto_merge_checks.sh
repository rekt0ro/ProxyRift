#!/usr/bin/env bash
set -euo pipefail

checks="$(cat)"
if jq -e '
  if type != "array" then
    false
  else
    . as $checks
    | [ $checks[] | select(.workflow == "CI") ] as $ci
    | ["Analyze (rust)", "Analyze (ruby)", "Analyze (python)"] as $required
    | ($ci | length > 0)
      and all($ci[]; .bucket == "pass")
      and all($required[]; . as $required_name
        | any($checks[]; .name == $required_name and .bucket == "pass"))
  end
' <<< "$checks" >/dev/null; then
  echo "[OK] CI and Rust, Ruby, Python checks passed. Aggregate CodeQL and Analyze (actions) are not merge gates."
  exit 0
fi

echo "[WAIT] CI and all required language checks must pass before auto-merge." >&2
jq -r '
  .[]
  | select(
      .workflow == "CI"
      or .name == "Analyze (rust)"
      or .name == "Analyze (ruby)"
      or .name == "Analyze (python)"
    )
  | "\(.workflow // "unknown") / \(.name): \(.bucket)"
' <<< "$checks" >&2
exit 1
