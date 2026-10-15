#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
remote="$tmp/remote.git"
seed="$tmp/seed"
work="$tmp/work"
verify="$tmp/verify"
state_files=(
  light-history.json
  light-training.jsonl
  light-training-stats.json
  light-candidate-ledger.json
  source-registry.json
)

git init --bare --initial-branch=main "$remote" >/dev/null
git clone "$remote" "$seed" >/dev/null 2>&1
git -C "$seed" config user.name "Test"
git -C "$seed" config user.email "test@example.com"
mkdir -p "$seed/scripts/update" "$seed/subscriptions"
cp "$repo_root/scripts/update/publish_light_state.sh" "$seed/scripts/update/publish_light_state.sh"

for file in "${state_files[@]}"; do
  printf 'old-%s\n' "$file" > "$seed/subscriptions/$file"
done
printf 'old-light-output\n' > "$seed/subscriptions/light.txt"
printf 'old-all-output\n' > "$seed/subscriptions/all.txt"
git -C "$seed" add .
git -C "$seed" commit -m "Seed repository" >/dev/null
git -C "$seed" push origin main >/dev/null 2>&1

git clone "$remote" "$work" >/dev/null 2>&1
git -C "$work" config user.name "Test"
git -C "$work" config user.email "test@example.com"
for file in "${state_files[@]}"; do
  printf 'new-%s\n' "$file" > "$work/subscriptions/$file"
done
printf 'unpublished-light-output\n' > "$work/subscriptions/light.txt"
printf 'unpublished-all-output\n' > "$work/subscriptions/all.txt"

real_git="$(command -v git)"
fetch_bin="$tmp/fetch-bin"
mkdir -p "$fetch_bin"
cat > "$fetch_bin/git" <<'GIT'
#!/usr/bin/env bash
if [[ "${1:-}" == "fetch" && "${2:-}" == "origin" && "${3:-}" == "main" && ! -e "${GIT_FETCH_FAIL_STATE:?}" ]]; then
  : > "$GIT_FETCH_FAIL_STATE"
  printf "fatal: unable to access 'https://github.com/test/repo.git/': Could not resolve host: github.com\\n" >&2
  exit 128
fi
exec "$REAL_GIT" "$@"
GIT
chmod +x "$fetch_bin/git"

(
  cd "$work"
  PATH="$fetch_bin:$PATH" REAL_GIT="$real_git" GIT_FETCH_FAIL_STATE="$tmp/fetch-failed" bash scripts/update/publish_light_state.sh
) > "$tmp/publish.log" 2>&1 || {
  cat "$tmp/publish.log" >&2
  echo "[ERROR] Light state-only publishing failed." >&2
  exit 1
}

git clone "$remote" "$verify" >/dev/null 2>&1
for file in "${state_files[@]}"; do
  grep -Fxq "new-$file" "$verify/subscriptions/$file"
done
grep -Fxq "old-light-output" "$verify/subscriptions/light.txt"
grep -Fxq "old-all-output" "$verify/subscriptions/all.txt"
grep -Fq "[WARN] [Light state] Fetch failed on attempt 1/5; retrying in 2s" "$tmp/publish.log"

actual="$(git -C "$verify" diff --name-only HEAD^ HEAD | sort)"
expected="$(printf '%s\n' \
  subscriptions/light-candidate-ledger.json \
  subscriptions/light-history.json \
  subscriptions/light-training-stats.json \
  subscriptions/light-training.jsonl \
  subscriptions/source-registry.json | sort)"
if [[ "$actual" != "$expected" ]]; then
  printf '[ERROR] Unexpected paths in learning-state commit.\nExpected:\n%s\nActual:\n%s\n' "$expected" "$actual" >&2
  exit 1
fi

echo "[OK] Light learning state is persisted without publishing subscription outputs."
