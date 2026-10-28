#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
remote="$tmp/remote.git"
seed="$tmp/seed"
work="$tmp/work"
verify="$tmp/verify"
output_files=(
  all.txt light.txt
  all-base64.txt light-base64.txt
  all-clash.yaml light-clash.yaml
  all-singbox.json light-singbox.json
)

git init --bare --initial-branch=main "$remote" >/dev/null
git clone "$remote" "$seed" >/dev/null 2>&1
git -C "$seed" config user.name "Test"
git -C "$seed" config user.email "test@example.com"
mkdir -p "$seed/scripts/update" "$seed/subscriptions"
cp "$repo_root/scripts/update/publish_generated_changes.py" "$seed/scripts/update/publish_generated_changes.py"
cp "$repo_root/scripts/update/publish_common.py" "$seed/scripts/update/publish_common.py"

for file in "${output_files[@]}"; do
  printf 'old-%s\n' "$file" > "$seed/subscriptions/$file"
done
printf 'old-source-state\n' > "$seed/subscriptions/source-registry.json"
git -C "$seed" add .
git -C "$seed" commit -m "Seed repository" >/dev/null
git -C "$seed" push origin main >/dev/null 2>&1

git clone "$remote" "$work" >/dev/null 2>&1
git -C "$work" config user.name "Test"
git -C "$work" config user.email "test@example.com"
for file in "${output_files[@]}"; do
  printf 'new-%s\n' "$file" > "$work/subscriptions/$file"
done

printf 'new-source-state\n' > "$work/subscriptions/source-registry.json"

real_git="$(command -v git)"
fetch_bin="$tmp/fetch-bin"
mkdir -p "$fetch_bin"
cat > "$fetch_bin/git" <<'GIT'
#!/usr/bin/env bash
if [[ "${1:-}" == "fetch" && "${2:-}" == "origin" && "${3:-}" == "main" && ! -e "${GIT_FETCH_FAIL_STATE:?}" ]]; then
  : > "$GIT_FETCH_FAIL_STATE"
  printf "fatal: unable to access 'https://github.com/test/repo.git/': Could not resolve host: github.com\n" >&2
  exit 128
fi
exec "$REAL_GIT" "$@"
GIT
chmod +x "$fetch_bin/git"

(
  cd "$work"
  PATH="$fetch_bin:$PATH" REAL_GIT="$real_git" GIT_FETCH_FAIL_STATE="$tmp/fetch-failed" python3 scripts/update/publish_generated_changes.py
) > "$tmp/publish-first.log" 2>&1 || {
  cat "$tmp/publish-first.log" >&2
  printf '[ERROR] Generated subscription publishing failed.\n' >&2
  exit 1
}

git -C "$work" log -1 --format=%s | grep -Fxq "Update subscriptions"
printf 'state-only-update\n' > "$work/subscriptions/source-registry.json"
(
  cd "$work"
  PATH="$fetch_bin:$PATH" REAL_GIT="$real_git" GIT_FETCH_FAIL_STATE="$tmp/fetch-failed" python3 scripts/update/publish_generated_changes.py
) > "$tmp/publish-state-only.log" 2>&1 || {
  cat "$tmp/publish-state-only.log" >&2
  printf '[ERROR] Source-state-only publishing failed.\n' >&2
  exit 1
}

git clone "$remote" "$verify" >/dev/null 2>&1
for file in "\${output_files[@]}"; do
  grep -Fxq "new-$file" "$verify/subscriptions/$file"
done
grep -Fxq "state-only-update" "$verify/subscriptions/source-registry.json"
grep -Fq "[WARN] [Publish] Fetch failed on attempt 1/5; retrying in 2s" "$tmp/publish-first.log"
grep -Fq "[INFO] [Publish] Subscription update published" "$tmp/publish-first.log"
git -C "$verify" log -1 --format=%s | grep -Fxq "Update source and validation state"

printf '[OK] Generated publishing retries transient fetch failures and labels state-only commits accurately.\n'