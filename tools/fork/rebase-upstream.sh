#!/usr/bin/env bash
# Rebase the checked-out fork branch onto the newest published OpenAI release,
# pre-releases included.
# This changes only local history; qualification and publication are separate.
set -euo pipefail
repo="$(git rev-parse --show-toplevel)"
cd "$repo"
git symbolic-ref --quiet HEAD >/dev/null || { echo 'Check out a fork branch first.' >&2; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo 'Commit or stash changes before rebasing.' >&2; exit 1; }
target="${1:-}"
if [[ -z "$target" ]]; then
    # /releases/latest skips pre-releases; the first entry of /releases is the newest.
    target="$(gh api 'repos/openai/codex/releases?per_page=20' --jq '[.[] | select(.draft | not) | select(.tag_name | startswith("rust-v"))][0].tag_name')"
fi
[[ "$target" =~ ^rust-v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || { echo "Expected a rust-vX.Y.Z[-pre] tag: $target" >&2; exit 1; }
# The base is the release the branch sits on, stable or not: an upstream release
# tag is a version-bump commit off main, so a stable tag is not an ancestor of a
# later pre-release and an exclude pattern would select a far older base.
base="$(git describe --tags --abbrev=0 --match='rust-v*' HEAD)"
git fetch https://github.com/openai/codex.git "refs/tags/$target:refs/tags/$target"
if git merge-base --is-ancestor "$target" HEAD; then
    printf 'Already based on %s.\n' "$target"
    exit 0
fi
backup="backup/upstream-rebase-$(date -u +%Y%m%dT%H%M%SZ)"
git branch "$backup" HEAD
printf 'Rebasing from %s onto %s; original history: %s\n' "$base" "$target" "$backup"
git rebase --onto "$target" "$base"
git merge-base --is-ancestor "$target" HEAD
printf 'Rebase complete. Run hosted qualification before publishing this branch.\n'
