#!/usr/bin/env bash
# Rebase the checked-out fork branch onto the latest stable OpenAI release.
# This changes only local history; qualification and publication are separate.
set -euo pipefail
repo="$(git rev-parse --show-toplevel)"
cd "$repo"
git symbolic-ref --quiet HEAD >/dev/null || { echo 'Check out a fork branch first.' >&2; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo 'Commit or stash changes before rebasing.' >&2; exit 1; }
target="${1:-}"
if [[ -z "$target" ]]; then
    target="$(gh api repos/openai/codex/releases/latest --jq .tag_name)"
fi
[[ "$target" =~ ^rust-v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Expected a stable rust-vX.Y.Z tag: $target" >&2; exit 1; }
base="$(git describe --tags --abbrev=0 --match='rust-v*' --exclude='rust-v*-*' HEAD)"
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
