#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
cur_branch="$(git rev-parse --abbrev-ref HEAD)"

need_stash=0
if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
  need_stash=1
  git stash push -u -m "autostash: $(date -Iseconds)" >/dev/null
fi

git fetch upstream --prune
if git show-ref --verify --quiet "refs/remotes/upstream/$cur_branch"; then
  base="upstream/$cur_branch"
elif git show-ref --verify --quiet "refs/remotes/upstream/dev"; then
  base="upstream/dev"
else
  base="upstream/main"
fi

if ! git rebase "$base"; then
  git rebase --abort || true
  git merge --no-edit "$base" || { echo "Conflicts; resolve and continue."; exit 1; }
fi

[ "$need_stash" -eq 1 ] && git stash pop || true
echo "Updated onto $base. Push privately when ready: git push origin $cur_branch"
