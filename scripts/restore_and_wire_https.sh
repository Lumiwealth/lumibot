#!/usr/bin/env bash
set -euo pipefail

# Run from anywhere; find repo root via this script's location
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Config you might tweak
GH_USER="${GH_USER:-txtr99}"
UPSTREAM_URL="${UPSTREAM_URL:-https://github.com/Lumiwealth/lumibot.git}"
PRIVATE_URL="${PRIVATE_URL:-https://github.com/${GH_USER}/lumibot-private.git}"

# Find the newest backup if not provided; expects backups like ../lumibot.OLD.*
PARENT="$(dirname "$REPO_ROOT")"
OLD_DIR="${OLD_DIR:-$(ls -1d "$PARENT"/lumibot.OLD.* 2>/dev/null | sort | tail -n1 || true)}"
[ -n "${OLD_DIR:-}" ] && [ -d "$OLD_DIR" ] || { echo "No OLD backup folder found next to repo (expected: $PARENT/lumibot.OLD.*). Set OLD_DIR=... if needed."; exit 1; }

# Zip the backup safely into repo/.backups
STAMP="$(date +%Y%m%d-%H%M%S)"
ZIP="$REPO_ROOT/.backups/$(basename "$OLD_DIR").$STAMP.zip"
( cd "$PARENT" && zip -r "$ZIP" "$(basename "$OLD_DIR")" >/dev/null )

# Merge OLD tree into current clone (preserves .git)
rsync -a --exclude='.git' "$OLD_DIR"/ "$REPO_ROOT"/

# Wire remotes: upstream fetch-only (public), origin private (HTTPS)
if ! git remote | grep -q '^upstream$'; then
  git remote add upstream "$UPSTREAM_URL"
else
  git remote set-url upstream "$UPSTREAM_URL"
fi
git remote set-url --push upstream DISABLED

if git remote | grep -q '^origin$'; then
  git remote set-url origin "$PRIVATE_URL"
else
  git remote add origin "$PRIVATE_URL"
fi
git config remote.pushDefault origin
git config --global credential.helper osxkeychain || true

git fetch upstream --prune

# Checkout dev tracking upstream (fallback main)
if git show-ref --verify --quiet refs/remotes/upstream/dev; then
  git switch -c dev --track upstream/dev 2>/dev/null || git switch dev
else
  git switch -c dev --track upstream/main 2>/dev/null || git switch dev
fi
# Make status/push UX clean
git branch --set-upstream-to=origin/dev dev 2>/dev/null || true

# Append an idempotent .gitignore block (includes .backups/)
BLOCK_START="# >>> txtr99-private-ignore >>>"
if ! grep -q "$BLOCK_START" .gitignore 2>/dev/null; then
  cat >> .gitignore <<'EOF'
# >>> txtr99-private-ignore >>>
.DS_Store
.idea/
.vscode/
__pycache__/
*.py[cod]
.venv/
venv/
.env
.env.*
*.env
.eggs/
*.egg-info/
dist/
build/
.mypy_cache/
.pytest_cache/
databento_exports/
profiler/
profiling/
*.bak
*.log
logs/
.backups/
# <<< txtr99-private-ignore <<<
EOF
fi

# Versioned hooks: keep them inside the repo
git config core.hooksPath .githooks
mkdir -p .githooks
cat > .githooks/pre-push <<'HOOK'
#!/bin/sh
[ "$1" = "upstream" ] && { echo "Blocked: refusing to push to upstream (public)."; exit 1; }
HOOK
chmod +x .githooks/pre-push

# Install the stash-aware upstream fetch helper in scripts/
cat > scripts/fetch_upstream.sh <<'SH'
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
SH
chmod +x scripts/fetch_upstream.sh

echo
echo "Backup zipped to: $ZIP"
echo "Repo ready at:   $REPO_ROOT"
echo
echo "Remotes:"
git remote -v
echo
echo "Next:"
echo "  • Review:  git status"
echo "  • First push (HTTPS): git push -u origin dev   # user: ${GH_USER}, pass: your PAT"
echo "  • Update later:       ./scripts/fetch_upstream.sh"