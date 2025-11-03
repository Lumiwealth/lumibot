# GitButler + Private Fork Workflow

## Overview
This repo is a private fork (`txtr99/lumibot-private`) of the public `Lumiwealth/lumibot` repo, managed with GitButler for virtual branching.

## Daily Development

### 1. Sync Upstream Changes (Weekly/As Needed)
```bash
# Fetch latest from public repo
./scripts/fetch_upstream.sh

# This will:
# - Fetch from upstream (public Lumiwealth/lumibot)
# - Rebase your gitbutler/workspace onto upstream/dev
# - Keep your private changes on top
```

### 2. Work in GitButler
- Open GitButler GUI
- Create virtual branches for different features
- Commit changes to appropriate branches
- GitButler will manage the `gitbutler/workspace` branch automatically

### 3. Push Private Work to Origin
```bash
# Push your GitButler workspace to private repo
git push origin gitbutler/workspace

# Or push other branches
git push origin <branch-name>
```

### 4. Merge Completed Features to Dev
```bash
# When a feature is complete, merge to dev
git checkout dev
git pull origin dev
git merge --no-ff gitbutler/workspace  # or specific branch
git push origin dev
```

## Safety Checklist

### Before Any Push:
- ✓ Check current remote: `git remote -v`
- ✓ Verify branch: `git branch --show-current`
- ✓ Confirm target: `git push` defaults to `origin` (not `upstream`)

### Protection Mechanisms:
1. **Pre-push hook**: Blocks any `git push upstream`
2. **Disabled push URL**: `upstream` has push URL = "DISABLED"
3. **Default remote**: `remote.pushDefault = origin`

### Test Safety:
```bash
# This will fail (expected):
git push upstream dev
# Output: "Blocked: refusing to push to upstream (public)."

# This succeeds (safe):
git push origin dev
```

## Branch Management

### Branches You'll Have:
- `dev` - Main development branch (synced with upstream/dev + your changes)
- `gitbutler/workspace` - GitButler's working branch
- `gitbutler/<feature>` - Virtual branches created by GitButler
- `gui-forward-drawdown-flag` - Example feature branch

### Keep Dev Updated:
```bash
# Periodically merge workspace changes to dev
git checkout dev
git merge gitbutler/workspace
git push origin dev
```

## Contributing Back to Public Repo

If you want to contribute a feature back to `Lumiwealth/lumibot`:

```bash
# 1. Create a clean feature branch from upstream/dev
git checkout -b feature-name upstream/dev

# 2. Cherry-pick or apply your changes
git cherry-pick <commit-hash>
# or manually apply changes

# 3. Push to your public fork (if you have one) or create PR from there
# NEVER: git push upstream (will be blocked anyway)
```

## Troubleshooting

### Accidentally on Wrong Remote?
```bash
# Check where you're about to push
git remote -v
git branch -vv

# Always be explicit if unsure
git push origin <branch>  # Good
git push                  # Relies on defaults (still safe due to config)
```

### GitButler Conflicts with Upstream?
```bash
# Rebase workspace onto latest upstream
git fetch upstream
git rebase upstream/dev gitbutler/workspace
```

### Want to Reset to Upstream State?
```bash
# DON'T DO THIS unless you want to lose private changes!
# Instead, create a new branch:
git checkout -b clean-upstream-copy upstream/dev
```

## File Organization

### Private Files (Never Go Upstream):
- `CLAUDE.md` - Project-specific Claude instructions
- `*.OLD.*` backups
- `tmp_*.py` - Temporary test files
- `.githooks/` - Custom git hooks
- Personal scripts in `scripts/`

### Modified Public Files:
- Files in `lumibot/` subdirectories
- `setup.py`, `requirements.txt`
- Tests

Strategy: Keep private additions in separate files when possible, minimize modifications to core lumibot files for easier upstream merging.

## Quick Commands

```bash
# Status check
git status
git branch -vv
git remote -v

# Sync upstream
./scripts/fetch_upstream.sh

# Push to private
git push origin <branch>

# Update dev
git checkout dev && git pull origin dev && git merge gitbutler/workspace && git push origin dev
```
