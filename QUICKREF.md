# GitButler Quick Reference

## Daily Workflow

```
┌─────────────────────────────────────┐
│  1. GitButler GUI                   │
│     • Work on features              │
│     • Commit in GUI                 │
│     • Click PUSH → origin ✅        │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  2. Merge to Dev (No PR)            │
│                                     │
│  ./scripts/merge_gitbutler_branch.sh│
│    <branch-name>                    │
└─────────────────────────────────────┘
```

## Commands

```bash
# 🔥 INTERACTIVE MERGE (recommended)
./merge

# Merge a specific branch
./scripts/merge_gitbutler_branch.sh gui-forward-drawdown-flag

# Sync from public repo (weekly)
./scripts/fetch_upstream.sh

# Check status
./scripts/status_check.sh

# Manual merge
git checkout dev
git pull origin dev
git merge --no-ff origin/<branch-name>
git push origin dev
```

## Safety

✅ **3 layers block upstream:**
1. Push URL = DISABLED
2. Pre-push hook
3. Default remote = origin

✅ **GitButler PUSH always goes to origin (private)**

## Where Things Go

```
GitButler PUSH → origin (private) ✅
Never          → upstream (public) ✅ BLOCKED
```
