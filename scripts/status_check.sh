#!/bin/bash
# Quick status check for private fork + GitButler workflow

set -e

echo "═══════════════════════════════════════════════════════"
echo "  Lumibot Private Fork Status"
echo "═══════════════════════════════════════════════════════"
echo ""

# Current branch
echo "📍 Current Branch:"
git branch --show-current
echo ""

# Remotes verification
echo "🔗 Remotes:"
git remote -v | grep -E '(origin|upstream)' | head -4
echo ""

# Safety checks
echo "🛡️  Safety Checks:"
if git config remote.pushdefault | grep -q "origin"; then
    echo "  ✓ Push default: origin"
else
    echo "  ✗ Push default: NOT SET"
fi

if git config remote.upstream.pushurl | grep -q "DISABLED"; then
    echo "  ✓ Upstream push: DISABLED"
else
    echo "  ✗ Upstream push: NOT DISABLED"
fi

if [ -x .githooks/pre-push ]; then
    echo "  ✓ Pre-push hook: ACTIVE"
else
    echo "  ✗ Pre-push hook: MISSING or not executable"
fi
echo ""

# Branches comparison
echo "📊 Branch Status:"
echo "  Local branches:"
git branch --list | grep -E '(dev|gitbutler)' | head -5 | sed 's/^/    /'
echo ""
echo "  Origin branches:"
git branch -r | grep 'origin' | head -5 | sed 's/^/    /'
echo ""

# Commits ahead/behind
echo "🔄 Sync Status:"
git fetch origin --quiet 2>/dev/null || true
git fetch upstream --quiet 2>/dev/null || true

CURRENT_BRANCH=$(git branch --show-current)
if git show-ref --verify --quiet refs/remotes/origin/$CURRENT_BRANCH; then
    AHEAD=$(git rev-list --count origin/$CURRENT_BRANCH..$CURRENT_BRANCH 2>/dev/null || echo "0")
    BEHIND=$(git rev-list --count $CURRENT_BRANCH..origin/$CURRENT_BRANCH 2>/dev/null || echo "0")
    echo "  Current branch vs origin/$CURRENT_BRANCH:"
    echo "    Ahead:  $AHEAD commits"
    echo "    Behind: $BEHIND commits"
else
    echo "  Current branch ($CURRENT_BRANCH) not yet on origin"
fi
echo ""

# Working tree status
echo "📝 Working Tree:"
if git diff-index --quiet HEAD -- 2>/dev/null; then
    echo "  ✓ Clean (no uncommitted changes)"
else
    echo "  ⚠️  Uncommitted changes:"
    git status --short | head -10 | sed 's/^/    /'
fi
echo ""

# Recent commits
echo "📜 Recent History (last 5):"
git log --oneline --graph --decorate -5 | sed 's/^/  /'
echo ""

echo "═══════════════════════════════════════════════════════"
echo "Quick Actions:"
echo "  • Sync upstream:    ./scripts/fetch_upstream.sh"
echo "  • Push to private:  git push origin <branch>"
echo "  • Check workflow:   cat GITBUTLER_WORKFLOW.md"
echo "═══════════════════════════════════════════════════════"
