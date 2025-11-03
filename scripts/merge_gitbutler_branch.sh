#!/bin/bash
# Merge a GitButler branch directly to dev (no PR needed)
# Usage: ./scripts/merge_gitbutler_branch.sh <branch-name>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <branch-name>"
    echo ""
    echo "Example:"
    echo "  $0 gui-forward-drawdown-flag"
    echo ""
    echo "Available origin branches:"
    git branch -r | grep origin/ | grep -v HEAD | sed 's/origin\//  /'
    exit 1
fi

BRANCH="$1"

echo "════════════════════════════════════════════════════════"
echo "  Merging GitButler Branch to Dev (No PR)"
echo "════════════════════════════════════════════════════════"
echo ""
echo "Branch to merge: $BRANCH"
echo ""

# Ensure we're up to date
echo "📥 Fetching latest from origin..."
git fetch origin

# Check if branch exists on origin
if ! git show-ref --verify --quiet refs/remotes/origin/$BRANCH; then
    echo "❌ Error: Branch 'origin/$BRANCH' doesn't exist"
    echo ""
    echo "Available branches:"
    git branch -r | grep origin/
    exit 1
fi

# Switch to dev
echo "📍 Switching to dev..."
git checkout dev

# Update dev from origin
echo "🔄 Updating dev from origin..."
git pull origin dev

# Merge the branch
echo "🔀 Merging origin/$BRANCH into dev..."
git merge --no-ff origin/$BRANCH -m "Merge $BRANCH into dev"

# Show result
echo ""
echo "✅ Merge complete!"
echo ""
echo "📊 Recent commits:"
git log --oneline --graph -5
echo ""
echo "Next steps:"
echo "  • Review the merge: git log"
echo "  • Push to private: git push origin dev"
echo "  • Delete remote branch: git push origin --delete $BRANCH"
echo "════════════════════════════════════════════════════════"
