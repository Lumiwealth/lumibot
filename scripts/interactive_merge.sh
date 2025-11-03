#!/bin/bash
# Interactive GitButler branch merger
# Finds unmerged branches and lets you select which to merge

set -e

# Colors for better UX
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  🔀 GitButler Branch Merger${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
echo ""

# Fetch latest
echo -e "${BLUE}📥 Fetching latest from origin...${NC}"
git fetch origin --prune --quiet

# Get dev branch
DEV_BRANCH="dev"

# Find all origin branches except dev and HEAD
echo -e "${BLUE}🔍 Scanning for unmerged branches...${NC}"
ALL_BRANCHES=$(git branch -r | grep 'origin/' | grep -v 'HEAD' | grep -v "origin/$DEV_BRANCH" | sed 's/origin\///' | xargs)

if [ -z "$ALL_BRANCHES" ]; then
    echo -e "${GREEN}✅ No unmerged branches found!${NC}"
    echo ""
    echo "All GitButler branches are already merged to $DEV_BRANCH."
    exit 0
fi

# Check which branches are actually ahead of dev
UNMERGED_BRANCHES=()
echo ""
for branch in $ALL_BRANCHES; do
    # Count commits in branch but not in dev
    AHEAD=$(git rev-list --count $DEV_BRANCH..origin/$branch 2>/dev/null || echo "0")
    if [ "$AHEAD" -gt 0 ]; then
        UNMERGED_BRANCHES+=("$branch")
        echo -e "  ${YELLOW}⚠️  $branch${NC} → $AHEAD commit(s) ahead of $DEV_BRANCH"
    fi
done

echo ""

if [ ${#UNMERGED_BRANCHES[@]} -eq 0 ]; then
    echo -e "${GREEN}✅ All branches are already merged!${NC}"
    echo ""
    echo "Available branches are up to date with $DEV_BRANCH."
    exit 0
fi

# Auto-select if only one branch
if [ ${#UNMERGED_BRANCHES[@]} -eq 1 ]; then
    SELECTED_BRANCH="${UNMERGED_BRANCHES[0]}"
    echo -e "${GREEN}✨ Auto-selected (only one unmerged branch):${NC} $SELECTED_BRANCH"
    echo ""
else
    # Interactive selection using select
    echo -e "${CYAN}Select a branch to merge into $DEV_BRANCH:${NC}"
    echo ""

    PS3=$'\n'"→ Enter number (or 'q' to quit): "
    select SELECTED_BRANCH in "${UNMERGED_BRANCHES[@]}" "❌ Cancel"; do
        if [ "$SELECTED_BRANCH" = "❌ Cancel" ]; then
            echo ""
            echo "Merge cancelled."
            exit 0
        elif [ -n "$SELECTED_BRANCH" ]; then
            echo ""
            break
        else
            echo -e "${RED}Invalid selection. Please try again.${NC}"
        fi
    done
fi

# Show what will be merged
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Branch to merge:${NC} $SELECTED_BRANCH"
echo -e "${CYAN}Target branch:${NC}   $DEV_BRANCH"
echo ""
echo -e "${YELLOW}Commits to be merged:${NC}"
git log --oneline --color $DEV_BRANCH..origin/$SELECTED_BRANCH | head -10
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Confirm
read -p "Proceed with merge? [Y/n] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! -z $REPLY ]]; then
    echo ""
    echo "Merge cancelled."
    exit 0
fi

echo ""
echo -e "${BLUE}🔄 Starting merge process...${NC}"
echo ""

# Check for uncommitted changes
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    echo -e "${YELLOW}⚠️  You have uncommitted changes.${NC}"
    echo ""
    git status --short
    echo ""
    read -p "Stash changes before merging? [Y/n] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
        echo -e "${BLUE}📦 Stashing changes...${NC}"
        git stash push -m "Auto-stash before merging $SELECTED_BRANCH"
        STASHED=true
    else
        echo -e "${RED}❌ Cannot proceed with uncommitted changes.${NC}"
        echo "Please commit or stash your changes first."
        exit 1
    fi
fi

# Switch to dev
echo -e "${BLUE}📍 Switching to $DEV_BRANCH...${NC}"
git checkout $DEV_BRANCH

# Update dev
echo -e "${BLUE}⬇️  Updating $DEV_BRANCH from origin...${NC}"
git pull origin $DEV_BRANCH

# Perform merge
echo -e "${BLUE}🔀 Merging origin/$SELECTED_BRANCH...${NC}"
if git merge --no-ff origin/$SELECTED_BRANCH -m "Merge $SELECTED_BRANCH into $DEV_BRANCH"; then
    echo ""
    echo -e "${GREEN}✅ Merge successful!${NC}"
    echo ""

    # Show result
    echo -e "${CYAN}Recent commits:${NC}"
    git log --oneline --graph --color -5
    echo ""

    # Ask to push
    read -p "Push $DEV_BRANCH to origin? [Y/n] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
        echo -e "${BLUE}⬆️  Pushing to origin/$DEV_BRANCH...${NC}"
        git push origin $DEV_BRANCH
        echo ""
        echo -e "${GREEN}✅ Push complete!${NC}"
    fi

    # Ask to delete remote branch
    echo ""
    read -p "Delete remote branch origin/$SELECTED_BRANCH? [y/N] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${BLUE}🗑️  Deleting origin/$SELECTED_BRANCH...${NC}"
        git push origin --delete $SELECTED_BRANCH
        echo -e "${GREEN}✅ Branch deleted!${NC}"
    fi
else
    echo ""
    echo -e "${RED}❌ Merge failed!${NC}"
    echo ""
    echo "Please resolve conflicts and run:"
    echo "  git merge --continue"
    echo "or abort with:"
    echo "  git merge --abort"
    exit 1
fi

# Restore stashed changes if we stashed
if [ "$STASHED" = true ]; then
    echo ""
    read -p "Return to previous branch and restore stashed changes? [Y/n] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
        git checkout -
        echo -e "${BLUE}📦 Restoring stashed changes...${NC}"
        git stash pop
    fi
fi

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✨ Done!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
