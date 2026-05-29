# LumiBot Release Branch Drift Investigation - 2026-05-29

## Summary

The local LumiBot checkout and pushed active branch drifted from the documented release process.
The package version is currently `4.5.40`, but the checked-out branch is still `version/4.5.37`.
This happened because new work and release tags were added on the stale `version/4.5.37` branch after the `v4.5.37` release instead of switching to the next active release branch.

## Evidence

- Current local branch: `version/4.5.37`.
- Current local `setup.py`: `version="4.5.40"`.
- `origin/dev`: `745cda0f`, tagged `v4.5.37`, with `setup.py` still at `4.5.37`.
- `origin/version/4.5.38`: `9b88690c chore: start 4.5.38`, created by GitHub Actions from `origin/dev`.
- `origin/version/4.5.39`: `261d2226 chore: start 4.5.39`, created by GitHub Actions from `origin/dev`, but its `setup.py` still shows `4.5.37`.
- Current stale branch history after `v4.5.37`:
  - `906d7e95 Fix Schwab stream startup race`
  - `d2c0b2db Bump LumiBot to 4.5.38`
  - `8b9728de Fix Schwab option order replacement`
  - `2645b0ec Add Schwab option modify live smoke`
  - `670df160 Bump LumiBot to 4.5.39`
  - `b1d49d1f Fix Schwab market order reconciliation`
  - `111a42ba Add Schwab option timeout cancel live smoke`
  - `a34118db Handle Schwab numeric live order ids in smoke tests`

## Release Workflow State

- `v4.5.37` release workflow succeeded.
- `v4.5.38` release workflow succeeded and published to PyPI from commit `d2c0b2db`.
- `v4.5.39` release workflow was cancelled during tests and did not publish to PyPI.
- No PRs were found for `4.5.38`, `4.5.39`, or `4.5.40`.

## Root Cause

`docs/DEPLOYMENT.md` says the local checkout must switch to the next version branch after release and that `setup.py` must match the branch name. That did not happen.

The GitHub Actions workflow created next-version branches, but nothing forced local developers or agents to use those branches. The release workflow only validates that the tag matches `setup.py`; it does not verify that the tagged commit came from `dev`, was merged through the documented release path, or was on the correct `version/X.Y.Z` branch before tagging.

## Repair Direction

Do not tag or deploy another LumiBot version from the stale `version/4.5.37` branch.

Recommended repair:

1. Freeze further LumiBot release activity until the branch is normalized.
2. Create the correct active branch for the next release from `origin/dev`.
3. Bring the real Schwab fixes from the stale branch onto that correct branch.
4. Ensure `setup.py`, `CHANGELOG.md`, and the branch suffix all match.
5. Merge the correct version branch to `dev`.
6. Tag the merge commit on `dev`.
7. Confirm PyPI publish and BotManager runtime install.
8. Confirm the post-release next-version branch exists and switch the canonical checkout to it.

## Follow-up Hardening

Docs were clear enough to detect the mistake, but not enough to prevent it. Add automation guards:

- A branch/version preflight that fails when `version/X.Y.Z` does not match `setup.py`.
- Release workflow validation that tagged commits are on `origin/dev` or exactly the merged release commit.
- A post-release verification command that confirms the next branch exists and the canonical checkout is on it.
- A BotManager deploy preflight that refuses to deploy when the LumiBot branch/version/tag state is inconsistent.
