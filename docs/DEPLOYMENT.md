# DEPLOYMENT

> Release/deployment workflow for LumiBot (version branches, changelog, tags, and GitHub releases).

**Last Updated:** 2026-05-29
**Status:** Active
**Audience:** Developers + AI Agents

---

## TL;DR (do this in order)

1) **Run the release preflight first.** If branch, `setup.py`, `CHANGELOG.md`, tag, or `dev` state is inconsistent, stop. Do not tag. Do not deploy BotManager. Fix the branch first.
2) **Make the release branch contain everything safe to ship.** Dirty files, untracked files, and local-only commits are not excuses to skip work. Review them, commit them, test them, push them, and include them in the `version/X.Y.Z` PR unless they are unsafe, secret-bearing, broken, generated junk, or explicitly out of scope.
3) Get the `version/X.Y.Z` PR **green** after that full inclusion sweep.
4) Merge latest `dev` into `version/X.Y.Z` and re-check CI (prevents drift / missing commits from other engineers).
5) Merge the PR into `dev` (no direct pushes to `dev`).
6) Tag the **merge commit on `dev`** as `vX.Y.Z` (this triggers GitHub Actions to publish to PyPI + create a GitHub Release).
7) Verify `pip install lumibot==X.Y.Z` works.
8) **Switch your LOCAL checkout to `version/X.Y.(Z+1)`** after the release. Carry-over is only for work that appeared after the release was tagged/published, or work intentionally excluded because it was unsafe or out of scope. Verify with `git branch --show-current`, `grep version= setup.py`, and `git status --porcelain=v1` before doing anything else. This is NOT optional. There are zero exceptions.
9) Trigger BotManager deploys (dev then prod) only after step 8 is complete — this takes ~30 minutes and should be the last step.
10) Post-deploy: run an MCP backtest against prod and assert `settings.json.lumibot_version == "X.Y.Z"`. See step 8.

## The `dev` Branch Is Sacred (CRITICAL)

`dev` is the **single source of truth** for this project. Multiple external engineers (Brett, David, etc.) submit PRs against it, and multiple AI agents work off it concurrently.

**Rules:**
- **Always branch from `dev`**: every `version/X.Y.Z` branch starts from `dev`.
- **Always merge back to `dev`**: every release lands on `dev` via PR merge before tagging.
- **Always merge `dev` into your version branch before deploying**: other engineers may have merged PRs to `dev` while you were working. Step 0.5 in the checklist below ensures you pick those up.
- **Never let `dev` fall behind a release**: if you tagged a release, `dev` must contain that tag’s commit.
- **Never push directly to `dev`**: all changes land via PR merge.

After a release is published, the next version branch must be cut from `dev` immediately so the team isn’t blocked.

## QuantStats dependency policy

For tearsheet metric contract changes, LumiBot should require:

```text
quantstats-lumi>=1.1.5,<1.2.0
```

Release order for tearsheet metric changes:

1. Release `quantstats_lumi` first.
2. Update LumiBot's dependency floor and tests/docs.
3. Validate the released QuantStats package against the local LumiBot source in a clean environment.
4. Only then release LumiBot and roll downstream consumers.

## Goals

- Make deployments traceable (what code was deployed, when, and why).
- Keep multi-agent collaboration safe (shared `version/*` branches).
- Avoid “version drift” between deployed artifacts and `setup.py`.
- Make “what changed” readable (changelog + PR description quality).

---

## Branch + Version Rules (STRICT)

- Active work happens on a shared version branch: `version/X.Y.Z` (example: `version/4.4.31`).
- **Do not create extra branches** off a version branch unless explicitly instructed.
- **Do not push directly to `dev`.** All changes land in `dev` via PR merge.
- **Never update an old `version/*` branch to make a stale GitHub URL look current.** Historical version branches are release records, not redirect targets. If someone is viewing an older branch, give them the latest active `version/X.Y.Z` URL instead of pushing current work to the old branch or switching the canonical checkout backwards.
- The canonical checkout at `/Users/robertgrzesik/Development/lumibot` must stay on the latest active `version/X.Y.Z` branch. If it is on an older version branch, stop and fix that state with `git switch` only after verifying the tree is clean.
- `setup.py` **must** match the version branch name (`X.Y.Z`).
  - When you start a new version branch, bump immediately and commit: `chore: start X.Y.Z`.
  - **Never downgrade** versions. If a bump was wrong, bump forward (and document why).
- If `setup.py` is ahead of the current branch suffix, stop immediately. Do not keep committing on that stale branch. Create or switch to the matching `version/X.Y.Z` branch, verify it, and push it.
- After a version branch is merged to `dev`, **immediately start the next version branch** (see Step 7).

## Release Preflight Hard Stop (MANDATORY)

Run this before changing versions, tagging, creating a release, or triggering BotManager deploys:

```bash
set -e

branch="$(git branch --show-current)"
setup_version="$(python3 - <<'PY'
import re
from pathlib import Path
text = Path("setup.py").read_text()
match = re.search(r'version=["\']([^"\']+)["\']', text)
if not match:
    raise SystemExit("MISSING setup.py version")
print(match.group(1))
PY
)"
expected_branch="version/${setup_version}"
latest_tag="$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)"

echo "branch=${branch}"
echo "setup.py=${setup_version}"
echo "expected_branch=${expected_branch}"
echo "latest_tag=${latest_tag}"
git fetch origin dev --tags --prune
git status --porcelain=v1

test "${branch}" = "${expected_branch}" || {
  echo "ERROR: branch/setup.py mismatch. Switch to ${expected_branch} or fix setup.py before release."
  exit 1
}

test -z "$(git status --porcelain=v1)" || {
  echo "ERROR: dirty or untracked files. Review, commit, or manually remove before release."
  exit 1
}

git merge-base --is-ancestor origin/dev HEAD || {
  echo "ERROR: release branch is not based on origin/dev."
  exit 1
}

git log --oneline "origin/${branch}..HEAD"
```

If this preflight fails, the release is blocked. Do not "just bump setup.py" on the old branch. Do not tag from the old branch. Do not deploy BotManager while this is failing.

Known bad state example:

```text
branch=version/4.5.37
setup.py=4.5.40
expected_branch=version/4.5.40
```

That state means the checkout is stale. Create or switch to the correct branch before doing anything else.

---

## Release Inclusion Rule (NO EXCEPTIONS)

When you deploy LumiBot, deploy everything in the checkout that is safe and
intended to ship. Do not treat dirty, untracked, or local-only files as a reason
to omit work from the current release.

Before merging/tagging `version/X.Y.Z`:

1. Run `git status --porcelain=v1` and `git log --oneline origin/version/X.Y.Z..HEAD`.
2. Review every dirty file, untracked file, and local-only commit.
3. If it is safe and relevant, commit it to `version/X.Y.Z`, test it, push it,
   and make sure it is included in the release PR.
4. If it is not safe to ship, manually remove or fix it before release. Examples:
   secrets, `.env` files, logs, generated junk, accidental binaries, broken code,
   or work the release captain explicitly decides must not ship.
5. If another agent has local work on the release branch, coordinate and get it
   pushed before release. A release should not knowingly strand another agent's
   safe work on the old branch.

The normal default is inclusion, not carry-over. Carry-over exists only for
changes that appear after the release is already tagged/published, or for work
that was intentionally excluded from the release after review.

## Post-Release Branch Switch Rule (NO EXCEPTIONS)

The canonical LumiBot checkout must never remain on the just-released branch after
a release. This applies to humans and AI agents.

After the release is tagged/published, switch to `version/X.Y.(Z+1)` immediately.
If `git status --porcelain=v1` is not empty at that point, it means work changed
after the release or something was intentionally excluded. Do one of these
immediately:

1. Commit the dirty files on the old `version/X.Y.Z` branch as a carry-over commit,
   switch to `version/X.Y.(Z+1)`, cherry-pick the carry-over commit, then push
   `version/X.Y.(Z+1)`.
2. If a dirty file is truly wrong and must not survive, manually edit it back or
   remove only that specific generated artifact, then prove the tree is clean.

Do not leave files dirty. Do not leave local-only commits on the released branch.
Do not trigger BotManager deploys while the canonical checkout is still on
`version/X.Y.Z`. Do not use post-release carry-over as a substitute for shipping
safe work in the release that should have included it. Do not rely on another
agent to fix this later.

Mandatory verification after every release:

```bash
git branch --show-current            # MUST print version/X.Y.(Z+1)
grep 'version=' setup.py | head -1   # MUST print version="X.Y.(Z+1)",
git status --porcelain=v1            # MUST be empty
git log --oneline origin/version/X.Y.(Z+1)..HEAD
```

If the final command prints commits, push them:

```bash
git push origin version/X.Y.(Z+1)
```

---

## Release Captain Rules (STRICT)

When you are “the person deploying”, you own the release notes even if you didn’t write the code.

- **Read the full commit range** since the last `setup.py` bump and ensure `CHANGELOG.md` covers it.
- **Audit PRs** in the range for correctness, perf claims, and risk (don’t assume other bots did it right).
- **Enforce PR description quality** (template below).
- **Enforce perf evidence** when perf is claimed (YAPPI + measured before/after).

---

## PR Description Template (STRICT)

Every release PR should include:

- **Title:** `vX.Y.Z - <short summary>` (example: `v4.4.40 - Router backtesting speed fixes`)
- **What / Why:** one paragraph each.
- **Risk:** what could break; how to detect it quickly.
- **Tests run:** local commands + GitHub CI.
- **Perf evidence (if relevant):** exact commands + before/after numbers + profiler artifact path(s).
- **Docs:** links to updated `docs/` Markdown, `docsrc/` RST, investigation notes, runbooks, and visual assets for user-visible changes.

---

## Deployment Checklist (Recommended, end-to-end)

### Prereqs (GitHub release publishing)

Publishing is **tag-driven** via `.github/workflows/release.yml`.

- Required secret: `PYPI_API_TOKEN`
  - Must exist as a **repository secret** or as an **environment secret** for the GitHub environment named `pypi`.
  - If it’s missing, the “Publish to PyPI” step will fail.
- Optional: configure the GitHub environment `pypi` to require approvals (human gate).

0) **Preflight: “ship everything safe” + security/hygiene sweep**
   - Run the mandatory release preflight above. If it fails, stop. Fix branch/version state before release work continues.
   - The release branch must contain every safe change in the checkout before tagging:
     - `git status --porcelain=v1`
     - `git log --oneline origin/version/X.Y.Z..HEAD`
     - If you see dirty files, untracked files, or local-only commits (even if you didn’t make them), **do not proceed** until you review them.
     - If they are safe and relevant, commit them to `version/X.Y.Z`, test them, push them, and include them in the release PR.
     - If they are unsafe, secret-bearing, broken, generated junk, or explicitly out of scope, manually remove/fix only those files and document why they were excluded.
     - The expected state before merge/tag is clean local checkout plus no unpushed commits, because the release PR already contains everything that should ship.
   - Review what will ship (and look for “bullshit files”):
     - `git diff --name-status origin/dev..HEAD`
     - `git diff --stat origin/dev..HEAD`
     - Confirm there are no: `*.env`, `*.log`, `dist/`, `tmp/`, large stray binaries, or accidental artifacts.
   - Manual code review (security, best-effort):
     - Scan the diff for unexpected behavior: new process execution, credential handling, network calls, filesystem writes, or workflow changes.
     - Explicitly look for “malicious” indicators:
       - obfuscated code (large base64 blobs, weird string concatenation around URLs/commands)
       - `eval`/`exec`, unsafe deserialization (`pickle.loads`) in new code
       - new network destinations / hard-coded private endpoints
       - silent secret capture/exfil paths (reading `.env`, keychains, `~/.ssh`, AWS creds)
       - changes under `.github/workflows/` (must be intentional and reviewed)
     - If new/renamed modules were added, ensure they’re “boring” (no hidden side effects at import time).
     - If any new binary is added, confirm it’s expected and justified (size + provenance).
     - If anything feels off, stop and escalate before merging/releasing.
   - Quick secret sanity checks (best-effort):
     - Ensure `.env*` stays untracked (except examples like `.env.local.example`).
     - Scan changed docs/scripts for tokens/keys if you touched any credentials-related files.

0) **Documentation + visuals gate**
   - For every user-visible feature, behavior change, new tool, new environment variable, or deployment/runtime change, confirm documentation exists in both places that apply:
     - `docs/` Markdown for developer/runbook/reference docs.
     - `docsrc/` RST for published user-facing documentation.
   - Update the PR description with the exact documentation files changed.
   - Include visual aids whenever they would make the change easier to understand:
     - screenshots for UI or workflow changes,
     - flow diagrams for architecture/runtime/tool-call behavior,
     - sequence diagrams for deployment, broker, agent, credential, or data-provider flows.
   - Generate documentation visuals only with Nano Banana MCP. This applies to flow diagrams, architecture diagrams, sequence diagrams, screenshots-as-illustrations, README images, and other docs/product visuals.
   - For Lumibot, BotSpot, and Lumiwealth visuals, include the canonical Spot mascot where it improves understanding. Use Nano Banana with `reference_profile="botspot_spot"` or the approved brand reference images, and make Spot perform a topic-relevant action instead of standing as a generic decoration.
   - Never ship placeholder SVG/HTML/canvas diagrams, Mermaid screenshots, Python-drawn diagrams, manually assembled box diagrams, generic image-generator outputs, or cheaper/lower-quality model outputs for documentation/product visuals.
   - If Nano Banana MCP access is unavailable, stop and treat the missing visual as a release blocker until the asset can be generated at the required quality.
   - Check generated or reproducible visuals into the appropriate docs assets folder, with source prompts/scripts/model noted when practical.
   - Visually inspect every generated image before release. Reject images with unreadable labels, broken text, bad arrows, inaccurate product claims, or cluttered layouts.
   - If a change genuinely does not need docs or visuals, state that explicitly in the PR description under **Docs** with the reason.
   - If docs are changed, run the relevant docs build or at minimum inspect the changed RST/Markdown for broken references and missing images before release.

0) **Agent prompt gate**
   - For every platform capability, behavior change, broker/data-provider change, or new failure mode that strategy generation/refinement should understand, inspect the BotSpot agent prompts and shared examples.
   - Prefer token-efficient edits: update an existing note, reminder, or example before adding a new block. Remove or compress stale wording when adding new guidance.
   - Record prompt changes, or the reason no prompt change was needed, in the release PR under **Docs/Prompts**.

0) **Sync your local repo**
   - `git switch dev && git pull --ff-only`
   - `git switch version/X.Y.Z && git pull --ff-only`
   - Confirm branch and package version match:
     - `git branch --show-current` must print `version/X.Y.Z`.
     - `grep 'version=' setup.py | head -1` must print `version="X.Y.Z",`.
     - If these disagree, stop and fix the branch. Do not keep working.
   - Confirm clean tree: `git status --porcelain=v1` (must be empty because all safe local work has already been committed and pushed)
   - **IMPORTANT (multi-agent safety):** ensure *everyone* working on `version/X.Y.Z` has pushed their commits. Do not release while known safe work is stranded in someone else’s clone.

0.5) **Bring `dev` into the version branch (avoid drift)**
   - Merge `dev` into `version/X.Y.Z` and push the merge commit.
   - Re-check GitHub CI on the version PR after this merge.
   - Rationale: other people may have merged changes to `dev` while the version branch was in flight; this step ensures the release includes those changes.

1) **Verify tests**
   - Ensure required CI checks are green (unit + backtest + acceptance gates as applicable).
   - Local quick check (matches release workflow selection):
     - `python3 -m pytest -m "not apitest and not downloader" --tb=short -q --durations=30`
   - If the local quick check times out, do not guess. Record the timeout result, run targeted tests for the changed
     areas, push the version branch, and gate release on green GitHub CI for the same marker expression.

2) **Update changelog (FULL RANGE, not just “recent work”)**
   - Add/refresh the `CHANGELOG.md` entry for `X.Y.Z` (dated) and ensure it includes:
     - user-visible behavior changes
     - major perf changes (include before/after numbers)
     - operational changes (caches, infra dependencies, env vars, runbooks)
   - Include: `Deploy marker: <commit>` referencing the `deploy X.Y.Z` commit hash (added in Step 3).
   - The entry must include **all significant commits** since the previous `setup.py` version bump:
     - Find the previous bump commit:
       - `git log -p -- setup.py`
     - Build the draft changelog from the full range (pre-deploy marker):
       - `git log --oneline <previous-bump-commit>..HEAD`
     - After Step 3 creates the deploy-marker commit, re-run the range using that commit:
       - `git log --oneline <previous-bump-commit>..<deploy-marker-commit>`
   - If you merged before the changelog is complete, fix it immediately as a follow-up PR to `dev`.

3) **Deploy-marker commit (no version downgrades)**
   - Confirm `setup.py` is already `version="X.Y.Z"` (it should match the `version/X.Y.Z` branch).
     - If it’s wrong, fix it by bumping forward (never downgrade).
   - Ensure `CHANGELOG.md` has `## X.Y.Z - YYYY-MM-DD` and includes the full range of changes.
   - Commit with message: `deploy X.Y.Z` (this is the deploy marker).
   - Merge the version PR into `dev` (this makes `dev` the source of truth for everything that shipped).

4) **Tag + publish (preferred path)**
   - Why we merge to `dev` *before* tagging: tagging the `dev` merge commit guarantees `dev` includes exactly what shipped,
     and the next `version/*` branch cut from `dev` cannot “miss” released commits.
   - Create an annotated tag `vX.Y.Z` pointing at the *merge commit on `dev`* (or the deploy-marker commit if it was fast-forwarded).
   - Before tagging, verify you are tagging `dev`, not a stale version branch:
     - `git branch --show-current` must print `dev`.
     - `grep 'version=' setup.py | head -1` must print `version="X.Y.Z",`.
     - `git merge-base --is-ancestor HEAD origin/dev` must pass.
   - Push the tag to GitHub.
   - Let `.github/workflows/release.yml` run:
     - validates tag ↔ `setup.py`,
     - runs `pytest -m "not apitest and not downloader"`,
     - builds + publishes to PyPI,
     - creates the GitHub Release.

5) **Verify published artifacts**
   - PyPI is sometimes eventually-consistent (CDN/cache); the publish job can succeed but installs may fail for a few
     minutes. Always wait for the version to be visible/instalable before proceeding with downstream rollouts.
   - Confirm PyPI shows the expected version:
     - `python3 -m pip index versions lumibot | head`
   - Confirm the version is actually installable (retry for a few minutes):
     - `python3 -m pip install --no-deps "lumibot==X.Y.Z"`
     - `python3 -m pip show lumibot` (verify `Version: X.Y.Z`)
     - If it fails, retry with a short loop:

       ```bash
       VERSION="X.Y.Z"
       for i in {1..20}; do
         if python3 -m pip install --no-deps "lumibot==${VERSION}"; then
           echo "OK: lumibot==${VERSION} is installable"
           break
         fi
         echo "Waiting for PyPI propagation (${i}/20)..."
         sleep 15
       done
       ```
   - If you want to “force” a fresh fetch in an environment that may have cached wheels:
     - `python3 -m pip install --upgrade --force-reinstall --no-deps "lumibot==X.Y.Z"`
   - Confirm the GitHub tag exists and points at the intended commit:
     - `git show -s vX.Y.Z`

5.5) **If the release workflow fails (fast triage)**
   - Wrong commit tagged:
     - Symptom: “Validate tag version matches setup.py” fails.
     - Fix: tag the correct `dev` merge commit (and if you already published to PyPI, bump forward).
   - Missing `PYPI_API_TOKEN`:
     - Symptom: “Publish to PyPI” fails with auth/permission errors.
     - Fix: add `PYPI_API_TOKEN` (repo secret or `pypi` environment secret).
   - Version already exists on PyPI:
     - Symptom: PyPI rejects upload (file/version already exists).
     - Fix: bump to a new version (never reuse a version number).
   - Find failing run quickly:
     - `gh run list -R Lumiwealth/lumibot -w "Release (PyPI + GitHub)" -L 10`

6) **Switch your LOCAL checkout to `version/X.Y.(Z+1)` (NOT OPTIONAL — DO NOT SKIP)**

   > **Why this is step 6 and not a footnote:** remote branches don't protect you from your own local state. If you stay on `version/X.Y.Z` locally after the release, every subsequent `git commit` lands on a released branch, every uncommitted edit belongs to the wrong version, and any new work from other agents collides with yours on a branch that is supposed to be frozen. The 2026-04-20 4.5.1 deploy hit exactly this: WEEX/Coinbase/iter_count work was sitting uncommitted on `version/4.5.1` after release and had to be manually carried over to `version/4.5.2` via cherry-pick because the local branch was never switched.

   6a) **The release workflow auto-creates the remote branch** `version/X.Y.(Z+1)` from `dev` (see `.github/workflows/release.yml` → "Start next version branch" job). You do NOT need to create it. You DO need to pull it and switch your local checkout to it.

   6b) **Handle only post-release or intentionally excluded work here.** This step is not a loophole for skipping safe work during release. If uncommitted changes, untracked files, or local-only commits existed before the PR merge/tag, they should already have been reviewed, committed, tested, pushed, and deployed in `X.Y.Z` unless unsafe. Carry-over is only for changes that appeared after the release was tagged/published, or changes the release captain intentionally excluded because they were unsafe or out of scope. Move those remaining changes to `version/X.Y.(Z+1)`:

     ```bash
     # Snapshot anything uncommitted as a throwaway commit on the OLD branch
     git status --porcelain=v1           # if non-empty, commit before switching
     git add -A && git commit -m "wip: carry-over to X.Y.(Z+1)"
     # Note SHAs of all commits that are on local version/X.Y.Z but not on origin/version/X.Y.Z
     git log --oneline origin/version/X.Y.Z..HEAD
     ```

   6c) **Switch locally and cherry-pick the carry-overs.** Use `git switch`, never `git checkout`:

     ```bash
     git fetch origin
     git switch version/X.Y.(Z+1)        # pulls the auto-created remote branch
     git cherry-pick <sha1> <sha2> ...   # reapply carry-overs from step 6b
     ```

   6d) **Audit the CHANGELOG.** The auto-created branch already has `## X.Y.(Z+1) - Unreleased`. If your carry-over commits left entries under the wrong heading (a squashed WIP commit often does), move them by hand. Also confirm the `## X.Y.Z - Unreleased` heading for the just-released version was updated to `## X.Y.Z - YYYY-MM-DD`; if not, fix it as part of this step.

   6e) **VERIFY the switch stuck — this is the enforcement point.** Before you touch anything else:

     ```bash
     git branch --show-current            # MUST print version/X.Y.(Z+1)
     grep 'version=' setup.py | head -1   # MUST print version="X.Y.(Z+1)",
     git status --porcelain=v1            # MUST be empty after pushing carry-overs
     ```

     If any of those three checks is wrong, STOP and fix before proceeding. Do not trigger BotManager deploy on the wrong local branch state.

     If GitHub Actions created the next branch but your local checkout is still on the released branch, fix your local checkout immediately:

     ```bash
     git fetch origin
     git switch version/X.Y.(Z+1)
     git pull --ff-only
     ```

     If the next branch does not exist, create it from `dev`, not from the released branch:

     ```bash
     git switch dev
     git pull --ff-only
     git switch -c version/X.Y.(Z+1)
     # bump setup.py and CHANGELOG.md
     git add setup.py CHANGELOG.md
     git commit -m "chore: start X.Y.(Z+1)"
     git push -u origin version/X.Y.(Z+1)
     ```

   6f) **Push** anything you cherry-picked:

     ```bash
     git push origin version/X.Y.(Z+1)
     ```

7) **Downstream rollout (BotManager) — LAST STEP**
   - This takes ~30 minutes. Only trigger it after Step 6 is done so the team isn’t blocked.
   - Confirm BotManager is pinned to the new version and deploy workflows ran:

     ```bash
     gh variable set -R Lumiwealth/bot_manager LUMIBOT_VERSION -b "X.Y.Z"
     gh variable list -R Lumiwealth/bot_manager | rg ‘^LUMIBOT_VERSION’

     gh workflow run -R Lumiwealth/bot_manager "CI/CD - Development Environment" --ref main \
       -f force_rebuild_images=false -f skip_tests=false

     gh workflow run -R Lumiwealth/bot_manager "CI/CD - Production Environment" --ref prod \
       -f force_rebuild_images=false -f skip_tests=false

     gh run list -R Lumiwealth/bot_manager -L 10
     ```

8) **Post-deployment verification (REQUIRED)**
   - After BotManager deploys finish, verify the new version is actually running in production.
   - **Primary check — backtest smoke test**: Run a short backtest via BotSpot MCP and confirm
     `settings.json` → `lumibot_version` matches the version you just deployed. Use a simple,
     fast strategy (e.g. `TQQQMedian`) over a two-week window so the run completes in under ~3 min.
     Concrete MCP flow:
     1. `list_strategies` → pick a simple/fast one, grab its `strategyId`.
     2. `list_revisions(strategyId=…)` → grab a `revisionId`.
     3. `start_backtest(revisionId=…, startDate="YYYY-MM-01", endDate="YYYY-MM-15")` → returns
        `backtestId`.
     4. `backtest_status(backtestId=…)` until `status=completed`.
     5. `get_backtest_artifact(backtestId=…, label="settings.json")` → assert
        `jsonContent.lumibot_version == "X.Y.Z"`. This is the authoritative gate.
   - **MCP smoke-test matrix (run in parallel after the backtest finishes)** — confirms the whole
     BotSpot MCP surface is healthy end-to-end, not just that one backtest passed:
     - `get_account_status` → auth token works, billing cycle + active products are reachable.
     - `list_backtests(limit=3)` → recent history (including the smoke-test run) is queryable.
     - `list_deployments(limit=3)` → live bots respond with `status=running` and `live.performance.stats`
       populated (use this to confirm the version bump didn’t kill any bot — look for non-null
       `lastUpdated` on bots that were running before the deploy).
     - `query_csv(backtestId=<smoke-test id>, artifactType="trades.csv", sql="SELECT COUNT(*) FROM data")` →
       DuckDB artifact pipeline works, Parquet sibling resolves.
     - Any of these erroring is a deploy-validation failure even if `lumibot_version` matches.
   - **Live trading check** (if applicable): Confirm active bots restarted cleanly by checking
     deployment logs via `get_deployment_logs` or the BotSpot dashboard.
   - **Version mismatch troubleshooting**:
     - BotManager bakes `lumibot==${LUMIBOT_VERSION}` into Docker dependency images.
     - If the variable was updated but `force_rebuild_images=false`, the cached image might still
       have the old wheel. Rebuild with `force_rebuild_images=true` to force a fresh pip install.
     - PyPI CDN propagation can lag a few minutes after publish — if the deploy ran immediately
       after tagging, it may have installed the old version from cache. Wait and re-deploy.

---

## Common pitfalls (learned the hard way)

- **Forgetting to cut the next version branch after a release** blocks the entire team.
  - Symptom: everyone is still on `version/X.Y.Z` with uncommitted work piling up on an already-released branch.
  - Happened during 4.4.56→4.4.57: the next branch was never created, so local work accumulated on the
    stale `version/4.4.56` branch while PRs #976 and #981 merged to `dev` without being picked up.
  - Fix: Step 6 (cut next version branch) is **mandatory and immediate** — do it before BotManager deploy.
- **Not merging `dev` into the version branch before release** causes community PRs to be silently excluded.
  - Other engineers merge PRs to `dev` independently. If you don’t pull `dev` into your version branch
    before deploying, those changes won’t ship even though they’re merged.
  - Step 0.5 exists specifically for this — don’t skip it.
- **Version drift (`setup.py` doesn’t match the branch name)** breaks traceability and confuses deployments.
  - Fix: enforce “`setup.py` == `version/X.Y.Z`” as a hard invariant.
  - Never downgrade versions; always bump forward if something went wrong.
- **Continuing work on a stale version branch after bumping `setup.py` creates fake releases.**
  - Symptom: the branch says `version/4.5.37`, `setup.py` says `4.5.40`, and tags or BotManager deploys no longer tell you what actually shipped.
  - Fix: stop immediately, create or switch to the matching branch, push it, and do not tag until that branch is merged back to `dev`.
- **Publishing to PyPI without pushing the `vX.Y.Z` tag first** breaks traceability.
  - The repo’s release workflow is tag-driven. If the version is already on PyPI, pushing the tag later will
    cause the publish step to fail (PyPI rejects re-uploading the same version), and the GitHub Release step
    may not run.
  - Fix for next time: **tag first, publish via the workflow**.
  - If you must backfill after a manual publish: either accept a failed publish job and create the GitHub Release
    manually, or add a dedicated “GitHub Release only” workflow (future improvement).
- **Releasing from a version branch without merging back to `dev`** causes missing commits in the next version branch.
  - Symptom: `version/X.Y.(Z+1)` is missing changes that “definitely shipped” in `version/X.Y.Z`.
  - Fix: treat “merge to `dev`” as part of the release. Prefer tagging the `dev` merge commit (Step 4).
- **Perf claims without evidence** cause churn.
  - If a PR claims speedups, it must include: the exact benchmark command(s), measured before/after numbers, and
    profiler artifacts (e.g., YAPPI CSV path) or it doesn’t ship as “performance work”.
- **Release workflow environment drift** can break releases unexpectedly.
  - The release workflow runs a subset of tests (`pytest -m "not apitest and not downloader"`).
  - If a “unit” test actually requires external credentials (e.g., vendor logins, remote cache), the release workflow
    may not have those secrets available and will fail even when normal CI is green.
  - Fix direction: keep unit tests pure; use markers/skips for tests that require external services; document any
    required secrets and ensure the workflow environment is configured intentionally.
- **Local-only commits can silently hitch a ride in a release branch.**
  - Symptom: `git log origin/dev..HEAD` includes commits that were never pushed/reviewed on the mainline.
  - Fix: treat that range as required release-review input (code + changelog + risk) before creating the deploy marker.
- **Workflow file edits can be permission-gated**.
  - Some auth setups cannot push changes under `.github/workflows/` without a token that has the `workflow` scope.
  - If you hit this, don’t thrash: either use an appropriately-scoped token, or make a safe repo-side change that
    doesn’t require workflow edits (and document the limitation).
- **Use `python3`**, not `python` (macOS environments often don’t have `python`).
- **Wrap long commands** with `/Users/robertgrzesik/bin/safe-timeout …` to avoid hanging sessions.
- **Broker apitests are opt-in**:
  - Run with `pytest -m apitest …` and expect skips when the market is closed.
  - Tradier’s sandbox environment does not behave like a full live account for certain order lifecycle endpoints;
    design smoke tests to skip appropriately.

## Automation in place

- **Auto-create next version branch**: After the release workflow publishes to PyPI, a `start-next-version`
  job automatically creates `version/X.Y.(Z+1)` from `dev`, bumps `setup.py`, and pushes. This prevents
  the "forgot to create the next branch" problem that blocked development after v4.4.56.
- **Version logged at startup**: `LumiBot v{version} starting` is logged via `logger.info` when the
  package is imported, making it visible in CloudWatch, backtest logs, and live trading logs.
- **Version in backtest artifacts**: `settings.json` includes `lumibot_version` for every backtest,
  enabling post-deploy verification without log parsing.

## Future improvements (not yet implemented)

- **Post-deploy canary in BotManager CI**: After the BotManager deploy workflow finishes, a final job
  should trigger a short canary backtest via the API, wait for completion, and assert that
  `settings.json → lumibot_version` matches the deployed version. This catches stale Docker images
  and PyPI propagation lag automatically. Implementation lives in bot_manager's CI workflows and
  should be done carefully since a flaky canary would block production deploys.
- **Automated version assertion in `scheduled_test_backtest.py`**: The existing Lambda that runs
  canary backtests on a schedule (`bot_manager/lambda/scheduled_test_backtest.py`) should check
  `lumibot_version` in the result and alert if it doesn't match the pinned `LUMIBOT_VERSION` variable.

## Notes

- Avoid destructive git operations (`git checkout`, `git reset --hard`, `git stash`).
- Keep release bookkeeping changes small and explicit (version bump + changelog + tag/release).
