# DEPLOYMENT

> Release/deployment workflow for LumiBot (version branches, changelog, tags, and GitHub releases).

**Last Updated:** 2026-03-30
**Status:** Active
**Audience:** Developers + AI Agents

---

## TL;DR (do this in order)

1) Get the `version/X.Y.Z` PR **green** (and ensure everyone has pushed their commits).
2) Merge latest `dev` into `version/X.Y.Z` and re-check CI (prevents drift / missing commits from other engineers).
3) Merge the PR into `dev` (no direct pushes to `dev`).
4) Tag the **merge commit on `dev`** as `vX.Y.Z` (this triggers GitHub Actions to publish to PyPI + create a GitHub Release).
5) Verify `pip install lumibot==X.Y.Z` works.
6) **Immediately** cut `version/X.Y.(Z+1)` from updated `dev`, bump `setup.py`, and push — **do this BEFORE triggering BotManager deploys** so other engineers/agents can continue working on the new branch while the slow downstream deploy runs.
7) Trigger BotManager deploys (dev then prod) — this takes ~30 minutes and should be the last step.

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
quantstats-lumi>=1.1.3,<1.2.0
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
- `setup.py` **must** match the version branch name (`X.Y.Z`).
  - When you start a new version branch, bump immediately and commit: `chore: start X.Y.Z`.
  - **Never downgrade** versions. If a bump was wrong, bump forward (and document why).
- After a version branch is merged to `dev`, **immediately start the next version branch** (see Step 7).

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
- **Docs:** links to investigation notes / runbooks if new operational behavior was added.

---

## Deployment Checklist (Recommended, end-to-end)

### Prereqs (GitHub release publishing)

Publishing is **tag-driven** via `.github/workflows/release.yml`.

- Required secret: `PYPI_API_TOKEN`
  - Must exist as a **repository secret** or as an **environment secret** for the GitHub environment named `pypi`.
  - If it’s missing, the “Publish to PyPI” step will fail.
- Optional: configure the GitHub environment `pypi` to require approvals (human gate).

0) **Preflight: “no-loss” + security/hygiene sweep**
   - Ensure there is **no local-only work** (multi-agent safety):
     - `git status --porcelain=v1` (must be empty)
     - `git log --oneline origin/version/X.Y.Z..HEAD` (must be empty)
     - If you see unexpected local changes/commits (even if you didn’t make them), **do not proceed** until you either:
       - review the diff, commit, and push, or
       - intentionally discard/revert them (manually; avoid destructive git commands).
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

0) **Sync your local repo**
   - `git switch dev && git pull --ff-only`
   - `git switch version/X.Y.Z && git pull --ff-only`
   - Confirm clean tree: `git status --porcelain=v1` (must be empty)
   - **IMPORTANT (multi-agent safety):** ensure *everyone* working on `version/X.Y.Z` has pushed their commits. Avoid releasing with local-only work in someone else’s clone.

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

6) **Start the next version branch (do this BEFORE BotManager deploy)**
   - This step is time-sensitive: other engineers and AI agents need a working branch to continue
     development while the slow BotManager deploy (~30 min) runs in the background.
   - Create `version/X.Y.(Z+1)` from `dev` (or from the just-deployed commit once it’s on `dev`).
   - Immediately bump `setup.py` to `X.Y.(Z+1)` and commit: `chore: start X.Y.(Z+1)`.
   - Add a new `CHANGELOG.md` section: `## X.Y.(Z+1) - Unreleased`.
   - Push the new branch to GitHub (so other agents don’t keep working on the old version branch):

     ```bash
     git switch dev
     git pull --ff-only
     git switch -c version/X.Y.(Z+1)
     # bump setup.py + CHANGELOG.md, then:
     git push -u origin version/X.Y.(Z+1)
     ```
   - Switch your local checkout to the new branch so you’re ready for the next cycle.

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
   - **Backtest smoke test**: Run a short backtest via BotSpot MCP or the API, then check that
     `settings.json` → `lumibot_version` matches the version you just deployed.
     - Use `get_backtest_artifact` with `label=settings.json` — the `lumibot_version` field in the
       JSON content shows exactly which version the ECS task ran.
     - If it shows the OLD version, the Docker image cache may be stale — re-trigger BotManager
       deploy with `force_rebuild_images=true`.
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
