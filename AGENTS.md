# LumiBot Agent Instructions (Theta / Downloader Focus)

These rules are mandatory whenever you work on ThetaData integrations.

## Backtesting Accuracy (Definition)

Backtesting “accuracy” is measured against live broker behavior when possible (replay a live-traded interval and reproduce fills + PnL within tolerances). Vendor parity (e.g., stored DataBento artifacts) is a regression signal, not absolute truth.

## Multi-Agent Collaboration (CRITICAL)
This repo is frequently edited by **multiple AI sessions**. To avoid lost work:

- **Release workflow (STRICT):**
  - **Never push directly to `dev`.** All work must land via a PR (usually from `version/X.Y.Z` → `dev`).
  - **Stay on the current branch.** If you start on a `version/*` branch, keep all commits on that branch and push that branch.
  - **Never switch branches without explicit user instruction.** If you suspect you are on the wrong branch, stop and ask.
  - **PRs must be version-scoped.** If a PR is needed for review/release, the PR head must be the existing `version/X.Y.Z` branch.
  - **PR title must be release-scoped.** Use `vX.Y.Z - <summary>` and include all notable changes shipped in that version (not just one feature).

- **Branch etiquette (STRICT):** if you are on a version branch (e.g., `version/4.4.31`), treat it as the shared collaboration branch.
  - **Do not create additional branches** (no `git switch -c`, no `git branch`, no `version/4.4.31-foo`, no `version/4.4.31/<topic>`), unless the user explicitly asks.
  - If a PR is needed for review, **the PR head must be the existing version branch** (e.g., `version/4.4.31`), not a new feature branch.
  - Do not switch branches unless explicitly instructed; if you suspect you're on the wrong branch, stop and ask.
- **No “feature branch chaining”:** if you’re already on a feature/WIP or version branch (e.g., `feature/*`, `fix/*`, `wip/*`, `version/*`, `release/*`, or a version-named branch like `X.Y.Z`), keep working there; don’t create another feature branch from it unless explicitly instructed.
- **Branch naming (LumiBot convention):** prefer version-scoped branches so multiple agents can collaborate without “feature branch naming drift”. Use the repo’s existing convention (e.g., `4.4.25` or `version/X.Y.Z`).
  - Default for active release work: the shared version branch (e.g., `4.4.25`).
  - Avoid `feature/*` and `fix/*` here unless explicitly requested.
  - If you truly need isolation and are explicitly instructed to branch, use a scoped suffix (e.g., `4.4.25/<topic>` or `version/X.Y.Z/<topic>`)—but don’t chain off that unless explicitly instructed.
- **Never run `git checkout`.** Avoid other destructive operations (`git reset --hard`, `git clean -f`, `git stash`).
- **Dirty-tree safety:** if you need to branch with uncommitted changes, create the new branch from the current working tree so the changes come with you; avoid `git stash`. Verify with `git status --porcelain=v1`.
- **Before committing:** `git status` must be clean/understood; read diffs for any changes you didn’t personally create.
- **Avoid stepping on the CI agent:** if `tests/backtest/`, baselines, or CI workflows are in-flight, coordinate via `docs/handoffs/` and keep edits non-overlapping.
- **Document + test behavioral changes:** update the relevant docs in `docs/` and add regression tests in the same commit; add comments explaining “why/invariants” for non-obvious logic.
- **Cache safety:** never delete shared caches. Use S3 namespace versioning (e.g., `LUMIBOT_CACHE_S3_VERSION=...`) for cold-cache simulations; only delete cache objects when explicitly requested and tightly scoped (symbol/version-specific).
- **Test gating (STRICT):** do not introduce new environment variables just to skip/disable tests or to paper over CI failures.
  - Prefer existing pytest markers (`apitest`, `acceptance_backtest`, etc.) and normal test skips with clear reasons.
  - If a new env var is truly required for a user-facing feature, document it in `docsrc/environment_variables.rst` in the same PR.

1. **Never launch ThetaTerminal locally WITH PRODUCTION CREDENTIALS.** Production has the only licensed session for that account. Starting the jar with prod credentials (even briefly or via Docker) instantly terminates the prod connection and halts all customers.
2. **Use the downloader for backtests.** All tests/backtests must set `DATADOWNLOADER_BASE_URL` and `DATADOWNLOADER_API_KEY` via the runtime environment. Do not short-cut by hitting Theta directly.
3. **Never hardcode or share private downloader URLs.** Do not paste real downloader hostnames/URLs into code, docs, tests, logs, AGENTS, or CLAUDE; use placeholders (e.g., `http://localhost:8080` or `https://<your-downloader-host>:8080`) and refer to `DATADOWNLOADER_BASE_URL`.

### Dev Credentials for Local ThetaTerminal Testing (SAFE)

There is a **separate dev account** that CAN be used for local debugging without affecting production:

| Field | Value |
|-------|-------|
| Username | `rob-dev@lumiwealth.com` |
| Password | `TestTestTest` |
| Bundle | STOCK.PRO, OPTION.PRO, INDEX.PRO |
| Location | `Strategy Library/Demos/.env` (commented out) |

**Verified working:** Dec 7, 2025

```bash
# Quick test with dev credentials
mkdir -p "/Users/robertgrzesik/Documents/Development/tmp/theta-dev-test"
echo -e "rob-dev@lumiwealth.com\nTestTestTest" > "/Users/robertgrzesik/Documents/Development/tmp/theta-dev-test/creds.txt"
java -jar $(python -c "import lumibot; import os; print(os.path.join(os.path.dirname(lumibot.__file__), 'tools', 'ThetaTerminal.jar'))") "/Users/robertgrzesik/Documents/Development/tmp/theta-dev-test/creds.txt" &
sleep 10
curl "http://127.0.0.1:25510/v2/status"  # Should show CONNECTED
pkill -f "ThetaTerminal.jar"  # Clean up
rm -rf "/Users/robertgrzesik/Documents/Development/tmp/theta-dev-test"
```

**Use dev credentials ONLY for:** Debugging ThetaTerminal itself, testing API endpoints, investigating data issues.
**Do NOT use for:** Running backtests (always use prod Data Downloader for consistent results).
3. **Respect the queue/backoff contract.** LumiBot no longer enforces a 30 s client timeout; instead it listens for the downloader’s `{"error":"queue_full"}` responses and retries with exponential backoff. If you add new downloader
   integrations, reuse that helper so we never DDoS the server.
4. **Long commands = safe-timeout (20m default max).** Wrap backtests/pytest/stress jobs with `/Users/robertgrzesik/bin/safe-timeout 1200s …` and break work into smaller chunks if it would run longer. Only use longer timeouts when absolutely necessary (e.g., explicit full-window acceptance backtests).
5. **Artifacts.** When demonstrating fixes, capture `Strategy\ Library/logs/*.log`, tear sheets, and downloader stress JSONs so the accuracy/dividend/resilience story stays reproducible.
6. **Write Location Policy (no “code files” outside Development).** Do not create helper scripts (e.g., `*.py`) under `/tmp` or other non-Development locations. Put LumiBot helpers under `scripts/` in this repo.

Failure to follow these rules will break everyone's workflows—double-check env vars before running anything.

---

## AGENTS.md / CLAUDE.md Best Practices (how we keep instructions useful)
- These instruction files are loaded automatically at session start. Keep guidance here **universal** and put deep, task-specific material in `docs/`.
- Prefer **progressive disclosure**:
  - Architecture + runbooks: `docs/` (start with `docs/BACKTESTING_ARCHITECTURE.md`).
  - Investigations and full trade audits: `docs/investigations/`.
  - Cross-session coordination: `docs/handoffs/`.
  - One-off helpers: `scripts/` (and keep them safe-timeout friendly).
  - **Public docs (Sphinx):** `docsrc/` is the source for the public documentation site; keep docstrings and Sphinx pages up to date for user-facing behaviors.
- When a workflow changes (new env vars, new cache semantics, new harness flags), update the relevant `docs/*` page in the same change set so other agents don’t re-learn it.
- **AI Navigation:** See `llms.txt` in repo root for structured documentation index
- **File naming convention (MANDATORY):** All docs use **UPPERCASE** names with underscores:
  - Root docs: `TOPIC_NAME.md` (e.g., `BACKTESTING_ARCHITECTURE.md`)
  - Handoffs: `YYYY-MM-DD_TOPIC_NAME.md` (e.g., `2026-01-04_THETADATA_HANDOFF.md`)
  - Investigations: `YYYY-MM-DD_TOPIC_NAME.md` (e.g., `2026-01-02_ACCURACY_AUDIT.md`)
  - Date-first for chronological sorting; UPPERCASE for consistency
- **File header (REQUIRED):** New docs must start with: Title, one-line description, Last Updated date, Status, Audience, and Overview section
- Interop note: `AGENTS.md` is the cross-tool convention; `CLAUDE.md` is Claude Code’s native file. If you want a single source of truth, Claude Code supports importing:
  - `@AGENTS.md`

## Env var documentation (REQUIRED)
- **Do not add new environment variables by default.** Env vars are hard to discover, hard to document, and easy to
  drift across deploy targets. Prefer explicit function parameters, config objects, or stable defaults.
- **Testing policy:** do not add env vars just to toggle/skip tests. If a test is slow/flaky/non-deterministic in CI,
  mark it with existing pytest markers like `pytest.mark.apitest` and/or `pytest.mark.downloader` (CI already runs
  with `-m "not apitest and not downloader"`).
- **Broker change policy:** when changing broker behavior (Tradier/Alpaca especially), add/extend:
  - deterministic unit/regression tests, and
  - `pytest.mark.apitest` smoke coverage against the real paper APIs when feasible (see `docs/SMART_LIMIT_LIVE_TESTING.md`).
- Only introduce a new env var when it is genuinely required for deployment/runtime configuration (secrets, endpoints,
  toggles needed for ops/rollout), and keep it narrowly scoped.
- If you add or change an environment variable, update:
  - `docsrc/environment_variables.rst` (public docs), and
  - `docs/ENV_VARS.md` when engineering notes help contributors.

## Changelog Maintenance (MANDATORY)

**Location:** `CHANGELOG.md`

**CRITICAL:** The changelog MUST be updated for every deployment, release, or significant change. The changelog documents ALL changes in a version, not just what the current AI session worked on.

### Version Source of Truth

The **authoritative version** is in `setup.py` (look for the `version=` line):
```python
version="X.Y.Z",  # e.g., "4.4.25", "4.5.0", etc.
```

When this version changes and is committed, it signals a deploy/release is happening or imminent.

### How to Find Changes for a Version

Before updating the changelog, **gather ALL commits** since the last version change:

```bash
# Find the current version
grep 'version=' setup.py

# Find when setup.py version was last changed
git log --oneline -p setup.py | grep -A2 -B2 'version=' | head -30

# Get commits since a specific commit/tag (replace X.Y.Z with actual previous version)
git log --oneline <last-version-commit>..HEAD

# Get commits since last version bump - replace X.Y.Z with the PREVIOUS version
git log --oneline $(git log --oneline -1 --all -S 'version="X.Y.Z"' -- setup.py | cut -d' ' -f1)..HEAD

# Or use the tag if available (replace X.Y.Z with previous version)
git log --oneline vX.Y.Z..HEAD
```

**IMPORTANT:** The changelog should include changes from ALL contributors (multiple AI agents, human developers), not just the current session. Read through ALL commits to capture the full picture.

### When to Update

- **Deployments** - Any code deployed to production
- **Version bumps** - New version tags or releases
- **Bug fixes** - Data source, split, dividend, or broker fixes
- **New features** - New brokers, data sources, strategy capabilities
- **Breaking changes** - API changes, env var changes (mark with ⚠️)
- **Performance/dependency updates**

### Format

```markdown
## X.Y.Z - YYYY-MM-DD

### Added
- New feature

### Changed
- Modified behavior

### Fixed
- Bug fix

### Deprecated / Removed / Security
- As applicable
```

### Pre-Deployment Checklist

- [ ] Reviewed git commits since last version (`git log <last-version>..HEAD`)
- [ ] Changelog entry added with current date
- [ ] Version number updated in setup.py (if applicable)
- [ ] All significant changes documented (from ALL contributors)
- [ ] Breaking changes marked with ⚠️

**If changelog wasn't updated, add a retroactive entry before the next deployment.**

### GitHub Release Markers (RECOMMENDED)

To keep deployments traceable (and easy to diff):

- **Follow the repo workflow** in `docs/DEPLOYMENT.md` (version branch → `deploy X.Y.Z` commit → changelog → tag → GitHub Release → next version branch).
- **Tag the deploy commit** with the semantic version (annotated tag): `vX.Y.Z`
- **Push the tag** to GitHub
- **Create a GitHub Release** from that tag and paste the corresponding `CHANGELOG.md` entry
- **PR title convention:** `X.Y.Z` (or `Release X.Y.Z`) so the version is visible in the PR list

## Scoped instruction files
- `tests/AGENTS.md` — rules for everything under `tests/` (legacy-test authority policy; treat tests before 2025-06-01 as high-authority, before 2025-01-01 as effectively frozen).

## Documentation Layout

- `docs/` = hand-authored markdown (architecture, investigations, handoffs, ops notes); start with `docs/BACKTESTING_ARCHITECTURE.md`
- Backtesting speed/parity playbook: `docs/BACKTESTING_PERFORMANCE.md` (update when you learn a new perf pattern)
- Handoffs: `docs/handoffs/`
- Investigations: `docs/investigations/`
- `docsrc/` = Sphinx source for the public docs site
- `generated-docs/` = local build output from `docsrc/` (gitignored)
- Docs publishing should happen via GitHub Actions on `dev` (avoid committing generated HTML)

## Public Documentation Updates (MANDATORY)

**Location:** `docsrc/` (Sphinx source files)

The public documentation at lumibot.lumiwealth.com is a critical resource for users. **Always update it when making user-facing changes.**

### When to Update Which File

| Change Type | Update This File |
|-------------|------------------|
| New broker added | `docsrc/brokers.*.rst` (create new file following existing pattern) |
| New strategy method | `docsrc/strategy_methods.*.rst` (add to appropriate category) |
| New strategy property | `docsrc/strategy_properties.rst` |
| New lifecycle method | `docsrc/lifecycle_methods.*.rst` |
| New entity class | `docsrc/entities.*.rst` |
| New backtesting data source | `docsrc/backtesting.*.rst` |
| New environment variable | `docsrc/environment_variables.rst` |
| Bug fix users hit often | `docsrc/common_mistakes.rst` |
| Common user question | `docsrc/faq.rst` |
| New feature (general) | `docsrc/getting_started.rst` or relevant section |
| Deployment changes | `docsrc/deployment.rst` |

### Documentation Update Checklist

When making code changes, ask yourself:
- [ ] Does this add a new user-facing feature? → Update relevant docsrc page
- [ ] Does this change existing behavior? → Update docs to reflect new behavior
- [ ] Did users report confusion about this? → Add to FAQ or Common Mistakes
- [ ] Is this a new broker/data source? → Create new broker/backtesting page
- [ ] Does this require new environment variables? → Update environment_variables.rst

### Building and Testing Docs Locally

```bash
cd docsrc
make clean html
# Preview at _build/html/index.html
# Or start local server:
python3 -m http.server 8765 --directory _build/html
```

### Periodic Documentation Audits

**Frequency:** At least once per major release or quarterly

**Audit checklist:**
- [ ] All public methods in Strategy class are documented
- [ ] All brokers have complete documentation
- [ ] All environment variables are listed
- [ ] Examples are up-to-date and working
- [ ] FAQ reflects recent user questions (check Discord/GitHub issues)
- [ ] Common Mistakes reflects recent bug reports
- [ ] Getting Started guide works for new users
- [ ] No broken links or missing images

**How to find gaps:**
```bash
# List all public methods not in docs
grep -r "def " lumibot/strategies/strategy.py | grep -v "^#" | grep -v "_" | head -20
# Compare against docsrc/strategy_methods*.rst

# Check for undocumented brokers
ls lumibot/brokers/*.py | grep -v __init__ | grep -v base
# Compare against docsrc/brokers*.rst
```

### AI Agent Documentation Responsibility (CRITICAL)

**Every time you work on LumiBot code, you MUST check and update BOTH documentation locations:**

| Location | Purpose | Audience |
|----------|---------|----------|
| `docs/` | Architecture, investigations, handoffs, engineering notes | AI agents & contributors |
| `docsrc/` | Public Sphinx documentation | End users & customers |

**When working on LumiBot, AI agents MUST:**
1. **Proactively update docs** - Don't wait to be asked; update both `docs/` and `docsrc/` as needed
2. **Check for doc gaps** - If you notice missing docs while working, flag or fix them in BOTH locations
3. **Keep examples current** - If you change API, update example code in both folders
4. **Add to FAQ/Common Mistakes** - When fixing user-reported bugs, document the fix
5. **Update engineering docs** - When you learn something about the codebase, add it to `docs/` so future AI sessions benefit

**Workflow for every code change:**
```
1. Identify relevant files in BOTH docs/ and docsrc/
2. Read them - are they accurate? Complete? Up to date?
3. If NO → Update them as part of your changes
4. If feature is undocumented → Add documentation to both locations
5. Build docsrc locally to verify customer-facing changes render correctly
```

**For `docs/` (AI/Engineering documentation):**
- Update `docs/BACKTESTING_ARCHITECTURE.md` when changing data flow
- Add to `docs/investigations/` when debugging complex issues
- Use `docs/handoffs/` for cross-session coordination
- Document "why" decisions and gotchas that future AI sessions need to know

**For `docsrc/` (Customer-facing documentation):**
- Update the relevant `.rst` file when changing user-facing behavior
- Add code examples that customers can copy-paste
- Document all parameters, return values, and edge cases
- Keep Getting Started guide accessible to beginners

**Examples of proactive documentation:**
- Working on `get_historical_prices()`? Check `strategy_methods.data.rst` AND `docs/BACKTESTING_ARCHITECTURE.md`
- Fixing a bug in Alpaca broker? Check `brokers.alpaca.rst` - does it mention this edge case?
- Adding a new order type? Update `strategy_methods.orders.rst` AND add an example to `examples.rst`
- Discovered a tricky caching behavior? Add it to `docs/` so future AI sessions don't rediscover it

**Do NOT:**
- Assume docs are complete because they exist
- Skip doc updates because "it's a small change"
- Leave doc updates for "later" or "another PR"
- Only update one location when both need updating
- Forget that `docs/` helps future AI sessions be more effective

**Remember:** If you touch code, check BOTH doc locations. If docs are incomplete, fix them. This is part of the definition of done for any task.

---

## Test Philosophy (CRITICAL FOR ALL PROJECTS)

### Test Age = Test Authority

When tests fail, how you fix them depends on **how old the test is**:

| Test Age | Authority Level | How to Fix |
|----------|----------------|------------|
| **>1 year old** | LEGACY - High authority | **Fix the CODE**, not the test. These tests have proven themselves over time. |
| **6-12 months** | ESTABLISHED - Medium authority | Investigate carefully. Likely fix the code, but could be test issue. |
| **<6 months** | NEW - Lower authority | Test may need adjustment. Still verify code isn't broken. |
| **<1 month** | EXPERIMENTAL | Test is still being refined. Adjust as needed. |

### Check Test Age Before Fixing

```bash
git log --format="%ai" --follow -- tests/path/to/test.py | tail -1
```

### Conflict Resolution

When old tests and new tests conflict:
1. **Old test wins by default** - it has proven track record
2. If the new test represents genuinely new functionality, **ask the user for judgment**
3. Document any judgment calls in the test file with comments

This philosophy applies to ALL projects, not just LumiBot.
