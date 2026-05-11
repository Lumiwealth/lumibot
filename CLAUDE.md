# CLAUDE.md - AI Assistant Instructions for LumiBot

## 🚨🚨🚨 RULE #1 — NEVER FABRICATE BACKTEST DATA 🚨🚨🚨

**Fake / synthesized / default-filled market data is STRICTLY FORBIDDEN.**
**Missing data is fine. Fake data is a lawsuit waiting to happen.**

- If real bars are unavailable for a day / symbol / timeframe, return empty. Do NOT invent a placeholder bar, do NOT carry-forward the last price, do NOT substitute a constant (e.g. `VIX=14.2`), do NOT interpolate.
- This applies to **every provider path** (IBKR, ThetaData, Polygon, Yahoo, DataBento, custom) and **every timestep** (minute, day, tick).
- Placeholder rows used for cache bookkeeping ("we tried this day, nothing was there") must NEVER be returned to a strategy as real bars. Filter them out, and surface the absence to the caller.
- If you find code that fabricates/synthesizes/forward-fills missing market data to keep a strategy alive — **delete it**. The honest outcome (empty frame, explicit warning, strategy skip) is the ONLY correct outcome.
- A backtest that makes decisions on fake data produces fake PnL. Showing fake PnL to customers is a legal and reputational catastrophe.
- Full details and enforcement rules live in `docs/BACKTESTING_ARCHITECTURE.md` under "RULE #1". Read it before touching any data-fetching code.

## Quick Start

**First, read these files:**
1. `docs/BACKTESTING_ARCHITECTURE.md` - Understand the backtesting data flow AND the no-fake-data rule
2. `AGENTS.md` - Critical rules for ThetaData (DO NOT SKIP)

## Image Generation Rule (CRITICAL)

- For any generated or AI-edited image, infographic, diagram, marketing visual, README visual, documentation visual, or repo asset image, use Nano Banana v3+ (`mcp__nano_banana__generate_image` / `mcp__nano_banana__edit_image`) or GPT Image 2+ quality only.
- This is non-negotiable for flow diagrams, architecture diagrams, sequence diagrams, screenshots-as-illustrations, and documentation visuals. Use models strong enough for readable labels, arrows, and layout.
- Never use generic image generators, cheaper/lower-quality image models, local SVG/HTML/canvas placeholders, Python drawing scripts, Mermaid screenshots, manually assembled box diagrams, or other fallback image pipelines for generated documentation/product images.
- Every generated image must be visually inspected before it is committed. Reject outputs with broken/missing text, awkward arrows, cluttered layouts, inaccurate product claims, or anything that looks like a placeholder.
- If Nano Banana v3+ or GPT Image 2+ access is unavailable or broken, stop and report that blocker instead of making replacement images another way.

## Backtesting Accuracy (Definition)

Backtesting “accuracy” is measured against live broker behavior when possible (replay a live-traded interval and reproduce fills + PnL within tolerances). Vendor parity (e.g., stored DataBento artifacts) is a regression signal, not absolute truth.

## Multi-Agent Collaboration (CRITICAL)
This repo is often worked on by **multiple AI sessions** at the same time.

- Canonical checkout: normal LumiBot work must happen in
  `/Users/robertgrzesik/Development/lumibot`. Keep that checkout on the active
  `version/X.Y.Z` branch, clean, and ready for release. Do not create sibling
  worktrees like `lumibot-version-X.Y.Z` for normal development. Temporary
  worktrees outside this folder are acceptable only for isolated review of
  unusually large or risky external PRs, and must not become the active release
  workspace.
- Branch etiquette: if a task mandates a specific version branch (e.g., `4.4.25`), treat it as the shared branch—stay on it and do not create new branches/PRs unless explicitly instructed. Otherwise, start new work branches from a stable base branch (e.g., `dev`/`main`/`master`), and avoid chaining feature/WIP branches.
- No “feature branch chaining”: if you’re already on a feature/WIP or version branch (e.g., `feature/*`, `fix/*`, `wip/*`, `version/*`, `release/*`, or a version-named branch like `X.Y.Z`), keep working there; don’t create another feature branch from it unless explicitly instructed.
- Branch naming (LumiBot convention): prefer version-scoped branches so multiple agents can collaborate without “feature branch naming drift”.
  - Default for active release work: the shared version branch (e.g., `4.4.25` or `version/X.Y.Z`).
  - Avoid `feature/*` and `fix/*` here unless explicitly requested.
  - If you truly need isolation and are explicitly instructed to branch, use a scoped suffix (e.g., `4.4.25/<topic>` or `version/X.Y.Z/<topic>`)—but don’t chain off that unless explicitly instructed.
- Never run `git checkout`; avoid destructive commands (`git reset --hard`, `git clean -f`, `git stash`).
- Dirty-tree safety: if you need to branch with uncommitted changes, create the new branch from the current working tree so the changes come with you; avoid `git stash`. Verify with `git status --porcelain=v1`.
- Before committing: `git status` must be clean/understood; read diffs for changes you didn’t personally create.
- Coordinate via `docs/handoffs/` when touching shared areas (CI/baselines/backtest harnesses).
- Any behavioral change must include docs updates + regression tests, with comments explaining “why/invariants”.

## Branch Management (CRITICAL - Branches Are Extremely Sensitive)

**ALWAYS stay on a `version/X.Y.Z` branch.** Never work on `master`, `main`, or `dev` directly. If you find yourself on `master` or `main`, something is wrong. Fix it immediately by switching to the current version branch.

### At session start (MANDATORY every time)
```bash
git branch --show-current  # MUST be version/X.Y.Z
grep ‘version=’ setup.py   # MUST match the branch name
```
If the branch is NOT `version/X.Y.Z`, find the latest version branch and switch to it:
```bash
git fetch origin
git branch -r | grep ‘version/’ | sort -V | tail -1  # Find latest
git switch version/X.Y.Z                               # Switch to it
```

### Before every commit (MANDATORY)
```bash
git branch --show-current  # Verify still on version/X.Y.Z
```

### Deployment procedure (follow `docs/DEPLOYMENT.md` strictly)
1. **All work** happens on `version/X.Y.Z`. Never push directly to `dev`.
2. **Before release**: merge `dev` into `version/X.Y.Z` to pick up other engineers’ changes.
3. **Release**: merge `version/X.Y.Z` into `dev` via PR, tag `vX.Y.Z` on the dev merge commit.
4. **After release (CRITICAL, DO NOT SKIP)**: the release workflow auto-creates `version/X.Y.(Z+1)` on the REMOTE. You must switch your LOCAL checkout to it. Steps:
   - `git status --porcelain=v1` — if non-empty, commit as `wip: carry-over to X.Y.(Z+1)` on the OLD branch first.
   - `git fetch origin && git switch version/X.Y.(Z+1)` (never `git checkout`).
   - If you had local-only commits on `version/X.Y.Z` (including the carry-over above), cherry-pick them onto the new branch.
   - Verify: `git branch --show-current` must print `version/X.Y.(Z+1)`; `grep 'version=' setup.py` must match.
   - Staying on the released branch locally causes every new commit to land on frozen code and uncommitted work to silently belong to the wrong version (the 2026-04-20 4.5.1 incident — WEEX/Coinbase/iter_count work had to be manually cherry-picked to 4.5.2 because the local checkout was never switched).
5. **BotManager deploy**: update `LUMIBOT_VERSION` variable, trigger dev then prod workflows.
6. **Post-deploy**: verify `lumibot_version` in a backtest’s `settings.json` matches (via BotSpot MCP `start_backtest` + `get_backtest_artifact` with `label="settings.json"`).

### What went wrong (2026-04-15 incident)
During the flat-price bug investigation, the local checkout silently switched from `version/4.4.62` to `master`. This was not caught until the user noticed. The deploy itself was correct (all remote artifacts were on the right branch), but the local state was wrong. **Always verify your branch before and after every operation.**

### The `master` branch is NOT used for active work
`master` is a legacy branch used only for GitHub Pages. The active branches are `dev` (source of truth) and `version/X.Y.Z` (active work). If you find yourself on `master`, you are in the wrong place.

## AGENTS.md / CLAUDE.md Best Practices (how we keep instructions useful)
- These instruction files are loaded automatically at session start, so keep guidance here **universal** and avoid dumping long, task-specific walls of text here.
- Prefer **progressive disclosure**:
  - Architecture + runbooks: `docs/` (start with `docs/BACKTESTING_ARCHITECTURE.md`).
  - Investigations and full trade audits: `docs/investigations/`.
  - Cross-session coordination: `docs/handoffs/`.
  - One-off helpers: `scripts/` (safe-timeout friendly).
  - **Public docs (Sphinx):** `docsrc/` is the source for the public documentation site; keep docstrings and Sphinx pages up to date for user-facing behaviors.
- When a workflow changes (env vars, cache semantics, harness flags), update the relevant `docs/*` page in the same change set.
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

### When to Update the Changelog

Update the changelog when ANY of these occur:
1. **Deployments** - Any code deployed to production
2. **Version bumps** - New version tags or releases
3. **Bug fixes** - Especially data source, split adjustment, or dividend fixes
4. **New features** - New brokers, data sources, or strategy capabilities
5. **Breaking changes** - API changes, env var changes, config changes
6. **Performance improvements** - Cache optimizations, query efficiency
7. **Dependency updates** - Major library version changes

### Changelog Format

```markdown
## X.Y.Z - YYYY-MM-DD

### Added
- New feature description

### Changed
- Modified behavior description

### Fixed
- Bug fix description

### Deprecated
- Feature being phased out

### Removed
- Removed feature

### Security
- Security-related changes
```

### Pre-Deployment Checklist

Before any deployment:
- [ ] Reviewed git commits since last version (`git log <last-version>..HEAD`)
- [ ] Changelog entry added with current date
- [ ] Version number updated in setup.py (if applicable)
- [ ] All significant changes documented (from ALL contributors)
- [ ] Breaking changes clearly marked with ⚠️

**If you forget to update the changelog, you MUST add a retroactive entry before the next deployment.**

## GitHub Release Markers (RECOMMENDED)

To keep deployments traceable (and easy to diff):

- Tag the deploy commit with the semantic version (annotated tag): `vX.Y.Z`
- Push the tag to GitHub
- Create a GitHub Release from that tag and paste the corresponding `CHANGELOG.md` entry
- PR title convention: `X.Y.Z` (or `Release X.Y.Z`) so the version is visible in the PR list

## Documentation Layout

- `docs/` = hand-authored markdown (architecture, investigations, handoffs, ops notes)
- Backtesting speed/parity playbook: `docs/BACKTESTING_PERFORMANCE.md`
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

## Project Overview

LumiBot is a trading and backtesting framework supporting multiple data sources (Yahoo, ThetaData, Polygon) and brokers (Alpaca, Interactive Brokers, Tradier, etc.).

## Key Locations

| What | Where |
|------|-------|
| LumiBot library | `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/` |
| Strategy Library | `/Users/robertgrzesik/Documents/Development/Strategy Library/` |
| Demo strategies | `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/` |
| Environment config | `Demos/.env` for strategies, `lumibot/.env` for library |
| Backtest logs | `/Users/robertgrzesik/Documents/Development/Strategy Library/logs/` |

## Critical Rules

### ThetaData Rules (MUST FOLLOW)

1. **NEVER run ThetaTerminal locally WITH PRODUCTION CREDENTIALS** - It will kill production connections
2. **Only use the Data Downloader** configured via `DATADOWNLOADER_BASE_URL` for backtests (avoid hard-coded IPs/hostnames—they can change on redeploy)
3. **Always compare ThetaData vs Yahoo** - Yahoo is the gold standard for split-adjusted prices
4. **Dev credentials available for local testing** - See `AGENTS.md` for details:
   - Username: `rob-dev@lumiwealth.com` / Password: `TestTestTest`
   - Safe to use locally without affecting production
   - Verified working Dec 7, 2025 with STOCK.PRO, OPTION.PRO, INDEX.PRO bundle
5. **Wrap long commands with safe-timeout (20m default max).** Use `/Users/robertgrzesik/bin/safe-timeout 1200s …` and split work into smaller chunks if it would run longer.
6. See `AGENTS.md` for complete rules

### Private endpoint hygiene (MUST FOLLOW)

- Never hardcode or paste private downloader URLs/hostnames into code, docs, tests, logs, `AGENTS.md`, or `CLAUDE.md`.
- Use placeholders like `http://localhost:8080` or `https://<your-downloader-host>:8080`, and refer readers to `DATADOWNLOADER_BASE_URL`.

### Data Source Selection

The `BACKTESTING_DATA_SOURCE` env var **OVERRIDES** explicit code:
```bash
# In .env file
BACKTESTING_DATA_SOURCE=thetadata  # Uses ThetaData regardless of code
BACKTESTING_DATA_SOURCE=yahoo      # Uses Yahoo regardless of code
BACKTESTING_DATA_SOURCE=none       # Uses whatever class the code specifies
```

### Cache Management

**LOCAL/DEV ONLY — if seeing wrong/stale data on your own machine or in dev:**
1. Bump `LUMIBOT_CACHE_S3_VERSION` (e.g., v5 → v6) — invalidates everything, forces clean rebuild
2. Clear local cache: `rm -rf ~/Library/Caches/lumibot/`

**🚨 PRODUCTION — NEVER bump `LUMIBOT_CACHE_S3_VERSION`.** Doing so invalidates the entire prod cache including ThetaData option chains, which are extremely expensive to rewarm and will make the site/backtests painfully slow for a long time. For prod issues:
- Prove bad data is actually *cached* (download a specific parquet and inspect) before deleting anything — the bug may be runtime-only
- Use **surgical** `aws s3 rm --recursive` on the narrowest affected prefix (e.g., `s3://lumibot-cache-prod/prod/cache/v1/ibkr/stock/` or `.../ibkr/index/`)
- It is OK to clear most of `prod/cache/v1/ibkr/` **except `ibkr/future/`** (futures cache is costly to rebuild)
- Never touch `prod/cache/v1/thetadata/` — that data is clean and rebuilds are slow

## Common Tasks

### Run a Backtest

```bash
cd "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos"
python3 "TQQQ 200-Day MA.py"
```

### Compare Yahoo vs ThetaData

1. Edit `Demos/.env`:
   - Set `BACKTESTING_DATA_SOURCE=yahoo`
2. Run backtest, note results
3. Edit `Demos/.env`:
   - Set `BACKTESTING_DATA_SOURCE=thetadata`
4. Run backtest, compare results
5. Results should match within ~1-2%

### Check Backtest Results

```bash
ls -la "/Users/robertgrzesik/Documents/Development/Strategy Library/logs/" | grep TQQQ | tail -10
```

Look at `*_tearsheet.csv` for CAGR and metrics.

## Known Issues & Fixes

### ✅ ThetaData Split Adjustment (FIXED - Nov 28, 2025)

**Status:** FIXED - Split handling now correct

**Root cause:** The `_apply_corporate_actions_to_frame()` function was being called 26+ times per backtest without any idempotency check, causing split adjustments to be applied multiple times.

**Fix applied:**
1. Added idempotency check at start of `_apply_corporate_actions_to_frame()` - checks for `_split_adjusted` column marker
2. Added marker at end of function after successful adjustment
3. Cache version bumped to v7

### ✅ ThetaData Dividend Split Adjustment (FIXED - Nov 28, 2025)

**Status:** FIXED - 17/21 dividends now match Yahoo within 5%

**Root causes found:**
1. `_update_cash_with_dividends()` was called 3 times per day without idempotency
2. ThetaData dividend amounts were UNADJUSTED for splits
3. ThetaData returned duplicate dividends for same ex_date (e.g., 2019-03-20 appeared 4x)
4. ThetaData returned special distributions with `less_amount > 0` (e.g., 2015-07-02)

**Fixes applied:**
1. Added `_dividends_applied_tracker` in `_strategy.py` to prevent multiple applications
2. Added split adjustment to `get_yesterday_dividends()` in `thetadata_backtesting_pandas.py`
3. Added deduplication by ex_date in `_normalize_dividend_events()`
4. Added filter for `less_amount > 0` to exclude special distributions

**Verified split adjustment:**
- ThetaData cumulative factor for 2014 dividends: 48x (2×3×2×2×2)
- After adjustment: $0.01182 raw → $0.000246 adjusted ≈ Yahoo's $0.000250 ✓

**Current results:** ~47% CAGR with ThetaData vs ~42% with Yahoo (gap due to phantom dividends)

### ⚠️ ThetaData Phantom Dividend (KNOWN ISSUE - Reported to ThetaData)

**Status:** KNOWN DATA QUALITY ISSUE - Reported to ThetaData support team

| Date | ThetaData | Yahoo | Status |
|------|-----------|-------|--------|
| 2014-09-18 | $0.41 raw | None | ⚠️ PHANTOM - main cause of CAGR gap |
| 2015-07-02 | $1.22 raw | None | ✅ FILTERED (less_amount=22.93) |
| 2020-12-23 | $0.000283 | None | ⚠️ PHANTOM |
| 2021-12-23 | $0.000119 | None | ⚠️ PHANTOM |

**Root cause:** ThetaData phantom dividends are DATA ERRORS in the SIP feed, not Return of Capital (ROC) distributions. Confirmed via Perplexity research - these amounts don't appear in any other financial database (Yahoo, Bloomberg, SEC filings).

**Workaround options:**
1. Use `BACKTESTING_DATA_SOURCE=yahoo` for dividend-sensitive strategies
2. Wait for ThetaData to fix the data quality issue
3. Accept ~5% CAGR gap as known ThetaData limitation

**Key files:**
- `lumibot/tools/thetadata_helper.py` - `_apply_corporate_actions_to_frame()`, `_normalize_dividend_events()`
- `lumibot/backtesting/thetadata_backtesting_pandas.py` - `get_yesterday_dividends()`
- `lumibot/strategies/_strategy.py` - `_update_cash_with_dividends()`

### ✅ ThetaData Zero-Price Data Filtering (FIXED - Nov 28, 2025)

**Status:** FIXED - Zero-price rows now filtered automatically

**Root cause:** ThetaData sometimes returns rows with all-zero OHLC values (e.g., Saturday 2019-06-08 for MELI), which caused `ZeroDivisionError` when strategies tried to calculate positions.

**Fix applied:**
1. Added zero-price filtering when loading from cache (`thetadata_helper.py` lines ~2501-2513)
2. Added zero-price filtering when receiving new data from ThetaData (`thetadata_helper.py` lines ~2817-2829)
3. Cache is self-healing - bad data is filtered on load

**Unit tests added:**
- `TestZeroPriceFiltering` class with 6 tests covering all edge cases
- Tests verify: zero-row removal, valid-zero-volume preservation, weekend-zero handling, partial-zeros, empty DF, all-zero DF

### Cache Version Mismatch

Always ensure `.env` files have matching cache versions:
- `lumibot/.env`
- `Demos/.env`

## Split Adjustment Testing (Added Dec 11, 2025)

### Test Files

Two new test files provide comprehensive split adjustment coverage:

| File | Tests | Purpose |
|------|-------|---------|
| `tests/test_split_adjustment.py` | 18 unit tests | Tests split-related functions with mocked data |
| `tests/test_thetadata_yahoo_parity.py` | 14 tests | Compares ThetaData vs Yahoo (gold standard) |

### Running Split Tests

```bash
cd /Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot

# Run unit tests only (no API calls, fast)
pytest tests/test_split_adjustment.py tests/test_thetadata_yahoo_parity.py -v -m "not apitest"

# Run integration tests (requires ThetaData credentials)
THETADATA_USERNAME=<username> THETADATA_PASSWORD=<password> \
pytest tests/test_thetadata_yahoo_parity.py -v -m apitest
```

### Test Coverage

**test_split_adjustment.py** covers:
- `_get_option_query_strike()` - Strike conversion for API queries (GOOG 20:1, AAPL 4:1, TSLA 3:1)
- `_apply_corporate_actions_to_frame()` - Price/dividend adjustments, idempotency
- `_normalize_split_events()` - Split event parsing (numerator/denominator, ratio string)
- `_normalize_dividend_events()` - Deduplication, special distribution filtering
- Edge cases: fractional splits, reverse splits, timezone handling

**test_thetadata_yahoo_parity.py** covers:
- Price parity before and after splits for GOOG, AAPL, TSLA
- GOOG March 2020 COVID crash (the specific scenario that triggered this testing)
- SPY baseline test (no recent splits)
- Multi-date consistency checks

### When to Run These Tests

1. **Before any change to `thetadata_helper.py`** - Run the full unit test suite
2. **After fixing a split-related bug** - Run integration tests to verify against Yahoo
3. **When investigating a backtest discrepancy** - Run parity tests for the affected symbol

### Known Test Status

- All 18 unit tests in `test_split_adjustment.py`: **PASSING**
- All 5 mocked tests in `test_thetadata_yahoo_parity.py`: **PASSING**
- Integration tests (9 tests): Require ThetaData credentials, marked with `@pytest.mark.apitest`

## Test Philosophy (CRITICAL - READ THIS)

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
# Check when a test file was first created
git log --format="%ai" --follow -- tests/path/to/test.py | tail -1

# Check when a specific test function was added
git log -p --all -S 'def test_function_name' -- tests/
```

### Conflict Resolution

When old tests and new tests conflict:
1. **Old test wins by default** - it has proven track record
2. If the new test represents genuinely new functionality, ask the user
3. Document any judgment calls in the test file with comments

### Adding Comments to Tests

For tests over 1 year old, add a comment when modifying:
```python
# LEGACY TEST (created Aug 2023) - This test has proven correct behavior
# DO NOT modify expected values without understanding the full impact
# If this test fails, FIX THE CODE, not this test
def test_important_behavior():
    ...
```

## Testing Checklist for Data Source Changes

1. Run TQQQ 200-Day MA with Yahoo (2013-2025) → expect ~30-45% CAGR
2. Run same strategy with ThetaData → should match Yahoo within ~5%
3. Check for anomalous daily returns (>50% gain/loss indicates split issue)
4. Compare specific prices around split dates (esp. Jan 13, 2022 2:1 forward split)

## Architecture Quick Reference

```
Strategy.backtest()
    │
    ▼
Data Source Selection (env var overrides code)
    │
    ├── Yahoo: yfinance → split-adjusted prices
    ├── ThetaData: Data Downloader → split-adjusted prices
    └── Polygon: Polygon API → handles splits in cache validation
    │
    ▼
BacktestingBroker (simulates trades)
    │
    ▼
Results (tearsheet, trades, logs)
```

See `BACKTESTING_ARCHITECTURE.md` for detailed data flow diagrams.

## CRITICAL: MCP Servers (MUST USE)

**ATTENTION CLAUDE:** These MCP servers are **ESSENTIAL** for working effectively on LumiBot. They have proven to make a **massive difference** in debugging complex data issues, validating financial data accuracy, and navigating this large codebase. **USE THEM PROACTIVELY AND FREQUENTLY.**

**IF THESE MCP SERVERS ARE NOT AVAILABLE:** Inform the user immediately and strongly recommend they install them. The debugging experience without these tools is significantly degraded. Installation instructions can be found in the Claude Code documentation.

### Why These Tools Matter for LumiBot

LumiBot deals with:
- Complex financial data from multiple sources (Yahoo, ThetaData, Polygon)
- Split adjustments, dividend calculations, and corporate actions
- Large codebase with intricate data flow paths
- Data accuracy requirements where small errors compound over time

Without MCP tools, debugging these issues is slow and error-prone. With them, you can validate data against external sources, systematically break down problems, and navigate the codebase efficiently.

---

### Sequential Thinking (`mcp__sequential-thinking__sequentialthinking`) - **USE THIS FIRST**

**STRONGLY RECOMMENDED** for ANY complex debugging task. This tool has been critical for:
- Breaking down backtesting discrepancies into systematic steps
- Analyzing why ThetaData vs Yahoo results differ
- Planning fixes that don't introduce regressions
- Debugging split/dividend calculation issues

**USE IT:** Before diving into complex code changes, use sequential thinking to plan your approach.

### Perplexity (`mcp__perplexity__*`) - **ESSENTIAL FOR DATA VALIDATION**

**CRITICAL** for validating financial data. LumiBot data issues often stem from incorrect source data. Perplexity lets you:
- `perplexity_search` - Verify stock split dates and ratios
- `perplexity_research` - Deep dive into dividend history discrepancies
- `perplexity_ask` - Quick validation of corporate action data

**REAL EXAMPLE:** We discovered ThetaData "phantom dividends" by using Perplexity to cross-reference against Yahoo, Bloomberg, and SEC filings. This identified data quality issues that would have been impossible to find otherwise.

**USE IT:** Whenever you see unexpected financial data, validate it with Perplexity before assuming the code is wrong.

### Memory (`mcp__memory__*`) - **TRACK YOUR FINDINGS**

**HIGHLY RECOMMENDED** for maintaining context across debugging sessions:
- Store known data source discrepancies
- Record phantom dividend dates and amounts
- Track cache version changes and their reasons
- Remember which fixes were applied and why

**USE IT:** When you discover an issue, store it in memory so future sessions don't have to rediscover it.

### X-Ray (`mcp__xray__*`) - **NAVIGATE THE CODEBASE**

**PARTIALLY USEFUL** for understanding the LumiBot codebase:
- `explore_repo` - **WORKS** - Map the directory structure
- `what_breaks` - **WORKS** - Find all usages of a function (text search)
- `find_symbol` - **REQUIRES ast-grep** - If this fails, install: `brew install ast-grep`

**USE IT:** Before modifying any function, use `what_breaks` to understand the impact. If `find_symbol` fails, use Grep tool instead.

### Context7 (`mcp__context7__*`) - **GET CURRENT DOCS**

**USEFUL** for library documentation:
- `resolve-library-id` - Find library IDs
- `get-library-docs` - Get current pandas, yfinance, polygon docs

### Chrome DevTools (`mcp__chrome-devtools__*`)

**USEFUL** for debugging Data Downloader issues:
- Test API endpoints directly
- Inspect network responses from ThetaData

---

## North Star Metrics and OKRs (CRITICAL)

**Every project MUST have clearly defined North Star metrics and OKRs.** If metrics don't exist for this project, propose them as your first action. If they exist, reference them when making decisions.

- Track leading indicators weekly. Create dashboards and graphs to visualize progress.
- When starting any task, check: does this move a North Star metric? If not, question its priority.
- See `/Users/robertgrzesik/Documents/Development/CLAUDE.md` for the full framework.
