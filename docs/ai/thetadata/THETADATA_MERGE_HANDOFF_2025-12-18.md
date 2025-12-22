# ThetaData Backtesting (LumiBot) — Merge Handoff (2025-12-18)

This is a handoff for the next agent to take the **working** `theta-bug-fixes` branch and make it **merge-ready** into `dev` (resolve PR conflicts, clean up docs/files, ensure full test suite passes, then re-run acceptance backtests).

**Hard constraints**
- **Never run `git checkout`** (absolute ban).
- **Do not modify any strategy files** in `Documents/Development/Strategy Library/Demos/*`.
- **Do not modify any live brokers** or any non-ThetaData backtesting sources; scope is **ThetaData backtesting only** inside LumiBot.
- Wrap long runs with `/Users/robertgrzesik/bin/safe-timeout …` (no dev servers/watchers without a timeout).

---

## 0) Repo + Branch Context

- LumiBot repo: `Documents/Development/lumivest_bot_server/strategies/lumibot`
- Active branch: `theta-bug-fixes`
- PR with conflicts: https://github.com/Lumiwealth/lumibot/pull/914
- Strategy Library (do not edit): `Documents/Development/Strategy Library/Demos`

---

## 1) Current Acceptance Targets (what “done” means)

### 1.1 Deep Dip Calls (GOOG; file name says AAPL)
- Strategy file: `Documents/Development/Strategy Library/Demos/AAPL Deep Dip Calls (Copy 4).py`
- Full acceptance window (final boss): **2020-01-01 → 2025-12-01**

**Acceptance checks (must)**
- **Trade entries:** opens an option position on each of the 3 major dips:
  - 2020 COVID crash window
  - 2022 bear market window (also validates GOOG split period handling)
  - early 2025 dip window (“tariff shock”)
- **No split cliff:** no catastrophic “portfolio value to near-zero” cliff around the GOOG split date (mid‑July 2022).
- **Backtest artifacts:** produces `*_trades.html`, `*_indicators.html`, `*_tearsheet.html` under `Documents/Development/Strategy Library/logs/`.

**How to validate quickly (no code changes)**
- From `.../logs/<run>_trades.csv`:
  - Count `side=buy` and `status=fill` rows for the option entries; expected **≥ 3** total across the full 2020–2025 run.
  - If you see **4** buys, it can be legitimate (stop-loss exit + re-entry in 2022). Confirm by checking for an intervening sell/exit around the re-entry.
- From `.../logs/<run>_stats.csv`:
  - Inspect the equity curve for **split cliffs** in July 2022.
  - Inspect “boxy” MTM behavior in 2025:
    - Some “flat then jump” can occur if option prices are unavailable and the MTM engine forward-fills (see §6.1).
    - The goal is: no long stretches of **stale/unchanged valuation caused by missing quotes** when NBBO exists.

**Why this should work now**
- Day bars are timestamped at **market close** (no lookahead into a full day bar pre-open).
- Option day bars preserve **NBBO bid/ask** when available (so quote-based mid can be used even when `close=0`).
- `get_last_price()` remains **trade-only** (no hidden mid fallback).

**Notes**
- A previous full run (pre timestamp fix) showed **4 buys** due to stop-loss exit + re-entry in 2022; re-validate expected buy count after conflict/test cleanup.
- User observed “boxy/flat then jumpy” portfolio value during 2025; likely MTM pricing availability (see §6.1).

### 1.2 Alpha Picks LEAPS (Leaps Call Debit Spread)
- Strategy file: `Documents/Development/Strategy Library/Demos/Leaps Buy Hold (Alpha Picks).py`

**Acceptance checks (must)**
- Trades **UBER, CLS, MFC** in the target window.
- It’s acceptable for:
  - **STRL** to skip if it truly cannot satisfy `min_days_to_expiry=300` for that sim date.
  - **APP** to skip if the spread violates the budget cap (spread too expensive).

**How to validate**
- From `.../logs/<run>_trades.csv`:
  - For each traded symbol (UBER/CLS/MFC), confirm the debit spread has **both legs** recorded (a buy leg and a sell leg).
  - Confirm fills exist (not just “new” orders).
- From `.../logs/<run>_logs.csv` (set `BACKTESTING_QUIET_LOGS=false` when debugging):
  - Confirm skip reasons for STRL/APP are strategy-logic reasons (DTE constraint, budget cap), not missing data.

**Why this should work now**
- OptionsHelper in ThetaData backtests searches nearby strikes and prefers **actionable** two-sided quotes instead of selecting an untradeable strike (bid=0).

### 1.3 TQQQ 200-Day MA (sanity + dividends/splits parity)
- Strategy file: `Documents/Development/Strategy Library/Demos/TQQQ 200-Day MA.py`
- Window: **2013-01-01 → 2025-12-01**

**Acceptance checks**
- ThetaData backtest performance is **not inflated** relative to Yahoo for the same window/strategy.
- Primary intent: ensure dividends + splits are not double-counted and day-bar timestamping does not introduce lookahead.

**How to validate**
- Compare `..._tearsheet.html` summary stats between Yahoo and ThetaData runs:
  - CAGR should be “close” (directionally consistent; not wildly inflated on ThetaData).
  - If ThetaData is materially higher, revisit dividend handling + split adjustment layering.

### 1.4 Evidence from recent runs (on this workstation)
These files exist under `Documents/Development/Strategy Library/logs/` and are useful as “known-good” references while resolving conflicts:

- Deep Dip (2025 window): `Documents/Development/Strategy Library/logs/AAPLDeepDipCalls_2025-12-17_23-58_1mNFwn_trades.html`
- Deep Dip (2022 window): `Documents/Development/Strategy Library/logs/AAPLDeepDipCalls_2025-12-17_23-59_c0PJco_trades.html`
- Alpha Picks LEAPS (UBER/CLS/MFC): `Documents/Development/Strategy Library/logs/LeapsCallDebitSpread_2025-12-17_23-52_NjZlum_trades.html`
- TQQQ SMA200 (ThetaData): `Documents/Development/Strategy Library/logs/TqqqSma200Strategy_2025-12-17_23-22_d6drvb_tearsheet.html`

If these disappear later, it’s still useful to preserve their **trade counts** and **shape** as the sanity baseline for post-merge re-runs.

---

## 2) What Was Fixed (Key Engineering Changes)

### 2.1 Day-bar timestamp alignment (eliminates lookahead bias)
**Problem:** Theta EOD day bars were coming in timestamped at midnight UTC, which converts to the prior evening in NY time. That makes the *whole day* bar observable before the session (lookahead bias).

**Fix:** Align ThetaData “day” frames to **market close** (16:00 America/New_York) converted to UTC, and make the transform idempotent.

- File: `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/tools/thetadata_helper.py`
  - Added helpers:
    - `_market_close_utc_for_date(date)`
    - `_align_day_index_to_market_close_utc(frame)`
  - Applied alignment on:
    - EOD fetch results
    - cache-hit returns
    - cache-load paths
  - Placeholder rows also now timestamp at market close (instead of midnight UTC).

- Test: `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_thetadata_day_timestamp_alignment.py`

### 2.2 `get_last_price()` purity for ThetaData backtesting (trade-only)
**Non-negotiable contract (backtesting):** `get_last_price()` must be **trade-only** (never bid/ask/mid). Options often have no prints; that should not silently turn last price into mark.

- File: `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/backtesting/thetadata_backtesting_pandas.py`
  - `get_last_price()` returns the most recent positive trade close at-or-before sim time.
  - For options in daily cadence, a controlled lookback expansion exists so “no print today” can still return a prior trade (matching typical broker “last trade may be stale” behavior).
  - It does **not** use bid/ask/mid.

- Test: `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_thetadata_get_last_price_trade_only.py`

### 2.3 Guarantee option EOD NBBO columns exist for day-mode quote usage
Goal: `get_quote()` should work for options in day-cadence ThetaData backtests using EOD NBBO (bid/ask) so strategies don’t fall back to `get_last_price(option)` (which may be stale/None).

- File: `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/tools/thetadata_helper.py`
  - Added/expanded logic so option day EOD caches can include bid/ask (“include_nbbo” / “include_eod_nbbo” path).
  - Ensures rows with OHLC=0 aren’t dropped when actionable bid/ask exists.
  - Added schema-upgrade behavior: if an option day cache exists without bid/ask columns and the caller requires NBBO, force refresh for that window.

### 2.4 Alpha Picks: actionable strike selection in ThetaData backtests (no strategy changes)
Observed failure mode: strategy sometimes picked an untradeable strike (bid=0) or failed spread checks even though nearby strikes had tradable quotes.

- File: `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/components/options_helper.py`
  - ThetaData-backtest-only improvements in `OptionsHelper.find_next_valid_option()`:
    - Scans nearby strikes rather than only “closest”.
    - Prefers strikes with two-sided actionable quotes (bid/ask).
    - Respects strategy-provided spread caps (`max_option_spread_pct` or `max_spread_pct`) when present.
  - Gated to `strategy.is_backtesting` and ThetaData backtesting data source.

- Test: `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_options_helper_thetadata_actionable_strikes.py`

---

## 2.5 Why the HTML artifacts sometimes seemed “missing” (and how it was fixed)
The `*_trades.html` and `*_indicators.html` files are only generated when the corresponding flags are enabled:
- `SHOW_PLOT=True` (trades plot)
- `SHOW_INDICATORS=True` (indicators plot)
- `SHOW_TEARSHEET=True` (tearsheet)

When users ran with these disabled (or with `SHOW_*` not set as expected), only the tearsheet could appear, leading to confusion.

---

## 3) How to Run Acceptance Backtests (reproducible commands)

### 3.1 General requirements
- Use the working downloader:
  - `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
  - Avoid hard-coded downloader IPs (they can change on redeploy)
- Ensure HTML artifacts are produced:
  - `SHOW_PLOT=True`
  - `SHOW_INDICATORS=True`
  - `SHOW_TEARSHEET=True`
- If you need logs/debugging, disable quiet logs:
  - `BACKTESTING_QUIET_LOGS=false`
  - (When `true`, `*_logs.csv` may be empty.)
- Run from Strategy Library so logs land in `Documents/Development/Strategy Library/logs`.

### 3.2 Deep Dip Calls (GOOG)
Run (examples):
```
/Users/robertgrzesik/bin/safe-timeout 2400s env \
  IS_BACKTESTING=True \
  BACKTESTING_DATA_SOURCE=thetadata \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=false \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=2025-01-01 BACKTESTING_END=2025-06-30 \
  python3 "Demos/AAPL Deep Dip Calls (Copy 4).py"
```

Full acceptance (longer; adjust timeout as needed):
```
/Users/robertgrzesik/bin/safe-timeout 7200s env \
  IS_BACKTESTING=True \
  BACKTESTING_DATA_SOURCE=thetadata \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=true \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=2020-01-01 BACKTESTING_END=2025-12-01 \
  python3 "Demos/AAPL Deep Dip Calls (Copy 4).py"
```

### 3.3 Alpha Picks LEAPS
```
/Users/robertgrzesik/bin/safe-timeout 2400s env \
  IS_BACKTESTING=True \
  BACKTESTING_DATA_SOURCE=thetadata \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=false \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=2025-10-01 BACKTESTING_END=2025-10-15 \
  python3 "Demos/Leaps Buy Hold (Alpha Picks).py"
```

### 3.4 TQQQ 200-Day MA (ThetaData vs Yahoo)
ThetaData:
```
/Users/robertgrzesik/bin/safe-timeout 7200s env \
  IS_BACKTESTING=True \
  BACKTESTING_DATA_SOURCE=thetadata \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=true \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=2013-01-01 BACKTESTING_END=2025-12-01 \
  python3 "Demos/TQQQ 200-Day MA.py"
```

Yahoo:
```
/Users/robertgrzesik/bin/safe-timeout 7200s env \
  IS_BACKTESTING=True \
  BACKTESTING_DATA_SOURCE=yahoo \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=true \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=2013-01-01 BACKTESTING_END=2025-12-01 \
  python3 "Demos/TQQQ 200-Day MA.py"
```

---

## 4) Where Outputs Land (and why trades/indicators sometimes “go missing”)

Artifacts are generated by LumiBot only when the flags are enabled:
- `SHOW_PLOT=True` → creates `*_trades.html` (+ trades CSV)
- `SHOW_INDICATORS=True` → creates `*_indicators.html` (+ indicators CSV)
- `SHOW_TEARSHEET=True` / `save_tearsheet=True` → creates `*_tearsheet.html` (+ tearsheet CSV)

Outputs directory (when running from Strategy Library):  
`Documents/Development/Strategy Library/logs`

Example artifacts that exist:
- `.../logs/AAPLDeepDipCalls_2025-12-17_23-58_1mNFwn_trades.html`
- `.../logs/AAPLDeepDipCalls_2025-12-17_23-59_c0PJco_trades.html`

---

## 5) Cache + Downloader Notes (ThetaData)

- Cache root (macOS typical): `~/Library/Caches/lumibot/1.0/thetadata`
- Files often include a `.meta.json` sidecar; helper validates parity and may delete corrupt caches (seen “sidecar_mismatch”).
- Remote downloader is enabled when `DATADOWNLOADER_BASE_URL` is set and not loopback.
- Data downloader codebase (local checkout): `Documents/Development/botspot_data_downloader`
- DNS/infra troubleshooting (read-only):
  - `data-downloader.lumiwealth.com` is the working host in this environment.
  - If the host/IP mapping is questioned, use AWS Route53 **read-only** commands to inspect records (do not mutate DNS).

---

## 6) Known Risks / Follow-ups (important)

### 6.1 Deep Dip 2025 “boxy PV” / flatlines
User reported portfolio value being flat for several days then making large jumps (e.g., June 2025).

Likely contributors to investigate after merge conflicts/tests are clean:
- Backtesting MTM uses `Strategy._update_portfolio_value()` which ultimately calls `_get_price_from_source()`:
  - It will try a **price snapshot** first (fast), then **last trade**, then (for options) it may attempt a quote-based mid.
  - If it still can’t price an option at the current bar, it may **forward-fill the last known price** (producing “flat then jump”).
- ThetaData option MTM quality in day mode depends on whether the option day EOD frames have actionable `bid`/`ask` on each day.
  - If `bid/ask` are missing/zero on many days for that contract, forward-fill becomes more common.

Concrete validation steps (after conflicts/tests):
- Re-run Deep Dip 2025 window with `BACKTESTING_QUIET_LOGS=false` and look for log warnings about forward-filled prices.
- Spot-check `..._stats.csv` for long runs of identical `portfolio_value` at the market-close rows.

This is not blocking if acceptance strategies trade correctly and no obvious split cliffs exist, but it’s the #1 “does this MTM feel real?” concern to keep eyes on.

### 6.2 Full Deep Dip run after timestamp alignment
Full 2020–2025 run was not re-captured after the day timestamp alignment change in this session. It should be re-run after conflicts/tests to confirm the final “3 dip trades” acceptance.

---

## 7) Merge-Readiness Work (next agent’s main tasks)

### 7.0 Merge game plan (high-level order of operations)
1) Inspect PR conflicts and CI status (read-only).
2) Bring `dev` into this branch and resolve conflicts (prefer keeping ThetaData backtesting fixes from this branch).
3) Run the full local test suite; fix any failures (ThetaData backtesting scope only; no live broker edits).
4) Documentation cleanup + restructure:
   - Decide where “human/AI docs” live (recommended: `docs/`).
   - Move repo-root handoff/investigation markdown there.
   - Change `docsrc/` output to a new `generated-docs/` (or similar) folder to avoid collisions.
   - Adjust GitHub Pages source folder + optionally add GitHub Actions doc build so local doc builds stop generating 200-file diffs.
5) Re-run the 3 acceptance backtests (Deep Dip full run, Alpha Picks, TQQQ Yahoo-vs-Theta).
6) Push branch; confirm GitHub Actions pass; only then merge PR.

### 7.0.1 Concrete command skeleton (no `git checkout`)
From `Documents/Development/lumivest_bot_server/strategies/lumibot`:
```
git fetch origin
git status -sb

# Merge dev into current branch (creates a merge commit; resolves PR conflicts locally)
git merge origin/dev

# Resolve conflicts manually; favor this branch’s ThetaData backtesting fixes unless clearly wrong.
git status

# After resolving:
git add -A
git commit -m "Merge dev into theta-bug-fixes (resolve conflicts)"

# Run tests (wrap in safe-timeout to avoid runaway)
/Users/robertgrzesik/bin/safe-timeout 1200s pytest -q
```

Then re-run the 3 acceptance backtests (commands in §3).

### 7.0.2 Detailed game plan (what still needs to be done)

This is the full “merge to dev” execution plan, in the order it should be done, with explicit deliverables.

**Phase A — PR triage + conflict inventory**
1) Confirm you are in the LumiBot repo and on the correct branch:
   - `pwd` should be `Documents/Development/lumivest_bot_server/strategies/lumibot`
   - `git status -sb` should show `theta-bug-fixes`
2) Review PR #914 status without touching branches:
   - `gh pr view 914`
   - `gh pr diff 914` (scan for “out of scope” changes; live broker edits should not be introduced as part of this ThetaData-only work)
   - `gh pr checks 914`
3) Capture a local baseline of what’s currently in this branch (so you can tell what a conflict resolution broke):
   - Record `git rev-parse HEAD`
   - Record the latest “known-good” backtest artifacts in §1.4 (trade counts + rough shape)

**Phase B — Resolve merge conflicts (prefer this branch’s ThetaData backtesting fixes)**
4) Bring `dev` into this branch via a merge commit (avoid rebase; avoid anything that might invoke `git checkout`):
   - `git fetch origin`
   - `git merge origin/dev`
5) Resolve conflicts file-by-file with these rules:
   - **Primary preference:** keep this branch’s changes for:
     - `lumibot/tools/thetadata_helper.py` (day timestamp alignment, option EOD NBBO preservation, cache integrity)
     - `lumibot/backtesting/thetadata_backtesting_pandas.py` (`get_last_price` trade-only, day-mode quote behavior)
     - `lumibot/components/options_helper.py` (ThetaData-backtest-only actionable strike selection)
     - new/updated tests under `tests/` that cover the above behavior
   - **Scope guardrail:** if conflicts touch live brokers or unrelated backtesting sources, do *not* “invent” new behavior—prefer `dev` unless the change is clearly required for ThetaData backtesting correctness.
   - **Do not reintroduce the old bug:** `get_last_price()` must never fall back to bid/ask/mid.
6) Commit the conflict resolution:
   - `git add -A`
   - `git commit -m "Merge dev into theta-bug-fixes (resolve conflicts)"`

**Phase C — Unit tests (local + CI)**
7) Run a fast targeted test pass first (quick signal before burning time on full suite):
   - `pytest -q tests/test_thetadata_day_timestamp_alignment.py`
   - `pytest -q tests/test_thetadata_get_last_price_trade_only.py`
   - `pytest -q tests/test_options_helper_thetadata_actionable_strikes.py`
8) Run the full test suite locally (must pass before merge):
   - `/Users/robertgrzesik/bin/safe-timeout 1200s pytest -q`
9) If tests fail:
   - Fix only what is necessary to make tests pass.
   - Keep changes **ThetaData-backtesting-only**; do not alter live broker implementations.
   - Re-run the failing subset first, then re-run the full suite.

**Phase D — Documentation cleanup + docs folder restructure**
10) Clean up repo-root “AI markdown” files (user request):
   - Move `THETADATA_INVESTIGATION_2025-12-11.md`, `THETADATA_BACKTESTING_HANDOFF_2025-12-17.md`, and this handoff into a stable home under a human-maintained docs folder (recommended: `docs/ai/` or `docs/backtesting/`).
11) Implement the docs folder split so generated docs no longer collide with human/AI docs:
   - Target structure:
     - `generated-docs/` = output built from `docsrc/`
     - `docs/` = human/AI-authored markdown (architecture + investigations + handoffs)
   - Update `docsrc/` build configuration so `make github` outputs to `generated-docs/` instead of `docs/`.
   - Update GitHub Pages settings to publish from `generated-docs/` (not `docs/`).
12) GitHub Actions for documentation (strongly recommended to avoid local 200-file diffs):
   - Add a workflow that builds docs on pushes to `dev` and publishes Pages artifacts.
   - This keeps doc generation deterministic and out of feature PR diffs.
13) Update agent guidance files so future agents follow the new doc layout:
   - Update `AGENTS.md` and `CLAUDE.md` to:
     - treat `docs/` as the home for AI/human markdown,
     - treat `generated-docs/` as build output (do not hand-edit),
     - document the doc build workflow and the “don’t run doc build locally unless requested” rule.

**Phase E — Re-run acceptance backtests (post-merge-conflict + post-test fixes)**
14) Re-run the 3 acceptance backtests (commands in §3) and confirm:
   - Deep Dip full window (2020–2025): has ≥3 dip entries; no split cliff in July 2022; artifacts produced; sanity-check MTM shape.
   - Alpha Picks: UBER/CLS/MFC trade; each spread has both legs filled; STRL/APP skip reasons remain strategy-logic (not data failure).
   - TQQQ: ThetaData vs Yahoo parity remains close (no inflated ThetaData result).

**Phase F — Final PR readiness**
15) Push branch and verify GitHub Actions green:
   - `git push`
   - `gh pr checks 914`
16) Only after CI is green, merge PR #914 into `dev`.

Deliverable for completion: PR merges cleanly, CI passes, and rerun artifacts confirm the three acceptance strategies still behave as expected.

### 7.1 Resolve PR conflicts (PR 914)
- Use the PR as the conflict inventory. Prefer this branch’s ThetaData backtesting fixes unless there’s a clear reason not to.
- Avoid `git checkout`. Use `git switch` if you must change branches.
- After conflicts are resolved:
  1) run full unit tests locally
  2) re-run acceptance backtests (Deep Dip / Alpha Picks / TQQQ)
  3) push and confirm GitHub Actions pass

Suggested `gh` commands (read-only safe):
```
gh pr view 914
gh pr diff 914
gh pr checks 914
```
(Do not use `gh pr checkout …` because it uses `git checkout` under the hood.)

If you need to watch checks without leaving a long-running process:
```
/Users/robertgrzesik/bin/safe-timeout 600s gh pr checks 914
```

### 7.2 Clean up “random markdown files” in repo root
These exist and should be moved or deleted:
- `Documents/Development/lumivest_bot_server/strategies/lumibot/THETADATA_INVESTIGATION_2025-12-11.md`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/THETADATA_BACKTESTING_HANDOFF_2025-12-17.md`
- (This file) `Documents/Development/lumivest_bot_server/strategies/lumibot/THETADATA_MERGE_HANDOFF_2025-12-18.md`

User preference: AI docs should live under a dedicated `docs/` (human-maintained) folder, not the generated docs output folder.

### 7.3 Docs folder re-org (to stop docsrc output fighting AI docs)
Current situation:
- `docsrc/` builds into `docs/` (generated), which clashes with AI-authored markdown.

Proposed target architecture:
- `generated-docs/` (or similar) = output of doc generation from `docsrc/`
- `docs/` = human/AI-authored markdown (backtesting architecture, handoffs, ops notes)

Actions needed:
- Update whatever build script in `docsrc/` controls output folder (do not run it unless requested).
- Update GitHub Pages config to point at `generated-docs/`.
- Update `AGENTS.md` + `CLAUDE.md` guidance so agents read/write the human `docs/` folder.

### 7.3.1 GitHub Actions docs build (recommended)
Current pain: running `make github` locally generates a huge number of diffs, drowning code review.

Proposed direction:
- Add a GitHub Action on `dev` that:
  - runs the doc build (e.g., `make github` or equivalent) in `docsrc/`,
  - outputs to `generated-docs/`,
  - publishes GitHub Pages from that artifact/folder.

This keeps documentation generation out of local dev and out of feature PR diffs.

### 7.4 Full test suite must pass
Run the full LumiBot tests locally (and ensure CI passes on GitHub).
If tests fail, fix only what’s required to pass; avoid touching live broker code.

### 7.5 Re-run acceptance backtests after conflict/test cleanup
Re-run:
- Deep Dip full window (2020–2025)
- Alpha Picks window (verify UBER/CLS/MFC)
- TQQQ (Yahoo vs ThetaData parity)

---

## 8) Files Most Likely Involved (for conflict resolution / review)

High-impact ThetaData backtesting files:
- `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/tools/thetadata_helper.py`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/backtesting/thetadata_backtesting_pandas.py`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/components/options_helper.py`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/strategies/_strategy.py` (MTM + pricing path)

Tests added/updated:
- `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_thetadata_day_timestamp_alignment.py`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_thetadata_get_last_price_trade_only.py`
- `Documents/Development/lumivest_bot_server/strategies/lumibot/tests/test_options_helper_thetadata_actionable_strikes.py`

---

## 9) Notes About ai_bot_builder (separate repo; not part of merge work)

- Only prompts should be edited in that repo; nothing else.
- `src/routes/chat_stream.py` must continue to yield `data: [DONE]` (this was accidentally removed once and reinstated).
- Prompt overhaul work is still pending and should not block merging the LumiBot ThetaData backtesting fixes.
