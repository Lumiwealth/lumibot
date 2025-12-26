<!--
Session handoff for a new Codex/LLM session.
Scope: LumiBot ThetaData backtesting + acceptance validation backtests + CI status.
Do not delete: this file is referenced by humans when continuing work.
-->

# ThetaData Backtesting (LumiBot) — Session Handoff (2025-12-26)

This handoff captures the current state of **ThetaData backtesting** work in LumiBot, the **validation backtests** we use as acceptance criteria, and the most important gotchas for continuing safely.

## Hard constraints / guardrails (do not violate)

- **Never run `git checkout`** (or any tool that shells out to it such as `gh pr checkout`).
- Stay on branch `theta-bug-fixes` unless explicitly instructed otherwise; prefer `git switch` if needed.
- **Do not edit demo strategy files** under:
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/*`
- Backtesting + tests: always use the **stable** downloader URL:
  - `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
  - Do not use the legacy numeric IP (it can change on redeploy).
- Wrap long commands with `/Users/robertgrzesik/bin/safe-timeout …` (20m default; longer only when truly necessary).

## Repos / file locations

### LumiBot repo (this PR)

- Repo root:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot`
- Branch:
  - `theta-bug-fixes`
- Single PR to merge:
  - https://github.com/Lumiwealth/lumibot/pull/918
  - CI status (as of 2025-12-26): `gh pr checks 918` is **green** (unit shards + backtest tests).

### Strategy Library (acceptance backtests)

- Demo strategies (read-only):
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos`
- Backtest artifacts (HTML/CSV):
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/logs`

### Data Downloader (remote service + local checkout)

- Stable endpoint used by backtests:
  - `http://data-downloader.lumiwealth.com:8080`
- Local checkout (codebase, not part of this PR unless explicitly requested):
  - `/Users/robertgrzesik/Documents/Development/botspot_data_downloader`

## What this work is trying to guarantee (high level)

ThetaData backtests should behave broker-like:

- **No lookahead bias** (especially on day bars).
- **`get_last_price()` is trade-only** (never silently mid/quote).
- **Option MTM uses quotes when available** (bid/ask mid) because options often have no prints.
- Missing option pricing should not flip portfolio valuation to 0 (avoid “sawtooth” equity curves).
- CI must be green and reasonably fast, while preserving legacy test coverage.

## Docs you should read first

- Backtesting architecture:
  - `docs/BACKTESTING_ARCHITECTURE.md`
- ThetaData rules and safety:
  - `AGENTS.md`
  - `CLAUDE.md`

## Key code changes included in PR #918 (where to look)

### 1) ThetaData option MTM “sawtooth” fix (MELI was the repro)

Symptoms: large alternating down/up swings in portfolio value day-to-day while holding options.

Fix components:
- **Preserve daily option quote columns across session gaps**
  - File: `lumibot/entities/data.py`
  - Regression test: `tests/test_data_repair_times_and_fill_daily_quotes.py`
- **Option MTM prefers quote-derived mark and avoids “bad zeros”**
  - File: `lumibot/strategies/_strategy.py`
  - Regression test: `tests/test_thetadata_option_mtm_prefers_quote_mark.py`

### 2) Downloader URL normalization

We eliminate hard-coded numeric IP usage.
- File: `lumibot/tools/thetadata_queue_client.py`
  - Rewrites numeric IP `DATADOWNLOADER_BASE_URL` to `http://data-downloader.lumiwealth.com:8080`.

### 3) PR consolidation

There were multiple PRs open (916/917/918). They were consolidated into **one** PR:
- Kept as single PR: **#918**
- Closed as superseded:
  - https://github.com/Lumiwealth/lumibot/pull/916
  - https://github.com/Lumiwealth/lumibot/pull/917

## Unit tests (local + CI)

### Local tests run (targeted, fast, and relevant)

These passed locally (each wrapped in `safe-timeout 1200s`):
- `pytest -q tests/test_data_repair_times_and_fill_daily_quotes.py`
- `pytest -q tests/test_thetadata_option_mtm_prefers_quote_mark.py`
- `pytest -q tests/test_tearsheet.py`
- `pytest -q tests/test_portfolio_valuation_fallbacks.py`
- `pytest -q tests/backtest/test_theta_strategies_integration.py`

### CI (GitHub Actions) gotchas

- CI is sharded into unit test shards + a separate **Backtest Tests** job.
- Backtest tests write timing info to:
  - `tests/backtest/backtest_performance_history.csv`
  - This file is tracked in git, so local runs will dirty it; typically `git restore` it before committing unless the team explicitly wants to persist new rows.

## Backtesting acceptance validation suite (manual, end-to-end)

All acceptance backtests must be run from **Strategy Library** so artifacts land in `Strategy Library/logs`.

### Shared run requirements (use these every time)

Environment variables:
- `IS_BACKTESTING=True`
- `BACKTESTING_DATA_SOURCE=thetadata`
- `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
- `SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True`
- Optional:
  - `BACKTESTING_QUIET_LOGS=false` to get verbose logs (otherwise `*_logs.csv` may be empty)

Command skeleton:
```bash
cd "/Users/robertgrzesik/Documents/Development/Strategy Library"
/Users/robertgrzesik/bin/safe-timeout 2400s env \
  PYTHONPATH="/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot" \
  IS_BACKTESTING=True BACKTESTING_DATA_SOURCE=thetadata \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  SHOW_PLOT=True SHOW_INDICATORS=True SHOW_TEARSHEET=True \
  BACKTESTING_QUIET_LOGS=true BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_START=YYYY-MM-DD BACKTESTING_END=YYYY-MM-DD \
  python3 "Demos/<strategy>.py"
```

Note: Some “logs.csv” files are actually line-based logs (not a strict CSV). Use `head`/`rg` to inspect them.

## Acceptance backtests — definitions + what to check + last known run artifacts

### 1) Deep Dip Calls (GOOG; file name says AAPL)

- File: `Demos/AAPL Deep Dip Calls (Copy 4).py`
- Required window: `2020-01-01 → 2025-12-01`
- Acceptance checks:
  - At least **3** option-entry buy fills, covering the 2020 / 2022 / early-2025 dip eras.
  - **No GOOG split cliff** around mid-July 2022 (portfolio must not drop to ~0 from split math).
  - Artifacts exist: `*_trades.html`, `*_indicators.html`, `*_tearsheet.html`.

Last run (full window) artifacts:
- `Strategy Library/logs/AAPLDeepDipCalls_2025-12-25_19-08_WHRsPm_trades.csv`
  - Option-entry buy fills (4 total):
    - 2020-03-13
    - 2022-05-19
    - 2022-09-27 (re-entry)
    - 2025-04-04
- `Strategy Library/logs/AAPLDeepDipCalls_2025-12-25_19-08_WHRsPm_stats.csv`
- `Strategy Library/logs/AAPLDeepDipCalls_2025-12-25_19-08_WHRsPm_tearsheet.html`

### 2) Alpha Picks LEAPS (Call Debit Spread)

- File: `Demos/Leaps Buy Hold (Alpha Picks).py`
- Short acceptance window: `2025-10-01 → 2025-10-15`
  - Must trade **UBER, CLS, MFC** and record **both legs** (buy + sell) as fills.
- 1-year confidence window: `2025-01-01 → 2025-12-01`
  - Ideally trades more symbols; if not, capture explicit skip reasons in logs.

Last runs:
- Short window:
  - `Strategy Library/logs/LeapsCallDebitSpread_2025-12-25_19-14_lLFnSk_trades.csv`
    - Filled spreads for **UBER/CLS/MFC** (each has buy+sell leg).
- 1-year window:
  - `Strategy Library/logs/LeapsCallDebitSpread_2025-12-25_19-15_N2f6Qi_trades.csv`
    - Traded **UBER/CLS/APP**; **STRL/MFC** skipped.
  - Skip reasons (line-based log file):
    - `Strategy Library/logs/LeapsCallDebitSpread_2025-12-25_19-15_N2f6Qi_logs.csv`
      - STRL: “could not find a valid long-dated expiration; skipping.”
      - MFC: “could not find a valid long-dated expiration; skipping.”

### 3) TQQQ SMA200 (ThetaData vs Yahoo sanity)

- File: `Demos/TQQQ 200-Day MA.py`
- Window: `2013-01-01 → 2025-12-01`
- Acceptance check: ThetaData should not be obviously inflated vs Yahoo (close-ish parity).

Last runs:
- ThetaData:
  - `Strategy Library/logs/TqqqSma200Strategy_2025-12-25_19-22_UoZ2yn_tearsheet.html`
  - Printed summary (from run output):
    - CAGR ~ 0.413
- Yahoo:
  - `Strategy Library/logs/TqqqSma200Strategy_2025-12-25_19-20_cQkd1T_tearsheet.html`
  - Printed summary:
    - CAGR ~ 0.409

### 4) Backdoor Butterfly 0DTE (index + index options)

- File: `Demos/Backdoor Butterfly 0 DTE (Copy).py`
- Window: `2025-01-01 → 2025-12-01`
- Acceptance check:
  - Must not crash with `[THETA][COVERAGE][TAIL_PLACEHOLDER]` for SPX index data.
  - Artifacts must generate.

Last run:
- `Strategy Library/logs/BackdoorButterfly0DTE_2025-12-25_18-29_KAD4Qk_tearsheet.html`

If it ever crashes again:
- Look for `ValueError: [THETA][COVERAGE][TAIL_PLACEHOLDER] asset=SPX/USD (minute) ends with placeholders …`
- That usually means the downloader returned placeholder-only bars for SPX index minute coverage; investigate downloader index endpoints and SPX coverage.

### 5) MELI Deep Drawdown Calls (legacy strategy, MTM stability + tearsheet sanity)

- File: `Demos/Meli Deep Drawdown Calls.py`
- Required window: `2013-01-01 → 2025-12-18`
- What to check:
  - **Sawtooth pattern in 2024 is gone** (portfolio value should not alternate huge down/up swings day-to-day).
  - Tearsheets render and look internally consistent.

Most recent full-window run:
- `Strategy Library/logs/MeliDeepDrawdownCalls_2025-12-25_20-38_33bGtY_trades.html`
- `Strategy Library/logs/MeliDeepDrawdownCalls_2025-12-25_20-38_33bGtY_stats.csv`
- `Strategy Library/logs/MeliDeepDrawdownCalls_2025-12-25_20-38_33bGtY_tearsheet.html`

Comparison to the known-bad sawtooth run:
- Old run (bad): `Strategy Library/logs/MeliDeepDrawdownCalls_2025-12-24_13-39_jjcjPM_stats.csv`
  - 2024: 51 daily swings ≥20%, 23 “sawtooth pairs” (adjacent big opposite moves).
- New run (fixed): `Strategy Library/logs/MeliDeepDrawdownCalls_2025-12-25_20-38_33bGtY_stats.csv`
  - 2024: 4 daily swings ≥20%, 0 sawtooth pairs.

## Quick “sanity commands” for the next session

Check PR and CI:
```bash
cd "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot"
/Users/robertgrzesik/bin/safe-timeout 600s gh pr view 918
/Users/robertgrzesik/bin/safe-timeout 600s gh pr checks 918
```

Check you’re using local source (not the installed wheel):
```bash
python3 -c 'import lumibot; import inspect; print(lumibot.__file__)'
```

(Note: `Lumibot version X.Y.Z` printed at runtime comes from installed package metadata; it may not reflect `setup.py` unless you install the branch into your environment.)

