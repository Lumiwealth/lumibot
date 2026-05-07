# Martin PR #1011 Backtest Correctness Review Handoff

Date: 2026-05-07

## Purpose

Review Martin's large Lumibot performance/RAM optimization PR before it is allowed into a release. This work should be handled separately from the AI trading agents / investment committee launch work because the PR changes core backtesting behavior and has already changed at least one acceptance backtest baseline.

## Repositories and Worktrees

- Main active release branch for normal Lumibot work:
  `/Users/robertgrzesik/Development/lumibot-version-4.5.11`
  - Branch: `version/4.5.11`
  - Expected version: `setup.py` contains `version="4.5.11"`

- Martin PR review worktree:
  `/private/tmp/lumibot-pr1011`
  - Detached at PR head `27dd1b4e`
  - Remote branch: `origin/codex/lumibot-performance-optimizations`

- Current accidental local combined stack:
  `/Users/robertgrzesik/Development/lumibot`
  - Branch: local `dev`
  - Contains local-only AI committee commits plus merge commit `842b84d6 Merge Martin memory optimization PR`
  - Do not push this local `dev` branch directly.

Do not use `git checkout`. Use separate worktrees or `git switch` only where explicitly safe.

## PR Commits

Martin's PR range is from `07d280b1` to `27dd1b4e`:

- `ef2edac0 optimize lumibot backtest performance`
- `18a59c74 fix lumibot ci lint`
- `ee27d998 fix alpaca backtesting timestep parsing`
- `27dd1b4e rebaseline ibkr mes futures acceptance`

Local merge commit in the accidental combined branch:

- `842b84d6 Merge Martin memory optimization PR`

## Why This Is Risky

This PR is not just a memory optimization. It changes core backtest execution paths and changed the IBKR MES futures acceptance baseline:

- Old total return: approximately `-0.04%`
- New total return: approximately `-6.64%`
- Old CAGR: approximately `-4.46%`
- New CAGR: approximately `-99.98%`

That is a release blocker unless the review proves the old result was wrong and the new result is correct. Passing tests is not enough. The reviewer must explain every meaningful result drift in accounting terms.

## Main Change Areas

The PR touches 132 files with roughly `+9303/-1580` lines.

High-risk areas:

- `lumibot/backtesting/backtesting_broker.py`
  - New fast simple-order paths.
  - Direct filled-order processing.
  - Faster trade event logging.
  - Futures/crypto shortcut paths.
  - Bypasses parts of stream dispatch in selected cases.

- `lumibot/strategies/strategy.py` and `lumibot/strategies/_strategy.py`
  - Fast IBKR REST backtesting historical price cache.
  - Fast last-price paths.
  - Timestep parse caches.
  - Lazy strategy executor / indicators / agent manager loading.

- `lumibot/entities/data.py`, `lumibot/entities/bars.py`, `lumibot/entities/order.py`, `lumibot/entities/position.py`
  - Lazy pandas slice frames.
  - Fast native bar retrieval.
  - Deferred return columns.
  - Order/position changes that can affect accounting.

- Package exports and lazy imports:
  - `lumibot/__init__.py`
  - `lumibot/backtesting/__init__.py`
  - `lumibot/brokers/__init__.py`
  - `lumibot/data_sources/__init__.py`
  - `lumibot/entities/__init__.py`
  - `lumibot/tools/__init__.py`

- Provider/data helpers:
  - Alpaca, IBKR REST, Polygon, ThetaData, Tradier, Schwab, CCXT, Databento, Yahoo.

## Required Review Questions

1. Did Martin's PR change any backtest result that should not have changed?
2. Is the MES futures baseline drift correct, or did a fast path break accounting?
3. Do fast order paths preserve the same lifecycle events and callback semantics as the old path?
4. Do futures multiplier PnL, margin release, cash events, and position flips match the intended accounting model?
5. Do historical price caches respect simulation time and avoid lookahead?
6. Do cached histories distinguish asset, quote asset, exchange, timestep, length, and data source correctly?
7. Do lazy DataFrame slices behave like normal pandas DataFrames for strategy-facing operations?
8. Do lazy package exports preserve public import compatibility?
9. Are any CI workflow changes unrelated or risky?

## Required A/B Setup

Compare at least these states:

- Baseline before Martin: `07d280b1` or clean `origin/dev` before PR.
- Martin PR head: `27dd1b4e`.
- If needed, accidental local merge stack: `842b84d6` or current local `dev` in `/Users/robertgrzesik/Development/lumibot`.

Use separate worktrees or clean clones. Do not disturb `/Users/robertgrzesik/Development/lumibot-version-4.5.11`.

## Required Tests

Focused local tests:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 1800 .venv/bin/python -m pytest \
  tests/test_lazy_exports.py \
  tests/test_data_entity.py \
  tests/test_backtesting_broker.py \
  tests/test_order.py \
  tests/test_backtesting_futures_flips.py \
  tests/test_ibkr_crypto_backtesting_smoke_stubbed.py \
  tests/test_ibkr_futures_backtesting_smoke_stubbed.py \
  tests/test_ibkr_futures_daily_series.py \
  tests/test_ibkr_speed_burner_stubbed.py \
  tests/backtest/test_yahoo.py \
  -q
```

Broader release-style test:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 3600 .venv/bin/python -m pytest -m "not apitest and not downloader" --tb=short -q --durations=30
```

Performance claim check, only after correctness is proven:

```bash
LUMIBOT_BENCH_ITERATIONS=1000 /Users/robertgrzesik/Development/bin/safe-timeout 1800 .venv/bin/python scripts/bench_ibkr_speed_burner_stubbed.py
```

## Required Backtest Result Validation

For any changed acceptance result, especially IBKR MES futures, collect:

- Final portfolio value.
- Total return, CAGR, max drawdown.
- Trades count.
- Orders and fills.
- Per-trade PnL.
- Cash before/after each fill.
- Margin/multiplier behavior.
- Position quantity before/after each fill.
- Trade event log rows.
- Whether callbacks fired or were skipped.

The review must produce a written explanation of why the new numbers are correct or why the PR should not ship.

## Acceptance Criteria

Martin's PR can ship only if one of these is true:

1. The old MES baseline is proven wrong, the new result is proven correct, and explicit accounting regression tests are added.
2. The behavior-changing parts are removed or corrected, leaving only safe performance/lazy-loading changes.
3. The PR is deferred entirely and the AI trading agents work ships separately.

Do not accept "it passes tests" as enough. Result drift requires accounting-level proof.

## Short Prompt for Another Agent

Please review Martin's Lumibot PR #1011 (`origin/codex/lumibot-performance-optimizations`, head `27dd1b4e`) in `/private/tmp/lumibot-pr1011`. Treat it as a correctness-risk investigation, not a normal performance review. The PR changes 132 files and changed the IBKR MES futures acceptance baseline from about `-0.04%` total return to about `-6.64%`. Your job is to determine whether that result drift is correct or a regression. Compare against the pre-PR baseline (`07d280b1` / clean `origin/dev` before PR), inspect futures accounting/order lifecycle/cache changes, run focused and release-style tests, and produce a written accounting explanation. Do not touch `/Users/robertgrzesik/Development/lumibot-version-4.5.11`, which is reserved for the AI trading agents release work.
