# PR 1100 Startup Optimization Risk Review

One-line description: Merge-risk review for Martin Pelteshki's scheduled startup optimization PR.

Last Updated: 2026-07-03

Status: Review complete

Audience: LumiBot maintainers, BotSpot deployment operators, release captain

## Overview

Reviewed <https://github.com/Lumiwealth/lumibot/pull/1100> without merging or switching the canonical checkout. The PR is titled `Optimize scheduled deployment startup paths` and targets `dev` from `perf/startup-optimization`.

Current GitHub metadata at review time:

- 56 changed files.
- 6,150 additions and 1,124 deletions.
- Mergeable, review required.
- Visible checks were limited: GitHub Pages `build` passed, GitHub Pages `deploy` skipped, CodeRabbit passed. No full LumiBot test matrix was visible in the PR check rollup.

## Scope Touched

The PR is broad. It adds lazy import helpers and changes startup/materialization behavior across:

- `lumibot._lazy_imports`, `lumibot._lazy_timezone`, package exports.
- Broker constructors and provider modules for Alpaca, CCXT, Schwab, Tradier, Tradovate, Bitunix, ProjectX, and IBKR REST.
- `lumibot.credentials` default broker/data-source resolution.
- `Strategy`, `StrategyExecutor`, and `Trader` import/constructor/runtime paths.
- Scheduled `run_once` market-open handling and stream/order-thread defaults.
- Environment variable docs and startup optimization investigation notes.

This is not a small or low-blast-radius patch even though the intended behavior is performance-oriented.

## Local Verification

Review was done from a temporary detached PR worktree at `/Users/robertgrzesik/Development/lumibot-pr1100-review.mYCiwe`; the canonical `/Users/robertgrzesik/Development/lumibot` checkout remained on `version/4.5.65`.

Commands run:

```bash
git diff --check origin/dev...origin/pr/1100
```

Result: passed.

```bash
/Users/robertgrzesik/Development/lumibot/.venv/bin/python -m compileall -q lumibot
```

Result: passed.

```bash
env -u DATA_SOURCE -u TRADING_BROKER -u POLYMARKET_PRIVATE_KEY -u POLYMARKET_OWNER_ADDRESS -u POLYMARKET_WALLET_ADDRESS -u POLYMARKET_CLOB_API_KEY -u POLYMARKET_CLOB_API_SECRET -u POLYMARKET_CLOB_API_PASSPHRASE -u POLYMARKET_RELAYER_API_KEY -u POLYMARKET_RELAYER_API_KEY_ADDRESS LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_DISABLE_DOTENV_LOCAL=1 /Users/robertgrzesik/Development/lumibot/.venv/bin/python -m pytest tests/test_lazy_exports.py tests/test_scheduled_run_once.py tests/test_strategy_methods.py tests/test_smart_limit_multileg_unit.py tests/test_smart_limit_single_leg_unit.py tests/test_backtesting_parameters.py tests/test_backtesting_data_source_env.py tests/test_backtesting_datetime_normalization.py tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage -q
```

Result: 110 passed.

```bash
env -u DATA_SOURCE -u TRADING_BROKER -u POLYMARKET_PRIVATE_KEY -u POLYMARKET_OWNER_ADDRESS -u POLYMARKET_WALLET_ADDRESS -u POLYMARKET_CLOB_API_KEY -u POLYMARKET_CLOB_API_SECRET -u POLYMARKET_CLOB_API_PASSPHRASE -u POLYMARKET_RELAYER_API_KEY -u POLYMARKET_RELAYER_API_KEY_ADDRESS LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_DISABLE_DOTENV_LOCAL=1 /Users/robertgrzesik/Development/lumibot/.venv/bin/python -m pytest -m 'not apitest' tests/test_broker_initialization.py tests/test_alpaca.py tests/test_broker_bitunix.py tests/test_ccxt.py tests/test_tradier.py tests/test_projectx.py tests/test_projectx_data.py tests/test_tradovate.py -q
```

Result: 150 passed, 4 skipped, 7 deselected.

Initial runs without clearing the local shell's broker/data-source environment failed because local `DATA_SOURCE`/`TRADING_BROKER` style state polluted tests and caused explicit Alpaca/CCXT strategies to attach `PolymarketData`; Alpaca API tests also attempted real credentials and returned 401. Clean-env reruns passed and are the relevant signal for this PR review.

## Risk Call

Recommendation: cautiously merge only after the normal full LumiBot gate and one BotSpot scheduled paper-deployment smoke. Do not treat this as a trivial safe merge based only on the visible GitHub checks.

Backtesting risk: medium-low for ordinary backtests based on the diff and local checks. The patch does not intentionally change market-data fabrication, fill logic, cash accounting, or order simulation semantics. The representative stock backtest and backtesting env/datetime tests passed. Remaining backtesting risk is coverage breadth: this review did not run the full acceptance backtest suite, Theta/IBKR/options/futures parity, or live-replay baselines.

Deployment risk: medium to medium-high. This is the main blast radius. The PR changes when live brokers authenticate, start streams, start order workers, materialize data sources, and decide pre-open scheduled exits. The intended scheduled path is sensible and covered by unit tests, and the IB queue-worker CodeRabbit concern appears addressed with `REQUIRES_ORDERS_THREAD`. Still, failures could move from constructor time to first real broker/data/order call, and that matters for BotSpot scheduled deployments.

Safe-to-merge conditions:

- Full LumiBot CI/test gate is green on the current PR head or post-merge release branch.
- A clean-env local or CI run includes the focused startup/scheduled tests.
- At least one BotSpot scheduled paper deployment smoke proves startup, market-open/closed behavior, first data access, order submission path, and clean exit with the intended broker.

## Bottom Line

This looks directionally good and probably worth merging for scheduled startup performance, but it is not a low-risk patch. The file count, constructor deferral, broker/data-source lazy materialization, and scheduled `run_once` changes make it a controlled medium-risk merge. Backtesting is less concerning than live/scheduled deployment startup, provided the full backtest gate still passes.
