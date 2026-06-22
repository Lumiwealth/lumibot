# Crypto Backtest Validation

One-line description: Controlled validation plan and artifact runner for CCXT/Coinbase crypto backtesting.

Last Updated: 2026-06-20

Status: Active validation workflow

Audience: LumiBot and BotSpot engineers validating crypto data-source, cache, and fill semantics.

## Overview

Crypto backtests need separate validation from customer strategies. A customer strategy can be noisy, sparse, or intentionally flat, so it is a poor first proof that the data source, cache, order fills, and tear sheet are correct.

The validation workflow is:

1. Run deterministic unit tests for cache and fill invariants.
2. Run controlled live-data acceptance strategies against the chosen public CCXT exchange.
3. Compare every fill to the audited execution model at the same timestamp: quote bid/ask when quote data exists, or OHLC bar open for CCXT's OHLC fill path.
4. Then replay the customer strategy unchanged and compare old versus new artifacts.

## Invariants

- Preserve exact quote symbols. `BTC/USDT` means `BTC/USDT`, not `BTC/USD`.
- Cache coverage metadata is not executable data. Empty/no-tick windows may be recorded, but they must not create synthetic bars.
- Missing rows are allowed only as missing rows. They must not be forward-filled into executable candles.
- A fill can wait until a real executable bar when the order time-in-force permits it.
- The fill must be recorded at the executable bar timestamp, never at the older order timestamp with a future price.
- Crypto `DAY` orders expire on the UTC date boundary.
- `IOC` and `FOK` orders cancel immediately when the current executable bar is unavailable.
- Crypto `get_last_price()` and `get_quote()` must not be silently downshifted to daily data because a routed backtest inferred day mode.

## Unit Tests

Run the focused deterministic suite:

```bash
/Users/robertgrzesik/bin/safe-timeout 1200s python3 -m pytest \
  tests/test_ccxt_store.py \
  tests/test_backtesting_ccxt_execution_semantics.py \
  tests/test_hour_timestep_support.py \
  tests/test_routed_backtesting_unit.py \
  tests/test_routed_backtesting_routing_validation.py \
  tests/test_crypto_backtest_validation_runner.py
```

These tests cover:

- `1m`, `1h`, and `1d` CCXT cache cold/warm reads.
- partial overlap cache fetches.
- empty provider responses.
- corrupt/duplicate/legacy rows.
- quote preservation.
- DAY/GTC/GTD/IOC/FOK behavior.
- market, limit, and stop orders waiting for real bars.
- crypto routed price/quote lookups staying on minute data even when day mode is inferred.

## Live-Data Acceptance Runner

Use the controlled runner for public Coinbase/CCXT validation:

```bash
/Users/robertgrzesik/bin/safe-timeout 1200s python3 scripts/run_crypto_backtest_validation.py \
  --exchange coinbase \
  --symbol BTC/USDT \
  --start 2026-03-15T00:00:00+00:00 \
  --end 2026-03-17T00:00:00+00:00 \
  --sleeptime 1H \
  --warm-repeat
```

The runner writes durable artifacts under:

```text
/Users/robertgrzesik/Development/support-artifacts/crypto-backtest-validation-<timestamp>/
```

Each run includes:

- `manifest.json` with git branch, SHA, dirty status, command, cache root, and case summaries.
- one folder per controlled strategy.
- full LumiBot logs and trade-event CSVs.
- `cache_price_checks.csv` verifying the cached provider candle exists at the fill timestamp, the audited bar timestamp matches the fill timestamp, and the fill price matches the correct execution price for that fill model.
- per-case `summary.json` with fill timestamp gap, cache row presence, execution price match status, stale submit-quote diagnostics, and wall time.

The controlled strategies are:

- `buy_hold`: one entry, then mark-to-market through the end of the window.
- `round_trip`: buy and sell at fixed timestamps.
- `alternating`: repeated predictable entries/exits.

### 2026-06-20 Controlled Coinbase Proof

The no-dotenv validation runs below were executed with:

- `BACKTESTING_DATA_SOURCE=ccxt`
- `LUMIBOT_DISABLE_DOTENV=1`
- `LUMIBOT_DISABLE_DOTENV_LOCAL=1`
- isolated artifact-local cache folders.

BTC/USD:

- Artifact root: `/Users/robertgrzesik/Development/support-artifacts/crypto-backtest-validation-20260620T-usd-strict-nodotenv`
- Cases: `buy_hold`, `round_trip`, `alternating`, and warm-cache repeats.
- Result: all six cases had `all_cache_rows_exist=true`,
  `all_fill_prices_match_expected_execution=true`,
  `all_audit_bar_times_match_fill_times=true`, and
  `all_audit_bars_match_requested_symbol_cache=true`.
- Cold `buy_hold` wall time: about `15.66s`; warm repeats: about
  `2.28s` to `2.47s`.

BTC/USDT:

- Artifact root: `/Users/robertgrzesik/Development/support-artifacts/crypto-backtest-validation-20260620T-usdt-strict-nodotenv`
- Cases: `buy_hold`, `round_trip`, `alternating`, and warm-cache repeats.
- Result: all six cases had `all_cache_rows_exist=true`,
  `all_fill_prices_match_expected_execution=true`,
  `all_audit_bar_times_match_fill_times=true`, and
  `all_audit_bars_match_requested_symbol_cache=true`.
- Cold `buy_hold` wall time: about `13.18s`; warm repeats: about
  `2.23s` to `2.38s`.

The runner has a permanent regression test in
`tests/test_crypto_backtest_validation_runner.py` to prevent a BTC/USD run from
silently using the strategy-class default `USDT` quote. LumiBot stores
`run_backtest(..., parameters=...)` on `self.parameters`, so validation
strategies must read from `self.parameters`, not only from `initialize()`
default arguments.

## Greg Replay

Greg's strategy must be replayed unchanged. Do not edit customer code to make the replay pass.

Use the controlled matrix first. Then run Greg's exact saved artifact and compare:

- old production fill count versus new fill count.
- old production fill timestamps versus new fill timestamps.
- fill price versus same-timestamp provider candle.
- repeated stale prices.
- largest equity drops.
- time in market.
- final return.

The old IBKR artifact had suspicious fill behavior: many fills were outside the same-hour BTC candle and repeated stale-looking prices. A new flatter Coinbase chart is not automatically wrong; it must be judged against the controlled matrix and fill-vs-cache checks.

### 2026-06-20 Greg Coinbase Replay Check

The unchanged executed production code zip was replayed locally for the
`2026-03-15` to `2026-04-20` window with crypto routed to Coinbase:

- Replay artifact root: `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2`
- Executed code SHA-256:
  `91473f8fa813cd5fa818bdad6201053b936c999f3a2f725586c25d9e567e8b40`
- LumiBot version: `4.5.52`
- Strategy parameters still contain `symbol=BTC/USDT`, but the captured
  production entrypoint passes `quote_asset=USD`. Do not rewrite Greg's code for
  replay; validate the artifact exactly as executed.

Additional analysis artifacts were written under:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/analysis/fill_cache_summary.json`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/analysis/fill_cache_checks.csv`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/analysis/portfolio_shape_summary.json`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/analysis/old_vs_new_trade_count_summary.json`

Replay result:

- 10 fill rows and 10 matching new-order rows.
- Every fill used `audit.timestep=minute`.
- Every fill's event timestamp matched `audit.bar.datetime` exactly.
- Every fill price matched the audited execution price.
- Every audited fill bar matched the requested `BTC/USD` Coinbase cache row.
- No fill reused the old stale `70511.75` price.

The flat tear sheet is expected from this replay's trade shape:

- Stats rows: `10370`.
- Active position rows: `804`, about `7.75%` of the 5-minute stats rows.
- QuantStats reported `16%` time in market on its daily summary metric.
- The strategy had five position-holding windows, all ending by
  `2026-03-27 00:00:00-04:00`; after that, portfolio value stayed in cash
  through the end of the `2026-04-20` replay.

Comparison against the old production artifact over the same
`2026-03-15` to `2026-04-20` window:

- Old production: 49 fills.
- New Coinbase replay: 10 fills.
- Old production reused the fill price `70511.75` 34 times in that window.
- New Coinbase replay had 10 unique fill prices across 10 fills.

That means the old chart can look more active because the old stale-fill path
created many extra fills. The new flatter chart is not by itself a failure; the
fill/cache evidence shows the replay is no longer filling at a price from the
wrong timestamp.

## Cache Safety

Do not delete shared caches. The acceptance runner sets `LUMIBOT_CACHE_FOLDER` to an artifact-local cache directory before importing LumiBot, so cold/warm tests are isolated and reproducible.

For production-like S3 cache experiments, use an isolated S3 version prefix. Do not mutate or delete shared production cache objects unless Rob explicitly requests the exact operation.
