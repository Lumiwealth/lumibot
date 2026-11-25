# Daily Backtest Engine Overhead

Where simulation time goes on daily-bar backtests outside the strategy's own code, and what was cut.

- Last Updated: 2026-11-24
- Status: fixes on `wip/claude-s5/daily` (off `version/4.6.96`), not released
- Audience: contributors working on backtest speed

## Overview

On hosted daily backtests most of the simulation phase was engine work, not strategy code
(for example 27.6 of 34.7 simulation seconds on a 16-year buy-and-hold of three stocks, 39.6 of
48.2 seconds on a six-year monthly rebalance of 30 stocks). A local profile (yappi, wall clock)
of generic daily strategies on synthetic daily bars, with no network, shows why:

- Every lifecycle step (before market opens, before starting trading, after the trading
  iteration, before market closes, after market closes) records a stats row, so the portfolio
  is valued about five times per simulated day. Each valuation prices every held asset.
  Those rows are part of the output, so the valuations themselves are needed.
- Each of those price reads did work proportional to the length of the daily series:
  - Alpaca: `_newest_bar_position` converted the whole index to Python dates on every
    daily lookup; the pre-open mark read the previous close through a full
    `get_historical_prices` call that built a `Bars` object.
  - Routed/ThetaData pandas path: `_frame_has_ohlc_columns` ran `to_numeric`/`notna` on each
    OHLC column; the stale-daily split adjustment ran once per price field (six) and each run
    re-validated the frame's cached split facts by boxing timestamps; the per-asset split
    cache was built with `iterrows`.

## What changed (identical results)

| Change | File |
|---|---|
| Index dates computed once per index object (numpy days) | `lumibot/backtesting/alpaca_backtesting.py` |
| Pre-open previous close read in place for native daily series; one-row slices removed | `lumibot/backtesting/alpaca_backtesting.py` |
| Numeric OHLC columns answer the "has OHLC" check from their last value | `lumibot/backtesting/thetadata_backtesting_pandas.py` |
| Stale-daily split checks once per snapshot, same ratio applied to each field | `lumibot/data_sources/data_source.py` |
| Split cache built from the split column, not `iterrows` | `lumibot/data_sources/data_source.py` |
| Split facts re-read of the same frame/index/columns objects skips the shape check | `lumibot/data_sources/data_source.py` |
| Required-history fast path uses numpy NaN checks for float columns | `lumibot/tools/ibkr_history_health.py` |

Each change has an equivalence test against the original logic and a counting test that fails
if the per-read work returns (`tests/test_backtest_per_iteration_costs.py`,
`tests/test_required_history_fast_path.py`).

## Measured (local Mac, synthetic daily bars, trades and stats rows hashed)

| Run | Before s | After s | Results |
|---|---|---|---|
| Alpaca, 30 symbols monthly rebalance holding 5, 6 years (cached) | 15.7 | 5.1 | identical |
| Alpaca, 1 symbol 200-day trend, 10 years | 10.7 | 5.8 | identical |
| Routed IBKR, buy-and-hold 3 symbols, 16 years | 15.0 | 8.8 | identical |
| Routed IBKR, 30 symbols monthly rebalance, 5 years | 12.8 | 7.6 | identical |
| Routed IBKR, 1 symbol 200-day trend, 10 years | 9.4 | 8.1 | identical |

The Mac was shared with other jobs (1-minute load 20 to 90); compare rows, not absolutes.

## Not changed / next

- Market input evidence (`MarketInputEvidence.record`, about 1 ms per strategy history read,
  one sqlite transaction each) is the largest remaining per-iteration cost on single-symbol
  daily strategies.
- Shared-bars S3 reads already run in parallel per series; the summed `readSeconds` in
  run telemetry is not wall time. Symbols first used mid-run load one series at a time.
- `get_risk_free_rate` fetches from Yahoo during end-of-run stats (several seconds when the
  network is slow); that is report time, not simulation.
