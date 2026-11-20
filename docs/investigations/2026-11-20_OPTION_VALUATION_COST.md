# Option Valuation Cost in Minute Option Backtests

Where a one-year minute iron condor backtest spends its time after the same-moment option valuation fix.

Last Updated: 2026-11-20
Status: Open (measured; partial fix in 4.6.93)
Audience: LumiBot contributors working on backtest speed

## Overview

A one-year SPY iron condor on Alpaca minute data (2025-10-01 to 2026-09-30, about 97,500
iterations, 2,055 trades) gave identical results on two warm hosted runs six days apart, but
took about twice as long on the later one:

| Run date | LumiBot | Total s | Simulation s | Iteration total s | Simulation outside iterations s |
|---|---|---|---|---|---|
| 2026-11-14 | before 4.6.8x valuation fix | 278 | 238 | 182 | 56 |
| 2026-11-20 | 4.6.91 | 633 | 581 | 266 | 316 |

The extra time sits outside `on_trading_iteration`: portfolio valuation. Since the
2026-11-17 fix "value every option leg of a backtest account for the same moment"
(`BacktestingBroker.option_marks_for_valuation`), every held leg without a real price on the
current minute is estimated (implied volatility from its last trade, then Black-Scholes at the
current underlying price). That fix made account values correct; this note is about its cost.

## Profile (3 months of the same strategy, profiled)

| Function | Calls | Profiled s (total) |
|---|---|---|
| Main loop (`StrategyExecutor._run_backtesting_loop`) | 64 | 321 |
| `_strategy._update_portfolio_value` | 49,457 | 145 |
| `BacktestingBroker.option_marks_for_valuation` | 19,110 | 117 |
| `_valuation_bars` (frame path, mostly underlying for implied volatility) | 7,836 | 28 |
| `implied_volatility_forward` | 7,836 | 25 |
| `_valuation_arrays` | 76,440 | 23 |
| `_bar_matches_now` | 76,916 | 18 |
| `OptionsHelper.find_strike_for_delta` (strike search, waits on bar loads) | 108 | 94 |
| Shared bar cache partition loads (pool threads) | 2,761 | 418 |

## What changed in 4.6.93

- The "newest trade is in the current minute" check on the array path compares integer
  minutes instead of building a timestamp per leg per bar (about 15 of the 18 profiled seconds).

## Next candidates

- The implied-volatility reference spot reads an underlying stock frame (`_valuation_bars`),
  because `recent_bar_arrays` answers options only. A stock array answer would remove most of
  that 28 s.
- Strike search waits on shared cache partition loads; prefetching the expiration's
  partitions earlier would overlap them with the loop.

Results must stay identical: `tests/backtest/test_option_valuation_array_marks_identical_results.py`.
