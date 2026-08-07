# AI Agent Wait Left Backtest Orders Unfilled

One-line description: Agent `orders_wait_for_terminal` raced the sim clock without processing fills.
Last Updated: 2026-08-06
Status: Fixed on version/4.5.84
Audience: AI agents and contributors
Overview: Root-cause analysis for ORB/VWAP/credit-spread flat tearsheets.

## Overview

AI-only ThetaData backtests submitted real orders that stayed `new` until
end-of-run cancel. Tearsheets reported flat/degenerate return series (zero trades).

## Evidence

| Strategy | Submit | Wait result |
| --- | --- | --- |
| ORB | SPY market buy 34 @ 2026-06-24 10:00 | 513052 polls / 10s wall, sim clock to 2026-06-30, timed out unfilled |
| VWAP | SPY market buy 136 @ 2026-06-24 13:30 | ~508k polls, same failure mode |
| Credit spread | multileg credit mid @ 2026-06-02 09:30 | wait on parent, ~2M polls / 30s, unfilled |
| Iron condor | same multileg tool | fills when agent did **not** call wait after submit; waits that did run also raced the clock |

Artifacts under `logs/AIOpeningRangeBreakoutStrategy_2026-08-06_18-05_malxUc*`,
`logs/AIVWAPStrategy_2026-08-06_18-05_Qf6KFt*`,
`logs/AICreditSpreadStrategy_2026-08-06_17-53_myR5iH*`.

## Root cause

1. `orders_wait_for_terminal` called `strategy.sleep(poll, process_pending_orders=True)`.
2. Committed `Strategy.sleep` ignored `process_pending_orders` in backtesting and
   delegated to `broker.sleep`, which StrategyExecutor replaces with `safe_sleep`.
3. `safe_sleep` advances `_update_datetime` and drains the event queue, but does
   **not** call `process_pending_orders`.
4. Each 1s poll therefore advanced simulated time without evaluating fills. Hundreds
   of thousands of polls raced past the backtest window while market orders stayed
   `new`, then were canceled at cleanup.

This was not a ThetaData quote/OHLC fill-model bug. Direct
`process_pending_orders` with minute OHLC present fills stock market orders.

Secondary issues addressed in the same change set:

- Multileg credit/debit parents dropped `kwargs["price"]`, so parent `limit_price`
  was null in trade logs.
- ORB prompt allowed claiming breakout when close was not strictly above OR high.

## Fix

- `Strategy.sleep` in backtesting: `process_pending_orders` before and after
  `_update_datetime` when requested.
- `orders_wait_for_terminal`: process pending once up front; bound backtest polls
  (max 120) so waits cannot consume the remainder of the sim.
- Multileg `_submit_orders`: persist net `price` / `limit_price` on the parent.

## Regression tests

```bash
~/bin/safe-timeout 120s python3 -m pytest \
  tests/backtest/test_backtesting_broker_processing.py::test_strategy_sleep_fills_stock_market_order_when_ohlc_exists \
  tests/backtest/test_backtesting_broker_processing.py::test_orders_wait_for_terminal_fills_stock_market_order_in_backtest \
  -v
```
