# Greg BTC Routed Quote Fill Investigation

One-line description: Local reproduction of Greg's BTC backtest drop traced to IBKR crypto quote-fill coverage validation.
Last Updated: 2026-06-11
Status: Fix implemented locally with focused regression coverage
Audience: LumiBot, BotSpot Node, and BotSpot support engineers

## Overview

Greg reported a large backtest result drop in a BotSpot email thread with subject `april 17`. The production backtest was:

- Backtest: `64d6495f-bfaf-46e8-b150-0110ed773b4a`
- Strategy: `72953aee-a7d0-45f3-b65c-877f1c3694ad`
- Revision: `9c8e12ce-7ffd-438e-907d-57a13660a1f8`
- AI strategy: `1e3d4506-3ebd-49a8-9d14-496c4e42ca66`
- Period: 2026-03-09 to 2026-06-05
- Production summary: total return about `-8.55%`, CAGR about `-30.99%`, max drawdown about `-64.18%`

The exact saved revision files were exported from the production database through the read-only support path into:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/main.py`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/requirements.txt`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/manifest.json`

No broker deployment secrets or account credentials were copied.

## Reproduction

The copied strategy is `MultiMineralBot`, trading spot `BTC/USDT` in BotSpot while the local backtest harness maps it to BTC/USD quote data. It uses:

- `set_market("24/7")`
- `check_interval: "5M"`
- `timestep: "1h"`
- budget `1300`
- leverage `10`
- routed BotSpot Auto data provider config with crypto routed to IBKR

The local production-like replay used the copied revision and the same 2026-03-09 to 2026-06-05 window. Logs are under:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/local-run-v100-prodlike-remote-dd/subprocess_greg-v100-prodlike-remote-dd.log`

The run was intentionally stopped after it had already reproduced the incorrect early fill path. It did not need to finish the full multi-month backtest to prove the failing invariant.

## Finding

The replay showed IBKR crypto history was underfilled for the early simulation window:

- Available BTC minute data started around 2026-03-22.
- The simulation was evaluating and submitting orders on 2026-03-12.
- LumiBot logged that requested BTC dates were outside the available data range.
- Despite that, market orders could still fill from quote/bid-ask data before the cached data object actually covered the simulated timestamp.

The narrow code path was `BacktestingBroker._fast_get_bid_ask_for_fill()` in `lumibot/backtesting/backtesting_broker.py`.

That helper is an IBKR REST backtesting speed path. It reads bid/ask directly from the cached `Data` object's datalines and bypasses the normal guarded `Data.get_quote()` wrapper. Before the fix, it called `data_obj.get_iter_count(now)` directly. For timestamps before the first cached row, that can produce an invalid positional lookup instead of surfacing missing data.

This is a backtest engine bug, not a Greg strategy logic bug. The strategy submitted an order during a period where the data source did not cover the simulated timestamp; the engine should not fill that order from an out-of-window bid/ask row.

## Fix

`BacktestingBroker._fast_get_bid_ask_for_fill()` now validates that the simulated timestamp is inside the cached data object's actual `datetime_start` and `datetime_end` before reading bid/ask datalines. It also rejects negative `get_iter_count()` results.

If coverage is missing, the helper returns `(None, None)`. Existing missing-data handling can then cancel, skip, or fall back through the honest non-quote path without fabricating a fill.

Regression coverage was added in:

- `tests/test_ibkr_crypto_backtesting_smoke_stubbed.py::test_ibkr_rest_fast_crypto_quote_fill_rejects_underfilled_cache_before_start`

The regression recreates the customer shape:

- backtest starts 2026-03-09;
- first real BTC minute row starts 2026-03-22;
- broker evaluates a March 12 quote-fill attempt;
- expected result is no bid/ask fill.

## Validation

Focused suite:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 1200 python3 -m pytest tests/test_ibkr_crypto_backtesting_smoke_stubbed.py -q
```

Result on 2026-06-11:

- `7 passed`
- one third-party `websockets.legacy` deprecation warning

## Follow-Up

Before telling a customer that the historical result is corrected in production, deploy the LumiBot version containing this fix through the normal version branch and BotManager release path. Then rerun Greg's copied revision for the same 2026-03-09 to 2026-06-05 window and compare:

- whether March 12-21 no longer creates quote fills from uncovered BTC data;
- portfolio value and drawdown around April 17;
- trades and trade events around the reported drop.

Do not add fake trades, synthetic bars, carry-forward bid/ask rows, or strategy-specific patches to make this backtest look better. Missing data must stay missing.
