# Expired options are never settled in Alpaca backtests

Expired option positions stay open forever, and orders on them stay "new", when a backtest runs on `AlpacaBacktesting`.

Last Updated: 2026-11-11

Status: Fixed in 4.6.50 (options settle on every backtesting source; see CHANGELOG). The proposal below is what shipped, with futures left unchanged.

Audience: LumiBot contributors working on backtesting and options.

## Overview

Two long Alpaca option backtests showed the same symptom:

1. A wheel strategy sold one put expiring 2025-10-31 and never traded again for the remaining 12 months.
2. An iron condor still held a long put that expired 2025-11-28 nine months later, with its closing order stuck at `new`.

The question was whether expired options are settled and removed on Alpaca's trade-only data when the contract never prints after the order. They are not, and missing prints are not the reason.

## Root cause

`BacktestingBroker.process_expired_option_contracts` (`lumibot/backtesting/backtesting_broker.py`) starts with:

```python
if self.data_source.SOURCE != "PANDAS":
    return
```

Every expiry step lives behind that line: cancelling open orders on the expired contract, cash-settling or exercising/assigning in-the-money contracts, and removing worthless ones. `PandasData` and the sources built on it (ThetaData, Polygon, DataBento pandas) report `SOURCE = "PANDAS"`, so they settle. `AlpacaBacktesting.SOURCE` is `"ALPACA"` (and `InteractiveBrokersRESTBacktesting` is `"InteractiveBrokersREST"`), so the method returns immediately on every call from the strategy executor and from the daily lifecycle hook.

Effects on an Alpaca backtest, from the expiry date onwards:

- The expired contract stays in `get_positions()` and is valued at its last real print.
- Orders on it are never cancelled. With no further prints they can never fill, so they stay `new`.
- A strategy that waits for "no open option position" before opening the next trade (a wheel, most income strategies) never trades again.
- Portfolio value carries the stale mark instead of the settled cash, so returns are wrong after the first expiry.

The guard dates from 2021 and 2023 (commits 238260856 and c98d86d0f), before Alpaca backtesting supported options.

## Reproduction (offline, no network)

Fake Alpaca clients from `tests/test_alpaca_backtesting_multitimeframe_unit.py`. Buy one SPY 640 call expiring Friday 2026-08-07 that prints only on Monday 2026-08-03. Record option positions at the first bar of each day.

| Run | 2026-08-07 (expiry day) | 2026-08-10 (next session) |
| --- | --- | --- |
| Minute steps, current code | held | **still held** |
| Daily steps, current code | held | **still held** |
| Minute steps, guard bypassed for the test only | held | settled and removed |
| Daily steps, guard bypassed for the test only | held | settled and removed |

With the guard bypassed, the log shows "Automatically selling expired contract for asset SPY 2026-08-07 640 CALL", which is the normal path every pandas-based source takes. So the settlement code itself works on Alpaca data. It never runs. Settlement uses the underlying's price for intrinsic value, not an option print, so trade-only option data does not block it.

The repro script used for the table was a scratch file. A regression test belongs with the fix.

Real data check (Alpaca, SPY, daily steps, 2025-10-01 to 2025-12-31): a simple wheel that waits while it holds a short option sold one 650 put expiring 2025-10-31 and placed no further order for the remaining two months, matching the 12-month cloud run.

## Proposed fix (not applied)

Replace the source-name check with a capability check that every backtesting source passes, for example "the broker is a backtesting broker and the data source can price the underlying". Keep the existing logic after it unchanged.

Tests to add with the fix:

1. Alpaca minute and daily: an out-of-the-money long option is removed after expiry; an in-the-money long is exercised or cash-settled; a short in-the-money put is assigned.
2. An order working on the contract at expiry is cancelled, not left `new`.
3. Pandas-based sources: results unchanged (existing tests).

Expected impact: every Alpaca option backtest that held a contract through expiry changes its results (positions, cash, returns, and any trades that were blocked by the stuck position). That is the reason this needs an explicit decision and a changelog entry.
