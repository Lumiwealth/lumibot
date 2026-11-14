# Bar metadata deep copies and backtest startup

Why intraday stock backtests spent most of their time copying frame metadata, what changed in 4.6.62, and what is still open.

**Last Updated:** 2026-11-14
**Status:** Shipped in 4.6.62 (metadata sharing, routed futures fill window, empty indicators page); two items open
**Audience:** Developers + AI Agents

---

## Overview

A profiled 12-month routed stock backtest spent about 70% of its simulation time in `copy.deepcopy`,
reached through pandas `NDFrame.__finalize__`. pandas deep-copies `DataFrame.attrs` whenever a new
object is derived from a frame whose `attrs` is not empty: `frame[col]`, `frame.iloc[...]`,
`frame.loc[...]`, `.copy()`, `.tail()`, `.dropna()`. The routed and direct IBKR stock loaders keep
the corporate-action ledger in `attrs["ibkr_equity_basis"]` (one entry per dividend or split, 127
for a 30-year quarterly payer), so every lookup rebuilt it dozens of times.

## Repro (synthetic, no provider calls)

```bash
PYTHON_DOTENV_DISABLED=1 LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_SPEED_BENCH_MONTHS=12 \
  python3 -m pytest tests/backtest/test_routed_intraday_metadata_speed.py -s
```

The test runs one strategy (5-minute bars, 21-bar average, `get_last_price`, occasional trades)
twice: ledger as a plain nested dict, then shared. It requires identical trades and final value.

| Window | Trades | Before | After |
|---|---|---|---|
| 1 month | 114 | 8.1 s | 2.5 s |
| 12 months | 1,340 | 98.6 s | 29.8 s |

Measured 2026-11-14 on an Apple M-series laptop, Python 3.10, pandas 2.3. Under a wall-clock
profiler the 1-month run had 17.5 million `deepcopy` calls from 40,427 `__finalize__` calls
(25.5 s of 33 s); after the change `__finalize__` totals 0.4 s.

## Fix

`lumibot/tools/frame_metadata.py` adds `share_frame_attrs(frame)`. It replaces nested dicts and
lists in `frame.attrs` with read-only subclasses whose `__deepcopy__` returns the same object.
Derived frames then share one metadata object. Equality, `isinstance(..., dict)`, JSON output and
pickling (which yields ordinary containers) are unchanged. The loaders call it on the merged frame
**before** `Data(...)` is built, because `Data` derives its own working frames in the constructor
and those would otherwise keep a plain copy.

Rule for new loaders: do not leave a plain nested container in `attrs` of a frame that is sliced
per iteration. Share it, or hold it on the wrapper object.

## Routed futures fill window

Separate bug found in the same investigation. A continuous future resolves to a dated contract
for fills. The routed loader requested that contract's minute bars up to the end of the backtest.
History pages backward from the requested end, so a contract that expired before the end of the
window returned an empty page, the series was marked empty for the run, and the order stayed
pending. The routed loader now ends dated-contract requests at the contract's last trading day
(shared helper `_explicit_future_history_end`) and starts a contract first used after a roll one
week before that moment.

Strategy note: `self.get_orders()` returns every tracked order, including filled and canceled
ones. A guard like `if not self.get_orders():` allows exactly one trade per backtest. Use
`self.get_orders(statuses="open")`.

## Still open

1. **Cold-start imports.** On a cold container several seconds pass between the start of
   `Strategy.backtest()` and the first data request. Locally (warm disk) the same span is under
   1 s and is almost all module import: the routed source subclasses the ThetaData pandas source,
   which imports the Polygon client, polars and the market-calendar packages even when no route
   uses them. Removing that needs the routed class split from the ThetaData class; that is a
   refactor, not a bug fix, so it was not done here. Measure on a cold machine first.
2. **Per-request evidence recording.** After the metadata fix, `record_market_input` and
   `record_required_history` (a `dropna` over the returned window per request) are the largest
   remaining per-lookup costs in the synthetic run (about 1 s each of 4 s inside the strategy
   loop for one month). Candidate: record once per distinct window.
3. **Daily gap repair that repeats.** When a provider request for a short daily gap repair times
   out, LumiBot skips the repair, writes nothing and reports the gap in history health. That is
   the honest outcome (no invented bars), but the next backtest pays the same wait. The fix
   belongs to the data service that answers the request; LumiBot already bounds the wait.
