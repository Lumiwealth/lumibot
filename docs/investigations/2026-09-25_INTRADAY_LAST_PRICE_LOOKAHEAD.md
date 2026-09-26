# Intraday last price and quotes looked one bar ahead (2026-09-25)

Status: fixed in `be0df267` on `version/4.6.2` (with `16ed3345`, closed bars visible across gaps). Found while
checking the release gate's "overnight last bar" item for 4.6.2.

## What was wrong

Minute and hour **trade** bars are stamped at their **start**. At simulated time T the bar stamped T is still
forming; its close is the price at T plus one bar.

- `ThetaDataBacktestingPandas.get_last_price` (`_resolve_last_trade_close`, `closes.iloc[: iter_count + 1]`, since
  `d0861e4c`, 2025-12) returned the close of the bar stamped T. `RoutedBacktestingPandas` (BotSpot Auto) inherits it.
- `Quote.price` is the bar close, and IBKR and Polygon history carry no quotes, so bid and ask were synthesized from
  that close (`Data.get_quote`, `DataPolars.get_quote`, the Theta pandas quote fast path).
- A market order at T fills at the bar's **open**.

End to end on a routed IBKR backtest with bars whose open is `HHMM` and close `HHMM.99`: at 10:00 the strategy saw a
last price of 1000.99 and a quote of (1000.99, 1000.99), and its market order filled at 1000.00. It could see where the
minute would close and buy at its open.

4.6.1 (`8b6c4523`) exposed this on BotSpot Auto intraday stock backtests: before it, routed stock quotes used daily bars
(stale, but no lookahead); since it, they use minute bars once a minute series is loaded.

## Evidence for the timestamp convention

- IBKR vs cached ThetaData minute OHLC, SPY 2024-01-30: identical bars at the same stamp (09:30 open 490.56 / close
  490.92; 15:59 open 490.96 / close 490.86). Theta trade bars run 04:00 to 19:59 (start stamps).
- Cached ThetaData NBBO minute quotes (SPY, 2026-01-27) run 04:01 to 20:00: they are snapshots at the stamp time.
  Over 300 regular-hours bars the quote mid at T is 0.0055 from IBKR open(T), 0.0055 from close(T-1) and 0.065 from
  close(T). Theta quote-based fills (options) were therefore not affected.

Sources: `prod/cache/v1/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet` and
`.../quote/stock_SPY_minute_quote.parquet` (read-only copies), IBKR via the production downloader (2 small requests).

## Fix

`lumibot/entities/data.py::_intraday_bar_state` classifies the bar at the simulated time as `forming` or `closed`:
closed once `bar_start + length <= T`, where length is the larger of the nominal step (1 minute, 1 hour) and the most
common spacing in the series (so 5-minute bars stored as "minute" and hourly bars after a 09:30 half-hour bar are never
treated as closed early). While forming, the last price and the quote price are the bar's open, and bid/ask move to
the open only when they equal the close (synthesized). Real quote snapshots are unchanged.

## Effect measured (warm cache, offline replay, same data)

| Strategy | Before | After |
| --- | --- | --- |
| SEH Simple, 58 ETFs, 1-minute + daily, Jan 2 to Sep 19 | 166 fills, final 110,223.18, 139 s | identical, 142 s |
| min_mkt, 1-minute market orders | 100 fills, 99,220.14 | identical |
| min_limit_stop | 0 fills | identical |
| day_rotation (daily) | 143 fills, 135,334.55 | identical (row order of same-time events differs: set iteration) |

Strategies change only when they decide from `get_last_price()` or `get_quote()` intraday (limit prices, thresholds).

## Tests

`tests/backtest/test_routed_backtesting_ibkr_prefetch.py -k "never_uses_the_close or price_the_strategy_saw"` and
`tests/test_data_get_bars_day_includes_latest_completed_bar.py -k forming_bar` (pandas and polars). Red on the code
before the fix; green after.

## Open

- `Data.get_quote` / `DataPolars.get_quote` round bid, ask, open and close to 2 decimals, so a sub-cent crypto quote
  becomes 0.0 and is dropped as non-positive (same class as the `Order.avg_fill_price` fix `fa631fb8`).
