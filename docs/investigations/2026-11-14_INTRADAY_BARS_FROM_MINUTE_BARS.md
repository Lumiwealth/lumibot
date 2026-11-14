# Intraday Bars From Cached Minute Bars (Measurement And What Shipped)

Compares Alpaca's own 5-minute bars with 5-minute bars built from cached 1-minute bars.

- Last Updated: 2026-11-14
- Status: Shipped for Alpaca stock backtests, on by default (see "What shipped"). Futures unchanged.
- Audience: contributors working on backtest speed.

## Overview

5-minute, 10-minute, 15-minute, 30-minute and hourly bars are downloaded as their own
series. Building them from the 1-minute series a backtest already holds would save those
downloads, but could change results. This note measures the difference first.

## Measurement

Local AlpacaBacktesting cache files only (no provider calls). Provider 5-minute file versus
the same window's 1-minute file resampled (`open` first, `high` max, `low` min, `close`
last, `volume` sum, bars labeled by start time).

| Symbol | Window | Provider 5-min bars | In regular session | OHLC identical | Volume identical | Provider bars outside regular session |
|---|---|---|---|---|---|---|
| TSLA | 2026-01-05 to 01-10 | 960 | 390 | 100% | 100% | 570 (59%) |
| XOM | 2026-01-05 to 01-10 | 786 | 390 | 100% | 100% | 396 (50%) |
| UBER | 2026-01-05 to 01-10 | 760 | 390 | 100% | 100% | 370 (49%) |
| SPY (adjusted) | 2026-08-03 to 08-08 | 959 | 390 | 100% | 100% | 569 (59%) |

- Regular session (09:30 to 15:55 New York): every bar equal in all five fields, 1,560 of 1,560.
- The only difference: the provider series also has pre-market and after-hours bars. The
  cached 1-minute series holds regular-session minutes only, so it cannot rebuild them.
- Not measured: 15-minute bars (no provider 15-minute files in the local cache), stressed
  or half days, symbols with trading halts, crypto.
- Expected from bar alignment, not measured: 10, 15 and 30-minute bars line up with 09:30.
  Hourly bars do not: the provider's 09:00 bar includes pre-market trades, a rebuilt one
  starts at 09:30.

## Second measurement (5, 10, 15 and 30-minute, stressed week, half day, futures)

Stocks: Alpaca provider bars fetched once for TSLA, XOM, UBER and SPY over three weeks:
2026-01-05 to 01-09 (normal), 2025-11-24 to 11-28 (holiday and a half day) and 2025-04-07 to
04-11 (a stressed week). Regular-session bars only (NYSE calendar, half day ends 13:00).

| Bar size | Provider regular-session bars | OHLC identical | Volume identical | Only in provider | Only in rebuilt |
|---|---|---|---|---|---|
| 5-minute | 4,224 | 4,224 | 4,224 | 0 | 0 |
| 10-minute | 2,112 | 2,112 | 2,112 | 0 | 0 |
| 15-minute | 1,408 | 1,408 | 1,408 | 0 | 0 |
| 30-minute | 704 | 704 | 704 | 0 | 0 |

Futures: local Interactive Brokers cache files only (no provider calls), single contracts,
24-hour session, whole days where both files have data.

| Contract | Bar size | Provider bars | Identical (OHLC and volume) | Only in one series |
|---|---|---|---|---|
| ES Dec 2026 | 5-minute | 6,072 | 6,072 | 0 |
| MES Dec 2026 | 5-minute | 5,796 | 5,796 | 0 |
| MNQ Dec 2026 | 15-minute | 2,024 | 2,024 | 0 |
| MGC Dec 2026 | 15-minute | 1,932 | 1,932 | 0 |

Single futures contracts are identical, including the bars at the daily session break.
Futures still use the provider series, for two reasons that were not measured: a continuous
future changes contract at a minute that is not on a 5-minute edge (a rebuilt bar at each
roll would mix two contracts), and those files carry bid, ask and gap columns besides OHLCV.
Not measured: 10 and 30-minute futures bars, crypto, a stock trading halt.

## What shipped

`AlpacaBacktesting` builds 5, 10, 15 and 30-minute **stock** bars from the 1-minute series
when all of these hold, and otherwise downloads the provider series exactly as before:

1. The 1-minute series for the window is already loaded or in the cache, and the provider
   series for that bar size is not (once the provider series exists it is always used).
2. Every requested bar is inside today's regular session (09:30 to 16:00 New York) and the
   newest one is the bar right before now. A request that reaches into pre-market, after
   hours or an earlier day gets the provider series, because that series has bars the
   1-minute cache does not hold.
3. A period with no trades has no bar (like the provider), never a filled one.

So an answer from minutes is the same bars the provider series would give: results do not
move. On by default. Off: `intraday_bars_from_minutes=False` in the config, or
`BACKTESTING_INTRADAY_BARS_FROM_MINUTES=false`. Each answer is counted in the telemetry
cache layer `alpaca_bars_from_minutes`.

Tests: `tests/test_alpaca_backtesting_multitimeframe_unit.py` (made-up bars shaped like provider bars, a full
day and a half day, every session minute, 5 and 15-minute; fallback cases; no-trade periods).

What it does not speed up: a strategy that asks for more bars than today's session holds
(for example 20 five-minute bars at 10:00) still downloads the provider series once.

## Original proposal (superseded)


1. Add a data source option `intraday_bars_from_minutes` (environment variable
   `BACKTESTING_INTRADAY_BARS_FROM_MINUTES`), default off, so existing results do not move.
2. When on, 5, 10, 15 and 30-minute stock bars are built from the cached 1-minute series
   when that series is already loaded; otherwise the provider series is used as today.
3. Hourly bars and any request that includes extended hours keep the provider series.
4. Results say which bars were rebuilt (count by source), like other data provenance.
5. Before the default changes: measure 15-minute provider bars, a half day and a halt day
   on recorded data, and get the project owner's approval, because indicators that read
   pre-market bars (for example a 5-minute moving average at 09:35) will change.
