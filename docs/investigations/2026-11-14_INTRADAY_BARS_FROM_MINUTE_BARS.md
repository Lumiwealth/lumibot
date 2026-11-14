# Intraday Bars From Cached Minute Bars (Measurement And Proposal)

Compares Alpaca's own 5-minute bars with 5-minute bars built from cached 1-minute bars.

- Last Updated: 2026-11-14
- Status: Proposal. Nothing changed in code.
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

## Proposal

1. Add a data source option `intraday_bars_from_minutes` (environment variable
   `BACKTESTING_INTRADAY_BARS_FROM_MINUTES`), default off, so existing results do not move.
2. When on, 5, 10, 15 and 30-minute stock bars are built from the cached 1-minute series
   when that series is already loaded; otherwise the provider series is used as today.
3. Hourly bars and any request that includes extended hours keep the provider series.
4. Results say which bars were rebuilt (count by source), like other data provenance.
5. Before the default changes: measure 15-minute provider bars, a half day and a halt day
   on recorded data, and get the project owner's approval, because indicators that read
   pre-market bars (for example a 5-minute moving average at 09:35) will change.
