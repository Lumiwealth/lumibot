# Risk-free rate can read a future date in backtests

`YahooHelper.get_risk_free_rate(dt=...)` picks the ^IRX row nearest to the simulated time in either direction, so a backtest can use a rate published after the simulated time.

Last Updated: 2026-11-11

Status: Evidence only. Behaviour NOT changed. Needs a decision before any fix.

Audience: LumiBot contributors working on backtesting accuracy and option greeks.

## Overview

`get_risk_free_rate` feeds `Strategy.risk_free_rate`, which `get_greeks`, implied volatility, delta strike selection and the tearsheet use. In a backtest it is called with the simulated datetime:

```python
irx_df = YahooHelper.get_symbol_data("^IRX", last_needed_datetime=dt)
delta = abs(irx_df.index - dt)
closest_date_index = delta.argmin()
irx_price = irx_df.iloc[closest_date_index]["Close"]
```

`abs(...)` treats a row after `dt` the same as a row before it. The loaded ^IRX table runs to today, not to the simulated time, so rows after `dt` exist and can win.

## Evidence (real ^IRX data from Yahoo, run 2026-11-11)

Daily ^IRX rows are stamped 01:00 America/New_York. Rate in percent (`Close`).

| Simulated time (New York) | Row picked today | Its close | Latest row at or before the simulated time | Its close |
| --- | --- | --- | --- | --- |
| Mon 2026-10-19 09:45 | 2026-10-19 | 4.075 | 2026-10-19 | 4.075 |
| Mon 2026-10-19 14:00 | **Tue 2026-10-20** | 4.080 | 2026-10-19 | 4.075 |
| Wed 2026-10-21 15:59 | **Thu 2026-10-22** | 4.022 | 2026-10-21 | 4.063 |
| Sun 2026-10-25 10:00 | **Mon 2026-10-26** | 4.025 | Fri 2026-10-23 | 4.032 |
| Fri 2026-10-23 20:00 | 2026-10-23 | 4.032 | 2026-10-23 | 4.032 |

So:

1. From about 13:00 New York time each day, the next trading day's closing rate is used. Every afternoon of every intraday backtest reads one day ahead.
2. On weekends and holidays the next session's rate is used once it is nearer.
3. Even the "at or before" row is not clean during the session: its `Close` is that day's closing rate, which is only known after the close. Before the close, only the previous session's close was actually available.

## Size of the effect

Small in rate terms (a few basis points day to day here), and greeks are not very sensitive to the rate for short-dated options. It is still look-ahead: a value from the future enters the simulation, and over volatile rate periods (for example 2022 to 2023) day-to-day moves were larger. It also makes results depend on when the backtest is run, because the loaded table grows over time.

## Proposed fix (not applied)

Use the latest ^IRX close that was published at or before the simulated time:

1. Filter the table to rows whose session has closed by `dt`: a row for date D counts only when `dt` is at or after D's close (16:00 New York; simplest rule: use rows dated strictly before `dt`'s New York date, plus `dt`'s own date only when `dt` is at or after 16:00).
2. Take the last such row. If none exists, return `None` (the caller already falls back to 0.0 and logs nothing extra); never use a later row.
3. Keep the live path (`dt is None`) unchanged.
4. Tests: the table above as fixed fixtures (Monday 14:00 must give the previous session close, never Monday or Tuesday; Monday 20:00 gives Monday; Sunday gives Friday).

Expected impact: greeks, implied volatility and delta strike choices move by a tiny amount in backtests that use the default rate. Strategies that set `risk_free_rate` explicitly are unaffected. Because results change, this needs an explicit decision and a changelog entry when shipped.
