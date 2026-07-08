# Live Market Calendar Startup

One-line description: Investigation note for live strategies waiting for market open after the market is already open.

Last Updated: 2026-07-08
Status: Fixed in 4.5.74
Audience: LumiBot maintainers and release operators

## Overview

Live startup could preload a broker market calendar that ended before the current
trading day. When the strategy reached the market-open wait path, the broker's
initialized-calendar fast path treated the stale calendar as an authoritative
closed-market result. The strategy then logged that it was sleeping until market
open even when the market was already open.

This was a framework startup issue, not a strategy `sleeptime` issue. It could
affect normal equity markets such as `NASDAQ` and `NYSE`, and it was especially
confusing for `24/5` strategies because those strategies still need current
calendar rows to distinguish weekday sessions from weekend gaps.

## Fix

- Live startup now initializes market calendars around the current session using
  an explicit bounded window.
- The broker initialized-calendar fast path now returns `None` when the calendar
  does not cover the current date, allowing normal broker/calendar logic to
  answer instead of forcing `False`.
- If the initialized calendar spans the current date but no session is open
  because of premarket, after-hours, weekends, holidays, or maintenance gaps, the
  fast path returns `False`.
- Base broker continuous-market detection now samples a multi-day window so
  `24/5` and `us_futures` weekend gaps are not misclassified as `24/7`.

## Regression Coverage

The 4.5.74 regression coverage includes:

- stale calendars before and after the current date,
- premarket and after-hours closed sessions,
- overnight sessions,
- extended trading minutes,
- weekend gaps inside a loaded calendar window,
- generated calendar rows for `NASDAQ`, `NYSE`, `24/5`, `us_futures`, and
  `24/7`,
- U.S. futures Saturday-night closed behavior versus Monday-night open behavior,
- equity market holidays such as observed Independence Day,
- `24/7` behavior on weekends and holidays,
- an Alpaca-shaped no-order broker path where a stale initialized calendar falls
  through to normal market-hours logic.

A read-only Alpaca paper broker smoke also verified that a real broker can
authenticate, read balances, read positions, read orders, and use the fixed
initialized-calendar path without submitting orders.

The focused local verification command used for this fix was:

```bash
.venv/bin/python -m pytest \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_is_market_open_uses_initialized_calendar \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_returns_none_when_current_date_not_covered \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_returns_none_when_current_date_before_calendar_window \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_returns_false_for_covered_closed_session \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_returns_false_for_weekend_inside_calendar_window \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_handles_overnight_sessions \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_initialized_calendar_applies_extended_trading_minutes \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_is_market_open_falls_back_when_initialized_calendar_is_stale \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_is_market_open_does_not_fall_back_for_weekend_inside_calendar_window \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_base_broker_continuous_market_detection_respects_weekend_gaps \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_real_market_calendars_cover_open_and_other_times \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_us_futures_real_calendar_weekend_closed_and_monday_night_open \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_24_5_real_calendar_weekend_closed_and_weeknight_open \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_equity_real_calendar_market_hours_weekend_and_holiday \
  tests/test_broker_initialization.py::TestBrokerInitializationSimple::test_24_7_real_calendar_ignores_weekends_and_holidays \
  tests/test_alpaca.py::TestAlpacaBroker::test_market_open_falls_back_when_initialized_calendar_is_stale \
  tests/test_market_type_detection.py \
  tests/test_scheduled_run_once.py \
  -q
```
