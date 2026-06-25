# NumPy 2.5 Market Calendar Warnings

One-line description: Reproduction and fix for repeated pandas-market-calendars deprecation warnings under NumPy 2.5.

Last Updated: 2026-06-25

Status: Fixed in 4.5.62 unreleased branch

Audience: LumiBot maintainers and support

## Overview

A user reported repeated warning spam after upgrading LumiBot. The screenshots
showed Python warning output from `pandas_market_calendars/market_calendar.py`
while checking bot status:

- Message: `The 'generic' unit for NumPy timedelta is deprecated, and will raise an error in the future. This includes implicit conversion of bare integers (e.g. '+ 1'). Please use a specific unit instead.`
- Source frame: `pandas_market_calendars/market_calendar.py`, inside the helper
  that returns `pd.Timedelta(days=day_offset, hours=t.hour, minutes=t.minute, seconds=t.second)`.

## Reproduction

The warning reproduces in a clean Python 3.12 environment with the current
released dependency resolution for `lumibot==4.5.61`:

```text
lumibot==4.5.61
numpy==2.5.0
pandas==2.3.3
pandas-market-calendars==5.4.0
exchange-calendars==4.13.2
```

Repro command used:

```bash
python3.12 -m venv tmp/lumibot-4561-full
tmp/lumibot-4561-full/bin/python -m pip install 'lumibot==4.5.61'
tmp/lumibot-4561-full/bin/python - <<'PY'
import warnings
import pandas_market_calendars as mcal

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    mcal.get_calendar("NYSE").schedule("2026-06-22", "2026-06-24", market_times="all")
    print(len(caught))
    for warning in caught[:4]:
        print(type(warning.message).__name__, warning.filename, warning.lineno, warning.message)
PY
```

Observed result: `41` repeated `DeprecationWarning` records from
`pandas_market_calendars/market_calendar.py`.

The same calendar call is quiet with `numpy==2.5.0`, `pandas==3.0.3`, and
`pandas-market-calendars==5.4.0`. The same calendar call is also quiet with
`numpy==2.4.4`, `pandas==3.0.2`, and `pandas-market-calendars==5.3.2`.

## Root Cause

NumPy 2.5.0 exposes a new deprecation for generic, unitless
`numpy.timedelta64` construction. The current released LumiBot dependency graph
can still resolve to pandas 2.3.x, and that pandas/calendar combination triggers
NumPy's warning repeatedly while pandas-market-calendars builds market schedule
timedeltas.

The warning is not emitted by strategy code, so user-side warning filters may
not help if the status/live-bot process initializes the calendar before user code
runs or if warnings are forced to always display.

## Fix

Cap NumPy below 2.5 in LumiBot runtime dependencies:

```text
numpy>=1.20.0,<2.5.0
```

This is safer than forcing `pandas>=3` because the current released LumiBot
dependency graph still resolves to pandas 2.3.x. Once pandas 2.x or the calendar
dependency is quiet under NumPy 2.5, the NumPy cap can be revisited.

## Verification

Focused verification added:

```bash
python -m pytest tests/test_dependency_bounds.py
```

Manual reproduction verified the warning exists before the cap in a clean
released-package install and that the warning source matches the user screenshot.
