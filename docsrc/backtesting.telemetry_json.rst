Telemetry JSON
==============

.. meta::
   :description: Every LumiBot backtest writes a small telemetry JSON file that shows where the time went and how many provider requests and cache reads it made.

Every backtest writes a small ``*_telemetry.json`` file next to the other output files (for example ``logs/MyStrategy_2026-11-11_14-00_AbC123_telemetry.json``). It answers "why did this backtest take so long?" without a profiler. It is always written and costs well under a tenth of a second, even on a year of minute data.

It contains only counts, seconds and sizes. It never contains your keys, account details, symbols or server addresses.

What it shows
-------------

- ``wall_seconds``: the whole run.
- ``phases``: ``data_setup_seconds`` (until the first trading iteration), ``simulation_seconds``, ``reports_seconds`` (tearsheet, trades and indicator files) and ``uploads_seconds`` (waiting for cache uploads at the end).
- ``iterations``: how many trading iterations ran, the time spent inside ``on_trading_iteration``, and the slowest iteration with its simulated time.
- ``providers``: one row per data source and asset class, with ``requests``, ``provider_seconds`` (time waiting for answers), ``rows``, ``retries``, and rate-limit or backoff waits (count and seconds).
- ``caches``: one row per cache layer, data source and asset class, with ``reads``, ``hits``, ``misses``, bytes read, and uploads.

Example (a 12-month options backtest on a repeat run, shortened):

.. code-block:: json

   {
    "schema": "lumibot.backtest_telemetry/1",
    "wall_seconds": 38.0,
    "phases": {"data_setup_seconds": 0.6, "simulation_seconds": 33.9, "reports_seconds": 3.5, "uploads_seconds": 0.0},
    "iterations": {"count": 97140, "total_seconds": 22.1, "slowest_seconds": 2.4, "slowest_at": "2025-10-06T10:00:00-04:00"},
    "providers": [{"source": "alpaca", "asset_class": "option", "requests": 48, "provider_seconds": 5.6, "rows": 0}],
    "caches": [{"layer": "shared_bars", "source": "alpaca", "asset_class": "option", "reads": 2984, "hits": 2561, "misses": 423}]
   }

Finding a slow backtest
-----------------------

1. Compare the phases with ``wall_seconds`` to see which part is slow.
2. If ``provider_seconds`` is most of ``simulation_seconds``, the backtest is waiting on market data. A second run of the same backtest should make far fewer requests, because data is cached.
3. If the provider and cache do not explain the time, profile your strategy with ``BACKTESTING_PROFILE=yappi``. This writes a ``*_profile_yappi.csv`` file you can sort by total time.

Field names are stable: new fields may be added, existing ones are never renamed.
