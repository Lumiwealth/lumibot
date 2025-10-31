DataBento Polars Speed-Up
=========================

Overview
--------
- Canonical DataBento aliases (`DataBentoData`, `DataBentoDataBacktesting`) now point to the Polars implementation.  
- The pandas classes remain available (`DataBentoDataPandas`, `DataBentoDataBacktestingPandas`) for opt-in legacy usage.  
- Parity and cache regression tests ensure polars results match pandas output row-for-row while delivering the intended speed-up.

How To Enable
-------------
- Strategies receive Polars-backed `Bars` by default; call `self.get_historical_prices(..., return_polars=False)` to force pandas.  
- Backtests can still reference the explicit classes when needed:  
  - `DataBentoDataBacktesting` → polars (default)  
  - `DataBentoDataBacktestingPandas` → pandas fallback  
- Live data uses the same alias (`DataBentoData`). Import `DataBentoDataPandas` explicitly if a legacy path is required.

Guaranteed Parity
-----------------
- `PYTHONPATH=. pytest tests/test_data_polars_parity.py tests/test_databento_backtesting_polars.py tests/test_databento_data.py tests/backtest/test_databento_parity.py` (2025‑10‑29): validates pandas vs polars DataBento parity, futures multipliers, and cache reuse with a live `DATABENTO_API_KEY`.  
- `tests/test_databento_backtesting_polars.py::test_get_historical_prices_reuses_cache` asserts warm runs skip network fetches and reuse parquet caches.  
- Existing regression suites (`tests/test_polars_resample.py`, `tests/backtest/test_polars_lru_eviction.py`) continue to cover resampling and LRU trimming logic shared across data sources.

Lookahead Guardrails
--------------------
- `tests/backtest/test_yahoo.py::TestYahooBacktestFull::test_yahoo_no_future_bars_before_open` confirms daily requests stay on the prior session.  
- `tests/test_polygon_helper.py::TestPolygonHelpers::test_polygon_no_future_bars_before_open` patches Polygon fetches and asserts timestamps never exceed the broker clock.  
- `tests/test_thetadata_helper.py::test_thetadata_no_future_minutes` covers ThetaData minute feeds.  
- `tests/test_databento_backtesting_polars.py::test_polars_no_future_minutes` exercises the Polars pipeline directly.  
- `tests/test_market_infinite_loop_bug.py::test_broker_timeshift_guard` snapshots the backtesting broker’s timeshift logic so future refactors cannot regress it.

Performance Notes
-----------------
- Polars caching keeps a rolling window (~5k bars) per asset and trims automatically; pandas retains its on-disk cache but remains opt-in.  
- Aggregated timeframe bars reuse an LRU cache so repeated resample requests stay in memory without recomputation.  
- Conversion guardrails in `Bars` warn when polars payloads are coerced back to pandas, helping strategies stay on the fast path.  
- Benchmark command: ``PYTHONPATH=. python3 tests/performance/profile_databento_comprehensive.py --mode both``  
  - Cold cache (after `rm -rf ~/Library/Caches/lumibot/1.0/databento*`): pandas **25.70 s**, polars **19.85 s** → **1.29×** speed-up.  
  - Warm cache (immediately rerun, no cache clear): pandas **10.42 s**, polars **3.82 s** → **2.73×** speed-up.  
  - Logs live under `tests/performance/logs/databento_comprehensive_{cold,warm}.log`; warm run contains zero `Successfully retrieved` entries, confirming cache hits only.

Migration Checklist
-------------------
- Bars, DataPolars, PolarsData, Strategy, and Broker layers handle polars frames end-to-end (no ad-hoc conversions).  
- DataBento helper utilities share futures multiplier logic with the already merged futures-roll refactor.  
- Legacy multi-file variants were consolidated; only production-ready modules remain in `lumibot/data_sources/`.  
- Optional dependency handling remains unchanged—if `databento` is missing, imports raise `ImportError` and pytest marks affected tests `xfail`.
