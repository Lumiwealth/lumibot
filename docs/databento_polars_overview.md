DataBento Polars Speed-Up
=========================

Overview
--------
- DataBento backtests now support both pandas and polars backends.  
- Polars (`return_polars=True`) delivers 2x+ faster historical data pulls and indicator calculations.  
- Pandas remains the default for backwards compatibility; parity tests ensure both paths return identical values.

How To Enable
-------------
- Strategy calls: `self.get_historical_prices(..., return_polars=True)` to receive `Bars` backed by polars.  
- Backtesting CLI: `DataBentoDataBacktestingPolars` provides the performance path; pandas classes remain available via `DataBentoDataBacktestingPandas`.  
- Live data: `DataBentoData` auto-selects polars helpers but can fall back to pandas when necessary.

Guaranteed Parity
-----------------
- `pytest tests/test_data_polars_parity.py tests/test_databento_backtesting_polars.py tests/test_databento_data.py tests/backtest/test_databento_parity.py` (2025‑10‑29): pandas vs polars frames match exactly, including live API parity when a `DATABENTO_API_KEY` is present.  
- `tests/test_polars_resample.py`: covers resampling logic shared across strategy functions.  
- `tests/backtest/test_polars_lru_eviction.py`: exercises cache eviction so repeated runs match pandas memory footprints.

Performance Notes
-----------------
- Polars caching keeps a rolling window (~5k bars) per asset and trims automatically.  
- Aggregated timeframe bars reuse an LRU cache to avoid redundant grouping.  
- Conversion guardrails in `Bars` track pandas ⇄ polars conversions to avoid accidental slow paths.  
- Benchmark command: ``PYTHONPATH=. python3.12 tests/performance/profile_databento_comprehensive.py --mode both`` (Apple M3 Pro, cold cache) produced `pandas 24.76s`, `polars 7.14s`, yielding a `3.47×` speedup. Profile artefacts land in `tests/performance/logs/`.

Migration Checklist
-------------------
- Bars, DataPolars, PolarsData, Strategy, and Broker layers now accept polars frames without manual conversion.  
- DataBento helper utilities share futures multiplier logic with the futures roll refactor.  
- Legacy modules (`databento_data_polars_backtesting.py`, `databento_data_polars_live.py`, `yahoo_data_polars.py`) were removed in favour of unified implementations.  
- Optional dependency handling: if `databento` is not installed, data sources raise a clear `ImportError` and tests skip gracefully.
