# Changelog

## 4.3.8 - 2024-11-16

- ThetaData panda fetches now short-circuit once coverage metadata says the requested start/end are already in cache, and incremental requests append only the missing tail of bars instead of reprocessing the full frame.
- Daily strategies skip the expensive minute-quote merge/forward-fill path entirely, eliminating unnecessary `pd.concat`/transpose work for slow runs such as GoogleMomentum.
- The 09:30 open correction routine memoizes per (symbol, date) and groups contiguous days so the Theta minute downloader is hit exactly once per uncovered window.
- `_combine_duplicate_columns` now deduplicates columns in a single pass (no repeated transpose/drop cycles) and `pd.to_datetime` is skipped whenever frames already carry a `DatetimeIndex`, cutting GoogleMomentum’s Theta run time from ~109 s (dev) to ~75 s on feature.
- Added regression coverage for both the fast-path/append flow and the memoized open-correction behavior.

## 4.3.7 - 2024-11-16

- ThetaData backtests now keep an incremental, processed-frame cache so repeated `get_historical_prices` calls append only the missing bars instead of reloading and re-normalizing the full dataset on every iteration.
- The EOD 09:30 open correction flow reuses cached minute bars per asset/date, fetching only uncovered days and logging clearer diagnostics when Theta rejects the override window.
- Added regression coverage for the caching/retry code plus a downloader smoke script (`scripts/check_eod_chunking.py`) to validate prod chunking.

## 4.3.6 - 2024-11-16

- Fixed ThetaData EOD corrections by fetching a real 09:30–09:31 minute window for each trading day, preventing zero-length requests and the resulting terminal hangs.
- Logged the active downloader base URL whenever remote mode is enabled to make it obvious in backtest logs which data path is being used.
- Added regression tests covering the custom session window override plus the fallback path when Theta rejects an invalid minute range.
