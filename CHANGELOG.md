# Changelog

## 4.3.7 - 2024-11-16

- ThetaData backtests now keep an incremental, processed-frame cache so repeated `get_historical_prices` calls append only the missing bars instead of reloading and re-normalizing the full dataset on every iteration.
- The EOD 09:30 open correction flow reuses cached minute bars per asset/date, fetching only uncovered days and logging clearer diagnostics when Theta rejects the override window.
- Added regression coverage for the caching/retry code plus a downloader smoke script (`scripts/check_eod_chunking.py`) to validate prod chunking.

## 4.3.6 - 2024-11-16

- Fixed ThetaData EOD corrections by fetching a real 09:30–09:31 minute window for each trading day, preventing zero-length requests and the resulting terminal hangs.
- Logged the active downloader base URL whenever remote mode is enabled to make it obvious in backtest logs which data path is being used.
- Added regression tests covering the custom session window override plus the fallback path when Theta rejects an invalid minute range.
