# BACKTESTING_ARCHITECTURE.md - LumiBot Backtesting Architecture

## Overview

LumiBot is a trading and backtesting framework. This document focuses on the **backtesting architecture**, specifically how data flows from external sources (Yahoo, ThetaData, Polygon) into the backtesting engine.

## Directory Structure

```
lumibot/
├── backtesting/           # Backtesting data source implementations
│   ├── backtesting_broker.py        # Core BacktestingBroker class
│   ├── yahoo_backtesting.py         # Yahoo Finance adapter
│   ├── thetadata_backtesting_pandas.py  # ThetaData adapter
│   ├── polygon_backtesting.py       # Polygon.io adapter
│   └── pandas_backtesting.py        # Base class for pandas-based sources
│
├── data_sources/          # Base data source classes
│   ├── data_source.py               # Abstract DataSource base
│   ├── data_source_backtesting.py   # DataSourceBacktesting base
│   ├── yahoo_data.py                # Yahoo data fetching
│   ├── pandas_data.py               # Pandas data handling
│   └── polars_data.py               # Polars data handling
│
├── tools/                 # Helper modules for data fetching
│   ├── thetadata_helper.py          # ThetaData API & caching (IMPORTANT)
│   ├── yahoo_helper.py              # Yahoo Finance API
│   ├── polygon_helper.py            # Polygon.io API & caching
│   └── backtest_cache.py            # S3/local cache management
│
├── strategies/            # Strategy execution
│   ├── strategy.py                  # Main Strategy class
│   └── _strategy.py                 # Internal strategy logic
│
└── entities/              # Data structures
    ├── asset.py                     # Asset class
    ├── bars.py                      # OHLCV bars
    ├── data.py                      # Pandas-based Data class (ThetaData, Yahoo, Polygon)
    ├── data_polars.py               # Polars-based DataPolars class (Databento ONLY)
    └── order.py                     # Order handling
```

## Data Flow for Backtesting

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Strategy.backtest()                            │
│                    (lumibot/strategies/_strategy.py)                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Data Source Selection (line ~1466)                    │
│                                                                          │
│  BACKTESTING_DATA_SOURCE env var OVERRIDES explicit datasource_class    │
│                                                                          │
│  Options: yahoo, thetadata, polygon, alpaca, ccxt, databento             │
│  Set to "none" to use explicit class from code                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │    Yahoo     │ │  ThetaData   │ │   Polygon    │
           │  Backtesting │ │  Backtesting │ │  Backtesting │
           └──────────────┘ └──────────────┘ └──────────────┘
                    │               │               │
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │ YahooHelper  │ │ thetadata_   │ │  polygon_    │
           │              │ │   helper     │ │   helper     │
           └──────────────┘ └──────────────┘ └──────────────┘
                    │               │               │
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │  yfinance    │ │    Data      │ │ Polygon API  │
           │   library    │ │  Downloader  │ │              │
           └──────────────┘ └──────────────┘ └──────────────┘
                                    │
                                    ▼
                           ┌──────────────┐
                           │  S3 Cache    │
                           │  (optional)  │
                           └──────────────┘
```

## Key Components

### 1. BacktestingBroker (`backtesting/backtesting_broker.py`)

The core broker for simulating trades during backtests:
- Manages simulated positions, orders, and cash
- Tracks market sessions and trading calendars
- Handles futures margin requirements
- Requires a `DataSourceBacktesting` instance

### 2. Data Source Hierarchy

```
DataSource (ABC)
    └── DataSourceBacktesting (ABC)
            ├── PandasData                        # Uses entities/data.py (Data class)
            │   ├── PolygonDataBacktesting
            │   └── ThetaDataBacktestingPandas
            ├── YahooData
            │   └── YahooDataBacktesting
            └── PolarsData                        # Uses entities/data_polars.py (DataPolars class)
                └── DatabentoBacktestingPolars
```

### Entity Classes: Pandas vs Polars

**IMPORTANT:** The `Data` class (pandas-based) and `DataPolars` class (polars-based) are NOT interchangeable.

| Entity Class | File | Used By | Description |
|--------------|------|---------|-------------|
| `Data` | `entities/data.py` | ThetaData, Yahoo, Polygon, Alpaca, CCXT | Pandas-based OHLCV storage with bid/ask support |
| `DataPolars` | `entities/data_polars.py` | Databento ONLY | Polars-based OHLCV storage (optimized for Databento's format) |

**Why the distinction:**
- Databento provides data in a format optimized for polars
- Most other sources (ThetaData, Yahoo, Polygon) use pandas DataFrames
- The two entity classes have similar interfaces but different internal implementations
- **DO NOT** modify `data_polars.py` when fixing ThetaData issues

**Key Methods Both Provide:**
- `get_last_price(dt)` - Get price at datetime (Data has bid/ask fallback, DataPolars does not)
- `get_price_snapshot(dt)` - Get OHLC + bid/ask snapshot
- `get_iter_count(dt)` - Get iteration index for datetime

### 3. Yahoo Finance (`yahoo_backtesting.py` → `yahoo_data.py` → `yahoo_helper.py`)

**Flow:**
1. `YahooDataBacktesting` inherits from `YahooData`
2. `YahooData` uses `YahooHelper` to fetch data via `yfinance` library
3. Data is **already split-adjusted** by Yahoo
4. No additional split processing needed

**Key Function:** `YahooHelper.get_historical_prices()`

### 4. ThetaData (`thetadata_backtesting_pandas.py` → `thetadata_helper.py`)

**Flow:**
1. `ThetaDataBacktestingPandas` inherits from `PandasData`
2. Calls `thetadata_helper.get_price_data()` to fetch data
3. Data comes from **Data Downloader** (remote HTTP service)
4. Uses S3 cache for performance

**Key Functions:**
- `get_price_data()` - Main entry point (line 1248)
- `_apply_corporate_actions_to_frame()` - Handles splits (line 1018)

**Split Handling (FIXED - Nov 28, 2025)**

✅ **ThetaData split handling is now working correctly.**

The ThetaData Data Downloader returns **UNADJUSTED** prices (NOT split-adjusted like Yahoo).
The `_apply_corporate_actions_to_frame()` function applies split adjustments with idempotency protection.

**Root Cause (Fixed):**
- The function was being called 26+ times per backtest without any idempotency check
- Each call re-applied split adjustments, causing over-correction (81% CAGR vs expected 56%)

**Fix Applied:**
1. Added `_split_adjusted` column marker to track if data has been adjusted
2. Function now skips adjustment if marker is already present
3. Cache version bumped to v7 to invalidate stale data

**Test Results (After Split Fix):**
| Condition | CAGR | Worst Day | Status |
|-----------|------|-----------|--------|
| No adjustment | 7.5% | -64% | WRONG - unadjusted |
| Multiple adjustments (broken) | 81% | -95% | WRONG - over-adjusted |
| With idempotency fix | 55.07% | -18.69% | ✅ CORRECT |
| Yahoo baseline | 56% | -27% | ✅ CORRECT |

**Dividend Handling (FIXED - Nov 28, 2025)**

ThetaData returns **UNADJUSTED dividend amounts** (pre-split), but they were being applied directly without split adjustment.

**Issues Found & Fixed:**

1. **Multiple dividend application** - `_update_cash_with_dividends()` was called 3 times per day
   - Fix: Added `_dividends_applied_tracker` set in `_strategy.py` to track (date, symbol) combinations
   - Dividends now only applied once per day per asset

2. **Dividends not split-adjusted** - Raw ThetaData dividend amounts were used directly
   - Fix: `get_yesterday_dividends()` in `thetadata_backtesting_pandas.py` now fetches splits and divides dividend amounts by cumulative split factor
   - Example: $1.22 dividend from 2015 ÷ 6 (split factor) = $0.20 adjusted

**Test Results (After Dividend Fix):**
| Condition | CAGR | Best Day | Status |
|-----------|------|----------|--------|
| Dividends not adjusted | 51.71% | +24.4% | Inflated by raw dividends |
| With dividend split-adjustment | 47.92% | +18.43% | Better but still differs |
| Yahoo baseline | ~56% | ~30% | Target |

**REMAINING ISSUE: ThetaData Phantom Dividends**

ThetaData returns dividends on dates where Yahoo shows NONE:
- 2014-09-18: $0.41 (Yahoo: no dividend)
- 2015-07-02: $1.22 (Yahoo: no dividend)

Even after split adjustment, these phantom dividends affect results. Consider disabling ThetaData dividends entirely or cross-validating with Yahoo.

**Zero-Price Data Filtering (FIXED - Nov 28, 2025)**

ThetaData sometimes returns rows with all-zero OHLC values (e.g., Saturday 2019-06-08 for MELI). This caused `ZeroDivisionError` when strategies tried to calculate position sizes.

**Fix Applied:**
1. Zero-price filtering when loading from cache (`thetadata_helper.py` lines ~2501-2513)
2. Zero-price filtering when receiving new data (`thetadata_helper.py` lines ~2817-2829)
3. Cache is self-healing - bad data automatically filtered on load

**Filtering Logic:**
```python
# Filter rows where ALL OHLC values are zero
all_zero = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
df = df[~all_zero]
```

**Note:** Weekend filtering was intentionally NOT added because markets may trade on weekends in the future (crypto, futures). The issue is zero prices, not weekend dates.

### 5. Polygon (`polygon_backtesting.py` → `polygon_helper.py`)

**Flow:**
1. `PolygonDataBacktesting` inherits from `PandasData`
2. Calls `polygon_helper.get_price_data_from_polygon()` to fetch data
3. Uses local cache in `LUMIBOT_CACHE_FOLDER/polygon`
4. Handles split adjustments via `validate_cache()`

**Key Function:** `get_price_data_from_polygon()` (line 80)

## Progress Logging and Download Status Tracking

### Progress CSV Output

During backtests, LumiBot writes real-time progress to `logs/progress.csv` for frontend display.

**CSV Columns:**
| Column | Description |
|--------|-------------|
| `timestamp` | Wall-clock time of update |
| `percent` | Backtest completion percentage (0-100) |
| `elapsed` | Time elapsed since start |
| `eta` | Estimated time remaining |
| `portfolio_value` | Current portfolio value |
| `simulation_date` | Current datetime in the simulation (YYYY-MM-DD HH:MM:SS) |
| `cash` | Current cash balance |
| `total_return_pct` | Running total return percentage |
| `positions_json` | JSON array of minimal position dicts |
| `orders_json` | JSON array of minimal order dicts |
| `download_status` | JSON object tracking data download progress |

### Minimal Serialization Methods

Entity classes provide `to_minimal_dict()` methods for lightweight progress logging:

**Asset.to_minimal_dict()**
```python
# Stock:
{"symbol": "AAPL", "type": "stock"}

# Option:
{"symbol": "AAPL", "type": "option", "strike": 150.0, "exp": "2024-12-20", "right": "CALL", "mult": 100}

# Future:
{"symbol": "ES", "type": "future", "exp": "2024-12-20", "mult": 50}
```

**Position.to_minimal_dict()**
```python
{"asset": {...}, "qty": 100, "val": 15000.00, "pnl": 500.00}
```

**Order.to_minimal_dict()**
```python
{"asset": {...}, "side": "buy", "qty": 100, "type": "market", "status": "filled"}
# Limit orders add: "limit": 150.0
# Stop orders add: "stop": 140.0
```

### Download Status Tracking (ThetaData)

ThetaData downloads can occur at any point during a backtest when data is needed. The download status tracking system provides visibility into these downloads.

**Location:** `lumibot/tools/thetadata_helper.py`

**Functions:**
- `get_download_status()` - Get current download state
- `set_download_status(asset, quote_asset, data_type, timespan, current, total)` - Update status
- `clear_download_status()` - Clear status after download completes

**Download Status Format:**
```python
{
    "active": True,           # Whether download is in progress
    "asset": {...},           # Minimal asset dict being downloaded
    "quote": "USD",           # Quote asset symbol
    "data_type": "ohlc",      # Data type (ohlc, trades, quotes)
    "timespan": "minute",     # Timespan (minute, day, etc.)
    "progress": 50,           # Progress percentage (0-100)
    "current": 5,             # Current chunk number
    "total": 10               # Total chunks
}
```

**Extending to Other Data Sources:**

To add download status tracking to other data sources (Yahoo, Polygon, etc.):

1. Import the tracking functions:
   ```python
   from lumibot.tools.thetadata_helper import (
       get_download_status, set_download_status, clear_download_status
   )
   ```

2. Call `set_download_status()` during fetch operations with current progress

3. Call `clear_download_status()` when fetch completes (success or failure)

4. The status will automatically be included in the progress CSV

**Note:** The download status functions are thread-safe (use a lock internally), so they can be called from parallel download threads.

## Caching System

### S3 Cache (`tools/backtest_cache.py`)

Used primarily by ThetaData:
- Bucket: Configured via `LUMIBOT_CACHE_S3_BUCKET`
- Version: `LUMIBOT_CACHE_S3_VERSION` (bump to invalidate)
- Mode: `LUMIBOT_CACHE_MODE` (read, write, readwrite)

**Important:** If cache has corrupted data (e.g., from before a bug fix), bump the version number.

### Local Cache

Each data source has its own local cache:
- ThetaData: Parquet files in `~/Library/Caches/lumibot/`
- Polygon: Feather files in `LUMIBOT_CACHE_FOLDER/polygon/`

## Environment Variables

### Data Source Selection
```bash
BACKTESTING_DATA_SOURCE=thetadata  # Options: yahoo, thetadata, polygon, etc.
                                    # Set to "none" to use code-specified class
```

### ThetaData Configuration
```bash
THETADATA_USERNAME=xxx
THETADATA_PASSWORD=xxx
DATADOWNLOADER_BASE_URL=http://44.192.43.146:8080  # Data Downloader URL
DATADOWNLOADER_API_KEY=xxx
DATADOWNLOADER_SKIP_LOCAL_START=true  # Don't start local ThetaTerminal
```

### S3 Cache Configuration
```bash
LUMIBOT_CACHE_BACKEND=s3
LUMIBOT_CACHE_S3_BUCKET=lumibot-cache-dev
LUMIBOT_CACHE_S3_VERSION=v5  # Bump to invalidate cache
LUMIBOT_CACHE_MODE=readwrite
```

## Important Rules

### ThetaData Rules (from AGENTS.md)

1. **NEVER run ThetaTerminal locally** - Only use the Data Downloader
2. **Use the shared downloader endpoint** - Set `DATADOWNLOADER_BASE_URL`
3. **Respect queue/backoff** - Handle `{"error":"queue_full"}` responses
4. **Long commands need safe-timeout** - Use `safe-timeout` wrapper

### Split Adjustment Rules

- **Yahoo**: Already split-adjusted, no action needed ✅
- **ThetaData Data Downloader**: Returns UNADJUSTED data - adjustment code applies splits ✅
  - Fixed Nov 28, 2025: Added idempotency check to prevent multiple adjustments
  - Results now match Yahoo within ~1-2%
- **Polygon**: Handles splits in `validate_cache()`

## Troubleshooting

### Backtest Results Don't Match Between Data Sources

1. Check `BACKTESTING_DATA_SOURCE` env var - it overrides code
2. Verify cache version is consistent across .env files
3. Look for impossible daily returns (e.g., -50%, +100%) indicating split issues
4. Compare raw price data for specific dates (especially around split dates)

### TQQQ Split Dates for Testing

| Date       | Ratio | Type          |
|------------|-------|---------------|
| 2017-01-12 | 2:1   | Forward split |
| 2018-05-24 | 3:1   | Forward split |
| 2021-01-21 | 2:1   | Forward split |
| 2022-01-13 | 1:2   | REVERSE split |
| 2025-11-20 | 2:1   | Forward split |

### Cache Issues

If seeing wrong prices:
1. Bump `LUMIBOT_CACHE_S3_VERSION`
2. Clear local cache: `rm -rf ~/Library/Caches/lumibot/`
3. Re-run backtest to fetch fresh data

## File Locations Summary

| Component | Location |
|-----------|----------|
| LumiBot library | `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/` |
| Strategy Library | `/Users/robertgrzesik/Documents/Development/Strategy Library/` |
| Demo strategies | `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/` |
| Log output | `/Users/robertgrzesik/Documents/Development/Strategy Library/logs/` |
| Local cache | `~/Library/Caches/lumibot/` |

## See Also

- `AGENTS.md` - Critical rules for ThetaData usage
- `CLAUDE.md` - AI assistant instructions
- `CHANGELOG.md` - Version history
