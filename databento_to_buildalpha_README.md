# DataBento to Build Alpha Exporter

A script to download continuous futures data from DataBento and export it in Build Alpha CSV format.

## Features

- Downloads continuous futures data from DataBento
- Automatically handles futures contract rolling
- **Timezone conversion support (UTC or EST with automatic daylight savings)**
- Exports to Build Alpha-compatible CSV format
- Organizes data in structured directory hierarchy
- Ensures data is properly sorted and deduplicated

## Directory Structure

The script creates the following directory structure:

```
databento_exports/
    {SYMBOL_ROOT}/
        {YYYYMMDD}_{YYYYMMDD}/
            {timeframe}/
                {symbol_root}_{start_date}_{end_date}_{timeframe}_{timezone}.csv
```

**Examples:**
```
databento_exports/
    GC/
        20250101_20251031/
            1m/
                GC_20250101_20251031_1m_UTC.csv
                GC_20250101_20251031_1m_EST.csv
```

## CSV Format

The exported CSV follows Build Alpha's expected format:

```csv
Date,Time,Open,High,Low,Close,Vol,OI
01/02/2025,00:33:00,2652.0,2652.0,2652.0,2652.0,1,1
01/02/2025,03:12:00,2657.9,2657.9,2657.7,2657.7,1,1
...
```

### Column Specifications

- **Date**: m/d/yyyy format (e.g., 01/02/2025)
- **Time**: HH:MM:SS format in 24-hour time (e.g., 14:30:00) **in specified timezone**
- **Open, High, Low, Close**: Continuous futures prices (adjusted for contract rolls)
- **Vol**: Hardcoded to 1 (per Build Alpha requirements)
- **OI**: Hardcoded to 1 (per Build Alpha requirements)

### Timezone Support

The script supports two output timezones:

- **UTC** - Coordinated Universal Time (no daylight savings)
- **EST** - US Eastern Time (automatically handles EST/EDT transitions)
  - **EST (Standard Time)**: UTC-5 (roughly November through March)
  - **EDT (Daylight Time)**: UTC-4 (roughly March through November)

The timezone conversion uses Python's `zoneinfo` with the `America/New_York` timezone database, which automatically handles all daylight savings transitions according to US federal rules.

### Data Quality

- ✅ Sorted by Date+Time in ascending chronological order
- ✅ No duplicate Date+Time records
- ✅ No forward filling through gaps or weekends
- ✅ Continuous futures price series (contract rolls handled automatically)

## Requirements

1. **Environment Setup**
   ```bash
   source venv/bin/activate  # Activate Lumibot virtual environment
   ```

2. **API Key**

   Set your DataBento API key as an environment variable:
   ```bash
   export DATABENTO_API_KEY="your_api_key_here"
   ```

   Or add it to your `.env` file in the project root:
   ```
   DATABENTO_API_KEY=your_api_key_here
   ```

3. **Dependencies**

   All dependencies are already installed in the Lumibot environment:
   - databento
   - polars
   - lumibot

## Usage

### Basic Command

```bash
python databento_to_buildalpha.py SYMBOL START_DATE END_DATE TIMEFRAME TIMEZONE
```

### Arguments

- **SYMBOL** - Futures root symbol (e.g., GC, ES, NQ, CL)
- **START_DATE** - Start date in YYYY-MM-DD format
- **END_DATE** - End date in YYYY-MM-DD format (inclusive)
- **TIMEFRAME** - Candle timeframe: `1m` (1-minute), `1h` (1-hour), or `1d` (1-day)
- **TIMEZONE** - Output timezone: `UTC` or `EST`

### Examples

**Download 1-minute Gold futures data in UTC:**
```bash
python databento_to_buildalpha.py GC 2025-01-01 2025-10-31 1m UTC
```

**Download 1-minute Gold futures data in US Eastern Time:**
```bash
python databento_to_buildalpha.py GC 2025-01-01 2025-10-31 1m EST
```

**Download 1-hour E-mini S&P 500 futures data in EST:**
```bash
python databento_to_buildalpha.py ES 2024-01-01 2024-12-31 1h EST
```

**Download daily Crude Oil futures data in UTC:**
```bash
python databento_to_buildalpha.py CL 2023-01-01 2023-12-31 1d UTC
```

## Supported Futures Symbols

Common futures supported include:

- **Metals**: GC (Gold), SI (Silver)
- **Equity Indices**: ES (E-mini S&P 500), NQ (E-mini Nasdaq), YM (E-mini Dow)
- **Micro Indices**: MES (Micro E-mini S&P), MNQ (Micro E-mini Nasdaq)
- **Energy**: CL (Crude Oil), NG (Natural Gas)
- **Rates**: ZB (30-Year T-Bond), ZN (10-Year T-Note)

Any symbol available in DataBento's CME dataset (GLBX.MDP3) is supported.

## Output Examples

### Example 1: UTC Timezone

```bash
$ python databento_to_buildalpha.py GC 2025-01-02 2025-01-03 1m UTC

DataBento to Build Alpha Export
================================
Symbol:     GC
Start:      2025-01-02
End:        2025-01-03
Timeframe:  1m
Timezone:   UTC (UTC)

Step 1: Downloading data from DataBento...
  ✓ Downloaded 57 rows

Step 2: Converting to Build Alpha format...
  ✓ Converted 57 rows
  ✓ Date range: 01/02/2025 to 01/03/2025
  ✓ Timezone: UTC

Step 3: Saving to CSV...
  ✓ Saved to: databento_exports/GC/20250102_20250103/1m/GC_20250102_20250103_1m_UTC.csv

Sample data (first 5 rows):
┌────────────┬──────────┬────────┬────────┬────────┬────────┬─────┬─────┐
│ Date       │ Time     │ Open   │ High   │ Low    │ Close  │ Vol │ OI  │
│ 01/02/2025 │ 00:33:00 │ 2652.0 │ 2652.0 │ 2652.0 │ 2652.0 │ 1   │ 1   │
│ 01/02/2025 │ 03:12:00 │ 2657.9 │ 2657.9 │ 2657.7 │ 2657.7 │ 1   │ 1   │
...

✅ Export complete!
   Timestamps are in UTC timezone
```

### Example 2: EST Timezone (with automatic DST handling)

```bash
$ python databento_to_buildalpha.py GC 2025-01-02 2025-01-03 1m EST

DataBento to Build Alpha Export
================================
Symbol:     GC
Start:      2025-01-02
End:        2025-01-03
Timeframe:  1m
Timezone:   EST (America/New_York)

Step 1: Downloading data from DataBento...
  ✓ Downloaded 57 rows

Step 2: Converting to Build Alpha format...
  ✓ Converted 57 rows
  ✓ Date range: 01/01/2025 to 01/03/2025
  ✓ Timezone: EST

Step 3: Saving to CSV...
  ✓ Saved to: databento_exports/GC/20250102_20250103/1m/GC_20250102_20250103_1m_EST.csv

Sample data (first 5 rows):
┌────────────┬──────────┬────────┬────────┬────────┬────────┬─────┬─────┐
│ Date       │ Time     │ Open   │ High   │ Low    │ Close  │ Vol │ OI  │
│ 01/01/2025 │ 19:33:00 │ 2652.0 │ 2652.0 │ 2652.0 │ 2652.0 │ 1   │ 1   │  ← 5 hours behind UTC
│ 01/01/2025 │ 22:12:00 │ 2657.9 │ 2657.9 │ 2657.7 │ 2657.7 │ 1   │ 1   │
...

✅ Export complete!
   Timestamps are in EST timezone
```

**Note**: January uses EST (UTC-5), while summer months would use EDT (UTC-4) automatically.

## How It Works

1. **Continuous Futures Resolution**: The script creates a continuous futures asset using Lumibot's `Asset` class, which automatically handles contract rolling based on standard roll schedules.

2. **DataBento API**: Uses Lumibot's existing DataBento helper functions to fetch historical OHLCV data with proper authentication and caching. All data is fetched in UTC timezone.

3. **Timezone Conversion** (NEW):
   - Data is downloaded in UTC from DataBento
   - Converted to target timezone using Python's `zoneinfo` library
   - EST uses `America/New_York` timezone which automatically handles:
     - **EST (Standard Time)**: UTC-5 during winter months
     - **EDT (Daylight Time)**: UTC-4 during summer months
     - All DST transitions per US federal rules
   - Timestamps remain chronologically sorted after conversion

4. **Data Processing**:
   - Ensures data is sorted chronologically
   - Removes duplicate timestamps
   - Converts datetime to Build Alpha's Date/Time format
   - Sets Vol and OI to 1 as required

5. **Export**: Saves to CSV in the proper directory structure for Build Alpha import with timezone label in filename.

## Caching

The script uses Lumibot's built-in DataBento caching system:
- Cache location: `~/.lumibot/cache/databento_polars/`
- Subsequent requests for the same data will be faster
- Use `force_cache_update=True` in the code to bypass cache

## Troubleshooting

**Issue**: `DATABENTO_API_KEY not set`
- **Solution**: Set the environment variable or add to `.env` file

**Issue**: `ModuleNotFoundError: No module named 'polars'`
- **Solution**: Activate the virtual environment: `source venv/bin/activate`

**Issue**: No data returned
- **Solution**:
  - Verify the symbol is correct (use uppercase)
  - Check that DataBento has data for that date range
  - Ensure your API key has access to futures data

**Issue**: Data quality warnings from DataBento
- **Solution**: These warnings are informational and indicate degraded data quality for specific dates. The script will still export all available data.

## Notes

- The script downloads **continuous futures** data, which means contract rolls are handled automatically
- DataBento uses 1-digit year format internally (e.g., GCH5 for March 2025 Gold)
- Futures trading sessions may include overnight data (timestamps outside regular trading hours)
- Date ranges are inclusive (both start and end dates are included)
- **Timezone Conversion**: All data is fetched in UTC then converted to your chosen timezone
  - Use UTC for global markets or if you don't need timezone conversion
  - Use EST for US markets (recommended for CME futures during US trading hours)
  - EST automatically becomes EDT during daylight savings months (March-November)
  - The filename includes the timezone label for clarity (e.g., `_UTC.csv` or `_EST.csv`)

## Support

For issues with the script or DataBento integration, check:
- DataBento documentation: https://databento.com/docs
- Lumibot documentation: https://lumibot.lumiwealth.com/
- DataBento status page: https://databento.com/docs/api-reference-historical/metadata/metadata-get-dataset-condition
