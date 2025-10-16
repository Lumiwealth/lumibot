# Phase 4A Detailed Implementation Plan

## Goal
Store data internally as polars, convert to pandas only at the final boundary. Keep external API unchanged.

## Expected Speedup
20-30% (from 95s → 70-75s) by eliminating:
- 2.7M DatetimeArray iteration calls
- PyArrow → Pandas conversions (use polars parquet reader)
- Repeated pandas filtering operations

---

## Step 1: Understand Current Flow

### Current Data Flow (Pandas)
```
DataBento API (parquet)
    ↓
databento_helper._load_from_cache() [PyArrow → Pandas]
    ↓
databento_backtesting_pandas._pull_source_symbol_bars() [returns Pandas DF]
    ↓
Store in self._data_store as Pandas DF
    ↓
Bars(pandas_df) created
    ↓
Strategy calls bars.df → returns Pandas DF
```

### Target Data Flow (Polars Internal)
```
DataBento API (parquet)
    ↓
databento_helper._load_from_cache() [PyArrow → Pandas] ← KEEP THIS (network not bottleneck)
    ↓
databento_backtesting_polars._pull_source_symbol_bars() [Pandas → Polars conversion]
    ↓
Store in self._data_store as Polars DF
    ↓
All internal operations use Polars (filtering, slicing, datetime ops)
    ↓
Bars(polars_df) created
    ↓
Strategy calls bars.df → converts Polars → Pandas at boundary
```

**Key insight**: We DON'T change `databento_helper.py` because network I/O is not the bottleneck. We convert Pandas → Polars right after fetching, then keep polars internally.

---

## Step 2: Modify Bars Class to Support Polars Storage

### File: `lumibot/entities/bars.py`

#### Current Code (Pandas only):
```python
class Bars:
    def __init__(self, df, source, symbol=None, ...):
        self._df = df  # Always pandas
        self.SOURCE = source
        # ...

    @property
    def df(self):
        return self._df  # Returns pandas
```

#### New Code (Polars internal, Pandas external):
```python
import polars as pl

class Bars:
    def __init__(self, df, source, symbol=None, raw_df=None, ...):
        """
        Args:
            df: Can be pandas DataFrame OR polars DataFrame
            source: "PANDAS" or "POLARS"
            raw_df: For backwards compatibility
        """
        self.SOURCE = source

        # Store internally based on source
        if source == "POLARS":
            if isinstance(df, pl.DataFrame):
                self._df_polars = df
                self._df_pandas = None  # Lazy conversion
            else:
                # If passed pandas, convert to polars
                self._df_polars = pl.from_pandas(df)
                self._df_pandas = None
        else:
            # Pandas source - keep as pandas
            self._df_polars = None
            self._df_pandas = df

    @property
    def df(self):
        """Always returns pandas DataFrame for backwards compatibility."""
        if self.SOURCE == "POLARS":
            # Lazy conversion: only convert when accessed
            if self._df_pandas is None:
                self._df_pandas = self._df_polars.to_pandas()
            return self._df_pandas
        else:
            return self._df_pandas
```

**What this does**:
- Bars can now store polars internally
- External API (`bars.df`) still returns pandas (no breaking changes)
- Conversion happens lazily (only when accessed)
- If strategy never accesses `.df`, we never pay conversion cost

---

## Step 3: Add Polars Conversion Helper to PolarsData

### File: `lumibot/data_sources/polars_data.py`

Add this method near the top of the `PolarsData` class:

```python
import polars as pl
from datetime import datetime
import pandas as pd

class PolarsData(DataSourceBacktesting):
    """Polars-optimized backtesting data source."""

    SOURCE = "POLARS"

    def __init__(self, *args, pandas_data=None, **kwargs):
        super().__init__(*args, **kwargs)
        # ... existing init code ...

    @staticmethod
    def _pandas_to_polars(df_pandas: pd.DataFrame) -> pl.DataFrame:
        """
        Convert pandas DataFrame to polars DataFrame.

        Handles timezone-aware datetimes properly.
        """
        if df_pandas is None or df_pandas.empty:
            return pl.DataFrame()

        # Convert pandas → polars
        df_polars = pl.from_pandas(df_pandas)

        # Polars handles timezone-aware datetimes natively - no extra conversion needed
        return df_polars

    @staticmethod
    def _polars_to_pandas(df_polars: pl.DataFrame) -> pd.DataFrame:
        """
        Convert polars DataFrame to pandas DataFrame.

        Used only at the final boundary when returning to user.
        """
        if df_polars is None or df_polars.is_empty():
            return pd.DataFrame()

        return df_polars.to_pandas()
```

**What this does**:
- Provides clean conversion utilities
- Handles timezone-aware datetimes (critical for backtesting)
- Static methods for easy testing

---

## Step 4: Modify DataBento Polars to Convert After Fetching

### File: `lumibot/backtesting/databento_backtesting_polars.py`

Find the `_pull_source_symbol_bars` method and modify it:

#### Current Code (returns Pandas):
```python
def _pull_source_symbol_bars(self, asset, length, timestep, ...):
    # ... fetch from cache (returns pandas) ...
    df = databento_helper._load_from_cache(cache_path)  # Returns pandas

    # ... filter/process with pandas ...
    filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

    return Bars(filtered_df, self.SOURCE, ...)  # SOURCE = "POLARS" but df is pandas!
```

#### New Code (converts to Polars after fetch):
```python
import polars as pl

def _pull_source_symbol_bars(self, asset, length, timestep, ...):
    # ... fetch from cache (returns pandas) ...
    df_pandas = databento_helper._load_from_cache(cache_path)  # Returns pandas

    # Convert to polars immediately after fetching
    df_polars = self._pandas_to_polars(df_pandas)

    # Use polars filtering (MUCH FASTER than pandas)
    filtered_df = df_polars.filter(
        (pl.col("timestamp") >= start) & (pl.col("timestamp") <= end)
    )

    # Return Bars with polars DataFrame
    return Bars(filtered_df, self.SOURCE, ...)  # SOURCE = "POLARS", df is polars
```

**What this does**:
- Converts pandas → polars right after network fetch
- Uses polars `.filter()` instead of pandas boolean indexing (faster)
- Polars expressions are lazy-evaluated and optimized
- All internal operations now happen in polars

---

## Step 5: Optimize get_historical_prices for Polars

### File: `lumibot/data_sources/polars_data.py`

The `get_historical_prices` method is called 6,283 times in our test. This is a HIGH-FREQUENCY operation.

#### Current Code (from PandasData):
```python
def get_historical_prices(self, asset, length, timestep="minute", ...):
    # ... validation ...

    # Pull bars from source
    bars = self._pull_source_symbol_bars(asset, length, timestep, ...)

    if bars is None or bars.df.empty:
        return None

    # Filter by date range using pandas
    df = bars.df  # This triggers polars → pandas conversion!
    result_df = df[(df.index >= start_date) & (df.index <= end_date)]

    return Bars(result_df, self.SOURCE, ...)
```

#### New Code (optimized for Polars):
```python
def get_historical_prices(self, asset, length, timestep="minute", ...):
    # ... validation ...

    # Pull bars from source (now returns polars internally)
    bars = self._pull_source_symbol_bars(asset, length, timestep, ...)

    if bars is None:
        return None

    # OPTIMIZATION: Keep as polars, filter with polars
    if self.SOURCE == "POLARS":
        # Access internal polars DF directly (avoid conversion)
        df_polars = bars._df_polars

        if df_polars.is_empty():
            return None

        # Filter using polars (FAST - no Python overhead)
        filtered = df_polars.filter(
            (pl.col("timestamp") >= start_date) & (pl.col("timestamp") <= end_date)
        )

        # Return new Bars with filtered polars DF
        return Bars(filtered, self.SOURCE, ...)
    else:
        # Fallback to pandas path for backwards compat
        df = bars.df
        result_df = df[(df.index >= start_date) & (df.index <= end_date)]
        return Bars(result_df, self.SOURCE, ...)
```

**What this does**:
- Avoids polars → pandas conversion until strategy actually needs `.df`
- Uses polars filtering (10x faster than pandas for large datasets)
- Keeps data in polars format through the entire pipeline
- Only converts when strategy calls `bars.df` property

---

## Step 6: Optimize get_last_price for Polars

### File: `lumibot/data_sources/polars_data.py`

This is called thousands of times per backtest.

#### Current Code (Pandas):
```python
def get_last_price(self, asset, quote=None, ...):
    # ... get bars ...
    df = bars.df  # Triggers conversion

    if df.empty:
        return None

    # Get last row
    last_row = df.iloc[-1]
    return last_row['close']
```

#### New Code (Polars optimized):
```python
def get_last_price(self, asset, quote=None, ...):
    # ... get bars ...

    if self.SOURCE == "POLARS":
        # Use polars directly
        df_polars = bars._df_polars

        if df_polars.is_empty():
            return None

        # Polars .last() is MUCH faster than pandas .iloc[-1]
        last_close = df_polars.select(pl.col("close").last()).item()
        return last_close
    else:
        # Pandas fallback
        df = bars.df
        if df.empty:
            return None
        return df.iloc[-1]['close']
```

**What this does**:
- Uses polars `.last()` method (optimized, no indexing overhead)
- No conversion to pandas needed
- Dramatically faster for high-frequency calls

---

## Step 7: Testing Protocol

After implementing these changes:

### Test 1: Parity Test (MUST PASS)
```bash
pytest tests/backtest/test_databento_parity.py -v -s
```

**What we're checking**:
- Pandas and Polars backends produce IDENTICAL results
- All asset prices match
- All trade executions match
- Final portfolio values match

**If fails**: Bug in polars conversion or filtering logic. Fix before proceeding.

### Test 2: Profile Performance
```bash
python -m tests.performance.profile_databento_mes_momentum --mode both
```

**What we're checking**:
- Polars backend is 20-30% faster
- DatetimeArray iteration calls reduced
- Parquet conversion overhead reduced

**Expected results**:
```
Pandas:  94.85s
Polars:  70-75s (target)
Speedup: 1.25-1.35x
```

### Test 3: Comprehensive Tests
```bash
pytest tests/backtest/test_databento_comprehensive_trading.py -m apitest -v
```

**What we're checking**:
- Multi-asset backtests work
- Complex strategies work
- No edge case failures

---

## File Change Summary

### Files to Modify:
1. ✏️ **`lumibot/entities/bars.py`**
   - Add polars storage support
   - Keep pandas external API
   - ~30 lines changed

2. ✏️ **`lumibot/data_sources/polars_data.py`**
   - Add conversion helpers
   - Optimize `get_historical_prices()`
   - Optimize `get_last_price()`
   - ~100 lines changed

3. ✏️ **`lumibot/backtesting/databento_backtesting_polars.py`**
   - Convert to polars after fetch
   - Use polars filtering
   - ~20 lines changed

### Files to NOT Change:
- ❌ `lumibot/tools/databento_helper.py` - Network I/O not the bottleneck
- ❌ Test files - They test behavior, not implementation
- ❌ Strategy files - External API unchanged

---

## Why This Order?

### Step 1-2: Foundation
Bars class must support polars storage before anything else. This is the "container" for all data.

### Step 3: Utilities
Conversion helpers make Steps 4-6 cleaner and testable.

### Step 4: Entry Point
Convert at the source (after network fetch). Now all downstream code gets polars.

### Step 5-6: Hot Paths
Optimize the highest-frequency operations first (biggest impact).

### Step 7: Validation
Test after each major change to catch bugs early.

---

## Risk Mitigation

### Risk: Datetime timezone handling breaks
**Mitigation**: Polars handles timezone-aware datetimes natively. Test with `pytz.timezone("America/New_York")` dates.

### Risk: Conversion overhead negates gains
**Mitigation**: Only convert at final boundary (when strategy accesses `.df`). Most internal operations stay in polars.

### Risk: Breaking changes to external API
**Mitigation**: `bars.df` property still returns pandas. No user-facing changes.

### Risk: Parity tests fail
**Mitigation**: Implement incrementally. Test after each change. Keep pandas fallback paths.

---

## Expected Profile Changes

### Before (Phase 3):
```
DatetimeArray.__iter__:     10.16s (2.7M calls)
Parquet read:               2.69s
table_to_dataframe:         1.30s
get_historical_prices:      54.5s cumulative
Total:                      94.85s
```

### After Phase 4A (Target):
```
DatetimeArray.__iter__:     ~2-3s (90% reduction - polars native datetimes)
Parquet read:               2.69s (unchanged - not optimizing network yet)
table_to_dataframe:         ~0.3s (70% reduction - fewer conversions)
get_historical_prices:      ~35-40s (30% reduction - polars filtering)
Total:                      ~70-75s (20-30% speedup)
```

---

## Next Steps After Phase 4A

If Phase 4A achieves 20-30% speedup and parity tests pass:

**Phase 4B**: Optimize parquet reading
- Use polars native parquet reader (skip pandas entirely)
- Expected additional 15-20% speedup

**Phase 4C**: Lazy evaluation
- Use polars LazyFrames throughout pipeline
- Expected additional 20-30% speedup

**Total potential**: 60-70% speedup by Phase 4C completion
