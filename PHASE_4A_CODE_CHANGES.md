# Phase 4A: Exact Code Changes

## Overview
This document shows EXACTLY what code changes in each file, side-by-side.

---

## Change 1: Bars Class - Add Polars Storage

### File: `lumibot/entities/bars.py`

Let me first check what the current Bars class looks like, then show the changes:

**CURRENT CODE** (Pandas only):
```python
class Bars:
    def __init__(
        self,
        df,
        source,
        symbol=None,
        raw_df=None,
        quote_asset=None,
        missing=None,
        date_end=None,
    ):
        self._df = df  # ← Always stores pandas DataFrame
        self.SOURCE = source
        self.symbol = symbol
        self._raw_df = raw_df
        self.quote = quote_asset
        self.missing = missing
        self.date_end = date_end

    @property
    def df(self):
        """Returns the bars dataframe"""
        return self._df  # ← Always returns pandas
```

**NEW CODE** (Polars internal, Pandas external):
```python
import polars as pl  # ← ADD THIS IMPORT

class Bars:
    def __init__(
        self,
        df,
        source,
        symbol=None,
        raw_df=None,
        quote_asset=None,
        missing=None,
        date_end=None,
    ):
        self.SOURCE = source
        self.symbol = symbol
        self._raw_df = raw_df
        self.quote = quote_asset
        self.missing = missing
        self.date_end = date_end

        # ↓↓↓ NEW: Store polars or pandas based on source ↓↓↓
        if source == "POLARS":
            # Store as polars internally
            if isinstance(df, pl.DataFrame):
                self._df_polars = df
                self._df_pandas = None  # Lazy conversion
            else:
                # If someone passes pandas to polars source, convert
                self._df_polars = pl.from_pandas(df)
                self._df_pandas = None
        else:
            # Pandas source - use original behavior
            self._df_pandas = df
            self._df_polars = None

    @property
    def df(self):
        """
        Returns the bars dataframe as pandas (for backwards compatibility).

        If stored as polars, converts to pandas lazily.
        """
        if self.SOURCE == "POLARS":
            # Lazy conversion: only convert when first accessed
            if self._df_pandas is None:
                self._df_pandas = self._df_polars.to_pandas()
            return self._df_pandas
        else:
            return self._df_pandas
```

**What changed**:
1. Added `import polars as pl` at top of file
2. Split `self._df` into `self._df_polars` and `self._df_pandas`
3. Constructor stores appropriate format based on `source`
4. `.df` property converts polars → pandas lazily (only when accessed)

**Why this works**:
- External API unchanged: `bars.df` still returns pandas
- Internal storage is polars when `SOURCE = "POLARS"`
- Conversion only happens if strategy actually uses `.df`

---

## Change 2: PolarsData - Add Conversion Helpers

### File: `lumibot/data_sources/polars_data.py`

**WHERE TO ADD**: Right after the `__init__` method, add these helpers:

```python
import polars as pl  # ← ADD THIS IMPORT at top of file
import pandas as pd

class PolarsData(DataSourceBacktesting):
    """
    PolarsData is a Backtesting-only DataSource optimized with Polars.
    """

    SOURCE = "POLARS"

    def __init__(self, *args, pandas_data=None, auto_adjust=True, **kwargs):
        # ... existing __init__ code unchanged ...
        pass

    # ↓↓↓ ADD THESE TWO NEW METHODS ↓↓↓

    @staticmethod
    def _pandas_to_polars(df_pandas: pd.DataFrame) -> pl.DataFrame:
        """
        Convert pandas DataFrame to polars DataFrame.

        Handles timezone-aware datetimes properly for backtesting.
        """
        if df_pandas is None or df_pandas.empty:
            return pl.DataFrame()

        # Polars handles timezone-aware datetimes natively
        df_polars = pl.from_pandas(df_pandas)
        return df_polars

    @staticmethod
    def _polars_to_pandas(df_polars: pl.DataFrame) -> pd.DataFrame:
        """
        Convert polars DataFrame to pandas DataFrame.

        Used only at the final boundary when returning to user code.
        """
        if df_polars is None or df_polars.is_empty():
            return pd.DataFrame()

        return df_polars.to_pandas()

    # ... rest of class unchanged ...
```

**What changed**:
1. Added `import polars as pl` at top
2. Added two static helper methods for conversion
3. No changes to existing methods yet (that's next)

**Why static methods**:
- Easy to test independently
- Can be used anywhere in the class
- No dependency on instance state

---

## Change 3: Optimize get_historical_prices

### File: `lumibot/data_sources/polars_data.py`

**FIND**: The `get_historical_prices` method (around line 511)

**BEFORE**:
```python
def get_historical_prices(self, asset, length, timestep="minute", ...):
    # ... validation code ...

    # Pull bars from source
    bars = self._pull_source_symbol_bars(asset, length, timestep, ...)

    if bars is None or bars.df.empty:  # ← Triggers conversion!
        return None

    # Filter by date range
    df = bars.df  # ← Triggers conversion!
    result_df = df[(df.index >= start_date) & (df.index <= end_date)]

    return Bars(result_df, self.SOURCE, ...)
```

**AFTER**:
```python
def get_historical_prices(self, asset, length, timestep="minute", ...):
    # ... validation code unchanged ...

    # Pull bars from source
    bars = self._pull_source_symbol_bars(asset, length, timestep, ...)

    if bars is None:
        return None

    # ↓↓↓ NEW: Keep as polars, avoid conversion ↓↓↓
    if self.SOURCE == "POLARS":
        # Access internal polars DF directly
        df_polars = bars._df_polars

        if df_polars.is_empty():
            return None

        # Use polars filtering (FAST)
        filtered = df_polars.filter(
            (pl.col("timestamp") >= start_date) &
            (pl.col("timestamp") <= end_date)
        )

        return Bars(filtered, self.SOURCE, ...)
    else:
        # Pandas fallback (unchanged)
        df = bars.df
        if df.empty:
            return None
        result_df = df[(df.index >= start_date) & (df.index <= end_date)]
        return Bars(result_df, self.SOURCE, ...)
```

**What changed**:
1. Check if `SOURCE == "POLARS"` to use optimized path
2. Access `bars._df_polars` directly (no conversion)
3. Use polars `.filter()` with expressions (faster than pandas indexing)
4. Return `Bars` with polars DataFrame
5. Keep pandas path as fallback

**Impact**:
- Called 6,283 times in our test
- Each call avoids polars → pandas conversion
- Polars filtering is ~5-10x faster than pandas boolean indexing
- Expected: 30-40% reduction in this method's time

---

## Change 4: Optimize get_last_price

### File: `lumibot/data_sources/polars_data.py`

**FIND**: The `get_last_price` method

**BEFORE**:
```python
def get_last_price(self, asset, quote=None, ...):
    # ... get bars ...

    df = bars.df  # ← Triggers conversion!

    if df.empty:
        return None

    last_row = df.iloc[-1]
    return last_row['close']
```

**AFTER**:
```python
def get_last_price(self, asset, quote=None, ...):
    # ... get bars unchanged ...

    if self.SOURCE == "POLARS":
        # ↓↓↓ NEW: Use polars directly ↓↓↓
        df_polars = bars._df_polars

        if df_polars.is_empty():
            return None

        # Polars .last() is optimized (no indexing overhead)
        last_close = df_polars.select(pl.col("close").last()).item()
        return last_close
    else:
        # Pandas fallback (unchanged)
        df = bars.df
        if df.empty:
            return None
        return df.iloc[-1]['close']
```

**What changed**:
1. Add polars-optimized path
2. Use polars `.select().last()` instead of pandas `.iloc[-1]`
3. Keep pandas fallback

**Impact**:
- Called thousands of times per backtest
- Polars `.last()` is MUCH faster than pandas `.iloc[-1]`
- No conversion overhead

---

## Change 5: Convert to Polars After Network Fetch

### File: `lumibot/backtesting/databento_backtesting_polars.py`

**FIND**: The `_pull_source_symbol_bars` method (around line 564)

**BEFORE**:
```python
def _pull_source_symbol_bars(self, asset, length, timestep, ...):
    # ... setup code ...

    # Fetch from cache (returns pandas)
    df = databento_helper._load_from_cache(cache_path)

    # ... datetime processing with pandas ...

    # Filter with pandas boolean indexing
    filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

    # ... more pandas operations ...

    return Bars(filtered_df, self.SOURCE, ...)  # SOURCE="POLARS" but df is pandas!
```

**AFTER**:
```python
import polars as pl  # ← ADD THIS IMPORT at top

def _pull_source_symbol_bars(self, asset, length, timestep, ...):
    # ... setup code unchanged ...

    # Fetch from cache (returns pandas)
    df_pandas = databento_helper._load_from_cache(cache_path)

    # ↓↓↓ NEW: Convert to polars immediately ↓↓↓
    df_polars = self._pandas_to_polars(df_pandas)

    # ↓↓↓ NEW: Use polars filtering (FAST) ↓↓↓
    filtered_df = df_polars.filter(
        (pl.col("timestamp") >= start) &
        (pl.col("timestamp") <= end)
    )

    # ... other operations can now use polars expressions ...

    return Bars(filtered_df, self.SOURCE, ...)  # Now actually polars!
```

**What changed**:
1. Added `import polars as pl` at top
2. Rename fetched DF to `df_pandas` for clarity
3. Convert to polars immediately with `self._pandas_to_polars()`
4. Replace pandas boolean indexing with polars `.filter()` expressions
5. Return `Bars` with actual polars DataFrame

**Impact**:
- All downstream operations now work with polars
- Polars filtering is faster and uses expressions (optimized)
- Datetime operations handled by polars (native, faster)

---

## Summary of Changes

### Line Count:
- **bars.py**: ~30 lines changed
- **polars_data.py**: ~100 lines changed (mostly new methods)
- **databento_backtesting_polars.py**: ~20 lines changed

### Imports Added:
```python
import polars as pl  # Add to all 3 files
```

### Key Concepts:
1. **Dual storage in Bars**: `_df_polars` and `_df_pandas` (lazy conversion)
2. **Polars filtering**: `.filter(pl.col("x") > value)` instead of `df[df['x'] > value]`
3. **Polars last**: `.select(pl.col("x").last()).item()` instead of `df.iloc[-1]['x']`
4. **Convert early**: Pandas → Polars right after network fetch

### Testing Order:
1. Make Bars changes → test import
2. Add helpers to PolarsData → test import
3. Modify get_historical_prices → run parity test
4. Modify get_last_price → run parity test
5. Modify _pull_source_symbol_bars → run parity test
6. Run full profiler → check speedup

---

## Expected Parity Test Behavior

When you run:
```bash
pytest tests/backtest/test_databento_parity.py -v -s
```

**What should happen**:
1. Pandas backend runs → produces results
2. Polars backend runs → produces results
3. Test compares results → should be IDENTICAL
4. If identical → ✅ PASS (safe to continue)
5. If different → ❌ FAIL (debug conversion logic)

**Common issues if parity fails**:
- Datetime timezone handling (polars vs pandas)
- Floating point precision (rare, but possible)
- Empty DataFrame handling (polars `.is_empty()` vs pandas `.empty`)
- Index handling (polars doesn't have index like pandas)

---

## Expected Profile Improvement

After all changes:

**Before** (baseline):
```
MODE: POLARS
Elapsed time: 95.39s
```

**After Phase 4A** (target):
```
MODE: POLARS
Elapsed time: 70-75s
Speedup: 1.25-1.35x
```

**Breakdown of savings**:
- DateTime operations: ~7s saved (10s → 3s)
- Filtering operations: ~10s saved (polars expressions)
- Conversion overhead: ~3s saved (fewer conversions)
- Historical price fetching: ~5s saved (polars slicing)
- **Total**: ~25s saved (26% speedup)

---

## What NOT to Change

These files should NOT be modified in Phase 4A:

❌ **`lumibot/tools/databento_helper.py`**
- Network I/O is NOT the bottleneck
- Keep fetching as pandas (simpler)
- Convert pandas → polars after fetch

❌ **Test files**
- Tests check behavior, not implementation
- No changes needed

❌ **Strategy files**
- External API unchanged (`bars.df` still returns pandas)
- No user code breaks

❌ **Other data sources**
- Only modify PolarsData and its subclass
- PandasData untouched
