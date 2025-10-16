# Timezone Bug Investigation

## Date: October 15, 2025

## Status: RESOLVED ✅

## Summary

While implementing polars optimization and backwards compatibility fixes, discovered a **timezone handling bug** where the polars data source returns timestamps that are 4 hours off compared to the pandas data source.

## The Bug

### Symptom
Parity test `test_databento_price_parity` fails with timezone mismatch:
- **Pandas (CORRECT)**: `2025-09-12 14:39:00-04:00` (14:39 EDT = 18:39 UTC)
- **Polars (WRONG)**: `2025-09-12 18:39:00-04:00` (18:39 EDT = 22:39 UTC)
- **Difference**: 4 hours

### Root Cause Analysis

1. **Data Source**: DataBento returns data in UTC timezone
2. **Expected Flow**:
   - UTC: `18:39:00 UTC`
   - Should convert to: `14:39:00 EDT` (UTC-4 in September)
3. **Actual Bug**: The time `18:39:00` is being treated as naive and then localized to EDT, becoming `18:39:00 EDT` instead of being converted from UTC

### What Works

Created test script `test_timezone_debug.py` that proves:
✅ Polars preserves UTC timezone correctly when converting from pandas
✅ Polars→pandas conversion preserves timezone
✅ `tz_convert('America/New_York')` works correctly on timezone-aware index

The conversion logic itself is **CORRECT**. The bug is in the integration.

## Fixes Applied

### 1. Fixed Index Name Consistency
**File**: `lumibot/tools/databento_helper_polars.py:562-572`

**Problem**: Index name was sometimes "ts_event" instead of "datetime", causing `reset_index()` to create wrong column name.

**Fix**:
```python
def _ensure_datetime_index_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame index is a UTC-aware DatetimeIndex with standard name 'datetime'."""
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        # CRITICAL: Always set index name to 'datetime' for consistency
        df.index.name = "datetime"
    return df
```

### 2. Added Timezone Preservation Logic
**File**: `lumibot/tools/databento_helper_polars.py:1004-1040`

**Added**: Verification that timezone is preserved when converting to polars:
```python
# CRITICAL: Preserve timezone information during conversion
index_tz = combined.index.tz if isinstance(combined.index, pd.DatetimeIndex) else None

# ... conversion ...

# Verify timezone was preserved in polars
polars_tz = combined_polars["datetime"].dtype.time_zone
if polars_tz != str(index_tz):
    logger.warning(f"Timezone mismatch after polars conversion: pandas={index_tz}, polars={polars_tz}")
```

### 3. Enhanced DataPolars Timezone Handling
**File**: `lumibot/entities/data_polars.py:174-222`

**Added**: Proper UTC→DEFAULT_PYTZ conversion logic:
```python
@property
def df(self):
    """Return pandas DataFrame for compatibility. Converts from polars on-demand."""
    if self._pandas_df is None:
        # Check if polars DataFrame has timezone-aware datetime column
        polars_tz = None
        if "datetime" in self.polars_df.columns:
            polars_tz = self.polars_df["datetime"].dtype.time_zone

        # Convert polars to pandas
        self._pandas_df = self.polars_df.to_pandas()
        if "datetime" in self._pandas_df.columns:
            self._pandas_df.set_index("datetime", inplace=True)

        # CRITICAL TIMEZONE HANDLING:
        # Data from DataBento comes in UTC. When converting back to pandas,
        # we need to ensure correct timezone conversion (UTC → DEFAULT_PYTZ),
        # not just localization which treats naive times as already in DEFAULT_PYTZ.
        if polars_tz is not None:
            # Polars had timezone info (should be UTC from data source)
            if not self._pandas_df.index.tzinfo:
                # Timezone was lost, re-localize then convert
                self._pandas_df.index = self._pandas_df.index.tz_localize(polars_tz)
                self._pandas_df.index = self._pandas_df.index.tz_convert(DEFAULT_PYTZ)
            elif self._pandas_df.index.tzinfo != DEFAULT_PYTZ:
                # Timezone preserved, just convert
                self._pandas_df.index = self._pandas_df.index.tz_convert(DEFAULT_PYTZ)
        # ... fallback cases ...

    return self._pandas_df
```

### 4. Added Timezone Tests
**File**: `tests/backtest/test_return_type_backwards_compatibility.py`

**Added**:
- `test_timezone_correctness_pandas_return()`: Verifies pandas and polars timestamps match
- `test_timezone_correctness_polars_return()`: Verifies polars DataFrame has UTC timezone

## Remaining Issue

Despite the fixes above, the parity test still fails with 4-hour offset. The test script proves the conversion logic works, so the bug must be in:

### Possible Causes

1. **Bars Class Processing**: The Bars class might be doing additional processing that loses or mishandles timezone
2. **Backtesting Layer**: The backtesting class might be manipulating the DataFrame before passing to Bars
3. **Hidden Conversion**: There might be an intermediate step that's not logged where timezone is lost
4. **Timezone Comparison Issue**: The `tzinfo != DEFAULT_PYTZ` comparison might not work as expected

### Debug Steps Needed

1. **Add detailed logging** to track timezone at each step:
   - After DataBento fetch (should be UTC)
   - After polars conversion (should be UTC)
   - After DataPolars.df conversion (should be America/New_York)
   - When Bars receives DataFrame (should be America/New_York)

2. **Check Bars class __init__**: See if it's resetting index or re-localizing timezone

3. **Check if there are multiple code paths**: The backtesting class might have different paths for pandas vs polars

4. **Verify polars dtype**: Check if `Datetime(time_unit='ns', time_zone='UTC')` is exactly what we expect

## Action Plan

### Immediate (Must Fix Before Proceeding)
- [ ] Add detailed timezone logging at each conversion step
- [ ] Find exact location where timezone becomes incorrect
- [ ] Apply targeted fix

### Testing
- [ ] Run parity test until it passes
- [ ] Run timezone correctness tests
- [ ] Run backwards compatibility tests

### Performance Validation
- [ ] Once timezone fixed, re-run performance profiler
- [ ] Verify 1.8x speedup is maintained
- [ ] Proceed with strategy-level Polars experiment

## Files Modified

1. `lumibot/tools/databento_helper_polars.py` - Timezone preservation and index naming
2. `lumibot/entities/data_polars.py` - Enhanced timezone conversion logic
3. `tests/backtest/test_return_type_backwards_compatibility.py` - Added timezone tests
4. `test_timezone_debug.py` - Debug script (proves conversion logic works)

## RESOLUTION (October 15, 2025)

### Root Cause Identified

**File**: `lumibot/entities/data_polars.py:135-138`

The bug was in `DataPolars.__init__()` where a `.cast(pl.Datetime)` operation was **stripping the timezone** from incoming polars DataFrames:

```python
# BEFORE (BUG):
if self.polars_df["datetime"].dtype != pl.Datetime:
    self.polars_df = self.polars_df.with_columns(
        pl.col("datetime").cast(pl.Datetime)  # Strips timezone!
    )
```

When DataBento data arrived with `Datetime("ns", "UTC")`, this cast removed the timezone, creating naive timestamps. Later, `Bars._apply_timezone()` would localize instead of convert, causing the 4-hour offset.

### The Fix

**File**: `lumibot/entities/data_polars.py:135-145`

Preserve timezone during cast by checking if it exists first:

```python
# AFTER (FIX):
dtype = self.polars_df.schema["datetime"]
if isinstance(dtype, pl.datatypes.Datetime) and dtype.time_zone:
    # Column already has timezone, preserve it during cast
    desired = pl.datatypes.Datetime(time_unit=dtype.time_unit, time_zone=dtype.time_zone)
    self.polars_df = self.polars_df.with_columns(pl.col("datetime").cast(desired))
elif self.polars_df["datetime"].dtype != pl.Datetime:
    # No timezone, cast to naive datetime
    self.polars_df = self.polars_df.with_columns(
        pl.col("datetime").cast(pl.Datetime(time_unit="ns"))
    )
```

### Additional Fixes

1. **bars.py:301** - Changed `replace_time_zone()` to `convert_time_zone()` for proper timestamp conversion
2. **Removed temporary pandas filtering fallback** - Reverted to polars filtering for performance
3. **Cleaned up excessive [TZ_DEBUG] logging** - Reduced noise in backtest logs

### Test Results

✅ **test_databento_price_parity** - PASSED
✅ Timezone now correctly converts UTC → America/New_York
✅ Polars optimization maintained (no performance regression)

### Lessons Learned

- **Always preserve timezone information** through the entire data pipeline
- **Check incoming data types** before blindly casting them
- **Follow pandas' approach**: Keep timezone throughout, align comparison values to match
- **Polars `.cast()` behavior**: Casting to `pl.Datetime` without parameters strips timezone

### Files Modified (Final)

1. `lumibot/entities/data_polars.py` - Fixed timezone-stripping cast (ROOT CAUSE)
2. `lumibot/entities/bars.py` - Fixed replace_time_zone → convert_time_zone
3. `lumibot/tools/databento_helper_polars.py` - Reverted temporary pandas fallback, cleaned logging
4. `lumibot/entities/data_polars.py` - Cleaned excessive timezone logging

## Polars Filtering Precision Fix (October 15, 2025)

### Additional Issue Discovered
After fixing the main timezone bug, discovered that `return_polars=True` was still failing due to precision mismatch in polars filtering.

**Error**:
```
could not evaluate '>=' comparison between series 'datetime' of dtype: datetime[ns, UTC]
and series 'literal' of dtype: datetime[μs]
```

### Root Cause
**File**: `lumibot/tools/databento_helper_polars.py:633, 645`

When creating polars literals for datetime comparisons, `pl.lit(pandas_timestamp)` was creating literals with microsecond precision instead of nanosecond precision, causing type mismatch with the polars DataFrame column which has nanosecond precision.

### The Fix
Cast the literals to match the exact dtype of the column (including precision and timezone):

```python
# Get the datetime column dtype to match precision and timezone
datetime_dtype = df_polars[original_index_name].dtype

# Cast the literal to match the column's exact dtype (precision + timezone)
cond &= pl.col(original_index_name) >= pl.lit(start_aligned).cast(datetime_dtype)
cond &= pl.col(original_index_name) < pl.lit(end_aligned).cast(datetime_dtype)
```

### Test Results
✅ **test_timezone_correctness_polars_return** - PASSED
✅ **test_timezone_correctness_pandas_return** - PASSED
✅ **return_polars=True** optimization path - UNBLOCKED

### Performance Results (With All Fixes)
**Profiling with warm caches:**
- Pandas mode: 90.10s
- Polars mode: 50.13s
- **Speedup: 1.80x** (maintained - no performance regression)

## Previous Impact (NOW RESOLVED)

**Can now proceed** with:
- ✅ Parity verification (test passing)
- ✅ Performance profiling (results valid)
- ✅ Strategy-level Polars experiment (correctness verified)
- ✅ return_polars=True optimization (precision fix applied)

**BLOCKER REMOVED** - Can continue with optimization work.
