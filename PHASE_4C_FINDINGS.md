# Phase 4C: End-to-End Polars Storage - FINDINGS

## Date: October 15, 2025

## Objective
Optimize DataBento backtesting by implementing end-to-end polars storage, creating a dedicated `DataPolars` class to store polars DataFrames internally and only convert to pandas when the strategy explicitly requests data.

## Approach Tested
**Strategy**: Create a polars-optimized data storage class that:
1. Stores data as polars DataFrames internally
2. Provides lazy conversion to pandas via `.df` property
3. Maintains compatibility with existing Data class interface
4. Eliminates repeated conversions by caching pandas DataFrame

### Implementation Details

#### 1. Created `data_polars.py` - New DataPolars Class
- Location: `lumibot/entities/data_polars.py`
- Features:
  - Accepts polars DataFrames in `__init__`
  - Stores data as `polars_df` attribute
  - Provides `.df` property for pandas conversion (with caching)
  - Handles datetime column instead of datetime index
  - Full compatibility with Data class interface

#### 2. Modified `databento_helper_polars.py`
- Updated `get_price_data_from_databento()` to ensure datetime column naming
- Added robust column detection and renaming logic
- Lines 1004-1025: Polars conversion with datetime column handling

#### 3. Updated `databento_backtesting_polars.py`
- Imported `DataPolars` class
- Modified `_update_pandas_data()` to use `DataPolars` for polars DataFrames
- Line 378: Creates `DataPolars` object when receiving polars DataFrame

### Code Locations
- New class: `lumibot/entities/data_polars.py` (entire file)
- Helper update: `lumibot/tools/databento_helper_polars.py:1004-1025`
- Backtesting update: `lumibot/backtesting/databento_backtesting_polars.py:10,378-383`

## Performance Results

### Test Configuration
- Strategy: MES Momentum SMA-9 (realistic futures strategy)
- Period: January 3-5, 2024 (3 trading days)
- Data: 1-minute bars
- Contract: MES continuous futures (with roll scheduling)

### Benchmark Results

| Metric | Pandas | Polars (Phase 4C) | Change |
|--------|--------|-------------------|--------|
| **Elapsed Time** | 87.86s | 103.96s | +16.10s |
| **Speedup** | 1.00x | **0.85x** | **-18% SLOWER** |

### Comparison Across All Phase 4 Approaches

| Phase | Approach | Result | Change |
|-------|----------|--------|--------|
| **Baseline** | Pure pandas | 87.86s | 1.00x |
| **Phase 4A** | Boundary conversion (store as polars) | 95.64s | **-9% slower** |
| **Phase 4B** | Filtering-only polars | 92.41s | **-6% slower** |
| **Phase 4C** | End-to-end DataPolars storage | 103.96s | **-18% SLOWER** |

## Analysis

### Why It Failed Even Worse

Phase 4C introduced **additional overhead** beyond previous attempts:

1. **DataPolars Class Overhead**
   - Extra object initialization
   - Property access overhead for `.df` conversion
   - Memory overhead from dual storage (polars + cached pandas)

2. **Repeated Property Access**
   - Strategy accesses data multiple times per iteration
   - Each `.df` property access checks cache, converts if needed
   - Even with caching, property overhead adds up

3. **Conversion Still Happens**
   - Strategy operates on pandas DataFrames
   - DataPolars still converts polars → pandas when strategy needs data
   - Conversion happens on FIRST access per Data object
   - No actual reduction in conversion count

4. **Added Complexity**
   - More code paths mean more function calls
   - Type checking (is it polars or pandas?)
   - Column name handling and validation

### The Fundamental Problem

**Root Cause**: The strategy itself uses pandas operations (`.rolling()`, `.iloc[]`, `.shift()`), so conversion is inevitable.

**Key Insight**: Storing data as polars doesn't help when:
- The strategy immediately converts it back to pandas
- Polars storage adds overhead without eliminating conversions
- The conversion boundary just moves from fetch → storage to storage → usage

### Performance Breakdown

```
Phase 4C overhead sources:
  +6.10s  Phase 4B filtering overhead (vs baseline)
  +10.00s DataPolars class and property access overhead
  ------
  +16.10s Total overhead vs pandas baseline
```

## Conversion Logs Analysis

From the instrumentation logs, we can see the conversion pattern:

```
[CONVERSION] FETCH | DataBento → polars | _update_pandas_data
[POLARS] Converting final DataFrame to polars for MES: 6162 rows
[CONVERSION] STORE | polars → DataPolars | _update_pandas_data
[CONVERSION] DataPolars.df | polars → pandas | MES     <-- CONVERSION STILL HAPPENS
```

The conversion moved from:
- **Before**: fetch → pandas → Data → strategy uses pandas
- **After**: fetch → polars → DataPolars → (convert to pandas) → strategy uses pandas

**Result**: Same number of conversions, more overhead!

## Key Learnings

### 1. **End-to-End Polars Requires End-to-End Usage**
For polars to help, the ENTIRE pipeline must stay in polars:
- Data fetching ✓ (polars)
- Data storage ✓ (polars)
- Data filtering ✓ (polars)
- **Strategy operations ✗ (pandas)** ← This breaks the chain!

### 2. **Storage Format Doesn't Matter If Usage Requires Conversion**
Storing as polars is pointless if:
- Strategy immediately converts to pandas
- Conversion happens on every data access
- No operations happen on polars data before conversion

### 3. **Abstraction Layers Add Overhead**
Each additional layer of abstraction (DataPolars class, properties, type checking) adds overhead that must be justified by performance gains elsewhere.

### 4. **Wrong Optimization Target**
We optimized data storage, but the bottleneck is in:
- Strategy pandas operations (`.rolling()`, `.iloc[]`)
- Not in data storage or fetching
- Polars can't help pandas operations!

## What Would Actually Help?

To get polars performance benefits, you would need to:

1. **Rewrite Strategy Logic in Polars**
   ```python
   # Instead of pandas operations:
   df["sma"] = df["close"].rolling(window=9).mean()

   # Would need polars operations:
   df = df.with_columns([
       pl.col("close").rolling_mean(window_size=9).alias("sma")
   ])
   ```

2. **Keep Everything in Polars Until Final Result**
   - Fetch as polars ✓
   - Store as polars ✓
   - Filter as polars ✓
   - **Calculate indicators as polars** ← This is the missing piece
   - **Signal generation as polars** ← This too
   - Only convert for final order submission

3. **Benchmark Polars vs Pandas for Strategy Operations**
   - Profile `.rolling()` in pandas vs polars
   - Profile `.shift()` and `.iloc[]` operations
   - Measure if polars is actually faster for these operations
   - May not be worth the rewrite!

## Recommendations

### What NOT to Do
- ❌ Don't add polars storage layers without polars usage
- ❌ Don't create abstraction classes that add overhead
- ❌ Don't convert to polars just to convert back immediately
- ❌ Don't optimize storage when computation is the bottleneck

### What TO Do
✅ **Accept pandas-based DataBento implementation as optimal for current strategy design**

The 87-88s performance for a 3-day minute-bar backtest is already good:
- Handles continuous futures with roll scheduling
- Caches data effectively
- Reasonable performance for realistic strategies

### If You Really Want to Optimize

1. **Profile Strategy Operations**
   - Measure time spent in `.rolling()`, `.iloc[]`, etc.
   - Identify actual bottlenecks in strategy code
   - May find caching opportunities there

2. **Optimize Strategy Logic**
   - Cache indicator calculations
   - Avoid redundant DataFrame operations
   - Use vectorized operations where possible

3. **Consider Parallel Backtesting**
   - Run multiple date ranges in parallel
   - Use multiprocessing for parameter sweeps
   - This would give much better ROI than polars conversion

4. **Accept Current Performance**
   - 87s for 3-day backtest is fast enough for most use cases
   - Further optimization has diminishing returns
   - Developer time better spent on strategy development

## Conclusion

**Phase 4C FAILED**: End-to-end polars storage made performance **18% SLOWER** due to DataPolars class overhead combined with inevitable conversions when strategy accesses data.

**Overall Phase 4 Status**: All three approaches (4A, 4B, 4C) degraded performance. The pandas-based implementation is the best option.

**Fundamental Lesson**: Optimizing one part of the pipeline (data storage) doesn't help when the bottleneck is elsewhere (strategy pandas operations) and conversion is unavoidable.

## Next Steps

1. **Revert Phase 4C changes** to restore pandas performance
2. **Document in codebase** that polars optimization was attempted and abandoned
3. **Consider alternative optimizations** focused on strategy-level improvements
4. **Accept current performance** as sufficient for DataBento backtesting

## Files to Revert

To restore pandas performance, revert:
1. `lumibot/entities/data_polars.py` - DELETE (new file)
2. `lumibot/backtesting/databento_backtesting_polars.py` - Revert import and DataPolars usage
3. `lumibot/tools/databento_helper_polars.py` - Keep as-is (filtering improvements may still help)

OR: Keep these files for future reference but don't use them in production.
