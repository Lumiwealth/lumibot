# Phase 4B: Polars Filtering Optimization - FINDINGS

## Date: October 15, 2025

## Objective
Optimize DataBento backtesting performance by using polars for datetime filtering operations while keeping pandas storage for compatibility.

## Approach Tested
**Strategy**: Use polars ONLY for the expensive filtering operation (`_filter_front_month_rows_polars`), then convert back to pandas once.

### Implementation Details
1. Created `databento_helper_polars.py` as a full copy of `databento_helper.py`
2. Implemented `_filter_front_month_rows_polars()` function:
   - Converts pandas DataFrame → polars DataFrame
   - Uses polars filter expressions for datetime comparisons
   - Converts polars DataFrame → pandas DataFrame
3. Updated call site to use polars filtering

### Code Location
- File: `lumibot/tools/databento_helper_polars.py`
- Function: `_filter_front_month_rows_polars()` (lines 592-657)
- Called from: `get_price_data_from_databento()` (line 993)

## Performance Results

### Test Configuration
- Strategy: MES Momentum SMA-9 (realistic futures strategy)
- Period: January 3-5, 2024 (3 trading days)
- Data: 1-minute bars
- Contract: MES continuous futures (with roll scheduling)

### Benchmark Results

| Metric | Pandas | Polars | Change |
|--------|--------|--------|--------|
| **Elapsed Time** | 86.74s | 92.41s | +5.67s |
| **Speedup** | 1.00x | **0.94x** | **-6% SLOWER** |

## Analysis

### Why It Failed

1. **Conversion Overhead Too High**
   - `pl.from_pandas(df.reset_index())` - pandas→polars conversion
   - `filtered_polars.to_pandas()` - polars→pandas conversion
   - `set_index()` - restore index after conversion
   - This overhead occurs on EVERY contract roll

2. **Low Call Frequency**
   - Filtering is called only a few times per backtest (once per contract roll)
   - Not called frequently enough to amortize conversion costs
   - Unlike the DatetimeArray iterations that happen thousands of times

3. **Already Well-Optimized**
   - DataBento backtesting uses extensive caching
   - Most data is cached and not re-filtered
   - The actual filtering time is small compared to conversion overhead

### Comparison with Phase 4A

| Phase | Approach | Result |
|-------|----------|--------|
| **Phase 4A** | Boundary conversion (store as polars) | **-9% slower** |
| **Phase 4B** | Filtering-only polars optimization | **-6% slower** |

Both approaches failed due to conversion overhead.

## Key Learnings

1. **Conversion Overhead Dominates**
   - For DataBento backtesting, the conversion cost pandas↔polars is MORE expensive than any potential polars speedup
   - This is true even when targeting specific bottleneck operations

2. **Frequency Matters**
   - Polars optimizations only pay off when operations are called MANY times
   - Contract roll filtering happens too infrequently to benefit

3. **Existing Optimizations Work**
   - The current pandas-based implementation with caching is already well-optimized
   - No low-hanging fruit for polars conversion

## Recommendations

### What NOT to Do
- ❌ Don't add polars conversions in low-frequency operations
- ❌ Don't store data as polars if it will be immediately converted back
- ❌ Don't optimize operations that are already cached

### Alternative Approaches to Explore

1. **Reduce DataBento API Calls**
   - Profile API call frequency and caching effectiveness
   - Improve cache hit rates

2. **Optimize Strategy Logic**
   - The bottleneck may be in the strategy's pandas operations (`.rolling()`, `.iloc[]`)
   - Consider strategy-level optimizations instead of data source level

3. **Parallel Data Loading**
   - Load data for multiple contracts concurrently
   - Use threading or multiprocessing for API calls

4. **Accept Current Performance**
   - 85-95s for a 3-day backtest with minute data is already fast
   - Further optimization may not be worth the complexity

## Conclusion

**Phase 4B FAILED**: Polars filtering optimization made performance 6% SLOWER due to conversion overhead negating any filtering speedup.

**Overall Phase 4 Status**: Both Phase 4A and Phase 4B approaches have been tested and both degraded performance. The pandas-based implementation remains the best option for DataBento backtesting.

## Next Steps

1. Mark Phase 4B as completed (failed)
2. Revert polars filtering changes to restore pandas performance
3. Document that polars optimization is not beneficial for DataBento backtesting
4. Consider alternative optimization strategies OR accept current performance as sufficient
