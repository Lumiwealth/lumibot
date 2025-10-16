# Polars Optimization Plan - Phase 3C Results

## Executive Summary

After profiling a realistic MES Momentum strategy over 3 trading days with 3270 iterations:
- **Pandas backend**: 94.85s
- **Polars backend**: 95.39s
- **Current speedup**: 0.99x (essentially identical)

Both backends currently use identical code from `PolarsData` base class (byte-for-byte copy of `PandasData`). This confirms our parity baseline is correct.

## Profiling Results - Top Bottlenecks

### 1. DatetimeArray Iteration: ~10s (2.7M calls)
**Current**: Pandas datetime handling with 2.7M iterator calls
- `datetimes.py:647(DatetimeArray.__iter__)`: 10.16s

**Opportunity**: Replace with polars datetime operations (lazy evaluation, native date types)

**Expected speedup**: 2-5x on datetime operations

### 2. Parquet Reading: ~2.7s (5,194 reads)
**Current**: PyArrow → Pandas conversion via `core.py:1473(ParquetDataset.read)`
- Parquet read: 2.69s
- `table_to_dataframe`: 1.30s

**Opportunity**: Use polars native parquet reader (zero-copy, lazy loading)

**Expected speedup**: 2-3x on I/O operations

### 3. DataFrame Operations: ~4.6s
**Current**: Pandas DataFrame construction and manipulation
- `pandas_compat.py:780(table_to_dataframe)`: 1.30s
- Array operations, take operations: ~3.3s

**Opportunity**: Replace with polars lazy operations and expression API

**Expected speedup**: 1.5-2x on filtering/slicing

### 4. Historical Price Fetching: 54.5s cumulative (6,283 calls)
**Current**: `get_historical_prices` → `_pull_source_symbol_bars`
- Most time spent in datetime conversions and filtering
- Repeated slicing operations on same dataset

**Opportunity**:
- Use polars for internal storage and filtering
- Lazy evaluation to avoid intermediate copies
- Convert to pandas only at final boundary

**Expected speedup**: 2-3x on data operations

## Optimization Strategy

### Phase 4A: Internal Storage Migration (Target: 20-30% speedup)

**Goal**: Keep pandas external API, use polars internally

**Files to modify**:
- `lumibot/data_sources/polars_data.py` - Change `_data_store` from pandas to polars

**Changes**:
1. Store data internally as polars DataFrames
2. Convert parquet → polars directly (skip pandas)
3. Use polars for filtering, slicing, datetime operations
4. Convert polars → pandas only when returning from public methods

**Expected result**: ~70-75s total time (20-25% speedup)

### Phase 4B: Optimize High-Frequency Operations (Target: 40-50% speedup)

**Focus on methods called thousands of times**:
- `get_historical_prices()` - 6,283 calls
- `get_last_price()` - thousands of calls
- `_get_bars_between_dates()` - filtering operations

**Optimizations**:
1. Use polars lazy frames for filtering
2. Cache polars expressions for repeated queries
3. Avoid datetime conversions in loops

**Expected result**: ~50-60s total time (40-45% speedup)

### Phase 4C: Advanced Optimizations (Target: 60-70% speedup)

**Advanced polars features**:
1. Lazy evaluation throughout pipeline
2. Expression-based filtering (no Python overhead)
3. Native datetime operations (no Python datetime objects)
4. Column pruning (only read needed columns)

**Expected result**: ~30-40s total time (60-70% speedup)

## Implementation Order

### Step 1: Modify PolarsData.__init__ to use polars storage
```python
class PolarsData(DataSourceBacktesting):
    def __init__(self, *args, pandas_data=None, **kwargs):
        super().__init__(*args, **kwargs)
        # Convert pandas_data to polars for internal storage
        if pandas_data:
            self._data_store = self._convert_pandas_to_polars_store(pandas_data)
        else:
            self._data_store = {}
```

### Step 2: Modify _pull_source_symbol_bars to return polars
```python
def _pull_source_symbol_bars(self, asset, ...):
    # Read parquet directly to polars
    df_polars = pl.read_parquet(cache_path)

    # Filter using polars (fast!)
    filtered = df_polars.filter(
        (pl.col("timestamp") >= start) &
        (pl.col("timestamp") <= end)
    )

    # Store as polars internally
    return Bars(filtered, self.SOURCE, ...)
```

### Step 3: Modify Bars class to store polars, convert on access
```python
class Bars:
    def __init__(self, df_polars, source, ...):
        self._df_polars = df_polars
        self._df_pandas = None  # Lazy conversion

    @property
    def df(self):
        """Convert to pandas only when accessed"""
        if self._df_pandas is None:
            self._df_pandas = self._df_polars.to_pandas()
        return self._df_pandas
```

### Step 4: Optimize high-frequency methods
- `get_historical_prices()` - use polars slicing
- `get_last_price()` - use polars `last()` method
- Date filtering - use polars filter expressions

### Step 5: Incremental testing
After each change:
1. Run parity tests to ensure identical results
2. Run profiler to measure speedup
3. Commit if tests pass and performance improves

## Success Metrics

### Phase 4A Success Criteria:
- [ ] Parity tests pass (bit-identical results)
- [ ] Profiler shows 20-30% speedup (70-75s vs 95s)
- [ ] DatetimeArray calls reduced significantly

### Phase 4B Success Criteria:
- [ ] Parity tests still pass
- [ ] 40-50% total speedup (50-60s)
- [ ] Parquet reading time cut in half

### Phase 4C Success Criteria:
- [ ] Parity tests still pass
- [ ] 60-70% total speedup (30-40s)
- [ ] Comprehensive tests pass

## Risk Mitigation

1. **Incremental changes** - One optimization at a time
2. **Test after each change** - Catch bugs early
3. **Keep pandas boundary** - External API unchanged
4. **Profiling validation** - Measure actual speedup, not guesses

## Files Requiring Changes

### Primary:
- `lumibot/data_sources/polars_data.py` - Main optimization target
- `lumibot/entities/bars.py` - Add polars storage support

### Secondary (if needed):
- `lumibot/backtesting/databento_backtesting_polars.py` - Polars-specific overrides
- Helper methods for polars ↔ pandas conversion

### Do NOT change:
- `lumibot/tools/databento_helper.py` - Network I/O is not the bottleneck
- External API signatures - Keep backwards compatibility
- Test files - They test behavior, not implementation

## Next Steps

1. Implement Phase 4A (internal polars storage)
2. Run profiler to measure actual gains
3. Proceed to Phase 4B if gains are significant
4. Document actual vs expected performance at each phase
