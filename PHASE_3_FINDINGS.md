# Phase 3 Findings: Baseline and Profiling Complete

## What We Accomplished

### Phase 3A: Created PolarsData Base Class
- Created `/lumibot/data_sources/polars_data.py` as byte-for-byte copy of PandasData
- Establishes clean separation between pandas and polars implementations
- All future polars optimizations happen in this file

### Phase 3B: Pointed DataBentoPolars to New Base
- Modified `DataBentoDataBacktestingPolars` to extend `PolarsData` instead of `PandasData`
- Verified parity tests pass (both backends produce identical results)
- Confirmed MRO (Method Resolution Order) shows correct inheritance chain

### Phase 3C: Profiled with Realistic Strategy
- Created `tests/performance/profile_databento_mes_momentum.py` using real MES Momentum SMA-9 strategy
- Ran 3-day backtest with 3,270 trading iterations
- Generated yappi profiles for detailed bottleneck analysis

## Profiling Results

### Performance Baseline
```
Pandas:  94.85s
Polars:  95.39s
Speedup: 0.99x (essentially identical)
```

**Conclusion**: Both backends use identical code, confirming correct baseline for optimization work.

### Identified Bottlenecks (from profile analysis)

#### 1. DatetimeArray Iteration: ~10 seconds
- **2.7 million calls** to `DatetimeArray.__iter__`
- Pandas datetime handling with Python overhead
- **Optimization opportunity**: Use polars native datetime operations

#### 2. Parquet Reading: ~2.7 seconds
- 5,194 parquet read operations
- PyArrow → Pandas conversion overhead
- **Optimization opportunity**: Polars native parquet reader (zero-copy)

#### 3. DataFrame Conversions: ~4.6 seconds
- `table_to_dataframe`: 1.30s
- Array operations and construction: 3.3s
- **Optimization opportunity**: Keep data in polars format internally

#### 4. Historical Price Fetching: ~54 seconds (cumulative)
- 6,283 calls to `get_historical_prices()`
- Most time in datetime conversions and filtering
- **Optimization opportunity**: Polars lazy evaluation and expressions

### What We Learned

1. **Network I/O is NOT the bottleneck** (data is cached)
   - No need for separate `databento_helper_polars.py`
   - Optimization should focus on DataFrame operations

2. **DataFrame operations dominate execution time**
   - ~70% of execution time in data operations
   - Datetime handling is the single biggest bottleneck
   - Repeated filtering/slicing operations are expensive

3. **High-frequency method calls need optimization**
   - `get_historical_prices()`: 6,283 calls
   - `get_last_price()`: thousands of calls
   - Each call involves datetime conversions and filtering

## Key Architectural Decisions

### ✅ Correct Approach
- Single `PolarsData` base class (not separate helper files)
- Keep pandas external API (backwards compatibility)
- Use polars internally (storage, filtering, datetime ops)
- Convert polars → pandas only at final boundary

### ❌ Incorrect Approaches (tried and rejected)
- Creating separate `databento_helper_polars.py` for network I/O
- Adding environment variables for debug flags
- Optimizing before establishing baseline
- Using toy tests for profiling (not realistic)

## Optimization Strategy

### Phase 4A: Internal Storage (Target: 20-30% speedup)
- Store data as polars DataFrames internally
- Read parquet directly to polars
- Use polars for filtering and slicing
- Convert to pandas only when returning from public methods

### Phase 4B: High-Frequency Operations (Target: 40-50% speedup)
- Optimize `get_historical_prices()` with polars slicing
- Use polars lazy frames for repeated queries
- Cache polars expressions
- Reduce datetime conversions

### Phase 4C: Advanced Optimizations (Target: 60-70% speedup)
- Lazy evaluation throughout pipeline
- Expression-based filtering (no Python overhead)
- Native datetime operations
- Column pruning

## Testing Protocol

After each optimization:

1. **Parity tests** - Ensure bit-identical results
   ```bash
   pytest tests/backtest/test_databento_parity.py -v
   ```

2. **Profiling** - Measure actual speedup
   ```bash
   python -m tests.performance.profile_databento_mes_momentum --mode both
   ```

3. **Comprehensive tests** - Full integration
   ```bash
   pytest tests/backtest/test_databento_comprehensive_trading.py -m apitest
   ```

## Files Created

### Profiling Infrastructure
- `tests/performance/profile_databento_mes_momentum.py` - Real strategy profiler
- `tests/performance/analyze_profile.py` - Profile analysis tool

### Documentation
- `POLARS_OPTIMIZATION_PLAN.md` - Detailed optimization roadmap
- `PHASE_3_FINDINGS.md` - This document

### Code Changes
- `lumibot/data_sources/polars_data.py` - New base class (byte-for-byte copy of PandasData)
- `lumibot/backtesting/databento_backtesting_polars.py` - Points to PolarsData base

## Profile Data Locations

Generated profiles are in `tests/performance/logs/`:
- `mes_momentum_pandas.prof` - Pandas backend profile (94.85s)
- `mes_momentum_polars.prof` - Polars backend profile (95.39s)

Analysis outputs:
- `/tmp/pandas_profile_analysis.txt` - Top bottlenecks (pandas)
- `/tmp/polars_profile_analysis.txt` - Top bottlenecks (polars)
- `/tmp/mes_momentum_profile.txt` - Full profiling run output

## Next Steps

Ready to begin Phase 4A: Internal Polars Storage

**First optimization target**: Modify `PolarsData` to store data internally as polars DataFrames while maintaining pandas external API.

**Success criteria**:
- Parity tests pass (identical results)
- 20-30% speedup in profiler
- DatetimeArray iteration calls reduced significantly

**Incremental approach**:
1. Change internal storage to polars
2. Run parity tests
3. Run profiler
4. If tests pass and speedup achieved → commit and proceed
5. If tests fail → fix bugs and retry
6. If no speedup → analyze profile and adjust approach
