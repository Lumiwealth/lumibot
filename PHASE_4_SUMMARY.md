# Phase 4: Polars Optimization - COMPREHENSIVE SUMMARY

## Date: October 15, 2025

## Executive Summary

**Objective**: Optimize DataBento backtesting performance using polars DataFrames instead of pandas.

**Result**: All three polars optimization approaches (4A, 4B, 4C) **degraded performance** by 6-18%. The pandas-based implementation remains the best option.

**Conclusion**: Polars conversion overhead dominates any potential speedup. The pandas-based DataBento implementation should be used for production.

---

## Performance Results Overview

| Approach | Elapsed Time | vs Baseline | Details |
|----------|--------------|-------------|---------|
| **Pandas Baseline** | 87.86s | 1.00x | Pure pandas implementation |
| **Phase 4A** | 95.64s | 0.92x | **-9% slower** - Boundary conversion |
| **Phase 4B** | 92.41s | 0.95x | **-6% slower** - Filtering-only polars |
| **Phase 4C** | 103.96s | 0.85x | **-18% slower** - End-to-end DataPolars |

**Winner**: Pandas baseline (87.86s)

---

## Phase 4A: Boundary Conversion

### Approach
Store data as polars internally in Data class, convert to pandas on demand via `.df` property.

### Implementation
- Modified Data class to accept polars DataFrames
- Added `.polars_df` and `.df` properties
- Convert at boundaries when strategy requests data

### Results
- **Elapsed**: 95.64s (pandas: 87.86s)
- **Performance**: -9% slower
- **Reason**: Conversion overhead on EVERY property access

### Key Finding
Lazy property conversion doesn't help when property is accessed frequently (every iteration).

---

## Phase 4B: Filtering-Only Polars

### Approach
Use polars ONLY for the expensive filtering operation `_filter_front_month_rows_polars()`, convert back to pandas once.

### Implementation
- Created `databento_helper_polars.py` with polars filtering
- Function: `_filter_front_month_rows_polars()` (lines 592-657)
- Called from: `get_price_data_from_databento()` (line 993)

### Results
- **Elapsed**: 92.41s (pandas: 87.86s)
- **Performance**: -6% slower
- **Reason**: Conversion overhead on every contract roll

### Key Finding
Filtering happens too infrequently (once per contract roll) to amortize conversion costs.

---

## Phase 4C: End-to-End DataPolars Storage

### Approach
Create dedicated `DataPolars` class to store polars end-to-end, only convert when strategy needs data.

### Implementation
1. Created `lumibot/entities/data_polars.py` - New DataPolars class
2. Modified `databento_helper_polars.py` to ensure datetime column naming
3. Updated `databento_backtesting_polars.py` to use DataPolars

### Results
- **Elapsed**: 103.96s (pandas: 87.86s)
- **Performance**: -18% slower (WORST result)
- **Reason**: DataPolars class overhead + conversion still happens

### Key Finding
Storing as polars doesn't help when strategy immediately converts to pandas for operations.

---

## Root Cause Analysis

### Why All Approaches Failed

1. **Conversion Overhead Dominates**
   ```
   pandas ↔ polars conversion time > any polars speedup
   ```

2. **Strategy Uses Pandas Operations**
   ```python
   # Strategy code uses pandas:
   df["sma"] = df["close"].rolling(window=9).mean()  # pandas operation
   df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)  # pandas operation
   ```
   - `.rolling()`, `.shift()`, `.iloc[]` are all pandas
   - Polars storage doesn't help pandas operations!

3. **Low Operation Frequency**
   - Filtering happens once per contract roll (~few times per backtest)
   - Not frequent enough to justify conversion overhead
   - High-frequency operations (strategy logic) already use pandas

4. **Caching Already Effective**
   - DataBento backtesting uses extensive caching
   - Most data is cached and reused
   - Little opportunity for additional optimization

### The Fundamental Problem

```
┌─────────────────────────────────────────────────────────────┐
│ CURRENT FLOW (Pandas - 87.86s)                             │
├─────────────────────────────────────────────────────────────┤
│ DataBento API → pandas DataFrame → Data class → Strategy    │
│                                                              │
│ ✓ No conversions                                            │
│ ✓ Direct pandas operations                                  │
│ ✓ Minimal overhead                                          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ PHASE 4C FLOW (Polars - 103.96s)                           │
├─────────────────────────────────────────────────────────────┤
│ DataBento API → pandas → polars → DataPolars →              │
│                          (convert) → pandas → Strategy       │
│                                                              │
│ ✗ Conversion: pandas → polars (+overhead)                   │
│ ✗ Storage: DataPolars class (+overhead)                     │
│ ✗ Conversion: polars → pandas (+overhead)                   │
│ ✗ Same pandas operations (+no benefit)                      │
└─────────────────────────────────────────────────────────────┘
```

**Result**: More overhead, same operations, worse performance.

---

## What Would Actually Work?

### Option 1: Full Polars Strategy Rewrite
```python
# Instead of pandas strategy:
df["sma"] = df["close"].rolling(window=9).mean()

# Rewrite with polars:
df = df.with_columns([
    pl.col("close").rolling_mean(window_size=9).alias("sma")
])
```

**Pros**: Could potentially be faster
**Cons**:
- Requires complete strategy rewrite
- Polars API different from pandas
- May not actually be faster for these operations
- Not worth the effort for 87s baseline

### Option 2: Optimize Strategy Logic
- Cache indicator calculations
- Avoid redundant DataFrame operations
- Use vectorized operations
- Profile strategy code, not data source

**Pros**: Targets actual bottleneck
**Cons**: Still may have limited gains

### Option 3: Parallel Backtesting
- Run multiple date ranges in parallel
- Use multiprocessing for parameter sweeps
- Much better ROI than polars conversion

**Pros**: Actual speedup for common use case
**Cons**: More complex implementation

### Option 4: Accept Current Performance ✓ **RECOMMENDED**
- 87s for 3-day minute-bar backtest is already fast
- Handles continuous futures with roll scheduling
- Effective caching
- Reasonable for production use

---

## Lessons Learned

### 1. **Measure Before Optimizing**
✓ We profiled first (Phase 3)
✓ We identified apparent bottlenecks (DatetimeArray iteration)
✓ We tested hypotheses (Phase 4A/B/C)
✗ The "bottleneck" wasn't the real problem!

**Lesson**: Profiler can mislead. DatetimeArray iteration was visible but not the actual bottleneck.

### 2. **Consider the Whole Pipeline**
✗ Optimized data storage
✓ But strategy operations use pandas
✓ Conversion negates any gains

**Lesson**: Optimize the right part. If strategy uses pandas, keep data in pandas.

### 3. **Conversion Overhead Is Real**
Every pandas ↔ polars conversion costs time:
- Memory allocation
- Data copy
- Type conversion
- Index handling

**Lesson**: Minimize conversions. If you must convert, do it once and keep it.

### 4. **Abstraction Has Cost**
Every layer of abstraction (class, property, type check) adds overhead.

**Lesson**: Only add abstraction if it provides measurable benefit.

### 5. **Not All Bottlenecks Are Equal**
- Low-frequency operations (filtering): Not worth optimizing
- High-frequency operations (strategy logic): Worth targeting
- Cached operations: Already optimized

**Lesson**: Profile operation frequency, not just time.

---

## Recommendations

### Production Use
✅ **Use pandas-based DataBento implementation**
- File: `databento_backtesting.py` (not `databento_backtesting_polars.py`)
- Performance: 87.86s baseline
- Reliable, well-tested, no overhead

### Code Cleanup Options

#### Option A: Delete Polars Code (Clean Slate)
```bash
rm lumibot/backtesting/databento_backtesting_polars.py
rm lumibot/tools/databento_helper_polars.py
rm lumibot/entities/data_polars.py
rm PHASE_4*_FINDINGS.md
```
**Pros**: Clean codebase
**Cons**: Lose reference implementation

#### Option B: Keep for Reference (Recommended)
- Keep files but don't use in production
- Add comments: "⚠️ DEPRECATED - Polars optimization attempted, 18% slower. See PHASE_4_SUMMARY.md"
- Useful for future reference

**Pros**: Historical record, future reference
**Cons**: Extra files in codebase

#### Option C: Archive in Separate Branch
```bash
git checkout -b archive/polars-optimization-attempt
git add PHASE_4*.md lumibot/*polars*
git commit -m "Archive polars optimization attempt (Phase 4A/B/C)"
git checkout main
# Remove polars files from main
```

**Pros**: Clean main branch, preserved history
**Cons**: Extra branch to maintain

### Future Optimization Efforts

If you want to optimize DataBento backtesting in the future:

1. **Profile Strategy Code**
   - Measure time in `.rolling()`, `.shift()`, `.iloc[]`
   - Identify actual strategy bottlenecks
   - Cache calculations if possible

2. **Optimize High-Frequency Operations**
   - Focus on operations that happen every iteration
   - Not one-time setup or rare operations

3. **Consider Parallelization**
   - Backtest multiple periods concurrently
   - Parameter sweeps with multiprocessing
   - Better ROI than micro-optimizations

4. **Benchmark Alternative Approaches**
   - Test polars for specific operations
   - Measure before committing to rewrite
   - Consider effort vs. benefit

### Documentation
Add to `databento_backtesting.py` docstring:

```python
"""
DataBento backtesting data source.

NOTE: Polars optimization was attempted (Phase 4A/B/C) but all approaches
degraded performance by 6-18% due to conversion overhead. This pandas-based
implementation is the optimal choice. See PHASE_4_SUMMARY.md for details.
"""
```

---

## Final Verdict

**Polars optimization for DataBento backtesting**: ❌ **NOT RECOMMENDED**

**Reason**: Conversion overhead (6-18% slower) outweighs any potential gains. Strategy uses pandas operations, so keeping data in pandas is most efficient.

**Action**: Use pandas-based implementation (`databento_backtesting.py`) for production. Keep polars files for reference or archive them.

**Performance**: 87.86s for 3-day minute-bar backtest is already fast enough for most use cases.

---

## Files Modified During Phase 4

### New Files Created
1. `lumibot/backtesting/databento_backtesting_polars.py` - Polars backtesting (slower)
2. `lumibot/tools/databento_helper_polars.py` - Polars helpers (slower)
3. `lumibot/entities/data_polars.py` - DataPolars class (Phase 4C, slowest)
4. `PHASE_4A_FINDINGS.md` - Phase 4A documentation
5. `PHASE_4B_FINDINGS.md` - Phase 4B documentation
6. `PHASE_4C_FINDINGS.md` - Phase 4C documentation
7. `PHASE_4_SUMMARY.md` - This comprehensive summary

### Existing Files Modified
1. `tests/performance/profile_databento_mes_momentum.py` - Profiling script

### Test Files
1. `tests/backtest/test_databento_accuracy_quick.py` - Accuracy tests (still passing)

---

## Acknowledgments

This optimization effort followed proper engineering practices:
- ✓ Established baseline (Phase 3)
- ✓ Profiled to find bottlenecks
- ✓ Tested hypotheses (Phase 4A/B/C)
- ✓ Measured results objectively
- ✓ Documented findings
- ✓ Made evidence-based recommendations

While the optimization didn't improve performance, the process was valuable for understanding the system and ruling out ineffective approaches.

**Lesson**: Not all optimization attempts succeed, but measuring and documenting failures is just as valuable as successes.
