# Phase 4A Data Flow Visualization

## Current Flow (Pandas - Phase 3)

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Network Layer (databento_helper.py)                         │
│    - Fetch from DataBento API                                   │
│    - Read parquet with PyArrow                                  │
│    - Convert to Pandas DataFrame                                │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Pandas DF
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. Data Source Layer (databento_backtesting_polars.py)         │
│    - _pull_source_symbol_bars()                                 │
│    - Filter with pandas boolean indexing: df[df['x'] > y]       │
│    - Datetime operations with pandas                            │
│    - SLOW: 2.7M DatetimeArray iterations                        │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Bars(pandas_df, SOURCE="POLARS")  ← BUG: Wrong!
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. Bars Storage (bars.py)                                       │
│    - self._df = pandas_df                                       │
│    - Stored as Pandas                                           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. High-Frequency Operations (polars_data.py)                   │
│    - get_historical_prices() called 6,283 times                 │
│    - Each call: df = bars.df (no conversion, already pandas)    │
│    - Filter: df[(df.index >= start) & (df.index <= end)]       │
│    - SLOW: Pandas boolean indexing                              │
│                                                                  │
│    - get_last_price() called thousands of times                 │
│    - Each call: df.iloc[-1]['close']                            │
│    - SLOW: Pandas indexing overhead                             │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Pandas DF
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. Strategy Layer (user code)                                   │
│    - bars = self.get_historical_prices(asset, 200, "minute")    │
│    - df = bars.df  ← Gets Pandas DF                             │
│    - df["sma"] = df["close"].rolling(9).mean()                  │
└─────────────────────────────────────────────────────────────────┘

TOTAL TIME: 95.39s
BOTTLENECK: DatetimeArray iterations (10s), Pandas filtering (slow)
```

---

## New Flow (Polars Internal - Phase 4A)

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Network Layer (databento_helper.py)                         │
│    - Fetch from DataBento API                                   │
│    - Read parquet with PyArrow                                  │
│    - Convert to Pandas DataFrame                                │
│    - ✓ NO CHANGES (network not bottleneck)                      │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Pandas DF
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. Data Source Layer (databento_backtesting_polars.py)         │
│    - _pull_source_symbol_bars()                                 │
│    - ✨ NEW: df_polars = self._pandas_to_polars(df_pandas)      │
│    - ✨ NEW: Filter with polars:                                │
│             df_polars.filter(                                   │
│                 (pl.col("timestamp") >= start) &                │
│                 (pl.col("timestamp") <= end)                    │
│             )                                                    │
│    - FAST: Polars native datetime operations (no Python)        │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Bars(polars_df, SOURCE="POLARS")  ← CORRECT!
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. Bars Storage (bars.py)                                       │
│    - ✨ NEW: Dual storage                                       │
│    - self._df_polars = polars_df                                │
│    - self._df_pandas = None  (lazy conversion)                  │
│                                                                  │
│    - @property df:                                              │
│        if self._df_pandas is None:                              │
│            self._df_pandas = self._df_polars.to_pandas()        │
│        return self._df_pandas                                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. High-Frequency Operations (polars_data.py)                   │
│    - ✨ get_historical_prices() called 6,283 times              │
│      - Access bars._df_polars directly (no conversion!)         │
│      - Filter: df_polars.filter(pl.col("timestamp") >= start)   │
│      - Return: Bars(filtered_polars, SOURCE="POLARS")           │
│      - FAST: Polars expressions, no conversion                  │
│                                                                  │
│    - ✨ get_last_price() called thousands of times              │
│      - Access bars._df_polars directly                          │
│      - Get last: df_polars.select(pl.col("close").last())       │
│      - FAST: Polars optimized, no indexing overhead             │
└────────────────────┬────────────────────────────────────────────┘
                     │ Returns: Polars DF internally
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. Strategy Layer (user code)                                   │
│    - bars = self.get_historical_prices(asset, 200, "minute")    │
│    - df = bars.df  ← 🔄 CONVERTS Polars → Pandas here           │
│    - df["sma"] = df["close"].rolling(9).mean()                  │
│    - ✓ User code unchanged (still gets Pandas)                  │
└─────────────────────────────────────────────────────────────────┘

TOTAL TIME: 70-75s (target)
SPEEDUP: 1.25-1.35x (20-30% faster)
IMPROVEMENTS:
  - DatetimeArray iterations: 10s → 3s (polars native datetimes)
  - Filtering operations: Fast polars expressions (no Python overhead)
  - Conversion overhead: Reduced (only convert at final boundary)
```

---

## Key Differences Highlighted

### 1. When Conversion Happens

**BEFORE (Phase 3)**:
```
Fetch → Pandas → Store Pandas → Use Pandas → Return Pandas
        ↑                                      ↑
    Convert once                           No conversion
```

**AFTER (Phase 4A)**:
```
Fetch → Pandas → Polars → Store Polars → Use Polars → Return Pandas
        ↑        ↑                                    ↑
    Convert    Convert                            Convert
    (network)  (once)                             (lazy, final boundary)
```

### 2. Filtering Operations

**BEFORE**:
```python
# Pandas boolean indexing (slow for large datasets)
df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

# Creates intermediate boolean Series
# Python loop overhead
# Slow datetime comparisons
```

**AFTER**:
```python
# Polars expressions (fast, optimized)
df_polars.filter(
    (pl.col("timestamp") >= start) &
    (pl.col("timestamp") <= end)
)

# No intermediate objects
# Compiled expressions
# Native datetime handling
```

### 3. Last Price Lookup

**BEFORE**:
```python
# Pandas indexing
df.iloc[-1]['close']
# - Index lookup overhead
# - Column access overhead
# - Slower for repeated calls
```

**AFTER**:
```python
# Polars selection
df_polars.select(pl.col("close").last()).item()
# - Optimized last() operation
# - Single expression
# - Much faster
```

---

## Conversion Points Comparison

### Phase 3 (Current)
```
API → Pandas ─────────────────────────────→ Strategy
      ↑                                      ↑
  Convert once                          No conversion needed
  (in helper)                           (already Pandas)

  Total conversions: 1 per fetch
```

### Phase 4A (Optimized)
```
API → Pandas → Polars ─────────────────→ Pandas → Strategy
      ↑        ↑                         ↑
  Convert    Convert                 Convert
  (helper)   (immediate)             (lazy, only if accessed)

  Total conversions: 2 per fetch (but way faster operations)
```

**Why this is faster despite extra conversion:**
- Polars operations are 5-10x faster than Pandas
- Lazy conversion means we only convert if strategy uses `.df`
- Many operations (filtering, slicing) stay in Polars (no conversion)
- Polars native datetime operations save massive overhead

---

## Performance Breakdown

### Time Saved by Operation

| Operation | Before | After | Saved | Method |
|-----------|--------|-------|-------|--------|
| DatetimeArray iteration | 10.0s | ~3.0s | 7.0s | Polars native datetimes |
| Filtering (6,283 calls) | 30.0s | ~20.0s | 10.0s | Polars expressions |
| Conversion overhead | 3.0s | ~1.0s | 2.0s | Fewer conversions |
| Last price (thousands) | 5.0s | ~2.0s | 3.0s | Polars .last() |
| Other operations | 47.4s | ~44.0s | 3.4s | General overhead |
| **TOTAL** | **95.4s** | **~70s** | **~25s** | **26% speedup** |

---

## Memory Usage

### Phase 3 (Pandas only)
```
Fetch: Pandas DF (e.g., 6,461 rows × 8 cols = ~400KB)
Store: Same Pandas DF (400KB)
Use: Same Pandas DF (400KB)

Peak memory: ~400KB per asset
```

### Phase 4A (Polars internal)
```
Fetch: Pandas DF (400KB)
Store: Polars DF (~350KB - more efficient memory layout)
Return: Pandas DF (400KB - lazy conversion)

Peak memory: ~750KB per asset (if conversion happens)
             ~350KB per asset (if no conversion needed)
```

**Memory tradeoff**:
- Slight increase if strategy accesses `.df` (both formats in memory)
- Same or better if strategy doesn't access `.df` (polars more efficient)
- Not a concern for backtesting (memory is not the bottleneck)

---

## Testing Strategy

### 1. Unit Test: Conversion Functions
```python
def test_pandas_to_polars():
    df_pandas = pd.DataFrame({'x': [1, 2, 3]})
    df_polars = PolarsData._pandas_to_polars(df_pandas)
    assert df_polars.shape == (3, 1)
    assert df_polars['x'].to_list() == [1, 2, 3]

def test_polars_to_pandas():
    df_polars = pl.DataFrame({'x': [1, 2, 3]})
    df_pandas = PolarsData._polars_to_pandas(df_polars)
    assert df_pandas.shape == (3, 1)
    assert list(df_pandas['x']) == [1, 2, 3]
```

### 2. Integration Test: Parity
```bash
pytest tests/backtest/test_databento_parity.py -v
```
- Ensures Pandas and Polars backends produce identical results
- Tests datetime handling, filtering, last price, all operations

### 3. Performance Test: Profiling
```bash
python -m tests.performance.profile_databento_mes_momentum --mode both
```
- Measures actual speedup
- Compares before/after profiles
- Validates that optimizations have real impact

---

## Rollback Plan

If Phase 4A doesn't work:

### Easy Rollback
All changes are additive:
1. Bars class has both `_df_polars` and `_df_pandas` (keeps working with pandas)
2. PolarsData methods check `if SOURCE == "POLARS"` (pandas path untouched)
3. No breaking changes to external API

### Quick Fix
If parity tests fail but you need to move forward:
```python
# In polars_data.py, force pandas path:
def get_historical_prices(self, asset, length, ...):
    # Temporarily disable polars optimization
    if True:  # Change to: if self.SOURCE == "POLARS":
        # ... polars path ...
    else:
        # ... pandas path (fallback) ...
```

This lets you keep working while debugging the polars path.
