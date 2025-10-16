# Phase 4A Quick Reference Card

## TL;DR

**What**: Store data as polars internally, convert to pandas only at boundary
**Why**: Polars operations are 5-10x faster than pandas
**Where**: 3 files, ~150 lines of code
**When**: 1-2 hours to implement
**Result**: 20-30% speedup (95s → 70-75s)

---

## The 5 Changes

### 1. Bars: Dual Storage
```python
# bars.py: Store polars internally, return pandas externally
if source == "POLARS":
    self._df_polars = df
    self._df_pandas = None  # Lazy conversion
```

### 2. PolarsData: Conversion Helpers
```python
# polars_data.py: Add conversion utilities
@staticmethod
def _pandas_to_polars(df_pandas):
    return pl.from_pandas(df_pandas)

@staticmethod
def _polars_to_pandas(df_polars):
    return df_polars.to_pandas()
```

### 3. get_historical_prices: Polars Filtering
```python
# polars_data.py: Use polars for filtering
if self.SOURCE == "POLARS":
    df_polars = bars._df_polars
    filtered = df_polars.filter(pl.col("timestamp") >= start)
    return Bars(filtered, self.SOURCE)
```

### 4. get_last_price: Polars Last
```python
# polars_data.py: Use polars .last()
if self.SOURCE == "POLARS":
    df_polars = bars._df_polars
    return df_polars.select(pl.col("close").last()).item()
```

### 5. _pull_source_symbol_bars: Convert Early
```python
# databento_backtesting_polars.py: Convert after fetch
df_pandas = databento_helper._load_from_cache(cache_path)
df_polars = self._pandas_to_polars(df_pandas)
filtered = df_polars.filter(pl.col("timestamp") >= start)
return Bars(filtered, self.SOURCE)
```

---

## Testing Commands

```bash
# 1. Baseline
python -m tests.performance.profile_databento_mes_momentum --mode both

# 2. Make changes (see above)

# 3. Test parity
pytest tests/backtest/test_databento_parity.py -v

# 4. Test comprehensive
pytest tests/backtest/test_databento_comprehensive_trading.py -m apitest

# 5. Profile performance
python -m tests.performance.profile_databento_mes_momentum --mode both

# 6. Analyze
python tests/performance/analyze_profile.py tests/performance/logs/mes_momentum_polars.prof
```

---

## Expected Results

### Before
```
MODE: POLARS
Elapsed time: 95.39s
Speedup: 0.99x (identical to pandas)
```

### After
```
MODE: POLARS
Elapsed time: 70-75s
Speedup: 1.25-1.35x
```

### Profile Changes
```
DatetimeArray.__iter__:  10s → 3s   (70% reduction)
Filtering operations:    30s → 20s  (33% reduction)
Conversion overhead:     3s  → 1s   (66% reduction)
```

---

## Key Concepts

### Polars Filtering
```python
# ❌ Pandas (slow)
df[(df['x'] >= start) & (df['x'] <= end)]

# ✅ Polars (fast)
df.filter((pl.col('x') >= start) & (pl.col('x') <= end))
```

### Polars Last
```python
# ❌ Pandas (slow)
df.iloc[-1]['close']

# ✅ Polars (fast)
df.select(pl.col('close').last()).item()
```

### Lazy Conversion
```python
# Only convert when accessed
@property
def df(self):
    if self._df_pandas is None:
        self._df_pandas = self._df_polars.to_pandas()
    return self._df_pandas
```

---

## Files to Change

✏️ `lumibot/entities/bars.py` (~30 lines)
✏️ `lumibot/data_sources/polars_data.py` (~100 lines)
✏️ `lumibot/backtesting/databento_backtesting_polars.py` (~20 lines)

❌ DON'T change `databento_helper.py` (network not bottleneck)

---

## Success Criteria

✅ Parity tests pass (identical results)
✅ 20-30% speedup in profiling
✅ DatetimeArray time reduced 70%+
✅ No breaking changes to external API

---

## Rollback

If tests fail, disable polars path:
```python
if False:  # Was: if self.SOURCE == "POLARS":
    # polars path
else:
    # pandas path (fallback)
```

---

## Next Phase

**Phase 4B**: Polars native parquet reader
- Skip pandas at network layer
- Additional 15-20% speedup
- Target: 70s → 50s
