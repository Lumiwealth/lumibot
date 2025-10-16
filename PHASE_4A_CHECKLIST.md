# Phase 4A Implementation Checklist

## Quick Reference

**Goal**: Internal polars storage, pandas external API
**Target**: 20-30% speedup (95s → 70-75s)
**Files**: 3 files, ~150 lines of code
**Time**: 1-2 hours to implement, 30 mins to test

---

## Pre-Implementation

### ✅ Prerequisites Verified
- [x] Phase 3 complete (baseline established)
- [x] Parity tests exist and pass
- [x] Profiling infrastructure works
- [x] Both backends currently identical (0.99x speedup)

### 📋 Documentation Created
- [x] `PHASE_4A_DETAILED_PLAN.md` - Overall strategy
- [x] `PHASE_4A_CODE_CHANGES.md` - Exact code changes
- [x] `PHASE_4A_DATAFLOW.md` - Visual data flow
- [x] `PHASE_4A_CHECKLIST.md` - This file

---

## Implementation Steps

### Step 1: Modify Bars Class (15 minutes)
**File**: `lumibot/entities/bars.py`

- [ ] Add `import polars as pl` at top of file
- [ ] Split `self._df` into `self._df_polars` and `self._df_pandas`
- [ ] Modify `__init__` to store polars when `SOURCE == "POLARS"`
- [ ] Modify `.df` property to convert polars → pandas lazily
- [ ] **Test**: `python -c "from lumibot.entities import Bars; print('OK')"`

**Lines changed**: ~30

**Verification**:
```bash
# Should import without error
python -c "from lumibot.entities import Bars; print('Bars import OK')"
```

---

### Step 2: Add Conversion Helpers to PolarsData (10 minutes)
**File**: `lumibot/data_sources/polars_data.py`

- [ ] Add `import polars as pl` at top of file
- [ ] Add `_pandas_to_polars()` static method after `__init__`
- [ ] Add `_polars_to_pandas()` static method after `_pandas_to_polars`
- [ ] **Test**: Import and call helpers

**Lines changed**: ~30

**Verification**:
```python
from lumibot.data_sources import PolarsData
import pandas as pd

df_pandas = pd.DataFrame({'x': [1, 2, 3]})
df_polars = PolarsData._pandas_to_polars(df_pandas)
print(f"Converted to polars: {df_polars.shape}")

df_back = PolarsData._polars_to_pandas(df_polars)
print(f"Converted back: {df_back.shape}")
```

---

### Step 3: Optimize get_historical_prices (20 minutes)
**File**: `lumibot/data_sources/polars_data.py`

- [ ] Find `get_historical_prices` method
- [ ] Add `if self.SOURCE == "POLARS":` branch
- [ ] Access `bars._df_polars` directly (no conversion)
- [ ] Use polars `.filter()` instead of pandas boolean indexing
- [ ] Keep pandas fallback in `else:` branch
- [ ] **Test**: Run parity test

**Lines changed**: ~25

**Verification**:
```bash
pytest tests/backtest/test_databento_parity.py::test_get_historical_prices_parity -v
```

**Expected**: ✅ PASS (Pandas and Polars produce identical results)

---

### Step 4: Optimize get_last_price (15 minutes)
**File**: `lumibot/data_sources/polars_data.py`

- [ ] Find `get_last_price` method
- [ ] Add `if self.SOURCE == "POLARS":` branch
- [ ] Access `bars._df_polars` directly
- [ ] Use polars `.select().last()` instead of pandas `.iloc[-1]`
- [ ] Keep pandas fallback in `else:` branch
- [ ] **Test**: Run parity test

**Lines changed**: ~20

**Verification**:
```bash
pytest tests/backtest/test_databento_parity.py::test_get_last_price_parity -v
```

**Expected**: ✅ PASS

---

### Step 5: Convert to Polars After Network Fetch (20 minutes)
**File**: `lumibot/backtesting/databento_backtesting_polars.py`

- [ ] Add `import polars as pl` at top of file
- [ ] Find `_pull_source_symbol_bars` method
- [ ] After `databento_helper._load_from_cache()`, convert to polars
- [ ] Replace pandas filtering with polars `.filter()` expressions
- [ ] Return `Bars` with polars DataFrame
- [ ] **Test**: Run full parity test

**Lines changed**: ~20

**Verification**:
```bash
pytest tests/backtest/test_databento_parity.py -v
```

**Expected**: ✅ ALL PASS

---

## Testing Phase

### Test 1: Parity Test (MUST PASS)
```bash
pytest tests/backtest/test_databento_parity.py -v -s
```

**What to check**:
- [ ] All tests pass (green)
- [ ] No assertion errors
- [ ] Pandas and Polars results are identical

**If fails**:
- Check datetime timezone handling
- Check empty DataFrame handling (polars `.is_empty()` vs pandas `.empty`)
- Check conversion functions
- Add debug prints to see where results diverge

---

### Test 2: Comprehensive Tests
```bash
pytest tests/backtest/test_databento_comprehensive_trading.py -m apitest -v
```

**What to check**:
- [ ] Multi-asset tests pass
- [ ] Complex strategy tests pass
- [ ] No edge case failures

**If fails**:
- Check specific test that fails
- Debug that operation
- May need to add more polars optimizations

---

### Test 3: Performance Profiling
```bash
python -m tests.performance.profile_databento_mes_momentum --mode both
```

**What to check**:
- [ ] Polars backend is 20-30% faster
- [ ] Output shows ~70-75s for Polars (vs 95s before)
- [ ] Speedup is 1.25x-1.35x

**If speedup is less than expected**:
- Run profile analyzer to see bottlenecks
- Check if conversions are happening too often
- May need additional optimizations

---

### Test 4: Profile Analysis
```bash
python tests/performance/analyze_profile.py tests/performance/logs/mes_momentum_polars.prof
```

**What to check**:
- [ ] DatetimeArray iterations reduced (from 10s → ~3s)
- [ ] Conversion overhead reduced
- [ ] Filtering operations faster

**Red flags**:
- If DatetimeArray time didn't decrease → datetime handling issue
- If lots of time in `to_pandas()` → too many conversions
- If no improvement → optimizations not being used

---

## Success Criteria

### ✅ Phase 4A Complete When:
1. [ ] All parity tests pass
2. [ ] All comprehensive tests pass
3. [ ] Polars backend shows 20-30% speedup
4. [ ] DatetimeArray iterations reduced by 70%+
5. [ ] No breaking changes to external API

---

## Rollback Plan

### If Parity Tests Fail:
1. Comment out polars optimizations
2. Force pandas path:
   ```python
   if False:  # Was: if self.SOURCE == "POLARS":
       # polars path
   else:
       # pandas path
   ```
3. Investigate issue
4. Fix and retry

### If Performance Is Worse:
1. Run profile analyzer to find new bottlenecks
2. Check if conversions are happening in tight loops
3. May need to optimize different operations first
4. Document findings and adjust plan

---

## Post-Implementation

### Documentation Updates:
- [ ] Update `PHASE_3_FINDINGS.md` with Phase 4A results
- [ ] Document actual speedup achieved
- [ ] Note any unexpected issues encountered
- [ ] Update optimization plan for Phase 4B

### Code Review Checklist:
- [ ] No breaking changes to external API
- [ ] All tests pass
- [ ] Performance improvement validated
- [ ] Code is clean and documented
- [ ] No debug code left in

### Git Commit:
```bash
git add lumibot/entities/bars.py
git add lumibot/data_sources/polars_data.py
git add lumibot/backtesting/databento_backtesting_polars.py
git commit -m "Phase 4A: Internal polars storage (20-30% speedup)

- Modified Bars class to support polars internal storage
- Added polars conversion helpers to PolarsData
- Optimized get_historical_prices with polars filtering
- Optimized get_last_price with polars expressions
- Convert to polars immediately after network fetch

Results:
- Pandas: 94.85s
- Polars: 70-75s (target)
- Speedup: 1.25-1.35x
- All parity tests pass
"
```

---

## Next Steps (Phase 4B)

After Phase 4A is complete and validated:

### Phase 4B Focus:
1. Use polars native parquet reader (skip pandas conversion at network layer)
2. Add lazy evaluation for filtering operations
3. Optimize column selection (only read needed columns)
4. Expected additional speedup: 15-20%

### Target:
- Phase 4A: 95s → 70s (25% speedup)
- Phase 4B: 70s → 50s (additional 28% speedup)
- Phase 4C: 50s → 35s (additional 30% speedup)
- **Total**: 95s → 35s (63% speedup across all phases)

---

## Quick Start Command Sequence

```bash
# 1. Run baseline profile (verify current state)
python -m tests.performance.profile_databento_mes_momentum --mode both

# 2. Make code changes (Steps 1-5 above)

# 3. Test imports
python -c "from lumibot.entities import Bars; from lumibot.data_sources import PolarsData; print('Imports OK')"

# 4. Run parity tests
pytest tests/backtest/test_databento_parity.py -v

# 5. Run comprehensive tests
pytest tests/backtest/test_databento_comprehensive_trading.py -m apitest -v

# 6. Run performance profile
python -m tests.performance.profile_databento_mes_momentum --mode both

# 7. Analyze results
python tests/performance/analyze_profile.py tests/performance/logs/mes_momentum_polars.prof

# 8. If all pass, commit
git add -A
git commit -m "Phase 4A: Internal polars storage"
```

---

## Troubleshooting

### Issue: Import error for polars
```
ModuleNotFoundError: No module named 'polars'
```
**Fix**:
```bash
pip install polars
```

### Issue: Parity test fails on datetime comparison
```
AssertionError: Timestamps don't match
```
**Fix**: Check timezone handling in conversion functions. Polars handles timezones differently than pandas.

### Issue: Bars object has no attribute '_df_polars'
```
AttributeError: 'Bars' object has no attribute '_df_polars'
```
**Fix**: Bars class initialization didn't set both attributes. Check Step 1 implementation.

### Issue: No speedup observed
```
Polars: 95s (same as before)
```
**Fix**: Optimizations not being used. Check:
1. Is `SOURCE == "POLARS"` condition working?
2. Are conversion functions being called?
3. Add debug prints to verify polars path is taken

### Issue: Speedup but parity fails
```
Polars faster but results don't match pandas
```
**Fix**: Likely datetime or floating point issue. Check:
1. Timezone handling in conversions
2. Empty DataFrame handling
3. Index vs no-index differences
