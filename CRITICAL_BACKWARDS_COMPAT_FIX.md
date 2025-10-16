# CRITICAL BACKWARDS COMPATIBILITY FIX

## Date: October 15, 2025

## Summary

Fixed a **critical backwards compatibility bug** where the polars data source was returning polars DataFrames by default instead of pandas DataFrames. This would have broken ALL existing strategies (hundreds to thousands of strategies).

## The Bug

### Root Cause
In `lumibot/data_sources/polars_data.py`, the `return_polars` parameter was incorrectly defaulting to `True`:

```python
# BEFORE (BROKEN - Line 522):
return_polars: bool = True,  # Default to True for PolarsData (performance optimization)
```

This meant that calling `get_historical_prices()` without explicit parameters would return polars DataFrames instead of pandas DataFrames.

### Why This Was Critical

**ALL existing strategies assume pandas DataFrames** and use pandas-specific operations:
```python
# Typical strategy code:
bars = self.get_historical_prices(asset, 100, "minute")
df = bars.df  # Expects pandas DataFrame!

# Uses pandas operations:
df["sma"] = df["close"].rolling(window=9).mean()  # pandas method
df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)  # pandas method
last_price = df.iloc[-1]["close"]  # pandas indexing
```

If `df` is a polars DataFrame, all of these operations would **fail** or behave differently!

## The Fix

### Files Modified

1. **`lumibot/data_sources/polars_data.py:523`**
   ```python
   # AFTER (FIXED):
   # CRITICAL: Default MUST be False for backwards compatibility with existing strategies!
   return_polars: bool = False,
   ```

2. **`lumibot/data_sources/polars_data.py:415`**
   ```python
   # AFTER (FIXED):
   def _parse_source_symbol_bars(self, response, asset, quote=None, length=None, return_polars=False):
       """parse broker response for a single asset

       CRITICAL: return_polars defaults to False for backwards compatibility.
       Existing strategies expect pandas DataFrames!
       """
   ```

3. **`lumibot/entities/__init__.py:8`**
   ```python
   # Added missing export:
   from .data_polars import DataPolars
   ```

## Tests Added

Created comprehensive backwards compatibility tests in:
**`tests/backtest/test_return_type_backwards_compatibility.py`**

### Test Coverage

1. **test_pandas_data_source_returns_pandas_by_default**
   - Verifies pandas data source returns pandas DataFrames

2. **test_polars_data_source_returns_pandas_by_default** ✅ CRITICAL
   - Verifies polars data source ALSO returns pandas DataFrames by default
   - Even though implementation uses polars internally for performance

3. **test_polars_data_source_returns_polars_when_requested**
   - Verifies opt-in behavior: `return_polars=True` returns polars DataFrames

4. **test_explicit_return_polars_false**
   - Verifies explicit opt-out: `return_polars=False` returns pandas DataFrames

5. **test_backwards_compatibility_documentation**
   - Uses introspection to verify the default parameter value
   - Will fail if default is changed back to True
   - Serves as living documentation of the contract

### Test Results
```bash
$ python -m pytest tests/backtest/test_return_type_backwards_compatibility.py -v -m apitest
================= 4 passed, 1 deselected, 2 warnings in 7.88s ==================
```

## Verification

### Parity Test
```bash
$ python -m pytest tests/backtest/test_databento_parity.py::test_databento_price_parity -v -m apitest
======================== 1 passed, 2 warnings in 6.25s =========================
```

### Performance Verification
```bash
# Pandas baseline
$ python -m tests.performance.profile_databento_mes_momentum --mode pandas
Elapsed time: 87.13s

# Polars (with backwards compat fix)
$ python -m tests.performance.profile_databento_mes_momentum --mode polars
Elapsed time: 52.33s

# Speedup: 87.13 / 52.33 = 1.67x (67% faster)
```

**Performance maintained!** The fix did not impact the 1.7x speedup.

## The Backwards Compatibility Contract

### MUST ALWAYS BE TRUE:
1. **Default behavior**: `get_historical_prices()` returns pandas DataFrames
2. **Default parameter**: `return_polars` defaults to `False`
3. **Opt-in only**: Only when `return_polars=True` is **explicitly set** should polars be returned

### WHY:
- Thousands of existing strategies depend on pandas DataFrame API
- Strategies use pandas-specific operations: `.rolling()`, `.shift()`, `.iloc[]`, etc.
- Breaking this contract would break ALL existing strategies
- This would cause massive production failures

### HOW PERFORMANCE STILL WORKS:
- Polars is used **internally** for performance (filtering, caching, storage)
- Conversion to pandas happens **once** when strategy first accesses data
- This still provides **1.67x speedup** while maintaining compatibility
- Future: strategies can **opt-in** to polars with `return_polars=True` for additional speedup

## Future Strategy Migration Path

When we want strategies to use polars for additional speedup, the migration path is:

### Step 1: Update strategy to request polars
```python
# Before (uses pandas):
bars = self.get_historical_prices(asset, 100, "minute")

# After (opts into polars):
bars = self.get_historical_prices(asset, 100, "minute", return_polars=True)
```

### Step 2: Update strategy operations to use polars API
```python
# Before (pandas):
df["sma"] = df["close"].rolling(window=9).mean()

# After (polars):
df = df.with_columns([
    pl.col("close").rolling_mean(window_size=9).alias("sma")
])
```

### Step 3: Update AI code generation
- Teach AI to generate polars-compatible code
- Provide both pandas and polars templates
- Let users choose which API to use

**This migration is OPTIONAL and can happen gradually!**

## Lessons Learned

1. **Default parameters matter for backwards compatibility**
   - Changing a default can break thousands of downstream consumers
   - Always preserve backwards-compatible defaults

2. **Test the contract, not just the implementation**
   - Added tests that verify the default behavior
   - Tests will catch if defaults are accidentally changed

3. **Performance optimizations must be transparent**
   - Polars is used internally for performance
   - But external API contract (pandas by default) is maintained
   - Users can opt-in to polars when ready

4. **Document the "why" not just the "what"**
   - Added comments explaining WHY defaults must be False
   - Future developers will understand the constraint

## Impact

### Without This Fix:
- ❌ ALL existing strategies would break
- ❌ Production failures across hundreds/thousands of strategies
- ❌ Loss of user trust
- ❌ Massive rollback and hotfix required

### With This Fix:
- ✅ Existing strategies work without modification
- ✅ 1.67x performance improvement is transparent
- ✅ Future opt-in path for additional speedup
- ✅ Tests prevent regression

## Recommendation

**ALWAYS run backwards compatibility tests before releasing:**
```bash
pytest tests/backtest/test_return_type_backwards_compatibility.py -v
```

**NEVER change the default value of `return_polars` from `False`!**

This parameter exists for **opt-in** performance optimization, not as a **default** behavior change.
