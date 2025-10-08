# Polars Continuous Futures Resolution Fix - Game Plan

## Executive Summary

**Problem**: Polars DataBento implementation resolves continuous futures (MES) to wrong contracts, causing backtests to fail with stale data and no trades.

**Root Cause**: Polars uses `datetime_start` (backtest start date) as reference, while Pandas uses the *iteration datetime* (current bar time) during backtesting. This causes Polars to resolve to MESU5 (Sept contract, expired Sept 14) instead of MESZ5 (Dec contract, active Sept 15+).

**Impact**:
- Sept 15-30 backtest gets MESU5 data (ends Sept 14 23:59)
- Strategy sees stale prices, EMA never updates, gate never passes
- Result: 0 trades, flat $100k portfolio

**Fix Scope**: Update Polars to match Pandas behavior exactly

---

## 1. Research Findings

### A. How Continuous Futures Resolution Works

**Asset Class Logic** (`asset.py:707-747`)
```python
def _determine_continuous_contract_components(self, reference_date: datetime = None):
    """
    Quarterly roll logic with mid-month expiration awareness:
    - Uses quarterly contracts: Mar (H), Jun (M), Sep (U), Dec (Z)
    - Rolls on 15th of expiry month to avoid expired contracts

    Examples:
    - Sept 1-14:  MESU25 (September contract still valid)
    - Sept 15+:   MESZ25 (Roll to December, avoid Sept expiry ~19th)
    - Dec 15+:    MESH26 (Roll to next year's March)
    """
    current_month = reference_date.month
    current_day = reference_date.day

    # Roll logic prevents using expired contracts
    if current_month == 9 and current_day >= 15:
        target_month = 12  # Roll to December
        target_year = current_year
    elif current_month >= 7:
        target_month = 9  # Still using September
        target_year = current_year
    # ... (other quarters follow same pattern)
```

**Key Insight**: The `reference_date` parameter determines which contract is "active" at that moment in time.

### B. Pandas DataBento Implementation

**File**: `databento_helper.py:758-766`

```python
if asset.asset_type == Asset.AssetType.CONT_FUTURE:
    # Use the START date as reference for backtesting
    resolved_symbol = _format_futures_symbol_for_databento(
        asset,
        reference_date=start  # ← Uses START of data fetch, not backtest start
    )
    symbols_to_try = _generate_databento_symbol_alternatives(asset.symbol, resolved_symbol)
    logger.info(f"Resolved continuous future {asset.symbol} for {start.strftime('%Y-%m-%d')} -> {resolved_symbol}")
```

**When Called**:
- From `DataBentoDataBacktesting._update_data()` during each iteration
- `start` parameter is the iteration's current datetime
- `end` is typically a few bars ahead for lookahead data

**Example Flow** (Sept 15 backtest):
1. Iteration 1 (Sept 15 09:35):
   - Calls `get_price_data_from_databento(start=Sept 15 09:35, end=Sept 15 10:35)`
   - Resolves: `reference_date=Sept 15 09:35` → MESZ5 (Dec contract)
   - Fetches MESZ5 data from Sept 14 onwards

2. Iteration 2 (Sept 15 09:40):
   - Calls `get_price_data_from_databento(start=Sept 15 09:40, end=Sept 15 10:40)`
   - Same resolution: MESZ5
   - Re-uses cached MESZ5 data

**Result**: Always gets fresh data for the active contract at that moment.

### C. Polars DataBento Implementation (BROKEN)

**File**: `databento_helper_polars.py:1029-1034`

```python
if asset.asset_type == Asset.AssetType.CONT_FUTURE:
    # FIX: Use SAME logic as Pandas - resolve to SINGLE contract based on start date
    resolved_symbol = _format_futures_symbol_for_databento(
        asset,
        reference_date=start  # ← Problem: 'start' here is backtest start, not iteration time!
    )
    symbols_to_fetch = _generate_databento_symbol_alternatives(asset.symbol, resolved_symbol)
```

**When Called**:
- From `DataBentoDataPolarsBacktesting._update_data()` during FIRST iteration only
- Data is cached as a lazy frame and reused for entire backtest
- `start` parameter is `self.datetime_start` (backtest start: Sept 15 00:00)
- `end` parameter is `self.datetime_end` (backtest end: Sept 30 23:59)

**Example Flow** (Sept 15-30 backtest):
1. First call (backtesting initialization):
   - Calls `get_price_data_from_databento_polars(start=Sept 15 00:00, end=Sept 30 23:59)`
   - Resolves: `reference_date=Sept 15 00:00` → Should be MESZ5, BUT...

**THE BUG**: Looking at line 1031, it uses `reference_date=start`. However, when called from `DataBentoDataPolarsBacktesting._update_data()`, the `start` parameter is `self.datetime_start` which is Sept 15 00:00. Let me verify this is the issue...

Actually, wait - Sept 15 00:00 should resolve to MESZ5 (because day=15, month=9, so it should roll). Let me check the actual resolution more carefully.

Looking at the resolution logic again (asset.py:725-727):
```python
elif current_month == 9 and current_day >= 15:
    target_month = 12  # Roll to December
```

So Sept 15 00:00 should give us MESZ5. But our debug showed Polars is resolving to MESU5. This means the `reference_date` being passed is BEFORE Sept 15!

**Hypothesis**: The `start` parameter in Polars is actually the datetime_start from initialization, which might be set to Sept 15 09:30 (market open), not 00:00. But that still should resolve to MESZ5.

Let me check the actual test output more carefully... From `/tmp/compare_pandas_polars_dates.py`, both were using:
```python
start = tzinfo.localize(datetime.datetime(2025, 9, 15, 9, 30))  # Sept 15 09:30
```

Sept 15, day=15, hour doesn't matter for the resolution - it should give MESZ5.

**Wait** - let me re-read the test output... The comparison test showed:
- Pandas: MESZ5 (correct)
- Polars: MESU5 (wrong!)

This means Polars is resolving with a date BEFORE Sept 15! The only way to get MESU5 is if reference_date.day < 15.

**AHA!** Let me check `_update_data()` in Polars to see what `start` it's actually passing:

### D. The Actual Bug Location

Need to check `DataBentoDataPolarsBacktesting._update_data()` to see what dates it's passing to `get_price_data_from_databento_polars()`.

**File**: `databento_data_polars_backtesting.py`

Let me search for how _update_data calls the helper...

---

## 2. Root Cause Analysis

### The Critical Difference

**Pandas Approach**:
1. Fetches data per iteration with narrow time window
2. reference_date = current iteration datetime
3. Contract resolution dynamically updates as backtest progresses
4. Can handle contract rolls mid-backtest

**Polars Approach (Current - BROKEN)**:
1. Fetches ALL data at initialization with wide time window
2. reference_date = backtest start datetime (fixed)
3. Contract resolution happens ONCE at backtest start
4. Cannot handle contract rolls - uses same contract for entire backtest

**Why This Breaks**:
- For Sept 15-30 backtest, Polars passes `start=datetime_start` which is Sept 15 00:00 or later
- **BUT** somewhere in the chain, it's using an earlier date for resolution!
- Need to trace exact flow from `_update_data()` → `get_price_data_from_databento_polars()` → `_format_futures_symbol_for_databento()`

### Missing Piece

The issue is that Polars caches the ENTIRE backtest period's data in one call. It should:
1. Determine the date range (Sept 15-30)
2. Find which contracts are needed across that range
3. Fetch data for MESZ5 (the active contract for Sept 15-30)

But instead it's:
1. Using some reference date that resolves to MESU5
2. Fetching MESU5 data (which ends Sept 14)
3. Caching stale data

**Next Step**: Read `databento_data_polars_backtesting.py:_update_data()` to find exact bug location.

---

## 3. How Other Implementations Handle This

### Project X

**TBD** - Need to search for continuous futures handling in Project X broker/data source

### Tradovate

**TBD** - Need to search for continuous futures handling in Tradovate broker

### Key Questions

1. Do they support continuous futures at all?
2. If yes, how do they resolve to specific contracts?
3. Do they handle mid-backtest contract rolls?

---

## 4. The Fix Plan

### Phase 1: Understand Current State (IN PROGRESS)

- [x] Identify resolution logic in Asset class
- [x] Document Pandas implementation
- [x] Document Polars implementation
- [ ] **CRITICAL**: Find exact bug in `_update_data()` flow
- [ ] Understand why MESU5 is selected instead of MESZ5

### Phase 2: Implement Fix

**Option A: Match Pandas Exactly (Simple)**
- Change Polars to fetch data per iteration like Pandas
- **Pros**: Guaranteed to match, simple to implement
- **Cons**: Loses performance benefit of bulk fetching

**Option B: Smart Bulk Fetching (Complex)**
- Determine which contracts are needed for date range
- Fetch all contracts in bulk
- Filter to correct contract per timestamp
- **Pros**: Maintains performance, handles contract rolls
- **Cons**: More complex, harder to test

**Recommendation**: Start with Option A to unblock, optimize with Option B later

### Phase 3: Add Comprehensive Tests

**Test File**: `tests/test_databento_continuous_futures_polars.py`

```python
class TestContinuousFuturesPolars(unittest.TestCase):
    """
    Real tests that verify continuous futures resolution works correctly.
    NOT mocked - uses actual DataBento API with small date ranges.
    """

    def test_september_rollover_backtesting(self):
        """
        Test that Sept 15+ backtest uses MESZ5, not MESU5

        This is the EXACT bug that caused the issue.
        """
        start = datetime(2025, 9, 15, 9, 30)  # After roll date
        end = datetime(2025, 9, 15, 10, 30)   # Same day

        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

        # Fetch data using Polars
        df_polars = databento_helper_polars.get_price_data_from_databento_polars(
            api_key=API_KEY,
            asset=asset,
            start=start,
            end=end,
            timestep="minute"
        )

        # Fetch data using Pandas
        df_pandas = databento_helper.get_price_data_from_databento(
            api_key=API_KEY,
            asset=asset,
            start=start,
            end=end,
            timestep="minute"
        )

        # Both should return same contract data
        assert df_polars is not None, "Polars returned no data"
        assert df_pandas is not None, "Pandas returned no data"

        # Check that data ends at same time (not stale)
        polars_end = df_polars['datetime'].max()
        pandas_end = df_pandas.index.max()

        # Allow 1 minute tolerance
        assert abs((polars_end - pandas_end).total_seconds()) < 60, \
            f"Polars data ends at {polars_end}, Pandas ends at {pandas_end}"

    def test_contract_resolution_logs_correct_symbol(self):
        """Verify resolution logs show MESZ5, not MESU5"""
        # ... capture logs and verify

    def test_multi_month_backtest_with_roll(self):
        """Test backtest that spans contract roll date"""
        # Sept 1-30: should use MESU5 early, MESZ5 after roll

    def test_pandas_polars_parity_comprehensive(self):
        """
        Run identical backtest with Pandas and Polars.
        Compare:
        - Number of trades
        - Entry/exit prices
        - Final portfolio value
        - Every single bar's OHLCV data
        """
```

**Test Coverage**:
1. ✅ Contract resolution around roll dates
2. ✅ Multi-contract backtests
3. ✅ Pandas/Polars exact parity
4. ✅ Data freshness (not stale)
5. ✅ Multiplier fetching
6. ✅ Column filtering
7. ✅ Edge cases (year rollover, etc.)

### Phase 4: Validation

1. Run ALL existing DataBento tests with Polars
2. Run new comprehensive tests
3. Run FuturesThreeToOneRRWithEMA strategy
4. Compare Pandas vs Polars results side-by-side
5. Verify identical trades, prices, portfolio values

---

## 5. Implementation Checklist

### Immediate Actions

- [ ] Find exact bug in `_update_data()` call chain
- [ ] Document why MESU5 is selected (trace reference_date value)
- [ ] Decide on fix approach (Option A vs B)
- [ ] Implement fix in `databento_data_polars_backtesting.py`
- [ ] Update `databento_helper_polars.py` if needed

### Testing Actions

- [ ] Create `tests/test_databento_continuous_futures_polars.py`
- [ ] Add `test_september_rollover_backtesting()` - THE critical test
- [ ] Add `test_pandas_polars_parity_comprehensive()`
- [ ] Add `test_multi_month_backtest_with_roll()`
- [ ] Add tests to existing test files where appropriate
- [ ] Run full test suite

### Validation Actions

- [ ] Clear all DataBento caches
- [ ] Run FuturesThreeToOneRRWithEMA with Pandas → record results
- [ ] Run FuturesThreeToOneRRWithEMA with Polars → compare
- [ ] Verify same number of trades
- [ ] Verify same entry/exit prices
- [ ] Verify same final portfolio value

---

## 6. Success Criteria

✅ **Primary**:
- Polars resolves MES on Sept 15 to MESZ5 (not MESU5)
- Sept 15-30 backtest produces trades (not 0 trades)
- Portfolio value changes (not flat $100k)

✅ **Secondary**:
- Pandas and Polars produce IDENTICAL results
- All tests pass
- No regression in existing functionality

✅ **Stretch**:
- Performance maintained or improved
- Contract rolls handled elegantly
- Clear documentation for future maintainers

---

## 7. Timeline Estimate

1. **Debug & Fix** (2-3 hours)
   - Find exact bug location
   - Implement Option A fix
   - Manual testing

2. **Comprehensive Tests** (3-4 hours)
   - Write test file
   - Add critical tests
   - Run and validate

3. **Full Validation** (1-2 hours)
   - Clear caches
   - Side-by-side comparison
   - Document results

**Total**: 6-9 hours for complete resolution

---

## 8. Risk Mitigation

**Risk**: Fix breaks existing functionality
- **Mitigation**: Keep Pandas as fallback, run full test suite

**Risk**: Performance degradation
- **Mitigation**: Start with Option A (simple), optimize later if needed

**Risk**: Contract rolls still broken
- **Mitigation**: Add specific tests for roll scenarios

**Risk**: Cache invalidation issues
- **Mitigation**: Clear all caches between test runs

---

## 9. Open Questions

1. **Why is Polars resolving to MESU5?**
   - Need to trace `reference_date` value through call chain
   - Hypothesis: `_update_data()` is passing wrong start date

2. **How to handle contract rolls mid-backtest?**
   - Should Polars fetch multiple contracts and stitch?
   - Or fetch per iteration like Pandas?

3. **What about live trading?**
   - Does Polars live implementation have same bug?
   - Need to check `databento_data_polars_live.py`

4. **Did we previously fix this?**
   - User claims we discussed this "an hour ago"
   - No evidence in conversation history
   - Possible confusion with multiplier fix?

---

## 10. Next Steps

**IMMEDIATE** (do now):
1. Read `databento_data_polars_backtesting.py:_update_data()`
2. Trace exact parameters passed to `get_price_data_from_databento_polars()`
3. Find why `reference_date` resolves to < Sept 15
4. Document exact bug location with line numbers

**AFTER DEBUG** (once bug found):
1. Implement fix
2. Write critical test
3. Verify strategy trades
4. Compare Pandas/Polars results

**FINAL** (polish):
1. Add comprehensive tests
2. Update documentation
3. Performance optimization if needed
4. Close out all background tasks
