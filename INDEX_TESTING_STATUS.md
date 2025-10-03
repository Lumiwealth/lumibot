# Index Testing - Current Status

## Summary

**Index data testing is BLOCKED** pending ThetaData support ticket resolution.

**Issue**: Indices subscription not yet activated for minute-level data access.

**Error**: `PERMISSION - a indices Standard or Pro subscription is required to access indices data that is under 15 minute intervals`

## What's Ready

✅ **Code fixes**:
- Index asset type handling added to `thetadata_helper.py:531-539`
- Removed `rth` parameter for index assets (indexes are calculated values, not traded securities)
- Skip decorator removed from index test

✅ **Test infrastructure**:
- `test_index_data_verification.py` - Comprehensive index tests (6 tests)
- `verify_index_access.py` - Quick verification script
- `test_thetadata_vs_polygon.py` - Updated with proper index comparison

✅ **What will be tested once access is granted**:
1. SPX data accessibility
2. VIX data accessibility
3. Timestamp accuracy (no +1 minute offset like stocks had)
4. Price accuracy vs Polygon
5. OHLC consistency
6. No missing bars

## Subscription Requirements

Based on ThetaData documentation:

| Tier | Index Data | Resolution | Real-time |
|------|-----------|-----------|-----------|
| VALUE | ✓ | 15-minute | No (delayed) |
| STANDARD | ✓ | Minute-level | Yes |
| PRO | ✓ | Minute-level | Yes (more symbols) |

**Current status**: Need STANDARD or PRO tier for minute-level index data.

## When Subscription is Active

### Step 1: Quick Verification

```bash
# Restart ThetaTerminal to refresh permissions
pkill -f ThetaTerminal.jar

# Wait 3 seconds for it to fully stop
sleep 3

# Run verification script
python3 verify_index_access.py
```

**Expected output**:
```
✓ INDICES SUBSCRIPTION IS ACTIVE AND WORKING
✓ SUCCESS: Got 5 bars
✓ First bar at correct time: 2024-08-01 09:30:00-04:00
✓ All bars are 60 seconds apart
```

### Step 2: Clear Cache

```bash
rm -rf ~/Library/Caches/lumibot/1.0/thetadata/
```

### Step 3: Run Full Index Tests

```bash
# Comprehensive index tests (6 tests)
pytest tests/backtest/test_index_data_verification.py -v

# Expected: 6 PASSED
```

### Step 4: Run Comparison Tests vs Polygon

```bash
# Full comparison suite including indexes
pytest tests/backtest/test_thetadata_vs_polygon.py::TestThetaDataVsPolygonComparison -v

# Expected: 6 PASSED (stocks, options, indexes, fills, portfolio, cash)
```

## Tests That Will Run

### Test 1: SPX Data Accessible
- Verifies minute-level SPX data is accessible
- Checks data format and structure

### Test 2: VIX Data Accessible
- Verifies minute-level VIX data is accessible
- Checks data format and structure

### Test 3: Timestamp Accuracy (CRITICAL)
- Verifies first bar is at exactly 9:30:00
- Verifies all bars are exactly 60 seconds apart
- **This is the +1 minute bug we fixed for stocks** - need to verify indexes don't have it

### Test 4: SPX vs Polygon Comparison
- Compares SPX prices at 9:30, 9:45, 10:00
- Tolerance: $0.50 (0.01% for SPX ~$5000)
- Verifies ThetaData matches trusted Polygon baseline

### Test 5: VIX vs Polygon Comparison
- Compares VIX prices at 9:30, 9:45, 10:00
- Tolerance: $0.10 (0.5% for VIX ~20)
- Verifies ThetaData matches trusted Polygon baseline

### Test 6: OHLC Consistency
- Verifies high >= open, close, low
- Verifies low <= open, close, high
- Verifies all prices > 0
- Verifies prices in reasonable range (3000-7000 for SPX)

### Test 7: No Missing Bars
- Verifies full trading day (9:30-16:00) has ~390 bars
- Checks for gaps in data

## Critical Verification Points

The goal is to ensure **indexes work identically to stocks and options**:

1. ✓ **Accessibility**: Index data returns successfully
2. ✓ **Timestamps**: No +1 minute offset (the bug we fixed for stocks)
3. ✓ **Accuracy**: Prices match Polygon within tight tolerances
4. ✓ **Consistency**: OHLC data is internally consistent
5. ✓ **Completeness**: No missing bars

## Files Modified for Index Support

1. `lumibot/tools/thetadata_helper.py:531-539`
   ```python
   elif asset.asset_type == "index":
       # For indexes (SPX, VIX, etc.), don't use rth parameter
       # Indexes are calculated values, not traded securities
       querystring = {
           "root": asset.symbol,
           "start_date": start_date,
           "end_date": end_date,
           "ivl": ivl
       }
   ```

2. `tests/backtest/test_thetadata_vs_polygon.py:374`
   - Removed `@pytest.mark.skip` decorator
   - Index test will now run

3. `tests/backtest/test_index_data_verification.py`
   - New file: Comprehensive index-specific tests

4. `verify_index_access.py`
   - New file: Quick verification script

## Why No Index Data Available Yet

ThetaData requires a separate **Indices subscription** (introduced March 1st, 2024) for minute-level data:

- Base subscription (VALUE): 15-minute delayed index data only
- Indices STANDARD/PRO: Minute-level real-time index data

Your support ticket should resolve this. Once activated:
1. Terminal will refresh permissions
2. Minute-level data will become accessible
3. All tests should pass

## Current Test Results

### Stocks: ✅ PASSING
- Price accuracy: 0.5¢ difference
- Timestamp: Correct (market open at 9:30)
- Portfolio: $2.70 difference on $100k

### Options: ✅ PASSING
- Price accuracy: 0¢ difference (perfect match)
- Chains: 82 expirations available
- ATM pricing: Working correctly

### Indexes: ⏳ BLOCKED
- Waiting for subscription activation
- Tests ready to run
- Infrastructure verified

## Next Action

**Wait for ThetaData support** to activate indices subscription, then:
1. Run `python3 verify_index_access.py`
2. If successful, run full test suite
3. Verify all 6 index tests pass
4. Confirm prices match Polygon within tolerance

---

**Created**: 2025-10-01
**Status**: Awaiting subscription activation
**Tests ready**: 6 comprehensive index tests
**Estimated test time**: ~2 minutes once active
