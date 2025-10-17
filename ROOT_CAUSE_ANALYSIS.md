# ThetaData Cache & Parity Root Cause Analysis

## Executive Summary

**Total Test Failures:** 20 tests (5 cache warmth, 15 parity)
**Root Causes Identified:** 2 distinct issues
**Evidence:** DEBUG logs + diff analysis from test runs

---

## Pattern 1: Cache Warmth Test Failures (5 tests)

### Issue
**Cold run (API fetch) does NOT apply start/end time filtering**
**Warm run (cache load) DOES apply filtering**

### Evidence

#### stock-minute
- **Cold run:** 13,216 rows starting at `2024-07-01T08:09:00+00:00` (4:09 AM ET - pre-market)
- **Warm run:** 13,009 rows starting at `2024-07-01T13:30:00+00:00` (9:30 AM ET - market open)
- **Difference:** 207 rows (all pre-market data)

#### stock-5minute
- **Cold run:** 3,646 rows starting at `2024-07-01T08:05:00+00:00`
- **Warm run:** 3,560 rows starting at `2024-07-01T13:30:00+00:00`
- **Difference:** 86 rows

#### stock-15minute
- **Cold run:** 1,359 rows starting at `2024-07-01T08:00:00+00:00`
- **Warm run:** 1,323 rows starting at `2024-07-01T13:30:00+00:00`
- **Difference:** 36 rows

#### stock-hour
- **Cold run:** 349 rows (includes `08:00-13:00 UTC` pre-market AND `21:00-23:00 UTC` after-hours)
- **Warm run:** 340 rows (filtered to market hours only)
- **Difference:** 9 rows (6 pre-market + 3 after-hours)

#### option-hour
- **Cold run:** 141 rows (includes `2024-07-01T13:00:00+00:00` = 9:00 AM ET)
- **Warm run:** 140 rows (filtered to 9:30 AM ET start)
- **Difference:** 1 row (30 minutes before market open)

### Log Evidence

**Warm run correctly filters:**
```
[DEBUG][FILTER][INTRADAY_ENTRY] start_param=2024-07-01T09:30:00 end_param=2024-07-31T16:00:00
[DEBUG][FILTER][TZ_LOCALIZE] localized start to UTC: 2024-07-01T13:30:00+00:00
[DEBUG][FILTER][NO_DT] using end=2024-07-31T20:00:00+00:00 for upper bound
[THETA][FILTER][AFTER] rows=13009 first_ts=2024-07-01T13:30:00+00:00
```

**Cold run does NOT filter:**
```
[DEBUG][CACHE][UPDATE_WRITE] total_rows=13216 | min_ts=2024-07-01T08:09:00+00:00 max_ts=2024-07-31T23:59:00+00:00
```

### Root Cause Location
File: `lumibot/tools/thetadata_helper.py`

**Code path analysis:**
1. **Cold run path:** API fetch → direct return (NO filtering)
2. **Warm run path:** Cache load → `_apply_intraday_filters()` → filtered return

The filtering logic at line 365-501 is only executed for cached data, NOT for fresh API data.

### Fix Required
Apply `_apply_intraday_filters()` to BOTH cold and warm code paths to ensure consistent filtering.

---

## Pattern 2: Pandas/Polars Parity Test Failures (15 tests)

### Issue
**Timezone mismatch error when converting polars DataFrame to pandas for test comparison**

### Evidence

**Error Message:**
```
AssertionError: Inferred time zone not equal to passed time zone
```

**Location:** Test file line 101-104 when calling `polars_df.to_pandas()`

**What's Working:**
- Pandas mode: Returns 100 rows, timestamps `2024-07-10T12:50:00-04:00` to `2024-07-10T14:29:00-04:00`
- Polars mode: Correctly filters data (1793 rows → dt filtered), NO look-ahead bias

**What's Broken:**
- Test conversion logic has timezone handling bug
- The actual polars implementation is working correctly!

### Log Evidence

**Pandas mode (working):**
```
[DEBUG][DATA][GET_BARS][RETURN] asset=HIMS | rows=100 | first_ts=2024-07-10T12:50:00-04:00 last_ts=2024-07-10T14:29:00-04:00 | future_bars=0 OK
```

**Polars mode (working, but test fails on conversion):**
```
[DEBUG][FILTER][DT_AFTER] asset=HIMS | rows_after=1793 | future_bars_removed=YES
[THETA][FILTER][AFTER] asset=HIMS rows=1793 first_ts=2024-07-05T16:50:00+00:00 last_ts=2024-07-10T18:30:00+00:00
[POLARS] Converting final DataFrame to polars for HIMS: 1793 rows
```

### Root Cause Location
File: `tests/test_thetadata_cache_and_parity.py` line 101-104

**Issue:** Test's polars→pandas conversion doesn't preserve timezone correctly

### Fix Required
Fix the test's timezone handling when converting polars to pandas for comparison.

---

## Summary

| Issue | Tests Affected | Root Cause | Fix Location |
|-------|---------------|------------|--------------|
| Inconsistent filtering | 5 cache warmth tests | Cold run doesn't apply start/end filtering | `thetadata_helper.py` filtering logic |
| Timezone conversion bug | 15 parity tests | Test's conversion code has TZ issue | Test file conversion logic |

## Next Steps

1. ✅ Evidence gathered via DEBUG logs and diff analysis
2. ✅ Root causes identified with exact locations
3. ⏭️ Implement Fix #1: Apply filtering to both cold and warm paths
4. ⏭️ Implement Fix #2: Fix test's timezone conversion
5. ⏭️ Re-run tests to verify all 20 failures are resolved
6. ⏭️ Run actual backtest to confirm portfolio value parity
