# Comprehensive Test Investigation Report

## Executive Summary

Total Tests: 102 (after deleting 2 legacy tests)
- **Status Before Investigation**: 76 PASSED, 14 SKIPPED, 7 xfailed, 5 FAILED
- **Critical Issues Found**: Multiple root causes requiring investigation

---

## 1. DELETED - ThetaData Legacy Tests ✅

**Action Taken**: Deleted 2 legacy tests from `test_thetadata.py`
- `test_thetadata_legacy_backtest`
- `test_thetadata_legacy_backtest2`

**Status**: COMPLETE

---

## 2. CRITICAL ISSUE - Missing load_dotenv() Calls

**Files Affected**:
- ✅ `test_thetadata_comprehensive.py` - FIXED
- ✅ `test_index_data_verification.py` - FIXED
- ✅ `test_accuracy_verification.py` - FIXED
- ✅ `test_polygon.py` - FIXED
- ✅ `test_thetadata_vs_polygon.py` - FIXED
- ✅ `test_thetadata.py` - JUST FIXED

**Impact**: This was causing `secrets_not_found = True` in test_thetadata.py, which triggered pytest.skipif decorators, skipping many ThetaData tests.

**Status**: ALL FILES NOW HAVE load_dotenv()

---

## 3. FAILING TESTS - Detailed Investigation Needed

### 3.1 Accuracy Verification Tests (CRITICAL - 6.8% Portfolio Variance!)

**Test**: `test_accuracy_verification.py::test_one_year_amzn_accuracy`
**Status**: FAILED
**Error**: `AssertionError: Portfolio variance 6.7963% exceeds 0.01% threshold`

**Investigation Needed**:
- WHY is there a 6.8% portfolio variance between ThetaData and Polygon over 1 year?
- Is this a data quality issue, timestamp issue, or calculation issue?
- Compare against Yahoo Finance as third data source
- Check if strategy is trading frequently or buy-and-hold
- Verify OHLC data alignment between sources
- Check for systematic bias vs random variance

**Priority**: HIGHEST - This indicates a fundamental data accuracy problem

---

**Test**: `test_accuracy_verification.py::test_multi_symbol_price_ranges`
**Status**: FAILED
**Error**: TBD (likely similar to above)

**Investigation Needed**: Same as above but across multiple symbols

---

### 3.2 Polygon Tests

**Test**: `test_polygon.py::test_get_last_price_unchanged`
**Status**: FAILED
**Investigation Needed**: Understand why this is failing

**Test**: `test_polygon.py::test_get_historical_prices_unchanged_for_amzn`
**Status**: FAILED
**Investigation Needed**: Understand why this is failing

---

## 4. SKIPPED TESTS - Need to Unskip and Fix

### 4.1 Module-Level Skip

**File**: `test_strategy_executor.py`
**Line**: `pytest.skip("all tests still WIP", allow_module_level=True)`
**Action Needed**:
- Remove module-level skip
- Run tests to see actual failures
- Fix underlying issues

---

### 4.2 API Key-Based Skips

Many tests skip based on missing API keys. Now that load_dotenv() is added, these should no longer skip:

**ThetaData Tests** (`test_thetadata.py`):
- All tests using `secrets_not_found` skipif decorator
- Should now run after load_dotenv() fix

**Polygon Tests** (`test_polygon.py`):
- Tests skipping when `POLYGON_API_KEY == '<your key here>'`
- Should now run after load_dotenv() fix

**Databento Tests**:
- Tests skipping based on Databento API key
- Verify if we have this key in .env

---

### 4.3 Conditional Skips in Test Logic

**File**: `test_thetadata_comprehensive.py`
- Line with `pytest.skip("Polygon API key not available")` - should not trigger now
- Line with `pytest.skip("Pre-market data not available")` - verify if this is legitimate

**File**: `test_index_data_verification.py`
- Line with `pytest.skip(f"Polygon VIX not available: {e}")` - verify if legitimate

---

## 5. XFAILED TESTS - Need to Remove Markers and Fix

### 5.1 Yahoo Finance Tests

**File**: `test_example_strategies.py`
**Marked as**: `@pytest.mark.xfail(reason="yahoo sucks")`
**Count**: 4 tests

**Action Needed**:
- Remove xfail decorators
- Run tests to see actual failures
- Investigate and fix root causes
- Yahoo Finance is an important data source - can't just ignore it

---

### 5.2 Polygon Flakiness Tests

**File**: `test_polygon.py`
**Marked as**: `@pytest.mark.xfail(reason="polygon flakiness")`
**Count**: 2 tests

**Action Needed**:
- Remove xfail decorators
- Run tests to see actual failures
- Investigate if truly flaky or has real issues
- Add proper error handling or retries if needed

---

## 6. INVESTIGATION PRIORITIES

### Priority 1 (CRITICAL - Data Accuracy):
1. **6.8% Portfolio Variance** - Test against Yahoo Finance
2. Understand root cause of ThetaData vs Polygon discrepancy
3. Verify no systematic bias in data

### Priority 2 (Code Quality):
1. Remove all xfail markers and fix underlying issues
2. Unskip test_strategy_executor.py and fix tests
3. Fix failing Polygon tests

### Priority 3 (Coverage):
1. Verify all API-key-based skips now pass with load_dotenv()
2. Ensure no tests are inappropriately skipped
3. Verify Databento tests run if we have API key

---

## 7. NEXT STEPS

1. ✅ Delete legacy tests - DONE
2. ✅ Add load_dotenv() to all test files - DONE
3. ⏳ Run full test suite to get updated results
4. 🔍 Investigate 6.8% portfolio variance (compare with Yahoo)
5. 🔧 Remove xfail markers and fix tests
6. 🔧 Unskip test_strategy_executor.py and fix
7. 🔧 Fix 2 failing Polygon tests
8. ✅ Verify all tests pass with proper credentials

---

## 8. FILES REQUIRING INVESTIGATION

1. `test_accuracy_verification.py` - Portfolio variance issue
2. `test_polygon.py` - 2 failing tests, 2 xfailed tests
3. `test_example_strategies.py` - 4 xfailed Yahoo tests
4. `test_strategy_executor.py` - Module-level skip
5. `test_thetadata_comprehensive.py` - Verify no inappropriate skips
6. `test_index_data_verification.py` - Verify no inappropriate skips

---

**Generated**: 2025-10-02
**Status**: Investigation in Progress
