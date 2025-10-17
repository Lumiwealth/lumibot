# ThetaData Cache & Parity Testing Protocol

**Date Created:** October 16, 2025, 10:26 PM EST
**Current Date:** October 16, 2025
**Current Time:** 10:26 PM EST

## CRITICAL: Why This Protocol Exists

I (Claude) have made MULTIPLE critical mistakes testing ThetaData:
1. ❌ Running cold and warm tests simultaneously instead of sequentially
2. ❌ Clearing cache BETWEEN pandas and polars tests (breaking cache sharing)
3. ❌ Using old log files from hours/days ago instead of current run
4. ❌ Not monitoring tests while they run (missing failures)
5. ❌ Not checking dates properly - using logs from before changes were made

**This protocol prevents these mistakes from happening again.**

---

## The Problem We're Solving

**Original Issue:**
- Pandas mode: 1,124 network requests on "warm" run (should be 0)
- Polars mode: 83 network requests on "warm" run (should be 0)
- Portfolio value mismatch: $134K (polars) vs $146K (pandas)

**Previous Test Results (Oct 16, 12:31 PM):**
- Polars cold run: 7,797 network requests in 24,402 seconds (6.78 hours)
- Cache files were created, but cache validation kept failing during backtest
- Even with cache present, warm runs were hitting the network

**Root Cause Hypothesis:**
- Cache validation logic in backtester is too strict or incorrect
- Filtering inconsistencies between cold (API fetch) and warm (cache load) paths
- Pandas and Polars may have different filtering behavior

---

## The Game Plan (Original Design)

### Phase 1: Add Comprehensive Logging ✅ COMPLETED

**Files Modified:**
1. `lumibot/tools/thetadata_helper.py` (~50 logs)
   - Cache decision logic
   - Network requests
   - Date filtering (start/end/dt parameters)
   - FIXED: dt shadowing bug at line 342 (renamed `import datetime as dt` to `import datetime as datetime_module`)

2. `lumibot/backtesting/thetadata_backtesting_polars.py` (~40 logs)
   - Cache validation logic
   - End date validation
   - Why cache misses happen

3. `lumibot/backtesting/thetadata_backtesting_pandas.py` (~40 logs)
   - Same as polars (keeping parity)

4. `lumibot/entities/data.py` & `data_polars.py` (~10 logs each)
   - Final hop: what data gets handed to strategy
   - Look-ahead bias detection

**Total:** ~150 DEBUG logs added

### Phase 2: Create Comprehensive Test Suite ✅ COMPLETED

**Test File:** `tests/test_thetadata_cache_and_parity.py`

**Test Matrix:**
- **Assets:** stock (HIMS), option (HIMS 8/16 $22 CALL), index (SPX)
- **Timeframes:** minute, 5minute, 15minute, hour, day
- **Total:** 26 tests

**Test Classes:**
1. `TestCacheWarmth`: Verify warm run = 0 network requests
2. `TestPandasPolarsParity`: Verify pandas == polars (same data, no look-ahead bias)

**Test Results (2min 36sec):**
- ✅ 6 tests PASSED
- ❌ 20 tests FAILED (EXPECTED - discovery tests)
- Cache IS working in isolated tests (warm runs = 0 network requests)
- Failures are data consistency issues (cold vs warm, pandas vs polars)

### Phase 3: Run Real Backtest Tests ⏳ IN PROGRESS

This is where we are now. We need to run the weekly_momentum backtest properly.

---

## How To Run Tests CORRECTLY

### Test Setup

**Cache Location:** `/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/`

**Test Script:** `tests/performance/profile_weekly_momentum.py`

**Backtest Parameters:**
- Date range: July 1 - December 31, 2024 (6 months)
- Strategy: Weekly Momentum Options
- Assets: 10 momentum stocks

### THE CORRECT TEST SEQUENCE

**❌ WRONG (what I did):**
```bash
# This runs them one after another, clearing cache each time
Clear cache → Pandas cold → Pandas warm → Clear cache → Polars cold → Polars warm
```

**✅ CORRECT (what we need):**

```bash
# Phase 1: Cold Runs (populate cache)
Clear entire cache
Run pandas cold    # Populates cache
Run polars cold    # Uses/validates same cache

# Phase 2: Warm Runs (verify cache usage)
Run pandas warm    # Should be 0 network requests
Run polars warm    # Should be 0 network requests
```

### Execution Commands

```bash
# Current date check
date
# Should show: Oct 16 22:XX:XX EDT 2025

# Step 1: Clear cache
rm -rf /Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/*
echo "Cache cleared at $(date)"

# Step 2: Run pandas cold
python3 tests/performance/profile_weekly_momentum.py --mode pandas \
  > logs/pandas_cold_$(date +%Y%m%d_%H%M%S).log 2>&1
echo "Pandas cold complete at $(date)"

# Step 3: Run polars cold (DO NOT clear cache!)
python3 tests/performance/profile_weekly_momentum.py --mode polars \
  > logs/polars_cold_$(date +%Y%m%d_%H%M%S).log 2>&1
echo "Polars cold complete at $(date)"

# Step 4: Run pandas warm (reuse cache)
python3 tests/performance/profile_weekly_momentum.py --mode pandas \
  > logs/pandas_warm_$(date +%Y%m%d_%H%M%S).log 2>&1
echo "Pandas warm complete at $(date)"

# Step 5: Run polars warm (reuse cache)
python3 tests/performance/profile_weekly_momentum.py --mode polars \
  > logs/polars_warm_$(date +%Y%m%d_%H%M%S).log 2>&1
echo "Polars warm complete at $(date)"
```

---

## What To Check After Tests Run

### 1. Verify Log File Dates

**CRITICAL:** Only use logs from AFTER 10:00 PM on October 16, 2025.

```bash
# Check log file timestamps
ls -lh logs/pandas_cold_*.log logs/polars_cold_*.log \
  logs/pandas_warm_*.log logs/polars_warm_*.log | \
  awk '{print $6, $7, $8, $9}'
```

Any file dated before Oct 16 22:00 is INVALID.

### 2. Extract Key Metrics

**CRITICAL:** We need to extract BOTH caching metrics AND portfolio/trading metrics.

For each log file, extract:

```bash
# === CACHING METRICS ===
# Network requests (should be 0 for warm runs)
grep "network_requests=" LOG_FILE | tail -1

# Execution time
grep "elapsed=" LOG_FILE | tail -1

# Cache hit/miss ratio
grep -c "\[DEBUG\]\[CACHE\]\[HIT\]" LOG_FILE
grep -c "\[DEBUG\]\[CACHE\]\[MISS\]" LOG_FILE

# === PORTFOLIO METRICS ===
# Annual Return
grep "Annual Return" LOG_FILE | tail -1

# Max Drawdown
grep "Max Drawdown" LOG_FILE | tail -1

# Total Return
grep "Total Return" LOG_FILE | tail -1

# Sharpe Ratio
grep "Sharpe" LOG_FILE | tail -1

# Final Portfolio Value
grep -E "(portfolio_value|Portfolio Value)" LOG_FILE | tail -5

# === TRADING METRICS ===
# Number of trades
grep -c "\[BROKER_FILL\]" LOG_FILE

# Extract all trades (for comparison)
grep "\[BROKER_FILL\]" LOG_FILE > ${LOG_FILE%.log}_trades.txt

# Sample of trades (first 10)
grep "\[BROKER_FILL\]" LOG_FILE | head -10
```

### 3. Expected Results Table

| Test | Network Requests | Time (sec) | Expected |
|------|-----------------|------------|----------|
| Pandas Cold | ~1000-2000 | 4-8 hours | First run, populates cache |
| Polars Cold | ~1000-2000 | 4-8 hours | Should use/share cache with pandas |
| Pandas Warm | **0** | 1-2 hours | MUST be zero |
| Polars Warm | **0** | 1-2 hours | MUST be zero |

### 4. Portfolio Metrics Comparison

**CRITICAL:** We need to verify data parity between pandas and polars modes.

| Test | Annual Return | Max Drawdown | Total Return | Sharpe Ratio | # Trades | Portfolio Value |
|------|---------------|--------------|--------------|--------------|----------|-----------------|
| Pandas Cold | ? | ? | ? | ? | ? | ? |
| Pandas Warm | (should match cold) | (should match cold) | (should match cold) | (should match cold) | (should match cold) | (should match cold) |
| Polars Cold | ? | ? | ? | ? | ? | ? |
| Polars Warm | (should match cold) | (should match cold) | (should match cold) | (should match cold) | (should match cold) | (should match cold) |

**Critical Checks:**
1. **Pandas results MUST MATCH Polars results** (both cold and warm)
2. **Cold and warm runs MUST produce identical results** (same mode)
3. **Individual trades MUST match** between pandas and polars:
   - Same symbols
   - Same strikes (for options)
   - Same fill prices
   - Same timestamps
   - Same quantities

**To compare trades:**
```bash
# Extract trades from each run
grep "\[BROKER_FILL\]" logs/pandas_cold_*.log > pandas_cold_trades.txt
grep "\[BROKER_FILL\]" logs/polars_cold_*.log > polars_cold_trades.txt

# Compare trades
diff pandas_cold_trades.txt polars_cold_trades.txt
# Should show NO differences (exit code 0)

# Count trades
wc -l pandas_cold_trades.txt polars_cold_trades.txt
# Should show same number of lines
```

---

## Analyzing Test Failures

### If Warm Run Has Network Requests > 0

**This means cache validation is failing.**

Search logs for:
```bash
grep "\[DEBUG\]\[CACHE\]\[DECISION_RESULT\].*CACHE_MISS" LOG_FILE
grep "\[DEBUG\]\[BACKTEST\]\[CACHE_VALIDATION\]" LOG_FILE
grep "\[DEBUG\]\[BACKTEST\]\[END_VALIDATION\].*end_ok=FALSE" LOG_FILE
```

Common causes:
- `existing_start > start_threshold` (cache doesn't go back far enough)
- `existing_end < end_requirement` (cache doesn't extend far enough)
- `existing_rows < requested_length` (not enough rows)
- Date/timezone mismatches

### If Pandas ≠ Polars

**This means data parity is broken.**

Search logs for:
```bash
grep "\[DEBUG\]\[FILTER\]\[DT_RESULT\]" LOG_FILE
grep "\[DEBUG\]\[DATA\]\[GET_BARS\]\[RETURN\]" LOG_FILE
grep "LOOK_AHEAD_BIAS" LOG_FILE
```

Check:
- Are timestamps identical?
- Are OHLC values identical?
- Are row counts identical?
- Is look-ahead bias present?

### If Cold vs Warm Data Differs

**This means filtering is inconsistent.**

Compare:
```bash
# Cold run
grep "\[THETA\]\[RETURN\]" logs/pandas_cold*.log | head -20

# Warm run
grep "\[THETA\]\[RETURN\]" logs/pandas_warm*.log | head -20
```

Look for:
- Different row counts
- Different timestamp ranges
- Pre-market or after-hours data in one but not the other

---

## Next Steps After Test Results

### Scenario 1: Warm runs have network_requests = 0, but pandas ≠ polars

**Action:** Fix parity issues
- Investigate filtering differences
- Check timezone handling
- Look for polars-specific bugs (e.g., `repair_times_and_fill`)

### Scenario 2: Warm runs have network_requests > 0

**Action:** Fix cache validation
- Analyze `[DEBUG][BACKTEST][CACHE_VALIDATION]` logs
- Fix overly strict validation logic
- Ensure start/end date requirements are correct

### Scenario 3: Cold vs warm data differs

**Action:** Fix filtering consistency
- Ensure both code paths apply same filters
- Check dt parameter handling
- Verify start/end date filtering

---

## Common Mistakes To Avoid

1. ❌ **Don't clear cache between pandas and polars tests**
   - They should SHARE the cache to test compatibility

2. ❌ **Don't use old log files**
   - Always check timestamps: Oct 16 after 10:00 PM only

3. ❌ **Don't run tests in parallel**
   - Run sequentially: both colds, then both warms

4. ❌ **Don't forget to monitor**
   - Watch logs while tests run
   - Check for errors immediately

5. ❌ **Don't assume cache is working**
   - Verify network_requests = 0 on warm runs
   - Check actual cache file contents

---

## Success Criteria

✅ **Phase 3 Complete When:**
1. **Caching works perfectly:**
   - Pandas warm: 0 network requests ✅
   - Polars warm: 0 network requests ✅
   - Cache hit rate: 100% on warm runs ✅

2. **Data parity verified:**
   - Annual Return: pandas == polars ✅
   - Max Drawdown: pandas == polars ✅
   - Total Return: pandas == polars ✅
   - Sharpe Ratio: pandas == polars ✅
   - Number of trades: pandas == polars ✅
   - Individual trades match exactly ✅
   - Portfolio values match exactly ✅

3. **No data issues:**
   - No look-ahead bias detected ✅
   - Cold and warm runs return identical data ✅
   - Cache validation logs show clear reasoning ✅

✅ **Project Complete When:**
1. All tests in `test_thetadata_cache_and_parity.py` pass
2. Real backtest shows 0 network requests on warm run
3. Portfolio values match between pandas and polars
4. All trades match between pandas and polars
5. No unexplained cache misses in logs

---

## Status Tracking

**Current Status:** Phase 3 - Running real backtest tests

**Completed:**
- [x] Phase 1: Add comprehensive logging (~150 logs)
- [x] Phase 2: Create test suite (26 tests)
- [ ] Phase 3: Run real backtest tests correctly
- [ ] Phase 4: Analyze results and fix issues
- [ ] Phase 5: Verify all tests pass

**Last Updated:** October 16, 2025, 10:26 PM EST
