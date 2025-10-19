# ThetaData Cache & Parity Validation Test Results

**Test Date:** October 17, 2025, 8:24 PM - 8:30 PM EDT
**Tester:** Claude (AI Assistant)
**Test Script:** `tests/performance/profile_weekly_momentum.py`
**Protocol:** `tests/performance/THETADATA_TESTING_PROTOCOL.md`

---

## Test Configuration

### Test Parameters
```python
BACKTEST_START = datetime(2024, 7, 15)
BACKTEST_END = datetime(2024, 7, 26)  # 2 weeks
STRATEGY_PARAMS = {}
BUDGET = 100000
```

### Cache Location
```
/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/
```

### Test Sequence (4-Pass Protocol)
```bash
# Pass 1: Pandas Cold (cache population)
rm -rf /Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/*
python tests/performance/profile_weekly_momentum.py --mode pandas \
  > logs/pandas_cold_20251017_202406.log 2>&1

# Pass 2: Polars Cold (verify cache sharing)
python tests/performance/profile_weekly_momentum.py --mode polars \
  > logs/polars_cold_20251017_202629.log 2>&1

# Pass 3: Pandas Warm (verify cache effectiveness)
python tests/performance/profile_weekly_momentum.py --mode pandas \
  > logs/pandas_warm_20251017_202700.log 2>&1

# Pass 4: Polars Warm (verify cache effectiveness)
python tests/performance/profile_weekly_momentum.py --mode polars \
  > logs/polars_warm_20251017_202730.log 2>&1
```

---

## Test Results

### 1. Caching Metrics

| Test         | Log File                            | Network Requests | Time (sec) | Status |
|--------------|-------------------------------------|------------------|------------|--------|
| Pandas Cold  | `logs/pandas_cold_20251017_202406.log` | 107              | 59.30s     | ✅ PASS |
| Polars Cold  | `logs/polars_cold_20251017_202629.log` | **0**            | 3.93s      | ✅ PASS |
| Pandas Warm  | `logs/pandas_warm_20251017_202700.log` | **0**            | 3.93s      | ✅ PASS |
| Polars Warm  | `logs/polars_warm_20251017_202730.log` | **0**            | 3.96s      | ✅ PASS |

**Verification Commands:**
```bash
# Extract network request counts
grep "network_requests=" logs/pandas_cold_20251017_202406.log | tail -1
# Output: [theta diagnostics] mode=pandas network_requests=107 ...

grep "network_requests=" logs/polars_cold_20251017_202629.log | tail -1
# Output: [theta diagnostics] mode=polars network_requests=0 ...

grep "network_requests=" logs/pandas_warm_20251017_202700.log | tail -1
# Output: [theta diagnostics] mode=pandas network_requests=0 ...

grep "network_requests=" logs/polars_warm_20251017_202730.log | tail -1
# Output: [theta diagnostics] mode=polars network_requests=0 ...
```

**✅ CACHE SHARING VERIFIED:** Polars cold run had 0 network requests, confirming it used the cache populated by pandas cold.

---

### 2. Portfolio Metrics

| Test         | Stats File                                                    | Final Value | Return | Cash        |
|--------------|---------------------------------------------------------------|-------------|--------|-------------|
| Pandas Cold  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_stats.csv` | $100,000.00 | 0.0%   | $100,000.00 |
| Polars Cold  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_stats.csv` | $100,000.00 | 0.0%   | $100,000.00 |
| Pandas Warm  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_stats.csv` | $100,000.00 | 0.0%   | $100,000.00 |
| Polars Warm  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_stats.csv` | $100,000.00 | 0.0%   | $100,000.00 |

**Verification Commands:**
```bash
# Extract final portfolio values
tail -2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_stats.csv
tail -2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_stats.csv
tail -2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_stats.csv
tail -2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_stats.csv
# All show: 2024-07-26 16:00:00-04:00,100000.0,100000.0,[],0.0
```

**✅ PORTFOLIO PARITY VERIFIED:** All 4 runs ended with identical portfolio values.

---

### 3. Trade Comparison

| Test         | Trades File                                                         | Orders Placed | Trade Details |
|--------------|---------------------------------------------------------------------|---------------|---------------|
| Pandas Cold  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_trades.csv` | 1             | HIMS CALL $22 exp 2024-08-09 @ 2024-07-25 09:30:00 |
| Polars Cold  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_trades.csv` | 1             | HIMS CALL $22 exp 2024-08-09 @ 2024-07-25 09:30:00 |
| Pandas Warm  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_trades.csv` | 1             | HIMS CALL $22 exp 2024-08-09 @ 2024-07-25 09:30:00 |
| Polars Warm  | `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_trades.csv` | 1             | HIMS CALL $22 exp 2024-08-09 @ 2024-07-25 09:30:00 |

**Diff Verification:**
```bash
# Compare trades between pandas and polars (cold runs)
diff \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_trades.csv | cut -d',' -f1,3-) \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_trades.csv | cut -d',' -f1,3-)
# Exit code: 0 (IDENTICAL except for UUID identifier column)

# Compare trades between cold and warm runs (pandas)
diff \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_trades.csv | cut -d',' -f1,3-) \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_trades.csv | cut -d',' -f1,3-)
# Exit code: 0 (IDENTICAL except for UUID identifier column)

# Compare trades between cold and warm runs (polars)
diff \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_trades.csv | cut -d',' -f1,3-) \
  <(tail -n +2 logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_trades.csv | cut -d',' -f1,3-)
# Exit code: 0 (IDENTICAL except for UUID identifier column)
```

**Trade Details (All 4 Runs Identical):**
```csv
time,symbol,side,type,status,multiplier,time_in_force,asset.right,asset.strike,asset.multiplier,asset.expiration,asset.asset_type
2024-07-25 09:30:00-04:00,HIMS,buy,limit,new,1,gtc,CALL,22.0,100,2024-08-09,option
```

**✅ TRADE PARITY VERIFIED:** All 4 runs placed identical trades at identical timestamps.

---

### 4. Cache Files Created

**Cache Directory Contents:**
```bash
ls -lh /Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/*.parquet
```

**Total:** 15 parquet files, 432K total size

**File List:**
```
-rw-r--r--  7.4K  option_HIMS_240809_22.0_CALL_minute_ohlc.parquet
-rw-r--r--   46K  option_HIMS_240809_22.0_CALL_minute_quote.parquet
-rw-r--r--  8.6K  stock_APP_day_ohlc.parquet
-rw-r--r--  8.5K  stock_CCJ_day_ohlc.parquet
-rw-r--r--  8.6K  stock_CVNA_day_ohlc.parquet
-rw-r--r--  8.5K  stock_HIMS_day_ohlc.parquet
-rw-r--r--   70K  stock_HIMS_minute_ohlc.parquet
-rw-r--r--   63K  stock_HIMS_minute_quote.parquet
-rw-r--r--  8.6K  stock_HOOD_day_ohlc.parquet
-rw-r--r--  8.5K  stock_NRG_day_ohlc.parquet
-rw-r--r--  8.5K  stock_PLTR_day_ohlc.parquet
-rw-r--r--  8.6K  stock_STX_day_ohlc.parquet
-rw-r--r--  8.6K  stock_VST_day_ohlc.parquet
-rw-r--r--  8.6K  stock_WDC_day_ohlc.parquet
```

**✅ CACHE FILES VERIFIED:** All files created during pandas cold run, reused by subsequent runs.

---

## Summary & Assessment

### ✅ Tests Passed

1. **Cache Sharing Works:** Polars cold run had 0 network requests (used pandas cache)
2. **Cache Effectiveness:** All warm runs had 0 network requests (100% cache hit rate)
3. **Data Parity:** Pandas == Polars (portfolio values, trades, timestamps all identical)
4. **Cold vs Warm Consistency:** Same results regardless of cache state

### ⚠️ Test Limitations

1. **Test Period Too Short (2 weeks):**
   - Only 1 order placed (HIMS CALL $22 on July 25)
   - Order status = "new" (never filled before backtest ended on July 26)
   - Portfolio unchanged ($100K → $100K, 0% return)
   - No actual fills, exits, or P&L to validate

2. **Missing Portfolio Statistics:**
   - Annual Return: NOT AVAILABLE (tearsheet disabled)
   - Max Drawdown: NOT AVAILABLE (tearsheet disabled)
   - Sharpe Ratio: NOT AVAILABLE (tearsheet disabled)
   - Win Rate: NOT AVAILABLE (no fills)

3. **Limited Trading Activity:**
   - Strategy trades only on Thursdays (weekly_momentum logic)
   - Test included only 2 Thursdays (July 18, July 25)
   - First Thursday: no trades (ranking phase)
   - Second Thursday: 1 order placed
   - Need longer test period (≥1 month) to validate fills, exits, and look-ahead mechanics

### 📋 Next Steps

1. **Extend Test Period:**
   - Recommended: 1-3 months (July 1 - October 1, 2024)
   - This will capture 8-12 Thursdays with multiple fills and exits
   - Will validate P&L calculations, TP/SL logic, and look-ahead bias

2. **Enable Tearsheet (Optional):**
   - Modify `profile_weekly_momentum.py` line 69-70:
     ```python
     show_tearsheet=True
     save_tearsheet=True
     ```
   - This will generate Annual Return, Max Drawdown, Sharpe Ratio, etc.

3. **Run Full Validation:**
   - Clear cache
   - Run 4-pass sequence with extended dates
   - Document results in new markdown file
   - Verify diff outputs with actual fills and P&L

---

## Conclusion

**The timeshift type annotation fix is VALIDATED for basic functionality:**
- ✅ Cache sharing between pandas and polars works
- ✅ Data parity confirmed (identical orders, timestamps, portfolio values)
- ✅ No regressions in cache validation logic

**However, the current test is FRAGILE:**
- ❌ Test window too short (only 1 pending order, no fills)
- ❌ Cannot validate P&L calculations or exit logic
- ❌ Cannot validate look-ahead bias with actual trade fills

**Recommendation:** Re-run with extended test period (1-3 months) to validate real trading scenarios before considering this complete.

---

## Artifact Locations

**Log Files:**
- Pandas Cold: `logs/pandas_cold_20251017_202406.log`
- Polars Cold: `logs/polars_cold_20251017_202629.log`
- Pandas Warm: `logs/pandas_warm_20251017_202700.log`
- Polars Warm: `logs/polars_warm_20251017_202730.log`

**Stats CSV Files:**
- Pandas Cold: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_stats.csv`
- Polars Cold: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_stats.csv`
- Pandas Warm: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_stats.csv`
- Polars Warm: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_stats.csv`

**Trades CSV Files:**
- Pandas Cold: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-24_3jZfRI_trades.csv`
- Polars Cold: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-26_LTEG13_trades.csv`
- Pandas Warm: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_SV9bg8_trades.csv`
- Polars Warm: `logs/WeeklyMomentumOptionsStrategy_2025-10-17_20-27_ticPRj_trades.csv`

**Cache Directory:**
- `/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata/` (15 parquet files, 432K)
