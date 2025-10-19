# Manual 4-Test Protocol Commands

**Date:** October 17, 2025, 03:55 PM EST
**Purpose:** Run pandas/polars parity tests with LIVE manual supervision

**CRITICAL RULES:**
1. ✅ Run each command MANUALLY (copy/paste one at a time)
2. ✅ Watch logs LIVE with `tail -f` before moving to next test
3. ✅ Record PID, start time, and end time for each test
4. ✅ NO automation - you must be present watching logs
5. ✅ Pandas is truth - never modify pandas path

---

## QUICK CHECKLIST

Before you start, verify:
- [ ] All previous test processes killed (`ps aux | grep profile_weekly_momentum`)
- [ ] Unit tests passed live (3 passed, 1 skipped - ThetaData 474)
- [ ] Instrumentation confirmed working (logs firing correctly)
- [ ] ThetaTerminal accessible (check status below)
- [ ] Clear at least 4-8 hours per test for manual supervision

**The 4-Test Sequence:**
1. [ ] **Step 0:** Pre-flight checks (processes, cache, ThetaTerminal)
2. [ ] **Step 1:** Clear cache ONCE
3. [ ] **Step 2:** Run pandas cold, `tail -f`, record PID/times/network requests
4. [ ] **Step 3:** Run polars cold, `tail -f`, record PID/times/network requests
5. [ ] **Step 4:** Run pandas warm, `tail -f`, **verify network_requests = 0**
6. [ ] **Step 5:** Run polars warm, `tail -f`, **verify network_requests = 0**
7. [ ] **Step 6:** Compare all 4 trade CSVs with `diff -u`
8. [ ] **Step 7:** Document results with PIDs, timestamps, exit codes

**Expected Duration:** 16-32 hours total (each test: 4-8 hours)

---

## STEP 0: Pre-Flight Checks

### Check 1: No Running Test Processes

```bash
# Verify no test processes running
ps aux | grep -E "profile_weekly_momentum|run_parity|run_4_tests" | grep -v grep
# Should show: (empty - no results)

# If any processes found, kill them:
# pkill -f profile_weekly_momentum
```

**Expected:** No output (clean)

### Check 2: ThetaTerminal Status

```bash
# Check if ThetaTerminal is running
ps aux | grep -i "thetadata\|thetaterminal" | grep -v grep

# If running, note the PID
# Example output: robertgrzesik  12345  ...  java -jar ThetaTerminal.jar

# Check connection (optional - will auto-start if needed)
# curl -s http://127.0.0.1:25510/v2/list/exch 2>/dev/null || echo "ThetaTerminal not responding (will auto-start)"
```

**Expected:** Either running (note PID) or not running (tests will auto-start it)

**Record for documentation:**
```
ThetaTerminal PID: _____ (or "not running - will auto-start")
ThetaTerminal Status: CONNECTED / NOT_RUNNING
Timestamp: $(date)
```

### Check 3: Current Cache State

```bash
# Check current cache size
du -sh ~/Library/Caches/lumibot/1.0/thetadata

# Count cache files
ls -1 ~/Library/Caches/lumibot/1.0/thetadata 2>/dev/null | wc -l

# Show most recent cache files
ls -lht ~/Library/Caches/lumibot/1.0/thetadata | head -10
```

**Record current state before clearing:**
```
Cache size: _____
Cache file count: _____
```

---

## STEP 1: Clear Cache (ONCE ONLY)

```bash
# Clear the theta cache
rm -rf ~/Library/Caches/lumibot/1.0/thetadata
mkdir -p ~/Library/Caches/lumibot/1.0/thetadata

# Verify it's empty
ls -lh ~/Library/Caches/lumibot/1.0/thetadata
du -sh ~/Library/Caches/lumibot/1.0/thetadata
# Should show: 0B or empty directory
```

**IMPORTANT:** Only clear cache ONCE. Do NOT clear between tests.

---

## STEP 2: Run Pandas Cold (Populate Cache)

### Start the test

```bash
# Create unique log filename with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/parity_pandas_cold_${TIMESTAMP}.log"

echo "=========================================="
echo "TEST 1/4: PANDAS COLD"
echo "=========================================="
echo "Start time: $(date)"
echo "Log file: $LOG_FILE"
echo ""

# Start test in background
python3 tests/performance/profile_weekly_momentum.py --mode pandas > "$LOG_FILE" 2>&1 &
PANDAS_COLD_PID=$!

echo "Pandas cold started with PID: $PANDAS_COLD_PID"
echo ""

# Verify it's running
sleep 10
ps -p $PANDAS_COLD_PID -o pid,etime,command
echo ""
```

### Monitor LIVE (Required)

```bash
# Watch the log in real-time
tail -f logs/parity_pandas_cold_*.log

# In another terminal, check progress periodically:
ps -p $PANDAS_COLD_PID -o pid,etime,command
```

**What to watch for:**
- ThetaTerminal launches successfully
- Network requests happening (this is normal for cold run)
- No Python exceptions or tracebacks
- Progress updates from backtest

### When test completes

```bash
# Verify process finished
ps -p $PANDAS_COLD_PID
# Should show: (no matching processes)

# Extract key metrics
grep "network_requests=" logs/parity_pandas_cold_*.log | tail -1
grep "mode=pandas elapsed=" logs/parity_pandas_cold_*.log

# Find the trade CSV
ls -lh logs/WeeklyMomentumOptionsStrategy_*_trades.csv | tail -1

# Record these values:
# - PID: $PANDAS_COLD_PID
# - Start time: (from above)
# - End time: $(date)
# - Network requests: (from grep)
# - Elapsed time: (from grep)
# - Trade CSV: (filename)
```

---

## STEP 3: Run Polars Cold (Use Cache)

**DO NOT CLEAR CACHE - Polars should use pandas cache**

### Start the test

```bash
# Create unique log filename
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/parity_polars_cold_${TIMESTAMP}.log"

echo "=========================================="
echo "TEST 2/4: POLARS COLD"
echo "=========================================="
echo "Start time: $(date)"
echo "Log file: $LOG_FILE"
echo ""

# Start test in background
python3 tests/performance/profile_weekly_momentum.py --mode polars > "$LOG_FILE" 2>&1 &
POLARS_COLD_PID=$!

echo "Polars cold started with PID: $POLARS_COLD_PID"
echo ""

# Verify it's running
sleep 10
ps -p $POLARS_COLD_PID -o pid,etime,command
echo ""
```

### Monitor LIVE (Required)

```bash
# Watch the log in real-time
tail -f logs/parity_polars_cold_*.log

# In another terminal:
ps -p $POLARS_COLD_PID -o pid,etime,command
```

**What to watch for:**
- Uses existing cache from pandas run
- Network requests should still happen (cold cache)
- Check for `[BACKTESTER][DATA_CHECK]` logs
- Verify no Python exceptions

### When test completes

```bash
# Verify process finished
ps -p $POLARS_COLD_PID

# Extract metrics
grep "network_requests=" logs/parity_polars_cold_*.log | tail -1
grep "mode=polars elapsed=" logs/parity_polars_cold_*.log

# Find trade CSV
ls -lh logs/WeeklyMomentumOptionsStrategy_*_trades.csv | tail -2

# Record these values:
# - PID: $POLARS_COLD_PID
# - Start time: (from above)
# - End time: $(date)
# - Network requests: (from grep)
# - Elapsed time: (from grep)
# - Trade CSV: (filename)
```

### Compare pandas cold vs polars cold

```bash
# Find the two most recent trade CSVs
PANDAS_COLD_CSV=$(ls -t logs/WeeklyMomentumOptionsStrategy_*_trades.csv | sed -n '2p')
POLARS_COLD_CSV=$(ls -t logs/WeeklyMomentumOptionsStrategy_*_trades.csv | head -1)

echo "Pandas cold CSV: $PANDAS_COLD_CSV"
echo "Polars cold CSV: $POLARS_COLD_CSV"

# Count rows
echo "Pandas rows: $(wc -l < "$PANDAS_COLD_CSV")"
echo "Polars rows: $(wc -l < "$POLARS_COLD_CSV")"

# Compare files
diff -u "$PANDAS_COLD_CSV" "$POLARS_COLD_CSV"

# If diff shows differences:
echo "⚠ WARNING: Pandas and polars cold runs produced different trades!"

# If diff is silent (exit code 0):
echo "✓ SUCCESS: Pandas and polars cold runs are IDENTICAL"
```

---

## STEP 4: Run Pandas Warm (Verify Cache)

**Cache should now be fully populated - expect 0 network requests**

### Start the test

```bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/parity_pandas_warm_${TIMESTAMP}.log"

echo "=========================================="
echo "TEST 3/4: PANDAS WARM (Cache Verification)"
echo "=========================================="
echo "Start time: $(date)"
echo "Log file: $LOG_FILE"
echo "EXPECTING: network_requests = 0"
echo ""

python3 tests/performance/profile_weekly_momentum.py --mode pandas > "$LOG_FILE" 2>&1 &
PANDAS_WARM_PID=$!

echo "Pandas warm started with PID: $PANDAS_WARM_PID"
sleep 10
ps -p $PANDAS_WARM_PID -o pid,etime,command
echo ""
```

### Monitor LIVE (Required)

```bash
tail -f logs/parity_pandas_warm_*.log

# Watch for:
# - [THETA][CACHE][HIT] messages (should be all hits)
# - NO [THETA][CACHE][MISS] messages
# - NO network API requests
```

### When test completes

```bash
ps -p $PANDAS_WARM_PID

# CRITICAL CHECK: Network requests MUST be 0
grep "network_requests=" logs/parity_pandas_warm_*.log | tail -1

# If network_requests > 0:
echo "❌ FAILURE: Pandas warm made network requests (cache not working)"

# If network_requests = 0:
echo "✓ SUCCESS: Pandas warm made 0 network requests (cache working)"

# Extract metrics
grep "mode=pandas elapsed=" logs/parity_pandas_warm_*.log

# Find trade CSV
ls -lh logs/WeeklyMomentumOptionsStrategy_*_trades.csv | tail -1

# Record values
```

---

## STEP 5: Run Polars Warm (Verify Cache)

**Final test - polars should also use cache with 0 network requests**

### Start the test

```bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/parity_polars_warm_${TIMESTAMP}.log"

echo "=========================================="
echo "TEST 4/4: POLARS WARM (Cache Verification)"
echo "=========================================="
echo "Start time: $(date)"
echo "Log file: $LOG_FILE"
echo "EXPECTING: network_requests = 0"
echo ""

python3 tests/performance/profile_weekly_momentum.py --mode polars > "$LOG_FILE" 2>&1 &
POLARS_WARM_PID=$!

echo "Polars warm started with PID: $POLARS_WARM_PID"
sleep 10
ps -p $POLARS_WARM_PID -o pid,etime,command
echo ""
```

### Monitor LIVE (Required)

```bash
tail -f logs/parity_polars_warm_*.log

# Watch for:
# - [THETA][CACHE][HIT] messages
# - [BACKTESTER][DATA_CHECK] showing has_polars=True
# - NO network requests
```

### When test completes

```bash
ps -p $POLARS_WARM_PID

# CRITICAL CHECK: Network requests MUST be 0
grep "network_requests=" logs/parity_polars_warm_*.log | tail -1

# If network_requests > 0:
echo "❌ FAILURE: Polars warm made network requests (cache not working)"

# If network_requests = 0:
echo "✓ SUCCESS: Polars warm made 0 network requests (cache working)"

# Extract metrics
grep "mode=polars elapsed=" logs/parity_polars_warm_*.log

# Find trade CSV
ls -lh logs/WeeklyMomentumOptionsStrategy_*_trades.csv | tail -1

# Record values
```

---

## STEP 6: Final Comparisons

### Compare all 4 trade CSVs

```bash
# Get the 4 most recent trade CSVs (in reverse chronological order)
CSV_FILES=($(ls -t logs/WeeklyMomentumOptionsStrategy_*_trades.csv | head -4))

echo "=========================================="
echo "FINAL TRADE CSV COMPARISON"
echo "=========================================="
echo ""
echo "1. Pandas cold:  ${CSV_FILES[3]}"
echo "2. Polars cold:  ${CSV_FILES[2]}"
echo "3. Pandas warm:  ${CSV_FILES[1]}"
echo "4. Polars warm:  ${CSV_FILES[0]}"
echo ""

# Row counts
echo "Row counts:"
for f in "${CSV_FILES[@]}"; do
  echo "  $(basename $f): $(wc -l < "$f") rows"
done
echo ""

# Compare pandas cold vs pandas warm
echo "Comparing PANDAS COLD vs PANDAS WARM:"
diff -u "${CSV_FILES[3]}" "${CSV_FILES[1]}"
if [ $? -eq 0 ]; then
  echo "✓ IDENTICAL"
else
  echo "✗ DIFFERENT"
fi
echo ""

# Compare polars cold vs polars warm
echo "Comparing POLARS COLD vs POLARS WARM:"
diff -u "${CSV_FILES[2]}" "${CSV_FILES[0]}"
if [ $? -eq 0 ]; then
  echo "✓ IDENTICAL"
else
  echo "✗ DIFFERENT"
fi
echo ""

# Compare pandas vs polars (cold runs)
echo "Comparing PANDAS COLD vs POLARS COLD:"
diff -u "${CSV_FILES[3]}" "${CSV_FILES[2]}"
if [ $? -eq 0 ]; then
  echo "✓ IDENTICAL - PARITY ACHIEVED"
else
  echo "✗ DIFFERENT - PARITY BROKEN"
fi
echo ""

# Compare pandas vs polars (warm runs)
echo "Comparing PANDAS WARM vs POLARS WARM:"
diff -u "${CSV_FILES[1]}" "${CSV_FILES[0]}"
if [ $? -eq 0 ]; then
  echo "✓ IDENTICAL - PARITY ACHIEVED"
else
  echo "✗ DIFFERENT - PARITY BROKEN"
fi
echo ""
```

---

## STEP 7: Document Results

Create a summary in `tests/performance/TEST_RESULTS_OCT16_2025.md`:

```markdown
### Manual 4-Test Protocol Results (October 17, 2025)

**Run ID:** YYYYMMDD_HHMMSS

| Test | PID | Start Time | End Time | Elapsed | Network Requests | Trade CSV | Row Count |
|------|-----|------------|----------|---------|------------------|-----------|-----------|
| Pandas Cold | XXX | HH:MM:SS | HH:MM:SS | XXXXs | XXX | filename | XX |
| Polars Cold | XXX | HH:MM:SS | HH:MM:SS | XXXXs | XXX | filename | XX |
| Pandas Warm | XXX | HH:MM:SS | HH:MM:SS | XXXXs | 0 | filename | XX |
| Polars Warm | XXX | HH:MM:SS | HH:MM:SS | XXXXs | 0 | filename | XX |

**Parity Check Results:**

- Pandas cold == Pandas warm: [✓/✗]
- Polars cold == Polars warm: [✓/✗]
- Pandas cold == Polars cold: [✓/✗]
- Pandas warm == Polars warm: [✓/✗]

**Cache Verification:**

- Pandas warm network requests: [0/NON-ZERO]
- Polars warm network requests: [0/NON-ZERO]

**Conclusion:**

[Document whether parity was achieved and cache is working correctly]
```

---

## SUCCESS CRITERIA

✅ **All tests must meet these requirements:**

1. **Pandas cold == Polars cold** (exact trade CSV match)
2. **Pandas warm == Polars warm** (exact trade CSV match)
3. **Pandas warm: network_requests = 0** (cache working)
4. **Polars warm: network_requests = 0** (cache working)
5. **Cold == Warm for each implementation** (consistency)

If ANY of these fail, parity is NOT achieved.

---

## TROUBLESHOOTING

### Test crashes immediately

```bash
# Check the log for errors
tail -100 logs/parity_[mode]_[temp]_*.log

# Common issues:
# - Python version mismatch (need python3)
# - Missing dependencies
# - ThetaTerminal not accessible
```

### Network requests > 0 on warm run

```bash
# Search for cache miss reasons
grep "\[THETA\]\[CACHE\]\[MISS\]" logs/parity_[mode]_warm_*.log

# Check cache validation
grep "\[BACKTESTER\]\[DATA_CHECK\]" logs/parity_[mode]_warm_*.log

# Check cache file existence
ls -lh ~/Library/Caches/lumibot/1.0/thetadata/ | wc -l
```

### Trade CSVs don't match

```bash
# Show actual differences
diff -y --width=200 pandas_trades.csv polars_trades.csv | head -50

# Common issues:
# - Timestamp differences
# - OHLC value differences
# - Missing/extra trades
```

---

## REMEMBER

**Manual supervision is mandatory.**
**Live log monitoring is required.**
**Pandas is truth - fix only polars path.**
**Document everything - PIDs, timestamps, exit codes.**
