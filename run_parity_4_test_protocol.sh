#!/bin/bash

################################################################################
# 4-TEST PARITY PROTOCOL
# Tests pandas/polars parity with cold and warm cache runs
################################################################################

CACHE_DIR=~/Library/Caches/lumibot/1.0/thetadata
RUN_ID=$(date +%Y%m%d_%H%M%S)
LOG_DIR=logs
MONITOR_LOG="$LOG_DIR/parity_4_test_protocol_${RUN_ID}.log"
CHECK_INTERVAL=${CHECK_INTERVAL:-60}
MAX_WAIT_MINUTES=${MAX_WAIT_MINUTES:-360}
MAX_WAIT_SECONDS=$((MAX_WAIT_MINUTES * 60))
ARCHIVE_DIR="$LOG_DIR/parity_${RUN_ID}"
SUMMARY_FILE="$ARCHIVE_DIR/summary.txt"

# Allow override via environment but default to python3
PYTHON=${PYTHON:-python3}

mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
RUN_STARTED_EPOCH=$(date +%s)
RUN_STARTED_ISO=$(date +%Y-%m-%dT%H:%M:%S%z)
RUN_STARTED_HUMAN=$(date)

echo "================================================================================" | tee "$MONITOR_LOG"
echo "4-TEST PARITY PROTOCOL" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Run ID: $RUN_ID" | tee -a "$MONITOR_LOG"
echo "Started: $RUN_STARTED_HUMAN" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# TEST 1: PANDAS COLD (populate cache)
################################################################################

echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "TEST 1/4: PANDAS COLD (populating cache)" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Clearing ThetaData cache directory: $CACHE_DIR" | tee -a "$MONITOR_LOG"
$PYTHON - <<PY
from pathlib import Path
import shutil
cache_dir = Path("$CACHE_DIR").expanduser()
if cache_dir.exists():
    shutil.rmtree(cache_dir)
cache_dir.mkdir(parents=True, exist_ok=True)
print(f"[CACHE RESET] {cache_dir}")
PY
echo "Cache cleared. Starting pandas test to populate cache..." | tee -a "$MONITOR_LOG"
echo "Start time: $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

PANDAS_COLD_LOG="$LOG_DIR/parity_pandas_cold_${RUN_ID}.log"
$PYTHON tests/performance/profile_weekly_momentum.py --mode pandas > "$PANDAS_COLD_LOG" 2>&1 &
PANDAS_COLD_PID=$!

echo "Pandas cold started with PID: $PANDAS_COLD_PID" | tee -a "$MONITOR_LOG"
echo "Log file: $PANDAS_COLD_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Verify it's running
sleep 10
if ! ps -p $PANDAS_COLD_PID > /dev/null 2>&1; then
    echo "ERROR: Pandas cold test failed to start!" | tee -a "$MONITOR_LOG"
    echo "Check log: $PANDAS_COLD_LOG" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Test running. Monitoring progress every ${CHECK_INTERVAL}s (timeout ${MAX_WAIT_MINUTES}m)..." | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Wait for pandas cold to complete
PANDAS_COLD_START=$(date +%s)
while ps -p $PANDAS_COLD_PID > /dev/null 2>&1; do
    sleep "$CHECK_INTERVAL"
    if ! ps -p $PANDAS_COLD_PID > /dev/null 2>&1; then
        break
    fi
    CURRENT_TIME=$(date +%s)
    ELAPSED_SECS=$((CURRENT_TIME - PANDAS_COLD_START))
    ELAPSED_HMS=$(printf "%02d:%02d:%02d" $((ELAPSED_SECS/3600)) $(((ELAPSED_SECS%3600)/60)) $((ELAPSED_SECS%60)))
    echo "[$(date +%H:%M:%S)] Pandas cold still running (elapsed: $ELAPSED_HMS)" | tee -a "$MONITOR_LOG"
    if [ "$ELAPSED_SECS" -gt "$MAX_WAIT_SECONDS" ]; then
        echo "ERROR: Pandas cold exceeded ${MAX_WAIT_MINUTES} minutes. Terminating run." | tee -a "$MONITOR_LOG"
        kill "$PANDAS_COLD_PID" >/dev/null 2>&1
        wait "$PANDAS_COLD_PID" 2>/dev/null
        exit 1
    fi
done

echo "" | tee -a "$MONITOR_LOG"
echo "Pandas cold test completed at $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Find pandas cold trade CSV
sleep 5
PANDAS_COLD_CSV=$(ls -t $LOG_DIR/WeeklyMomentumOptionsStrategy_*_trades.csv 2>/dev/null | head -1)

if [ -z "$PANDAS_COLD_CSV" ]; then
    echo "ERROR: No trade CSV found for pandas cold!" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Pandas cold trade CSV: $PANDAS_COLD_CSV" | tee -a "$MONITOR_LOG"
PANDAS_COLD_COUNT=$(wc -l < "$PANDAS_COLD_CSV")
PANDAS_COLD_COUNT=${PANDAS_COLD_COUNT//[[:space:]]/}
echo "Pandas cold trade count: $PANDAS_COLD_COUNT" | tee -a "$MONITOR_LOG"
PANDAS_COLD_ARCHIVE="$ARCHIVE_DIR/pandas_cold_trades.csv"
cp "$PANDAS_COLD_CSV" "$PANDAS_COLD_ARCHIVE"
cp "$PANDAS_COLD_LOG" "$ARCHIVE_DIR/"

# Extract network requests from log
PANDAS_COLD_REQUESTS=$(grep "network_requests=" "$PANDAS_COLD_LOG" 2>/dev/null | tail -1 | sed 's/.*network_requests=\([0-9]*\).*/\1/')
if [ -z "$PANDAS_COLD_REQUESTS" ]; then
    PANDAS_COLD_REQUESTS="N/A"
fi
echo "Pandas cold network requests: $PANDAS_COLD_REQUESTS" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# TEST 2: POLARS COLD (use cache)
################################################################################

echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "TEST 2/4: POLARS COLD (using cached data)" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Cache populated. Starting polars test with same cache..." | tee -a "$MONITOR_LOG"
echo "Start time: $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

POLARS_COLD_LOG="$LOG_DIR/parity_polars_cold_${RUN_ID}.log"
$PYTHON tests/performance/profile_weekly_momentum.py --mode polars > "$POLARS_COLD_LOG" 2>&1 &
POLARS_COLD_PID=$!

echo "Polars cold started with PID: $POLARS_COLD_PID" | tee -a "$MONITOR_LOG"
echo "Log file: $POLARS_COLD_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Verify it's running
sleep 10
if ! ps -p $POLARS_COLD_PID > /dev/null 2>&1; then
    echo "ERROR: Polars cold test failed to start!" | tee -a "$MONITOR_LOG"
    echo "Check log: $POLARS_COLD_LOG" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Test running. Monitoring progress every ${CHECK_INTERVAL}s (timeout ${MAX_WAIT_MINUTES}m)..." | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Wait for polars cold to complete
POLARS_COLD_START=$(date +%s)
while ps -p $POLARS_COLD_PID > /dev/null 2>&1; do
    sleep "$CHECK_INTERVAL"
    if ! ps -p $POLARS_COLD_PID > /dev/null 2>&1; then
        break
    fi
    CURRENT_TIME=$(date +%s)
    ELAPSED_SECS=$((CURRENT_TIME - POLARS_COLD_START))
    ELAPSED_HMS=$(printf "%02d:%02d:%02d" $((ELAPSED_SECS/3600)) $(((ELAPSED_SECS%3600)/60)) $((ELAPSED_SECS%60)))
    echo "[$(date +%H:%M:%S)] Polars cold still running (elapsed: $ELAPSED_HMS)" | tee -a "$MONITOR_LOG"
    if [ "$ELAPSED_SECS" -gt "$MAX_WAIT_SECONDS" ]; then
        echo "ERROR: Polars cold exceeded ${MAX_WAIT_MINUTES} minutes. Terminating run." | tee -a "$MONITOR_LOG"
        kill "$POLARS_COLD_PID" >/dev/null 2>&1
        wait "$POLARS_COLD_PID" 2>/dev/null
        exit 1
    fi
done

echo "" | tee -a "$MONITOR_LOG"
echo "Polars cold test completed at $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Find polars cold trade CSV (most recent, should be different from pandas)
sleep 5
POLARS_COLD_CSV=$(ls -t $LOG_DIR/WeeklyMomentumOptionsStrategy_*_trades.csv 2>/dev/null | head -1)

if [ -z "$POLARS_COLD_CSV" ]; then
    echo "ERROR: No trade CSV found for polars cold!" | tee -a "$MONITOR_LOG"
    exit 1
fi

if [ "$POLARS_COLD_CSV" = "$PANDAS_COLD_CSV" ]; then
    echo "ERROR: Polars cold CSV is same as pandas cold CSV!" | tee -a "$MONITOR_LOG"
    echo "This means polars test didn't create a new CSV file." | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Polars cold trade CSV: $POLARS_COLD_CSV" | tee -a "$MONITOR_LOG"
POLARS_COLD_COUNT=$(wc -l < "$POLARS_COLD_CSV")
POLARS_COLD_COUNT=${POLARS_COLD_COUNT//[[:space:]]/}
echo "Polars cold trade count: $POLARS_COLD_COUNT" | tee -a "$MONITOR_LOG"
POLARS_COLD_ARCHIVE="$ARCHIVE_DIR/polars_cold_trades.csv"
cp "$POLARS_COLD_CSV" "$POLARS_COLD_ARCHIVE"
cp "$POLARS_COLD_LOG" "$ARCHIVE_DIR/"

# Extract network requests from log
POLARS_COLD_REQUESTS=$(grep "network_requests=" "$POLARS_COLD_LOG" 2>/dev/null | tail -1 | sed 's/.*network_requests=\([0-9]*\).*/\1/')
if [ -z "$POLARS_COLD_REQUESTS" ]; then
    POLARS_COLD_REQUESTS="N/A"
fi
echo "Polars cold network requests: $POLARS_COLD_REQUESTS" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# TEST 3: PANDAS WARM (verify cache)
################################################################################

echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "TEST 3/4: PANDAS WARM (verifying cache usage)" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Cache should be fully populated. Pandas warm run should make 0 network requests..." | tee -a "$MONITOR_LOG"
echo "Start time: $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

PANDAS_WARM_LOG="$LOG_DIR/parity_pandas_warm_${RUN_ID}.log"
$PYTHON tests/performance/profile_weekly_momentum.py --mode pandas > "$PANDAS_WARM_LOG" 2>&1 &
PANDAS_WARM_PID=$!

echo "Pandas warm started with PID: $PANDAS_WARM_PID" | tee -a "$MONITOR_LOG"
echo "Log file: $PANDAS_WARM_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Verify it's running
sleep 10
if ! ps -p $PANDAS_WARM_PID > /dev/null 2>&1; then
    echo "ERROR: Pandas warm test failed to start!" | tee -a "$MONITOR_LOG"
    echo "Check log: $PANDAS_WARM_LOG" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Test running. Monitoring progress every ${CHECK_INTERVAL}s (timeout ${MAX_WAIT_MINUTES}m)..." | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Wait for pandas warm to complete
PANDAS_WARM_START=$(date +%s)
while ps -p $PANDAS_WARM_PID > /dev/null 2>&1; do
    sleep "$CHECK_INTERVAL"
    if ! ps -p $PANDAS_WARM_PID > /dev/null 2>&1; then
        break
    fi
    CURRENT_TIME=$(date +%s)
    ELAPSED_SECS=$((CURRENT_TIME - PANDAS_WARM_START))
    ELAPSED_HMS=$(printf "%02d:%02d:%02d" $((ELAPSED_SECS/3600)) $(((ELAPSED_SECS%3600)/60)) $((ELAPSED_SECS%60)))
    echo "[$(date +%H:%M:%S)] Pandas warm still running (elapsed: $ELAPSED_HMS)" | tee -a "$MONITOR_LOG"
    if [ "$ELAPSED_SECS" -gt "$MAX_WAIT_SECONDS" ]; then
        echo "ERROR: Pandas warm exceeded ${MAX_WAIT_MINUTES} minutes. Terminating run." | tee -a "$MONITOR_LOG"
        kill "$PANDAS_WARM_PID" >/dev/null 2>&1
        wait "$PANDAS_WARM_PID" 2>/dev/null
        exit 1
    fi
done

echo "" | tee -a "$MONITOR_LOG"
echo "Pandas warm test completed at $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Find pandas warm trade CSV
sleep 5
PANDAS_WARM_CSV=$(ls -t $LOG_DIR/WeeklyMomentumOptionsStrategy_*_trades.csv 2>/dev/null | head -1)

if [ -z "$PANDAS_WARM_CSV" ]; then
    echo "ERROR: No trade CSV found for pandas warm!" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Pandas warm trade CSV: $PANDAS_WARM_CSV" | tee -a "$MONITOR_LOG"
PANDAS_WARM_COUNT=$(wc -l < "$PANDAS_WARM_CSV")
PANDAS_WARM_COUNT=${PANDAS_WARM_COUNT//[[:space:]]/}
echo "Pandas warm trade count: $PANDAS_WARM_COUNT" | tee -a "$MONITOR_LOG"
PANDAS_WARM_ARCHIVE="$ARCHIVE_DIR/pandas_warm_trades.csv"
cp "$PANDAS_WARM_CSV" "$PANDAS_WARM_ARCHIVE"
cp "$PANDAS_WARM_LOG" "$ARCHIVE_DIR/"

# Extract network requests from log
PANDAS_WARM_REQUESTS=$(grep "network_requests=" "$PANDAS_WARM_LOG" 2>/dev/null | tail -1 | sed 's/.*network_requests=\([0-9]*\).*/\1/')
if [ -z "$PANDAS_WARM_REQUESTS" ]; then
    PANDAS_WARM_REQUESTS="N/A"
fi
echo "Pandas warm network requests: $PANDAS_WARM_REQUESTS" | tee -a "$MONITOR_LOG"

if [ "$PANDAS_WARM_REQUESTS" != "0" ]; then
    echo "⚠ WARNING: Pandas warm made $PANDAS_WARM_REQUESTS network requests (expected 0)" | tee -a "$MONITOR_LOG"
else
    echo "✓ SUCCESS: Pandas warm made 0 network requests (cache working)" | tee -a "$MONITOR_LOG"
fi
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# TEST 4: POLARS WARM (verify cache)
################################################################################

echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "TEST 4/4: POLARS WARM (verifying cache usage)" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Cache should be fully populated. Polars warm run should make 0 network requests..." | tee -a "$MONITOR_LOG"
echo "Start time: $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

POLARS_WARM_LOG="$LOG_DIR/parity_polars_warm_${RUN_ID}.log"
$PYTHON tests/performance/profile_weekly_momentum.py --mode polars > "$POLARS_WARM_LOG" 2>&1 &
POLARS_WARM_PID=$!

echo "Polars warm started with PID: $POLARS_WARM_PID" | tee -a "$MONITOR_LOG"
echo "Log file: $POLARS_WARM_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Verify it's running
sleep 10
if ! ps -p $POLARS_WARM_PID > /dev/null 2>&1; then
    echo "ERROR: Polars warm test failed to start!" | tee -a "$MONITOR_LOG"
    echo "Check log: $POLARS_WARM_LOG" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Test running. Monitoring progress every ${CHECK_INTERVAL}s (timeout ${MAX_WAIT_MINUTES}m)..." | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Wait for polars warm to complete
POLARS_WARM_START=$(date +%s)
while ps -p $POLARS_WARM_PID > /dev/null 2>&1; do
    sleep "$CHECK_INTERVAL"
    if ! ps -p $POLARS_WARM_PID > /dev/null 2>&1; then
        break
    fi
    CURRENT_TIME=$(date +%s)
    ELAPSED_SECS=$((CURRENT_TIME - POLARS_WARM_START))
    ELAPSED_HMS=$(printf "%02d:%02d:%02d" $((ELAPSED_SECS/3600)) $(((ELAPSED_SECS%3600)/60)) $((ELAPSED_SECS%60)))
    echo "[$(date +%H:%M:%S)] Polars warm still running (elapsed: $ELAPSED_HMS)" | tee -a "$MONITOR_LOG"
    if [ "$ELAPSED_SECS" -gt "$MAX_WAIT_SECONDS" ]; then
        echo "ERROR: Polars warm exceeded ${MAX_WAIT_MINUTES} minutes. Terminating run." | tee -a "$MONITOR_LOG"
        kill "$POLARS_WARM_PID" >/dev/null 2>&1
        wait "$POLARS_WARM_PID" 2>/dev/null
        exit 1
    fi
done

echo "" | tee -a "$MONITOR_LOG"
echo "Polars warm test completed at $(date)" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Find polars warm trade CSV
sleep 5
POLARS_WARM_CSV=$(ls -t $LOG_DIR/WeeklyMomentumOptionsStrategy_*_trades.csv 2>/dev/null | head -1)

if [ -z "$POLARS_WARM_CSV" ]; then
    echo "ERROR: No trade CSV found for polars warm!" | tee -a "$MONITOR_LOG"
    exit 1
fi

echo "Polars warm trade CSV: $POLARS_WARM_CSV" | tee -a "$MONITOR_LOG"
POLARS_WARM_COUNT=$(wc -l < "$POLARS_WARM_CSV")
POLARS_WARM_COUNT=${POLARS_WARM_COUNT//[[:space:]]/}
echo "Polars warm trade count: $POLARS_WARM_COUNT" | tee -a "$MONITOR_LOG"
POLARS_WARM_ARCHIVE="$ARCHIVE_DIR/polars_warm_trades.csv"
cp "$POLARS_WARM_CSV" "$POLARS_WARM_ARCHIVE"
cp "$POLARS_WARM_LOG" "$ARCHIVE_DIR/"

# Extract network requests from log
POLARS_WARM_REQUESTS=$(grep "network_requests=" "$POLARS_WARM_LOG" 2>/dev/null | tail -1 | sed 's/.*network_requests=\([0-9]*\).*/\1/')
if [ -z "$POLARS_WARM_REQUESTS" ]; then
    POLARS_WARM_REQUESTS="N/A"
fi
echo "Polars warm network requests: $POLARS_WARM_REQUESTS" | tee -a "$MONITOR_LOG"

if [ "$POLARS_WARM_REQUESTS" != "0" ]; then
    echo "⚠ WARNING: Polars warm made $POLARS_WARM_REQUESTS network requests (expected 0)" | tee -a "$MONITOR_LOG"
else
    echo "✓ SUCCESS: Polars warm made 0 network requests (cache working)" | tee -a "$MONITOR_LOG"
fi
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# TRADE CSV COMPARISONS
################################################################################

echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "TRADE CSV COMPARISONS" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Compare pandas cold vs polars cold
echo "Comparing PANDAS COLD vs POLARS COLD:" | tee -a "$MONITOR_LOG"
echo "  Pandas: $PANDAS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "  Polars: $POLARS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

PANDAS_POLARS_COLD_DIFF_RESULT="match"
if diff "$PANDAS_COLD_CSV" "$POLARS_COLD_CSV" > /dev/null 2>&1; then
    echo "✓ SUCCESS: Pandas cold and polars cold trade CSVs are IDENTICAL!" | tee -a "$MONITOR_LOG"
else
    PANDAS_POLARS_COLD_DIFF_RESULT="diff"
    echo "✗ FAILURE: Pandas cold and polars cold trade CSVs DIFFER!" | tee -a "$MONITOR_LOG"
    echo "" | tee -a "$MONITOR_LOG"
    echo "First 20 lines of diff:" | tee -a "$MONITOR_LOG"
    diff "$PANDAS_COLD_CSV" "$POLARS_COLD_CSV" | head -20 | tee -a "$MONITOR_LOG"
fi
echo "" | tee -a "$MONITOR_LOG"

# Compare pandas cold vs pandas warm
echo "Comparing PANDAS COLD vs PANDAS WARM:" | tee -a "$MONITOR_LOG"
echo "  Cold: $PANDAS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "  Warm: $PANDAS_WARM_CSV" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

PANDAS_COLD_WARM_DIFF_RESULT="match"
if diff "$PANDAS_COLD_CSV" "$PANDAS_WARM_CSV" > /dev/null 2>&1; then
    echo "✓ SUCCESS: Pandas cold and warm trade CSVs are IDENTICAL!" | tee -a "$MONITOR_LOG"
else
    PANDAS_COLD_WARM_DIFF_RESULT="diff"
    echo "✗ FAILURE: Pandas cold and warm trade CSVs DIFFER!" | tee -a "$MONITOR_LOG"
    echo "" | tee -a "$MONITOR_LOG"
    echo "First 20 lines of diff:" | tee -a "$MONITOR_LOG"
    diff "$PANDAS_COLD_CSV" "$PANDAS_WARM_CSV" | head -20 | tee -a "$MONITOR_LOG"
fi
echo "" | tee -a "$MONITOR_LOG"

# Compare polars cold vs polars warm
echo "Comparing POLARS COLD vs POLARS WARM:" | tee -a "$MONITOR_LOG"
echo "  Cold: $POLARS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "  Warm: $POLARS_WARM_CSV" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

POLARS_COLD_WARM_DIFF_RESULT="match"
if diff "$POLARS_COLD_CSV" "$POLARS_WARM_CSV" > /dev/null 2>&1; then
    echo "✓ SUCCESS: Polars cold and warm trade CSVs are IDENTICAL!" | tee -a "$MONITOR_LOG"
else
    POLARS_COLD_WARM_DIFF_RESULT="diff"
    echo "✗ FAILURE: Polars cold and warm trade CSVs DIFFER!" | tee -a "$MONITOR_LOG"
    echo "" | tee -a "$MONITOR_LOG"
    echo "First 20 lines of diff:" | tee -a "$MONITOR_LOG"
    diff "$POLARS_COLD_CSV" "$POLARS_WARM_CSV" | head -20 | tee -a "$MONITOR_LOG"
fi
echo "" | tee -a "$MONITOR_LOG"

################################################################################
# FINAL SUMMARY
################################################################################

RUN_COMPLETED_EPOCH=$(date +%s)
RUN_COMPLETED_ISO=$(date +%Y-%m-%dT%H:%M:%S%z)
RUN_COMPLETED_HUMAN=$(date)
RUN_DURATION_SECS=$((RUN_COMPLETED_EPOCH - RUN_STARTED_EPOCH))
if [ "$RUN_DURATION_SECS" -lt 0 ]; then
    RUN_DURATION_SECS=0
fi
RUN_DURATION_HMS=$(printf "%02d:%02d:%02d" $((RUN_DURATION_SECS/3600)) $(((RUN_DURATION_SECS%3600)/60)) $((RUN_DURATION_SECS%60)))
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "FINAL SUMMARY" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
echo "Completed: $RUN_COMPLETED_HUMAN" | tee -a "$MONITOR_LOG"
echo "Total duration: $RUN_DURATION_HMS" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

echo "Network Requests:" | tee -a "$MONITOR_LOG"
echo "  Pandas cold:  $PANDAS_COLD_REQUESTS" | tee -a "$MONITOR_LOG"
echo "  Polars cold:  $POLARS_COLD_REQUESTS" | tee -a "$MONITOR_LOG"
echo "  Pandas warm:  $PANDAS_WARM_REQUESTS" | tee -a "$MONITOR_LOG"
echo "  Polars warm:  $POLARS_WARM_REQUESTS" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

echo "Trade Counts:" | tee -a "$MONITOR_LOG"
echo "  Pandas cold:  $(wc -l < "$PANDAS_COLD_CSV") rows" | tee -a "$MONITOR_LOG"
echo "  Polars cold:  $(wc -l < "$POLARS_COLD_CSV") rows" | tee -a "$MONITOR_LOG"
echo "  Pandas warm:  $(wc -l < "$PANDAS_WARM_CSV") rows" | tee -a "$MONITOR_LOG"
echo "  Polars warm:  $(wc -l < "$POLARS_WARM_CSV") rows" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

echo "Trade CSV Files:" | tee -a "$MONITOR_LOG"
echo "  Pandas cold:  $PANDAS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "  Polars cold:  $POLARS_COLD_CSV" | tee -a "$MONITOR_LOG"
echo "  Pandas warm:  $PANDAS_WARM_CSV" | tee -a "$MONITOR_LOG"
echo "  Polars warm:  $POLARS_WARM_CSV" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

echo "Test Logs:" | tee -a "$MONITOR_LOG"
echo "  Pandas cold:  $PANDAS_COLD_LOG" | tee -a "$MONITOR_LOG"
echo "  Polars cold:  $POLARS_COLD_LOG" | tee -a "$MONITOR_LOG"
echo "  Pandas warm:  $PANDAS_WARM_LOG" | tee -a "$MONITOR_LOG"
echo "  Polars warm:  $POLARS_WARM_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

cp "$MONITOR_LOG" "$ARCHIVE_DIR/"
cat > "$SUMMARY_FILE" <<EOF
Run ID: $RUN_ID
Started: $RUN_STARTED_ISO
Completed: $RUN_COMPLETED_ISO
Duration: $RUN_DURATION_HMS
Check interval: ${CHECK_INTERVAL}s
Timeout: ${MAX_WAIT_MINUTES}m

Network requests:
  Pandas cold:  $PANDAS_COLD_REQUESTS
  Polars cold:  $POLARS_COLD_REQUESTS
  Pandas warm:  $PANDAS_WARM_REQUESTS
  Polars warm:  $POLARS_WARM_REQUESTS

Trade counts:
  Pandas cold:  $PANDAS_COLD_COUNT
  Polars cold:  $POLARS_COLD_COUNT
  Pandas warm:  $PANDAS_WARM_COUNT
  Polars warm:  $POLARS_WARM_COUNT

Diff status:
  Pandas cold vs polars cold:  $PANDAS_POLARS_COLD_DIFF_RESULT
  Pandas cold vs pandas warm:  $PANDAS_COLD_WARM_DIFF_RESULT
  Polars cold vs polars warm:  $POLARS_COLD_WARM_DIFF_RESULT

Archives:
  Pandas cold trades:  $PANDAS_COLD_ARCHIVE
  Polars cold trades:  $POLARS_COLD_ARCHIVE
  Pandas warm trades:  $PANDAS_WARM_ARCHIVE
  Polars warm trades:  $POLARS_WARM_ARCHIVE
  Monitor log copy:    $ARCHIVE_DIR/$(basename "$MONITOR_LOG")

Log files:
  Pandas cold:  $PANDAS_COLD_LOG
  Polars cold:  $POLARS_COLD_LOG
  Pandas warm:  $PANDAS_WARM_LOG
  Polars warm:  $POLARS_WARM_LOG
EOF

echo "Summary file: $SUMMARY_FILE" | tee -a "$MONITOR_LOG"
echo "Archived artifacts directory: $ARCHIVE_DIR" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

echo "Full protocol log: $MONITOR_LOG" | tee -a "$MONITOR_LOG"
echo "================================================================================" | tee -a "$MONITOR_LOG"
