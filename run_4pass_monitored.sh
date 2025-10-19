#!/bin/bash

# ThetaData 4-Pass Protocol - MONITORED VERSION
# After cache collision fix: October 18, 2025

set -e  # Exit on error

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CACHE_DIR="/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata"

echo "================================================================================"
echo "THETADATA 4-PASS PROTOCOL - POST CACHE COLLISION FIX"
echo "================================================================================"
echo "Start time: $(date)"
echo "Timestamp: $TIMESTAMP"
echo ""

# Function to extract metrics from log file
extract_metrics() {
    local logfile=$1
    local test_name=$2

    echo ""
    echo "=== EXTRACTING METRICS: $test_name ==="
    echo "Log file: $logfile"

    # Wait for file to be written
    sleep 2

    # Check if file exists and has content
    if [ ! -f "$logfile" ]; then
        echo "ERROR: Log file not found!"
        return 1
    fi

    local file_size=$(wc -c < "$logfile")
    echo "File size: $file_size bytes"

    if [ "$file_size" -lt 100 ]; then
        echo "WARNING: Log file seems too small, showing content:"
        cat "$logfile"
        return 1
    fi

    # CACHING METRICS
    echo ""
    echo "--- Caching Metrics ---"
    grep "Elapsed" "$logfile" || echo "  Elapsed time: NOT FOUND"
    grep -E "(cache hit|cache miss|CACHE_HIT|CACHE_MISS)" "$logfile" | tail -5 || echo "  Cache stats: NOT FOUND"

    # PORTFOLIO METRICS
    echo ""
    echo "--- Portfolio Metrics ---"
    grep -i "annual return" "$logfile" | tail -1 || echo "  Annual Return: NOT FOUND"
    grep -i "max drawdown" "$logfile" | tail -1 || echo "  Max Drawdown: NOT FOUND"
    grep -i "total return" "$logfile" | tail -1 || echo "  Total Return: NOT FOUND"
    grep -i "sharpe" "$logfile" | tail -1 || echo "  Sharpe Ratio: NOT FOUND"
    grep -i "portfolio.*value" "$logfile" | tail -3 || echo "  Portfolio Value: NOT FOUND"

    # TRADING METRICS
    echo ""
    echo "--- Trading Metrics ---"
    local trade_count=$(grep -c "\[BROKER_FILL\]" "$logfile" || echo "0")
    echo "  Number of trades: $trade_count"

    if [ "$trade_count" -gt 0 ]; then
        echo "  First 3 trades:"
        grep "\[BROKER_FILL\]" "$logfile" | head -3 || echo "  (none found)"
        echo "  Last 3 trades:"
        grep "\[BROKER_FILL\]" "$logfile" | tail -3 || echo "  (none found)"
    fi

    # Extract trades to separate file for comparison
    grep "\[BROKER_FILL\]" "$logfile" > "${logfile%.log}_trades.txt" 2>/dev/null || touch "${logfile%.log}_trades.txt"
    echo "  Trades extracted to: ${logfile%.log}_trades.txt"
}

# Function to monitor a running test
monitor_test() {
    local logfile=$1
    local test_name=$2
    local pid=$3

    echo ""
    echo ">>> MONITORING: $test_name (PID: $pid) <<<"
    echo ">>> Log: $logfile"
    echo ""

    local count=0
    while kill -0 $pid 2>/dev/null; do
        sleep 10
        count=$((count + 1))

        # Show progress every 10 seconds
        if [ -f "$logfile" ]; then
            local last_lines=$(tail -5 "$logfile" 2>/dev/null)
            echo "[${count}0s] Last 5 lines:"
            echo "$last_lines"
            echo ""
        fi
    done

    # Wait for process to fully terminate
    wait $pid 2>/dev/null || true

    echo ""
    echo ">>> COMPLETED: $test_name (took ${count}0 seconds) <<<"
}

# STEP 0: Clear cache
echo ""
echo "================================================================================"
echo "STEP 0: CLEARING CACHE"
echo "================================================================================"
rm -rf "$CACHE_DIR"/*
mkdir -p "$CACHE_DIR"
echo "Cache cleared: $CACHE_DIR"
ls -lh "$CACHE_DIR" | wc -l
echo ""

# STEP 1: PANDAS COLD
echo "================================================================================"
echo "STEP 1/4: PANDAS COLD (populates cache)"
echo "================================================================================"
PANDAS_COLD_LOG="logs/pandas_cold_${TIMESTAMP}.log"
echo "Starting: $(date)"
echo "Log: $PANDAS_COLD_LOG"

python3 tests/performance/profile_weekly_momentum.py --mode pandas > "$PANDAS_COLD_LOG" 2>&1 &
PANDAS_COLD_PID=$!

monitor_test "$PANDAS_COLD_LOG" "PANDAS COLD" $PANDAS_COLD_PID
extract_metrics "$PANDAS_COLD_LOG" "PANDAS COLD"

echo ""
echo "PANDAS COLD completed at: $(date)"
echo "Cache files created:"
ls -lh "$CACHE_DIR" | head -20

# STEP 2: POLARS COLD
echo ""
echo "================================================================================"
echo "STEP 2/4: POLARS COLD (uses same cache)"
echo "================================================================================"
POLARS_COLD_LOG="logs/polars_cold_${TIMESTAMP}.log"
echo "Starting: $(date)"
echo "Log: $POLARS_COLD_LOG"
echo "CRITICAL: NOT clearing cache - should reuse pandas cache!"

python3 tests/performance/profile_weekly_momentum.py --mode polars > "$POLARS_COLD_LOG" 2>&1 &
POLARS_COLD_PID=$!

monitor_test "$POLARS_COLD_LOG" "POLARS COLD" $POLARS_COLD_PID
extract_metrics "$POLARS_COLD_LOG" "POLARS COLD"

echo ""
echo "POLARS COLD completed at: $(date)"

# STEP 3: PANDAS WARM
echo ""
echo "================================================================================"
echo "STEP 3/4: PANDAS WARM (should have 0 network requests)"
echo "================================================================================"
PANDAS_WARM_LOG="logs/pandas_warm_${TIMESTAMP}.log"
echo "Starting: $(date)"
echo "Log: $PANDAS_WARM_LOG"

python tests/performance/profile_weekly_momentum.py --mode pandas > "$PANDAS_WARM_LOG" 2>&1 &
PANDAS_WARM_PID=$!

monitor_test "$PANDAS_WARM_LOG" "PANDAS WARM" $PANDAS_WARM_PID
extract_metrics "$PANDAS_WARM_LOG" "PANDAS WARM"

echo ""
echo "PANDAS WARM completed at: $(date)"

# STEP 4: POLARS WARM
echo ""
echo "================================================================================"
echo "STEP 4/4: POLARS WARM (should have 0 network requests)"
echo "================================================================================"
POLARS_WARM_LOG="logs/polars_warm_${TIMESTAMP}.log"
echo "Starting: $(date)"
echo "Log: $POLARS_WARM_LOG"

python tests/performance/profile_weekly_momentum.py --mode polars > "$POLARS_WARM_LOG" 2>&1 &
POLARS_WARM_PID=$!

monitor_test "$POLARS_WARM_LOG" "POLARS WARM" $POLARS_WARM_PID
extract_metrics "$POLARS_WARM_LOG" "POLARS WARM"

echo ""
echo "POLARS WARM completed at: $(date)"

# FINAL SUMMARY
echo ""
echo "================================================================================"
echo "4-PASS PROTOCOL COMPLETE"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Log files created:"
echo "  - $PANDAS_COLD_LOG"
echo "  - $POLARS_COLD_LOG"
echo "  - $PANDAS_WARM_LOG"
echo "  - $POLARS_WARM_LOG"
echo ""
echo "Trade files created:"
echo "  - ${PANDAS_COLD_LOG%.log}_trades.txt"
echo "  - ${POLARS_COLD_LOG%.log}_trades.txt"
echo "  - ${PANDAS_WARM_LOG%.log}_trades.txt"
echo "  - ${POLARS_WARM_LOG%.log}_trades.txt"
echo ""
echo "================================================================================"
echo "NEXT STEPS:"
echo "1. Compare trade counts between all 4 runs (should be identical)"
echo "2. Verify warm runs have 0 network requests"
echo "3. Verify portfolio metrics match between pandas and polars"
echo "================================================================================"
