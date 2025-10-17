#!/bin/bash

# Parity Test Monitoring Script
# Monitors pandas test, then runs polars, then compares trades

PANDAS_PID=63139
TIMESTAMP=20251017_015147

echo "================================================================================"
echo "PARITY TEST MONITORING SCRIPT"
echo "================================================================================"
echo "Started: $(date)"
echo ""
echo "Step 1: Waiting for pandas test (PID $PANDAS_PID) to complete..."
echo "  Log file: logs/parity_test_pandas_${TIMESTAMP}.log"
echo ""

# Wait for pandas to complete
while ps -p $PANDAS_PID > /dev/null 2>&1; do
    sleep 60
    ELAPSED=$(ps -p $PANDAS_PID -o etime= 2>/dev/null | tr -d ' ')
    echo "[$(date +%H:%M:%S)] Pandas test still running (elapsed: $ELAPSED)"
done

echo ""
echo "================================================================================"
echo "Step 1 COMPLETE: Pandas test finished at $(date)"
echo "================================================================================"
echo ""

# Find the pandas trade CSV file
PANDAS_TRADES=$(ls -t logs/WeeklyMomentumOptionsStrategy_*_trades.csv | head -1)
echo "Pandas trades: $PANDAS_TRADES"
echo "Pandas trade count: $(wc -l < "$PANDAS_TRADES")"
echo ""

# Wait a bit to ensure files are written
sleep 5

echo "================================================================================"
echo "Step 2: Starting polars test..."
echo "================================================================================"
echo ""

POLARS_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
python tests/performance/profile_weekly_momentum.py --mode polars > logs/parity_test_polars_${POLARS_TIMESTAMP}.log 2>&1 &
POLARS_PID=$!

echo "Polars test started with PID: $POLARS_PID"
echo "  Log file: logs/parity_test_polars_${POLARS_TIMESTAMP}.log"
echo ""

# Wait for polars to complete
while ps -p $POLARS_PID > /dev/null 2>&1; do
    sleep 60
    ELAPSED=$(ps -p $POLARS_PID -o etime= 2>/dev/null | tr -d ' ')
    echo "[$(date +%H:%M:%S)] Polars test still running (elapsed: $ELAPSED)"
done

echo ""
echo "================================================================================"
echo "Step 2 COMPLETE: Polars test finished at $(date)"
echo "================================================================================"
echo ""

# Find the polars trade CSV file (most recent one)
POLARS_TRADES=$(ls -t logs/WeeklyMomentumOptionsStrategy_*_trades.csv | head -1)
echo "Polars trades: $POLARS_TRADES"
echo "Polars trade count: $(wc -l < "$POLARS_TRADES")"
echo ""

# Wait a bit to ensure files are written
sleep 5

echo "================================================================================"
echo "Step 3: Comparing trade CSV files..."
echo "================================================================================"
echo ""

echo "Pandas trades: $PANDAS_TRADES"
echo "Polars trades: $POLARS_TRADES"
echo ""

# Compare the files
if diff "$PANDAS_TRADES" "$POLARS_TRADES" > /dev/null 2>&1; then
    echo "✓ SUCCESS: Trade CSV files are IDENTICAL!"
    echo ""
    echo "Parity confirmed:"
    echo "  - Pandas and polars generate identical trades"
    echo "  - Trade count: $(wc -l < "$PANDAS_TRADES") rows"
    echo "  - All fixes working correctly"
else
    echo "✗ FAILURE: Trade CSV files DIFFER!"
    echo ""
    echo "Showing differences:"
    diff "$PANDAS_TRADES" "$POLARS_TRADES" | head -50
fi

echo ""
echo "================================================================================"
echo "PARITY TEST COMPLETE"
echo "================================================================================"
echo "Completed: $(date)"
echo ""
echo "Full test logs:"
echo "  Pandas: logs/parity_test_pandas_${TIMESTAMP}.log"
echo "  Polars: logs/parity_test_polars_${POLARS_TIMESTAMP}.log"
echo ""
echo "Trade CSV files:"
echo "  Pandas: $PANDAS_TRADES"
echo "  Polars: $POLARS_TRADES"
echo "================================================================================"
