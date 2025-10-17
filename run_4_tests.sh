#!/bin/bash
set -e

CACHE_DIR="/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata"
LOG_DIR="logs"

echo "=========================================="
echo "RUNNING 4 BACKTEST SCENARIOS"
echo "=========================================="

# Test 1: Pandas Cold
echo "TEST 1/4: Pandas Cold (clearing cache)..."
rm -rf "$CACHE_DIR"
mkdir -p "$CACHE_DIR"
python3 tests/performance/profile_weekly_momentum.py --mode pandas > "$LOG_DIR/pandas_cold.log" 2>&1
echo "✓ Pandas cold done"

# Test 2: Pandas Warm
echo "TEST 2/4: Pandas Warm (reusing cache)..."
python3 tests/performance/profile_weekly_momentum.py --mode pandas > "$LOG_DIR/pandas_warm.log" 2>&1
echo "✓ Pandas warm done"

# Test 3: Polars Cold
echo "TEST 3/4: Polars Cold (clearing cache)..."
rm -rf "$CACHE_DIR"
mkdir -p "$CACHE_DIR"
python3 tests/performance/profile_weekly_momentum.py --mode polars > "$LOG_DIR/polars_cold.log" 2>&1
echo "✓ Polars cold done"

# Test 4: Polars Warm
echo "TEST 4/4: Polars Warm (reusing cache)..."
python3 tests/performance/profile_weekly_momentum.py --mode polars > "$LOG_DIR/polars_warm.log" 2>&1
echo "✓ Polars warm done"

echo "=========================================="
echo "ALL 4 TESTS COMPLETE - ANALYZING RESULTS"
echo "=========================================="
