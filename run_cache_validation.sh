#!/bin/bash
# Run ThetaData cache validation scenarios (pandas only)

set -euo pipefail

CACHE_DIR="/Users/robertgrzesik/Library/Caches/lumibot/1.0/thetadata"
LOG_DIR="tests/performance/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

log "===================================================================================="
log "THETADATA CACHE VALIDATION - PANDAS COLD/WARM"
log "Cache directory: $CACHE_DIR"
log "Logs: $LOG_DIR"
log "===================================================================================="

log "Clearing cache directory"
rm -rf "$CACHE_DIR"
mkdir -p "$CACHE_DIR"

log "Running pandas cold backtest (pytest tests/test_thetadata_pandas_verification.py)"
pytest tests/test_thetadata_pandas_verification.py::test_pandas_cold_warm -vv --maxfail=1 \
  > "$LOG_DIR/pandas_cold_warm.log" 2>&1
log "✓ Cold and warm runs completed (see pandas_cold_warm.log)"

log "Extracting diagnostics"
COLD_NET=$(grep -o "network_requests=[0-9\-]*" "$LOG_DIR/pandas_cold_warm.log" | head -1 | cut -d'=' -f2 || echo "?")
WARM_NET=$(grep -o "network_requests=[0-9\-]*" "$LOG_DIR/pandas_cold_warm.log" | tail -1 | cut -d'=' -f2 || echo "?")
log "Cold run network requests: ${COLD_NET}"
log "Warm run network requests: ${WARM_NET}"

log "===================================================================================="
log "OPTIONAL: parity profiling"
log "Run manually if desired: python tests/backtest/profile_thetadata_vs_polygon.py"
log "This will emit cold/warm parity logs for ThetaData vs Polygon comparisons"
log "===================================================================================="
