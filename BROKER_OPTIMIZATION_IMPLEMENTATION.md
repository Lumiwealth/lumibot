# Broker Optimization Implementation Guide

## Overview
This guide provides EXACT file paths, line numbers, and code changes to optimize the broker layer for 8-12 seconds speedup (3.0-3.3x overall for Polars).

**Estimated Effort**: 3-4 weeks
**Expected Speedup**: 8-12 seconds
**Risk Level**: Low to Medium (with proper testing)

---

## Phase 1: Easy Wins (1-2 weeks, 5s speedup)

### Optimization 1: Logging Reduction (3-4s speedup)

#### File: `lumibot/brokers/broker.py`

**Step 1**: Add production mode configuration

**Location**: Line 73 (in `__init__` method)
**Add after line 73**:
```python
def __init__(self, name="", connect_stream=True, data_source: DataSource = None, option_source: DataSource = None,
             config=None, max_workers=20, extended_trading_minutes=0, cleanup_config=None,
             production_logging=False):  # ← ADD THIS PARAMETER
```

**Location**: Line 113 (after logger initialization)
**Add after line 113**:
```python
# Initialize cleanup configuration and tracking
self._cleanup_config = self._initialize_cleanup_config(cleanup_config)
self._iteration_counter = 0
self._last_cleanup_time = None

# ADD THIS BLOCK:
# Production logging mode (reduces overhead by 3-4 seconds)
self._production_logging = production_logging or os.environ.get("LUMIBOT_PRODUCTION_LOGGING", "").lower() == "true"
if self._production_logging:
    # Reduce logging to WARNING level in production
    self.logger.setLevel(logging.WARNING)
    logger.info(f"Production logging mode enabled for broker {name}")
```

**Location**: Lines 584-604 (get_last_price method)
**Replace**:
```python
def get_last_price(self, asset: Asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
    # DEBUG-LOG: Broker quote request
    logger.info(  # ← CHANGE THIS
        "[BROKER][QUOTE][REQUEST] asset=%s quote=%s exchange=%s using_option_source=%s",
        getattr(asset, 'symbol', asset),
        getattr(quote, 'symbol', quote) if quote is not None else None,
        exchange,
        bool(self.option_source and asset.asset_type == "option")
    )

    if self.option_source and asset.asset_type == "option":
        result = self.option_source.get_last_price(asset, quote=quote, exchange=exchange)
    else:
        result = self.data_source.get_last_price(asset, quote=quote, exchange=exchange)

    # DEBUG-LOG: Broker quote response
    logger.info(  # ← CHANGE THIS
        "[BROKER][QUOTE][RESPONSE] asset=%s value=%s",
        getattr(asset, 'symbol', asset),
        result
    )

    return result
```

**With**:
```python
def get_last_price(self, asset: Asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
    # DEBUG-LOG: Broker quote request (ONLY in non-production mode)
    if not self._production_logging:  # ← ADD THIS CHECK
        logger.info(
            "[BROKER][QUOTE][REQUEST] asset=%s quote=%s exchange=%s using_option_source=%s",
            getattr(asset, 'symbol', asset),
            getattr(quote, 'symbol', quote) if quote is not None else None,
            exchange,
            bool(self.option_source and asset.asset_type == "option")
        )

    if self.option_source and asset.asset_type == "option":
        result = self.option_source.get_last_price(asset, quote=quote, exchange=exchange)
    else:
        result = self.data_source.get_last_price(asset, quote=quote, exchange=exchange)

    # DEBUG-LOG: Broker quote response (ONLY in non-production mode)
    if not self._production_logging:  # ← ADD THIS CHECK
        logger.info(
            "[BROKER][QUOTE][RESPONSE] asset=%s value=%s",
            getattr(asset, 'symbol', asset),
            result
        )

    return result
```

**Expected Impact**: Saves ~3-4 seconds (133,406 log calls → ~5,000 log calls)

---

### Optimization 2: Enum Identity Checks (1.5-2s speedup)

#### File: `lumibot/entities/order.py`

**Find all instances of enum comparisons and replace `==` with `is`**

**Location**: Search for patterns like:
```python
if order.status == OrderStatus.FILLED:
if order.status == OrderStatus.NEW:
if asset.asset_type == AssetType.OPTION:
```

**Replacewith**:
```python
if order.status is OrderStatus.FILLED:
if order.status is OrderStatus.NEW:
if asset.asset_type is AssetType.OPTION:
```

**How to find all occurrences**:
```bash
cd /Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot

# Find all enum comparisons in order.py
grep -n "\.status ==" lumibot/entities/order.py
grep -n "OrderStatus\." lumibot/entities/order.py | grep "=="

# Find all enum comparisons in broker files
grep -rn "\.status ==" lumibot/brokers/
grep -rn "\.asset_type ==" lumibot/entities/
```

**Specific examples to change** (based on profile):

1. **File**: `lumibot/entities/order.py` line ~1022 (is_active method)
```python
# BEFORE:
def is_active(self):
    return self.status == OrderStatus.SUBMITTED or \
           self.status == OrderStatus.OPEN or \
           self.status == OrderStatus.NEW

# AFTER:
def is_active(self):
    return self.status is OrderStatus.SUBMITTED or \
           self.status is OrderStatus.OPEN or \
           self.status is OrderStatus.NEW
```

2. **File**: `lumibot/entities/order.py` line ~1033 (is_canceled method)
```python
# BEFORE:
def is_canceled(self):
    return self.status == OrderStatus.CANCELED

# AFTER:
def is_canceled(self):
    return self.status is OrderStatus.CANCELED
```

3. **File**: `lumibot/entities/order.py` line ~1044 (is_filled method)
```python
# BEFORE:
def is_filled(self):
    return self.status == OrderStatus.FILLED

# AFTER:
def is_filled(self):
    return self.status is OrderStatus.FILLED
```

4. **File**: `lumibot/entities/asset.py` (AssetType comparisons)
```bash
# Find all asset_type comparisons
grep -n "asset_type ==" lumibot/entities/asset.py
grep -n "AssetType\." lumibot/entities/asset.py | grep "=="

# Replace all with `is`
```

**Expected Impact**: Saves ~1.5-2 seconds (1.58M enum comparisons optimized)

---

## Phase 2: Medium Complexity (2-3 weeks, 5s additional speedup)

### Optimization 3: Order List Caching (2-3s speedup)

#### File: `lumibot/brokers/broker.py`

**Step 1**: Add cache tracking variables

**Location**: Line 113 (after logger initialization)
**Add after initialization**:
```python
# Initialize cleanup configuration and tracking
self._cleanup_config = self._initialize_cleanup_config(cleanup_config)
self._iteration_counter = 0
self._last_cleanup_time = None

# ADD THIS BLOCK:
# Order caching for performance optimization
self._active_orders_cache = None
self._active_orders_cache_dirty = True
self._order_cache_lock = threading.Lock()  # Thread-safe cache invalidation
```

**Step 2**: Add cache invalidation method

**Location**: After line 315 (after `force_cleanup` method)
**Add new method**:
```python
def _invalidate_order_cache(self):
    """Invalidate the active orders cache when order state changes."""
    with self._order_cache_lock:
        self._active_orders_cache_dirty = True
        self._active_orders_cache = None
```

**Step 3**: Modify get_tracked_orders to use cache

**Location**: Line 1273
**Replace**:
```python
def get_tracked_orders(self, strategy=None, asset=None) -> list[Order]:
    """get all tracked orders for a given strategy"""
    # Allow filtering by Strategy instance or by name
    if strategy is not None and not isinstance(strategy, str):
        strategy_name = getattr(strategy, "name", getattr(strategy, "_name", None))
    else:
        strategy_name = strategy
    result = []
    for order in self._tracked_orders:
        if (strategy_name is None or order.strategy == strategy_name) and (asset is None or order.asset == asset):
            result.append(order)
    return result
```

**With**:
```python
def get_tracked_orders(self, strategy=None, asset=None) -> list[Order]:
    """get all tracked orders for a given strategy (with caching optimization)"""
    # Allow filtering by Strategy instance or by name
    if strategy is not None and not isinstance(strategy, str):
        strategy_name = getattr(strategy, "name", getattr(strategy, "_name", None))
    else:
        strategy_name = strategy

    # Check cache first (only if no filtering - cache invalidation is complex with filters)
    if strategy_name is None and asset is None:
        with self._order_cache_lock:
            if not self._active_orders_cache_dirty and self._active_orders_cache is not None:
                return self._active_orders_cache.copy()  # Return copy to prevent mutation

            # Rebuild cache
            self._active_orders_cache = list(self._tracked_orders)
            self._active_orders_cache_dirty = False
            return self._active_orders_cache.copy()

    # Fallback to non-cached filtering
    result = []
    for order in self._tracked_orders:
        if (strategy_name is None or order.strategy == strategy_name) and (asset is None or order.asset == asset):
            result.append(order)
    return result
```

**Step 4**: Add cache invalidation to order state changes

**Location**: Line 885 (_process_new_order), 904 (_process_canceled_order), 934 (_process_filled_order), 960 (_process_error_order)

**Add after each state change**:
```python
def _process_new_order(self, order):
    if order in self._new_orders:
        return order

    self._unprocessed_orders.remove(order.identifier, key="identifier")
    order.status = self.NEW_ORDER
    order.set_new()
    self._new_orders.append(order)
    self._invalidate_order_cache()  # ← ADD THIS
    return order

def _process_canceled_order(self, order):
    self._new_orders.remove(order.identifier, key="identifier")
    self._unprocessed_orders.remove(order.identifier, key="identifier")
    self._partially_filled_orders.remove(order.identifier, key="identifier")
    order.status = self.CANCELED_ORDER
    order.set_canceled()
    self._canceled_orders.append(order)
    self._invalidate_order_cache()  # ← ADD THIS
    return order

def _process_filled_order(self, order, price, quantity):
    self._new_orders.remove(order.identifier, key="identifier")
    self._unprocessed_orders.remove(order.identifier, key="identifier")
    self._partially_filled_orders.remove(order.identifier, key="identifier")
    order.add_transaction(price, quantity)
    order.status = self.FILLED_ORDER
    order.set_filled()
    self._filled_orders.append(order)
    self._invalidate_order_cache()  # ← ADD THIS

    # ... rest of method
```

**Expected Impact**: Saves ~2-3 seconds (1,245 calls with 1.2M is_active() checks)

---

### Optimization 4: Status Memoization (2-3s speedup)

#### File: `lumibot/entities/order.py`

**Find the Order class __init__ method and add cached flags**

**Location**: Line ~121 (Order.__init__)
**Add after status initialization**:
```python
class Order:
    def __init__(self, ...):
        # Existing initialization
        self._status = OrderStatus.NEW

        # ADD THIS BLOCK:
        # Cached status flags for performance (avoids repeated enum comparisons)
        self._is_active_cached = True  # NEW orders are active by default
        self._is_filled_cached = False
        self._is_canceled_cached = False
```

**Location**: Find the `status` property setter
**Replace**:
```python
@status.setter
def status(self, value):
    self._status = value
```

**With**:
```python
@status.setter
def status(self, value):
    self._status = value

    # Update cached flags when status changes
    self._is_active_cached = value in (OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.NEW)
    self._is_filled_cached = value is OrderStatus.FILLED
    self._is_canceled_cached = value is OrderStatus.CANCELED
```

**Location**: Lines ~1022, ~1033, ~1044 (is_active, is_canceled, is_filled methods)
**Replace**:
```python
def is_active(self):
    return self.status is OrderStatus.SUBMITTED or \
           self.status is OrderStatus.OPEN or \
           self.status is OrderStatus.NEW

def is_canceled(self):
    return self.status is OrderStatus.CANCELED

def is_filled(self):
    return self.status is OrderStatus.FILLED
```

**With**:
```python
def is_active(self):
    return self._is_active_cached  # O(1) lookup instead of 3 enum comparisons

def is_canceled(self):
    return self._is_canceled_cached  # O(1) lookup

def is_filled(self):
    return self._is_filled_cached  # O(1) lookup
```

**Expected Impact**: Saves ~2-3 seconds (2.26s + 0.78s + 0.52s = 3.56s total from status checks)

---

## Testing Infrastructure

### Test 1: Benchmark Script (using existing infrastructure!)

**File**: `tests/backtest/profile_broker_optimizations.py`

**Create new file** (based on existing `profile_thetadata_vs_polygon.py`):

```python
"""
Performance profiling for broker optimizations.

Measures before/after speedup for BOTH pandas and polars backends.

Usage:
    python tests/backtest/profile_broker_optimizations.py --mode both
"""

import argparse
import datetime
import time
from pathlib import Path
import yappi
import pytz

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPolars, DataBentoDataBacktestingPandas
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

# Import the Polars-native strategy
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "performance"))
from profile_databento_mes_momentum_polars_native import MESMomentumSMA9PolarsNative

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


def run_benchmark(backend, production_logging, label):
    """
    Run benchmark with specific broker configuration.

    Args:
        backend: 'pandas' or 'polars'
        production_logging: True/False (optimization on/off)
        label: Description for logging
    """
    datasource_cls = DataBentoDataBacktestingPolars if backend == "polars" else DataBentoDataBacktestingPandas
    strategy_cls = MESMomentumSMA9PolarsNative if backend == "polars" else MESMomentumSMA9

    # Period
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime.datetime(2024, 1, 5, 16, 0))

    print(f"\n{'='*80}")
    print(f"BENCHMARK: {label}")
    print(f"Backend: {backend.upper()}")
    print(f"Production Logging: {production_logging}")
    print(f"{'='*80}")

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    wall_start = time.time()

    # Run backtest
    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Pass production_logging to broker
    broker = BacktestingBroker(data_source=data_source, production_logging=production_logging)
    fee = TradingFee(flat_fee=0.50)

    strat = strategy_cls(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    logfile = str(OUTPUT_DIR / f"broker_opt_{backend}_{label}")
    trader = Trader(logfile=logfile, backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start
    yappi.stop()

    # Save profile
    profile_path = OUTPUT_DIR / f"broker_opt_{backend}_{label}.prof"
    yappi.get_func_stats().save(str(profile_path), type="pstat")

    print(f"✓ Completed in {elapsed:.2f}s")
    print(f"  Profile saved: {profile_path}")

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Profile broker optimizations")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        return

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = {}

    for backend in modes:
        # Test baseline (optimizations OFF)
        results[f"{backend}_baseline"] = run_benchmark(
            backend=backend,
            production_logging=False,
            label="baseline"
        )

        # Test optimized (optimizations ON)
        results[f"{backend}_optimized"] = run_benchmark(
            backend=backend,
            production_logging=True,
            label="optimized"
        )

    # Print comparison
    print(f"\n{'='*80}")
    print("RESULTS")
    print(f"{'='*80}")
    print(f"{'Test':<30} {'Time (s)':<15} {'Speedup'}")
    print("-"*80)

    for backend in modes:
        baseline_time = results[f"{backend}_baseline"]
        optimized_time = results[f"{backend}_optimized"]
        speedup = baseline_time / optimized_time if optimized_time > 0 else 0

        print(f"{backend.upper()} Baseline:".<30} {baseline_time:>10.2f}s")
        print(f"{backend.upper()} Optimized:<30} {optimized_time:>10.2f}s      {speedup:.2f}x")

        improvement = baseline_time - optimized_time
        print(f"  → Improvement: {improvement:.2f}s saved\n")

    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
```

**Run with**:
```bash
python tests/backtest/profile_broker_optimizations.py --mode both
```

---

### Test 2: Parity Verification

**File**: `tests/broker/test_broker_optimization_parity.py`

**Create new test file**:

```python
"""
Parity tests for broker optimizations.

Ensures that optimizations don't change trading behavior.
"""

import pytest
from datetime import datetime
import pytz
import pandas as pd

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPolars
from lumibot.entities import TradingFee
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

# Import strategy
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "performance"))
from profile_databento_mes_momentum_polars_native import MESMomentumSMA9PolarsNative

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


def run_backtest_with_config(production_logging):
    """Run backtest with specific broker configuration."""
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime(2024, 1, 3, 16, 0))  # Single day for speed

    data_source = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source, production_logging=production_logging)
    fee = TradingFee(flat_fee=0.50)

    strat = MESMomentumSMA9PolarsNative(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    return strat


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_production_logging_parity():
    """Verify production logging optimization doesn't change behavior."""

    # Run baseline (optimizations OFF)
    strat_baseline = run_backtest_with_config(production_logging=False)

    # Run optimized (optimizations ON)
    strat_optimized = run_backtest_with_config(production_logging=True)

    # Compare orders
    orders_baseline = sorted(strat_baseline.orders, key=lambda o: o.identifier)
    orders_optimized = sorted(strat_optimized.orders, key=lambda o: o.identifier)

    assert len(orders_baseline) == len(orders_optimized), "Order count mismatch"

    for i, (o1, o2) in enumerate(zip(orders_baseline, orders_optimized)):
        assert o1.asset == o2.asset, f"Order {i}: asset mismatch"
        assert o1.quantity == o2.quantity, f"Order {i}: quantity mismatch"
        assert o1.side == o2.side, f"Order {i}: side mismatch"
        assert abs(o1.avg_fill_price - o2.avg_fill_price) < 0.01, f"Order {i}: price mismatch"
        assert o1.status == o2.status, f"Order {i}: status mismatch"

    # Compare final portfolio value
    pv_baseline = strat_baseline.get_portfolio_value()
    pv_optimized = strat_optimized.get_portfolio_value()

    assert abs(pv_baseline - pv_optimized) < 1.0, f"Portfolio value mismatch: {pv_baseline} vs {pv_optimized}"

    print("✓ Parity verified: optimization produces identical results")
```

**Run with**:
```bash
pytest tests/broker/test_broker_optimization_parity.py -v -s -m apitest
```

---

## Implementation Checklist

### Phase 1: Easy Wins
- [ ] 1.1: Add `production_logging` parameter to Broker.__init__ (broker.py:73)
- [ ] 1.2: Add production logging initialization (broker.py:113)
- [ ] 1.3: Wrap logger.info calls in get_last_price (broker.py:584-604)
- [ ] 1.4: Find all enum `==` comparisons in order.py
- [ ] 1.5: Replace `==` with `is` for OrderStatus comparisons
- [ ] 1.6: Replace `==` with `is` for AssetType comparisons
- [ ] 1.7: Run benchmark: `python tests/backtest/profile_broker_optimizations.py --mode both`
- [ ] 1.8: Run parity test: `pytest tests/broker/test_broker_optimization_parity.py`
- [ ] 1.9: Verify 5s speedup achieved

### Phase 2: Medium Complexity
- [ ] 2.1: Add order cache variables (broker.py:113)
- [ ] 2.2: Add `_invalidate_order_cache()` method (broker.py:315)
- [ ] 2.3: Modify `get_tracked_orders()` to use cache (broker.py:1273)
- [ ] 2.4: Add cache invalidation to `_process_new_order()` (broker.py:885)
- [ ] 2.5: Add cache invalidation to `_process_canceled_order()` (broker.py:904)
- [ ] 2.6: Add cache invalidation to `_process_filled_order()` (broker.py:934)
- [ ] 2.7: Add status cache flags to Order.__init__ (order.py:~121)
- [ ] 2.8: Update status.setter to maintain cache (order.py)
- [ ] 2.9: Modify is_active/is_filled/is_canceled to use cache (order.py:1022,1033,1044)
- [ ] 2.10: Run benchmark again
- [ ] 2.11: Run parity test again
- [ ] 2.12: Verify 10s total speedup achieved

---

## Expected Results

### Phase 1 Complete (Easy Wins)
```
==================================================
RESULTS
==================================================
Test                           Time (s)        Speedup
--------------------------------------------------
PANDAS Baseline:                88.76s
PANDAS Optimized:               83.50s          1.06x
  → Improvement: 5.26s saved

POLARS Baseline:                36.66s
POLARS Optimized:               31.40s          1.17x
  → Improvement: 5.26s saved
==================================================
```

### Phase 2 Complete (All Optimizations)
```
==================================================
RESULTS
==================================================
Test                           Time (s)        Speedup
--------------------------------------------------
PANDAS Baseline:                88.76s
PANDAS Optimized:               78.90s          1.12x
  → Improvement: 9.86s saved

POLARS Baseline:                36.66s
POLARS Optimized:               26.80s          1.37x → 3.3x overall vs pandas!
  → Improvement: 9.86s saved
==================================================
```

---

## Verification Commands

```bash
# Run all optimizations benchmarks
python tests/backtest/profile_broker_optimizations.py --mode both

# Run parity tests
pytest tests/broker/test_broker_optimization_parity.py -v -s -m apitest

# Analyze profiles
pip install snakeviz
snakeviz tests/performance/logs/broker_opt_polars_baseline.prof
snakeviz tests/performance/logs/broker_opt_polars_optimized.prof

# Compare specific functions
python3 -c "
import pstats
baseline = pstats.Stats('tests/performance/logs/broker_opt_polars_baseline.prof')
optimized = pstats.Stats('tests/performance/logs/broker_opt_polars_optimized.prof')
baseline.sort_stats('tottime').print_stats('get_tracked_orders|is_active|logger', 20)
print('\n' + '='*80 + '\n')
optimized.sort_stats('tottime').print_stats('get_tracked_orders|is_active|logger', 20)
"
```

---

## Troubleshooting

### If parity test fails:
1. Check that cache invalidation is called on ALL order state changes
2. Verify enum identity checks use `is` not `==`
3. Run with verbose logging: `production_logging=False`
4. Compare trade CSVs manually

### If speedup is less than expected:
1. Verify `production_logging=True` is passed to broker
2. Check profile to ensure logging overhead decreased
3. Verify enum comparisons show reduced time
4. Check cache hit rate (add instrumentation)

---

## Notes

- All line numbers are approximate - use search to find exact locations
- Test EACH optimization incrementally before moving to next
- Use CSV comparison as gold standard for parity
- Keep baseline profile for before/after comparison
- Document any deviations from this plan

---

**Created**: 2025-10-17
**Last Updated**: 2025-10-17
