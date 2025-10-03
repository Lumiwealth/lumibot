# CRITICAL FIX GAME PLAN
## Removing Live Trading Contamination from Futures Implementation

**Date:** October 1, 2025
**Severity:** CRITICAL - Legal/Financial Liability
**Status:** READY FOR EXECUTION (Awaiting User Approval)

---

## Problem Statement

I made a critical architecture error by placing backtesting-specific code in files that affect BOTH live and backtesting modes:

1. **`strategy_executor.py`** - Added futures cash handling that would modify cash from live brokers
2. **`_strategy.py`** - Modified portfolio value calculation affecting live broker displays

**Impact:** In live trading, Lumibot would display incorrect cash/portfolio values to users, potentially causing legal liability.

**Root Cause:** Misunderstanding of architecture - live brokers handle ALL accounting, Lumibot is display-only.

---

## Changes Identified (Git Diff Results)

### File 1: `lumibot/strategies/strategy_executor.py`

**Lines Added:**
- Lines 24-85: `TYPICAL_FUTURES_MARGINS` dict and `get_futures_margin_requirement()` function
- Lines 539-620: Futures cash handling logic in `FILLED_ORDER` event handler
- Lines 650-722: Futures entry price lookup in `PARTIALLY_FILLED_ORDER` event handler
- Lines 838-927: `_update_futures_mark_to_market()` method

**Total:** ~200 lines of futures-specific code

### File 2: `lumibot/strategies/_strategy.py`

**Lines Modified:**
- Line 755: Changed `asset.asset_type in ["option", "future"]` to include `"cont_future"`
- Lines 758-774: Added futures-specific portfolio value calculation (margin + unrealized P&L)

**Total:** ~20 lines modified

---

## Execution Plan

### Phase 1: Surgical Removal ✂️

**1.1 Remove from `strategy_executor.py`**

**Actions:**
- Remove lines 24-85: `TYPICAL_FUTURES_MARGINS` and `get_futures_margin_requirement()`
- Restore line 539 to original: Standard cash update call
- Remove lines 540-620: Futures position reconstruction and cash handling
- Restore line 650 to original: Standard cash update call
- Remove lines 651-722: Futures entry price lookup
- Remove lines 838-927: `_update_futures_mark_to_market()` method

**Method:** Direct text editing (NO git checkout)

**1.2 Remove from `_strategy.py`**

**Actions:**
- Line 755: Revert multiplier check to `["option", "future"]` (remove `"cont_future"`)
- Lines 758-774: Replace futures-specific logic with original single-line calculation

**Original Code (to restore):**
```python
multiplier = asset.multiplier if asset.asset_type in ["option", "future"] else 1
portfolio_value += float(quantity) * float(price) * multiplier
```

**Method:** Direct text editing (NO git checkout)

---

### Phase 2: Move to Backtesting Broker 🏗️

**2.1 Add to `backtesting_broker.py`**

**Location:** After `_process_crypto_quote()` method (around line 1003)

**Add These Components:**

**A. Margin Requirements Function**
```python
# Typical initial margin requirements for common futures contracts
TYPICAL_FUTURES_MARGINS = {
    # [Copy entire dict from strategy_executor.py]
}

def get_futures_margin_requirement(asset: Asset) -> float:
    # [Copy entire function from strategy_executor.py]
```

**B. Futures Cash Handling in `_execute_filled_order()`**

Currently at line 931-941, there's a `pass` statement for futures. Replace with actual logic:

```python
if asset_type in (Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE):
    # Reconstruct position state BEFORE order
    futures_qty_before = 0
    futures_entry_price = None

    # [Add position reconstruction logic]
    # [Add margin deduction on entry]
    # [Add margin release + P&L on exit]
    # [Handle short positions with inverted P&L]
```

**C. Portfolio Value Override**

Add new method in BacktestingBroker:
```python
def get_portfolio_value(self, strategy):
    """
    Calculate portfolio value for backtesting with futures support.

    For futures: portfolio = cash + margin_tied_up + unrealized_pnl
    For other assets: portfolio = cash + position_values
    """
    # [Implement futures-aware portfolio calculation]
```

**D. Mark-to-Market Updates**

Add method to BacktestingBroker:
```python
def update_futures_mark_to_market(self, strategy):
    """Update cash to reflect futures mark-to-market P&L changes"""
    # [Copy _update_futures_mark_to_market logic]
```

---

### Phase 3: Unit Tests 🧪

**3.1 Test: Strategy Executor Never Modifies Cash (Live Protection)**

**File:** `tests/test_strategy_executor_immutability.py` (NEW)

```python
def test_strategy_executor_does_not_modify_cash_for_live_brokers():
    """
    CRITICAL: Verify strategy_executor never modifies broker cash in live trading.

    In live trading, brokers handle ALL cash tracking. Strategy executor must
    NEVER call _set_cash_position() or _update_cash() when processing broker events.

    This test prevents legal liability from showing incorrect account values.
    """
    # Mock live broker (Tradovate, IB, etc.)
    # Send filled order events for futures, stocks, options
    # Assert: strategy.cash was NEVER modified by strategy_executor
    # Assert: Only broker.get_cash() is called, never strategy._set_cash_position()
```

**3.2 Test: Strategy Never Modifies Portfolio Value (Live Protection)**

**File:** `tests/test_strategy_portfolio_immutability.py` (NEW)

```python
def test_strategy_does_not_modify_portfolio_value_for_live_brokers():
    """
    CRITICAL: Verify strategy never calculates portfolio value for live brokers.

    In live trading, brokers return portfolio value via API. Strategy must
    NEVER calculate or modify this value.
    """
    # Mock live broker
    # Set broker portfolio value to $100,000
    # Call strategy.get_portfolio_value()
    # Assert: Returns broker value exactly, no calculation performed
```

**3.3 Test: Backtesting Broker Futures Cash Handling**

**File:** `tests/backtest/test_backtesting_broker_futures.py` (NEW)

```python
def test_backtesting_broker_futures_margin_deduction():
    """Verify margin is deducted when opening futures position"""

def test_backtesting_broker_futures_margin_release():
    """Verify margin is released when closing futures position"""

def test_backtesting_broker_futures_short_selling():
    """Verify short selling with inverted P&L calculation"""

def test_backtesting_broker_futures_portfolio_value():
    """Verify portfolio value includes margin + unrealized P&L"""
```

**3.4 Test: Regression - Verify Existing Tests Still Pass**

```bash
pytest tests/backtest/test_futures*.py -v
pytest tests/backtest/test_databento_comprehensive_trading.py -v
```

---

### Phase 4: Validation ✅

**4.1 Run Full Test Suite**
```bash
cd lumibot
pytest tests/backtest/test_futures*.py -v
pytest tests/test_strategy_executor_immutability.py -v
pytest tests/test_strategy_portfolio_immutability.py -v
```

**4.2 Verify No Live Broker Contamination**
- Check: All futures logic is in `backtesting_broker.py`
- Check: `strategy_executor.py` has NO futures-specific code
- Check: `_strategy.py` has NO futures-specific code
- Check: Unit tests pass for immutability

---

### Phase 5: Git Checkpoint 💾

**5.1 Stage Changes**
```bash
git add lumibot/strategies/strategy_executor.py
git add lumibot/strategies/_strategy.py
git add lumibot/backtesting/backtesting_broker.py
git add tests/test_strategy_executor_immutability.py
git add tests/test_strategy_portfolio_immutability.py
git add tests/backtest/test_backtesting_broker_futures.py
```

**5.2 Commit with Descriptive Message**
```bash
git commit -m "CRITICAL: Move futures logic from live code to backtesting broker

PROBLEM:
- Futures cash handling was in strategy_executor.py (affects live trading)
- Portfolio value calculation was in _strategy.py (affects live trading)
- Could show incorrect cash/portfolio to live users (legal liability)

FIX:
- Moved ALL futures logic to backtesting_broker.py
- Restored strategy_executor.py to original (no broker modification)
- Restored _strategy.py portfolio calc to original
- Added unit tests to prevent future contamination

TESTS:
- test_strategy_executor_immutability.py: Verifies no cash modification
- test_strategy_portfolio_immutability.py: Verifies no portfolio modification
- test_backtesting_broker_futures.py: Validates futures accounting

IMPACT:
- Live trading: No changes (brokers handle everything)
- Backtesting: Futures accounting moved to correct location
- All existing tests pass
"
```

**5.3 Push to Remote**
```bash
git push origin main
```

---

## Risk Mitigation

### Before Starting

- [x] Git diff completed - know exactly what to remove
- [x] Todo list created for tracking
- [ ] User approval obtained

### During Execution

- [ ] Remove code surgically (no git checkout)
- [ ] Test after each phase
- [ ] Verify no regressions

### After Completion

- [ ] All unit tests pass
- [ ] Futures backtesting still works
- [ ] No live broker code affected
- [ ] Git checkpoint created

---

## Timeline

**Estimated Time:** 2-3 hours

1. Phase 1 (Surgical Removal): 30 minutes
2. Phase 2 (Move to Backtesting): 45 minutes
3. Phase 3 (Unit Tests): 60 minutes
4. Phase 4 (Validation): 15 minutes
5. Phase 5 (Git Checkpoint): 10 minutes

---

## Success Criteria

✅ **Code:**
- [ ] No futures logic in `strategy_executor.py`
- [ ] No futures logic in `_strategy.py`
- [ ] All futures logic in `backtesting_broker.py`

✅ **Tests:**
- [ ] Immutability tests pass (live protection)
- [ ] Futures backtest tests pass (functionality)
- [ ] All existing tests pass (no regressions)

✅ **Git:**
- [ ] Changes committed with clear message
- [ ] Changes pushed to remote
- [ ] Clean git status

---

## Approval Checklist

Before proceeding, user must confirm:

- [ ] Game plan reviewed and approved
- [ ] Understand the surgical removal approach (no git checkout)
- [ ] Agree with moving logic to backtesting_broker.py
- [ ] Approve unit test strategy
- [ ] Ready for execution

---

## Notes

**Why This Happened:**
- Misunderstood architecture (thought strategy tracks cash for both)
- Didn't realize live brokers handle ALL accounting
- Added futures logic without checking broker behavior

**Lessons Learned:**
- Always check if code affects live vs backtesting
- Live brokers are source of truth for all values
- Backtesting broker simulates what real brokers do
- Never modify data from live brokers

**Prevention:**
- Unit tests will catch future violations
- Better architecture documentation needed
- Code review process for broker-related changes

---

**STATUS: AWAITING USER APPROVAL TO EXECUTE**

Once approved, I will proceed with Phase 1 immediately.
