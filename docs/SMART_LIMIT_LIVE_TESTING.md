## SMART_LIMIT live testing (paper)

This repo has two layers of coverage:

- **Unit tests** (deterministic): validate SMART_LIMIT math + state machine behavior without broker APIs.
- **API tests** (`pytest -m apitest`): place real paper orders to validate submission + repricing + fills.

### Background loop note (memory-constrained workers)

In live mode, LumiBot runs a small background loop (`StrategyExecutor.check_queue`) that periodically calls
`_process_smart_limit_orders()`. This is required so SMART_LIMIT orders can be repriced/canceled over time without
waiting for the strategy’s main `on_trading_iteration` cadence.

**Pitfall:** the SMART_LIMIT loop must stay cheap when *no* SMART_LIMIT orders exist. Avoid scanning large historical
order lists here; prefer broker “active order” fast paths (e.g., `broker.get_active_tracked_orders(...)`).

### 1) One-time setup

- Configure dedicated Alpaca test credentials. Never use a live-money account or
  a shared account with existing positions/orders for these execution tests.
- The Alpaca harness uses only ``ALPACA_TEST_API_KEY`` and
  ``ALPACA_TEST_API_SECRET``. Invalid test keys fail rather than falling back to
  the default trading account. The test requires an initially flat isolated
  account; it never resets an account or clears unrelated orders.
- The wait helper uses the ordinary strategy order accessor and broker
  reconciliation. It must not copy raw provider status into the local Order.
- Crypto cleanup cancels only this invocation's orders and bounds closing
  quantity by this invocation's acquired units, not an account-wide balance.
  A failed/uncertain close is reported, not overlapped with a blind market sell.
- Caller-owned SDK clients remain supported for account-bound managed test
  connections. Private connection provisioning stays outside the public SDK.

### 2) Run unit tests (any time)

```bash
/Users/robertgrzesik/bin/safe-timeout 1200s python3 -m pytest -q \
  tests/test_smart_limit_utils_unit.py \
  tests/test_smart_limit_single_leg_unit.py \
  tests/test_smart_limit_multileg_unit.py \
  tests/test_tradier_stream_optional_unit.py
```

### 3) Run live paper smoke (market hours)

These are “fast confidence” tests (stocks + single-leg options + 4-leg options) and should complete quickly.

```bash
/Users/robertgrzesik/bin/safe-timeout 2400s python3 -m pytest -q -m "apitest and not smartlimit_matrix" \
  tests/test_alpaca_broker_smoke_apitest.py \
  tests/test_tradier_broker_smoke_apitest.py \
  tests/test_smart_limit_live_alpaca.py \
  tests/test_smart_limit_live_tradier.py
```

Notes:
- Alpaca smoke submits+then cancels an ultra-low limit order (should not fill).
- Tradier smoke includes a paper connectivity check; the submit/cancel lifecycle test requires explicit live config
  (`TRADIER_IS_PAPER=false`) and should be run only when you intend to touch the live API.

### 4) Run live matrix (market hours)

These are heavier tests that add:

- debit + credit multi-leg packages (condor + butterfly)
- puts + long-dated options (wider spreads)
- short options (skips if broker rejects)
- non-SMART multi-leg LIMIT UX parity (no debit/credit/even in strategy code)

```bash
/Users/robertgrzesik/bin/safe-timeout 7200s python3 -m pytest -q -m smartlimit_matrix \
  tests/test_smart_limit_live_matrix_alpaca.py \
  tests/test_smart_limit_live_matrix_tradier.py
```

### 5) Benchmarks (market hours; not a pytest)

Benchmarks are in `scripts/` and write CSVs to `logs/`. These are used for price-improvement statistics and are not
treated as strict pass/fail (paper fills can be unrealistic).
