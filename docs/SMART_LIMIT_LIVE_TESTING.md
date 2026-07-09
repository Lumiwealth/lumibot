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

- Put broker creds in `.env` (repo root). The tests load it automatically.

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

### 4) Strict CI strategy-run smoke

CI runs a strict paper-broker strategy path for Alpaca and Tradier:

- instantiates the real broker from `ALPACA_TEST_*` / `TRADIER_TEST_*` secrets;
- submits, polls, and cancels a direct broker GTC limit order as a broker contract preflight;
- runs `Trader.run_all(run_once=True)`;
- executes `before_starting_trading`, `on_trading_iteration`, and `on_strategy_end`;
- submits a deep non-marketable AAPL GTC limit order through `Strategy.submit_order(...)`;
- polls the real paper broker for the order id/status;
- cancels through `Strategy.cancel_order(...)`;
- fails if required secrets are missing or the broker order does not reach canceled state.

The strategy-run test overrides only the executor's market-open gate so pull-request CI is not tied to US equity
session timing. Broker config and submitted assets stay unchanged; the paper broker still accepts or rejects the real
GTC order.

```bash
/Users/robertgrzesik/bin/safe-timeout 1200s python3 -m pytest -q -m "broker_strategy_live" \
  tests/test_broker_live_strategy_run_apitest.py
```

### 5) Run live matrix (market hours)

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

### 6) Benchmarks (market hours; not a pytest)

Benchmarks are in `scripts/` and write CSVs to `logs/`. These are used for price-improvement statistics and are not
treated as strict pass/fail (paper fills can be unrealistic).
