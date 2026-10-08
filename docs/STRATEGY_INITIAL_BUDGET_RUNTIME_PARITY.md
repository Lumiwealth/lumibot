# Strategy `initial_budget` runtime parity

`Strategy.initial_budget` is available in both execution modes:

- Backtest: the configured starting cash after `BACKTESTING_BUDGET` resolution.
- Live: the first broker-verified portfolio equity snapshot captured during strategy
  initialization. This includes the marked value of existing positions and must not
  be interpreted as cash alone.

If a live broker cannot provide the initial balance snapshot, the value remains
`None`; strategies that require a persistent risk baseline should validate the value
before using it and save their accepted baseline in strategy state.

The regression test in `tests/test_strategy_initial_budget.py` covers cash-only and
existing-position accounts and protects the constructor path that previously raised
`AttributeError` in live execution.
