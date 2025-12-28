# Backtesting Tests (Unit + Integration + Acceptance)

This project relies on a layered test strategy:

1. **Unit tests** (fast, deterministic): validate core entities and backtest engine rules.
2. **Backtest integration tests** (slower, high value): run real strategies against backtesting data sources.
3. **Acceptance backtests** (manual, end-to-end): run from `Strategy Library/` and inspect artifacts
   (`*_trades.html`, `*_tearsheet.html`, `*_stats.csv`) for realism and regressions.

## Test authority (“Legacy tests win”)

When tests fail, **how you fix them depends on how old the test is**:

- **> 1 year old:** treat as **LEGACY / high-authority**. Fix the **code**, not the test.
- **6–12 months:** investigate carefully; usually fix the code.
- **< 6 months:** the test may still be evolving; confirm intent before changing.

This prevents “performance fixes” from silently changing broker-like semantics.

## Acceptance backtests (ThetaData)

The acceptance suite lives in:

- `Strategy Library/Demos/*` (do not edit demo strategy files)
- `Strategy Library/logs/*` (artifacts)

See the session handoff for the current required windows and what to validate:

- `docs/handoffs/THETADATA_SESSION_HANDOFF_2025-12-26.md`

## Performance regressions

Performance changes are only accepted when:

- Unit tests stay green
- Backtest integration tests stay green
- Acceptance backtests remain **broker-like** (no lookahead bias, stable option MTM, realistic fills)

If you’re unsure whether a behaviour change is “more accurate” vs “just faster”, prefer accuracy and add a regression
test to lock in the correct semantics.
