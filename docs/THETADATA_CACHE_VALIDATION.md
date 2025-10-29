# ThetaData Cache Validation Playbook

This document captures the steps required to validate the refreshed ThetaData pandas implementation and caching behaviour. The branch intentionally ships a pandas-only backtester; Polars experimentation is deferred until after cache parity is stable.

## Environment

Set the following credentials in your shell or `.env`:

- `THETADATA_USERNAME`
- `THETADATA_PASSWORD`
- `POLYGON_API_KEY` (required for parity tests)

Ensure a local ThetaTerminal installation is available (`ThetaTerminal.jar` is bundled) and Java 11+ is on the `PATH`.

## Automated Test Matrix

### Credential-free

```
pytest tests/test_thetadata_helper.py tests/test_thetadata_backwards_compat.py
```

### Requires live services / caches

```
pytest tests/test_thetadata_pandas_verification.py::test_pandas_cold_warm
pytest tests/backtest/test_thetadata.py -m apitest
pytest tests/backtest/test_thetadata_comprehensive.py -m apitest
pytest tests/backtest/test_accuracy_verification.py -m apitest
pytest tests/backtest/test_index_data_verification.py -m apitest
pytest tests/backtest/test_thetadata_vs_polygon.py -m apitest
```

All live tests are decorated with `@pytest.mark.apitest` and will skip automatically when the required credentials are missing.

## Cache Validation Workflow

1. Run `./run_cache_validation.sh`.
   - Executes the pandas cold → warm flow and captures diagnostics in `tests/performance/logs/pandas_cold_warm.log`.
   - The script prints the observed network request counts so you can confirm the warm run shows zero fetches.
2. Inspect the log for entries prefixed with `[THETA][DEBUG]` to confirm placeholder injection/removal totals, cache hits, and network request counts.
3. (Optional) Run parity profiling: `python tests/backtest/profile_thetadata_vs_polygon.py`.
   - Produces cold and warm profiles for ThetaData and Polygon via `yappi` so regressions are easy to spot.

## Manual Spot Checks

- Execute a deterministic strategy (e.g., WeeklyMomentumOptionsStrategy) twice and confirm the second run produces no network traffic in the `[THETA][DEBUG][API]` logs.
- Delete a specific cache file and rerun to confirm placeholders are repopulated and subsequently cleared when real data becomes available.
- Verify that cache files contain `missing=True` rows for assets with no data and that these rows are stripped before returning data to callers.

## Logging Notes

- Temporary diagnostics are prefixed with `[THETA][DEBUG]` for easy removal once the cache changes are battle-tested.
- No short-lived environment variables are used for tuning retry logic; constants live in `lumibot/tools/thetadata_helper.py`.

## Follow-up Items

- Re-enable a lightweight CI-friendly regression test that replays cached fixtures without launching ThetaTerminal.
- Revisit Polars support after pandas parity and logging clean-up land successfully.
