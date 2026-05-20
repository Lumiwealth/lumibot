# BotSpot Startup Account Snapshot

Date: 2026-05-18

## Context

BotSpot live deployment `a082b421-0f66-4968-a428-1def265503f8` for Christy's `Cash-Secured Put Scanner CWO V1` showed as running in production, but the BotSpot detail page stayed in the "waiting for first account update" state before market open.

CloudWatch logs confirmed the runner was alive:

- saved BotSpot environment variables fetched successfully
- `LumiBot v4.5.25 starting`
- strategy initialized with 505 TipRanks rows
- runner logged `Sleeping until the market opens`

The missing account snapshot was caused by runtime ordering, not a deployment crash. `StrategyExecutor._setup_market_session()` blocked on `strategy.await_market_to_open()` before the live scheduler and one-minute `send_update_to_cloud()` loop started.

## Change

`StrategyExecutor._setup_market_session()` now calls `_send_startup_cloud_update()` before waiting for market open.

The startup update:

- runs only for live strategies, not backtests
- calls the same verified `send_update_to_cloud()` path used by the normal live loop
- records `_last_updated_cloud` only if the send succeeds
- catches/logs errors and continues startup so a snapshot failure cannot block trading

## Verification

Ran:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 120s python3 -m pytest tests/test_strategy_executor_startup_cloud_update.py tests/test_cloud_account_snapshot.py
```

Result: 6 passed.

## Follow-Up

This Lumibot change must be released and then picked up by Bot Manager images before existing/new BotSpot deployments will send a pre-market startup snapshot. Until then, a pre-market BotSpot bot can still be healthy while the detail page waits for the first post-open account update.
