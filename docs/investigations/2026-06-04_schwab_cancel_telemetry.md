# Schwab Cancel Telemetry Probe - 2026-06-04

## Scope

Added Schwab cancel telemetry only. No LumiBot deploy was performed.

The goal was to distinguish between:

- no cancel request sent to Schwab
- Schwab rejecting the cancel request
- Schwab accepting cancel but not reflecting it on direct read
- strategy code staying locked even after Schwab cancel acceptance

## Code Change

`lumibot/brokers/schwab.py` now logs `[SchwabCancelTelemetry]` events around `Schwab.cancel_order()`:

- local skip reasons before returning or raising
- cancel request metadata: order id, symbol, asset type, side, quantity, order type, local status, masked account hash
- Schwab cancel response HTTP status, elapsed milliseconds, and short response body
- best-effort direct `get_order` read after cancel acceptance, including broker status, strategy type, entered/close timestamps, cancelable/editable flags, and child count

This is diagnostic telemetry only and does not change cancel behavior.

## Test Evidence

Focused unit test:

```text
python3 -m pytest tests/test_schwab_positions_unit.py -k "cancel_order or pull_broker_order" -q
7 passed, 22 deselected
```

Live Schwab smoke:

```text
python3 scripts/schwab_titus_fast_cancel_strategy_smoke.py --account-suffix 364 --token-path schwab_token.json --wait-seconds 4 --option-limit 0.01 --measure-broad-orders --summary-path logs/schwab_cancel_telemetry_20260604_live.json
```

Result summary:

- Account suffix selected: `4364`
- Asset: TSLL 2026-06-05 14.5 CALL
- Resting order status before cancel: `WORKING`
- Schwab cancel response: HTTP `200`
- Cancel request elapsed time: `305 ms`
- Direct read immediately after cancel accepted: `CANCELED`
- Final parsed order state: canceled and not active
- Broad order pull timing measured separately: `0.451 s` for 52 raw orders
- Smoke result: pass

Full JSON summary is saved at:

```text
logs/schwab_cancel_telemetry_20260604_live.json
```

## Current Interpretation

For Rob's live Schwab account, the direct Schwab cancel path worked for a resting option limit order:

1. Schwab accepted the cancel.
2. Direct order lookup immediately showed `CANCELED`.
3. LumiBot parsed the order as non-active/canceled.

This does not prove Titus's generated strategy logic is correct, but it does prove the broker adapter can send and confirm a direct cancel for the same class of operation.

## Next Diagnostic Step

Use the new telemetry on Titus-style BotSpot runs. If Titus still reports "not canceling", compare the deployment logs for:

- whether `[SchwabCancelTelemetry] request` appears
- whether Schwab returns HTTP 2xx, 4xx, or an exception
- whether the post-cancel direct read says `CANCELED`, `PENDING_CANCEL`, `WORKING`, or another status
- whether the strategy continues waiting after cancel acceptance

That will separate a broker/API failure from a strategy-loop or generated-code failure.
