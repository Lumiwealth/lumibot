# Titus Schwab Cancel Exact Path Investigation

One-line description: Documents the exact-strategy reproduction of Titus's Schwab cancel issue, the local Lumibot fix, and the remaining generated-strategy bug.

Last Updated: 2026-06-05

Status: Broker fix released in LumiBot 4.5.47 and deployed to BotManager
development and production. Exact Titus strategy still needs a fresh market-hours
run on the deployed image.

Audience: LumiBot, BotSpot, and support agents debugging live Schwab order execution.

## Overview

Titus repeatedly reported that his live Schwab option orders would not cancel.
Earlier simplified Schwab cancel tests proved only that a normal direct Schwab
cancel could work. They did not reproduce the exact customer path. The issue was
only found after running Titus's actual saved strategy revision locally against
Rob's Schwab account.

This was a process failure. For live broker support issues, a passing nearby
smoke test is not enough. The exact customer strategy, same broker, same order
state, same asset class, and same lifecycle path must be run whenever feasible.

## Artifacts

Private customer artifacts are stored under `logs/titus_private/` and are
gitignored/private. Do not expose raw account numbers, tokens, or customer
strategy code outside approved support channels.

- Exact saved revision used for the live reproduction:
  `logs/titus_private/revision_v85_ffe083f0-13cc-42b8-b9bc-4ac451a61d41.py`
- Pre-fix exact strategy run:
  `logs/titus_private/run_exact_v85_imported_correct_account_20260605_151804.log`
- Local patched method-level proof:
  `logs/titus_private/imported_cancel_methods_local_patch_20260605_154044.log`
  and `logs/titus_private/imported_cancel_methods_local_patch_20260605_154044.json`
- Local patched full strategy proof with close-window override:
  `logs/titus_private/run_v85_no_close_block_live_20260605_154748.log`
- Run-only close-window override copy:
  `logs/titus_private/revision_v85_live_no_close_block_20260605_154715.py`

## What Was Proven

1. The exact Titus v85 strategy reproduced the cancel failure before the broker
   patch.

   Evidence from `run_exact_v85_imported_correct_account_20260605_151804.log`:

   - It submitted a real LW option buy order:
     `Submitted BUY LW 2026-06-05 41.0 CALL limit 1.0700 as buy_to_open`
   - It reached the strategy cancel path:
     `BUY not filled after 8.9s; canceling and resuming scan.`
   - Schwab cancel was skipped before the API call because local status was
     already `cancelling`:
     `[SchwabCancelTelemetry] skip_terminal order_id=1006639759692 symbol=LW local_status=cancelling`

2. The local Schwab broker fix made the same cancellation path send the cancel
   request to Schwab and receive broker confirmation.

   Evidence from `imported_cancel_methods_local_patch_20260605_154044.log`:

   - Order ending `40519875`: Schwab cancel request sent, HTTP `200`, direct
     read `broker_status=CANCELED`.
   - Order ending `40519890`: Schwab cancel request sent, HTTP `200`, direct
     read `broker_status=CANCELED`.

3. The full Titus v85 strategy, with only the final-five-minute buy block
   disabled for the live test, placed and canceled a real LW option order after
   the broker patch.

   Evidence from `run_v85_no_close_block_live_20260605_154748.log`:

   - It submitted order ending `41456881`:
     `Submitted BUY LW 2026-06-05 40.0 CALL limit 2.0300 as buy_to_open`
   - It sent the Schwab cancel request even though local status was
     `cancelling`:
     `[SchwabCancelTelemetry] request order_id=1006641456881 ... local_status=cancelling`
   - Schwab accepted the cancel:
     `[SchwabCancelTelemetry] response order_id=1006641456881 http_status=200`
   - Direct Schwab read confirmed cancellation:
     `broker_status=CANCELED ... close_time=2026-06-05T19:50:32+0000`

4. The full strategy then exposed a separate generated-strategy bug in the
   hedge fallback path.

   Evidence from `run_v85_no_close_block_live_20260605_154748.log`:

   - It submitted and filled order ending `41456908`, LW 2026-06-05 40.5C.
   - It tried a stock hedge fallback: `sell_short 100 shares of LW`.
   - Schwab returned that hedge order as `error`.
   - The strategy crashed because it referenced a non-existent enum:
     `Order.OrderStatus.REJECTED`
   - Python raised `AttributeError: REJECTED`.

5. Cleanup was performed after the live test.

   - The filled LW 40.5C position was closed with a sell-to-close order ending
     `41457123`, status `FILLED`.
   - A direct Schwab account check after cleanup showed no open LW orders and no
     LW positions on the tested account.

## Cancel vs Cancelling: The Important Distinction

There are two different layers, and confusing them caused the earlier reasoning
mistake.

1. Strategy-level behavior:

   It can be reasonable for a strategy to treat `CANCELLING` as an intermediate
   state for workflow purposes. For example, after asking the broker to cancel,
   the strategy should not block forever just because Schwab has not yet
   returned final `CANCELED`.

2. Broker adapter behavior:

   The Schwab broker adapter must not treat local `CANCELLING` as terminal
   before it sends the cancel API call. If `cancel_order()` checks
   `order.is_canceled()` and `is_canceled()` returns true for `CANCELLING`, the
   broker can skip sending the actual Schwab cancel request.

The broken path was the broker adapter preflight, not the idea that strategy code
may observe `CANCELLING` as a non-blocking intermediate state after a cancel has
actually been requested.

## Local Lumibot Fix

The local change in `lumibot/brokers/schwab.py` does two things:

1. Schwab account selection now supports a unique account-number suffix and no
   longer silently falls back to the first returned account when a suffix like
   `364` is provided.

2. `Schwab.cancel_order()` no longer skips the cancel call based on local order
   status. If there is an order identifier and the Schwab client/account hash is
   available, it attempts the Schwab cancel request and lets Schwab answer.

3. The same local-state no-op pattern was also removed from Schwab modify,
   Tradier cancel, and Tradier modify. A repo-wide broker check found no
   remaining broker-adapter `order.is_canceled()` preflight guards after this
   change.

## Strategy Guidance

Titus v85 contains generated code that checks:

```python
refreshed.status == Order.OrderStatus.REJECTED
```

Schwab rejected/error orders are represented in the observed Lumibot path as:

```python
Order.OrderStatus.ERROR
```

LumiBot now also provides `Order.OrderStatus.REJECTED` as a compatibility alias
for `ERROR` so generated strategies that use the broker-style word do not crash.
New generated fast-trading strategies should still prefer `ERROR` and other
terminal non-filled states, for example:

```python
if refreshed and refreshed.status in {
    Order.OrderStatus.ERROR,
    Order.OrderStatus.CANCELED,
    Order.OrderStatus.EXPIRED,
}:
    retries += 1
    self.log_message(f"Stock hedge did not complete ({refreshed.status}); retrying.", color="yellow")
    continue
```

The BotSpot Agent fast-order skill should keep steering new strategy code toward
`ERROR` as the portable Lumibot status while accepting `REJECTED` as
compatibility wording.

## Release And Deployment Evidence

LumiBot 4.5.47 contains the Schwab/Tradier broker-adapter direct-call fix and
the `Order.OrderStatus.REJECTED` compatibility alias.

- LumiBot PR `#1078` merged to `dev` at
  `d190153ba9aeb4f311279908a6e07831dab3d07e`.
- LumiBot PR `#1079` merged to `dev` at
  `7f78e66615c72094e3d7c9b404fe8e5dfffdd3a9`.
- Git tag `v4.5.47` points to
  `7f78e66615c72094e3d7c9b404fe8e5dfffdd3a9`.
- GitHub release workflow `27043071439` completed successfully and published
  `lumibot==4.5.47`.
- Direct PyPI install check returned `Version: 4.5.47`.

BotManager was then pointed at LumiBot 4.5.47 with repository variable
`LUMIBOT_VERSION=4.5.47`.

- BotManager production workflow `27044088862` completed successfully:
  `https://github.com/Lumiwealth/bot_manager/actions/runs/27044088862`
- BotManager development workflow `27044060685` completed successfully:
  `https://github.com/Lumiwealth/bot_manager/actions/runs/27044060685`
- Both deploy logs showed `LUMIBOT_VERSION: 4.5.47`, installed
  `lumibot==4.5.47`, and started with `LumiBot v4.5.47 starting`.

This proves the fixed LumiBot package was built into the BotManager dev and
production deployment images. It does not yet prove Titus's exact strategy has
passed on the deployed runtime; that requires a new market-hours run.

## Process Correction

For future customer broker incidents:

1. Pull the exact deployed strategy revision and exact deployment logs first.
2. Run the exact strategy path locally when feasible, with only minimal
   explicitly documented test overrides.
3. If a run-only override is needed, save it as a separate clearly named file and
   state exactly what changed.
4. Treat direct broker smoke tests only as smoke tests.
5. Do not report the customer bug fixed until the exact path either passes or is
   documented as impossible to run.
6. Keep a private evidence directory with exact logs, code revisions, cleanup
   proof, and a short investigation doc before summarizing status to Rob.

## Remaining Work

1. Run Titus's exact strategy path again during market hours using the deployed
   BotManager image with LumiBot 4.5.47.
2. Compare the deployed run timeline against local proof: order placement,
   cancel request, Schwab HTTP response, direct broker status, and any cleanup
   order.
3. Use the support log export work to capture complete deployment logs instead
   of relying on partial console evidence.
4. Decide whether to deploy the BotSpot Agent fast-order skill update after
   isolating it from unrelated dirty files in `botspot_agent`.
5. Audit the broader explicit broker action contract across adapters, especially
   cancel and modify behavior, so no adapter silently skips a requested broker
   action based only on local cached status.
