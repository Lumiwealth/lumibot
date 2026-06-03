# Titus Schwab Strategy Validation - 2026-06-01

## Scope

Investigate whether Titus's current cancel and hedge issues are still a LumiBot Schwab broker bug or a generated-strategy structure problem. This note intentionally avoids customer secrets and does not include token values.

## Strategy Provenance

The proposed local copy is based on Titus's saved LW ITM call scanner, not a generic scratch strategy.

Evidence checked:

- The saved strategy export at `.codex_tmp_titus_research/lw_itm_call_v18_main.py` has the `LWITMCallScanner` class and the generated-code header for the same refinement thread.
- The exported conversation includes repeated requests to cancel option orders after 4 seconds if they do not fill.
- The same conversation includes Schwab cancel failures from before the option cancel fix, including `cancel_order` not being implemented for Schwab option orders.
- Later generated versions moved toward IOC-style option handling and bounded hedge retries, which matches the workaround path Titus was using after the earlier cancel failures.

That means the proposed copy is addressing the strategy shape Titus was working with: a fast option entry, a short cancel window, and a stock hedge that should fire immediately after the first leg fills.

## Current Finding

The live Schwab broker path is working for the critical primitives:

- A TSLL option limit order can be submitted.
- The submitted order can be read back by exact order id.
- A 4 second wait can be followed by `cancel_order`.
- Schwab returns the order as canceled through direct order lookup.
- Broker-native OTO and bracket structures can be submitted, read back as Schwab `TRIGGER` orders, and canceled.

The remaining risk is strategy structure, not basic Schwab cancel support.

Update after the latest local strategy pass:

- The proposed Titus strategy copy now uses a 1 second exact-order polling loop during the 4 second cancel window.
- If the option order fills during that direct polling window, it submits the hedge immediately from the same code path.
- `on_filled_order()` remains as a backup trigger, but a per-order hedge guard prevents duplicate hedge orders if both direct polling and the callback see the same fill.
- If the order does not fill, it cancels the exact submitted order id, treats `CANCELLING` / pending-cancel states as an unlock condition, and avoids broad `get_orders()` in the hot path.

## Live Evidence

Rob-owned Schwab account suffix used: `4364`.

Summary files:

- `tmp/schwab_titus_fast_cancel_strategy_1780357004.json`
- `tmp/schwab_titus_fast_cancel_strategy_1780360399.json`
- `tmp/schwab_titus_workflow_smoke_1780359992.json`

The most complete workflow smoke ran:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s \
  python3 scripts/schwab_titus_workflow_smoke.py \
  --account-suffix 4364 \
  --token-path schwab_token.json \
  --wait-seconds 4 \
  --option-limit 0.01 \
  --child-limit 0.50 \
  --stop-loss 0.01
```

Result:

- `titus_style_cancel`: passed.
- `oto_structure`: passed.
- `bracket_structure`: passed.

The latest strategy-level fast-cancel rerun used the saved gitignored token path and passed:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s \
  python3 scripts/schwab_titus_fast_cancel_strategy_smoke.py \
  --account-suffix 4364 \
  --token-path schwab_token.json \
  --wait-seconds 4 \
  --option-limit 0.01 \
  --measure-broad-orders
```

Result:

- submitted TSLL option limit order,
- waited 4 seconds,
- canceled by exact order id,
- direct-read final status as `CANCELED`,
- strategy unlock condition returned true,
- `used_broad_get_orders_in_hot_path` was false.

Latest rerun:

- summary: `tmp/schwab_titus_fast_cancel_strategy_1780364001.json`
- result: `pass: true`
- elapsed time: 5.368 seconds
- before cancel: `PENDING_ACTIVATION`
- immediate and final direct reads: `CANCELED`
- broad order pull was measured separately and took 0.357 seconds, but the strategy hot path did not depend on broad order history.

Because the market was closed by the time the final checks ran, this does not prove a real fill-triggered hedge leg. That needs market-hours validation.

## Titus Strategy Issues Found

### 1. Hedge can wait one full strategy cycle

In the ITM call scanner version:

- `initialize()` sets `self.sleeptime = "1M"`.
- `_submit_option_order()` sets `self.vars.pending_hedge` when it detects a fill.
- `on_filled_order()` also only sets `self.vars.pending_hedge`.
- The hedge is actually placed from the next `on_trading_iteration()`.

That means a fill can legitimately wait close to a minute before the hedge order is submitted. This matches Titus's report that the hedge leg can take about a minute.

Fix direction:

- Submit the hedge immediately inside `on_filled_order()` or immediately after direct fill detection in `_submit_option_order()`.
- Do not only set `pending_hedge` and wait for the next minute-based iteration.
- If `pending_hedge` remains as a retry mechanism, reduce `sleeptime` to a much shorter interval, for example `5S`, and keep duplicate-order guards.

### 2. Cancel path can stay locked too conservatively

In the ITM call scanner version:

- The strategy submits an option order.
- It sleeps 4 seconds.
- It calls `get_order(submitted.identifier)`.
- If not filled, it calls `cancel_order(submitted)`.
- It then checks only a small set of statuses and keeps `pair_lock` if the order still looks active.

This can leave the strategy locked if Schwab is in a canceling or transition state, or if local order state has not fully settled.

Fix direction:

- Track the submitted order id and use direct `get_order(order_id)` in the hot path.
- After `cancel_order(submitted)`, unlock when any of these are true:
  - local `submitted.is_canceled()` is true,
  - direct status is `CANCELED`, `CANCELLED`, `CANCELLING`, or `PENDING_CANCEL`,
  - direct order lookup says the order is not active.
- Do not use a broad `get_orders()` scan to decide whether this exact order is still blocking the pair.

### 3. Another generated version avoids option cancel entirely

In the call spread strategy version, `_cancel_stale_live_orders()` explicitly skips manual cancel for options and logs that it is relying on IOC/FOK behavior. That was a workaround for the old Schwab cancel limitation.

That workaround should now be removed for strategies that require "cancel after 4 seconds." The current Schwab option cancel path has been validated live.

## Recommended Strategy Pattern

For Titus's current workflow, the next generated/refined strategy should:

1. Submit one option order and store the returned `submitted.identifier`.
2. Sleep 4 seconds with `process_pending_orders=True`.
3. Directly call `get_order(submitted.identifier)`.
4. If filled, submit the hedge immediately from the same code path or from `on_filled_order()`.
5. If not filled, call `cancel_order(submitted)`.
6. Directly re-read that same order id and unlock on canceled, canceling, pending cancel, or non-active state.
7. Avoid broad `get_orders()` scans in the 4 second submit/cancel/hedge path.
8. Prefer broker-native OTO/bracket orders for the fastest parent/child behavior once live fill-trigger testing is complete.

## Proposed Local Strategy Copy

A proposed copy was created from the read-only saved Titus strategy:

- source: `.codex_tmp_titus_research/lw_itm_call_v18_main.py`
- proposed: `.codex_tmp_titus_research/lw_itm_call_v18_proposed_fast_cancel.py`

Changes in the proposed copy:

- changes `self.sleeptime` from `1M` to `1S` so fallback hedge retry is not delayed by a full minute,
- polls the exact submitted order id once per second during the 4 second fill/cancel window,
- submits the stock hedge immediately if the exact direct read shows a fill,
- keeps `on_filled_order()` as a backup path only,
- adds a duplicate hedge guard keyed by option order id,
- adds a `_submit_hedge_now(...)` helper,
- treats Schwab `CANCELLING` as cancel-pending and not as a reason to keep the pair locked forever,
- unlocks the pair after `cancel_order(...)` when the local order or direct broker read confirms canceled/canceling/non-active state.

Validation performed:

```bash
python3 -m py_compile \
  /Users/robertgrzesik/Development/.codex_tmp_titus_research/lw_itm_call_v18_proposed_fast_cancel.py \
  /Users/robertgrzesik/Development/.codex_tmp_titus_research/validate_lw_itm_fast_cancel.py
```

```bash
PYTHONPATH=/Users/robertgrzesik/Development/lumibot \
  python3 /Users/robertgrzesik/Development/.codex_tmp_titus_research/validate_lw_itm_fast_cancel.py
```

Result:

- `PASS: proposed Titus fast-cancel strategy control flow`

The fake-control-flow test proves:

- direct exact-order fill detection submits one hedge immediately,
- callback-first fill handling also submits one hedge immediately,
- a later fill callback does not submit a duplicate hedge,
- a later direct poll after the callback does not submit a duplicate hedge,
- the cancel path sleeps in 1 second increments,
- `CANCELLING` unlocks `pair_lock` and clears `active_order_id`.

The proposed copy compiles and passes the local control-flow harness. It has not been deployed and has not been written back to Titus's saved strategy.

## Prompt / Skills Follow-up

The shared strategy lifecycle prompt and focused runtime skill were updated in BotSpot Agent:

- `botspot_agent/src/botspot_agent/strategy/prompts/markdown/shared_lifecycle_methods.md`
- `botspot_agent/src/botspot_agent/strategy/prompts/markdown/shared_notes.md`
- `botspot_agent/src/botspot_agent/strategy/prompts/markdown/lumibot_fast_order_management.md`
- `botspot_agent/src/botspot_agent/strategy/skills/lumibot-fast-order-management/SKILL.md`
- `botspot_agent/src/botspot_agent/runtime.py`

New guidance tells generate/refine to use exact-order polling for very fast live order workflows, avoid broad `get_orders()` as the hot-path truth for a just-submitted order, treat cancel-pending states as in progress, and use an idempotency guard so polling plus `on_filled_order()` cannot place a duplicate hedge. The main strategy agent now has a runtime skill-loading rule for fast live cancels, immediate hedge legs, pair/spread legs, manual-cancel confusion, volatile quotes, and broker-native OTO/OCO/bracket workflows.

This is intentionally short because the guidance is loaded into strategy generation/refinement prompts.

Deployment evidence:

- BotSpot Agent production commit: `8838855d9146ec48abe657edf11fcb8c93548515`
- GitHub Actions run: `26838995473`
- Result: CI and production deploy succeeded on June 2, 2026.
- Focused prompt tests: `71 passed`
- Full BotSpot Agent test suite: `485 passed, 3 skipped`
- MyPy: `Success: no issues found in 86 source files`

## Market-Hours Broker Proof, June 2, 2026

Account suffix used: Rob-owned Schwab account `4364`.

Timed cancel smoke:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s \
  python3 scripts/schwab_titus_fast_cancel_strategy_smoke.py \
  --account-suffix 4364 \
  --token-path schwab_token.json \
  --wait-seconds 4 \
  --option-limit 0.01 \
  --measure-broad-orders \
  --summary-path tmp/schwab_titus_fast_cancel_strategy_1780424917.json
```

Result:

- submitted TSLL `2026-06-05 $15 CALL` limit order at `0.01`,
- direct order reads showed Schwab status `WORKING` through the 4 second wait,
- `cancel_order(...)` was accepted by Schwab,
- immediate direct read showed final status `CANCELED`,
- `used_broad_get_orders_in_hot_path=false`,
- broad order pull was measured separately at `0.393s`,
- result `pass=true`.

OTO/bracket structure smoke:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s \
  python3 scripts/schwab_titus_workflow_smoke.py \
  --account-suffix 4364 \
  --token-path schwab_token.json \
  --wait-seconds 4 \
  --option-limit 0.01 \
  --child-limit 0.50 \
  --stop-loss 0.01 \
  --summary-path tmp/schwab_titus_workflow_smoke_1780425067.json
```

Result:

- Titus-style timed cancel: `pass=true`, final status `CANCELED`.
- OTO structure: Schwab accepted parent as `orderStrategyType=TRIGGER` with one child, then parent canceled; `pass=true`.
- Bracket structure: Schwab accepted parent as `orderStrategyType=TRIGGER` with child structure, then parent canceled; `pass=true`.

This proves Schwab accepts the order structures and that direct exact-order cancel/read works during market hours. It does not prove parent-fill child-trigger behavior or real filled-option-to-stock-hedge timing.

## Still Needs Market-Hours Proof

The after-hours tests and June 2 market-hours resting-order tests prove submit/read/cancel and order-structure acceptance. They do not prove a real fill event triggering the hedge leg.

Next market-hours validation:

1. Run a Rob-owned strategy using the corrected pattern on a cheap option or stock.
2. Force or allow one parent fill.
3. Confirm the hedge order is submitted immediately, not after the next 1 minute iteration.
4. Confirm no broad `get_orders()` scan blocks the hedge.
5. If OTO/bracket is used, confirm Schwab triggers the child order after the parent fill.

## Filled Option To Stock Hedge Proof, June 2, 2026

Rob-owned Schwab account suffix used: `4364`.

Summary file:

- `tmp/schwab_titus_live_fill_hedge_1780429984.json`

Command:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s \
  python3 scripts/schwab_titus_live_fill_hedge_smoke.py \
  --account-suffix 4364 \
  --token-path schwab_token.json \
  --option-qty 1 \
  --hedge-qty 100 \
  --fill-timeout-seconds 12 \
  --summary-path tmp/schwab_titus_live_fill_hedge_1780429984.json
```

Result:

- selected TSLL `2026-06-05 $15 CALL`,
- submitted one option contract at a marketable limit,
- direct exact-order read confirmed the option order filled,
- submitted the 100-share TSLL hedge `0.661s` after fill detection,
- direct exact-order read confirmed the hedge filled,
- restored the 100 TSLL shares,
- sold to close the option contract,
- result `pass=true`, `cleanup_pass=true`.

This proves the corrected fast-order structure can run a real Schwab fill-to-hedge path without waiting for the next one-minute strategy cycle. It still does not prove Titus's exact production deployment, because this was a Rob-owned controlled smoke using TSLL instead of Titus's LW symbol.
