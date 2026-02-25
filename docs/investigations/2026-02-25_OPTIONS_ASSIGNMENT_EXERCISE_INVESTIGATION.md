# Options Assignment/Exercise Investigation (LumiBot + BotSpot)

**Date:** 2026-02-25
**Status:** Research complete + Phase 1 implementation integrated (2026-02-25)
**Scope:** LumiBot backtesting engine + generated artifacts + BotSpot consumers

---

## Executive Summary

Current LumiBot backtesting behavior cash-settles option expirations through a single `cash_settled` path. It does not currently model physical exercise/assignment outcomes for equity/ETF options.

Broker/exchange behavior indicates default realism should be:

- equity/ETF options: physical settlement (long exercise, short assignment),
- index options: cash settlement.

To align with broker realism, LumiBot should move from a one-size-fits-all cash settlement model to a product-aware expiration engine with explicit `exercised` / `assigned` / `expired` / `cash_settled` event semantics, plus underlying stock delivery rows.

As of 2026-02-25, this Phase 1 behavior has been implemented in LumiBot and propagated to BotSpot consumers where fill-only assumptions existed.

---

## Implementation Snapshot (2026-02-25)

### LumiBot core

- Added broker lifecycle event support for `assigned`, `exercised`, and `expired` alongside `cash_settled`.
- Added product-aware expiration routing:
  - equity/ETF-like options -> physical settlement (exercise/assignment + underlying delivery),
  - index-like options -> cash settlement.
- Added DNE-like guard for unsupported long exercise (insufficient account support) with `expired` lifecycle event.
- Added/updated tests for:
  - short put assignment share delivery,
  - long call exercise share delivery,
  - unsupported long ITM expiration handling,
  - index-option ITM cash settlement,
  - status propagation in trades CSV/parquet outputs.

### BotSpot propagation

- `botspot_react`
  - chart parsers now accept lifecycle statuses (`cash_settled`, `assigned`, `exercised`, `expired`) in addition to fills.
  - centralized and unit-tested lifecycle status utility.
- `botspot_node`
  - trade status normalization updated to canonicalize assignment/exercise/cash-settle/expiry variants.
  - artifact description updated from "every fill" to lifecycle-event semantics.
- `botspot_agent`
  - trades artifact summary updated from fill-only counting to terminal lifecycle-event counting.
  - prompt enum/docs updated to include `ASSIGNED` and `EXERCISED`.
  - added tests covering lifecycle summary behavior.

---

## External Behavior Findings (What Brokers/Rules Actually Do)

## 1) Settlement style is product-defined

- Cboe states that equity and ETF options physically deliver shares when exercised/assigned, while index options (e.g., SPX/XSP) are cash settled.
- This means default behavior should not be “shorts assigned, longs cash-settled.”

## 2) ITM auto-exercise baseline and contrary instructions

- FINRA notices describe exercise-by-exception flows for expiring standardized equity options and the use of Contrary Exercise Advice (DNE/override mechanics).
- Fidelity documents a common auto-exercise threshold of $0.01 ITM.

## 3) Assignment distribution mechanics

- FINRA requires member firms to use approved allocation methods (FIFO/random/random-equivalent) when assigning exercise notices to short positions.
- Simulators cannot reproduce exact market-wide assignment randomness ex ante, but can model deterministic broker-like outcomes at account level.

## 4) Unsupported exercise/assignment risk controls

- Broker guidance (Schwab, IBKR) shows firms may liquidate/close positions or block exercise/assignment outcomes when accounts cannot support resulting delivery or margin requirements.
- IBKR explicitly reserves the right to prohibit exercise and/or close short options when assignment/exercise would create margin deficit.

## 5) Early assignment is possible but path-dependent

- Short American options can be assigned early.
- QuantConnect's documented default assignment model is heuristic for early assignment and deterministic for ITM assignment at expiration.

---

## LumiBot Current-State Findings

## A) Expiration handling is currently cash-settlement-centric

- `lumibot/backtesting/backtesting_broker.py:1131` defines `cash_settle_options_contract(...)`.
- `lumibot/backtesting/backtesting_broker.py:1315` `process_expired_option_contracts(...)` calls cash settlement on expiration.
- No physical delivery branch (exercise/assignment) exists in this expiration workflow.

## B) Broker event model has no assignment/exercise constants

- `lumibot/brokers/broker.py:99-106` defines event constants including `CASH_SETTLED`, but no `ASSIGNED` / `EXERCISED` event constants.
- `lumibot/brokers/broker.py:2153-2187` and `2189-2225` handle known event types; no dedicated assignment/exercise branches.

## C) Order status enum and helper logic are fill/cash-settle focused

- `lumibot/entities/order.py:102` `VALID_STATUS` lacks assignment/exercise statuses.
- `lumibot/entities/order.py:163-174` `OrderStatus` enum lacks assignment/exercise statuses.
- `lumibot/entities/order.py:1229-1232` `is_filled()` currently treats only `filled/fill/cash_settled` as terminal filled states.

## D) Indicators already partially anticipate new statuses

- `lumibot/tools/indicators.py:32-43` marker status allowlist already includes `assigned`, `assignment`, `exercise`, `exercised`, and `expired`.

This is useful: plotting code is partly prepared even if broker/status plumbing is not.

## E) Existing tests encode cash-settlement assumptions

- `tests/backtest/test_example_strategies.py:322-333` asserts `cash_settled` behavior for options hold-to-expiry.
- `tests/test_indicator_subplots.py:204-285` validates `cash_settled` status retention in CSV/parquet and tooltips.

## F) Asset typing relevant to settlement routing

- `lumibot/entities/asset.py:159-168` includes `stock` and `index` types (ETF treated as stock-type in this model).

---

## Artifact Propagation Findings (LumiBot)

- Trade events are exported via `Broker.export_trade_events_to_csv(...)` (`lumibot/brokers/broker.py:2370+`) to CSV and parquet.
- Backtest analysis writes:
  - `*_trade_events.csv/.parquet` from broker export,
  - `*_trades.csv/.parquet` through plot path (`plot_returns`) when plotting enabled,
  - plus trades HTML/indicator artifacts.

Implication: new statuses must be consistently carried through both full trade-events export and simplified trades export.

---

## Downstream Consumer Findings (BotSpot)

## 1) botspot_react

### Hard fill-only chart assumptions

- `src/pages/BacktestChartPage.js:47-73`
  - Parser comments and filters only keep rows with `status === 'fill'`.
- `src/components/BacktestIndicatorChart/BacktestIndicatorChart.js:188-192`
  - Trade markers shown only for fill-like statuses (`fill`/`filled`/contains `fill`).

### Generic status table ingestion is mostly fine

- `src/hooks/useTradesData.js:92-112` passes through `status` and asset fields without strict fill filtering.

Net: chart overlays will drop assignment/exercise/cash-settled lifecycle rows unless updated.

## 2) botspot_node

- `src/services/dataEnvelope.service.ts:460-480`
  - only normalizes `fill/filled`, does not map or describe new terminal statuses.
- `src/Backtest/backtest.service.ts:61`
  - artifact description says trades.csv is "every fill," which becomes semantically outdated once lifecycle events broaden.

## 3) botspot_agent

- `src/botspot_agent/artifacts.py:45-83`
  - summary logic counts only `status == "fill"`; can incorrectly report "0 fills" for assignment/exercise-heavy runs.
- `src/botspot_agent/runtime.py:614`
  - guidance text frames trades artifacts as fill counts first.
- `src/botspot_agent/strategy/prompts/shared/shared_entities.py:102+`
  - prompt-side enum includes `cash_settled`/`expired` but no assignment/exercise statuses.

---

## Recommended LumiBot Behavior (Default)

1. **Physical settlement for equity/ETF options (stock-type underlyings).**
2. **Cash settlement for index options (index-type underlyings).**
3. **Expiration defaults:**
   - long ITM equity/ETF -> exercise,
   - short ITM equity/ETF -> assignment,
   - OTM -> expire worthless,
   - index ITM -> cash settled.
4. **Unsupported account outcomes:**
   - attempt protective close/liquidation pre-expiration cutoff,
   - if still unsupported, apply broker-like DNE/abandon behavior for longs,
   - avoid force-opening unbounded negative balances by default.
5. **Early assignment:** optional heuristic model (not hard default), deterministic and explainable.

---

## Proposed Implementation Plan (Phased)

## Phase 1: Expiration-day physical settlement + explicit events

- Add broker constants and status support for:
  - `ASSIGNED`, `EXERCISED`, `EXPIRED` lifecycle events.
- Extend order status model and helper predicates to recognize these as terminal lifecycle outcomes where appropriate.
- Replace single cash-settlement expiration branch with settlement router:
  - route by underlying product type (`index` vs non-index).
- For physical settlement:
  - emit option lifecycle event row (`assigned` or `exercised`),
  - generate underlying stock delivery row with correct side/quantity at strike.
- Preserve existing cash settlement for index options.

## Phase 2: Risk controls and DNE-like behavior

- Add account-support checks before forced exercise/assignment at expiration.
- Add broker-like fallback policy:
  - pre-close liquidation attempt,
  - DNE-like suppression for unsupported long exercise.
- Add explicit strategy/broker config knobs for strictness.

## Phase 3: Optional early assignment model

- Add opt-in early assignment model for short American-style equity/ETF options.
- Deterministic heuristic baseline (e.g., deep ITM + low extrinsic near expiry; ex-dividend bias for calls).
- Keep index options excluded from early physical assignment.

---

## Test Plan

## A) LumiBot unit tests

- Settlement routing unit tests:
  - stock/ETF underlying -> physical,
  - index underlying -> cash.
- Event/status tests:
  - `assigned`, `exercised`, `expired` flow through `_process_trade_event` and order lifecycle.
- Position accounting tests:
  - underlying shares and option positions adjust correctly for each scenario.

## B) LumiBot integration/backtest tests

Add deterministic scenario strategies and assertions for:

1. short put ITM expiry -> assigned (long shares appear)
2. short covered call ITM expiry -> shares called away
3. long call ITM expiry with sufficient cash -> exercised (shares acquired)
4. long put ITM expiry with sufficient shares -> exercised (shares delivered)
5. long ITM but unsupported account -> protective close or DNE path
6. index option ITM expiry -> cash-settled (no shares delivered)
7. OTM expiries -> `expired` lifecycle event

## C) Artifact contract tests

Validate propagation in:

- `*_trade_events.csv/.parquet`
- `*_trades.csv/.parquet`
- trades tooltip and marker generation
- `*_trades.html`
- `*_indicators.csv/.parquet` and indicators HTML (where statuses appear in tooltips)

Also expand existing tests that currently lock only `cash_settled` assumptions.

## D) Acceptance backtests

- Add at least one acceptance case with physical assignment/exercise outcomes and fixed expected status mix.
- Extend acceptance assertions to include status histogram checks (not just return metrics), and verify artifact presence/content for new statuses.

## E) Downstream BotSpot tests

- botspot_react: chart parsers/renderers should include new lifecycle statuses when plotting trade markers.
- botspot_node: envelope normalization and artifact descriptions should treat trades as lifecycle events, not fills-only.
- botspot_agent: artifact summarizer should count terminal execution/lifecycle events instead of `fill` only.

---

## Key Risks

1. Status proliferation can break legacy fill-only consumers.
2. `is_filled()` and equivalent-status assumptions may silently misclassify new lifecycle outcomes if not updated holistically.
3. Realistic risk handling (unsupported exercise/assignment) is broker-specific and needs explicit documented defaults.
4. Early assignment simulation can become non-deterministic/noisy unless heuristic and seed behavior are controlled.

---

## Sources

- Cboe settlement style explainer (equity/ETF physical; index cash)
  - https://www.cboe.com/insights/posts/why-option-settlement-style-matters/
- Cboe XSP cash-settlement page
  - https://www.cboe.com/tradable_products/sp_500/mini_spx_options/cash_settlement/
- FINRA exercise cut-off and contrary advice notice
  - https://www.finra.org/rules-guidance/notices/information-notice-020321
- FINRA options assignment allocation rules page
  - https://www.finra.org/filing-reporting/regulatory-filing-systems/options-allocation-exercise-assignment-notices
- Fidelity option auto-exercise rules
  - https://www.fidelity.com/options-trading/options-auto-exercise-rules
- Schwab exercise/assignment basics and expiration handling
  - https://www.schwab.com/learn/story/options-exercise-assignment-and-more-beginners-guide
- IBKR expiration/corporate action related liquidations
  - https://www.ibkrguides.com/kb/en-us/article-1767.htm
- QuantConnect option assignment model docs
  - https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/options-models/assignment
- QuantConnect option exercise orders docs
  - https://www.quantconnect.com/docs/v2/writing-algorithms/trading-and-orders/order-types/option-exercise-orders
