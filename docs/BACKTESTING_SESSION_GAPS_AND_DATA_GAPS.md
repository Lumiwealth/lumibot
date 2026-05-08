# BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md

> How LumiBot backtests should behave when bars/quotes are missing (session gaps, early closes, thin trading, multi-asset markets).

**Last Updated:** 2026-01-22
**Status:** Active
**Audience:** Developers, AI Agents

---

## Overview

This document describes **data-driven execution correctness** for backtesting.

We intentionally treat the strategy’s `set_market(...)` as **scheduling/lifecycle convenience**, not the source of truth for execution.

Execution should be based on **per-asset data availability** (OHLC and/or quotes). This allows mixed strategies (e.g., crypto + futures, or equities + crypto) to run under `market="24/7"` without breaking accuracy.

---

## Core rules (execution correctness)

### Rule 1 — No synthetic bars for intraday execution

Do not create “fake” intraday bars (e.g., forward-filling minute timestamps across multi-hour gaps).

Synthetic bars can:
- distort indicators (ATR/RSI/SMA) and cascade trade divergence
- allow fills to occur at times where the market was closed
- hide real data problems by making the series look continuous

### Rule 2 — Orders can only fill when there is actionable data

At a minimum, a fill requires **some** actionable price source:

1) Prefer OHLC/trades bars if available (dense markets).
2) If OHLC is missing but bid/ask quotes are available and the modeled order type supports it, use quotes.
3) If neither OHLC nor quotes are available, the order cannot fill at that time.

This rule is intentionally **data-driven** and does not assume a calendar is always correct.

### Rule 3 — Session gaps are modeled as “no data”

For markets that truly close (futures daily maintenance, weekend gaps, holiday early closes):
- there are no bars
- there are no fills
- pending orders must wait until the next available bar/quote event

If a broker accepts order submission during a closed session, the order is still “working”, but it cannot fill until the market reopens (i.e., until data resumes).

### Rule 4 — Vendor gaps stay explicit

Daily crypto data can have vendor-side missing days even for assets that trade continuously. Do not forward-fill those missing days into synthetic OHLCV candles. Preserve missing rows as `missing=True` so execution code can skip them or fall back through canonical data handling instead of trading against fabricated prices.

---

## Concrete example: CME equity futures early close (Labor Day)

Observed in parity investigations: CME equity index futures can close early (example: 2025-09-01 13:00 ET) and reopen later (18:00 ET).

Data-driven consequence:
- there is a multi-hour gap in minute bars
- a backtest must not simulate fills “inside the gap”
- a market order submitted during the gap can only fill at the next available bar open after reopen (or remain pending if no data appears)

This scenario should remain covered by a deterministic regression test because it is a high-risk correctness edge.

---

## How this interacts with `set_market(...)`

### Scheduling/lifecycle (what `set_market` controls)

Today, `set_market(...)` controls when lifecycle callbacks fire:
- `before_market_opens`, `before_market_closes`, etc.

### Execution (what `set_market` must NOT control)

Execution correctness must not rely on `set_market(...)` because users can legitimately:
- trade multiple venues/products in one strategy (e.g., futures + crypto)
- set `market="24/7"` for convenience

Therefore: fills and mark-to-market should be per-asset and data-driven.

---

## Future work (not required for current speed/accuracy goals)

### Per-asset lifecycle callbacks

Eventually we may want lifecycle hooks that include which market (or asset class) opened/closed,
instead of a single global market.

This is a feature request, not a current requirement.
