# Options Expiration, Exercise, Assignment, and Settlement Policy

**Last Updated:** 2026-02-25
**Status:** Active policy (Phase 1 implemented on 2026-02-25; early-assignment model still optional/future)
**Audience:** LumiBot maintainers, strategy authors, BotSpot consumers

---

## Purpose

Define the default broker-like behavior LumiBot should use for option expiration outcomes, including:

- Physical settlement (shares delivered) vs cash settlement
- Long auto-exercise and short assignment outcomes at expiration
- Handling of unsupported exercise/assignment due to buying power or share constraints
- Optional early assignment simulation model
- Artifact/event semantics that downstream systems can rely on

This policy follows LumiBot's core principle of broker realism (`docs/BACKTESTING_ARCHITECTURE.md`).

---

## Default Settlement Rules

### 1) Product-based settlement type (not long-vs-short based)

- **Equity/ETF options:** default to **physical settlement** (deliver/receive shares)
- **Index options:** default to **cash settlement**

### 2) Expiration outcome matrix (default)

- **Long ITM equity/ETF option:** auto-exercise unless a risk control blocks exercise (see below)
- **Short ITM equity/ETF option:** assigned (shares delivered/received)
- **OTM option (all products):** expires worthless
- **ITM index option (long or short):** cash-settled intrinsic value

### 3) Risk controls for unsupported delivery

When account constraints would make exercise/assignment unsupported:

- Prefer broker-like protective actions:
  - attempt to close/auto-liquidate before cutoff,
  - then apply contrary exercise instruction (DNE) behavior for longs if still unsupported,
  - avoid silently forcing unrealistic unlimited negative cash/short stock by default.
- Allow explicit opt-in config for permissive behavior (force exercise/assignment with negative balances) for research users.

---

## Event and Artifact Semantics

Expiration outcomes should be explicit in the event stream and artifacts.

### Required trade event statuses/types

- `exercised` for long option exercise
- `assigned` for short option assignment
- `expired` for OTM expiration
- `cash_settled` for index options and any explicit cash-settlement paths

### Underlying delivery logging

For physically-settled outcomes, emit both:

1. Option lifecycle event row (`exercised` or `assigned`)
2. Underlying stock trade row that reflects delivered shares

This keeps accounting correct and preserves auditability in:

- `*_trade_events.csv/.parquet`
- `*_trades.csv/.parquet`
- `*_trades.html`

---

## Early Assignment Policy

Early assignment should be supported as an **optional model**.

- Default mode should prioritize deterministic expiration handling.
- Optional early assignment mode can apply deterministic heuristics for short American-style equity/ETF options.
- Do not apply early assignment logic to cash-settled index options.

---

## Why This Policy

Public exchange, FINRA, and broker documentation consistently show:

- Settlement is primarily **product-defined** (equity/ETF physical, many index options cash), not “short assigned / long cash-settled.”
- ITM options are generally auto-exercised by exception unless contrary instructions are submitted.
- Brokers may liquidate or block unsupported exercise/assignment near expiration for risk.

---

## References

- Cboe: Why Option Settlement Style Matters
  - https://www.cboe.com/insights/posts/why-option-settlement-style-matters/
- Cboe XSP cash settlement explainer
  - https://www.cboe.com/tradable_products/sp_500/mini_spx_options/cash_settlement/
- FINRA Information Notice (exercise cut-off + contrary exercise advice)
  - https://www.finra.org/rules-guidance/notices/information-notice-020321
- FINRA Rule 2360 exercise assignment allocation methods
  - https://www.finra.org/filing-reporting/regulatory-filing-systems/options-allocation-exercise-assignment-notices
- Schwab: Options exercise/assignment and expiration guide
  - https://www.schwab.com/learn/story/options-exercise-assignment-and-more-beginners-guide
- Fidelity: Option auto-exercise rules
  - https://www.fidelity.com/options-trading/options-auto-exercise-rules
- IBKR: Expiration and corporate-action related liquidations
  - https://www.ibkrguides.com/kb/en-us/article-1767.htm
