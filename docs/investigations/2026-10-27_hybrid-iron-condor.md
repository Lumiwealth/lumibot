# Hybrid iron-condor lifecycle

The existing example now has one read-only planner and deterministic strategy-owned
execution. This is an explicitly requested hybrid example, not a new platform default.
No shared broker, scheduler, tool or trading policy was changed by the strategy patch.

## Behavior and evidence

- One planning attempt per exchange session, using `chatgpt/gpt-6-luna`, medium
  reasoning and the subscription adapter's Standard/default processing tier.
- Structured plans must name observed listed contracts on the next NYSE session,
  correctly ordered protected wings and a valid credit. Numerical quantity uses
  configured portfolio risk, available cash and a commission allowance.
- Every-minute monitoring groups positions by expiration and ignores unrelated
  underlying assets and orders. It preserves the original 40% price-distance rule.
- Expiry exit starts thirty minutes before the actual session close; ordinary
  iterations continue through the closing window. New entry starts fifteen minutes
  before close only when the strategy's packages and related orders are flat.
- Partial exposure is reconciled before closing, signed quantities determine close
  sides, and required exits stay latched across price rebounds. Active close orders
  are canceled after the strategy's five-minute reprice interval, then terminal
  status is observed before another submission. Three attempts are allowed.
- Uncertain submissions and exhausted exit attempts are explicit incomplete states;
  they are not invented fills or successful risk exits. Live operation would need
  operator attention for those states.

Regression baseline: eleven failures and one pass against the original class.
After implementation, the affected example suite passed 137 tests; the existing
four multi-leg backtesting contracts also passed. Tests use real Asset and Order
objects with isolated strategy inputs; they are not model or broker integration proof.

## Historical qualification

The bounded run is January 5, 2026 at 09:30 through January 23 at 15:40 New York
time, with $10,000 initial equity and an assumed $0.65 per-contract commission on
both entry and exit. Underlying prices are unadjusted so they match option strikes.
The end precedes the final day's new entry. Minute underlying and next-session
option contract/price preflights succeeded.

The initial native runs exposed a setup defect before model inference: output-limit
metadata lookup delegates `chatgpt/<model>` to LiteLLM's separate ChatGPT provider,
starting an unrelated device-authentication flow instead of using the connected
LumiBot subscription adapter. A focused regression reproduces that lookup.
The proposed generic repair maps metadata to the existing OpenAI model namespace;
it does not replace inference, extract credentials, or change account settings.

The model-backed historical lifecycle is **not yet qualified**. There is no new
accepted entry/exit/re-entry result, return claim or measured completed-run cost yet.

## Reproduction and reporting

Use `scripts/backtest_hybrid_iron_condor.py --start <local-ISO-datetime> --end
<local-ISO-datetime> --output-dir <private-run-directory>` after connecting the
existing supported ChatGPT profile and configuring Alpaca/FRED data credentials.
The runner uses the actual agent/runtime and BacktestingBroker, not a fixed model
transcript, fixture trades or a special proof mode.

The ordinary agent detail trace and per-turn usage records retain completed
inference-response token usage, including tool continuations. Count planner turns
separately from completed responses. Reasoning is part of output, not an extra
token total. Report failed/interrupted calls with unknown usage separately.
API charges are zero on this subscription route; token-based API-equivalent cost
is diagnostic. The inference protocol does not expose actual ChatGPT credit debits.

Alpaca historical option marks are trade prints, not historical NBBO. Historical
mark pricing is explicitly labeled, and missing prices never become invented bids
or asks. A mechanically completed run alone cannot prove live execution quality,
profitability, or an AI advantage. Source publication does not release or deploy it.
