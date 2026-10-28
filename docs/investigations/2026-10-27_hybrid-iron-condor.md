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

The accepted run at source revision `3f768aaed` completed **eight four-leg
entries and eight signed closes**, each with three contracts per leg. The first
entry filled January 6 at 15:45, its close filled January 7 at 09:47, and the
next entry filled January 8 at 15:45, all New York time. No positions or active
orders remained at the end. There were five skipped daily recommendations and
no failed/omitted planner decisions in the accepted run.

The 64 option-leg fill rows reconcile exactly to **-$192.00 gross P&L**, **$124.80
commissions**, **-$316.80 net P&L** and **$9,683.20 ending equity**. Net return was
-3.168%; the simulator's marked maximum drawdown was 4.536%. This proves the
recorded mechanics, not profitability. All native-run closes were price-distance
stops. Expiry timing, partial exposure, ambiguous submission and repricing remain
regression-test proof rather than exercised historical branches in this window.

Thirteen daily planning sessions produced **116 completed inference responses**
including continuations. Measured usage was **3,680,855 input tokens**, including
**2,170,240 cached**, and **19,093 output tokens**, including the 1,038-token
reasoning subset. Do not add reasoning to output again. Monitoring and exits made
zero model requests. API cash billing on Login with ChatGPT is zero. The registered
Luna Standard API rates ($0.10/M input, $0.01/M cached, $0.50/M output) produce a
**$0.1823104 API-equivalent estimate**, not a cash charge or ChatGPT credit debit.
Actual account credit debits are not exposed by the inference protocol.

Diagnostics are retained separately: 53 additional completed responses with known
usage, about $0.13939754 API-equivalent. An interrupted early diagnostic planner
session has unreported additional request usage, so the known task-wide total
($0.32170794 equivalent across 169 completed responses) is a lower bound. The
accepted backtest's ledger has no unmatched request starts. No API-key fallback
was used.

Two generic core defects were explicitly approved and repaired: subscription
model limits now use OpenAI metadata without entering LiteLLM's unrelated device
login, and completion-only function arguments survive conversion into ADK calls.
They reproduced as two metadata failures and four argument-roundtrip failures
before their fixes. Ordinary argument deltas, parallel calls, continuation history
and unsuccessful-stream handling remain covered. No strategy-specific rules were
added to these shared implementations.

The automatic provenance emitter logged a class/instance `broker` attribute
warning; the separate observed-route artifact confirms AlpacaBacktesting/Alpaca.
The audited receipt below independently records the source, window and hashes.
This warning did not prevent fills, reconciliation, usage recording or completion;
no additional unapproved core repair was made.

The small source/run receipt is
[`2026-10-28_hybrid-iron-condor-backtest.json`](2026-10-28_hybrid-iron-condor-backtest.json).
The final affected inventory passed 385 tests; the documentation HTML build
succeeded with seven warnings. Full market-data and model traces remain private. Both source changes and the
receipt are source publication only: no package release or deployment occurred.

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
mark pricing is explicitly labeled; historical Greeks are calculated locally, and missing prices never become invented bids
or asks. A mechanically completed run alone cannot prove live execution quality,
profitability, or an AI advantage. Source publication does not release or deploy it.
