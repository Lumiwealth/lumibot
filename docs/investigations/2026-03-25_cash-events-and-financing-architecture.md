# 2026-03-25 Cash Events And Financing Architecture

## Objective

Lock a LumiBot-only design that satisfies:

- explicit backtest cash accounting for deposits, withdrawals, and financing
- live broker cash-event collection for Alpaca and Tradier
- future compatibility for Schwab, IBKR, and CCXT-style brokers

## Decisions

### 1. Backtest financing uses one public interface

Keep only:

- `adjust_cash`
- `deposit_cash`
- `withdraw_cash`
- `configure_cash_financing`
- `set_cash_financing_rates`

Remove the duplicate `cash_financing_rates()` hook. The strategy API should match the normal LumiBot lifecycle style
instead of inventing a second callback pattern.

### 2. Cash events are not order events

Do not reuse `_process_trade_event`.

Reason:

- trade events assume a stored order and order-state mutation
- broker cash activities are account-history events
- many are delayed or historical instead of stream-driven

Correct boundary:

- keep the order engine for orders
- add broker cash-activity retrieval
- emit normalized `cash_events` in the live cloud payload

### 3. Normalize broker differences into one `CashEvent` model

Required normalized fields:

- `event_id`
- `broker_event_id`
- `broker_name`
- `event_type`
- `raw_type`
- `raw_subtype`
- `amount`
- `currency`
- `occurred_at`
- `description`
- `direction`
- `is_external_cash_flow`

Canonical event types:

- `deposit`
- `withdrawal`
- `interest`
- `dividend`
- `fee`
- `journal`
- `adjustment`
- `tax`
- `other_cash`

ACH / wire / ACAT / check stay in `raw_type`; they should not become the top-level canonical type.

### 4. Stable IDs are mandatory

`event_id` must be stable across repeated polling and process restarts so downstream services can eventually implement
idempotency cleanly.

Rules:

- if broker provides a unique activity ID, use it
- otherwise synthesize a deterministic ID from normalized fields

### 5. Backtest artifacts reuse existing files

Do not create a new standalone backtest cash-events artifact.

Use the existing artifacts with clearer roles:

- `trades.csv` / `trades.parquet`: discrete event rows
- `stats.csv` / `stats.parquet`: time-series snapshots and aggregated cashflow columns
- `trades.html`: visual review artifact

Implementation choice:

- extend `trades.csv` with `event_kind="cash_event"` rows
- keep trade rows and cash rows in the same event log
- keep raw cash events out of `stats.csv`
- add `cash_adjusted_portfolio_value` to `stats.csv`
- show both `Cash-Adjusted Portfolio Value` and raw `Portfolio Value` in `trades.html`
- add markers for deposits, withdrawals, and financing events in `trades.html`

## Broker comparison

### Alpaca

What exists now:

- LumiBot uses `alpaca-py` `TradingClient`
- installed surface does not expose convenience account-activity methods
- `TradingClient` inherits Alpaca's generic `RESTClient`

Implementation choice:

- use the existing authenticated `TradingClient.get(...)`
- call the Trading API account-activities endpoint
- do not switch LumiBot to `BrokerClient`
- do not use raw `requests`

Observed limitation in this workspace:

- current paper credentials returned `401 unauthorized` when hitting the account-activities endpoint
- this blocks local live smoke validation until valid paper credentials are available

### Tradier

What exists now:

- LumiBot already depends on `lumiwealth-tradier`
- that wrapper already exposes `account.get_history(...)`

Implementation choice:

- call `self.tradier.account.get_history(...)`
- normalize each supported cash history type into `CashEvent`

Broker limitation:

- Tradier account history is not available for sandbox/paper accounts
- Tradier history is updated nightly, not intraday

Observed limitation in this workspace:

- the paper test account path returns unusable/non-history data for this endpoint
- real validation requires a live Tradier account with history

### Schwab

October 2026 implementation update: the adapter now implements
`get_cash_events(since=..., limit=...)` through the existing Schwab SDK client.
The installed SDK contract documents a 60-day recent window. An omitted start
does not mean all-time coverage; older explicit requests fail. Potentially capped
responses are split into overlapping windows with stable-ID deduplication,
bounded request count and explicit failure if a one-second window saturates.
No queue token or silently truncated list is invented.

Cash, ACH, electronic-fund and wire activity is external capital by signed
`netAmount`. `DIVIDEND_OR_INTEREST` remains non-external `other_cash` because
the combined provider type alone cannot distinguish those income categories.
Trade cash and embedded commissions remain performance. Exact bank-sweep labels
and `TRF FDS TO/FRM TYPE <number>` subaccount labels are non-external journals.
Authorized reads showed matching opposite-signed entries within each account;
the [Schwab cash-feature disclosure](https://www.schwab.com/legal/cash-features-disclosure-statement)
describes the automatic sweep of an account's free credit balance. The exact
`FED INC TAX WHLD IRA` label is external distribution withholding (`tax`), in
addition to the net cash disbursement. This distinction must not be applied to
ordinary tax on investment income. Unknown journals, in-kind transfers,
currency conversions, non-final statuses and ambiguous amounts or timestamps
fail rather than acquiring a guessed classification. Compact provider timezone
offsets are normalized for Python 3.10 as well as newer runtimes.

Large timed-out history windows are subdivided with a bounded timeout budget;
partial successful slices never become a successful partial result. The SDK's
normal saved-token refresh path is reused; no second credential owner exists.

The cloud sender removes events only after its existing successful HTTP
acknowledgement. Account/window coverage and durable ingestion proof must
be supplied by the downstream financial-contract owner before publishing a
verified adjusted return. This adapter does not enable that availability.

Regression baseline before implementation: `tests/test_schwab_cash_events.py`
had 32 failures and one pass because the inherited method returned `[]` without
contacting the provider. Regression coverage includes signed funding, income, unknown
classifications, duplicates/conflicts, timezones, sliced boundaries, request
failure after a successful slice and explicit response/retention/size failures.
All fixtures are synthetic; no account authorization, order or money movement
is part of this proof.

Research outcome:

- local `schwab-py` surface exposes transaction retrieval with ACH, cash in/out, wire, journal, and dividend/interest
- this fits the normalized `CashEvent` model cleanly

Original March decision (superseded by the October update above): defer the
adapter while keeping the broker abstraction generic.

### IBKR

Research outcome:

- IBKR exposes transfers/transactions/statements through heavier account-management APIs
- model still fits, but the integration surface is much larger

Decision:

- future-compatible, not in this release

### CCXT / Coinbase-style crypto brokers

Research outcome:

- CCXT exposes deposits/withdrawals/transactions on a capability-gated, exchange-by-exchange basis
- Coinbase transaction concepts also fit the normalized schema

Decision:

- future-compatible, not in this release

## Dedupe strategy

Inside LumiBot:

- poll a bounded lookback window
- keep pending unsent `CashEvent` objects until a cloud post succeeds
- mark `event_id` values as sent only after success
- maintain a bounded sent-ID cache in memory

This gives:

- retry safety on temporary network failures
- no repeat emission on successful polling loops
- stable identifiers for future downstream dedupe

## Payload-size constraints

Current listener compatibility work showed the main risk is payload size, not schema validation.

So the LumiBot payload should:

- include `cash_events` as a top-level sibling field
- send normalized event dictionaries only
- omit full raw broker payload blobs
- keep emission bounded and incremental

## Test strategy

### Unit tests

- one-sided financing updates
- `None` leaves rates unchanged
- `0.0` explicitly zeroes rates
- initialize/on-trading-iteration/on-filled-order strategy usage
- `CashEvent` deterministic ID and serialization
- Alpaca normalization
- Tradier normalization
- cloud payload retry/dedupe behavior

### Integration-style tests

- real backtest that writes `stats.csv`
- real backtest that writes `tearsheet_metrics.json`
- real backtest that writes `trades.csv` with cash-event rows
- real backtest that writes `trades.html` with cash-event markers
- artifact assertions for cash columns and cash metrics
- no-trade deposit/withdrawal regressions proving zero strategy return despite portfolio-value jumps
- tearsheet return-series regression proving external cashflows are subtracted before QuantStats sees the strategy series
- explicit-path regression proving caller-provided `plot_file_html` / `trades_file` paths are honored

### Live smoke validation

- Alpaca: feasible on paper once valid paper credentials are available
- Tradier: requires live account history

## Open validation blockers

1. Alpaca paper credentials in this workspace are currently unauthorized for account activities.
2. Tradier paper cannot validate account history by broker design.
3. `pytest` in this workspace is using an interpreter where `quantstats_lumi.reports.metrics_json` is missing, so
   `tearsheet_metrics.json` currently falls back to the existing placeholder path during integration tests. The cash
   metric contract is therefore validated directly in unit tests and through `stats.csv` artifact assertions.

## Additional implementation note

Backtest return math had to be corrected after the initial cash-accounting implementation. Tracking deposits and
withdrawals in the ledger was not enough on its own because the existing stats/tearsheet path still used raw
`portfolio_value.pct_change()`.

The final implementation now:

- derives `cash_adjustments_net_period` from the cumulative ledger
- computes strategy returns as `(ending_value - starting_value - external_flow) / starting_value`
- preserves financing/dividend/fee effects inside strategy performance
- passes the corrected return series through stats, plot generation, and tearsheet generation
- writes cash-event rows into `trades.csv` / parquet for manual review
- writes `cash_adjusted_portfolio_value` into `stats.csv` / parquet
- plots deposits, withdrawals, and financing markers in `trades.html`
