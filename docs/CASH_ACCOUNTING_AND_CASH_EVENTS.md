# Cash Accounting And Cash Events

## Scope

This document covers two related LumiBot capabilities:

1. Backtest cash accounting inside strategies
2. Live broker cash-event emission in the cloud payload

These are intentionally separate from the order lifecycle engine.

## Documentation map

The public documentation entry points for this feature are:

1. `docsrc/cash_accounting.rst`
2. `docsrc/strategy_methods.account.rst`
3. `docsrc/backtesting.trades_files.rst`
4. `docsrc/backtesting.tearsheet_html.rst`
5. `docsrc/brokers.alpaca.rst`
6. `docsrc/brokers.tradier.rst`

This markdown document is the long-form implementation guide inside the repo.

## North Star

Backtests and live telemetry should treat non-trade cash movement explicitly instead of letting external cashflows get
mixed into strategy performance implicitly.

## Backtest cash accounting

The public strategy-facing API is:

- `adjust_cash(amount, reason="manual_adjustment", allow_negative=None)`
- `deposit_cash(amount, reason="deposit")`
- `withdraw_cash(amount, reason="withdrawal", allow_negative=None)`
- `configure_cash_financing(enabled=True, account_mode="margin", day_count_basis=360, missing_rate_policy="carry_forward")`
- `set_cash_financing_rates(credit_rate_annual=None, debit_rate_annual=None)`

The intended user pattern is explicit and local to the strategy lifecycle:

- configure financing in `initialize()`
- update rates in `on_trading_iteration()`
- deposit or withdraw cash from `on_trading_iteration()` or `on_filled_order()`

Example:

```python
from lumibot.strategies import Strategy


class CashAwareStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.configure_cash_financing(
            enabled=True,
            account_mode="margin",
            day_count_basis=360,
            missing_rate_policy="carry_forward",
        )
        self.set_cash_financing_rates(
            credit_rate_annual=0.0,
            debit_rate_annual=0.0,
        )

    def on_trading_iteration(self):
        fed_funds = self.parameters["fed_funds_by_date"].get(self.get_datetime().date())
        if fed_funds is not None:
            self.set_cash_financing_rates(
                credit_rate_annual=fed_funds,
                debit_rate_annual=fed_funds + 0.01,
            )

        if self.first_iteration:
            self.deposit_cash(5000, reason="monthly_contribution")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.withdraw_cash(1500, reason="distribution")
```

### Semantics

- `None` in `set_cash_financing_rates(...)` means "leave that side unchanged"
- `0.0` means "explicitly set that side to zero"
- `account_mode="margin"` allows negative cash
- `account_mode="cash"` blocks negative cash
- `missing_rate_policy="carry_forward"` reuses the last valid rate for that side

### Accrual timing

Financing accrual is applied once per simulated trading date after `on_trading_iteration()` completes. That means a
strategy can update the daily credit/debit rates inside `on_trading_iteration()` and have those rates apply for that
day's accrual.

### Return calculation

Backtest returns are now cashflow-adjusted instead of using raw `portfolio_value.pct_change()`.

That matters because external capital movement should not be treated as performance:

- deposits should not create artificial positive return
- withdrawals should not create artificial negative return
- financing, dividends, fees, and trading P&L should still remain inside strategy performance

The period return formula is:

```text
(ending_portfolio_value - starting_portfolio_value - net_external_cash_flow) / starting_portfolio_value
```

Where `net_external_cash_flow` comes from the cumulative `cash_adjustments_net_total` ledger.

For manual validation, `stats.csv` now includes period-delta columns:

- `cash_deposits_period`
- `cash_withdrawals_period`
- `cash_adjustments_net_period`
- `cash_financing_credit_period`
- `cash_financing_debit_period`
- `cash_financing_net_period`

This makes it possible to inspect the exact row where a deposit or withdrawal hit cash and confirm that the matching
`return` value stayed economically correct.

### Backtest artifacts

Backtest cash events now appear in the existing artifacts instead of a new standalone cash-events file.

`trades.csv` / `trades.parquet`:

- contains the discrete event stream
- trade rows use `event_kind="trade"`
- cash rows use `event_kind="cash_event"`
- cash-event rows include sparse columns such as:
  - `cash_event_type`
  - `cash_event_amount`
  - `cash_event_reason`
  - `cash_event_direction`
  - `is_external_cash_flow`

`stats.csv` / `stats.parquet`:

- remains the time-series snapshot artifact
- includes `portfolio_value`
- includes `cash_adjusted_portfolio_value`
- includes `cash`
- includes cumulative and period cashflow columns

`trades.html`:

- plots `Cash-Adjusted Portfolio Value` as the primary strategy line
- plots raw `Portfolio Value` as a secondary dashed line
- plots `cash` on the secondary axis
- renders markers for trade fills plus cash events such as deposits, withdrawals, and financing credits/debits

This split keeps raw event rows in the event artifact while keeping snapshots and period totals in the stats artifact.

## Live broker cash events

For live strategies, LumiBot now has a separate broker cash-event path. It does not reuse `_process_trade_event`
because broker cash activity is not an order lifecycle event.

The live payload can now include a top-level `cash_events` array alongside:

- `portfolio_value`
- `cash`
- `positions`
- `orders`

### Normalized event schema

Each `cash_event` entry contains:

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

Canonical `event_type` values:

- `deposit`
- `withdrawal`
- `interest`
- `dividend`
- `fee`
- `journal`
- `adjustment`
- `tax`
- `other_cash`

Broker-native transport remains in `raw_type` / `raw_subtype`. For example, ACH and wire are normalized to
`deposit` / `withdrawal` while preserving their raw transport classification.

### Why this is separate from order events

Order events mutate order state, position state, and strategy callbacks like `on_filled_order()`. Broker cash
activities generally:

- do not have an order object
- may be historical or delayed
- may arrive as nightly account history rather than a stream

So they belong in a separate normalized collection, not in the order event engine.

### Dedupe and payload sizing

LumiBot keeps `cash_events` small by:

- emitting normalized records only
- omitting full raw broker blobs from the payload
- using stable `event_id` values
- deduplicating at runtime before emission
- retrying pending unsent events if the cloud post fails

This keeps the payload future-compatible for downstream idempotency work without inflating the listener payload.

## Broker support in this release

- Alpaca: implemented through the authenticated Trading API client's inherited REST methods
- Tradier: implemented through `lumiwealth-tradier` account history
- Schwab: documented as future-compatible, not implemented in this release
- IBKR: documented as future-compatible, not implemented in this release
- CCXT / crypto exchanges: documented as future-compatible, not implemented in this release

## Current test coverage

- unit tests for cash mutation, financing accrual, and rate semantics
- integration-style backtest proving stats artifact coverage
- unit tests for `CashEvent` normalization and serialization
- payload tests for `send_update_to_cloud()` with retry-safe cash-event handling
- smoke-test entry points for Alpaca and Tradier cash-event retrieval

## Known validation constraints

- Alpaca paper read-path validation depends on valid paper credentials that are authorized for account activities
- Tradier account history is live-only and updated nightly, so paper accounts cannot fully validate the live cash-event
  path
