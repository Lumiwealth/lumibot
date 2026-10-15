Cash Accounting
===============

.. meta::
   :description: Cash Accounting. Lumibot supports explicit cash accounting for both:.

Lumibot supports explicit cash accounting for both:

- backtests inside a strategy
- live broker cash-event telemetry in the cloud payload

This keeps deposits and withdrawals out of strategy performance while still
capturing financing, dividends, fees, interest, and adjustments.

Strategy cash methods
---------------------

Use the account-management methods documented under :doc:`strategy_methods.account`
when you need framework-managed cashflows in backtests:

- ``adjust_cash(...)``
- ``deposit_cash(...)``
- ``withdraw_cash(...)``
- ``configure_cash_financing(...)``
- ``set_cash_financing_rates(...)``

Backtest artifacts
------------------

Cash accounting is reflected in the existing backtest outputs:

- :doc:`backtesting.trades_files`
- :doc:`backtesting.tearsheet_html`

Important outputs:

- ``trades.csv`` / ``trades.parquet`` keep discrete cash-event rows alongside trade rows
- ``stats.csv`` / ``stats.parquet`` include ``cash_adjusted_portfolio_value`` and period cashflow columns
- ``trades.html`` overlays raw portfolio value, cash-adjusted portfolio value, cash, and cash-event markers

Broker cash events
------------------

Live broker cash-event normalization is documented in:

- :doc:`brokers.alpaca`
- :doc:`brokers.tradier`
- :doc:`brokers.schwab`

These normalized events flow into the live cloud payload as top-level
``cash_events`` records, separate from the order lifecycle engine.

Account/window funding evidence
-------------------------------

Schwab's ``broker.get_cash_event_sync(since=..., limit=...)`` returns a
``CashEventSync`` containing the known cash events and the result of auditing
the requested window. The default is a bounded 60-day read. It does not prove
all-time history. Windows include their start and exclude their end.

Unknown journals, corrections, non-valid activities and security transfers
without a verified capital valuation produce ``partial`` coverage. Provider
failures produce ``unavailable`` coverage. Known cash remains available, and
neither state can claim a complete covered interval. A complete empty read is
different from a failed read. Do not treat an unresolved transfer as zero funding.

The shared cloud sender includes optional ``funding_sync`` evidence with the
account, requested window, covered intervals, retrieval state, observation
times and stable sync/batch identity. Brokers without this evidence continue
to send cash events; their coverage is unknown. Full selected account identities
belong only in authenticated telemetry and must not be logged.

For a one-shot account snapshot, call
``strategy.send_account_snapshot_to_cloud(max_batches=100)``. It drains pending
batches without running a trading iteration and returns ``False`` for delivery
failure, history-fetch exceptions, lack of progress or batch-budget exhaustion.
A delivered partial/unavailable receipt may return ``True``: the boolean
reports delivery, while ``funding_sync.state`` reports retrieval completeness.
Normal ``send_update_to_cloud()`` sends one batch per update.

Retries retain the same sync/batch identity. Each new sync includes its full
event set, including events in earlier overlapping syncs; stable event IDs let
the receiver deduplicate them. HTTP success confirms queue acceptance. The
receiver must verify every persisted batch before establishing complete
delivery or performance eligibility.

Long-form reference
-------------------

For the full implementation guide and rationale, see the repository document:

- ``docs/CASH_ACCOUNTING_AND_CASH_EVENTS.md``

Documentation map
-----------------

If you are trying to understand the whole feature, read in this order:

1. this page
2. :doc:`strategy_methods.account`
3. :doc:`backtesting.trades_files`
4. :doc:`backtesting.tearsheet_html`
5. :doc:`brokers.alpaca`, :doc:`brokers.tradier` and :doc:`brokers.schwab`
