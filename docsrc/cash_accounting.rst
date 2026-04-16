Cash Accounting
===============

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

These normalized events flow into the live cloud payload as top-level
``cash_events`` records, separate from the order lifecycle engine.

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
5. :doc:`brokers.alpaca` and :doc:`brokers.tradier`
