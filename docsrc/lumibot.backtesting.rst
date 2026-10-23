:orphan:

Backtesting
===========================

.. meta::
   :description: Backtesting. Backtesting Broker LumiBot documentation in the LumiBot Python trading framework.

Backtesting Broker
----------------------------------------------

.. automodule:: lumibot.backtesting.backtesting_broker
   :members:
   :undoc-members:
   :show-inheritance:

Data Source Backtesting
----------------------------------------------------

.. automodule:: lumibot.data_sources.data_source_backtesting
   :members:
   :undoc-members:
   :show-inheritance:

Frequently asked questions
--------------------------

What does the BacktestingBroker do?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BacktestingBroker`` stands in for a real broker during a backtest. It reads
prices from a backtesting data source, keeps track of the simulated clock and
market hours, fills your orders, and updates cash and positions. It also
handles option expiration. Your strategy code calls it the same way it would
call a live broker.

What asset classes can LumiBot backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot supports stocks, options, crypto, futures, forex, and prediction
markets. What you can backtest in practice depends on the data source you
pick, because each provider covers different assets. See :doc:`backtesting` for
the provider guides.

What is the difference between portfolio_value and get_portfolio_value()?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.portfolio_value`` is a property that returns the last portfolio value
(cash plus positions) the broker reported. It is fast and does not ask the
broker again. ``self.get_portfolio_value()`` asks the broker for a fresh value.
In live trading it forces a balance refresh and returns ``None`` if the broker
cannot give a fresh value.

How do I add trading fees to a backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create ``TradingFee`` objects from ``lumibot.entities`` and pass them as
``buy_trading_fees`` and ``sell_trading_fees`` to ``backtest()``. A
``TradingFee`` can have a ``flat_fee`` per order, a ``percent_fee`` of the
order value, or a ``per_contract_fee``, which is handy for option commissions.

Which benchmark does a backtest compare against?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default the benchmark is ``SPY``. Pass ``benchmark_asset`` to ``backtest()``
to use a different symbol or ``Asset``.

What happens to my options at expiration in a backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An option that is out of the money expires worthless. An in the money index
option is settled in cash. For other in the money options, a long position is
exercised and a short position is assigned, and the shares are delivered at
the strike price. If the account cannot support exercising a long option,
LumiBot settles its value in cash and logs a warning.

Does the backtester simulate early option assignment?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only if you turn it on. Early assignment is off by default. Set the strategy
parameter ``option_early_assignment_enabled`` to ``True`` to enable it. You can
tune it with ``option_early_assignment_max_dte_days`` (default ``1``) and
``option_early_assignment_max_extrinsic`` (default ``0.05``).