Strategy API Overview
=====================

.. meta::
   :description: Learn the core LumiBot Strategy API for prices, positions, orders, portfolio state, schedules, backtests, and trading lifecycle methods.

Most LumiBot strategies need one lifecycle method and a small group of data,
account, and order methods. Start with this complete daily-stock example:

.. code-block:: python

   from datetime import datetime

   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.strategies import Strategy


   class BuyAndHold(Strategy):
       def initialize(self):
           self.sleeptime = "1D"

       def on_trading_iteration(self):
           if self.first_iteration:
               price = self.get_last_price("SPY")
               quantity = int(self.get_cash() // price)
               order = self.create_order("SPY", quantity, "buy")
               self.submit_order(order)


   if __name__ == "__main__":
       BuyAndHold.run_backtest(
           YahooDataBacktesting,
           datetime(2025, 1, 1),
           datetime(2025, 2, 1),
       )

Core methods
------------

.. list-table:: Common Strategy methods
   :header-rows: 1
   :widths: 24 42 34

   * - Task
     - Method
     - Detailed reference
   * - Read the latest price
     - ``self.get_last_price(asset)``
     - :doc:`strategy_methods.data`
   * - Read historical bars
     - ``self.get_historical_prices(asset, length, timestep)``
     - :doc:`strategy_methods.data`
   * - Read cash and positions
     - ``self.get_cash()`` and ``self.get_positions()``
     - :doc:`strategy_methods.account`
   * - Create and submit an order
     - ``self.create_order(...)`` and ``self.submit_order(order)``
     - :doc:`strategy_methods.orders`
   * - Run logic on a schedule
     - ``initialize()`` and ``on_trading_iteration()``
     - :doc:`lifecycle_methods`
   * - Backtest the strategy
     - ``Strategy.run_backtest(...)``
     - :doc:`backtesting.backtesting_function`

Use :doc:`strategy_properties` for fields such as ``first_iteration`` and
``portfolio_value``. Continue to :doc:`strategy_methods` for the complete
categorized method reference or :doc:`agents_quickstart` to add an AI agent to
the same lifecycle.
