What is Lumibot?
****************

Lumibot is a Python library made by `Lumiwealth <https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_getting_started>`_ that allows you to create trading strategies and backtest them. It also allows you to run your strategies live on a paper trading account. You can also use Lumibot to run your strategies live on a real trading account, but we recommend you start with paper trading first.

Lumibot is designed to be easy to use, but also powerful. It is designed to be used by both beginners and advanced users. It is also designed to be flexible, so you can use it to create any kind of trading strategy you want. It is also designed to be fast, so you can backtest your strategies quickly.

Lumiwealth
**********

At Lumiwealth, you can join our **community of traders**, take comprehensive **courses on algorithmic trading**, and access our library of **profitable trading bots**. Our strategies have shown exceptional results, with some achieving over **100% annual returns** and others reaching up to **1,000% in backtesting**. 

.. important::

   Visit `Lumiwealth <https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_getting_started>`_ to learn more, and discover how you can enhance your trading skills and potentially achieve high returns with our expert guidance and resources.

Getting Started With Lumibot
*****************************

Welcome to Lumibot! This guide will help you get started with Lumibot. We hope you enjoy it!

Here are the steps to get started using the Alpaca broker. If you want to use a different broker, you can see the list of supported brokers under the brokers section.

Step 1: Install the Package
---------------------------

.. note::

   **Before proceeding, ensure you have installed the latest version of Lumibot**. You can do this by running the following command:

.. code-block:: bash

    pip install lumibot --upgrade

Install the package on your computer:

.. code-block:: bash

    pip install lumibot

Step 2: Import the Following Modules
------------------------------------

.. code-block:: python

    # importing the trader class
    from lumibot.traders import Trader
    # importing the alpaca broker class
    from lumibot.brokers import Alpaca

Step 3: Create an Alpaca Paper Trading Account
---------------------------------------------

Create an Alpaca paper trading account: `https://alpaca.markets/ <https://alpaca.markets/>`_ (you can also use other brokers, but Alpaca is easiest to get started with).

.. note::

   **Make sure to use a paper trading account** at first to get comfortable with Lumibot without risking real money.

Step 4: Configure Your API Keys
-------------------------------

Copy your API_KEY and API_SECRET from the Alpaca dashboard and create a Config class like this:

.. code-block:: python

    ALPACA_CONFIG = {
        # Put your own Alpaca key here:
        "API_KEY": "YOUR_ALPACA_API_KEY",
        # Put your own Alpaca secret here:
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # Set this to False to use a live account
        "PAPER": True
    }

Step 5: Create a Strategy Class
-------------------------------

Create a strategy class (See strategy section) e.g. class MyStrategy(Strategy) or import an example from our libraries, like this:

.. code-block:: python

    class MyStrategy(Strategy):
        # Custom parameters
        parameters = {
            "symbol": "SPY",
            "quantity": 1,
            "side": "buy"
        }

        def initialize(self, symbol=""):
            # Will make on_trading_iteration() run every 180 minutes
            self.sleeptime = "180M"

        def on_trading_iteration(self):
            symbol = self.parameters["symbol"]
            quantity = self.parameters["quantity"]
            side = self.parameters["side"]

            order = self.create_order(symbol, quantity, side)
            self.submit_order(order)

Step 6: Instantiate the Trader, Alpaca, and Strategy Classes
------------------------------------------------------------

.. code-block:: python

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(name="My Strategy", budget=10000, broker=broker, symbol="SPY")

Step 7: Backtest the Strategy (Optional)
----------------------------------------

.. note::

   **Backtesting is a crucial step** to understand how your strategy would have performed in the past. It helps in refining and improving your strategy before going live.

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={
            "symbol": "SPY"
        },
    )

Step 8: Run the Strategy
------------------------

.. note::

   **Running a strategy live** carries real financial risks. Start with paper trading to get familiar with the process and ensure your strategy works as expected.

.. code-block:: python

    trader.add_strategy(strategy)
    trader.run_all()

.. important::

   **And that's it!** Now try modifying the strategy to do what you want it to do.

Here it is all together:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.brokers import Alpaca
    from lumibot.strategies.strategy import Strategy
    from lumibot.traders import Trader

    ALPACA_CONFIG = {
        "API_KEY": "YOUR_ALPACA_API_KEY",
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # Set this to False to use a live account
        "PAPER": True
    }

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "SPY",
            "quantity": 1,
            "side": "buy"
        }

        def initialize(self, symbol=""):
            self.sleeptime = "180M"

        def on_trading_iteration(self):
            symbol = self.parameters["symbol"]
            quantity = self.parameters["quantity"]
            side = self.parameters["side"]
            order = self.create_order(symbol, quantity, side)
            self.submit_order(order)

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(broker=broker, parameters={"symbol": "SPY"})

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={"symbol": "SPY"}
    )

    trader.add_strategy(strategy)
    trader.run_all()

Or you can download the file here: `https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py <https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py>`_.

Adding Trading Fees
*******************

If you want to add trading fees to your backtesting, you can do so by setting up your backtesting like this:

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import TradingFee

    # Create two trading fees, one that is a percentage and one that is a flat fee
    trading_fee_1 = TradingFee(flat_fee=5)  # $5 flat fee
    trading_fee_2 = TradingFee(percent_fee=0.01)  # 1% trading fee

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={"symbol": "SPY"},
        buy_trading_fees=[trading_fee_1, trading_fee_2],
        sell_trading_fees=[trading_fee_1, trading_fee_2],
    )

Profiling to Improve Performance
********************************

Sometimes you may want to profile your code to see where it is spending the most time and improve performance.

We recommend using the `yappi` library to profile your code. You can install it with the following command in your terminal:

.. code-block:: bash

    pip install yappi

Once installed, you can use `yappi` to profile your code like this:

.. code-block:: python

    import yappi

    # Start the profiler
    yappi.start()

    #######
    # Run your code here, eg. a backtest
    #######
    MachineLearningLongShort.run_backtest(
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        benchmark_asset="TQQQ",
    )

    # Stop the profiler
    yappi.stop()

    # Save the results to files
    yappi.get_func_stats().print_all()
    yappi.get_thread_stats().print_all()

    # Save the results to a file
    yappi.get_func_stats().save("yappi.prof", type="pstat")

To get the results of the profiling, you can use snakeviz to visualize the results. You can install snakeviz with the following command in your terminal:

.. code-block:: bash

    pip install snakeviz

Once installed, you can use snakeviz to visualize the results like this:

.. code-block:: bash

    snakeviz yappi.prof

This will open a web browser with a visualization of the profiling results.

.. note::

   **Profiling can slow down your code**, so it is recommended to only use it when you need to.

.. note::

    **Profiling can be complex**, so it is recommended to read the `yappi documentation <


