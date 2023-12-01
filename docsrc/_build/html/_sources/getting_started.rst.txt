What is Lumibot?
************************

Lumibot is a Python library made by Lumiwealth (https://www.lumiwealth.com) that allows you to create trading strategies and backtest them. It also allows you to run your strategies live on a paper trading account. You can also use Lumibot to run your strategies live on a real trading account, but we recommend you start with paper trading first.

Lumibot is designed to be easy to use, but also powerful. It is designed to be used by both beginners and advanced users. It is also designed to be flexible, so you can use it to create any kind of trading strategy you want. It is also designed to be fast, so you can backtest your strategies quickly.

Getting Started
************************

Welcome to Lumibot! This guide will help you get started with Lumibot, we hope you enjoy it!

Here are the steps to get started using the Alpaca broker, if you want to use a different broker, you can see the list of supported brokers under the brokers section.

1. Install the package on your computer

.. code-block:: python

    pip install lumibot

2. import the following modules:

.. code-block:: python

    # importing the trader class
    from lumibot.traders import Trader
    # importing the alpaca broker class
    from lumibot.brokers import Alpaca

3. Create an Alpaca paper trading account: https://app.alpaca.markets/paper/dashboard/overview (you can also use other brokers, but Alpaca is easiest to get started with)
4. Copy your API_KEY and API_SECRET from alpaca dashboard and create a Config class like this:

.. code-block:: python

    ALPACA_CONFIG = {
        # Put your own Alpaca key here:
        "API_KEY": "YOUR_ALPACA_API_KEY",
        # Put your own Alpaca secret here:
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # If you want to go live, you must change this. It is currently set for paper trading
        "ENDPOINT": "https://paper-api.alpaca.markets"
    }

5. Create a strategy class (See strategy section) e.g. class MyStrategy(Strategy) or import an example from our libraries, like this:

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

6. Instantiate the Trader, Alpaca and strategy classes like so:

.. code-block:: python

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(name=strategy_name, budget=budget, broker=broker, symbol="SPY")


7. Backtest the strategy (optional):

.. code-block:: python

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters= {
            "symbol": "SPY"
        },
    )

8. Run the strategy:

.. code-block:: python

    trader.add_strategy(strategy)
    trader.run_all()


And that's it! Now try modifying the strategy to do what you want it to do.

Here it is all together:

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.brokers import Alpaca
    from lumibot.strategies.strategy import Strategy
    from lumibot.traders import Trader


    ALPACA_CONFIG = {
        # Put your own Alpaca key here:
        "API_KEY": "YOUR_ALPACA_API_KEY",
        # Put your own Alpaca secret here:
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # If you want to go live, you must change this. It is currently set for paper trading
        "ENDPOINT": "https://paper-api.alpaca.markets"
    }


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


    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(
        broker=broker, 
        parameters= {
            "symbol": "SPY"
        })

    # Backtest this strategy
    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        # You can also pass in parameters to the backtesting class, this will override the parameters in the strategy
        parameters= {
            "symbol": "SPY"
        },
    )

    # Run the strategy live
    trader.add_strategy(strategy)
    trader.run_all()

Or you can download the file here: https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py


Adding Trading Fees
************************

If you want to add trading fees to your backtesting, you can do so by setting up your backtesting like this:

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import TradingFee

    # Create two trading fees, one that is a percentage and one that is a flat fee
    trading_fee_1 = TradingFee(flat_fee=5) # $5 flat fee
    trading_fee_2 = TradingFee(percent_fee=0.01) # 1% trading fee

    # Backtest this strategy
    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        # You can also pass in parameters to the backtesting class, this will override the parameters in the strategy
        parameters= {
            "symbol": "SPY"
        },
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
    MachineLearningLongShort.backtest(
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        benchmark_asset="TQQQ",
    )

    # Stop the profiler
    yappi.stop()

    # Save the results
    threads = yappi.get_thread_stats()
    for thread in threads:
        print(
            "Function stats for (%s) (%d)" % (thread.name, thread.id)
        )  # it is the Thread.__class__.__name__
        yappi.get_func_stats(ctx_id=thread.id).save(
            f"profile{thread.name}.out", type="pstat"
        )

This will create a `.out` file for each thread that you can then analyze to see where your code is spending the most time.

Viewing the Results
-------------------

We recommend using snakeviz to view the results. You can install it with `pip install snakeviz`. You can then use it to view the results like this:

.. code-block:: bash

    pip install snakeviz

.. code-block:: bash

    snakeviz profile_MainThread.out


I personally like changing the style to sunburst, I find it to be easier to understand.
