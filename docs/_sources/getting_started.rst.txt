Getting Started
************************

Currently Alpaca and Interactive Brokers are available as a brokerage services. This quickstart is about using Alpaca services. After the quickstart will be instructions specific to Interactive Brokers.

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

    class AlpacaConfig:
        # Put your own Alpaca key here:
        API_KEY = "YOUR_ALPACA_API_KEY"
        # Put your own Alpaca secret here:
        API_SECRET = "YOUR_ALPACA_SECRET"
        # If you want to go live, you must change this. It is currently set for paper trading
        ENDPOINT = "https://paper-api.alpaca.markets"

5. Create a strategy class (See strategy section) e.g. class MyStrategy(Strategy) or import an example from our libraries, like this:

.. code-block:: python

    class MyStrategy(Strategy):
        def initialize(self, symbol=""):
            # Will make on_trading_iteration() run every 180 minutes
            self.sleeptime = "180M"

            # Custom parameters
            self.symbol = symbol
            self.quantity = 1
            self.side = "buy"

        def on_trading_iteration(self):
            self.order = self.create_order(self.symbol, self.quantity, self.side)
            self.submit_order(self.order)

6. Instantiate the Trader, Alpaca and strategy classes like so:

.. code-block:: python

    trader = Trader()
    broker = Alpaca(AlpacaConfig)
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


    class AlpacaConfig:
        # Put your own Alpaca key here:
        API_KEY = "YOUR_ALPACA_API_KEY"
        # Put your own Alpaca secret here:
        API_SECRET = "YOUR_ALPACA_SECRET"
        # If you want to go live, you must change this. It is currently set for paper trading
        ENDPOINT = "https://paper-api.alpaca.markets"


    class MyStrategy(Strategy):
        def initialize(self, symbol=""):
            # Will make on_trading_iteration() run every 180 minutes
            self.sleeptime = "180M"

            # Custom parameters
            self.symbol = symbol
            self.quantity = 1
            self.side = "buy"

        def on_trading_iteration(self):
            self.order = self.create_order(self.symbol, self.quantity, self.side)
            self.submit_order(self.order)


    trader = Trader()
    broker = Alpaca(AlpacaConfig)
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
        parameters= {
            "symbol": "SPY"
        },
    )

    # Run the strategy live
    trader.add_strategy(strategy)
    trader.run_all()

Or you can download the file here: https://github.com/Lumiwealth/lumibot/blob/master/example_strategies/simple_start_single_file.py


Adding Trading Fees
************************

If you want to add trading fees to your backtesting, you can do so by setting up your backtesting like this:

.. code-block:: python

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
        parameters= {
            "symbol": "SPY"
        },
        buy_trading_fees=[trading_fee_1, trading_fee_2],
        sell_trading_fees=[trading_fee_1, trading_fee_2],
    )