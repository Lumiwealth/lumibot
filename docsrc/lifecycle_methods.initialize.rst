def initialize
===================================

This lifecycle methods is executed only once, when the strategy execution starts. Use this lifecycle method to initialize parameters like:

.. code-block:: python

    # self.sleeptime: the sleeptime duration between each trading iteration in minutes
    # self.minutes_before_closing: number of minutes before the market closes to stop trading
    class MyStrategy(Strategy):
        def initialize(self, my_custom_parameter=True):
            self.sleeptime = "5M"
            self.minutes_before_closing = 15
            self.my_custom_parameter = my_custom_parameter

**Custom Parameters**

You can also use the initialize method to define custom parameters like my_custom_parameter in the example above. You can name these parameters however you'd like, and add as many as you'd like.

These parameters can easily be set using the strategy constructor later on.

.. code-block:: python

    strategy_1 = MyStrategy(
        name="strategy_1",
        budget=budget,
        broker=broker,
        my_custom_parameter=False,
        my_other_parameter=50
    )

.. code-block:: python

    strategy_2 = MyStrategy(
        name="strategy_2",
        budget=budget,
        broker=broker,
        my_custom_parameter=True,
        my_last_parameter="SPY"
    )

or just for backtesting

.. code-block:: python

    options = [True, False]
    for option in options:
        MyStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            stats_file=stats_file,
            my_custom_parameter=option,
            my_last_parameter="SPY"
            budget=budget,
        )

    # `options` in this example is not referring to trading options contracts.

**Changing Market Hours**

If you'd like to change the market hours for which the bot operates, then you can use the set_market() function like this:

.. code-block:: python

    def initialize(self, asset_symbol="MNQ", expiration=datetime.date(2021, 9, 17)):
        self.set_market('24/7')

Default is NASDAQ days and hours.

Possible calendars include:

.. code-block:: python

    ['MarketCalendar', 'ASX', 'BMF', 'CFE', 'NYSE', 'stock', 'NASDAQ', 'BATS', 'CME_Equity', 'CBOT_Equity', 'CME_Agriculture', 'CBOT_Agriculture', 'COMEX_Agriculture', 'NYMEX_Agriculture', 'CME_Rate', 'CBOT_Rate', 'CME_InterestRate', 'CBOT_InterestRate', 'CME_Bond', 'CBOT_Bond', 'EUREX', 'HKEX', 'ICE', 'ICEUS', 'NYFE', 'JPX', 'LSE', 'OSE', 'SIX', 'SSE', 'TSX', 'TSXV', 'BSE', 'TASE', 'TradingCalendar', 'ASEX', 'BVMF', 'CMES', 'IEPA', 'XAMS', 'XASX', 'XBKK', 'XBOG', 'XBOM', 'XBRU', 'XBUD', 'XBUE', 'XCBF', 'XCSE', 'XDUB', 'XFRA', 'XETR', 'XHEL', 'XHKG', 'XICE', 'XIDX', 'XIST', 'XJSE', 'XKAR', 'XKLS', 'XKRX', 'XLIM', 'XLIS', 'XLON', 'XMAD', 'XMEX', 'XMIL', 'XMOS', 'XNYS', 'XNZE', 'XOSL', 'XPAR', 'XPHS', 'XPRA', 'XSES', 'XSGO', 'XSHG', 'XSTO', 'XSWX', 'XTAE', 'XTAI', 'XTKS', 'XTSE', 'XWAR', 'XWBO', 'us_futures', '24/7', '24/5']

Reference
----------

.. autofunction:: strategies.strategy.Strategy.initialize