Strategy Properties
************************

Inside your strategy you can also get a lot of information about the state of the strategy and set specific variables to determine how the strategy works. Here is a list of those properties that you can use:

* **self.name**: indicates the name of the strategy.

* **self.initial_budget**: indicates the initial budget

* **self.minutes_before_closing**: The lifecycle method on_trading_iteration is executed inside a loop that stops only when there is only minutes_before_closing minutes remaining before market closes. By default equals to 5 minutes. This value can be overloaded when creating a strategy class in order to change the default behaviour. Another option is to specify it when creating an instance the strategy class

.. code-block:: python

    my_strategy = MyStrategy("my_strategy", budget, broker, minutes_before_closing=15)

* **self.minutes_before_opening**: The lifecycle method before_market_opens is executed minutes_before_opening minutes before the market opens. By default equals to 60 minutes. This value can be overloaded when creating a strategy class in order to change the default behaviour. Another option is to specify it when creating an instance the strategy class

.. code-block:: python

    my_strategy = MyStrategy("my_strategy", budget, broker, minutes_before_opening=15)

* **self.sleeptime**: Sleeptime in seconds or minutes after executing the lifecycle method on_trading_iteration. By default equals 1 minute. You can set the sleep time as an integer which will be interpreted as minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter the time as a string with the duration numbers first, followed by the time units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds, '10M' is 10 minutes. Only "S" and "M" are allowed.

This value can be overloaded when creating a strategy class in order to change the default behaviour. Another option is to specify it when instantiating the strategy class

.. code-block:: python

    my_strategy = MyStrategy("my_strategy", budget, broker, sleeptime=2)

* **self.parameters**: a dictionary that contains keyword arguments passed to the constructor. These keyords arguments will be passed to the self.initialize() lifecycle method

* **self.is_backtesting**: A boolean that indicates whether the strategy is run in live trading or in backtesting mode.

* **self.portfolio_value**: indicates the actual values of shares held by the current strategy plus the total unspent money.

* **self.unspent_money**: indicates the amount of unspent money from the initial budget allocated to the strategy. This property is updated whenever a transaction was filled by the broker or when dividends are paid.

* **self.first_iteration**: is True if the lifecycle method on_trading_iteration is being excuted for the first time.

* **self.timezone**: The string representation of the timezone used by the trading data_source. By default America/New_York.

* **self.pytz**: the pytz object representation of the timezone property.