README
-------

# Quickstart

Currently only alpaca is available as a brokerage service. This quickstart is about using Alpaca services.

1) Install the package on your computer
```shell
pip install lumibot
```
2) Create an alpaca paper trading account: https://app.alpaca.markets/paper/dashboard/overview
3) Copy your API_KEY and API_SECRET from alpaca dashboard 
   and create a credentials.py file in the root directory of this project with the following class:
```python
class AlpacaConfig:
    API_KEY = "YOUR ALPACA API KEY"
    API_SECRET = "YOUR ALPACA API SECRET"
```
```API_KEY``` and ```API_SECRET``` are obtained from alpaca paper trading dashboard: https://app.alpaca.markets/paper/dashboard/overview

4) Create your own strategy class (See strategy section) e.g. ```class MyStrategy(Startegy)```
5) Create another file meant to be the entrypoint of your code e.g. main.py
6) import the following modules in your main.py:
```python
# importing the trader class
from lumibot.traders import Trader
# importing the alpaca broker class
from lumibot.brokers import Alpaca
# importing the credential class created in step 2
from credentials import AlpacaConfig
# importing the strategy class created in step 3
from lumibot.strategies.examples import Momentum
```
7) In your main.py, define variables for the budget allocated to your strategy. 
   Additionally, define the destination of the logfile.
```python
budget = 40000
logfile = "logs/test.log"
```
8) Instantiate the ```Trader``` class and the ```Alpaca``` class like so:
```python
trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)
```
The ```Alpaca``` broker class needs your credentials created in step 3 to loging to your paper trading account.

9) Instantiate your strategy class like so:
```python
strategy = Momentum(budget=budget, broker=broker)
```
10) Register the strategy within the trader
```python
trader.add_strategy(strategy)
```
11) Run the trader
```python
trader.run_all()
```

Below an example of main.py:
```python
# main.py
from lumibot.traders import Trader
from lumibot.brokers import Alpaca
from lumibot.credentials import AlpacaConfig
from lumibot.strategies.examples import Momentum

budget = 40000
logfile = "logs/test.log"

trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

strategy = Momentum(budget=budget, broker=broker)
trader.add_strategy(strategy)
trader.run_all()
```

# Backtesting

You can also run backtests very easily on any of your strategies, you do not have to modify anything in your strategies. Simply call the `backtest()` function on your strategy. You will also have the details of your backtest (the portfolio value each day, unspent money, etc) put into a CSV file in the location of `stat_file`.

```python
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca

from my_strategy import MyStrategy

from credentials import AlpacaConfig

# Initialize your strategy
broker = Alpaca(AlpacaConfig)
budget = 100000
strategy = MyStrategy(budget=budget, broker=broker)

# Pick the dates that you want to start and end your backtest
backtesting_start = datetime(2018, 1, 1)
backtesting_end = datetime(2019, 1, 1)

# Run the backtest
stat_file = "logs/my_strategy_backtest.csv"
strategy.backtest(
    YahooDataBacktesting,
    budget,
    backtesting_start,
    backtesting_end,
    stat_file=stat_file,
)
```

# Entities

## bars

This object is a wrapper around pandas dataframe and contains bars data. The raw pandas dataframe
object corresponds to ```bars.df```. The dataframe has the following columns
- open
- high
- low
- close
- dividend
- volume

Bars objects has the following fields:
- source: the source of the data e.g. (yahoo, alpaca, ...)
- symbol: the symbol of the bars

Bars objects has the following helper methods:
- ```get_last_price()```: returns the closing price of the last dataframe row
- ```get_last_dividend()```: returns the dividend per share value of the last dataframe row
- ```get_total_volume()```: returns the sum of the volume column
- ```get_momentum_df(momentum_length)```: calculates the price change (momentum) 
  after ```momentum_length``` number of rows for each row and filters rows without momentum.
  Returns a dataframe.
- ```get_momentum()```: calculates the global price momentum of the dataframe

## order

This object represents an order. Each order belongs to a specific strategy. 
Order object has the following properties
- strategy (str): the strategy name that this order belongs to
- symbol (str): the string representation of the asset e.g. "GOOG" for Google
- quantity (int): the number of shares to buy/sell
- side (str): must be either ```"buy""``` for buying order or ```"sell""``` for selling order
- limit_price (float): The limit price of the transaction. If the price becomes greater
  than the limit_ price after submitting the order and before being filled, the order is canceled.
- stop_price (float): This option is for buying orders. Triggers a selling order when
  the asset price becomes lower and reach this value. 
- time_in_force (str): ```"day"``` by default. For more information, check this link: https://alpaca.markets/docs/trading-on-alpaca/orders/#time-in-force

Order objects have also the following helper methods
- ```to_position()```: convert an order to a position belonging to the same strategy with 
```order.quantity``` amount of shares.
- ```get_increment()```: for selling orders returns ```- order.quantity```, for buying orders returns ```order.quantity```
- ```get_momentum```

## position

This object represents a position. Each position belongs to a specific strategy.
Position object has the following properties
- strategy (str): the strategy name that this order belongs to
- symbol (str): the string representation of the asset e.g. "GOOG" for Google
- quantity (int): the number of shares held
- orders (list(order)): a list of orders objects that leds to the current state of the position

Position objects have also the following helper methods
- ```get_selling_order()```: returns an order for selling all the shares attached to this position.

# Strategies

## Strategy

All user defined strategies should inherit from the Strategy class.
```python
from strategies import Strategy

class MyStrategy(Strategy):
    pass
```

The abstract class ```Strategy``` has global parameters with default values, and some 
properties that can be used as helpers to build trading logic.

The methods of this class can be split into several categories:

**Lifecycle Methods** These are executed at different times during the execution of the bot. These represent the main flow of a strategy, some are mandatory.

**Event Methods** These methods are executed when an event is trigered. Similar to lifecycle methods, but only *might* happen.

**Broker Methods** How to interact with the broker (buy, sell, get positions, etc)

**Data Methods** How to get price data easily

All the methods in each of these categories are described below.

## Example Strategies

We have provided a set of several example strategies that you can copy to create your own, they are located in `lumibot->strategies->examples`. Here is a breakdown of each example strategy:

#### Diversification
Allocates the budget between self.portfolio and rebalances every self.period days.
For example, if there is a budget of $100,000 then we will buy $30,000 SPY, $40,000 TLT, etc.
We will then buy/sell assets every day depending on self.portfolio_value (the amount of money
we have in this strategy) so that we match the percentages laid out in self.portfolio.

#### Intraday Momentum
Buys the best performing asset from self.symbols over self.momentum_length number of minutes.
For example, if TSLA increased 0.03% in the past two minutes, but SPY, GLD, TLT and MSFT only 
increased 0.01% in the past two minutes, then we will buy TSLA.

#### Momentum
Buys the best performing asset from self.symbols over self.period number of days.
For example, if SPY increased 2% yesterday, but VEU and AGG only increased 1% yesterday,
then we will buy SPY.

#### Simple
Buys and sells 10 of self.buy_symbol every day (not meant to make money, just an example).
For example, Day 1 it will buy 10 shares, Day 2 it will sell all of them, Day 3 it will 
buy 10 shares again, etc.

## Lifecycle Methods

The abstract class Strategy define a design pattern that needs to be followed by user-defined strategies.
The design pattern was greatly influenced by React.js components and their lifecycle methods.

When building strategies, lifecycle methods needs to be overloaded.
Trading logics should be implemented in these methods.

![lifecycle methods](lifecycle_methods.png)

#### initialize

This lifecycle methods is executed only once, when the strategy execution starts.
Use this lifecycle method to initialize parameters like:
- ```self.sleeptime```: the sleeptime duration between each trading iteration in minutes
- ```self.minutes_before_closing```: number of minutes before the market closes to stop trading

```python
class MyStrategy(Strategy):
    def initialize(self):
        self.sleeptime = 5
        self.minutes_before_closing = 15
```

#### before_market_opens

This lifecycle method is executed each day before market opens. 
If the strategy is first run when the market is already open, this method will be skipped the first day.
Use this lifecycle methods to execute business logic before starting trading like canceling all open orders.

```python
class MyStrategy(Strategy):
    def before_market_opens(self):
        self.cancel_open_orders()
```

#### before_starting_trading

This lifecycle method is similar to before_market_opens.
However, unlike before_market_opens, this method will always be executed before starting 
trading even if the market is already open when the strategy was first launched.
After the first execution, both methods will be executed in the following order
1) before_market_opens
2) before_starting_trading.

Use this lifecycle method to reinitialize variables for day trading like resetting the list of
blacklisted shares.

```python
class MyStrategy(Strategy):
    def before_starting_trading(self):
        self.blacklist = []
```

#### on_trading_iteration

This lifecycle method contains the main trading logic.
When the market opens, it will be executed in a loop.
After each iteration, the strategy will sleep for ```self.sleeptime``` minutes.
If no crash or interuption, the loop will be stopped
```self.minutes_before_closing``` minutes before market closes and will restart 
on the next day when market opens again.

```python
class MyStrategy(Strategy):
    def on_trading_iteration(self):
        # pull data
        # check if should buy an asset based on data
        # if condition, buy/sell asset
        pass
```

#### before_market_closes

This lifecycle method is executed ```self.minutes_before_closing``` minutes before the market closes.
Use this lifecycle method to execute business logic like selling shares and closing open orders.

```python
class MyStrategy(Strategy):
    def before_market_closes(self):
        self.sell_all()
```

#### after_market_closes

This lifecycle method is executed right after the market closes.

```python
class MyStrategy(Strategy):
    def after_market_closes(self):
        pass
```

#### trace_stats

Lifecycle method that will be executed after on_trading_iteration. 
context is a dictionary containing the result of ```locals()``` of ```on_trading_iteration()```
at the end of its execution. 

```locals()``` returns a dictionary of the variables defined in the
scope where it is called.

Use this method to dump stats

```python
import random
class MyStrategy(Strategy):
    def on_trading_iteration(self):
        google_symbol = "GOOG"
  
def trace_stats(self, context, snapshot_before):
    print(context)
    # printing
    # { "google_symbol":"GOOG"}
    random_number = random.randint(0, 100)
    row = {"my_custom_stat": random_number}

    return row
```

## Event Methods

Events methods are similar to lifecycle methods. They are executed on particular conditions.

#### on_abrupt_closing

This event method is called when the strategy execution was interrupted.
Use this event method to execute code to stop trading gracefully like selling all assets

```python
class MyStrategy(Strategy):
    def on_abrupt_closing(self):
        self.sell_all()
```

#### on_bot_crash

This event method is called when the strategy crashes.
By default, if not overloaded,  it calls on_abrupt_closing.

```python
class MyStrategy(Strategy):
    def on_bot_crash(self):
        self.on_abrupt_closing()
```

## Broker Methods

When a strategy is instantiated, a broker object is passed to it (Check Quickstart).
The strategy is run with the passed broker object.
The following shortcuts executes broker methods within the strategy.

#### get_timestamp

Return the current timestamp according to the broker API. During backtesting this will be the time that the strategy thinks that it is.

Return type: float

```python
print(f"The current time is {self.get_timestamp()}")
```

#### get_datetime

Return the current datetime according to the broker API. During backtesting this will be the time that the strategy thinks that it is.

Return type: datetime

```python
print(f"The current time is {self.get_datetime()}")
```

#### await_market_to_open

If the market is closed, pauses code execution until market opens again. This means that `on_trading_iteration` will stop being called until the market opens again.

Return type: ```None```

#### await_market_to_close

If the market is open, pauses code execution until market closes. This means that `on_trading_iteration` will stop being called until the market closes.

Return type: ```None```

#### get_tracked_position

Return the strategy tracked position for a given symbol if found else ```None```.

Parameters:
- symbol (str): The share/asset string representation (e.g AAPL, GOOG, ...) 

Return type: position

#### get_tracked_positions

Return all the strategy tracked positions.

Return type: list(position)

#### get_tracked_order

Return the strategy tracked order with the specified identifier if found else ```None```.

Parameters:
- identifier (str): The broker order identifier 

Return type: order

#### get_tracked_orders

Return all the strategy tracked orders.

Return type: list(order)

#### get_tracked_assets

Return the strategy list of symbols for all tracked positions and orders.

Return type: list(str) 

#### get_asset_potential_total

Check the ongoing positions and the tracked orders of the strategy and returns the total number of shares provided all orders went through. In other words, add all outstanding orders and the total value of the position for an asset.

For example, if you own 100 SPY and have an outstanding limit order of 10 shares, we will count all 110 shares.

Parameters:
- symbol (str): the string representation of the asset/share

Return type: int

#### create_order

Create an order object attached to this strategy (Check the Entities, order section)

Required Parameters:
- symbol (str): representation of the asset to buy
- quantity (int): the quantity of the asset to buy
- side (str): either ```"buy"``` or ```"sell"```

Optional Parameters:
- limit_price (default = None)
- stop_price (default = None)
- time_in_force (default = "day")

Return type: order

```python
class MyStrategy(Strategy):
    def on_trading_iteration(self):
      # Buy 100 shares of SPY
      order = self.create_order("SPY", 100, "buy")
      self.submit_order(order)
```

#### submit_order

Submit an order

Parameters:
- order (order): the order object

Return type: order

```python
class MyStrategy(Strategy):
    def my_function(self):
      # Sell 100 shares of TLT
      order = self.create_order("TLT", 100, "sell")
      self.submit_order(order)
```

#### submit_orders

Submit a list of orders

Parameters:
- orders (list(order)): the list of orders

Return type: ```None```

#### cancel_order

Cancel an order.

Parameters:
- order (order): the order to cancel

Return type: ```None```

#### cancel_orders

Cancel a list of orders

Parameters:
- orders (list(order)): the list of orders to cancel

Return type: ```None```

#### cancel_open_orders

Cancel all the strategy open orders

Return type: ```None```

#### sell_all

Sell all strategy current positions

Return type: ```None```

```python
class MyStrategy(Strategy):
   # Will sell all shares that the strategy is tracking on Ctrl + C
   def on_abrupt_closing(self):
        self.sell_all()
```

#### get_last_price

Return the last known price for a given symbol

Parameters:
- symbol (str): the string representation of the asset/share

Return type: float

```python
symbol = "SPY"
current_price = self.get_last_price(symbol)
logging.info(f"The current price of {symbol} is {current_price}")
```

#### get_last_prices

Return the last known prices for a list symbols

Parameters:
- symbols (list(str)): list of asset/share representations

Return type: dict of str:float

#### get_tradable_assets

Return the list of tradable assets for the used broker

Return type: list(str)

## Data Source Methods

When a strategy is instantiated, a broker object is passed to it (Check Quickstart).
A data_source object can also be passed. When passed, the data_source will be used for
extracting bars and data. If not specified, the strategy will use the broker passed
as the default data source.

The following shortcuts executes data sources methods within the strategy.

#### get_symbol_bars

Return bars for a given symbol.

Parameters:
- symbol (str): The share/asset string representation (e.g AAPL, GOOG, ...) 
- length (int): The number of rows (number of timestamps)
- timestep (str): Either ```"minute""``` for minutes data or ```"day""``` for days data
  default value depends on the data_source (minute for alpaca, day for yahoo, ...)
- timeshift (timedelta): ```None``` by default. If specified indicates the time shift.

Example:
```python
import timedelta
#...

# Extract 10 rows of SPY data with one minute timestep between each row
# with the latest data being 24h ago (timedelta(days=1))
bars =  self.get_symbol_bars("SPY",10,"minute",timedelta(days=1))
```

Return type: bars

#### get_bars

Return a dictionary of bars for a given list of symbols. Works the same as get_symbol_bars
but take as first parameter a list of symbols.

Parameters:
- symbol (list(str)): A list of share/asset string representations (e.g AAPL, GOOG, ...) 
- length (int): The number of rows (number of timestamps)
- timestep (str): Either ```"minute""``` for minutes data or ```"day""``` for days data
  default value depends on the data_source (minute for alpaca, day for yahoo, ...)
- timeshift (timedelta): ```None``` by default. If specified indicates the time shift.

Return type: dict of str:bars

#### get_yesterday_dividend

Return dividend per share for the day before for a given symbol

Parameters:
- symbol (str): The share/asset string representation (e.g AAPL, GOOG, ...) 

Return type: float

#### get_yesterday_dividends

Return dividend per share for the day before for a given list of symbols. 
Works the same as get_yesterday_dividend but take as parameter a list of symbols.

Parameters:
- symbol (list(str)): A list of share/asset string representations (e.g AAPL, GOOG, ...) 

Return type: dict of str:float

## Properties and Parameters

- name (property): indicates the name of the strategy. By default equals to the class name.
```MyStrategy(Strategy)``` will have a name ```"MyStrategy"```
- unspent_money (property): indicates the amount of unspent money from the initial
budget allocated to the strategy. This property is updated whenever a transaction was filled 
by the broker or when dividends are paid.
- portfolio_value (property): indicates the actual values of shares held by 
  the current strategy plus the total unspent money.
- minutes_before_closing (parameter). The lifecycle method on_trading_iteration is 
  executed inside a loop that stops only when there is only ```minutes_before_closing``` 
  minutes remaining before market closes. By default equals to 5 minutes.
  This value can be overloaded when creating a strategy class in order to change the 
  default behaviour
- sleeptime (parameter): Sleeptime in minute after executing the lifecycle method 
  on_trading_iteration. By default equals to 1 minute. 
  This value can be overloaded when creating a strategy class in order to change the 
  default behaviour
