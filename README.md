README
-------

# Quickstart

Currently only alpaca is available as a brokerage service. This quickstart is about using Alpaca services.

1) Create an alpaca paper trading account: https://app.alpaca.markets/paper/dashboard/overview
2) Copy your API_KEY and API_SECRET from alpaca dashboard 
   and create a credentials.py file in the root directory of this project with the following class:
```python
class AlpacaConfig:
    API_KEY = "YOUR ALPACA API KEY"
    API_SECRET = "YOUR ALPACA API SECRET"
```
```API_KEY``` and ```API_SECRET``` are obtained from alpaca paper trading dashboard: https://app.alpaca.markets/paper/dashboard/overview

3) Create your own strategy class (See strategy section) e.g. ```class MyStrategy(Startegy)```
4) Create another file meant to be the entrypoint of your code e.g. main.py
5) import the following modules in your main.py:
```python
# importing the trader class
from traders import Trader
# importing the alpaca broker class
from brokers import Alpaca
# importing the credential class created in step 2
from credentials import AlpacaConfig
# importing the strategy class created in step 3
from strategies import MyStrategy
```
6) In your main.py, define variables for the budget allocated to your strategy
```python
budget = 40000
logfile = "logs/test.log"
```
7) Instantiate the ```Trader``` class and the ```Alpaca``` class like so:
```python
trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)
```
The ```Alpaca``` broker class needs your credentials created in step 2 to loging to your paper trading account.
8) Instantiate your strategy class like so:
```python
strategy = MyStrategy(budget=budget, broker=broker)
```
9) Register the strategy within the trader
```python
trader.add_strategy(strategy)
```
10) Run the trader
```python
trader.run_all()
```

Below an example of main.py:
```python
# main.py
from traders import Trader
from brokers import Alpaca
from credentials import AlpacaConfig
from strategies import MyStrategy

budget = 40000
logfile = "logs/test.log"

trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

strategy = MyStrategy(budget=budget, broker=broker)
trader.add_strategy(strategy)
trader.run_all()
```

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
- Lifecycle methods that are executed at different times during the execution of the bot.
- Events handling methods. These methods are executed when an event is trigered
- Helper methods for interacting with the broker passed as parameter
- Helper methods for interacting with the data source object passed as parameter
  
### Lifecycle methods

The abstract class Strategy define a design pattern that needs to be followed by user-defined strategies.
The design pattern was greatly influenced by React.js components and their lifecycle methods.

When building strategies, lifecycle methods needs to be overloaded.
Trading logics should be implemented in these methods.

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

### Events methods

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

### broker methods

When a strategy is instantiated, a broker object is passed to it (Check Quickstart).
The strategy is run with the passed broker object.
The following shortcuts executes broker methods within the strategy.

#### get_timestamp

Return the current timestamp according to the broker API.

Return type: float

#### get_datetime

Return the current datetime according to the broker API.

Return type: datetime

#### await_market_to_open

If the market is closed, pauses code execution until market opens again.

Return type: ```None```

#### await_market_to_close

If the market is open, pauses code execution until market closes.

Return type: ```None```

#### get_tracked_position

Return the strategy tracked position for a given symbol if found else ```None``.

Parameters:
- symbol (str): The share/asset string representation (e.g AAPL, GOOG, ...) 

Return type: position

#### get_tracked_positions

Return all the strategy tracked positions.

Return type: list(position)

#### get_tracked_order

Return the strategy tracked order with the specified identifier if found else ```None``.

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

Check the ongoing positions and the tracked orders of the strategy
and returns the total number of shares provided all orders went through 

Parameters:
- symbol (str): the string representation of the asset/share

Return type: int

#### create_order

Create an order object

Parameters:
- symbol (str): representation of the asset to buy
- quantity (int): the quantity of the asset to buy
- side (str): either ```"buy"``` or ```"sell"```

Return type: order

#### submit_order

Submit an order

Parameters:
- order (order): the order object

Return type: order

#### submit_orders

Submit a list of orders

Parameters:
- orders (list(order)): the list of orders

Return type: ```None```

#### cancel_order

Cancel an order

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

#### get_last_price

Return the last known price for a given symbol

Parameters:
- symbol (str): the string representation of the asset/share

Return type: float

#### get_last_prices

Return the last known prices for a list symbols

Parameters:
- symbols (list(str)): list of asset/share representations

Return type: dict of str:float

#### get_tradable_assets

Return the list of tradable assets for the used broker

Return type: list(str)

### data sources methods

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
- time_unit (timedelta): The timestep between each timestamp
- time_delta (timedelta): ```None``` by default. If specified indicates the time shift.

Example:
```python
import timedelta
#...

# Extract 10 rows of SPY data with one minute timestep between each row
# with the latest data being 24h ago (timedelta(days=1))
bars =  self.get_symbol_bars("SPY",10,timedelta(minutes=1),timedelta(days=1))
```

Return type: bars

#### get_bars

Return a dictionary of bars for a given list of symbols. Works the same as get_symbol_bars
but take as first parameter a list of symbols.

Parameters:
- symbol (list(str)): A list of share/asset string representations (e.g AAPL, GOOG, ...) 
- length (int): The number of rows (number of timestamps)
- time_unit (timedelta): The timestep between each timestamp
- time_delta (timedelta): ```None``` by default. If specified indicates the time shift.

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


### properties and parameters

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
  