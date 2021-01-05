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

The abstract class Strategy define a design pattern that needs to be followed by user-defined strategies.
The design pattern was greatly influenced by React.js components and their lifecycle methods.

### Lifecycle methods

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

#### get_datetime

#### await_market_to_open

#### await_market_to_close

#### get_tracked_position
=> symbol

#### get_tracked_positions

#### get_tracked_order
=> identifier

#### get_tracked_orders

#### get_tracked_assets

#### get_asset_potential_total
=> symbol

#### submit_order
=> order

#### submit_orders
=> orders

#### cancel_order
=> order

#### cancel_orders
=> orders

#### cancel_open_orders

#### sell_all
=> cancel_open_orders=True

#### get_last_price
=> symbol

#### get_last_prices
=> symbols

#### get_tradable_assets
=> easy_to_borrow=None, filter_func=None

















The methods of this class can be split into several categories:
- Helper methods for interacting with alpaca REST API
- Events handling methods. These methods are executed when an event is trigered (Example: trade update)
- Lifecycle methods that are executed at different times during the execution of the bot.

- ```minutes_before_closing```: The lifecycle method on_trading_iteration is executed inside a loop that stops only when there is only ```minutes_before_closing``` minutes remaining before market closes.
By default equals to 15 minutes
- ```sleeptime```: Sleeptime in minute after executing the lifecycle method on_trading_iteration. By default equals to 1 minute




