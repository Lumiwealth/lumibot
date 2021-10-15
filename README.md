README
-------

This library is covered by the MIT license for open sourced software which can be found here: https://github.com/Lumiwealth/lumibot/blob/master/LICENSE

# Quickstart

Currently Alpaca and Interactive Brokers are available as a brokerage services. This 
quickstart is about using Alpaca services. After the quickstart will be instructions 
specific to Interactive Brokers.

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
or import an example from our libraries

```python
from lumibot.strategies.examples import Momentum
```

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
strategy = Momentum(name="momentum", budget=budget, broker=broker)
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
from lumibot.strategies.examples import Momentum
from credentials import AlpacaConfig

budget = 40000
logfile = "logs/test.log"

trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

strategy = Momentum(name="momentum", budget=budget, broker=broker)
trader.add_strategy(strategy)
trader.run_all()
```

# Interactive Brokers

To trade in your interactive brokers account, you must install Trader Workstation 
(or Gateway). Instructions for installation can be found [here](https://interactivebrokers.github.io/tws-api/initial_setup.html).

Once installed, navigate in Trader Workstation to `File/Global Configuration/ then 
API/Settings` The key settings required to trade using Lumibot are: 
  - Enable ActiveX and Socket Clients
  - Disable Read-Only API
  - Socket port `7496` for live trading, `7497` for paper account trading. 
    > It is highly recommended to thoroughly test your algorithm in paper trading mode 
    before trading live.
  - Master API Client ID: You can find in the Trader Workstation by going to File -> Global Configurations -> API -> Settings, then looking for "Master API client ID". This can be any number you choose up to 999. You will use 
    this in your configuration file to log in.

Set up your `credentials.py` file as follows: 
    
    class InteractiveBrokersConfig:
        SOCKET_PORT = 7497 
        CLIENT_ID = "your Master API Client ID three digit number"
        IP = "127.0.0.1"

Set up your entry point file as above, except using Interactive Brokers. Here is an 
example of a completed file:

```python
# main.py
from lumibot.traders import Trader
from lumibot.brokers import InteractiveBrokers
from lumibot.strategies.examples import Strangle
from credentials import InteractiveBrokersConfig

budget = 40000
logfile = "logs/test.log"

trader = Trader(logfile=logfile)
interactive_brokers = InteractiveBrokers(InteractiveBrokersConfig)

strategy = Strangle(name="option", budget=budget, broker=interactive_brokers)
trader.add_strategy(strategy)
trader.run_all()
```

You can also see the file `simple_start_ib.py` for a working bot

# Backtesting

Lumibot has two modes for backtesting. 

1. Yahoo Backtesting: Daily stock backtesting with data from Yahoo. 
2. Pandas Backtesting: Intra-day and inter-day testing of stocks and futures using csv data 
supplied by the user. 

## Yahoo Backtesting
Yahoo backtesting is so named because we get data for the backtesting from the Yahoo Finance website. 
The user is not required to supply data. Any stock information that is available in the Yahoo Finance 
API should be available for backtesting. The Yahoo backtester is not used for futures, options, or forex. 
Additionally, you cannot use the Yahoo backtester for intra-day trading, daily trading only. For 
other securities, use the Pandas backtester.

Using Yahoo backtester, you can also run backtests very easily on your strategies, you do not have to 
modify anything in your strategies.

The backtesting module must be imported. 
```python
from lumibot.backtesting import YahooDataBacktesting
```
Simply call the `backtest()` function on your strategy class. The parameters you must define 
are included in the example below. 

There is a logging function that will save the
details of your backtest (the portfolio value each day, unspent money, etc) 
put into a CSV file in the location of `stats_file`.

```python
from lumibot.backtesting import YahooDataBacktesting
from my_strategy import MyStrategy

from datetime import datetime

# Pick the dates that you want to start and end your backtest
# and the allocated budget
backtesting_start = datetime(2020, 1, 1)
backtesting_end = datetime(2020, 12, 31)
budget = 100000

# Run the backtest
stats_file = "logs/my_strategy_backtest.csv"
plot_file = f"logs/my_strategy_backtest.jpg"
MyStrategy.backtest(
    "my_strategy",
    budget,
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
    stats_file=stats_file,
    plot_file=plot_file,
    benchmark_asset="SPY",
)
```

## Pandas Backtesting
Pandas backtester is named after the python dataframe library 
because the user must provide a strictly formatted dataframe. You can have 
any csv or parquet or database data you wish, but Lumibot will only accept one 
format of dataframe for the time being. 

Pandas backtester allows for intra-day and inter-day backtesting. Time frames for 
raw data are 1 minute and 1 day. Resampling of time frames is not yet available. 

Additionally, with Pandas backtester, it is possible to backtest stocks, stock-like
securities, and futures contracts. Options will be coming shortly and Forex is 
tagged for future development. 

Start by importing the Pandas backtester as follows: 
```python
from lumibot.backtesting import PandasDataBacktesting
```

The start and end dates for the backtest will be set using `datetime.datetime` as follows: 
```python
backtesting_start = datetime.datetime(2021, 1, 8)
backtesting_end = datetime.datetime(2021, 3, 10)
```
Lumibot will start trading at 0000 hrs for the first date and up to 2359 hrs for the last.
This is considered to be in the default time zone of Lumibot unless changed. This is 
America/New York (aka: EST)

Trading hours are set using`datetime.time`. You can restrict the trading times in the day. 
If your data includes a reduced workday, say a holiday Friday, Lumibot will shorten the 
day to match the data. Following is an example of setting the day: 
```python
trading_hours_start = datetime.time(9, 30)
trading_hours_end = datetime.time(16, 0)
```

Pandas backtester will receive a dataframe in the following format: 
```python
Index: 
name: datetime
type: datetime64

Columns: 
names: ['open', 'high', 'low', 'close', 'volume']
types: float
```
Your dataframe should look like this: 
```python

                        high	low	open    close	volume
datetime					
2020-01-02 09:31:00	3237.00	3234.75	3235.25	3237.00	16808
2020-01-02 09:32:00	3237.00	3234.00	3237.00	3234.75	10439
2020-01-02 09:33:00	3235.50	3233.75	3234.50	3234.75	8203
2020-01-02 09:34:00	3238.00	3234.75	3234.75	3237.50	8664
2020-01-02 09:35:00	3238.25	3236.25	3237.25	3236.25	7889
...	...	...	...	...	...
2020-04-22 15:56:00	2800.75	2796.25	2800.75	2796.25	8272
2020-04-22 15:57:00	2796.50	2794.00	2796.25	2794.00	7440
2020-04-22 15:58:00	2794.75	2793.00	2794.25	2793.25	7569
2020-04-22 15:59:00	2793.50	2790.75	2793.50	2790.75	10601
2020-04-22 16:00:00	2791.00	2787.75	2790.75	2789.00	57342
30397 rows × 5 columns
```
Any other formats for dataframes will not work. 

Every asset with data will have its own Data object to keep track of the 
information needed for backtesting that asset. Each Data has the following 
parameters: 
```python
"""
Parameters
----------
strategy : str
    Name of the current strategy object.
asset : Asset Object
    Asset object to which this data is attached.
df : dataframe
    Pandas dataframe containing OHLCV etc trade data. Loaded by user
    from csv.
    Index is date and must be pandas datetime64.
    Columns are strictly ["open", "high", "low", "close", "volume"]
date_start : Datetime or None
    Starting date for this data, if not provided then first date in
    the dataframe.
date_end : Datetime or None
    Ending date for this data, if not provided then last date in
    the dataframe.
trading_hours_start : datetime.time or None
    If not supplied, then default is 0001 hrs.
trading_hours_end : datetime.time or None
    If not supplied, then default is 2359 hrs.
timestep : str
    Either "minute" (default) or "day"
columns : list of str
    For feeding in desired columns (not yet used)."""
```
The data objects will be collected in a dictionary called `pandas_data` using the 
asset as key and the data object as value. Subsequent assets + data can be added
and then the dictionary can be passed into Lumibot for backtesting. 

One of the important differences when using Pandas backtester is that you must use an `Asset` object
for each data csv file loaded. You may not use a `symbol` as you might in Yahoo backtester. 
For an example, let's assume we have futures data for the ES mini. First step would be 
to create an asset object: 
```python
asset = Asset(
    symbol='ES',
    asset_type="future",
    expiration=datetime.date(2021, 6, 11),
    multiplier=50,
)
```
Next step will be to load the dataframe from csv. 
```python
# The names of the columns are important. Also important that all dates in the 
# dataframe are time aware before going into lumibot. 
df = pd.read_csv(
            f"es_data.csv",
            parse_dates=True,
            index_col=0,
            header=0,
            names=["datetime", "high", "low", "open", "close", "volume"],
        )
        df = df[["open", "high", "low", "close", "volume"]]
        df.index = df.index.tz_localize("America/New_York")
```
Third we make a data object. 
```python
data = Data(
    strategy_name,
    asset,
    df,
    date_start=datetime.datetime(2021, 3, 14), 
    date_end=datetime.datetime(2021, 6, 11), 
    trading_hours_start=datetime.time(9, 30),
    trading_hours_end=datetime.time(16, 0),
    timestep="minute",
)
```
When dealing with futures contracts, it is possible to run into some conflicts with the amount of data
available and the expiry date of the contract. Should you hold a position with the contract expires, 
the position will be closed on the last date of trading. If you hold a position and there is no 
data for pricing, Lumibot will throw an error since it has no data to value the position. 

Finally, we create or add to the dictionary that will be passed into Lumibot. 
```python
pandas_data = dict(
    asset = data
)
```
Create a path to save your stats to: 
```python
stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
```
As with Yahoo backtester, data is passed in by using `.backtest()` on your 
strategy class. 
```python
strategy_class.backtest(
        "strategy_name",
        budget,
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        stats_file=stats_file,
    )
```

Putting all of this together, and adding in budget and strategy information, the code would look 
like the following: 
```python
from lumibot.backtesting import PandasDataBacktesting

strategy_name = "Futures"
strategy_class = Futures
budget = 50000

backtesting_start = datetime.datetime(2021, 1, 8)
backtesting_end = datetime.datetime(2021, 3, 10)

trading_hours_start = datetime.time(9, 30)
trading_hours_end = datetime.time(16, 0)
 
asset = Asset(
    symbol='ES',
    asset_type="future",
    expiration=datetime.date(2021, 6, 11),
    multiplier=50,
)
df = pd.read_csv(
    f"es_data.csv",
    parse_dates=True,
    index_col=0,
    header=0,
    names=["datetime", "high", "low", "open", "close", "volume"],
)
df = df[["open", "high", "low", "close", "volume"]]
df.index = df.index.tz_localize("America/New_York")

data = Data(
    "my_strategy",
    asset,
    df,
    date_start=datetime.datetime(2021, 3, 14),
    date_end=datetime.datetime(2021, 6, 11),
    trading_hours_start=datetime.time(9, 30),
    trading_hours_end=datetime.time(16, 0),
    timestep="minute",
)
pandas_data = dict(
    asset=data
)

stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"


strategy_class.backtest(
    strategy_name,
    budget,
    PandasDataBacktesting,
    backtesting_start,
    backtesting_end,
    pandas_data=pandas_data,
    stats_file=stats_file,
)
```

## Example Strategies

There are two locations for Lumibot examples. 
1. Single file scripts that run by themselves, and scripts that call the examples in 2. below, can
be found in `getting_started`. 
2. Lumibot provides a set of several example strategies that you can copy from to create 
your own, they are located in `strategies->examples`. Here is a breakdown of each example strategy:

#### Diversification
Allocates the budget by the percent allocations set in self.portfolio and rebalances every self.
period days. For example, if there is a budget of $100,000 then the strategy will buy $30,000 SPY, 
$40,000 TLT, etc. at current default weights in the strategy. The strategy will then buy/sell 
assets every day depending on self.portfolio_value (the amount of money available in this 
strategy) so that the target percentages laid out in self.portfolio are achieved.

#### Intraday Momentum
Buys the best performing asset from self.symbols over self.momentum_length number of minutes.
For example, if TSLA increased 0.03% in the past two minutes, but SPY, GLD, TLT and MSFT only 
increased 0.01% in the past two minutes, then the strategy will buy TSLA.

#### Momentum
Buys the best performing asset from self.symbols over self.period number of days.
For example, if SPY increased 2% yesterday, but VEU and AGG only increased 1% yesterday,
then the strategy will buy SPY.

#### Simple
Buys and sells 10 of self.buy_symbol every day (not meant to make money, just an example).
For example, Day 1 it will buy 10 shares, Day 2 it will sell all of them, Day 3 it will 
buy 10 shares again, etc.

#### Strangle
An options strategy trading through Interactive Brokers only. A simple strangle 
strategy where the bot simultaneously buys an out-of-the-money call and an 
out-of-the-money put option. The call option's strike price is higher than the 
underlying asset's current market price, while the put has a strike price that is 
lower than the asset's market price.

# Entities

## asset

An asset object represents securities such as stocks or options in Lumibot. Attributes 
that are tracked for assets are: 
  - symbol(str): Ticker symbol representing the stock or underlying for options. So for 
    example if trading IBM calls the symbol would just be `IBM`. 
  - asset_type(str): Asset type can be either `stock`, `option`, `future`, `forex`. default: `stock`
  - name(str): Optional to add in the name of the corporation for logging or printout.  
  #### Options only
  - expiration (str): Expiration of the options contract. Format is datetime.date().
  - strike(float): Contract strike price.
  - right(str): May enter `call` or `put`.
  - multiplier(float): Contract multiplier to the underlying. (default: 1)
  
  #### Futures Only
  Set up a futures contract using the following: 
  - symbol(str): Ticker symbol for the contract, > eg: `ES`
  - asset_type(str): "future"
  - expiration(str): Expiry added as datetime.date() So June 2021 would be datetime.date(2021, 6, 18)`
  
  #### Forex Only
  - symbol(str): Currency base: eg: `EUR`
  - currency(str): Conversion currency. eg: `GBP`
  - asset_type(str): "forex"

When creating a new security there are two options. 
1. Security symbol: It is permissible to use the security symbol only when trading 
   stocks. Lumibot will convert the ticker symbol to an asset behind the scenes.
   
2. Asset object: Asset objects may be created at anytime for stocks or options. For 
   options, futures, or forex asset objects are mandatory due to the additional details required to 
   identify and trade these securities. 
   
Assets may be created using the `self.create_asset` method as follows: 
  `self.create_asset(symbol, asset_type=`option`, **kwargs)` 
    * see attributes above.
    
### Examples:

For stocks:

```python
asset = self.create_asset('SPY', asset_type="stock")
order = self.create_order(asset, 10, "buy")
self.submit_order(order)
```


For futures:

```python
asset = self.create_asset(
  'ES', asset_type="future", expiration=datetime.date(2021, 6, 18)
)
order = self.create_order(asset, 10, "buy")
self.submit_order(order)
```


## bars

This object is a wrapper around pandas dataframe and contains bars data. The raw pandas dataframe
object corresponds to ```bars.df```. The dataframe has the following columns
- open
- high
- low
- close
- volume
- dividend
- stock_splits

The dataframe index is of type ```pd.Timestamp``` localized at the timezone ```America/New_York```.   

Bars objects have the following fields:
- source: the source of the data e.g. (yahoo, alpaca, ...)
- symbol: the symbol of the bars
- df: the pandas dataframe containing all the datas

Bars objects has the following helper methods:
- ```get_last_price()```: Returns the closing price of the last dataframe row
- ```get_last_dividend()```: Returns the dividend per share value of the last dataframe row
- ```get_momentum(start=None, end=None)```: Calculates the global price momentum of the dataframe.
- ```aggregate_bars(frequency)```: Will convert a set of bars to a different timeframe (eg. 1 min to 15 min) frequency (string): The new timeframe that the bars should be in, eg. "15Min", "1H", or "1D". Returns a new Bars object.

When specified, start and end will be used to filter the daterange for the momentum calculation.
  If none of ``start`` or ``end`` are specified the momentum will be calculated from the first row untill
  the last row of the dataframe.
- ```get_total_volume(start=None, end=None)```: returns the sum of the volume column. 
  When ```start``` and/or ```end``` is/are specified use them to filter for that given daterange
  before returning the total volume
- ```filter(start=None, end=None)```: Filter the bars dataframe.
  When ```start``` and/or ```end``` is/are specified use them to filter for that given daterange
  before returning the total volume

When getting historical data from Interactive Brokers, it is important to note that they do not
consider themselves a data supplier. If you exceed these data access pacing rates, your data
will be throttled. Additionally, with respect to above three mentioned helpers, when using 
Interactive Brokers live, tick data is called instead of bar data. This allows for more frequent 
and accurate pricing updates. `get_last_dividend` are not available in Interactive Brokers. (see 
[Interactive Brokers' pacing rules](https://interactivebrokers.github.
io/tws-api/historical_limitations.html))

## order

This object represents an order. Each order belongs to a specific strategy. 

A simple market order can be constructed as follows:

```python
symbol = "SPY"
quantity = 50
side = "buy"
order = self.create_order(symbol, quantity, side)
```

With:
- symbol (str): the string representation of the asset e.g. "GOOG" for Google
- quantity (int): the number of shares to buy/sell
- side (str): must be either ```"buy"``` for buying order or ```"sell"``` for selling order

Order objects have the following helper methods
- ```to_position()```: convert an order to a position belonging to the same strategy with 
```order.quantity``` amount of shares.
- ```get_increment()```: for selling orders returns ```- order.quantity```, for buying orders returns ```order.quantity```
- ```wait_to_be_registered```: wait for the order to be registered by the broker
- ```wait_to_be_closed```: wait for the order to be closed by the broker (Order either filled or closed)

### advanced order types

#### limit order

A limit order is an order to buy or sell at a specified price or better.

To create a limit order object, add the keyword parameter `limit_price`

```python
my_limit_price = 500
order = self.create_order(symbol, quantity, side, limit_price=my_limit_price)
self.submit_order(order)
```

#### stop order

A stop (market) order is an order to buy or sell a security when its price moves past a particular point, ensuring a higher probability of achieving a predetermined entry or exit price.

To create a stop order object, add the keyword parameter `stop_price`.

```python
my_stop_price = 400
order = self.create_order(symbol, quantity, side, stop_price=my_stop_price)
self.submit_order(order)
```

#### stop_limit order

A stop_limit order is a stop order with a limit price (combining stop orders and limit orders)  

To create a stop_limit order object, add the keyword parameters `stop_price` and `limit_price`.

```python
my_limit_price = 405
my_stop_price = 400
order = self.create_order(symbol, quantity, side, stop_price=my_stop_price,               limit_price=my_limit_price)
self.submit_order(order)
```

#### trailing_stop order

Trailing stop orders allow you to continuously and automatically keep updating the stop price threshold 
based on the stock price movement.

To create trailing_stop orders, add either a `trail_price` or a `trail_percent` keyword parameter.

```python
my_trail_price = 20
order_1 = self.create_order(symbol, quantity, side, trail_price=my_trail_price)
self.submit_order(order_1)

my_trail_percent = 2.0 # 2.0 % 
order_2 = self.create_order(symbol, quantity, side, trail_percent=my_trail_percent)
self.submit_order(order_2)
```

### order with legs

#### bracket order

A bracket order is a chain of three orders that can be used to manage your position entry and exit.

The first order is used to enter a new long or short position, and once it is completely filled, 
two conditional exit orders will be activated. One of the two closing orders is called a 
take-profit order, which is a limit order, and the other closing order is a stop-loss order, 
which is either a stop or stop-limit order. 
Importantly, only one of the two exit orders can be executed. Once one of the exit orders fills, 
the other order cancels. Please note, however, that in extremely volatile and fast market 
conditions, both orders may fill before the cancellation occurs.

To create a bracket order object, add the keyword parameters `take_profit_price` and `stop_loss_price`.
A `stop_loss_limit_price` can also be specified to make the stop loss order a stop-limit order.

```python
my_take_profit_price = 420
my_stop_loss_price = 400
order = self.create_order(
  symbol, quantity, side, 
  take_profit_price=my_take_profit_price,
  stop_loss_price=my_stop_loss_price
)
self.submit_order(order)
```
> Interactive Brokers requires the main or parent order to be a limit order. Add  
> `limit_price=my_limit_price`. 

#### OTO (One-Triggers-Other) order 

OTO (One-Triggers-Other) is a variant of bracket order. 
It takes one of the take-profit or stop-loss order in addition to the entry order.

To create an OTO order object, add either a `take_profit_price` or a `stop_loss_price` keyword parameter.
A `stop_loss_limit_price` can also be specified in case of stop loss exit.

> Interactive Brokers requires the main or parent order to be a limit order. Add 
> `limit_price=my_limit_price`. 

#### OCO (One-Cancels-Other) order

OCO orders are a set of two orders with the same side (buy/buy or sell/sell).
In other words, this is the second part of the bracket orders where the entry order is already filled, 
and you can submit the take-profit and stop-loss in one order submission.

To create an OCO order object, add the keyword parameters `take_profit_price` and `stop_loss_price`
and set `position_filled` to `True`. 
A `stop_loss_limit_price` can also be specified to make the stop loss order a stop-limit order.

```python
my_take_profit_price = 420
my_stop_loss_price = 400
order = self.create_order(
  symbol, quantity, side, 
  take_profit_price=my_take_profit_price,
  stop_loss_price=my_stop_loss_price,
  position_filled=True
)
self.submit_order(order)
```
> Interactive Brokers requires the main or parent order to be a limit order. Add `limit_price=my_limit_price`. 

## position

This object represents a position. Each position belongs to a specific strategy.
Position object has the following properties
- strategy (str): the strategy name that this order belongs to
- symbol (str): the string representation of the asset e.g. "GOOG" for Google
- quantity (int): the number of shares held
- orders (list(order)): a list of orders objects responsible for the current state of the position

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

**Strategy Methods** These are strategy helper methods.

**Broker Methods** How to interact with the broker (buy, sell, get positions, etc)

**Data Methods** How to get price data easily

All the methods in each of these categories are described below.

## Lifecycle Methods

The abstract class Strategy defines a design pattern that needs to be followed by user-defined 
strategies. The design pattern was greatly influenced by React.js components and their lifecycle 
methods.

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
    def initialize(self, my_custom_parameter=True):
        self.sleeptime = 5
        self.minutes_before_closing = 15
        self.my_custom_parameter = my_custom_parameter
```

##### Custom Parameters

You can also use the initialize method to define custom parameters 
like ```my_custom_parameter``` in the example above. You can name these parameters however you'd like, and add as many as you'd like.

These parameters can easily be set using the strategy constructor later on.

```python
strategy_1 = MyStrategy(
  name="strategy_1",
  budget=budget,
  broker=broker,
  my_custom_parameter=False,
  my_other_parameter=50
)

strategy_2 = MyStrategy(
  name="strategy_2",
  budget=budget,
  broker=broker,
  my_custom_parameter=True,
  my_last_parameter="SPY"
)
```

or just for backtesting

```python
options = [True, False]
for option in options:
    MyStrategy.backtest(
        "my_strategy",
        budget,
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        stats_file=stats_file,
        my_custom_parameter=option,
        my_last_parameter="SPY"
    )
# `options` in this example is not referring to trading options contracts.
```

##### Changing Market Hours

If you'd like to change the market hours for which the bot operates, then you can use the `set_market()` function like this:
```python
def initialize(self, asset_symbol="MNQ", expiration=datetime.date(2021, 9, 17)):
    self.set_market('24/7')
```
Default is `NASDAQ` days and hours.

Possible calendars include:
```python
['MarketCalendar', 'ASX', 'BMF', 'CFE', 'NYSE', 'stock', 'NASDAQ', 'BATS', 'CME_Equity', 'CBOT_Equity', 'CME_Agriculture', 'CBOT_Agriculture', 'COMEX_Agriculture', 'NYMEX_Agriculture', 'CME_Rate', 'CBOT_Rate', 'CME_InterestRate', 'CBOT_InterestRate', 'CME_Bond', 'CBOT_Bond', 'EUREX', 'HKEX', 'ICE', 'ICEUS', 'NYFE', 'JPX', 'LSE', 'OSE', 'SIX', 'SSE', 'TSX', 'TSXV', 'BSE', 'TASE', 'TradingCalendar', 'ASEX', 'BVMF', 'CMES', 'IEPA', 'XAMS', 'XASX', 'XBKK', 'XBOG', 'XBOM', 'XBRU', 'XBUD', 'XBUE', 'XCBF', 'XCSE', 'XDUB', 'XFRA', 'XETR', 'XHEL', 'XHKG', 'XICE', 'XIDX', 'XIST', 'XJSE', 'XKAR', 'XKLS', 'XKRX', 'XLIM', 'XLIS', 'XLON', 'XMAD', 'XMEX', 'XMIL', 'XMOS', 'XNYS', 'XNZE', 'XOSL', 'XPAR', 'XPHS', 'XPRA', 'XSES', 'XSGO', 'XSHG', 'XSTO', 'XSWX', 'XTAE', 'XTAI', 'XTKS', 'XTSE', 'XWAR', 'XWBO', 'us_futures', '24/7', '24/5']
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

#### on_abrupt_closing

This lifecycle method runs when the strategy execution gets interrupted.
Use this lifecycle method to execute code to stop trading gracefully like selling all assets

```python
class MyStrategy(Strategy):
    def on_abrupt_closing(self):
        self.sell_all()
```

#### on_bot_crash

This lifecycle method runs when the strategy crashes.
By default, if not overloaded,  it calls on_abrupt_closing.

```python
class MyStrategy(Strategy):
    def on_bot_crash(self, error):
        self.on_abrupt_closing()
```

#### on_new_order

This lifecycle method runs when a new order has been successfully submitted to the broker.
Use this lifecycle event to execute code when the broker processes a new order.

Parameters:
- order (Order): The corresponding order object processed 

```python
class MyStrategy(Strategy):
    def on_new_order(self, order):
        self.log_message("%r is currently being processed by the broker" % order)
```

#### on_canceled_order

The lifecycle method called when an order has been successfully canceled by the broker.
Use this lifecycle event to execute code when an order has been canceled by the broker

Parameters:
- order (Order): The corresponding order object that has been canceled

```python
class MyStrategy(Strategy):
    def on_canceled_order(self, order):
        self.log_message("%r has been canceled by the broker" % order)
```

#### on_partially_filled_order

The lifecycle method called when an order has been partially filled by the broker.
Use this lifecycle event to execute code when an order has been partially filled by the broker.

Parameters:
- order (Order): The order object that is being processed by the broker
- price (float): The filled price
- quantity (int): The filled quantity
- multiplier (int): Options multiplier

```python
class MyStrategy(Strategy):
    def on_partially_filled_order(self, order, price, quantity, multiplier):
        missing = order.quantity - quantity
        self.log_message(f"{quantity} has been filled")
        self.log_message(f"{quantity} waiting for the remaining {missing}")
```

#### on_filled_order

The lifecycle method called when an order has been successfully filled by the broker.
Use this lifecycle event to execute code when an order has been filled by the broker

Parameters:
- position (Position): The updated position object related to the order symbol. 
  If the strategy already holds 200 shares of SPY and 300 has just been filled, 
  then `position.quantity` will be 500 shares otherwise if it is a new
  position, a new position object will be created and passed to this method.
- order (Order): The corresponding order object that has been filled
- price (float): The filled price
- quantity (int): The filled quantity
- multiplier (int): Options multiplier

```python
class MyStrategy(Strategy):
    def on_filled_order(self, position, order, price, quantity, multiplier):
        if order.side == "sell":
            self.log_message(f"{quantity} shares of {order.symbol} has been sold at {price}$")
        elif order.side == "buy":
            self.log_message(f"{quantity} shares of {order.symbol} has been bought at {price}$")

        self.log_message(f"Currently holding {position.quantity} of {position.symbol}")
```

## Strategy Methods

#### log_message

Logs an info message prefixed with the strategy name

## Broker Methods

When a strategy is instantiated, a broker object is passed to it (Check Quickstart).
The strategy is run with the passed broker object.
The following shortcuts executes broker methods within the strategy. Some methods 
can use either a `symbol` or an `asset` object. Please see [asset](#asset).

#### sleep

Sleeps for `sleeptime` seconds

Parameters:
- sleeptime (float): The sleep duration in seconds 

#### await_market_to_open

If the market is closed, pauses code execution until ```self.minutes_before_opening``` minutes
before market opens again. If an input (float) is passed as parameter, pauses code execution until 
```input``` minutes before market opens again.

Parameters:
- timedelta (float): Duration in minutes

Return type: ```None```

#### await_market_to_close

If the market is open, pauses code execution until ```self.minutes_before_closing``` minutes
before market closes. If an input (float) is passed as parameter, pauses code execution until 
```input``` minutes before market closes again.

Parameters:
- timedelta (float): Duration in minutes

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

Return type: list(str/asset) 

#### get_asset_potential_total

Check the ongoing positions and the tracked orders of the strategy and returns the total number of shares provided all orders went through. In other words, add all outstanding orders and the total value of the position for an asset.

For example, if you own 100 SPY and have an outstanding limit order of 10 shares, we will count all 110 shares.

Parameters:
- symbol (str/asset): the string representation of the share/asset

Return type: int

#### create_order

Create an order object attached to this strategy (Check the Entities, order section)

Required Parameters:
- symbol (str/asset): representation of the asset to buy
- quantity (int): the quantity of the asset to buy
- side (str): either ```"buy"``` or ```"sell"```

Optional Parameters:
- limit_price (default = None)
- stop_price (default = None)
- time_in_force (default = "day")
- take_profit_price (default = None),
- stop_loss_price (default = None),
- stop_loss_limit_price (default = None),
- trail_price (default = None),
- trail_percent (default = None),
- position_filled (default = None),
- exhange (default = "SMART")


Return type: order

```python
class MyStrategy(Strategy):
    def on_trading_iteration(self):
      # Buy 100 shares of SPY
      order = self.create_order("SPY", 100, "buy")
      self.submit_order(order)
```

For a limit order:

```python
class MyStrategy(Strategy):
    def on_trading_iteration(self):
      # Buy 100 shares of SPY
      order = self.create_order("SPY", 100, "buy", limit_price=100)
      self.submit_order(order)
```

#### submit_order

Submit an order. Returns the processed order.

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

Return type: ```list(order)```

#### wait_for_order_registration

Wait for the order to be registered by the broker

Parameters:
- order (order): the order object

Return type: ```None```

#### wait_for_order_execution

Wait for the order to execute/be canceled

Parameters:
- order (order): the order object

Return type: ```None```

#### wait_for_orders_registration

Wait for the orders to be registered by the broker

Parameters:
- orders (list(order)): the list of orders

Return type: ```None```

#### wait_for_orders_execution

Wait for the orders to execute/be canceled

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
- symbol (str/asset): the string representation of the asset/share

Return type: float

```python
symbol = "SPY"
current_price = self.get_last_price(symbol)
logging.info(f"The current price of {symbol} is {current_price}")
```

#### get_last_prices

Return the last known prices for a list symbols

Parameters:
- symbols (list(str/asset)): list of share/asset representations

Return type: dict of str:float or asset:asset object

#### get_tradable_assets
Return the list of tradable assets for the used broker  
Return type: list(str/asset)  
### Options
#### get_chains  
For a given symbol/asset, returns the full options chain for all exchanges.   
Parameters: symbol/asset  
Return type: Dictionary with `exchanges` as keys, `chain dictionary` as value.  

#### get_chain
Returns an option chain for one symbol on one exchange.   
Parameters: chains, exchange='SMART'   
Returns: Dictionary with:  
- Underlying_conid: Contract ID with Interactive Brokers. 
- TradingClass: Stock symbol
- Multiplier: Option leverage multiplier.
- Expiration: Set of expiration dates. Format 'YYYYMMDD'
- Strikes: Set of strike prices. 

#### get_expiration
Retrieves all of the expiration dates for an option chain, sorted by date.   
Parameters: chains, exchange='SMART'  
Returns: list of expirations date in the format "YYYYMMDD"  
```python
asset = self.create_asset("FB")
chains = self.get_chains(asset)
chain = self.get_chain(chains)
expiration = self.get_expiration(chains)
```


## Data Source Methods

When a strategy is instantiated, a broker object is passed to it (Check Quickstart).
A data_source object can also be passed. When passed, the data_source will be used for
extracting bars and data. If not specified, the strategy will use the broker passed
as the default data source.

The following shortcuts executes data sources methods within the strategy.

#### get_datetime

Return the current datetime localized the datasource timezone e.g. ```America/New_York```. 
During backtesting this will be the time that the strategy thinks that it is.

Return type: datetime

```python
print(f"The current time is {self.get_datetime()}")
```

#### get_timestamp

Return the current UNIX timestamp. 
During backtesting this will be the UNIX timestamp that the strategy thinks that it is.

Return type: float

```python
print(f"The current time is {self.get_timestamp()}")
```

#### get_round_minute

Returns a minute rounded datetime object.

Optional Parameters:
- timeshift (int): a timeshift in minutes from the present.

Example:
```python
# Return a midnight rounded datetime object of three minutes ago 
dt =  self.get_round_minute(timeshift=3)
print(dt)
# datetime.datetime(2021, 2, 21, 9, 17, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
```

Return type: datetime

#### get_last_minute

Returns the last minute rounded datetime object. Shortcut to ```straregy.get_round_minute(timeshift=1)```

Return type datetime.

#### get_round_day

Returns a day rounded datetime object.

Optional Parameters:
- timeshift (int): a timeshift in days from the present.

Example:
```python
# Return a midnight rounded datetime object of three days ago 
dt =  self.get_round_day(timeshift=3)
print(dt)
# datetime.datetime(2021, 2, 21, 0, 0, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
```

Return type datetime

#### get_last_day

Returns the last day rounded datetime object. Shortcut to ```straregy.get_round_day(timeshift=1)```

Return type datetime.

#### get_datetime_range

Takes as input length, timestep and timeshift and returns a tuple of datetime representing the start date and end date.

Parameters:
  - length (int): represents the number of bars required
  - timestep (str): represents the timestep, either ```minute``` (default value) or ```day```.
  - timeshift (timedelta): ```None``` by default. If specified indicates the time shift from the present.

Return type datetime

#### localize_datetime

Converts an unaware datetime object (datetime object without a timezone) to an aware datetime object.
The default timezone is ```America/New_York```.

Parameter:
- dt (datetime): the datetime object to convert.

Example:
```python
from datetime import datetime
dt =  datetime(2021, 2, 21)
print(dt)
# datetime.datetime(2021, 2, 21, 0, 0)
dt_aware = self.localize_datetime(dt)
print(dt_aware)
# datetime.datetime(2021, 2, 21, 0, 0, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
```

Return type: datetime

#### to_default_timezone

Transpose an aware datetime object to the default timezone ```America/New_York```.  

Parameter:
- dt (datetime): the datetime object to convert.

Return type: datetime

#### get_symbol_bars

Return bars for a given symbol.

Parameters:
- symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or 
  asset object. 
- length (int): The number of rows (number of timestamps)
- timestep (str): Either ```"minute""``` for minutes data or ```"day""``` for days data
  default value depends on the data_source (minute for alpaca, day for yahoo, ...)
- timeshift (timedelta): ```None``` by default. If specified indicates the time shift from the present.

Example:
```python
from datetime import timedelta
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
- symbol (list(str/asset)): The symbol string representation (e.g AAPL, GOOG, ...) or 
  asset object. 
- length (int): The number of rows (number of timestamps)
- timestep (str): Either ```"minute""``` for minutes data or ```"day""``` for days data
  default value depends on the data_source (minute for alpaca, day for yahoo, ...)
- timeshift (timedelta): ```None``` by default. If specified indicates the time shift from the present.

Return type: dict of str/asset:bars

#### start_realtime_bars

Starts a real time stream of tickers for Interactive Broker only.

This allows for real time data to stream to the strategy. Bars
are fixed at every five seconds.  They will arrive in the strategy
in the form of a dataframe. The data returned will be: 
- datetime   
- open   
- high   
- low   
- close   
- volume   
- vwap   
- count (trade count)   

Parameters:   
asset : Asset object
    The asset to stream data.
    
keep_bars : int
    How many bars/rows to keep of data. If running for an 
    extended period of time, it may be desirable to limit the 
    size of the data kept. 
    
Returns:   
None

#### get_realtime_bars

Retrieve the real time bars as dataframe. 
        
Returns the current set of real time bars as a dataframe. 
The `datetime` will be in the index. Time intervals will be set at 
5 secs. The columns of the dataframe are: 
- open
- high
- low
- close
- volume
- vwap
- count (trade count)

Parameters:   
asset : Asset object
    The asset that has a stream active. 
    
Returns:   
dataframe : Pandas Dataframe.   
Dataframe containing the most recent pricing information for the asset. The data returned will 
be the `datetime` in the index and the following columns.     
- open  
- high  
- low  
- close  
- volume  
- vwap  
- count (trade count) 

The length of the dataframe will have been set the initial start of the real time bars. 

#### cancel_realtime_bars

Cancels a stream of real time bars for a given asset. 
        
Cancels the real time bars for the given asset. 

Parameters:  
asset : Asset object  
    Asset object that has streaming data to cancel. 
    
Returns:   
None 

#### get_yesterday_dividend

Return dividend per share for the day before for a given symbol

Parameters:
- symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or
  asset object.

Return type: float or asset object

#### get_yesterday_dividends

Return dividend per share for the day before for a given list of symbols. 
Works the same as get_yesterday_dividend but take as parameter a list of symbols.

Parameters:
- symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or
  asset object.

Return type: dict of str:float


## Properties and Parameters

A strategy object has the following properties:

- name: indicates the name of the strategy.
- initial budget: indicates the initial budget
- minutes_before_closing. The lifecycle method on_trading_iteration is 
  executed inside a loop that stops only when there is only ```minutes_before_closing``` 
  minutes remaining before market closes. By default equals to 5 minutes.
  This value can be overloaded when creating a strategy class in order to change the 
  default behaviour. Another option is to specify it when creating an instance the strategy class
  ```python
  my_strategy = MyStrategy("my_strategy", budget, broker, minutes_before_closing=15)
  ```
- minutes_before_opening. The lifecycle method before_market_opens is executed ```minutes_before_opening```
  minutes before the market opens. By default equals to 60 minutes.
  This value can be overloaded when creating a strategy class in order to change the 
  default behaviour. Another option is to specify it when creating an instance the strategy class
  ```python
  my_strategy = MyStrategy("my_strategy", budget, broker, minutes_before_opening=15)
  ```
- sleeptime: Sleeptime in seconds or minutes after executing the lifecycle method 
  on_trading_iteration. By default equals 1 minute. You can set the sleep time as an integer 
  which will be interpreted as minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you 
  can enter the time as a string with the duration numbers first, followed by the time units: 
  'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds, '10M' is 10 minutes. Only "S" and 
  "M" are allowed.
  
  This value can be overloaded when creating a strategy class in order to change the 
  default behaviour. Another option is to specify it when instantiating the strategy class
  ```python
  my_strategy = MyStrategy("my_strategy", budget, broker, sleeptime=2)
  ```
- parameters: a dictionary that contains keyword arguments passed to the constructor. 
  These keyords arguments will be passed to the `self.initialize()` lifecycle method
- is_backtesting: A boolean that indicates whether the strategy is run in live trading
  or in backtesting mode.
- portfolio_value: indicates the actual values of shares held by 
  the current strategy plus the total unspent money.
- unspent_money: indicates the amount of unspent money from the initial
  budget allocated to the strategy. This property is updated whenever a transaction was filled 
  by the broker or when dividends are paid.
- first_iteration: is `True` if the lifecycle method `on_trading_iteration` is being excuted for the first time.
- timezone: The string representation of the timezone used by the trading data_source. 
  By default ``America/New_York``.
- pytz: the ```pytz``` object representation of the timezone property.
