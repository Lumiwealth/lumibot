Strategy Methods
************************

When a strategy is instantiated, a broker object is passed to it (Check Quickstart). The strategy is run with the passed broker object. The following shortcuts executes broker methods within the strategy. Some methods can use either a symbol or an asset object. Please see asset.

**self.log_message()**

Logs an info message prefixed with the strategy

**self.sleep()**

Sleeps for sleeptime seconds. The way that sleep should be used within a strategy. Using the regular Python sleep() method will throw an error.

Parameters:

sleeptime (float): The sleep duration in seconds

**self.await_market_to_open()**

If the market is closed, pauses code execution until self.minutes_before_opening minutes before market opens again. If an input (float) is passed as parameter, pauses code execution until input minutes before market opens again.

Parameters:

timedelta (float): Duration in minutes
Return type: None

**self.await_market_to_close()**

If the market is open, pauses code execution until self.minutes_before_closing minutes before market closes. If an input (float) is passed as parameter, pauses code execution until input minutes before market closes again.

Parameters:

timedelta (float): Duration in minutes
Return type: None

**self.get_tracked_position()**

Return the strategy tracked position for a given symbol if found else None.

Parameters:

symbol (str): The share/asset string representation (e.g AAPL, GOOG, ...)
Return type: position

**self.get_tracked_positions()**

Return all the strategy tracked positions.

Return type: list(position)

**self.get_tracked_order()**

Return the strategy tracked order with the specified identifier if found else None.

Parameters:

identifier (str): The broker order identifier
Return type: order

**self.get_tracked_orders()**

Return all the strategy tracked orders.

Return type: list(order)

**self.get_tracked_assets()**

Return the strategy list of symbols for all tracked positions and orders.

Return type: list(str/asset)

**self.get_asset_potential_total**

Check the ongoing positions and the tracked orders of the strategy and returns the total number of shares provided all orders went through. In other words, add all outstanding orders and the total value of the position for an asset.

For example, if you own 100 SPY and have an outstanding limit order of 10 shares, we will count all 110 shares.

Parameters:

symbol (str/asset): the string representation of the share/asset
Return type: int

**self.create_order**

Create an order object attached to this strategy (Check the Entities, order section)

Required Parameters:

symbol (str/asset): representation of the asset to buy
quantity (int): the quantity of the asset to buy
side (str): either "buy" or "sell"
Optional Parameters:

limit_price (default = None)
stop_price (default = None)
time_in_force (default = "day")
take_profit_price (default = None),
stop_loss_price (default = None),
stop_loss_limit_price (default = None),
trail_price (default = None),
trail_percent (default = None),
position_filled (default = None),
exhange (default = "SMART")
Return type: order

class MyStrategy(Strategy):
    def on_trading_iteration(self):
      # Buy 100 shares of SPY
      order = self.create_order("SPY", 100, "buy")
      self.submit_order(order)
For a limit order:

class MyStrategy(Strategy):
    def on_trading_iteration(self):
      # Buy 100 shares of SPY
      order = self.create_order("SPY", 100, "buy", limit_price=100)
      self.submit_order(order)
submit_order
Submit an order. Returns the processed order.

Parameters:

order (order): the order object
Return type: order

class MyStrategy(Strategy):
    def my_function(self):
      # Sell 100 shares of TLT
      order = self.create_order("TLT", 100, "sell")
      self.submit_order(order)
submit_orders
Submit a list of orders

Parameters:

orders (list(order)): the list of orders
Return type: list(order)

wait_for_order_registration
Wait for the order to be registered by the broker

Parameters:

order (order): the order object
Return type: None

wait_for_order_execution
Wait for the order to execute/be canceled

Parameters:

order (order): the order object
Return type: None

wait_for_orders_registration
Wait for the orders to be registered by the broker

Parameters:

orders (list(order)): the list of orders
Return type: None

wait_for_orders_execution
Wait for the orders to execute/be canceled

Parameters:

orders (list(order)): the list of orders
Return type: None

**self.cancel_order**
Cancel an order.

Parameters:

order (order): the order to cancel
Return type: None

**self.cancel_orders()**
Cancel a list of orders

Parameters:

orders (list(order)): the list of orders to cancel
Return type: None

**self.cancel_open_orders()**
Cancel all the strategy open orders

Return type: None

**self.sell_all()**
Sell all strategy current positions

Return type: None

class MyStrategy(Strategy):
   # Will sell all shares that the strategy is tracking on Ctrl + C
   def on_abrupt_closing(self):
        self.sell_all()

**self.get_last_price()**
Return the last known price for a given symbol

Parameters:

symbol (str/asset): the string representation of the asset/share
Return type: float

symbol = "SPY"
current_price = self.get_last_price(symbol)
logging.info(f"The current price of {symbol} is {current_price}")

**self.get_last_prices()**
Return the last known prices for a list symbols

Parameters:

symbols (list(str/asset)): list of share/asset representations
Return type: dict of str:float or asset:asset object

**self.get_tradable_assets()**

Return the list of tradable assets for the used broker
Return type: list(str/asset)

Options
"""""""""""""""""""

**self.get_chains**
For a given symbol/asset, returns the full options chain for all exchanges.
Parameters: symbol/asset
Return type: Dictionary with exchanges as keys, chain dictionary as value.

**self.get_chain**
Returns an option chain for one symbol on one exchange.
Parameters: chains, exchange='SMART'
Returns: Dictionary with:

Underlying_conid: Contract ID with Interactive Brokers.
TradingClass: Stock symbol
Multiplier: Option leverage multiplier.
Expiration: Set of expiration dates. Format 'YYYYMMDD'
Strikes: Set of strike prices.
get_expiration
Retrieves all of the expiration dates for an option chain, sorted by date.
Parameters: chains, exchange='SMART'
Returns: list of expirations date in the format "YYYYMMDD"

asset = self.create_asset("FB")
chains = self.get_chains(asset)
chain = self.get_chain(chains)
expiration = self.get_expiration(chains)
get_greeks
Returns the greeks for the option asset at the current bar.

Will return all the greeks available unless any of the individual greeks are selected, then will only return those greeks.

To return all of the greeks:

mygreeks = self.get_greeks(asset)
print(mygreeks)
{
    'implied_volatility': 0.43082467998525587, 
    'delta': 0.4261267500109485, 
    'option_price': 1.5367826121627828, 
    'pv_dividend': 0.0, 
    'gamma': 0.07865783808317735, 
    'vega': 0.04556740333269094, 
    'theta': -0.44406813241266924, 
    'underlying_price': 148.98
 }
Note that pv_dividend is only available in live testing.
To return only specific greeks, set them as True when calling the function..

mygreeks = self.get_greeks(asset, delta=True, theta=True)
print(mygreeks)
{'delta': 0.4192703569137862, 'theta': -0.44151812979314764}
Parameters
asset : Asset
     Option asset only for with greeks are desired.
**kwargs
implied_volatility : boolean
    True to get the implied volatility. (default: True)
delta : boolean
    True to get the option delta value. (default: True)
option_price : boolean
    True to get the option price. (default: True)
pv_dividend : boolean
    True to get the present value of dividends expected on the option's underlying. (default: True)
gamma : boolean
    True to get the option gamma value. (default: True)
vega : boolean
    True to get the option vega value. (default: True)
theta : boolean
    True to get the option theta value. (default: True)
underlying_price : boolean
    True to get the price of the underlying. (default: True)


Returns: Returns a dictionary with greeks as keys and greek values as values.
implied_volatility : float
    The implied volatility.
delta : float
    The option delta value.
option_price : float
    The option price.
pv_dividend : float
    The present value of dividends expected on the option's underlying.
gamma : float
    The option gamma value.
vega : float
    The option vega value.
theta : float
    The option theta value.
underlying_price : float     The price of the underlying.


**self.get_datetime()**
Return the current datetime localized the datasource timezone e.g. America/New_York. During backtesting this will be the time that the strategy thinks that it is.

Return type: datetime

print(f"The current time is {self.get_datetime()}")

**self.get_timestamp()**
Return the current UNIX timestamp. During backtesting this will be the UNIX timestamp that the strategy thinks that it is.

Return type: float

print(f"The current time is {self.get_timestamp()}")

**self.get_round_minute()**
Returns a minute rounded datetime object.

Optional Parameters:

timeshift (int): a timeshift in minutes from the present.
Example:

# Return a midnight rounded datetime object of three minutes ago 
dt =  self.get_round_minute(timeshift=3)
print(dt)
# datetime.datetime(2021, 2, 21, 9, 17, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
Return type: datetime

**self.get_last_minute()**
Returns the last minute rounded datetime object. Shortcut to straregy.get_round_minute(timeshift=1)

Return type datetime.

**self.get_round_day()**
Returns a day rounded datetime object.

Optional Parameters:

timeshift (int): a timeshift in days from the present.
Example:

# Return a midnight rounded datetime object of three days ago 
dt =  self.get_round_day(timeshift=3)
print(dt)
# datetime.datetime(2021, 2, 21, 0, 0, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
Return type datetime

**self.get_last_day()**
Returns the last day rounded datetime object. Shortcut to straregy.get_round_day(timeshift=1)

Return type datetime.

**self.get_datetime_range()**
Takes as input length, timestep and timeshift and returns a tuple of datetime representing the start date and end date.

Parameters:

length (int): represents the number of bars required
timestep (str): represents the timestep, either minute (default value) or day.
timeshift (timedelta): None by default. If specified indicates the time shift from the present.
Return type datetime

**self.localize_datetime()**
Converts an unaware datetime object (datetime object without a timezone) to an aware datetime object. The default timezone is America/New_York.

Parameter:

dt (datetime): the datetime object to convert.
Example:

from datetime import datetime
dt =  datetime(2021, 2, 21)
print(dt)
# datetime.datetime(2021, 2, 21, 0, 0)
dt_aware = self.localize_datetime(dt)
print(dt_aware)
# datetime.datetime(2021, 2, 21, 0, 0, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
Return type: datetime

**self.to_default_timezone()**

Transpose an aware datetime object to the default timezone America/New_York.

Parameter:

dt (datetime): the datetime object to convert.
Return type: datetime

**self.get_symbol_bars()**
Return bars for a given symbol.

Parameters:

symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or asset object.
length (int): The number of rows (number of timestamps)
timestep (str): Either "minute"" for minutes data or "day"" for days data default value depends on the data_source (minute for alpaca, day for yahoo, ...)
timeshift (timedelta): None by default. If specified indicates the time shift from the present.
Example:

from datetime import timedelta
#...

# Extract 10 rows of SPY data with one minute timestep between each row
# with the latest data being 24h ago (timedelta(days=1))
bars =  self.get_symbol_bars("SPY",10,"minute",timedelta(days=1))
Return type: bars

**self.get_bars()**

Return a dictionary of bars for a given list of symbols. Works the same as get_symbol_bars but take as first parameter a list of symbols.

Parameters:

symbol (list(str/asset)): The symbol string representation (e.g AAPL, GOOG, ...) or asset object.
length (int): The number of rows (number of timestamps)
timestep (str): Either "minute"" for minutes data or "day"" for days data default value depends on the data_source (minute for alpaca, day for yahoo, ...)
timeshift (timedelta): None by default. If specified indicates the time shift from the present.
Return type: dict of str/asset:bars

**self.start_realtime_bars()**

Starts a real time stream of tickers for Interactive Broker only.

This allows for real time data to stream to the strategy. Bars are fixed at every five seconds. They will arrive in the strategy in the form of a dataframe. The data returned will be:

datetime
open
high
low
close
volume
vwap
count (trade count)
Parameters:
asset : Asset object The asset to stream data.

keep_bars : int How many bars/rows to keep of data. If running for an extended period of time, it may be desirable to limit the size of the data kept.

Returns:
None

**self.get_realtime_bars()**

Retrieve the real time bars as dataframe.

Returns the current set of real time bars as a dataframe. The datetime will be in the index. Time intervals will be set at 5 secs. The columns of the dataframe are:

open
high
low
close
volume
vwap
count (trade count)
Parameters:
asset : Asset object The asset that has a stream active.

Returns:
dataframe : Pandas Dataframe.
Dataframe containing the most recent pricing information for the asset. The data returned will be the datetime in the index and the following columns.

open
high
low
close
volume
vwap
count (trade count)
The length of the dataframe will have been set the initial start of the real time bars.

**self.cancel_realtime_bars()**
Cancels a stream of real time bars for a given asset.

Cancels the real time bars for the given asset.

Parameters:
asset : Asset object
Asset object that has streaming data to cancel.

Returns:
None

**self.get_yesterday_dividend()**

Return dividend per share for the day before for a given symbol

Parameters:

symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or asset object.
Return type: float or asset object

**self.get_yesterday_dividends()** 

Return dividend per share for the day before for a given list of symbols. Works the same as get_yesterday_dividend but take as parameter a list of symbols.

Parameters:

symbol (str/asset): The symbol string representation (e.g AAPL, GOOG, ...) or asset object.
Return type: dict of str:float
