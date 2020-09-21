README
-------

# Bots

## BlueprintBot

All bots should inherit from the BlueprintBot class. The methods of this class can be split into several categories:
- Helper methods for interacting with alpaca REST API
- Events handling methods. These methods are executed when an event is trigered (Example: trade update)
- Lifecycle methods that are executed at different times during the execution of the bot.

### Lifecycle Methods

Custom bots should inherit from BlueprintBot and redefine lifecycle methods to perform trading 
actions then call the ```run()``` method.

```python
from BlueprintBot import BlueprintBot

class CustomBot(BlueprintBot):
    def on_market_open(self):
        """Perform some trading logic"""
        pass

myBot = CustomBot(API_KEY, API_SECRET, logfile='test.log')
myBot.run()
```

#### \_\_init\_\_

This is the constructor. It sets the logging and several other parameters and also connect to alpaca REST API.
Below a list of its parameters:

- ```api_key```: alpaca api_key
- ```api_secret```: alpaca secret variable
- ```api_base_url```. By default equal to "https://paper-api.alpaca.markets" for papertrading
- ```version```: Alpaca api version. By default 'v2'.
- ```logfile```: If defined, logging will be stored in that path. By default None.
- ```max_workers```: Number of max_workers used for multithreading tasks. By default set to 200 which is alpaca maximum requests per minute and per API key.
- ```chunk_size```: When requesting too many data from Alpaca REST API, Iterating over each item 
and sending requests item per item would be too slow and and requesting all bars at the same time 
won't always work as alpaca sets a maximum 100 symbols per request. Thus, inputs should be split 
into chunks with size ```chunk_size``` and data would be requested chunk by chunk before being merged.
By default equals to 100 which is maximum number of symbols by request allowed by alpaca.
- ```minutes_before_closing```: The lifecycle method on_market_open is executed inside a loop that stops only when there is only ```minutes_before_closing``` minutes remaining before market closes.
By default equals to 15 minutes
- ```sleeptime```: Sleeptime in minute after executing the lifecycle method on_market_open. By default equals to 1 minute
- ```debug```: Set to True to log ```logging.DEBUG``` level messages else log ```logging.INFO``` level messages. 

#### initialize

This is the first lifecycle method that the not would execute. Use this to set parameters for example or cancel orders from previous trading sessions.

#### before_market_opens

This lifecycle method is executed before the market opens. 
If the bot starts when the market is already open, this method won't be executed.

#### on_market_open

This lifecycle is executed inside an infinite loop when the market is open and is not within ```minutes_before_closing``` minutes of closing.
After each execution, the bot would sleep ```sleeptime```  minutes before executing the ```on_market_open``` again.
Use this lifecycle method for building trading iterations.

#### before_market_closes

This lifecycle method is executed when the market is still open and there is only ```minutes_before_closing``` before closing.

#### after_market_closes

This lifecycle method is executed after the market close. 
It can be used for generating statistics for example.

#### on_bot_crash

This lifecycle method is executed when an unhandled error occurs.

#### run

After instantiating the Bot class, call this method to execute the lifcycle methods.

### Helper methods

This methods are meant essentially for interacting with Alpaca REST API and are used by the lifecycle methods and events to extract data and perform trading actions

#### get_positions

List the account current positions

#### get_open_orders

Get a list of the account open orders

#### cancel_buying_orders

Cancel all open orders

#### get_ongoing_assets

Get a list of symbols of the open orders and current positions

#### is_market_open

return True if the market is open else False

#### get_time_to_open

Get the time (in seconds) till the start of the next trading session

#### get_time_to_close

Get the time (in seconds) till the closing of the next trading session

#### await_market_to_close

Sleeps until the current market closes

#### await_market_to_open

Sleeps until the next trading sessions starts

#### get_account

Get the current account properties

#### get_tradable_assets

Get a list of all tradable assets from alpaca

#### get_last_price

Takes a symbol as input and returns the last known price.

#### get_chunks

Takes a list (of symbols) and splits it into a list of chunks. The chunk size is by default the ```chunk_size``` parameter passed in the constructor 
but can also be manually set in the function call.

#### get_bars

Takes as input a list of symbols, a time_unity parameter and a length parameter and returns a list of barset
with the corresponding symbols, timeunity and length. 

#### submit_order

Takes as argument a symbol, a quantity and an operation side (buy/sell) and submit an order.

By default submit a "market" order unless a ```limit_price``` parameter was set. 
In this case a limit order is submited with the given ```limit_price```.

If a ```stop_price``` parameter is defined then submit a "Stop Order" with that price. 

#### submit_orders

Takes as input a list of orders and call the ```submit_order``` method for each one using a Thread pool.

#### sell_all

sell all the account positions and by default cancel all open orders unless ```cancel_open_orders```
parameter was set to ```False```.

### Events

Events are asynchronous functions triggered after receiving data from Alpaca socket streams. 
These events will be primarily used for updating the bot internal values like:
- open orders
- positons
- last know prices

but can be used to build chain actions.

#### set_streams

This method defines asynchronous functions that will be executed each time an event is trigered by 
alpaca socket stream before connecting to the stream in a different thread.

```set_streams()``` defines the following asynchronous methods

-  ```default_on_trade_event```: is executed when a trade event is trigered. This functions calls two
static methods ```@static```:
    - ```log_trade_event```: This method handles logging the trading event and should not be overloaded
    - ```on_trade_event```: This method is executed after the logging method and is meant to be overloaded
    in order to execute trading logic after a trade event is trigered
    
#### log_trade_event (static method)

Takes a data object as parameter corresponding to the object sent by alpaca socket stream.
In this case, data corresponds to a trading event. ```log_trade_event``` will log that event

#### on_trade_event (static method)

Takes a data object as parameter corresponding to the object sent by alpaca socket stream.
In this case, data corresponds to a trading event. Chain actions to be defined after an event is trigered
should be based on that data object.

## MomentumBot

There isn't a clearly defined risk management component yet. 
This current bot uses a maximum budget of 40.000$ and spend at maximum 4.000$ per asset so in maximum, there would be 10 positions (40000/4000 = 10).
The bot iterates over all assets and buy if an asset did have a 2% increase over the last 24h at that moment.
The quantity is the budget per asset (4000$) divided by the last price (obtained via the API barsets).
The order is a stop loss order with a maximum 4% decrease in value.
