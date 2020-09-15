README
-------

# Bots

## BlueprintBot

All bots should inherit from the BlueprintBot class. The methods of this class can be split in two categories:
- Helper methods for interacting with alpaca REST API
- Lifecycle methods that are executed at different times during the execution of the bot.

### Lifecycle Methods

#### \_\_init\_\_
This is the constructor. It sets the logging and several other parameters and also connect to alpaca REST API.
Below a list of its parameters:
- api_key: alpaca api_key
- api_secret: alpaca secret variable
- api_base_url. By default equal to "https://paper-api.alpaca.markets" for papertrading
- version: Alpaca api version. By default 'v2'.
- logfile: If defined, logging will be stored in that path. By default None.
- max_workers: Number of max_workers used for multithreading tasks. By default set to 200 which is alpaca maximum requests per minute and per API key.
- chunk_size: When requesting too many data from Alpaca REST API, Iterating over each item and sending requests item per item would be too slow.
Requesting bars data for over 10.000 assets at the same time would generate a very long post request url, exceeding the limit. 
In this case, inputs should be split into chunks with size chunk_size and data would be requested chunk by chunk.
By default equals to 100 which is maximum number of symbols by request allowed by alpaca.
- minutes_before_closing: The lifecycle method on_market_open is executed inside a loop that stops only when there is only ```minutes_before_closing``` minutes remaining before market closes.
By default equals to 15 minutes
- sleeptime: Sleeptime in minute after executing the lifecycle method on_market_open. By default equals to 1 minute

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

#### run

After instantiating the Bot class, call this method to execute the lifcycle methods.

### Helper methods

This methods are meant essentially for interacting with Alpaca REST API and are used by the lifecycle methods and events to extract data and perform trading actions

### Events

Events are asynchronous functions triggered after receiving data from Alpaca socket streams. 
These events will be primarily used for updating the bot internal values like:
- open orders
- positons
- last know prices

but can be used to build chain actions.

Not implemented yet.

### Inheritance

Custom bots should inherit from BlueprintBot and redefine lifecycle methods to perform trading actions then call the ```run()``` method.

```python
from BlueprintBot import BlueprintBot

class CustomBot(BlueprintBot):
    def on_market_open(self):
        """Perform some trading logic"""
        pass

myBot = CustomBot(API_KEY, API_SECRET, logfile='test.log')
myBot.run()
```


## MomentumBot

There isn't a clearly defined risk management component yet. 
This current bot uses a maximum budget of 40.000$ and spend at maximum 4.000$ per asset so in maximum, there would be 10 positions (40000/4000 = 10).
The bot iterates over all assets and buy if an asset did have a 2% increase over the last 24h at that moment.
The quantity is the budget per asset (4000$) divided by the last price (obtained via the API barsets).
The order is a stop loss order with a maximum 4% decrease in value.
