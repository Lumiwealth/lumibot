# Interactive Brokers Options Tutorial

## Introduction
Trading options is more complicated than trading stock strategies. This tutorial will walk 
through elements of the `strangle` options strategy that is supplied as one of the 
example strategies in Lumibot.  The `strangle` module can be located in `strategies/examples`.

#### Nature of Options and Interactive Brokers
Trading options on Interactive Brokers presents some challenges. First, Interactive Brokers 
is an older system and has some idiosyncrasies to deal with. Second, options present some 
difficulties in algorithmic trading. The shear volume of possible combinations of 
expiration dates, strike prices, multiplied by calls/strikes makes trading options algorithmically 
difficult. 

It is also not unusual to find thinly traded options, or to receive no data back for a contract. 
Additionally, you could try to put on a pair trade and often have one side of the trade not fill 
properly, leaving your position unhedged. This results in a lot more double-checking of your 
positions 
and 
information before, 
during, and after your trades. 

#### Strategy Description
This is a `Strangle` strategy that executes daily and resets. If you are actually trading this 
strategy, the `take_profit_threshold` would normally be set higher and trading frequency 
would be inter-day. This example was 
structured to trade intra-day for training purpose. 
> From [Investopedia](https://www.investopedia.com/terms/s/strangle.asp): A strangle is an 
> options strategy in which the investor holds a position in both a call and a put option with 
> different strike prices, but with the same expiration date and underlying asset. A strangle is a 
> good strategy if you think the underlying security will experience a large price movement in the 
> near future but are unsure of the direction. However, it is profitable mainly if the asset does 
> swing sharply in price.

## Initialize and Setup
#### Create an Option Asset
When trading options, Lumibot requires an asset object. Create an asset object for an option as 
follows:  
```
self.create_asset(
   "FB",
   asset_type="option",
   name="Facebook, Inc.",
   expiration=datetime.date(2021, 9, 17),
   strike=335,
   right="CALL",
   multiplier=100,
)
```
- `symbol`: Tradeable symbol for the underlying security. 
- `asset_type`: There are only two asset types available in Lumibot. `stock` and `option`. Select `option` to 
create an option contract.
- `name`: Optional full name. Not used in the code but can be used for prints and logging. 
- `expiration`: Expiration dates are in the format of datetime.date().
- `strike`: is an integer representing the contract strike price. 
- `right`: Two options, `CALL` or `PUT`
- `multiplier`: An integer representing the multiple the option contract is multiplied by. Most 
  commonly `100`.
  
#### Tracking Information
Trading options requires tracking information. At a minimum, you will likely track the 
information of the underlying security with the option itself. More commonly, you will trade 
pairs or four legs in a strategy. This requires coordination. 

Using a dictionary you create for each strategy will help with this. The dictionary used for the 
`strangle` strategy helps to track the information for that strategy. It should be emphasized 
that each strategy will have its own information needs. The 'strangle' dictionary is just an 
example of how this can be used.

We will need a key for the dictionary. Fortunately, each underlying stock in the form of an 
asset object makes a great key. So using "FB" as example, the key for all the "FB" options data 
would be: 
```python
self.create_asset("FB", asset_type="stock")
```
We can have as many stocks as we want of course. Here is the function that creates the `strangle` dictionary. 
```python
 def create_trading_pair(self, symbol):
      # Add/update trading pair to self.trading_pairs
      self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
          "call": None,
          "put": None,
          "expirations": None,
          "strike_lows": None,
          "strike_highs": None,
          "buy_call_strike": None,
          "buy_put_strike": None,
          "expiration_date": None,
          "price_underlying": None,
          "price_call": None,
          "price_put": None,
          "trade_created_time": None,
          "call_order": None,
          "put_order": None,
          "status": 0,
      }
```
Here we would call the method above with a stock symbol to create a new addition to the 
dictionary with these default values.  

- `call` and `put` would be the actual option asset objects. 
- `expirations` would be the chain of maturity dates. 
- `strikes_lows` and `strikes_highs` are the strike prices moving low/high away from the current 
  underlying price. 
- `expiration_date` is the actual expiration used for this strategy for this stock. 
- `price_underlying` `price_call` `price_put` The prices are the current prices for the 
  underlying/call/put and can change over time. 
- `trade_create_date` records when the option trades were established. 
- `call_order` and `put_order` are the actual order objects. 

To create the dictionary for multiple symbols, just loop though all the symbols being used. 
```python
self.symbols_universe = [
            "AAL",
            "AAPL",
            "AMD",
            "AMZN",
            "BAC",
            "DIS",
            "EEM",
            "FB",
            "FXI",
            "MSFT",
            "TSLA",
            "UBER",
        ]

# Underlying Asset Objects.
self.trading_pairs = dict()
for symbol in self.symbols_universe:
    self.create_trading_pair(symbol)
```
Now you are ready to store and retrieve information for each asset while trading. 

#### Getting Chains
Option chains from Interactive Brokers returns option information every exchange. Downloading full 
chains is a slow part of the process. To get an option chain, use a stock asset as follows:
```python
self.get_chains(asset)
```
The return information contains the options data from every exchange and is more inforamation than 
needed. Reduce this information to a selected exchange as follows: 
```python
self.get_chain(chains, exchange="SMART")
```
There are many exchanges that options trade on, 'CBOE' being the main one. Interactive Brokers 
uses a `SMART` routing system to seek out the most appropriate exchange for the underlying asset.
`SMART` is the default setting for `exchange` and should normally work.  

#### Expiration Dates
To obtain all of the expirations of an option chain: 
```python
self.get_expiration(chains, self.exchange)
```
#### Strikes
The chain has strike information in it, but these are the strikes for all expiration dates. 
These are not necessarily the same for every expiration date. In order to obtain strikes for a 
particular expiration date, we must: 
1. Create an option asset but without the strike data. 
2. Retrieve the strike information using that option asset. 
```python
asset = self.create_asset(
            "FB",
            asset_type="option",
            expiration=datetime.date(2021, 6, 25),
        )
self.get_strikes(asset)
```

#### Multipliers
Each option chain contains the `multiplier` as a dictionary item. You can retrieve the 
multiplier using the method: 
```python
self.get_multiplier(chains, exchange=`SMART`)
* exchange defaults to `SMART`
```
#### Creating the Option Asset
With the above information in hand, and after applying the logic of your individual strategy, you 
can create an option for trading.    
```python
self.create_asset(
    "FB", # or self.create_asset("FB")
    asset_type="option",
    expiration=datetime.date(2021, 6, 25),
    strike=330,
    right="CALL",
    multiplier=100,
)
```
This asset should be stored in your tracking dictionary. 

## on_trading_iteration

#### Cash Management
Cash management is an important topic. Because options can be slow to fill, or not fill at all, 
it is important to understand cash management. Every time an order is submitted, there is an 
expectation of receiving or using cash. However, actual cash is not attributed to Lumibot until 
the order is fully filled. 

This is because the final details of filling the order are not known until after the order fills. 
It is at that time an accurate accounting for cash can take place. 

Due to the fact that options can take some time to fill, an extended period of cash uncertainty 
can arise. 

Generally speaking, you will want to start each `on_trading_iteration` lifecycle with no 
outstanding orders. Doing so you will make certain your cash value is up to date. If you do not 
make sure orders are filled at the beginning of the `on_trading_iteration`, you must take 
into account the possible cash outcomes of unfilled orders when processing the next iteration. 

In the strangle example, `cash`, `value` and `positions` are set. These will be modified as 
orders are issued throughout the `on_trading_iteration` lifecylce. 
```
value = self.portfolio_value
cash = self.cash
positions = self.get_tracked_positions()
```
`filled_asset` is a list assets for active positions. This allows for easy checking to verify if 
the calls/puts are already traded. 
```python
filled_assets = [p.asset for p in positions]
# then...
if options["call"] not in filled_assets and options["put"] not in filled_assets:
    continue
```

#### Always sell positions first. To free up cash and trading.
When working through your trading logic, try to sell any positions first then make purchases. 
This will assist cash flow. If you are trading near the maximum cash of your account, you may 
wish to wait for the selling trade to fill before make any purchases. This will avoid your 
trades being rejected for lack of funds. 
```python
# Single order
wait_for_order_execution(order)
# Multiple orders
wait_for_orders_execution(orders)
```

#### Placing Orders
Order can be placed in one statement as follow: 
```python
self.submit_order(
    self.create_order(
        option_asset,
        quantity,
        "buy",
        exchange="SMART",
    )
)
```
Or the order can be split out if saving the order in the tracking dictionary.

The full example can be followed through in the `strangle.py` located in the 
`strategies/examples` directory.

