# Interactive Brokers Options Tutorial

## Introduction
Trading options is more complicated than trading just stock strategies. This tutorial will walk 
through some of the elements of the `strangle` options strategy that is supplied as one of the 
example strategies in Lumibot.  

#### Nature of Options and Interactive Brokers
Trading options on Interactive Brokers presents some challenges. First, Interactive Brokers 
is an older system and has some idiosyncrasies to deal with. Second, options present some 
difficulties in algorithmic trading. The shear volume of possible combinations of 
expiration date, strike prices, multiplied by calls/strikes makes trading algorithmically difficult. 

It is also not unusual to find thinly traded stocks, or to receive no data back for a contract. 
Additionally, you could try to put on a pair trade and often have one side of the trade not fill 
properly. This results in a lot more double checking of your positions and information before, 
during, and after your trades. 

#### Strategy Description
- This is a strangle strategy that executes daily and resets. If you are actually trading this, 
  the profit would normally be set higher and traded interday. This example was structured to 
  trade daily for training purpose. 


## Initialize and Setup
#### Create an Option Asset
When trading options, Lumibot requires an asset object. Create an asset object for an option as 
follows:  
```
self.create_asset(
   "FB",
   asset_type="option",
   expiration="20210625",
   strike=335,
   right="CALL",
   multiplier=100,
)
```
- `asset_type`: There are only two asset types available in Lumibot. `stock` and `option`. Select `option` to 
create an option contract. 
- `expiration`: Expiration dates are in the format of `YYYYMMDD`
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

- `call` and `put` would be the actual asset objects for the options. 
- `expirations` would be the chain of maturity dates. 
- `strikes_lows` and `strikes_highs` are the strike prices moving low/high away from the current 
  underlying price. 
- `expiration_date` is the actual expiration used for this strategy for this stock. 
- `price_underlying` `price_call` `price_put` The prices are the current prices for the 
  underlying/call/put and can change over time. 
- `trade_create_date` records when the option trades were established. 
- `call_order` and `put_order` are the actual order objects. 

#### Getting Chains
Chains from Interactive Brokers return option information on each and every exchange and get can 
be time consuming to get. To get an option chain,
```python
self.get_chains(asset)
```
The return information is more than needed and it is desirable to a single exchange chain as 
follows: 
```python
self.get_chain(chains, self.exchange)
```
To obtain all of the expirations of an option chain: 
```python
self.get_expiration(chains, self.exchange)
```
To obtain all of the strike prices: 
```python

```


-
Exchanges
- Multipliers
- Expirations
- Strikes


## on_trading_iteration

#### Set initial iteration data
value = self.portfolio_value
cash = self.unspent_money
positions = self.get_tracked_positions()
filled_assets = [p.asset for p in positions]
trade_cash = self.portfolio_value / (self.max_trades * 2)

#### Always sell positions first. To free up cash and trading.
- check many status' 
- options in filled
- options status
- no price data?


- check for move signal.
- if signal, sell position, set status to 2/close

#### Buy next
- use of asset generator.
- check if symbol in active positions. 
- Try to get pricing, doesn't always return from IB, if not, just continue. Otherwise set prices in trading_pairs
- This trading strategy is designed to not be too close to earnings. Gather earnings data from 
  yahoo and check.
  
- Calculater trade quantity. Some large options amounts coupled with low budget and ability for 
  many orders can result in trade size < 1. Discard these stocks, set status to 2 so they won't trade. 
  
- Buy the positions.

