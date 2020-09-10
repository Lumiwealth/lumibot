README
-------

# Bots

## MomentumBot

There isn't a clearly defined risk management component yet. 
This current bot uses a maximum budget of 40.000$ and spend at maximum 4.000$ per asset so in maximum, there would be 10 positions (40000/4000 = 10).
The bot iterates over all assets and buy if an asset did have a 2% increase over the last 24h at that moment.
The quantity is the budget per asset (4000$) divided by the last price (obtained via the API barsets.
The order is a stop loss order. The limit is a maximum 4% decrease in value.

# Issues

## No Socket Stream at the moment

The bot is not using a socket stream at the moment. This is problematic. 
There are more than 10k assets. 
Requesting values for all these assets in order to know which asset to buy or not pose the following problems:
    - iterating over each one and sending HTTP requests to get data results in reaching Alpaca requests limit (200 requests per minute)
    - requesting data for all assets at once will result in exceeding the maximum length of a Url request.
    
Implementing Socket Streams needs to be implemented ASAP
    
## Bars objects can be empty

Some assets are market are tradable but when requesting a barset in order to get last price or compute variations, an empty list is returned.
What does that mean? Did notice that for one asset example, Boolean value 'shortable' was False 
