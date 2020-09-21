"""Testing connection streams"""

import sys, os
import alpaca_trade_api as tradeapi
import time
from alpaca_trade_api.common import URL

API_KEY = "PKMWTN5BTI83B117YE5C"
API_SECRET = "qMzfeBp51dYIJDmqAizEbwAp3Qz6tHZFOUnqNLl9"
ENDPOINT = "https://paper-api.alpaca.markets"
USE_POLYGON = False

conn = tradeapi.StreamConn(
    API_KEY,
    API_SECRET,
    base_url=URL('https://paper-api.alpaca.markets'),
    data_url=URL('https://data.alpaca.markets'),
    data_stream='polygon' if USE_POLYGON else 'alpacadatav1'
)

#===========Working=================================

os.remove('logs/trade_updates.log')

@conn.on(r'^trade_updates$')
async def on_account_updates(conn, channel, account):
    with open('logs/trade_updates.log', 'a+') as f:
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(account))
        f.write("\n" + "=" * 100 + "\n")

#==========Testing=================================
@conn.on(r'T\..+')
async def on_transaction(conn, channel, data):
    with open('logs/transaction.log', 'a+') as f:
        print('bars', data)
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(bar))
        f.write("\n" + "=" * 100 + "\n")

@conn.on(r'Q\..+', ['AAPL'])
async def on_quote(conn, channel, data):
    with open('logs/quote.log', 'a+') as f:
        print('bars', data)
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(bar))
        f.write("\n" + "=" * 100 + "\n")

@conn.on(r'^AM\..+$')
async def on_minute_bars(conn, channel, bar):
    with open('logs/minute_bars.log', 'a+') as f:
        print('bars', bar)
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(bar))
        f.write("\n" + "=" * 100 + "\n")

@conn.on(r'^A$')
async def on_second_bars(conn, channel, bar):
    with open('logs/bars.log', 'a+') as f:
        print('bars', bar)
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(bar))
        f.write("\n" + "=" * 100 + "\n")


#@conn.on(r'^AM\..+$')
#@conn.on(r'Q\..+', ['AAPL'])
#@conn.on(r'T\..+', ['AAPL'])

# blocks forever
conn.run(['trade_updates', 'alpacadatav1/AM.*', 'alpacadatav1/Q.*', 'alpacadatav1/T.*'])
