from main import API_KEY, API_SECRET, ENDPOINT, USE_POLYGON

import sys, os, time
import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

conn = tradeapi.StreamConn(
    API_KEY,
    API_SECRET,
    base_url=URL('https://paper-api.alpaca.markets'),
    data_url=URL('https://data.alpaca.markets'),
    data_stream='polygon' if USE_POLYGON else 'alpacadatav1'
)

if os.path.exists('logs/trade_updates.log'):
    os.remove('logs/trade_updates.log')

# ===========Working Stream channels=================================
@conn.on(r'^trade_updates$')
async def on_account_updates(conn, channel, account):
    with open('logs/trade_updates.log', 'a+') as f:
        f.write("\n" + "=" * 100 + "\n")
        f.write(str(account))
        f.write("\n" + "=" * 100 + "\n")

# ==========Testing Stream channels=================================
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

# ==========Add Subscribers=======================================
conn.run(['trade_updates', 'alpacadatav1/AM.*', 'alpacadatav1/Q.*', 'alpacadatav1/T.*'])
