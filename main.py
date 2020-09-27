from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies.quick_momentum import QuickMomentum
from brokers.alpaca import Alpaca
from traders.trader import Trader

BROKER = 'alpaca'
API_KEY = "PK6PZU5KKBSKJZ2DXVEC"
API_SECRET = "DXEhZMDu37npGWE36Z6gizdTnHxxSKEe7MkQeVEo"
ENDPOINT = "https://paper-api.alpaca.markets"
USE_POLYGON = False

if __name__ == '__main__':
    budget = 40000
    broker = Alpaca(API_KEY, API_SECRET)
    trader = Trader(logfile='logs/test.log', debug=True)
    quick_momentum = QuickMomentum(budget=budget, broker=broker)
    trader.add_strategy(quick_momentum)
    trader.run_all()

    print("The end")
