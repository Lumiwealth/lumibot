from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies.quick_momentum import QuickMomentum
from brokers.alpaca import Alpaca
from traders.trader import Trader

BROKER = 'alpaca'
API_KEY = "PKIAJFZIW6EJU30ZQZE6"
API_SECRET = "LzfCO9Uwj6X4j2pYMSriZNoBI503bRGR8kyAMAQO"
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
