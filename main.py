from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies import QuickMomentum
from brokers import Alpaca
from traders import Trader

from credentials import AlpacaConfig

if __name__ == '__main__':
    budget = 40000
    broker = Alpaca(AlpacaConfig.API_KEY, AlpacaConfig.API_SECRET)
    trader = Trader(logfile='logs/test.log', debug=True)
    quick_momentum = QuickMomentum(budget=budget, broker=broker)
    trader.add_strategy(quick_momentum)
    trader.run_all()

    print("The end")
