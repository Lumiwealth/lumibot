from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies import QuickMomentum, Momentum, Diversication, Demo
from brokers import Alpaca
from traders import Trader

from credentials import AlpacaConfig

if __name__ == '__main__':
    budget = 40000
    broker = Alpaca(AlpacaConfig)
    trader = Trader(logfile='logs/test.log', debug=False)

    quick_momentum = QuickMomentum(budget=budget, broker=broker)
    trader.add_strategy(quick_momentum)

    # momentum = Momentum(budget=budget, broker=broker)
    # trader.add_strategy(momentum)

    # diversication = Diversication(budget=budget, broker=broker)
    # trader.add_strategy(diversication)

    # demo = Demo(budget=budget, broker=broker)
    # trader.add_strategy(demo)

    trader.run_all()

    print("The end")
