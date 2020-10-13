from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies import QuickMomentum, Momentum, Diversification, Demo
from brokers import Alpaca
from data_sources import AlpacaData
from traders import Trader

from credentials import AlpacaConfig

if __name__ == '__main__':
    budget = 40000
    broker = Alpaca(AlpacaConfig)
    pricing_data = AlpacaData(AlpacaConfig)
    trader = Trader(logfile='logs/test.log', debug=False)

    # quick_momentum = QuickMomentum(budget=budget, broker=broker)
    # trader.add_strategy(quick_momentum)

    # momentum = Momentum(budget=budget, broker=broker)
    # trader.add_strategy(momentum)

    diversification = Diversification(budget=budget, broker=broker, pricing_data=pricing_data)
    trader.add_strategy(diversification)

    # demo = Demo(budget=budget, broker=broker, pricing_data=pricing_data)
    # trader.add_strategy(demo)

    trader.run_all()

    print("The end")
