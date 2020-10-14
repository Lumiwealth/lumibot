from threading import Thread
import sys, os, time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from strategies import Screener, Momentum, Diversification, IntradayMomentum
from brokers import Alpaca
from data_sources import AlpacaData
from traders import Trader

from credentials import AlpacaConfig

if __name__ == '__main__':
    budget = 40000
    broker = Alpaca(AlpacaConfig)
    pricing_data = AlpacaData(AlpacaConfig)
    trader = Trader(logfile='logs/test.log', debug=False)

    screener = Screener(budget=budget, broker=broker, pricing_data=pricing_data)
    trader.add_strategy(screener)

    # momentum = Momentum(budget=budget, broker=broker, pricing_data=pricing_data)
    # trader.add_strategy(momentum)

    # diversification = Diversification(budget=budget, broker=broker, pricing_data=pricing_data)
    # trader.add_strategy(diversification)

    # intraday_momentum = IntradayMomentum(budget=budget, broker=broker, pricing_data=pricing_data)
    # trader.add_strategy(intraday_momentum)

    trader.run_all()

    print("The end")
