import os
import sys
import time
from threading import Thread

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from brokers import Alpaca
from credentials import AlpacaConfig
from data_sources import AlpacaData
from strategies import Diversification, IntradayMomentum, Momentum, Screener
from traders import Trader

if __name__ == "__main__":
    budget = 40000
    broker = Alpaca(AlpacaConfig)
    pricing_data = AlpacaData(AlpacaConfig)
    trader = Trader(logfile="logs/test.log", debug=False)

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
