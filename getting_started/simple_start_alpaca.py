from datetime import datetime

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    DiversifiedLeverage,
    FastTrading,
    IntradayMomentum,
    Momentum,
    Simple,
)
from lumibot.traders import Trader

logfile = "logs/test.log"
backtesting_start = datetime(2012, 1, 1)
backtesting_end = datetime(2021, 1, 1)
benchmark_asset = "SPY"

# Initialize all our classes
trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

####
# Select our strategy
####

strategy = DiversifiedLeverage(broker=broker)

####
# Backtest
####
#
# strategy.backtest(
#     YahooDataBacktesting,
#     backtesting_start,
#     backtesting_end,
#     config=None,
# )

####
# Run the strategy
####

trader.add_strategy(strategy)
trader.run_all()
