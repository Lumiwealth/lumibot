from datetime import datetime

from credentials import InteractiveBrokersConfig
from lumibot.brokers import InteractiveBrokers
from lumibot.entities import Asset
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    DiversifiedLeverage,
    FastTrading,
    IntradayMomentum,
    Momentum,
    Simple,
    Strangle,
)
from lumibot.traders import Trader

logfile = "logs/test.log"
backtesting_start = datetime(2012, 1, 1)
backtesting_end = datetime(2021, 1, 1)
benchmark_asset = "SPY"

# Initialize all our classes
trader = Trader(logfile=logfile)
broker = InteractiveBrokers(InteractiveBrokersConfig)

####
# Select our strategy
####

assets = [Asset(symbol="AAPL")]

kwargs = {
    "assets": assets,
    "take_profit_threshold": 0.03,
    "stop_loss_threshold": -0.03,
    "sleeptime": 5,
    "total_trades": 0,
    "max_trades": 4,
    "max_days_expiry": 30,
    "days_to_earnings_min": 100,  # 15
}
strategy = Strangle(broker=broker, **kwargs)

####
# Run the strategy
####

trader.add_strategy(strategy)
trader.run_all()
