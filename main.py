from brokers import Alpaca
from backtesting import BacktestingBroker, YahooDataBacktesting
from credentials import AlpacaConfig
from data_sources import AlpacaData
from strategies import Diversification, IntradayMomentum, Momentum, Screener
from traders import Trader
from datetime import datetime

if __name__ == "__main__":
    debug = False
    budget = 40000
    broker = Alpaca(AlpacaConfig)
    pricing_data = AlpacaData(AlpacaConfig)
    yahoo_backtesting_source = YahooDataBacktesting(datetime(2010, 6, 1))
    backtesting_broker = BacktestingBroker(yahoo_backtesting_source)
    trader = Trader(logfile="logs/test.log", debug=debug)

    momentum = Momentum(budget=budget, broker=backtesting_broker)
    # momentum = Momentum(budget=budget, broker=broker)
    trader.add_strategy(momentum)

    # diversification = Diversification(budget=budget, broker=backtesting_broker)
    # diversification = Diversification(budget=budget, broker=broker)
    # trader.add_strategy(diversification)

    # intraday_momentum = IntradayMomentum(budget=budget, broker=backtesting_broker)
    # intraday_momentum = IntradayMomentum(budget=budget, broker=broker)
    # trader.add_strategy(intraday_momentum)

    # screener = Screener(budget=budget, broker=backtesting_broker)
    # screener = Screener(budget=budget, broker=broker)
    # trader.add_strategy(screener)

    trader.run_all()

    print("The end")
