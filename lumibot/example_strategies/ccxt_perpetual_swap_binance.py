from lumibot.entities import Asset, Order
from lumibot.brokers.ccxtswap import CcxtSwap
from lumibot.strategies.strategy import Strategy
from lumibot.backtesting import CcxtBacktesting

from datetime import datetime


class CcxtPerpetualSwapBinance(Strategy):
    def initialize(self, parameters=None):
        pass

    def on_trading_iteration(self):
        pass


if __name__ == "__main__":
    base_symbol = "BTC"
    quote_symbol = "USDT"

    backtesting_start = datetime(2024, 1, 1)
    backtesting_end = datetime(2024, 3, 1)

    # See https://github.com/ccxt/ccxt/blob/00238dc3e7f6ffec0cabfbc033eaf1c78dfbaf22/examples/ccxt.pro/py/binance-futures.py#L15
    exchange_id = "binancecoinm"  # Binance COIN-M Perpetual Futures, supported by ccxt library

    BINANCE_CONFIG = {
        "exchange_id": exchange_id,
        "apiKey": "<api_key>",
        "secret": "<api_secret>",
        "sandbox": False,
    }

    broker = CcxtSwap(BINANCE_CONFIG)
    strategy = CcxtPerpetualSwapBinance(broker=broker)

    strategy.backtest(CcxtBacktesting,
                      backtesting_start,
                      backtesting_end,
                      benchmark_asset=Asset(symbol=base_symbol, asset_type="crypto"),
                      quote_asset=Asset(symbol=quote_symbol, asset_type="crypto"),
                      settlement_asset=Asset(symbol=quote_symbol, asset_type="crypto"))
