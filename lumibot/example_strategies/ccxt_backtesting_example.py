# ta-lib install : https://cloudstrata.io/install-ta-lib-on-ubuntu-server/
# pip install pandas-ta

from lumibot.brokers import Ccxt
from lumibot.entities import Asset, Order
from lumibot.backtesting import CcxtBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.example_strategies.crypto_important_functions import ImportantFunctions
from datetime import datetime
from pandas import DataFrame
import pandas_ta

# Save API_KEY, SECRET_KEY in the .env file and import it like below.

from dotenv import load_dotenv
import os

load_dotenv()


class MyStrategy(Strategy):
    def initialize(self, asset:tuple[Asset,Asset] = None,
                   cash_at_risk:float=.5,window:int=14):
        if asset is None:
            raise ValueError("You must provide a valid asset pair")
        # for crypto, market is 24/7
        self.set_market("24/7")
        self.sleeptime = "1D"
        self.asset = asset
        self.base, self.quote = asset
        self.window = window
        self.symbol = f"{self.base.symbol}/{self.quote.symbol}"
        self.last_trade = None
        self.order_quantity = 0.0
        self.cash_at_risk = cash_at_risk

    def _position_sizing(self):
        cash = self.get_cash()
        last_price = self.get_last_price(asset=self.asset,quote=self.quote)
        quantity = round(cash * self.cash_at_risk / last_price,0)
        return cash, last_price, quantity

    def _get_historical_prices(self):
        return self.get_historical_prices(asset=self.asset,length=None,
                                    timestep="day",quote=self.quote).df

    def _get_bbands(self,history_df:DataFrame):
        bbands = history_df.ta.bbands(close='close',length=self.window,std=2)
        # BBL (Lower Bollinger Band): Can act as a support level based on price volatility, and can indicate an 'oversold' condition if the price falls below this line.
        # BBM (Breaking Bollinger Bands): This is essentially a moving average over a selected period of time, used as a reference point for price trends.
        # BBU (Upper Bollinger Band): Can act as a resistance level based on price volatility, and can indicate an 'overbought' condition if the price moves above this line.
        # BBB (Bollinger Band Width): Indicates the distance between the upper and lower bands, with a higher value indicating a more volatile market.
        # BBP (Bollinger Band Percentage): This shows where the current price is located within the Bollinger Bands as a percentage, where a value close to 0 means that the price is close to the lower band, and a value close to 1 means that the price is close to the upper band.
        bbands.columns = ['bbl','bbm','bbu','bbb','bbp']
        return bbands

    def on_trading_iteration(self):
        # backtest 진행시에는 self.get_datetime()으로 현재 시간을 가져온다.
        # 시간 간격은 self.sleeptime 이다.
        current_dt = self.get_datetime()
        cash, last_price, quantity = self._position_sizing()
        history_df = self._get_historical_prices()
        bbands = self._get_bbands(history_df)
        prev_bbp = bbands[bbands.index < current_dt].tail(1).bbp.values[0]

        if prev_bbp < -0.13 and cash > 0 and self.last_trade != Order.OrderSide.BUY and quantity > 0.0:
            order = self.create_order(self.base,
                                    quantity,
                                    side = Order.OrderSide.BUY,
                                    type = Order.OrderType.MARKET,
                                    quote=self.quote)
            self.submit_order(order)
            self.last_trade = Order.OrderSide.BUY
            self.order_quantity = quantity
            self.log_message(f"Last buy trade was at {current_dt}")
        elif prev_bbp > 1.2 and self.last_trade != Order.OrderSide.SELL and self.order_quantity > 0.0:
            order = self.create_order(self.base,
                                    self.order_quantity,
                                    side = Order.OrderSide.SELL,
                                    type = Order.OrderType.MARKET,
                                    quote=self.quote)
            self.submit_order(order)
            self.last_trade = Order.OrderSide.SELL
            self.order_quantity = 0.0
            self.log_message(f"Last sell trade was at {current_dt}")

exchange_id = "binance" #"bitmex"

if exchange_id == "binance":
    CCXT_CONFIG = {
        "exchange_id": exchange_id,
        "apiKey": os.getenv("B_API_KEY"),
        "secret": os.getenv("B_SECRET_KEY"),
        "sandbox": True,
        'options': {
            'defaultType': 'spot', # 'margine' or 'spot'
        },
    }
elif exchange_id == "bitmex":
    CCXT_CONFIG = {
        "exchange_id": exchange_id,
        "apiKey": os.getenv("BITMEX_API_KEY"),
        "secret": os.getenv("BITMEX_SECRET_KEY"),
        "sandbox": True,
        'options': {
            'defaultType': 'spot', # 'margine' or 'spot'
        },
    }
else:
    raise ValueError("Invalid exchange_id")

base_symbol = "ETH"
quote_symbol = "USDT"
start_date = datetime(2023,1,1)
end_date = datetime(2024,2,12)
asset = (Asset(symbol=base_symbol, asset_type="crypto"),
         Asset(symbol=quote_symbol, asset_type="crypto"))

broker = Ccxt(CCXT_CONFIG)
strategy = MyStrategy(name='mystrategy',
                    broker=broker,
                    quote_asset=Asset(symbol=quote_symbol, asset_type="crypto"),
                    )

# BinanceDataBacktesting default data download limit is 50,000
# If you want to change the maximum data download limit, you can do so by using 'max_data_download_limit'.
kwargs = {
    "max_data_download_limit":10000,
    "exchange_id":exchange_id, # bitmex
}
CcxtBacktesting.MIN_TIMESTEP = "day"
strategy.backtest(
    CcxtBacktesting,
    start_date,
    end_date,
    benchmark_asset=f"{base_symbol}/{quote_symbol}",
    quote_asset=Asset(symbol=quote_symbol, asset_type="crypto"),
    parameters={
            "asset":asset,
            "cash_at_risk":.25,
            "window":21},
    **kwargs,
)
