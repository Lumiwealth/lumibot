from lumibot.entities import Asset, Order
from lumibot.backtesting import CcxtBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.example_strategies.crypto_important_functions import ImportantFunctions
from datetime import datetime
from pandas import DataFrame

class CcxtBacktestingExampleStrategy(Strategy):
    def initialize(self, asset:tuple[Asset,Asset] = None,
                   cash_at_risk:float=.25,window:int=21):
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
        # BBL (Lower Bollinger Band): Can act as a support level based on price volatility, and can indicate an 'oversold' condition if the price falls below this line.
        # BBM (Breaking Bollinger Bands): This is essentially a moving average over a selected period of time, used as a reference point for price trends.
        # BBU (Upper Bollinger Band): Can act as a resistance level based on price volatility, and can indicate an 'overbought' condition if the price moves above this line.
        # BBB (Bollinger Band Width): Indicates the distance between the upper and lower bands, with a higher value indicating a more volatile market.
        # BBP (Bollinger Band Percentage): This shows where the current price is located within the Bollinger Bands as a percentage, where a value close to 0 means that the price is close to the lower band, and a value close to 1 means that the price is close to the upper band.
        # return bbands
        num_std_dev = 2.0
        close = 'close'

        df = DataFrame(index=history_df.index)
        df[close] = history_df[close]
        df['bbm'] = df[close].rolling(window=self.window).mean()
        df['bbu'] = df['bbm'] + df[close].rolling(window=self.window).std() * num_std_dev
        df['bbl'] = df['bbm'] - df[close].rolling(window=self.window).std() * num_std_dev
        df['bbb'] = (df['bbu'] - df['bbl']) / df['bbm']
        df['bbp'] = (df[close] - df['bbl']) / (df['bbu'] - df['bbl'])
        return df

    def on_trading_iteration(self):
        # During the backtest, we get the current time with self.get_datetime().
        # The time interval is self.sleeptime.
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


if  __name__ == "__main__":

    base_symbol = "ETH"
    quote_symbol = "USDT"
    start_date = datetime(2023,2,11)
    end_date = datetime(2024,2,12)
    asset = (Asset(symbol=base_symbol, asset_type="crypto"),
            Asset(symbol=quote_symbol, asset_type="crypto"))

    exchange_id = "kraken"  #"kucoin" #"bybit" #"okx" #"bitmex" # "binance"


    # CcxtBacktesting default data download limit is 50,000
    # If you want to change the maximum data download limit, you can do so by using 'max_data_download_limit'.
    kwargs = {
        # "max_data_download_limit":10000, # optional
        "exchange_id":exchange_id,
    }
    CcxtBacktesting.MIN_TIMESTEP = "day"
    results, strat_obj = CcxtBacktestingExampleStrategy.run_backtest(
        CcxtBacktesting,
        start_date,
        end_date,
        benchmark_asset=f"{base_symbol}/{quote_symbol}",
        quote_asset=Asset(symbol=quote_symbol, asset_type="crypto"),
        parameters={
                "asset":asset,
                "cash_at_risk":.25,
                "window":21,},
        **kwargs,
    )