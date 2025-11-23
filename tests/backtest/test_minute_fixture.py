import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies.strategy import Strategy


class _MinuteBuyOnce(Strategy):
    def initialize(self):
        self.sleeptime = "1M"
        self.did_buy = False
        self.fills = []

    def on_trading_iteration(self):
        if self.did_buy:
            return
        asset = Asset("PLTR", Asset.AssetType.STOCK)
        order = self.create_order(
            asset,
            1,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
        )
        self.submit_order(order)
        self.did_buy = True

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.fills.append(price)


def _build_minute_data(asset: Asset, quote: Asset, closes: list[float]) -> PandasDataBacktesting:
    index = pd.date_range("2024-01-02 09:30", periods=len(closes), freq="1min")
    df = pd.DataFrame(
        {
            "open": closes,
            "high": [c + 0.1 for c in closes],
            "low": [c - 0.1 for c in closes],
            "close": closes,
            "volume": [1_000] * len(closes),
        },
        index=index,
    )
    data = Data(
        asset=asset,
        df=df,
        quote=quote,
        timestep="minute",
        timezone="America/New_York",
    )
    pandas_data = {(asset, quote): data}
    ds = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=index[0],
        datetime_end=index[-1] + pd.Timedelta(minutes=1),
        market="24/7",
        show_progress_bar=False,
        auto_adjust=True,
    )
    ds.load_data()
    return ds


def test_minute_backtest_loads_and_fills():
    asset = Asset("PLTR", Asset.AssetType.STOCK)
    quote = Asset("USD", Asset.AssetType.FOREX)
    closes = [10.0, 10.1, 10.2, 10.3]
    ds = _build_minute_data(asset, quote, closes)
    broker = BacktestingBroker(data_source=ds)
    broker.initialize_market_calendars(ds.get_trading_days_pandas())
    broker._first_iteration = False

    strat = _MinuteBuyOnce(
        broker=broker,
        budget=10000,
        quote_asset=quote,
        analyze_backtest=False,
        parameters={},
    )
    strat._first_iteration = False
    strat.did_buy = False
    strat.fills = []

    strat.on_trading_iteration()
    broker.process_pending_orders(strat)
    strat._executor.process_queue()

    assert strat.did_buy is True
    assert len(strat.fills) == 1
    assert strat.fills[0] == closes[0]
