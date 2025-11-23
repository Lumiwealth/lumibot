import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies.strategy import Strategy


class _DailyBuyOnce(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.did_buy = False
        self.fills = []

    def on_trading_iteration(self):
        if self.did_buy:
            return
        asset = Asset("TQQQ", Asset.AssetType.STOCK)
        last_price = self.get_last_price(asset)
        assert last_price is not None
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


def _build_daily_data(asset: Asset, quote: Asset, closes: list[float]) -> PandasDataBacktesting:
    index = pd.date_range("2024-01-02", periods=len(closes), freq="1D")
    df = pd.DataFrame(
        {
            "open": closes,
            "high": [c + 1 for c in closes],
            "low": [c - 1 for c in closes],
            "close": closes,
            "volume": [1_000] * len(closes),
        },
        index=index,
    )
    data = Data(
        asset=asset,
        df=df,
        quote=quote,
        timestep="day",
        timezone="America/New_York",
    )
    pandas_data = {(asset, quote): data}
    ds = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=index[0],
        datetime_end=index[-1] + pd.Timedelta(days=1),
        market="24/7",
        show_progress_bar=False,
        auto_adjust=True,
    )
    ds.load_data()
    return ds


def test_tqqq_uses_daily_and_fills_with_latest_bar():
    asset = Asset("TQQQ", Asset.AssetType.STOCK)
    quote = Asset("USD", Asset.AssetType.FOREX)
    closes = [100.0, 101.5, 103.25]
    ds = _build_daily_data(asset, quote, closes)
    broker = BacktestingBroker(data_source=ds)
    broker.initialize_market_calendars(ds.get_trading_days_pandas())
    broker._first_iteration = False

    strat = _DailyBuyOnce(
        broker=broker,
        budget=1_000_000,
        quote_asset=quote,
        analyze_backtest=False,
        parameters={},
    )
    strat._first_iteration = False
    strat.did_buy = False
    strat.fills = []

    # Run a single iteration
    strat.on_trading_iteration()
    broker.process_pending_orders(strat)
    strat._executor.process_queue()

    assert strat.did_buy is True
    assert len(strat.fills) == 1
    # Should price using available daily bar
    assert strat.fills[0] == closes[0]
