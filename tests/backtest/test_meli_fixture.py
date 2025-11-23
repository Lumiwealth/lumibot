import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies.strategy import Strategy


class _MeliBuyCall(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.did_buy = False
        self.fills = []
        self.option_asset = Asset("MELI_CALL", Asset.AssetType.STOCK)

    def on_trading_iteration(self):
        if self.did_buy:
            return
        order = self.create_order(
            self.option_asset,
            1,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=self.get_last_price(self.option_asset),
        )
        self.submit_order(order)
        self.did_buy = True

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.fills.append(price)


def _build_data(entries: dict[tuple[Asset, Asset], Data], start, end) -> PandasDataBacktesting:
    ds = PandasDataBacktesting(
        pandas_data=entries,
        datetime_start=start,
        datetime_end=end,
        market="24/7",
        show_progress_bar=False,
        auto_adjust=True,
    )
    ds.load_data()
    return ds


def _make_df(closes, freq):
    index = pd.date_range("2024-01-02", periods=len(closes), freq=freq)
    return pd.DataFrame(
        {
            "open": closes,
            "high": [c + 1 for c in closes],
            "low": [c - 1 for c in closes],
            "close": closes,
            "volume": [1_000] * len(closes),
        },
        index=index,
    )


def test_meli_places_buy_on_available_option_bar():
    quote = Asset("USD", Asset.AssetType.FOREX)
    underlying = Asset("MELI", Asset.AssetType.STOCK)
    option = Asset("MELI_CALL", Asset.AssetType.STOCK)

    underlying_df = _make_df([1500, 1510, 1525], "1D")
    option_df = _make_df([12.5, 13.0, 13.5], "1D")

    data_entries = {
        (underlying, quote): Data(
            asset=underlying,
            df=underlying_df,
            quote=quote,
            timestep="day",
            timezone="America/New_York",
        ),
        (option, quote): Data(
            asset=option,
            df=option_df,
            quote=quote,
            timestep="day",
            timezone="America/New_York",
        ),
    }
    start = min(underlying_df.index[0], option_df.index[0])
    end = max(underlying_df.index[-1], option_df.index[-1]) + pd.Timedelta(days=1)

    ds = _build_data(data_entries, start, end)
    broker = BacktestingBroker(data_source=ds)
    broker.initialize_market_calendars(ds.get_trading_days_pandas())
    broker._first_iteration = False

    strat = _MeliBuyCall(
        broker=broker,
        budget=100000,
        quote_asset=quote,
        analyze_backtest=False,
        parameters={},
    )
    strat._first_iteration = False
    strat.did_buy = False
    strat.fills = []
    strat.option_asset = option

    strat.on_trading_iteration()
    broker.process_pending_orders(strat)
    strat._executor.process_queue()

    assert strat.did_buy is True
    assert len(strat.fills) == 1
    # Should fill using available option bar
    assert strat.fills[0] == option_df["close"].iloc[0]
