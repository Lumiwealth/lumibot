from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Position
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class _NoopSplitStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        pass


def _strategy_with_position(quantity, avg_fill_price=100.0):
    start = pd.Timestamp("2025-01-02 16:00:00-05:00").to_pydatetime()
    end = pd.Timestamp("2025-01-07 16:00:00-05:00").to_pydatetime()
    data_source = PandasDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    broker._update_datetime(pd.Timestamp("2025-01-06 16:00:00-05:00").to_pydatetime())
    strategy = _NoopSplitStrategy(broker=broker, data_source=data_source, budget=1000.0)

    asset = Asset("SPLT", Asset.AssetType.STOCK)
    broker._filled_positions.append(
        Position(
            strategy=strategy.name,
            asset=asset,
            quantity=Decimal(str(quantity)),
            avg_fill_price=avg_fill_price,
        )
    )
    return strategy, broker, asset


@pytest.mark.parametrize(
    "ratio,quantity,expected_quantity,avg_fill_price,expected_avg_fill_price",
    [
        (2.0, 100, 200, 80.0, 40.0),
        (3.0, 100, 300, 90.0, 30.0),
        (7.0, 5, 35, 70.0, 10.0),
        (10.0, 8, 80, 100.0, 10.0),
        (0.5, 100, 50, 20.0, 40.0),
        (0.1, 100, 10, 10.0, 100.0),
        (2.0, -100, -200, 80.0, 40.0),
    ],
)
def test_update_positions_with_splits_adjusts_quantity_basis_and_records_event(
    monkeypatch,
    ratio,
    quantity,
    expected_quantity,
    avg_fill_price,
    expected_avg_fill_price,
):
    strategy, broker, asset = _strategy_with_position(quantity, avg_fill_price=avg_fill_price)
    monkeypatch.setattr(
        broker.data_source,
        "get_yesterday_stock_splits",
        lambda assets, quote=None: {asset: ratio},
    )

    revision_before = broker._filled_positions.revision

    applied = strategy._update_positions_with_splits()
    applied_again = strategy._update_positions_with_splits()

    position = broker.get_tracked_position(strategy.name, asset)
    assert applied == 1
    assert applied_again == 0
    assert position.quantity == pytest.approx(expected_quantity)
    assert position.avg_fill_price == pytest.approx(expected_avg_fill_price)
    assert broker._filled_positions.revision == revision_before + 1

    split_events = broker._trade_event_log_df.loc[
        broker._trade_event_log_df["event_kind"] == "corporate_action"
    ]
    assert len(split_events) == 1
    row = split_events.iloc[0]
    assert row["status"] == "stock_split"
    assert row["symbol"] == "SPLT"
    assert row["price"] == pytest.approx(ratio)
    assert row["filled_quantity"] == pytest.approx(expected_quantity)
    assert "quantity" in row["cash_event_description"]


@pytest.mark.parametrize("ratio", [0, 1, -2, float("nan"), float("inf"), None, "not-a-ratio"])
def test_update_positions_with_splits_ignores_non_action_ratios(monkeypatch, ratio):
    strategy, broker, asset = _strategy_with_position(100, avg_fill_price=25.0)
    monkeypatch.setattr(
        broker.data_source,
        "get_yesterday_stock_splits",
        lambda assets, quote=None: {asset: ratio},
    )

    applied = strategy._update_positions_with_splits()

    position = broker.get_tracked_position(strategy.name, asset)
    assert applied == 0
    assert position.quantity == pytest.approx(100)
    assert position.avg_fill_price == pytest.approx(25.0)
    assert broker._trade_event_log_df.empty


def test_update_positions_with_splits_skips_split_adjusted_data():
    strategy, broker, asset = _strategy_with_position(100, avg_fill_price=25.0)
    quote = Asset("USD", Asset.AssetType.FOREX)
    index = pd.DatetimeIndex(
        [
            "2025-01-03 16:00:00-05:00",
            "2025-01-06 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [50.0, 50.0],
            "high": [50.0, 50.0],
            "low": [50.0, 50.0],
            "close": [50.0, 50.0],
            "volume": [2000, 2000],
            "stock_splits": [0.0, 2.0],
            "_split_adjusted": [True, True],
        },
        index=index,
    )
    data = Data(asset, frame, timestep="day", quote=quote)
    broker.data_source.pandas_data = {(asset, quote): data}
    broker.data_source._data_store = {(asset, quote): data}

    applied = strategy._update_positions_with_splits()

    position = broker.get_tracked_position(strategy.name, asset)
    assert applied == 0
    assert position.quantity == pytest.approx(100)
    assert position.avg_fill_price == pytest.approx(25.0)
    assert broker._trade_event_log_df.empty


def test_update_positions_with_splits_skips_auto_adjusted_data_source(monkeypatch):
    strategy, broker, asset = _strategy_with_position(100, avg_fill_price=25.0)
    broker.data_source.AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES = True
    broker.data_source.auto_adjust = True
    monkeypatch.setattr(
        broker.data_source,
        "get_yesterday_stock_splits",
        lambda assets, quote=None: {asset: 2.0},
    )

    applied = strategy._update_positions_with_splits()

    position = broker.get_tracked_position(strategy.name, asset)
    assert applied == 0
    assert position.quantity == pytest.approx(100)
    assert position.avg_fill_price == pytest.approx(25.0)
    assert broker._trade_event_log_df.empty


def test_update_positions_with_splits_only_adjusts_stock_positions(monkeypatch):
    strategy, broker, stock = _strategy_with_position(100)
    option = Asset("SPLT", Asset.AssetType.OPTION, expiration=datetime(2025, 1, 17).date(), strike=50, right="CALL")
    broker._filled_positions.append(
        Position(strategy=strategy.name, asset=option, quantity=Decimal("1"), avg_fill_price=2.0)
    )
    monkeypatch.setattr(
        broker.data_source,
        "get_yesterday_stock_splits",
        lambda assets, quote=None: {stock: 2.0, option: 2.0},
    )

    applied = strategy._update_positions_with_splits()

    assert applied == 1
    assert broker.get_tracked_position(strategy.name, stock).quantity == pytest.approx(200)
    assert broker.get_tracked_position(strategy.name, option).quantity == pytest.approx(1)


def test_pandas_data_preserves_stock_splits_and_does_not_forward_fill_events():
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
            "2025-01-06 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 50.0, 50.0],
            "high": [100.0, 50.0, 50.0],
            "low": [100.0, 50.0, 50.0],
            "close": [100.0, 50.0, 50.0],
            "volume": [1000, 1000, 1000],
            "stock_splits": [2.0, None, 0.0],
        },
        index=index,
    )
    data = Data(asset, frame, timestep="day")
    data.repair_times_and_fill(index)
    assert data.df.loc[index[1], "stock_splits"] == 0

    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    data_source._update_datetime(index[-1].to_pydatetime())
    bars = data_source.get_historical_prices(asset, 3, timestep="day")

    assert "stock_splits" in bars.df.columns
    assert list(bars.df["stock_splits"]) == [2.0, 0.0, 0.0]


@pytest.mark.parametrize("ratio", [2.0, 3.0, 7.0, 10.0, 0.5, 0.1])
def test_data_source_reads_daily_stock_split_ratios(ratio):
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
            "2025-01-06 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 100.0 / ratio, 100.0 / ratio],
            "high": [100.0, 100.0 / ratio, 100.0 / ratio],
            "low": [100.0, 100.0 / ratio, 100.0 / ratio],
            "close": [100.0, 100.0 / ratio, 100.0 / ratio],
            "volume": [1000, 1000, 1000],
            "stock_splits": [0.0, ratio, 0.0],
        },
        index=index,
    )
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    data_source._update_datetime(index[1].to_pydatetime())

    splits = data_source.get_yesterday_stock_splits([asset])

    assert splits[asset] == pytest.approx(ratio)
    data_source._update_datetime(index[2].to_pydatetime())
    assert data_source.get_yesterday_stock_splits([asset])[asset] == 0


def test_data_source_reads_split_effective_before_daily_bar_timestamp():
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
            "2025-01-06 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 50.0, 50.0],
            "high": [100.0, 50.0, 50.0],
            "low": [100.0, 50.0, 50.0],
            "close": [100.0, 50.0, 50.0],
            "volume": [1000, 1000, 1000],
            "stock_splits": [0.0, 2.0, 0.0],
        },
        index=index,
    )
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    data_source._update_datetime(pd.Timestamp("2025-01-03 09:30:00-05:00").to_pydatetime())
    assert data_source.get_yesterday_stock_splits([asset])[asset] == pytest.approx(2.0)


@pytest.mark.parametrize(
    "ratio,previous_close,expected_preclose_mark,split_close",
    [
        (2.0, 100.0, 50.0, 48.0),
        (3.0, 90.0, 30.0, 29.0),
        (7.0, 70.0, 10.0, 9.5),
        (10.0, 100.0, 10.0, 9.0),
        (0.5, 20.0, 40.0, 42.0),
        (0.1, 10.0, 100.0, 98.0),
    ],
)
def test_daily_split_pre_close_mark_uses_split_adjusted_previous_close(
    ratio,
    previous_close,
    expected_preclose_mark,
    split_close,
):
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [previous_close, expected_preclose_mark],
            "high": [previous_close, max(expected_preclose_mark, split_close)],
            "low": [previous_close, min(expected_preclose_mark, split_close)],
            "close": [previous_close, split_close],
            "volume": [1000, 1000],
            "stock_splits": [0.0, ratio],
        },
        index=index,
    )
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    data_source._update_datetime(pd.Timestamp("2025-01-03 09:30:00-05:00").to_pydatetime())
    assert data_source.get_last_price(asset) == pytest.approx(expected_preclose_mark)

    data_source._update_datetime(index[1].to_pydatetime())
    assert data_source.get_last_price(asset) == pytest.approx(split_close)


def test_daily_split_pre_close_mark_does_not_adjust_already_split_adjusted_frame():
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [50.0, 50.0],
            "high": [50.0, 50.0],
            "low": [50.0, 50.0],
            "close": [50.0, 50.0],
            "volume": [2000, 2000],
            "stock_splits": [0.0, 2.0],
            "_split_adjusted": [True, True],
        },
        index=index,
    )
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    data_source._update_datetime(pd.Timestamp("2025-01-03 09:30:00-05:00").to_pydatetime())

    assert data_source.get_last_price(asset) == pytest.approx(50.0)


class _HoldThroughSplitStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.asset = self.parameters["asset"]
        self.observed_quantities = {}
        self.did_buy = False
        self.did_sell = False

    def on_trading_iteration(self):
        current_date = self.get_datetime().date()
        position = self.get_position(self.asset)
        self.observed_quantities[current_date] = 0 if position is None else float(position.quantity)

        if not self.did_buy and position is None:
            self.submit_order(self.create_order(self.asset, 10, "buy"))
            self.did_buy = True
        elif current_date >= datetime(2025, 1, 7).date() and position is not None and not self.did_sell:
            self.submit_order(self.create_order(self.asset, abs(position.quantity), "sell"))
            self.did_sell = True


def test_daily_backtest_holding_through_split_sells_adjusted_quantity():
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    index = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
            "2025-01-06 16:00:00-05:00",
            "2025-01-07 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 100.0, 50.0, 50.0],
            "high": [100.0, 100.0, 50.0, 50.0],
            "low": [100.0, 100.0, 50.0, 50.0],
            "close": [100.0, 100.0, 50.0, 50.0],
            "volume": [1000, 1000, 1000, 1000],
            "stock_splits": [0.0, 0.0, 2.0, 0.0],
        },
        index=index,
    )
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=index[0].to_pydatetime(),
        datetime_end=index[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _HoldThroughSplitStrategy(
        broker=broker,
        data_source=data_source,
        budget=2000.0,
        risk_free_rate=0,
        benchmark_asset=None,
        analyze_backtest=False,
        parameters={"asset": asset},
    )

    trader = Trader(backtest=True, logfile="")
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

    assert strategy.observed_quantities[datetime(2025, 1, 6).date()] == pytest.approx(20)

    trade_log = broker._trade_event_log_df
    split_events = trade_log.loc[trade_log["event_kind"] == "corporate_action"]
    assert len(split_events) == 1
    assert split_events.iloc[0]["price"] == pytest.approx(2.0)
    assert split_events.iloc[0]["filled_quantity"] == pytest.approx(20)

    fills = trade_log.loc[trade_log["status"] == "fill"]
    sell_fills = fills.loc[fills["side"].astype(str).str.lower().str.startswith("sell")]
    assert sell_fills.iloc[-1]["filled_quantity"] == pytest.approx(20)

    stats = strategy.stats
    split_day_value = stats.loc[pd.Timestamp("2025-01-06 16:00:00-05:00"), "portfolio_value"]
    assert split_day_value == pytest.approx(2000.0)
