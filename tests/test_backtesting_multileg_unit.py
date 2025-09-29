from datetime import timedelta
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


class _StubStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True

    def on_trading_iteration(self):
        return


def _make_df(start="2025-01-13 09:30", periods=3, freq="1min", price=50.0):
    index = pd.date_range(start, periods=periods, freq=freq, tz="America/New_York")
    return pd.DataFrame(
        {
            "open": [price for _ in range(periods)],
            "high": [price + 0.5 for _ in range(periods)],
            "low": [price - 0.5 for _ in range(periods)],
            "close": [price for _ in range(periods)],
            "volume": [1_000 for _ in range(periods)],
        },
        index=index,
    )


def _build_stock_strategy(price=50.0):
    asset = Asset("AAA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    df = _make_df(price=price)
    local_df = df.tz_convert("America/New_York").tz_localize(None)
    data = Data(asset=asset, df=local_df, quote=quote, timestep="minute", timezone="America/New_York")
    pandas_data = {(asset, quote): data}

    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=df.index[0],
        datetime_end=df.index[-1] + pd.Timedelta(minutes=1),
        show_progress_bar=False,
        market="24/7",
        auto_adjust=True,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _StubStrategy(broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})
    strategy._first_iteration = False

    return strategy, broker, asset, quote


def _make_child_order(strategy, asset, quote, quantity, side):
    return strategy.create_order(
        asset,
        Decimal(quantity),
        side,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )


def test_multileg_parent_fills_after_all_children_complete():
    strategy, broker, asset, quote = _build_stock_strategy()

    child_buy = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    child_sell = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.SELL)

    parent = Order(
        strategy=strategy,
        asset=asset,
        quantity=Decimal("20"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[child_buy, child_sell],
    )

    strategy.submit_order(parent)
    
    broker._execute_filled_order(child_buy, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()

    broker._execute_filled_order(child_sell, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert parent.is_filled()
    assert parent not in broker._new_orders.get_list()
    assert parent.quantity == Decimal("20")
    assert parent.avg_fill_price == pytest.approx(0.0, abs=1e-9)
    assert parent.trade_cost == pytest.approx(0.0, abs=1e-9)


def test_multileg_parent_waits_if_any_child_pending():
    strategy, broker, asset, quote = _build_stock_strategy()

    child_buy = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    child_sell = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.SELL)

    child_buy.status = Order.OrderStatus.FILLED
    child_buy.set_filled()
    # child_sell remains open

    parent = Order(
        strategy=strategy,
        asset=asset,
        quantity=Decimal("20"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[child_buy, child_sell],
    )

    strategy.submit_order(parent)

    dispatch_mock = Mock(wraps=broker.stream.dispatch)
    broker.stream.dispatch = dispatch_mock

    broker.process_pending_orders(strategy)

    dispatch_mock.assert_not_called()
    assert not parent.is_filled()


def test_multileg_child_fills_adjust_cash_once_and_parent_is_cash_neutral():
    strategy, broker, asset, quote = _build_stock_strategy(price=50.0)

    buy_child = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    sell_child = _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL)

    parent = Order(
        strategy=strategy,
        asset=asset,
        quantity=Decimal("15"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[buy_child, sell_child],
    )

    strategy.submit_order(parent)
    
    _submit = broker._execute_filled_order

    _submit(buy_child, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()

    broker._update_datetime(broker.datetime + timedelta(minutes=1))

    _submit(sell_child, price=55.0, filled_quantity=Decimal("5"), strategy=strategy)
    strategy._executor.process_queue()

    cash_after_children = strategy.cash

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    # Parent fill does not alter cash beyond child contributions.
    assert strategy.cash == pytest.approx(cash_after_children, rel=1e-9)

    # Expected cash: initial 100k - (10*50) + (5*55)
    expected_cash = 100_000.0 - 500.0 + 275.0
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)

    # Ensure both children are recorded as filled positions where appropriate.
    filled_positions = {pos.asset.symbol: float(pos.quantity) for pos in broker._filled_positions.get_list()}
    assert filled_positions.get(asset.symbol, 0.0) == pytest.approx(5.0)

    # Only the two child fills should be logged (parent is informational only).
    fills = strategy.broker._trade_event_log_df
    child_fills = fills[fills["status"] == "fill"]
    assert len(child_fills) == 2
    assert parent.trade_cost == pytest.approx(0.0, abs=1e-9)
