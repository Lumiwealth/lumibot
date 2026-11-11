from datetime import timedelta
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Position, SmartLimitConfig, SmartLimitPreset
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
    # Ten 1:1 packages preserve both requested ten-unit child fills.
    assert parent.quantity == Decimal("10")
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


def test_canceled_incomplete_package_does_not_claim_a_complete_parent_fill():
    strategy, broker, asset, quote = _build_stock_strategy(price=50.0)
    buy_child = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    sell_child = _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL)
    parent = broker._submit_orders([buy_child, sell_child], is_multileg=True)[0]
    broker._execute_filled_order(buy_child, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()
    broker.cancel_order(sell_child)
    strategy._executor.process_queue()
    assert not parent.is_filled()
    assert parent.is_canceled()
    assert buy_child.is_filled()
    assert sell_child.is_canceled()
    assert strategy.cash == pytest.approx(99_500.0)
    assert any(position.asset == asset and position.quantity == 10
               for position in broker._filled_positions.get_list())


@pytest.mark.parametrize("expire_day_orders", [False, True])
def test_child_cancellation_reconciles_package_parent(expire_day_orders):
    strategy, broker, asset, quote = _build_stock_strategy()
    children = [
        _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY),
        _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL),
    ]
    for child in children:
        child.time_in_force = "day"
    parent = broker._submit_orders(children, is_multileg=True)[0]
    if expire_day_orders:
        broker.data_source._update_datetime(pd.Timestamp("2025-01-14 09:30", tz="America/New_York"))
    for child in children:
        if expire_day_orders:
            assert broker._cancel_order_if_time_in_force_elapsed(child)
        else:
            broker.cancel_order(child)
    strategy._executor.process_queue()
    assert all(child.is_canceled() for child in children)
    assert parent.is_canceled()
    assert not parent.is_active()
    assert not parent.is_filled()
    assert strategy.cash == pytest.approx(100_000)


def test_canceling_one_child_preserves_working_package_sibling():
    strategy, broker, asset, quote = _build_stock_strategy()
    children = [
        _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY),
        _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL),
    ]
    parent = broker._submit_orders(children, is_multileg=True)[0]
    broker.cancel_order(children[0])
    strategy._executor.process_queue()
    assert parent.is_active()
    assert children[1].is_active()


def test_canceling_package_parent_records_each_cancellation_once():
    strategy, broker, asset, quote = _build_stock_strategy()
    children = [
        _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY),
        _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL),
    ]
    parent = broker._submit_orders(children, is_multileg=True)[0]
    broker.cancel_order(parent)
    strategy._executor.process_queue()
    canceled = broker._canceled_orders.get_list()
    for order in [parent, *children]:
        assert order.is_canceled()
        assert sum(item.identifier == order.identifier for item in canceled) == 1


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
    # Five 2:1 packages: 2*50 - 55 = 45 per package, not 50 - 55.
    assert parent.quantity == Decimal("5")
    assert parent.get_fill_price() == pytest.approx(45.0)

    # Ensure both children are recorded as filled positions where appropriate.
    filled_positions = {pos.asset.symbol: float(pos.quantity) for pos in broker._filled_positions.get_list()}
    assert filled_positions.get(asset.symbol, 0.0) == pytest.approx(5.0)

    # Only the two child fills should be logged (parent is informational only).
    fills = strategy.broker._trade_event_log_df
    child_fills = fills[fills["status"] == "fill"]
    assert len(child_fills) == 2
    assert parent.trade_cost == pytest.approx(0.0, abs=1e-9)


def test_multileg_parent_fill_price_uses_net_child_prices():
    strategy, broker, asset, quote = _build_stock_strategy(price=50.0)

    buy_child = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    sell_child = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.SELL)

    parent = Order(
        strategy=strategy,
        asset=asset,
        quantity=Decimal("20"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[buy_child, sell_child],
    )

    strategy.submit_order(parent)

    broker._execute_filled_order(buy_child, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()

    broker._execute_filled_order(sell_child, price=60.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert parent.is_filled()
    assert parent.avg_fill_price == pytest.approx(-10.0, abs=1e-9)
    assert parent.trade_cost == pytest.approx(0.0, abs=1e-9)


def test_multileg_parent_limit_order_still_fills_after_children():
    strategy, broker, asset, quote = _build_stock_strategy(price=50.0)

    buy_child = _make_child_order(strategy, asset, quote, "5", Order.OrderSide.BUY)
    sell_child = _make_child_order(strategy, asset, quote, "5", Order.OrderSide.SELL)

    parent = Order(
        strategy=strategy,
        asset=asset,
        quantity=Decimal("10"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=49.0,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[buy_child, sell_child],
    )

    strategy.submit_order(parent)

    broker._execute_filled_order(buy_child, price=50.0, filled_quantity=Decimal("5"), strategy=strategy)
    strategy._executor.process_queue()

    broker._execute_filled_order(sell_child, price=50.0, filled_quantity=Decimal("5"), strategy=strategy)
    strategy._executor.process_queue()

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert parent.is_filled()
    assert parent.trade_cost == pytest.approx(0.0, abs=1e-9)


def test_multileg_smart_limit_net_fill_price():
    asset_a = Asset("AAA", asset_type=Asset.AssetType.STOCK)
    asset_b = Asset("BBB", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    df_a = _make_df(price=100.0)
    df_a["bid"] = [99.0 for _ in range(len(df_a))]
    df_a["ask"] = [101.0 for _ in range(len(df_a))]
    df_b = _make_df(price=20.0)
    df_b["bid"] = [19.0 for _ in range(len(df_b))]
    df_b["ask"] = [21.0 for _ in range(len(df_b))]

    data_a = Data(asset=asset_a, df=df_a.tz_convert("America/New_York").tz_localize(None), quote=quote, timestep="minute", timezone="America/New_York")
    data_b = Data(asset=asset_b, df=df_b.tz_convert("America/New_York").tz_localize(None), quote=quote, timestep="minute", timezone="America/New_York")
    pandas_data = {(asset_a, quote): data_a, (asset_b, quote): data_b}

    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=df_a.index[0],
        datetime_end=df_a.index[-1] + pd.Timedelta(minutes=1),
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

    config = SmartLimitConfig(preset=SmartLimitPreset.FAST, slippage=0.1)
    buy_leg = strategy.create_order(asset_a, Decimal("1"), "buy", order_type=Order.OrderType.MARKET)
    sell_leg = strategy.create_order(asset_b, Decimal("1"), "sell", order_type=Order.OrderType.MARKET)

    parent = Order(
        strategy=strategy.name,
        asset=asset_a,
        quantity=Decimal("2"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[buy_leg, sell_leg],
        smart_limit=config,
    )

    strategy.submit_order(parent)
    parent._date_created = broker.datetime - timedelta(seconds=60)

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert parent.is_filled()
    assert parent.get_fill_price() == pytest.approx(80.1)


def _build_quoted_spread_strategy():
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    long_asset = Asset("SPY", asset_type="option", expiration="2025-01-17", strike=592, right="put")
    short_asset = Asset("SPY", asset_type="option", expiration="2025-01-17", strike=594, right="put")
    pandas_data = {}
    for asset, last, bid, ask in [(long_asset, 0.50, 0.45, 0.55), (short_asset, 1.00, 0.95, 1.05)]:
        frame = _make_df(price=last)
        frame["high"] = last + 0.05
        frame["low"] = last - 0.05
        frame["bid"] = bid
        frame["ask"] = ask
        pandas_data[(asset, quote)] = Data(
            asset=asset, df=frame.tz_localize(None), quote=quote,
            timestep="minute", timezone="America/New_York",
        )
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=frame.index[0],
        datetime_end=frame.index[-1] + pd.Timedelta(minutes=1),
        show_progress_bar=False, market="24/7", auto_adjust=True,
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    strategy = _StubStrategy(broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})
    strategy._first_iteration = False
    return strategy, broker, long_asset, short_asset, quote


@pytest.mark.parametrize("kind,limit", [("debit", 0.50), ("credit", 0.50), ("even", 0.0)])
def test_quoted_package_limit_rejects_worse_net_without_any_leg_fill(kind, limit):
    """The quoted close costs 0.60 and quoted entry earns 0.40 per package.

    Independent market-leg fills must not violate the parent's explicit net
    limit, even when each leg has an executable bid and ask.
    """
    strategy, broker, long_asset, short_asset, quote = _build_quoted_spread_strategy()
    if kind in {"debit", "even"}:
        broker._filled_positions.append(Position(strategy.name, long_asset, Decimal("3")))
        broker._filled_positions.append(Position(strategy.name, short_asset, Decimal("-3")))
        sides = (Order.OrderSide.SELL_TO_CLOSE, Order.OrderSide.BUY_TO_CLOSE)
    else:
        sides = (Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.SELL_TO_OPEN)
    legs = [
        strategy.create_order(asset, Decimal("3"), side, order_type=Order.OrderType.MARKET, quote=quote)
        for asset, side in zip((long_asset, short_asset), sides)
    ]
    before_positions = {position.asset: position.quantity for position in broker._filled_positions.get_list()}
    before_cash = strategy.cash
    parent = strategy.submit_orders(legs, is_multileg=True, order_type=kind, price=limit)[0]

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert not parent.is_filled()
    assert all(not leg.is_filled() for leg in legs)
    assert parent.is_active()
    assert all(leg.is_active() for leg in legs)
    assert {position.asset: position.quantity for position in broker._filled_positions.get_list()} == before_positions
    assert strategy.cash == before_cash
    events = broker._trade_event_log_df
    assert events.empty or not events["status"].eq("fill").any()


@pytest.mark.parametrize(
    "kind,limit,expected_prices,expected_net",
    [
        ("debit", 0.60, (0.45, 1.05), 0.60),
        ("debit", 0.70, (0.45, 1.05), 0.60),
        ("credit", 0.40, (0.55, 0.95), -0.40),
        ("credit", 0.30, (0.55, 0.95), -0.40),
        ("even", 0.0, (0.55, 0.95), -0.40),
    ],
)
def test_quoted_package_limit_fills_all_legs_at_eligible_observed_prices(kind, limit, expected_prices, expected_net):
    strategy, broker, long_asset, short_asset, quote = _build_quoted_spread_strategy()
    if kind == "debit":
        broker._filled_positions.append(Position(strategy.name, long_asset, Decimal("3")))
        broker._filled_positions.append(Position(strategy.name, short_asset, Decimal("-3")))
        sides = (Order.OrderSide.SELL_TO_CLOSE, Order.OrderSide.BUY_TO_CLOSE)
    else:
        sides = (Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.SELL_TO_OPEN)
    legs = [
        strategy.create_order(asset, Decimal("3"), side, order_type=Order.OrderType.MARKET, quote=quote)
        for asset, side in zip((long_asset, short_asset), sides)
    ]
    before_cash = strategy.cash
    parent = strategy.submit_orders(legs, is_multileg=True, order_type=kind, price=limit)[0]

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert parent.is_filled()
    assert all(leg.is_filled() for leg in legs)
    assert [leg.avg_fill_price for leg in legs] == pytest.approx(expected_prices)
    assert parent.avg_fill_price == pytest.approx(expected_net)
    assert strategy.cash == pytest.approx(before_cash - expected_net * 3 * 100)
    # The atomic package also emits its cash-neutral parent completion event;
    # count only the two executable option legs as trades.
    fills = broker._trade_event_log_df.query("status == 'fill' and `asset.asset_type` != 'multileg'")
    assert len(fills) == 2
    assert len(set(fills["time"])) == 1
