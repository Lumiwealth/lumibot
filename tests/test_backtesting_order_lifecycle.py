from datetime import datetime as dt, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytz

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset, Order, Quote


class _StrategyStub:
    name = "order_lifecycle_test"

    def __init__(self):
        self.log_message = MagicMock()


def _broker_at(now, time_to_close=3600):
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = SimpleNamespace(
        _timestep="minute",
        get_datetime=lambda: now,
    )
    broker.logger = MagicMock()
    broker.get_time_to_close = MagicMock(return_value=time_to_close)

    def _cancel(order):
        order.status = Order.OrderStatus.CANCELED

    broker.cancel_order = MagicMock(side_effect=_cancel)
    return broker


def _market_order(time_in_force="day", created_at=None, asset=None, quote=None):
    asset = asset or Asset("SPY", asset_type=Asset.AssetType.STOCK)
    order = Order(
        asset=asset,
        quote=quote,
        quantity=1,
        side="buy",
        order_type=Order.OrderType.MARKET,
        strategy=_StrategyStub.name,
        time_in_force=time_in_force,
        date_created=created_at,
    )
    order.status = Order.OrderStatus.NEW
    return order


def test_day_market_order_waits_when_execution_data_missing_before_close():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    order = _market_order("day", created_at=now - timedelta(minutes=5))
    strategy = _StrategyStub()

    assert broker._handle_unavailable_execution_data(order, "no current bar", strategy=strategy) is True

    broker.cancel_order.assert_not_called()
    assert order.is_active()
    strategy.log_message.assert_called_once()


def test_ioc_and_fok_cancel_when_no_current_executable_data():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    strategy = _StrategyStub()

    for tif in ("ioc", "fok"):
        broker = _broker_at(now, time_to_close=3600)
        order = _market_order(tif, created_at=now)

        assert broker._handle_unavailable_execution_data(order, "no current bar", strategy=strategy) is True

        broker.cancel_order.assert_called_once_with(order)
        assert order.is_canceled()


def test_day_order_cancels_after_session_close_without_fabricated_fill():
    now = pytz.UTC.localize(dt(2026, 4, 17, 16, 1))
    broker = _broker_at(now, time_to_close=0)
    order = _market_order("day", created_at=now.replace(hour=10, minute=0))

    assert broker._cancel_order_if_time_in_force_elapsed(order, strategy=_StrategyStub()) is True

    broker.cancel_order.assert_called_once_with(order)
    assert order.is_canceled()


def test_day_order_cancels_on_next_trading_date_without_current_bar():
    created_at = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    now = pytz.UTC.localize(dt(2026, 4, 20, 9, 31))
    broker = _broker_at(now, time_to_close=6 * 60 * 60)
    order = _market_order("day", created_at=created_at)

    assert broker._cancel_order_if_time_in_force_elapsed(order, strategy=_StrategyStub()) is True

    broker.cancel_order.assert_called_once_with(order)
    assert order.is_canceled()


def test_day_order_hard_tif_cancels_before_next_date_fill_attempt():
    created_at = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    now = pytz.UTC.localize(dt(2026, 4, 20, 9, 31))
    broker = _broker_at(now, time_to_close=6 * 60 * 60)
    order = _market_order("day", created_at=created_at)

    assert broker._cancel_order_if_hard_time_in_force_elapsed(order, strategy=_StrategyStub()) is True

    broker.cancel_order.assert_called_once_with(order)
    assert order.is_canceled()


def test_day_order_hard_tif_allows_same_date_final_bar_evaluation():
    now = pytz.UTC.localize(dt(2026, 4, 17, 16, 0))
    broker = _broker_at(now, time_to_close=0)
    order = _market_order("day", created_at=now.replace(hour=10, minute=0))

    assert broker._cancel_order_if_hard_time_in_force_elapsed(order, strategy=_StrategyStub()) is False

    broker.cancel_order.assert_not_called()
    assert order.is_active()


def test_gtd_order_cancels_at_good_till_date():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    order = _market_order("gtd", created_at=now - timedelta(hours=1))
    order.good_till_date = now - timedelta(seconds=1)

    assert broker._cancel_order_if_time_in_force_elapsed(order, strategy=_StrategyStub()) is True

    broker.cancel_order.assert_called_once_with(order)
    assert order.is_canceled()


def test_gtd_order_hard_tif_cancels_before_fill_attempt():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    order = _market_order("gtd", created_at=now - timedelta(hours=1))
    order.good_till_date = now

    assert broker._cancel_order_if_hard_time_in_force_elapsed(order, strategy=_StrategyStub()) is True

    broker.cancel_order.assert_called_once_with(order)
    assert order.is_canceled()


def test_gtd_order_waits_before_good_till_date_when_data_missing():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    order = _market_order("gtd", created_at=now - timedelta(hours=1))
    order.good_till_date = now + timedelta(hours=1)

    assert broker._handle_unavailable_execution_data(order, "no current bar", strategy=_StrategyStub()) is True

    broker.cancel_order.assert_not_called()
    assert order.is_active()


def test_thin_option_market_order_can_fill_from_current_quote_without_trade_bar():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    option = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=dt(2026, 4, 17).date(),
        strike=500,
        right=Asset.OptionRight.CALL,
    )
    order = _market_order("day", created_at=now, asset=option)
    broker.get_quote = MagicMock(
        return_value=Quote(
            asset=option,
            bid=1.20,
            ask=1.35,
            raw_data={"bar_timestamp": now, "bar_timestep": "minute"},
        )
    )

    assert broker._try_fill_with_quote(order, strategy=_StrategyStub(), open_=None, high_=None, low_=None) == 1.35


def test_stale_quote_does_not_fill_intraday_ibkr_market_order():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    broker = _broker_at(now, time_to_close=3600)
    broker.data_source = SimpleNamespace(
        SOURCE="INTERACTIVEBROKERSREST",
        _timestep="minute",
        get_datetime=lambda: now,
    )
    order = _market_order("day", created_at=now)
    broker.get_quote = MagicMock(
        return_value=Quote(
            asset=order.asset,
            bid=100.0,
            ask=101.0,
            raw_data={
                "bar_timestamp": pd.Timestamp(now) - pd.Timedelta(minutes=1),
                "bar_timestep": "minute",
            },
        )
    )

    assert broker._try_fill_with_quote(order, strategy=_StrategyStub(), open_=None, high_=None, low_=None) is None
    assert order.is_active()


def test_future_intraday_bar_does_not_fill_at_earlier_sim_timestamp():
    now = pytz.UTC.localize(dt(2026, 4, 17, 12, 30))
    future_bar = now + timedelta(minutes=2)
    broker = _broker_at(now, time_to_close=3600)
    clock = {"now": now}
    broker.data_source = SimpleNamespace(
        SOURCE="INTERACTIVEBROKERSREST",
        _timestep="minute",
        get_datetime=lambda: clock["now"],
    )
    order = _market_order("day", created_at=now)
    broker.get_quote = MagicMock(
        return_value=Quote(
            asset=order.asset,
            bid=100.0,
            ask=101.0,
            raw_data={"bar_timestamp": future_bar, "bar_timestep": "minute"},
        )
    )

    assert broker._execution_bar_matches_datetime(future_bar, broker.datetime, "minute") is False
    assert broker._try_fill_with_quote(order, strategy=_StrategyStub(), open_=None, high_=None, low_=None) is None
    assert broker._defer_market_order_with_unavailable_execution_data(
        order,
        f"selected minute bar {future_bar} does not match current sim dt {broker.datetime}",
        strategy=_StrategyStub(),
    ) is True

    broker.cancel_order.assert_not_called()
    assert order.is_active()

    clock["now"] = future_bar
    assert broker._execution_bar_matches_datetime(future_bar, broker.datetime, "minute") is True
    assert broker._try_fill_with_quote(order, strategy=_StrategyStub(), open_=None, high_=None, low_=None) == 101.0
