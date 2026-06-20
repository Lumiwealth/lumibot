import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset, Order
from lumibot.tools.lumibot_logger import get_logger


class _StubCcxtDataSource:
    SOURCE = "CCXT"
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, now: datetime.datetime, bars: pd.DataFrame):
        self._now = now
        self._bars = bars
        self._timestep = "minute"
        self.calls = []

    def get_datetime(self):
        return self._now

    def get_historical_prices(self, *args, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(df=self._bars, empty=self._bars.empty)


class _ListWrapper:
    def __init__(self, items):
        self._items = list(items)

    def get_list(self):
        return list(self._items)


class _DummyStrategy:
    name = "TestStrategy"
    parameters = {}

    def __init__(self):
        self.messages = []

    def log_message(self, message, color=None):
        self.messages.append((message, color))


def _bars(index, open_prices):
    return pd.DataFrame(
        {
            "open": open_prices,
            "high": [price + 1 for price in open_prices],
            "low": [price - 1 for price in open_prices],
            "close": [price + 0.5 for price in open_prices],
            "volume": [10.0 for _ in open_prices],
        },
        index=pd.DatetimeIndex(index),
    )


def _broker_for_ccxt(now: datetime.datetime, bars: pd.DataFrame, order: Order):
    data_source = _StubCcxtDataSource(now, bars)
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = data_source
    broker.logger = get_logger("ccxt_execution_semantics_test")
    broker.hybrid_prefetcher = None
    broker.prefetcher = None
    broker._trade_event_log_df = None
    broker._unprocessed_orders = _ListWrapper([])
    broker._new_orders = _ListWrapper([order])
    broker.get_quote = MagicMock(return_value=None)
    broker._execute_filled_order = MagicMock()
    broker.cancel_order = MagicMock()
    broker.get_time_to_close = MagicMock(return_value=999)
    broker._run_backtest_option_lifecycle_tasks = MagicMock()
    return broker, data_source


def _market_order(asset, quote, *, tif="day"):
    order = Order(
        strategy="TestStrategy",
        asset=asset,
        quantity=Decimal("0.01"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        time_in_force=tif,
        quote=quote,
    )
    return order


def test_ccxt_market_order_waits_when_only_future_bar_is_available():
    now = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    future_bar = now + datetime.timedelta(minutes=10)
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote)
    broker, data_source = _broker_for_ccxt(now, _bars([future_bar], [220.0]), order)

    broker.process_pending_orders(_DummyStrategy())

    broker._execute_filled_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    assert data_source.calls[0]["timeshift"] is None


def test_ccxt_market_order_fills_at_real_next_bar_timestamp_not_original_gap_time():
    now = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    next_bar = now + datetime.timedelta(minutes=10)
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote)
    broker, data_source = _broker_for_ccxt(now, _bars([next_bar], [220.0]), order)
    strategy = _DummyStrategy()

    broker.process_pending_orders(strategy)
    broker._execute_filled_order.assert_not_called()

    data_source._now = next_bar
    broker.process_pending_orders(strategy)

    broker._execute_filled_order.assert_called_once()
    _, kwargs = broker._execute_filled_order.call_args
    assert kwargs["order"] is order
    assert kwargs["price"] == 220.0
    assert broker.datetime == next_bar


def test_ccxt_gtc_market_order_waits_across_sparse_gap_and_fills_at_real_bar_timestamp():
    now = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    next_bar = now + datetime.timedelta(days=7)
    asset = Asset("RARE", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote, tif="gtc")
    broker, data_source = _broker_for_ccxt(now, _bars([next_bar], [12.0]), order)
    strategy = _DummyStrategy()

    broker.process_pending_orders(strategy)
    broker._execute_filled_order.assert_not_called()
    broker.cancel_order.assert_not_called()

    data_source._now = next_bar
    broker.process_pending_orders(strategy)

    broker._execute_filled_order.assert_called_once()
    _, kwargs = broker._execute_filled_order.call_args
    assert kwargs["order"] is order
    assert kwargs["price"] == 12.0
    assert broker.datetime == next_bar


def test_ccxt_gtd_market_order_expires_before_later_sparse_bar_can_fill():
    submitted_at = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    later_bar = submitted_at + datetime.timedelta(days=7)
    asset = Asset("RARE", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote, tif="gtd")
    order._date_created = submitted_at
    order.good_till_date = submitted_at + datetime.timedelta(hours=1)
    broker, _data_source = _broker_for_ccxt(later_bar, _bars([later_bar], [12.0]), order)

    broker.process_pending_orders(_DummyStrategy())

    broker._execute_filled_order.assert_not_called()
    broker.cancel_order.assert_called_once_with(order)


def test_ccxt_ioc_market_order_cancels_when_current_bar_missing():
    now = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    next_bar = now + datetime.timedelta(minutes=10)
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote, tif="ioc")
    broker, _data_source = _broker_for_ccxt(now, _bars([next_bar], [220.0]), order)

    broker.process_pending_orders(_DummyStrategy())

    broker._execute_filled_order.assert_not_called()
    broker.cancel_order.assert_called_once_with(order)


def test_ccxt_ioc_limit_order_cancels_when_current_bar_does_not_cross():
    now = datetime.datetime(2026, 3, 12, 12, 0, tzinfo=datetime.timezone.utc)
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = Order(
        strategy="TestStrategy",
        asset=asset,
        quantity=Decimal("0.01"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("100"),
        time_in_force="ioc",
        quote=quote,
    )
    broker, _data_source = _broker_for_ccxt(now, _bars([now], [220.0]), order)

    broker.process_pending_orders(_DummyStrategy())

    broker._execute_filled_order.assert_not_called()
    broker.cancel_order.assert_called_once_with(order)


def test_crypto_day_orders_expire_on_utc_date_boundary():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = _StubCcxtDataSource(
        datetime.datetime(2026, 1, 2, 0, 15, tzinfo=datetime.timezone.utc),
        pd.DataFrame(),
    )
    broker.get_time_to_close = MagicMock(return_value=999)
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    order = _market_order(asset, quote)
    order._date_created = datetime.datetime(2026, 1, 1, 23, 30, tzinfo=datetime.timezone.utc)

    assert broker._order_time_in_force_elapsed(order) is True


def test_stock_day_orders_keep_existing_non_utc_date_boundary_behavior():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = _StubCcxtDataSource(
        datetime.datetime(2026, 1, 1, 19, 15, tzinfo=datetime.timezone(datetime.timedelta(hours=-5))),
        pd.DataFrame(),
    )
    broker.get_time_to_close = MagicMock(return_value=999)
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    order = _market_order(asset, quote)
    order._date_created = datetime.datetime(
        2026,
        1,
        1,
        18,
        30,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-5)),
    )

    assert broker._order_time_in_force_elapsed(order) is False
