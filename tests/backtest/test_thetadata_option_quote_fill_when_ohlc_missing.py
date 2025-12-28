import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset, Order, Quote
from lumibot.tools.lumibot_logger import get_logger


class _StubDataSource:
    """Minimal DataSource stub for unit-testing BacktestingBroker order processing."""

    SOURCE = "PANDAS"
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, now: datetime.datetime):
        self._now = now
        self._timestep = "minute"

    def get_datetime(self):
        return self._now

    def get_historical_prices(self, *args, **kwargs):
        # Simulate sparse ThetaData option OHLC (None/empty), while quotes may still exist.
        return None


class _ListWrapper:
    def __init__(self, items):
        self._items = list(items)

    def get_list(self):
        return list(self._items)


class DummyStrategy:
    """Bare-minimum Strategy stub required by BacktestingBroker.process_pending_orders."""

    def __init__(self):
        self.name = "TestStrategy"
        self.parameters = {"max_spread_pct": 0.25}
        self.messages = []

    def log_message(self, message, color=None):
        self.messages.append((message, color))


def test_thetadata_option_market_order_fills_from_quote_when_ohlc_missing():
    """Regression test: ThetaData option minute OHLC can be missing while NBBO exists.

    If OHLC is missing, the broker should still attempt quote-based fills for option orders.

    NOTE (test authority):
    - This test guards against the "canceled BUY_TO_CLOSE -> cash_settled at expiry" failure mode that
      materially changes PnL and can make backtests non-deterministic.
    - After this has been in the suite for ~1 year, treat it as LEGACY/high-authority: fix code first.
    """

    now = datetime.datetime(2025, 1, 21, 15, 55, tzinfo=datetime.timezone.utc)
    data_source = _StubDataSource(now)

    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = data_source
    broker.logger = get_logger("thetadata_quote_fill_missing_ohlc_test")
    broker.hybrid_prefetcher = None
    broker.prefetcher = None
    broker._trade_event_log_df = None

    broker._is_thetadata_source = MagicMock(return_value=True)

    option_asset = Asset(
        "SPXW",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.date(2025, 1, 21),
        strike=5880.0,
        right="CALL",
    )
    order = Order(
        strategy="TestStrategy",
        asset=option_asset,
        quantity=Decimal("1"),
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
    )
    order.quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    broker._unprocessed_orders = _ListWrapper([])
    broker._new_orders = _ListWrapper([order])

    broker.get_quote = MagicMock(
        return_value=Quote(
            asset=option_asset,
            bid=169.2,
            ask=170.0,
            timestamp=now,
        )
    )

    broker._execute_filled_order = MagicMock()
    broker.process_expired_option_contracts = MagicMock()

    strategy = DummyStrategy()

    broker.process_pending_orders(strategy)

    broker._execute_filled_order.assert_called_once()
    _, kwargs = broker._execute_filled_order.call_args
    assert kwargs["order"] is order
    assert kwargs["price"] == pytest.approx(170.0)
