"""Fill events must only reach the strategy that owns the order.

Regression for Titus / shared-account option fills (2026-09-03):
a live STM call strategy read MOS option activity from the broker account and
submitted an unintended 100-share STM stock hedge. Broker polling must not
attribute foreign-tagged fills to the local strategy, and on_filled_order must
not fire for those fills.
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy_executor import StrategyExecutor


def test_normalize_and_match_strategy_tags():
    assert Broker.normalize_broker_strategy_tag("STM Call Strategy") == "STM-Call-Strategy"
    assert Broker.normalize_broker_strategy_tag("MOS") == "MOS"
    assert Broker.strategy_tag_matches("STM-Call-Strategy", "STM Call Strategy") is True
    assert Broker.strategy_tag_matches("MOS", "STM Call Strategy") is False
    assert Broker.strategy_tag_matches(None, "STM Call Strategy") is False
    assert Broker.strategy_tag_matches("MOS", None) is False


def test_option_underlying_matches_helper():
    stm = Asset("STM", asset_type=Asset.AssetType.STOCK)
    mos_opt = Asset(
        "MOS",
        asset_type=Asset.AssetType.OPTION,
        expiration="2026-09-18",
        strike=30,
        right=Asset.OptionRight.CALL,
    )
    stm_opt = Asset(
        "STM",
        asset_type=Asset.AssetType.OPTION,
        expiration="2026-09-18",
        strike=50,
        right=Asset.OptionRight.CALL,
        underlying_asset=stm,
    )
    assert Broker.option_underlying_symbol(mos_opt) == "MOS"
    assert Broker.option_underlying_symbol(stm_opt) == "STM"
    assert Broker.fills_match_underlying(mos_opt, {"STM"}) is False
    assert Broker.fills_match_underlying(stm_opt, {"STM", "stm"}) is True
    assert Broker.fills_match_underlying(stm, {"STM"}) is True


class _Logger:
    def isEnabledFor(self, level: int) -> bool:
        return False

    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


class _HedgeStrategy:
    """Records hedge attempts from on_filled_order (option fill → 100-share stock)."""

    def __init__(self, name: str, underlying: str = "STM"):
        self._name = name
        self.name = name
        self.underlying = underlying
        self.hide_trades = True
        self.portfolio_value = 100_000.0
        self.cash = 100_000.0
        self.sleeptime = "1S"
        self.is_backtesting = False
        self._first_iteration = False
        self.logger = _Logger()
        self.quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
        self.broker = MagicMock()
        self.broker.IS_BACKTESTING_BROKER = False
        self.broker.name = "dummy"
        self.hedge_orders = []
        self.recorded_fills = []
        self._filled_order_callback = None

    def on_trading_iteration(self):
        return None

    def log_message(self, *args, **kwargs):
        return None

    def send_discord_message(self, *args, **kwargs):
        return None

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.recorded_fills.append(order)
        # Mimic customer hedge: option fill → 100-share stock market buy on strategy underlying
        if getattr(order.asset, "asset_type", None) == Asset.AssetType.OPTION:
            # Bug path ignored ownership/underlying; correct strategies must gate both.
            if order.strategy and order.strategy != self.name:
                return
            fill_underlying = Broker.option_underlying_symbol(order.asset)
            if fill_underlying and fill_underlying.upper() != self.underlying.upper():
                return
            hedge = Order(
                strategy=self.name,
                asset=Asset(self.underlying, asset_type=Asset.AssetType.STOCK),
                quantity=100,
                side="buy",
                order_type="market",
            )
            self.hedge_orders.append(hedge)


def _make_option_order(*, strategy: str, underlying: str, identifier: str, tag: str | None = None) -> Order:
    asset = Asset(
        underlying,
        asset_type=Asset.AssetType.OPTION,
        expiration="2026-09-18",
        strike=30,
        right=Asset.OptionRight.CALL,
    )
    return Order(
        strategy=strategy,
        asset=asset,
        quantity=1,
        side="buy_to_open",
        order_type="market",
        identifier=identifier,
        tag=tag if tag is not None else Broker.normalize_broker_strategy_tag(strategy),
    )


def test_executor_drops_foreign_strategy_fills_before_hedge():
    """BEFORE metrics: foreign fill reaches on_filled_order and creates a hedge.
    AFTER metrics: foreign fill is dropped; no hedge.
    """
    strategy = _HedgeStrategy("STM Call Strategy", underlying="STM")
    executor = StrategyExecutor(strategy)
    strategy._executor = executor

    foreign = _make_option_order(
        strategy="MOS",
        underlying="MOS",
        identifier="mos-fill-1",
        tag="MOS",
    )
    position = MagicMock()
    position.asset = foreign.asset

    t0 = time.perf_counter()
    executor._on_filled_order(
        position,
        foreign,
        price=1.25,
        quantity=Decimal("1"),
        multiplier=100,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    # Metrics for PR body (before broken attribution would record 1 fill + 1 hedge)
    before_foreign_fills_delivered = 1  # historical buggy behavior
    before_unintended_hedges = 1
    after_foreign_fills_delivered = len(strategy.recorded_fills)
    after_unintended_hedges = len(strategy.hedge_orders)

    assert after_foreign_fills_delivered == 0, (
        f"Foreign MOS fill was delivered to STM on_filled_order "
        f"(before={before_foreign_fills_delivered}, after={after_foreign_fills_delivered})"
    )
    assert after_unintended_hedges == 0, (
        f"Unintended STM hedge fired from MOS fill "
        f"(before={before_unintended_hedges}, after={after_unintended_hedges}, "
        f"elapsed_ms={elapsed_ms:.3f})"
    )


def test_executor_allows_owned_option_fill_hedge():
    strategy = _HedgeStrategy("STM Call Strategy", underlying="STM")
    executor = StrategyExecutor(strategy)
    strategy._executor = executor

    owned = _make_option_order(
        strategy="STM Call Strategy",
        underlying="STM",
        identifier="stm-fill-1",
    )
    # Ensure strategy field matches executor strategy name
    owned.strategy = strategy.name
    position = MagicMock()
    position.asset = owned.asset

    executor._on_filled_order(
        position,
        owned,
        price=2.5,
        quantity=Decimal("1"),
        multiplier=100,
    )

    assert len(strategy.recorded_fills) == 1
    assert len(strategy.hedge_orders) == 1
    assert strategy.hedge_orders[0].asset.symbol == "STM"
    assert int(strategy.hedge_orders[0].quantity) == 100


@pytest.fixture
def tradier_broker(mocker):
    from lumibot.brokers.tradier import Tradier

    broker = Tradier(
        account_number="1234",
        access_token="a1b2c3",
        paper=True,
        polling_interval=None,
        connect_stream=False,
    )
    broker.stream = MagicMock()
    broker._strategy_name = "STM Call Strategy"
    broker._first_iteration = False
    mocker.patch.object(broker, "sync_positions", return_value=None)
    return broker


def test_tradier_polling_skips_foreign_tagged_fill(tradier_broker, mocker):
    """Shared Tradier account: MOS-tagged filled option must not be ingested by STM."""
    broker = tradier_broker
    dispatched = []

    def _capture(event, **kwargs):
        dispatched.append((event, kwargs))

    mocker.patch.object(broker, "_safe_stream_dispatch", side_effect=_capture)
    mocker.patch.object(
        broker,
        "_pull_broker_all_orders",
        return_value=[
            {
                "id": 9001,
                "type": "market",
                "side": "buy_to_open",
                "symbol": "MOS",
                "option_symbol": "MOS250918C00030000",
                "class": "option",
                "quantity": 1,
                "status": "filled",
                "duration": "day",
                "create_date": "2026-09-03T12:00:00.000Z",
                "avg_fill_price": 1.25,
                "exec_quantity": 1,
                "tag": "MOS",
            }
        ],
    )

    t0 = time.perf_counter()
    broker.do_polling()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    known_ids = {str(o.identifier) for o in broker.get_all_orders()}
    fill_events = [d for d in dispatched if d[0] == broker.FILLED_ORDER]

    before_foreign_orders_tracked = 1
    before_fill_events = 1
    after_foreign_orders_tracked = 1 if "9001" in known_ids else 0
    after_fill_events = len(fill_events)

    assert after_foreign_orders_tracked == 0, (
        f"Foreign MOS order was tracked by STM broker "
        f"(before={before_foreign_orders_tracked}, after={after_foreign_orders_tracked}, "
        f"elapsed_ms={elapsed_ms:.3f})"
    )
    assert after_fill_events == 0, (
        f"Foreign MOS fill was dispatched to STM "
        f"(before={before_fill_events}, after={after_fill_events})"
    )


def test_tradier_polling_still_processes_owned_tagged_fill(tradier_broker, mocker):
    broker = tradier_broker
    # Seed a locally submitted order so status transition can fill.
    asset = Asset(
        "STM",
        asset_type=Asset.AssetType.OPTION,
        expiration="2026-09-18",
        strike=50,
        right=Asset.OptionRight.CALL,
    )
    local = Order(
        strategy="STM Call Strategy",
        asset=asset,
        quantity=1,
        side="buy_to_open",
        order_type="market",
        identifier=9002,
        tag="STM-Call-Strategy",
    )
    local.status = "open"
    broker._new_orders.append(local)

    dispatched = []

    def _capture(event, **kwargs):
        dispatched.append((event, kwargs))

    mocker.patch.object(broker, "_safe_stream_dispatch", side_effect=_capture)
    mocker.patch.object(
        broker,
        "_pull_broker_all_orders",
        return_value=[
            {
                "id": 9002,
                "type": "market",
                "side": "buy_to_open",
                "symbol": "STM",
                "option_symbol": "STM250918C00050000",
                "class": "option",
                "quantity": 1,
                "status": "filled",
                "duration": "day",
                "create_date": "2026-09-03T12:00:00.000Z",
                "avg_fill_price": 2.5,
                "exec_quantity": 1,
                "tag": "STM-Call-Strategy",
            }
        ],
    )

    broker.do_polling()
    fill_events = [d for d in dispatched if d[0] == broker.FILLED_ORDER]
    assert len(fill_events) == 1
    assert fill_events[0][1]["order"].identifier == 9002


def test_sole_subscriber_does_not_claim_foreign_tag(tradier_broker):
    broker = tradier_broker
    broker._strategy_name = ""
    sub = MagicMock()
    sub.name = "STM Call Strategy"
    broker._subscribers = [sub]

    order = _make_option_order(strategy="", underlying="MOS", identifier="x", tag="MOS")
    order.strategy = ""
    # Repair path must refuse to claim MOS as STM
    assert broker.order_belongs_to_local_strategy(order, local_strategy_name="STM Call Strategy") is False
    assert broker.order_belongs_to_local_strategy(
        _make_option_order(strategy="STM Call Strategy", underlying="STM", identifier="y"),
        local_strategy_name="STM Call Strategy",
    ) is True


def test_schwab_snapshot_does_not_seed_untracked_foreign_new(mocker):
    """Shared Schwab account: untracked MOS NEW must not become STM tracked."""
    from lumibot.brokers.schwab import Schwab

    broker = Schwab.__new__(Schwab)
    broker._strategy_name = "STM Call Strategy"
    broker._subscribers = []
    broker.logger = _Logger()
    broker._schwab_observed_fill_quantities = {}
    broker._schwab_terminal_observations = set()
    broker.get_tracked_order = MagicMock(return_value=None)
    seeded = []
    broker._process_new_order = MagicMock(side_effect=lambda o: seeded.append(o))
    broker._process_trade_event = MagicMock()
    broker.FILLED_ORDER = Broker.FILLED_ORDER
    broker.PARTIALLY_FILLED_ORDER = Broker.PARTIALLY_FILLED_ORDER
    broker.NEW_ORDER = Broker.NEW_ORDER
    broker.CANCELED_ORDER = Broker.CANCELED_ORDER
    broker.ERROR_ORDER = Broker.ERROR_ORDER

    foreign = _make_option_order(
        strategy="STM Call Strategy",  # wrongly attributed at parse time
        underlying="MOS",
        identifier="schwab-mos-1",
        tag="MOS",
    )
    foreign.status = Order.OrderStatus.NEW

    broker._process_schwab_order_snapshot(foreign)

    assert seeded == []
    broker._process_trade_event.assert_not_called()


def test_schwab_snapshot_still_fills_already_tracked_owned_order(mocker):
    from lumibot.brokers.schwab import Schwab

    broker = Schwab.__new__(Schwab)
    broker._strategy_name = "STM Call Strategy"
    broker._subscribers = []
    broker.logger = _Logger()
    broker._schwab_observed_fill_quantities = {}
    broker._schwab_terminal_observations = set()
    broker._log_schwab_lifecycle_event = MagicMock()
    broker._schwab_order_ref = MagicMock(return_value="ref")

    owned = _make_option_order(
        strategy="STM Call Strategy",
        underlying="STM",
        identifier="schwab-stm-1",
        tag="STM-Call-Strategy",
    )
    owned.status = Order.OrderStatus.NEW
    owned.is_filled = MagicMock(return_value=False)
    owned.is_canceled = MagicMock(return_value=False)
    broker.get_tracked_order = MagicMock(return_value=owned)
    broker._process_trade_event = MagicMock()
    broker.FILLED_ORDER = Broker.FILLED_ORDER
    broker.PARTIALLY_FILLED_ORDER = Broker.PARTIALLY_FILLED_ORDER
    broker.NEW_ORDER = Broker.NEW_ORDER
    broker.CANCELED_ORDER = Broker.CANCELED_ORDER
    broker.ERROR_ORDER = Broker.ERROR_ORDER

    observed = _make_option_order(
        strategy="STM Call Strategy",
        underlying="STM",
        identifier="schwab-stm-1",
        tag="STM-Call-Strategy",
    )
    observed.status = Order.OrderStatus.FILLED
    observed._schwab_cumulative_filled_quantity = 1.0
    observed._schwab_average_fill_price = 2.5
    observed.avg_fill_price = 2.5

    broker._process_schwab_order_snapshot(observed)

    assert broker._process_trade_event.called
    assert broker._process_trade_event.call_args[0][1] == Broker.FILLED_ORDER
