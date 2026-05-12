from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


@dataclass(frozen=True)
class _Quote:
    bid: float
    ask: float


class _BrokerStub:
    IS_BACKTESTING_BROKER = False

    def __init__(self):
        self.name = "stub"
        self.submit_order = None
        self.modify_order = None
        self.cancel_order = None
        self.get_tracked_orders = None


class _MinimalStrategy(Strategy):
    """Strategy stub that avoids Strategy.__init__ (no broker side effects)."""

    def __init__(self, broker):
        self.broker = broker
        self._name = "unit"
        self.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    def log_message(self, *_args, **_kwargs):
        return

    def _validate_order(self, _order):  # noqa: SLF001 - intentional override for unit tests
        return True

    def on_trading_iteration(self):
        return


def test_single_leg_submit_sets_initial_limit_and_state(mocker):
    broker = _BrokerStub()
    strategy = _MinimalStrategy(broker)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.20))

    called_order_types = []

    def _submit(order):
        called_order_types.append(order.order_type)
        return order

    broker.submit_order = mocker.Mock(side_effect=_submit)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )

    submitted = strategy.submit_order(order)
    assert submitted is order

    # The broker sees the order as LIMIT (SMART_LIMIT is an internal wrapper).
    assert called_order_types == [Order.OrderType.LIMIT]
    assert order.order_type == Order.OrderType.SMART_LIMIT
    assert order.limit_price is not None
    assert order._smart_limit_state["steps"] >= 1  # noqa: SLF001 - state contract
    assert order._smart_limit_state["step_seconds"] >= 1  # noqa: SLF001 - state contract


def test_single_leg_executor_reprices_using_modify_order(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=999)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.20))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()

    assert broker.modify_order.called
    _, kwargs = broker.modify_order.call_args
    # With FAST preset (3 steps), step 1 targets the midpoint->ask ladder, i.e. ~1.15.
    assert abs(float(kwargs["limit_price"]) - 1.15) < 1e-6
    assert not broker.cancel_order.called


def test_single_leg_final_hold_cancels(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=2)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.20))
    # final_hold_start = step_seconds*(steps-1) = 2; cancel after +2 seconds => >=4
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=4.1)

    executor._process_smart_limit_orders()
    assert broker.cancel_order.called
    assert not broker.modify_order.called


def test_single_leg_quote_exception_does_not_crash_or_modify(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=999)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch.object(strategy, "get_quote", side_effect=RuntimeError("connection interrupted"))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()
    assert not broker.modify_order.called
    assert not broker.cancel_order.called


def test_single_leg_state_with_zero_step_seconds_is_defensive(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=999)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": 0,  # corrupt/invalid
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.20))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()

    assert order._smart_limit_state["step_seconds"] >= 1  # noqa: SLF001 - defensive clamp
    assert broker.modify_order.called


def test_single_leg_sell_ladder_moves_toward_bid(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=999)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 100.0
    order._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    # bid/ask produce mid=100, final(bid)=99, step1=99.5
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=99.0, ask=101.0))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()

    _, kwargs = broker.modify_order.call_args
    assert abs(float(kwargs["limit_price"]) - 99.5) < 1e-6


def test_single_leg_modify_failure_falls_back_to_cancel_and_resubmit(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock(side_effect=RuntimeError("connection interrupted"))
    broker.cancel_order = mocker.Mock()

    submitted_types: list[str] = []

    def _submit(order):
        submitted_types.append(str(order.order_type))
        return order

    broker.submit_order = mocker.Mock(side_effect=_submit)

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=999)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.20))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()

    assert broker.modify_order.called
    assert broker.cancel_order.called
    assert broker.submit_order.called
    assert Order.OrderType.LIMIT in submitted_types


def test_single_leg_final_hold_cancel_failure_is_swallowed(mocker):
    broker = _BrokerStub()
    broker.modify_order = mocker.Mock()
    broker.cancel_order = mocker.Mock(side_effect=RuntimeError("cancel failed"))

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=2)
    order = Order(
        "unit",
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.OPEN,
        identifier="oid",
    )
    order.limit_price = 1.10
    order._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
    }
    tracked.append(order)

    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=4.1)
    # No exception should propagate.
    executor._process_smart_limit_orders()
    assert broker.cancel_order.called
