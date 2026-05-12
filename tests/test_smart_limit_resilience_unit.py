from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace

from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


@dataclass(frozen=True)
class _Quote:
    bid: float | None
    ask: float | None


class _BrokerStub:
    IS_BACKTESTING_BROKER = False

    def __init__(self, *, name: str):
        self.name = name
        self.submit_order = None
        self.submit_orders = None
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


def _option(symbol: str, strike: float) -> Asset:
    return Asset(
        symbol,
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 1, 16),
        strike=float(strike),
        right="call",
    )


def _leg(strategy_name: str, asset: Asset, side: Order.OrderSide) -> Order:
    return Order(strategy_name, asset=asset, quantity=1, side=side, order_type=Order.OrderType.MARKET)


def test_single_leg_partially_filled_orders_still_reprice(mocker):
    broker = _BrokerStub(name="stub")
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
        quantity=10,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
        status=Order.OrderStatus.PARTIALLY_FILLED,
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


def test_multileg_final_hold_cancel_failure_is_swallowed(mocker):
    broker = _BrokerStub(name="tradier")
    broker.cancel_order = mocker.Mock(side_effect=RuntimeError("cancel failed"))
    broker.modify_order = mocker.Mock()
    broker.submit_orders = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=2)
    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    parent = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[leg_buy, leg_sell],
        status=Order.OrderStatus.OPEN,
        smart_limit=cfg,
        identifier="parent-id",
    )
    parent._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 0,
        "steps": cfg.get_step_count(),
        "step_seconds": cfg.get_step_seconds(),
        "final_hold_seconds": cfg.get_final_hold_seconds(),
        "multileg_order_type": "debit",
    }
    tracked.append(parent)

    # final_hold_start = step_seconds*(steps-1) = 2; cancel after +2 seconds => >=4
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=4.1)

    # No exception should propagate.
    executor._process_smart_limit_orders()
    assert broker.cancel_order.called


def test_multileg_cancel_replace_submit_failure_is_swallowed(mocker):
    broker = _BrokerStub(name="tradier")
    broker.cancel_order = mocker.Mock()
    broker.modify_order = mocker.Mock(side_effect=RuntimeError("modify not supported"))
    broker.submit_orders = mocker.Mock(side_effect=RuntimeError("rate limited"))
    broker.submit_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=mocker.Mock())

    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    parent = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[leg_buy, leg_sell],
        status=Order.OrderStatus.OPEN,
        smart_limit=SmartLimitConfig(preset=SmartLimitPreset.FAST, step_seconds=1, final_hold_seconds=999),
        identifier="parent-id",
    )
    parent.limit_price = 0.30
    parent._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 0,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "debit",
    }
    tracked.append(parent)

    def _get_quote(asset, **_kwargs):
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.20, ask=0.30)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    # No exception should propagate; errors should be logged.
    executor._process_smart_limit_orders()
    assert broker.cancel_order.called
    assert broker.submit_orders.called
    assert strategy.logger.error.called
    assert not broker.submit_order.called


def test_multileg_cross_even_to_debit_replaces_with_debit_price(mocker):
    broker = _BrokerStub(name="tradier")
    broker.cancel_order = mocker.Mock()
    broker.submit_orders = mocker.Mock(
        return_value=[Order("unit", asset=Asset("SPY"), quantity=1, side=Order.OrderSide.BUY)]
    )
    broker.modify_order = mocker.Mock()

    tracked: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(return_value=tracked)

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    # Quotes engineered to produce ladder: mid=-0.05, step1=0.0 (even), step2=+0.05 (debit)
    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    parent = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[leg_buy, leg_sell],
        status=Order.OrderStatus.OPEN,
        smart_limit=SmartLimitConfig(preset=SmartLimitPreset.FAST, step_seconds=1, final_hold_seconds=999),
        identifier="parent-id",
    )
    parent.limit_price = 0.0
    parent._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 1,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "even",
    }
    tracked.append(parent)

    def _get_quote(asset, **_kwargs):
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.55, ask=0.65)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=2.1)  # step_index -> 2

    executor._process_smart_limit_orders()

    assert broker.cancel_order.called
    assert broker.submit_orders.called
    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "debit"
    assert abs(float(kwargs["price"]) - 0.05) < 1e-9
