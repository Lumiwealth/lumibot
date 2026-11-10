from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


@dataclass(frozen=True)
class _Quote:
    bid: float
    ask: float


class _BrokerStub:
    IS_BACKTESTING_BROKER = False

    def __init__(self, *, name: str):
        self.name = name
        self.submit_orders = None
        self.submit_order = None
        self.modify_order = None
        self.cancel_order = None
        self.get_tracked_orders = None


def _confirmed_cancel(order):
    # Unit repricing fixtures deliver a terminal broker callback. A no-op
    # cancellation mock intentionally no longer permits a fresh submission.
    order.status = Order.OrderStatus.CANCELED
    order.set_canceled()


class _MinimalStrategy(Strategy):
    """Strategy stub that avoids Strategy.__init__ (no broker side effects)."""

    def __init__(self, broker):
        self.broker = broker
        self._name = "unit"

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


def test_multileg_limit_maps_to_debit_tradier(mocker):
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    def _get_quote(asset, **_kwargs):
        if asset == long_leg.asset:
            return _Quote(bid=1.00, ask=1.20)
        return _Quote(bid=0.50, ask=0.60)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.50)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "debit"
    assert kwargs["price"] == 0.50


def test_multileg_limit_maps_to_credit_tradier(mocker):
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    def _get_quote(asset, **_kwargs):
        if asset == long_leg.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=1.00, ask=1.20)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.30)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "credit"
    assert kwargs["price"] == 0.30


def test_multileg_limit_maps_to_even_tradier_price_none(mocker):
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg1 = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg2 = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    def _get_quote(_asset, **_kwargs):
        return _Quote(bid=1.00, ask=1.00)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    # Omit price; inferred "even" should map to Tradier type=even with no price.
    strategy.submit_order([leg1, leg2], is_multileg=True, order_type=Order.OrderType.LIMIT)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "even"
    assert kwargs["price"] is None


def test_multileg_limit_even_alpaca_uses_zero_price(mocker):
    broker = _BrokerStub(name="alpaca")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg1 = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg2 = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    def _get_quote(_asset, **_kwargs):
        return _Quote(bid=1.00, ask=1.00)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([leg1, leg2], is_multileg=True, order_type=Order.OrderType.LIMIT)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "even"
    assert kwargs["price"] == 0.0


def test_one_option_list_stays_a_single_order(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)

    strategy.submit_order([leg])

    assert broker.submit_orders.call_args.kwargs["is_multileg"] is False


def test_explicit_one_leg_package_is_preserved(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)

    strategy.submit_order([leg], is_multileg=True)

    assert broker.submit_orders.call_args.kwargs["is_multileg"] is True


def test_multileg_limit_weights_unequal_leg_ratios(mocker):
    # Buy 1 at 1.00 and sell 2 at 0.80 is a 0.60 credit. An unweighted sum is a debit.
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    sell.quantity = 2

    def _get_quote(asset, **_kwargs):
        if asset == buy.asset:
            return _Quote(bid=1.00, ask=1.00)
        return _Quote(bid=0.80, ask=0.80)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([buy, sell], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.10)

    assert broker.submit_orders.call_args.kwargs["order_type"] == "credit"


def test_strategy_modify_keeps_the_price_when_the_broker_refuses(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.modify_order = mocker.Mock(return_value=False)
    strategy = _MinimalStrategy(broker)
    order = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        limit_price=0.0,
        order_class=Order.OrderClass.MULTILEG,
        identifier="pkg",
        status=Order.OrderStatus.OPEN,
    )

    strategy.modify_order(order, limit_price=0.10)

    assert order.limit_price == 0.0


def test_schwab_package_reprice_keeps_its_price_when_modify_returns_false(mocker):
    """A refused Schwab package replace leaves the local limit, and the step is not retried in a loop."""
    broker = _BrokerStub(name="Schwab")
    broker.IS_BACKTESTING_BROKER = False
    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock()
    broker.submit_orders = mocker.Mock()
    broker.modify_order = mocker.Mock(return_value=False)
    broker.submit_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor

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
    parent._smart_limit_state = {
        "created_at": 0.0,
        "step_index": 0,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "debit",
    }
    tracked_orders.append(parent)

    def _get_quote(asset, **_kwargs):
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.20, ask=0.30)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.0)

    executor._process_smart_limit_orders()
    executor._process_smart_limit_orders()

    assert broker.modify_order.call_count == 1
    assert not broker.cancel_order.called
    assert parent.limit_price == 0.30
    assert parent._smart_limit_state["step_index"] == 1


def test_package_reprice_on_other_brokers_records_the_price_as_before(mocker):
    """Only Schwab keeps the local limit on a refused replace; other brokers behave as before."""
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False
    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock()
    broker.submit_orders = mocker.Mock()
    broker.modify_order = mocker.Mock(return_value=False)
    broker.submit_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)
    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor

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
    parent._smart_limit_state = {
        "created_at": 0.0,
        "step_index": 0,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "debit",
    }
    tracked_orders.append(parent)

    def _get_quote(asset, **_kwargs):
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.20, ask=0.30)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.0)

    executor._process_smart_limit_orders()
    executor._process_smart_limit_orders()

    assert broker.modify_order.call_count == 1
    assert parent.limit_price != 0.30
    assert parent.limit_price == broker.modify_order.call_args.kwargs["limit_price"]


def test_invalid_package_quantities_do_not_escape_quote_inference(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.10))

    assert strategy._compute_multileg_net_best_fastest([]) is None
    buy.quantity = float("inf")
    assert strategy._compute_multileg_net_best_fastest([buy, sell]) is None
    buy.quantity = float("nan")
    assert strategy._compute_multileg_net_best_fastest([buy, sell]) is None

    # No usable quote net and a positive price: Schwab cannot tell credit from debit.
    assert strategy.submit_order(
        [buy, sell], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.10
    ) is None
    assert not broker.submit_orders.called


def test_multileg_limit_maps_to_credit_schwab(mocker):
    # Schwab sends a positive unhinted limit as NET_DEBIT, so a credit spread
    # passed as order_type='limit' must be mapped to credit before it is sent.
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    def _get_quote(asset, **_kwargs):
        if asset == long_leg.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=1.00, ask=1.20)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.30)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "credit"
    assert kwargs["price"] == 0.30


def test_multileg_limit_even_schwab_uses_zero_price(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg1 = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg2 = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.00, ask=1.00))

    strategy.submit_order([leg1, leg2], is_multileg=True, order_type=Order.OrderType.LIMIT)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "even"
    assert kwargs["price"] == 0.0


def test_multileg_inference_allows_zero_bid(mocker):
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg1 = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg2 = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)

    # Bid=0 is valid for illiquid options; ask must still be >0.
    def _get_quote(asset, **_kwargs):
        if asset == leg1.asset:
            return _Quote(bid=0.00, ask=0.10)
        return _Quote(bid=0.05, ask=0.10)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([leg1, leg2], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.01)

    assert broker.submit_orders.called


def test_smart_limit_multileg_reprice_uses_submit_orders_on_modify_failure(mocker):
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False

    # Track one active multi-leg SMART_LIMIT parent order.
    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock(side_effect=_confirmed_cancel)
    broker.submit_orders = mocker.Mock(return_value=[Order("unit", asset=Asset("SPY"), quantity=1, side=Order.OrderSide.BUY)])
    broker.modify_order = mocker.Mock(side_effect=RuntimeError("modify not supported"))
    broker.submit_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    # Build a parent multileg order with 2 legs (enough to exercise the path).
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
    parent._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "debit",
    }
    tracked_orders.append(parent)

    def _get_quote(asset, **_kwargs):
        # Ensure target price differs from current limit_price so a reprice is attempted.
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.20, ask=0.30)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.0)

    executor._process_smart_limit_orders()

    assert broker.modify_order.called
    assert broker.cancel_order.called
    assert broker.submit_orders.called
    assert not broker.submit_order.called


def test_alpaca_modify_order_updates_identifier_and_child_parent(mocker):
    alpaca = Alpaca.__new__(Alpaca)  # Avoid Alpaca.__init__ (no network / credentials required).

    old_id = "old"
    new_id = "new"

    class _Api:
        def get_order_by_id(self, order_id):
            assert order_id == old_id
            return SimpleNamespace(status="new")

        def replace_order_by_id(self, order_id, order_data):
            assert order_id == old_id
            assert hasattr(order_data, "limit_price")
            return SimpleNamespace(id=new_id, status="accepted", limit_price=getattr(order_data, "limit_price", None))

    alpaca.api = _Api()

    child = Order("unit", asset=Asset("SPY"), quantity=1, side=Order.OrderSide.BUY)
    child.parent_identifier = old_id
    parent = Order("unit", asset=Asset("SPY"), quantity=1, side=Order.OrderSide.BUY, identifier=old_id, child_orders=[child])

    alpaca._modify_order(parent, limit_price=1.23)

    assert parent.identifier == new_id
    assert child.parent_identifier == new_id


def test_multileg_reprice_crosses_credit_to_even_uses_cancel_replace(mocker):
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False

    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock(side_effect=_confirmed_cancel)
    broker.submit_orders = mocker.Mock(return_value=[Order("unit", asset=Asset("SPY"), quantity=1, side=Order.OrderSide.BUY)])
    broker.modify_order = mocker.Mock()
    broker.submit_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    # Quotes engineered to produce a ladder that crosses 0: mid=-0.05, final=+0.05 -> step1=0.0 (even)
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
    parent.limit_price = 0.05
    parent._smart_limit_state = {  # noqa: SLF001 - direct state injection
        "created_at": 0.0,
        "step_index": 0,
        "steps": parent.smart_limit.get_step_count(),
        "step_seconds": parent.smart_limit.get_step_seconds(),
        "final_hold_seconds": parent.smart_limit.get_final_hold_seconds(),
        "multileg_order_type": "credit",
    }
    tracked_orders.append(parent)

    def _get_quote(asset, **_kwargs):
        if asset == leg_buy.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=0.55, ask=0.65)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)  # step_index -> 1

    executor._process_smart_limit_orders()

    # Type change requires cancel+replace (modify_order cannot change debit/credit/even).
    assert broker.cancel_order.called
    assert broker.submit_orders.called
    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "even"
    assert kwargs["price"] is None


def test_smart_limit_skips_quote_fetch_when_step_does_not_advance(mocker):
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False

    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock()
    broker.submit_orders = mocker.Mock()
    broker.modify_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    executor = StrategyExecutor(strategy=strategy)
    strategy._executor = executor  # noqa: SLF001 - test wiring

    order = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        status=Order.OrderStatus.OPEN,
        smart_limit=SmartLimitConfig(preset=SmartLimitPreset.FAST, step_seconds=5, final_hold_seconds=999),
        identifier="oid",
    )
    order.limit_price = 1.0
    order._smart_limit_state = {  # noqa: SLF001
        "created_at": 0.0,
        "step_index": 0,
        "steps": order.smart_limit.get_step_count(),
        "step_seconds": order.smart_limit.get_step_seconds(),
        "final_hold_seconds": order.smart_limit.get_final_hold_seconds(),
    }
    tracked_orders.append(order)

    get_quote = mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=1.0, ask=1.1))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.0)  # still step 0

    executor._process_smart_limit_orders()

    assert not get_quote.called
    assert not broker.modify_order.called
    assert not broker.cancel_order.called


def test_multileg_quote_exception_does_not_crash(mocker):
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False

    tracked_orders: list[Order] = []
    broker.get_tracked_orders = mocker.Mock(side_effect=lambda _name: tracked_orders)
    broker.cancel_order = mocker.Mock()
    broker.submit_orders = mocker.Mock()
    broker.modify_order = mocker.Mock()

    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)

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
    tracked_orders.append(parent)

    mocker.patch.object(strategy, "get_quote", side_effect=RuntimeError("connection interrupted"))
    mocker.patch("lumibot.strategies.strategy_executor.time.monotonic", return_value=1.1)

    executor._process_smart_limit_orders()

    assert not broker.modify_order.called
    assert not broker.cancel_order.called
    assert not broker.submit_orders.called


def test_multileg_submit_missing_quotes_downgrades_to_market(mocker):
    broker = _BrokerStub(name="tradier")
    broker.IS_BACKTESTING_BROKER = False
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    leg_buy.order_type = Order.OrderType.SMART_LIMIT
    leg_sell.order_type = Order.OrderType.SMART_LIMIT
    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST)
    leg_buy.smart_limit = cfg
    leg_sell.smart_limit = cfg

    def _get_quote(_asset, **_kwargs):
        return _Quote(bid=None, ask=None)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    strategy.submit_order([leg_buy, leg_sell], is_multileg=True)

    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == Order.OrderType.MARKET


def test_schwab_smart_limit_missing_quotes_does_not_submit_a_market_package(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.IS_BACKTESTING_BROKER = False
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)

    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    leg_buy.order_type = Order.OrderType.SMART_LIMIT
    leg_sell.order_type = Order.OrderType.SMART_LIMIT
    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST)
    leg_buy.smart_limit = cfg
    leg_sell.smart_limit = cfg

    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=None, ask=None))

    assert strategy.submit_order([leg_buy, leg_sell], is_multileg=True) is None
    assert not broker.submit_orders.called


def test_schwab_smart_limit_submit_error_does_not_retry_a_credit_as_a_debit(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.IS_BACKTESTING_BROKER = False
    broker.submit_orders = mocker.Mock(side_effect=RuntimeError("rejected"))
    strategy = _MinimalStrategy(broker)

    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    long_leg.order_type = Order.OrderType.SMART_LIMIT
    short_leg.order_type = Order.OrderType.SMART_LIMIT
    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST)
    long_leg.smart_limit = cfg
    short_leg.smart_limit = cfg

    def _get_quote(asset, **_kwargs):
        if asset == long_leg.asset:
            return _Quote(bid=0.50, ask=0.60)
        return _Quote(bid=1.00, ask=1.20)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    assert strategy.submit_order([long_leg, short_leg], is_multileg=True) is None
    assert broker.submit_orders.call_count == 1
    _, kwargs = broker.submit_orders.call_args
    assert kwargs["order_type"] == "credit"


def test_one_option_list_keeps_the_package_default_on_other_brokers(mocker):
    """Only Schwab needs two legs for a package; other brokers keep their old default."""
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)

    strategy.submit_order([leg])

    assert broker.submit_orders.call_args.kwargs["is_multileg"] is True


def test_strategy_modify_on_other_brokers_keeps_the_existing_behavior(mocker):
    """The refusal guard is Schwab's; another broker's False result is handled as before."""
    broker = _BrokerStub(name="projectx")
    broker.modify_order = mocker.Mock(return_value=False)
    strategy = _MinimalStrategy(broker)
    order = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        limit_price=0.0,
        order_type=Order.OrderType.LIMIT,
        identifier="single",
        status=Order.OrderStatus.OPEN,
    )

    strategy.modify_order(order, limit_price=0.10)

    assert order.limit_price == 0.10


def test_multileg_smart_limit_replacement_keeps_parent_gtc(mocker):
    """A canceled GTC package is resubmitted as GTC, not the day default."""
    broker = _BrokerStub(name="Schwab")
    replacement = Order("unit", Asset("SPY"), 1, Order.OrderSide.BUY)
    broker.submit_orders = mocker.Mock(return_value=[replacement])
    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)
    executor = StrategyExecutor(strategy=strategy)

    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    parent = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        time_in_force="gtc",
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[leg_buy, leg_sell],
    )
    parent._smart_limit_state = {
        "created_at": 0.0,
        "step_seconds": 1,
        "steps": 3,
        "final_hold_seconds": 999,
    }
    _confirmed_cancel(parent)

    executor._advance_smart_limit_replacement(
        {"order": parent, "price": 0.40, "order_type": "credit", "multileg": True},
        10,
    )

    assert broker.submit_orders.call_args.kwargs["duration"] == "gtc"
    assert replacement.order_type == Order.OrderType.SMART_LIMIT


def test_multileg_smart_limit_replacement_duration_is_unchanged_on_other_brokers(mocker):
    """Only Schwab receives the parent duration; other brokers get the call they got before."""
    broker = _BrokerStub(name="tradier")
    replacement = Order("unit", Asset("SPY"), 1, Order.OrderSide.BUY)
    broker.submit_orders = mocker.Mock(return_value=[replacement])
    strategy = _MinimalStrategy(broker)
    strategy.logger = SimpleNamespace(error=lambda *_a, **_k: None)
    executor = StrategyExecutor(strategy=strategy)

    leg_buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    leg_sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    parent = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.BUY,
        time_in_force="gtc",
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[leg_buy, leg_sell],
    )
    parent._smart_limit_state = {
        "created_at": 0.0,
        "step_seconds": 1,
        "steps": 3,
        "final_hold_seconds": 999,
    }
    _confirmed_cancel(parent)

    executor._advance_smart_limit_replacement(
        {"order": parent, "price": 0.40, "order_type": "credit", "multileg": True},
        10,
    )

    assert "duration" not in broker.submit_orders.call_args.kwargs
    assert replacement.order_type == Order.OrderType.SMART_LIMIT


def test_schwab_limit_package_without_quotes_is_not_submitted(mocker):
    """A positive price is not a debit hint when Schwab cannot see the quotes."""
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=None, ask=None))

    assert strategy.submit_order(
        [long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.30
    ) is None
    assert not broker.submit_orders.called

    strategy.submit_order(
        [long_leg, short_leg], is_multileg=True, order_type="credit", price=0.30
    )

    assert broker.submit_orders.call_args.kwargs["order_type"] == "credit"
    assert broker.submit_orders.call_args.kwargs["price"] == 0.30


def test_schwab_negative_limit_without_quotes_is_a_credit(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=None, ask=None))

    strategy.submit_order(
        [long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=-0.30
    )

    assert broker.submit_orders.call_args.kwargs["order_type"] == "credit"
    assert broker.submit_orders.call_args.kwargs["price"] == 0.30


def test_schwab_zero_limit_without_quotes_is_even(mocker):
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=None, ask=None))

    strategy.submit_order(
        [long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0
    )

    assert broker.submit_orders.call_args.kwargs["order_type"] == "even"
    assert broker.submit_orders.call_args.kwargs["price"] == 0.0


def test_tradier_positive_limit_without_quotes_stays_a_debit(mocker):
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    long_leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    short_leg = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    mocker.patch.object(strategy, "get_quote", return_value=_Quote(bid=None, ask=None))

    strategy.submit_order(
        [long_leg, short_leg], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.30
    )

    assert broker.submit_orders.call_args.kwargs["order_type"] == "debit"
    assert broker.submit_orders.call_args.kwargs["price"] == 0.30


def test_multileg_limit_on_other_brokers_keeps_the_unweighted_net_quote(mocker):
    """Quantity weighting is Schwab's; Tradier still sums one quote per leg."""
    # Buy 1 at 1.00 and sell 2 at 0.80: the unweighted sum is a 0.20 debit.
    broker = _BrokerStub(name="tradier")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    buy = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)
    sell = _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN)
    sell.quantity = 2

    def _get_quote(asset, **_kwargs):
        """Return the bid and ask scripted for this leg."""
        if asset == buy.asset:
            return _Quote(bid=1.00, ask=1.00)
        return _Quote(bid=0.80, ask=0.80)

    mocker.patch.object(strategy, "get_quote", side_effect=_get_quote)

    assert strategy._compute_multileg_net_best_fastest([buy, sell]) == pytest.approx((0.20, 0.20))
    strategy.submit_order([buy, sell], is_multileg=True, order_type=Order.OrderType.LIMIT, price=0.10)
    assert broker.submit_orders.call_args.kwargs["order_type"] == "debit"


def test_one_option_list_with_a_package_price_is_refused_on_schwab(mocker):
    """A list-level price would be dropped from a single Schwab order, so nothing is sent."""
    broker = _BrokerStub(name="Schwab")
    broker.submit_orders = mocker.Mock(return_value="ok")
    strategy = _MinimalStrategy(broker)
    leg = _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN)

    assert strategy.submit_order([leg], order_type=Order.OrderType.LIMIT, price=1.25) is None
    broker.submit_orders.assert_not_called()


class _FillNotificationStrategy:
    """Strategy double for the executor's live fill notification."""

    def __init__(self, broker):
        self.broker = broker
        self.hide_trades = False
        self.portfolio_value = 10_000.0
        self.messages = []
        self.fills = []

    def on_trading_iteration(self):
        return None

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.fills.append((position, order))

    def send_discord_message(self, message, silent=False):
        self.messages.append(message)


def test_schwab_package_fill_without_a_position_still_sends_its_notification():
    broker = _BrokerStub(name="Schwab")
    strategy = _FillNotificationStrategy(broker)
    executor = StrategyExecutor(strategy)
    package = Order(
        "unit",
        asset=Asset("SPY"),
        quantity=1,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.LIMIT,
        limit_price=1.25,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[
            _leg("unit", _option("SPY", 505), Order.OrderSide.SELL_TO_OPEN),
            _leg("unit", _option("SPY", 500), Order.OrderSide.BUY_TO_OPEN),
        ],
    )

    executor._on_filled_order(None, package, price=1.20, quantity=1, multiplier=1)

    assert strategy.fills == [(None, package)]
    assert len(strategy.messages) == 1 and "SPY" in strategy.messages[0]
    # The package parent carries no contract multiplier; the total uses the legs' 100.
    assert "Trade Total = $120.00" in strategy.messages[0]
