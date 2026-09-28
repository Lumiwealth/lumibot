from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.interactive_brokers import IBApp
from lumibot.entities import Asset, Order, Position


ALPACA_UNIT_CONFIG = {
    "API_KEY": "test_api_key",
    "API_SECRET": "test_api_secret",
    "PAPER": True,
}


def _option() -> Asset:
    return Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 9, 18),
        strike=500,
        right="CALL",
    )


def _broker() -> Alpaca:
    return Alpaca(ALPACA_UNIT_CONFIG, connect_stream=False)


def test_alpaca_single_leg_submission_preserves_explicit_position_intent():
    broker = _broker()
    broker.api.submit_order = MagicMock(
        return_value=SimpleNamespace(id="alpaca-order-1", status="accepted")
    )
    order = Order(
        strategy="intent-test",
        asset=_option(),
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
    )

    broker.submit_order(order)

    request = broker.api.submit_order.call_args.kwargs["order_data"]
    assert str(request.side) == "sell"
    assert str(request.position_intent) == "sell_to_close"


def test_alpaca_parser_restores_broker_returned_position_intent():
    broker = _broker()
    response = {
        "id": "alpaca-order-2",
        "symbol": "SPY260918C00500000",
        "qty": "1",
        "side": "sell",
        "position_intent": "sell_to_close",
        "asset_class": "us_option",
        "type": "market",
        "time_in_force": "day",
        "status": "accepted",
        "order_class": "simple",
    }

    parsed = broker._parse_broker_order(response, "intent-test")

    assert parsed.side == Order.OrderSide.SELL_TO_CLOSE


def test_close_position_emits_explicit_option_close():
    broker = _broker()
    option = _option()
    broker._filled_positions.append(Position("intent-test", option, 2))
    broker._submit_order = MagicMock(side_effect=lambda order: order)

    result = broker.close_position("intent-test", option)

    assert result.side == Order.OrderSide.SELL_TO_CLOSE


def test_generic_option_sell_fails_when_pending_close_exhausts_position():
    broker = _broker()
    option = _option()
    broker._filled_positions.append(Position("intent-test", option, 1))
    pending_close = Order(
        strategy="intent-test",
        asset=option,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        status=Order.OrderStatus.NEW,
    )
    broker._new_orders.append(pending_close)
    broker.api.submit_order = MagicMock()
    duplicate = Order(
        strategy="intent-test",
        asset=option,
        quantity=1,
        side=Order.OrderSide.SELL,
    )

    with pytest.raises(ValueError, match="remaining closable quantity"):
        broker.submit_order(duplicate)

    broker.api.submit_order.assert_not_called()


@pytest.mark.parametrize(
    ("position_quantity", "generic_side", "expected_side"),
    [
        (2, Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE),
        (2, Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN),
        (-2, Order.OrderSide.BUY, Order.OrderSide.BUY_TO_CLOSE),
        (-2, Order.OrderSide.SELL, Order.OrderSide.SELL_TO_OPEN),
        (0, Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN),
        (0, Order.OrderSide.SELL, Order.OrderSide.SELL_TO_OPEN),
    ],
)
def test_generic_option_sides_resolve_centrally(
    position_quantity, generic_side, expected_side
):
    broker = _broker()
    option = _option()
    if position_quantity:
        broker._filled_positions.append(
            Position("intent-test", option, position_quantity)
        )
    order = Order(
        strategy="intent-test",
        asset=option,
        quantity=1,
        side=generic_side,
    )

    resolved = broker.resolve_option_order_intent(order)

    assert resolved.side == expected_side


def test_explicit_option_open_intent_wins_even_with_opposite_position():
    broker = _broker()
    option = _option()
    broker._filled_positions.append(Position("intent-test", option, 2))
    order = Order(
        strategy="intent-test",
        asset=option,
        quantity=1,
        side=Order.OrderSide.SELL_TO_OPEN,
    )

    resolved = broker.resolve_option_order_intent(order)

    assert resolved.side == Order.OrderSide.SELL_TO_OPEN


def test_batch_of_generic_option_closes_cannot_cross_zero():
    broker = _broker()
    option = _option()
    broker._filled_positions.append(Position("intent-test", option, 1))
    broker._submit_orders = MagicMock()
    closes = [
        Order(
            strategy="intent-test",
            asset=option,
            quantity=1,
            side=Order.OrderSide.SELL,
        )
        for _ in range(2)
    ]

    with pytest.raises(ValueError, match="remaining closable quantity"):
        broker.submit_orders(closes)

    broker._submit_orders.assert_not_called()


def test_alpaca_rejection_is_raised_after_order_is_marked_error():
    broker = _broker()
    rejection = RuntimeError("account not eligible to trade uncovered option contracts")
    broker.api.submit_order = MagicMock(side_effect=rejection)
    order = Order(
        strategy="intent-test",
        asset=_option(),
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
    )

    with pytest.raises(RuntimeError, match="uncovered option contracts"):
        broker.submit_order(order)

    assert order.status == Order.OrderStatus.ERROR


@pytest.mark.parametrize(
    ("intent", "ibkr_action"),
    [
        (Order.OrderSide.BUY_TO_OPEN, "BUY"),
        (Order.OrderSide.BUY_TO_CLOSE, "BUY"),
        (Order.OrderSide.SELL_TO_OPEN, "SELL"),
        (Order.OrderSide.SELL_TO_CLOSE, "SELL"),
    ],
)
def test_ibkr_translation_accepts_all_explicit_option_intents(intent, ibkr_action):
    assert IBApp.get_safe_action(None, intent) == ibkr_action
