"""Bitunix HEDGE mode can hold a long and a short leg on one symbol at once.

LumiBot's generic Position/Order carry ``position_side`` and ``position_id`` so
separate legs never overwrite each other (see test_derivative_position_identity).
The Bitunix adapter must fill them in. Before 2026-10-23 a snapshot holding both
legs raised "Multiple active Bitunix positions for one symbol cannot be
represented safely", so a hedged account could not sync at all.

The single-leg tests at the bottom pin the behavior an existing live strategy
relies on (neutral synthetic names): ``get_position(asset)`` with no selector
returns the one leg with its quantity, entry price and raw exchange row
(``positionId``/``side``), and a direct hedge-mode CLOSE fill reduces that leg.
All exchange traffic is intercepted locally; no account or order is touched.
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lumibot.brokers.bitunix import Bitunix
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.tools.bitunix_helpers import BitUnixClient

SYMBOL = "BTCUSDT"
OWNER = "synthetic-hedge-strategy"


def contract():
    return Asset(SYMBOL, Asset.AssetType.CRYPTO_FUTURE)


def row(side, qty, position_id, price="60000"):
    return {"symbol": SYMBOL, "side": side, "qty": qty, "positionId": position_id,
            "avgOpenPrice": price, "positionMode": "HEDGE"}


BOTH_LEGS = [row("LONG", "0.03", "leg-long"), row("SHORT", "0.01", "leg-short", price="61000")]


@pytest.fixture
def hedged():
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client.get_trading_pairs = MagicMock(return_value={
        "code": 0, "data": [{"symbol": SYMBOL, "basePrecision": 4, "quotePrecision": 1, "minTradeVolume": "0.0001"}]})
    client.change_position_mode = MagicMock(return_value={"code": 0, "data": [{"positionMode": "HEDGE"}]})
    client.change_leverage = MagicMock(return_value={"code": 0})
    client.get_positions = MagicMock(return_value={"code": 0, "data": [dict(item) for item in BOTH_LEGS]})
    with patch("lumibot.brokers.bitunix.BitUnixClient", return_value=client), patch(
        "lumibot.brokers.bitunix.BitunixData"
    ), patch("requests.request") as request:
        request.return_value.json.return_value = {"code": 0, "data": {"orderId": "test-order"}}
        broker = Bitunix({"API_KEY": "test-key", "API_SECRET": "test-secret"}, connect_stream=False)
        broker._safe_stream_dispatch = MagicMock()
        yield broker, client, request


def strategy_for(broker):
    strategy = object.__new__(Strategy)
    strategy._name = OWNER
    strategy.broker = broker
    return strategy


def wire_bodies(request):
    return [json.loads(call.kwargs["data"]) for call in request.call_args_list]


def test_snapshot_with_long_and_short_on_one_symbol_keeps_both_legs(hedged):
    broker, _, _ = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    legs = sorted(broker.get_tracked_positions(OWNER), key=lambda position: position.position_side)
    assert [(p.position_side, p.position_id, p.quantity, p.avg_fill_price) for p in legs] == [
        ("long", "leg-long", 0.03, Decimal("60000")),
        ("short", "leg-short", -0.01, Decimal("61000")),
    ]
    assert legs[0]._raw["positionId"] == "leg-long"


def test_both_legs_survive_repeated_sync_and_update_independently(hedged):
    broker, client, _ = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    long_before = broker.get_tracked_position(OWNER, contract(), position_side="long")
    client.get_positions.return_value = {"code": 0, "data": [row("LONG", "0.05", "leg-long"),
                                                             row("SHORT", "0.01", "leg-short", price="61000")]}
    broker.sync_positions(SimpleNamespace(name=OWNER))
    assert broker.get_tracked_position(OWNER, contract(), position_side="long") is long_before
    assert long_before.quantity == float("0.05")
    assert broker.get_tracked_position(OWNER, contract(), position_side="short").quantity == float("-0.01")


def test_strategy_selects_a_leg_and_refuses_to_guess(hedged):
    broker, _, _ = hedged
    strategy = strategy_for(broker)
    assert strategy.get_position(contract(), position_side="short").quantity == float("-0.01")
    assert strategy.get_position(contract(), position_id="leg-long").quantity == float("0.03")
    with pytest.raises(ValueError, match="position_side"):
        strategy.get_position(contract())
    assert len(strategy.broker.get_tracked_positions(OWNER)) == 2


def test_closing_one_leg_sends_that_legs_hedge_close_only(hedged):
    broker, _, request = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    order = broker.close_position(OWNER, contract(), position_side="short")
    assert order.position_side == "short" and order.position_id == "leg-short"
    assert order.side == Order.OrderSide.BUY
    body = wire_bodies(request)[-1]
    assert (body["tradeSide"], body["side"], body["positionId"], body["qty"]) == ("CLOSE", "SELL", "leg-short", "0.0100")


def test_closing_without_selector_closes_both_legs_by_their_own_ids(hedged):
    broker, _, request = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    orders = broker.close_position(OWNER, contract())
    assert sorted(order.position_id for order in orders) == ["leg-long", "leg-short"]
    sent = sorted((body["positionId"], body["side"], body["qty"]) for body in wire_bodies(request))
    assert sent == [("leg-long", "BUY", "0.0300"), ("leg-short", "SELL", "0.0100")]


def test_close_by_position_id_uses_that_exact_exchange_position(hedged):
    broker, _, request = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    broker.close_position(OWNER, contract(), position_id="leg-long", fraction=0.5)
    body = wire_bodies(request)[-1]
    assert (body["positionId"], body["side"], body["qty"]) == ("leg-long", "BUY", "0.0150")


def test_opening_orders_name_the_leg_they_open(hedged):
    broker, _, _ = hedged
    buy = broker._submit_order(Order(OWNER, contract(), Decimal("0.01"), Order.OrderSide.BUY))
    sell = broker._submit_order(Order(OWNER, contract(), Decimal("0.01"), Order.OrderSide.SELL))
    assert (buy.position_side, sell.position_side) == ("long", "short")


def test_fills_land_on_the_matching_leg(hedged):
    # Live quantities come from the exchange snapshot; a fill must attach its
    # order to the leg it traded, never to the other leg on the same symbol.
    broker, _, _ = hedged
    broker.sync_positions(SimpleNamespace(name=OWNER))
    long_leg = broker.get_tracked_position(OWNER, contract(), position_side="long")
    short_leg = broker.get_tracked_position(OWNER, contract(), position_side="short")

    opening_short = broker._submit_order(Order(OWNER, contract(), Decimal("0.02"), Order.OrderSide.SELL))
    filled_into = broker._process_filled_order(opening_short, 61000, Decimal("0.02"))
    assert filled_into is short_leg
    assert any(o is opening_short for o in short_leg.orders)
    assert not any(o is opening_short for o in long_leg.orders)

    closing_long = broker.close_position(OWNER, contract(), position_side="long")
    filled_into = broker._process_filled_order(closing_long, 60500, Decimal("0.03"))
    assert filled_into is long_leg
    assert not any(o is closing_long for o in short_leg.orders)


@pytest.mark.parametrize(
    "wire_side, trade_side, expected_side, expected_leg",
    [("BUY", "OPEN", Order.OrderSide.BUY, "long"), ("SELL", "OPEN", Order.OrderSide.SELL, "short"),
     ("BUY", "CLOSE", Order.OrderSide.SELL, "long"), ("SELL", "CLOSE", Order.OrderSide.BUY, "short")],
)
def test_parsed_exchange_orders_carry_their_leg(hedged, wire_side, trade_side, expected_side, expected_leg):
    broker, _, _ = hedged
    order = broker._parse_broker_order({
        "orderId": "exchange-order", "symbol": SYMBOL, "status": "FILLED", "side": wire_side,
        "orderType": "MARKET", "qty": "0.01", "tradeQty": "0.01", "tradeSide": trade_side,
        "positionId": f"leg-{expected_leg}", "leverage": "10"}, OWNER)
    assert order.side == expected_side
    assert order.position_side == expected_leg
    assert order.position_id == f"leg-{expected_leg}"


def test_same_side_duplicate_without_distinct_ids_still_fails_closed(hedged):
    broker, client, _ = hedged
    client.get_positions.return_value = {"code": 0, "data": [row("LONG", "0.03", "leg-long"),
                                                             row("LONG", "0.02", "leg-long")]}
    from lumibot.brokers.broker import LumibotBrokerAPIError
    with pytest.raises(LumibotBrokerAPIError):
        broker.sync_positions(SimpleNamespace(name=OWNER))


# --- Single-leg usage an existing live strategy depends on (unchanged) ---------


def test_single_leg_get_position_without_selector_is_unchanged(hedged):
    broker, client, _ = hedged
    client.get_positions.return_value = {"code": 0, "data": [row("LONG", "0.03", "leg-long")]}
    live = strategy_for(broker).get_position(contract())
    assert live.quantity == float("0.03")
    assert live.avg_fill_price == Decimal("60000")
    assert live._raw["positionId"] == "leg-long" and str(live._raw["side"]).upper() == "LONG"
    assert [p.quantity for p in broker.get_tracked_positions(OWNER)] == [0.03]


def test_single_leg_direct_close_fill_attaches_to_that_leg_and_flip_follows_snapshot(hedged):
    broker, client, _ = hedged
    client.get_positions.return_value = {"code": 0, "data": [row("LONG", "0.03", "leg-long")]}
    strategy = strategy_for(broker)
    live = strategy.get_position(contract())
    # A hedge-mode CLOSE placed directly on the exchange API, seen by polling.
    close = broker._parse_broker_order({
        "orderId": "direct-close", "symbol": SYMBOL, "status": "FILLED", "side": "BUY", "orderType": "MARKET",
        "qty": "0.03", "tradeQty": "0.03", "tradeSide": "CLOSE", "positionId": "leg-long", "leverage": "10"}, OWNER)
    filled_into = broker._process_filled_order(close, 60500, Decimal("0.03"))
    assert filled_into is live and close.side == Order.OrderSide.SELL
    # Flat, then a short on the same symbol: one leg again, selector not needed.
    client.get_positions.return_value = {"code": 0, "data": []}
    assert strategy.get_position(contract()) is None
    client.get_positions.return_value = {"code": 0, "data": [row("SHORT", "0.02", "leg-short2")]}
    short = strategy.get_position(contract())
    assert short.quantity == float("-0.02") and short._raw["positionId"] == "leg-short2"
