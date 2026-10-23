"""Derivative legs retain their identity through the shared broker lifecycle."""

import pytest
from unittest.mock import Mock

from lumibot.entities import Asset, Order, Position
from tests.test_broker_sync_positions import _BrokerForSyncTest, _Strategy
from tests.test_strategy_close_position import DummyBroker, DummyStrategy


def contract():
    return Asset("BTC/USDT:USDT", asset_type="crypto_future", multiplier=0.001)


def leg(side, quantity, owner="strategy"):
    return Position(owner, contract(), quantity, position_side=side,
                    position_id=f"contract-{side}")


def test_position_round_trip_preserves_both_legs_and_optional_balances():
    for side, quantity in (("long", 10), ("short", -4)):
        position = leg(side, quantity)
        restored = Position.from_dict(position.to_dict())
        assert restored.position_side == side
        assert restored.position_id == f"contract-{side}"
        assert restored.asset == position.asset
        assert restored.quantity == quantity


def test_fill_order_round_trip_preserves_leg_identity():
    order = Order("strategy", contract(), 4, "sell", position_side="short", position_id="contract-short")
    restored = Order.from_dict(order.to_dict())
    position = restored.to_position(4)
    assert position.position_side == "short"
    assert position.position_id == "contract-short"
    assert position.quantity == -4
    assert order.to_minimal_dict()["position_side"] == "short"
    assert position.to_minimal_dict()["position_id"] == "contract-short"


def test_sync_preserves_hedge_legs_and_prunes_only_the_missing_leg():
    long, short = leg("long", 10), leg("short", -4)
    broker = _BrokerForSyncTest([leg("long", 12), leg("short", -6)])
    broker._filled_positions.extend([long, short])

    broker.sync_positions(_Strategy())

    assert long.quantity == 12
    assert short.quantity == -6
    assert broker.get_tracked_position("strategy", contract(), position_side="long") is long
    assert broker.get_tracked_position("strategy", contract(), position_side="short") is short
    broker._broker_positions = [leg("short", -5)]
    broker.sync_positions(_Strategy())
    assert broker.get_tracked_positions("strategy") == [short]
    assert short.quantity == -5


def test_unselected_position_lookup_does_not_silently_choose_a_hedge_leg():
    broker = _BrokerForSyncTest([])
    broker._filled_positions.extend([leg("long", 10), leg("short", -4)])
    with pytest.raises(ValueError, match="position_side"):
        broker.get_tracked_position("strategy", contract())


def test_closing_order_preserves_leg_and_reduce_only():
    broker = _BrokerForSyncTest([])
    order = broker._create_position_closing_order(leg("short", -4))
    assert order.position_side == "short"
    assert order.position_id == "contract-short"
    assert order.side == "buy"
    assert order.quantity == 4
    assert order.reduce_only


def test_fill_does_not_attach_short_order_to_long_leg():
    broker = _BrokerForSyncTest([])
    long = leg("long", 10)
    broker._filled_positions.append(long)
    order = Order("strategy", contract(), 4, "sell", position_side="short", position_id="contract-short")

    short = broker._process_filled_order(order, 20000, 4)

    assert short is not long
    assert short.position_side == "short"
    assert long.orders == []


def test_close_position_without_selector_closes_both_legs():
    broker = _BrokerForSyncTest([])
    broker._filled_positions.extend([leg("long", 10), leg("short", -4)])
    broker.submit_order = Mock(side_effect=lambda order: order)

    closed = broker.close_position("strategy", contract(), fraction=0.5)

    assert [(order.position_side, order.side, float(order.quantity)) for order in closed] == [
        ("long", "sell", 5), ("short", "buy", 2),
    ]


def test_sync_does_not_reassign_an_identified_leg_to_another_strategy():
    owned = leg("short", -4, owner="owner-strategy")
    broker = _BrokerForSyncTest([leg("short", -5)])
    broker._filled_positions.append(owned)

    broker.sync_positions(_Strategy())

    assert owned.strategy == "owner-strategy"
    assert owned.quantity == -5


def test_strategy_forwards_explicit_leg_selectors_to_broker():
    broker = DummyBroker()
    strategy = DummyStrategy(broker)
    short = leg("short", -4, owner=strategy.name)
    broker.get_tracked_position = Mock(return_value=short)
    broker.close_position = Mock(return_value=object())

    assert strategy.get_position(contract(), broker_refresh=False, position_side="short") is short
    strategy.close_position(contract(), position_side="short", position_id="contract-short")

    broker.close_position.assert_called_once_with(
        strategy.name, contract(), 1.0, position_side="short", position_id="contract-short")


def test_strategy_can_close_all_legs_after_ambiguous_single_position_lookup():
    broker = DummyBroker()
    strategy = DummyStrategy(broker)
    broker.get_tracked_position = Mock(side_effect=ValueError("select position_side"))
    broker.close_position = Mock(return_value=[object(), object()])

    assert len(strategy.close_position(contract())) == 2
    broker.close_position.assert_called_once_with(strategy.name, contract(), 1.0)



def test_sync_upgrades_an_unsided_tracked_position_in_place_and_keeps_its_owner():
    # A fill or older snapshot can create a position before the broker names its
    # leg. The first sided snapshot must update that same object, not replace it
    # with an ownerless copy (a background poll syncs with no strategy).
    tracked = Position("owner-strategy", contract(), 10)
    broker = _BrokerForSyncTest([leg("long", 12, owner="")])
    broker._filled_positions.append(tracked)

    broker.sync_positions(None)

    assert broker._filled_positions.get_list() == [tracked]
    assert tracked.strategy == "owner-strategy"
    assert tracked.position_side == "long" and tracked.position_id == "contract-long"
    assert tracked.quantity == 12
