from unittest.mock import Mock, call

import pytest

import lumibot.brokers.interactive_brokers_rest as ibkr_rest_module
from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST
from lumibot.entities import Asset, Order


@pytest.fixture
def broker(monkeypatch):
    monkeypatch.setattr(ibkr_rest_module, "colored", lambda text, *args, **kwargs: text)
    broker = InteractiveBrokersREST.__new__(InteractiveBrokersREST)
    broker.data_source = Mock()
    broker.data_source.get_conid_from_asset.return_value = 265598
    broker.data_source.get_contract_rules.return_value = {"rules": {"increment": 0.01}}
    return broker


def _order(**kwargs):
    defaults = {
        "strategy": "ibkr_rest_order_semantics",
        "asset": Asset("SPY"),
        "quantity": 1,
        "side": Order.OrderSide.BUY,
        "time_in_force": "gtc",
        "exchange": "SMART",
    }
    defaults.update(kwargs)
    return Order(**defaults)


def test_simple_order_payload_is_unchanged(broker):
    order = _order(
        identifier="simple-parent",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
    )

    payload = broker._get_order_data_for_submission(order)

    ticket = payload["orders"][0]
    assert ticket == {
        "conid": 265598,
        "quantity": 1,
        "orderType": "LMT",
        "side": "BUY",
        "tif": "GTC",
        "price": pytest.approx(99.99),
        "listingExchange": "SMART",
    }
    assert not {"cOID", "parentId", "isSingleGroup"}.intersection(ticket)
    broker.data_source.get_conid_from_asset.assert_called_once_with(order.asset, exchange="SMART")


def test_bracket_payload_with_one_child_links_parent_and_inherits_exchange(broker):
    parent = _order(
        identifier="bracket-parent",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
    )
    child = parent.child_orders[0]

    payload = broker._get_order_data_for_submission(parent)

    parent_ticket, child_ticket = payload["orders"]
    assert parent_ticket["cOID"] == "bracket-parent"
    assert child_ticket["parentId"] == "bracket-parent"
    assert "cOID" not in child_ticket
    assert parent_ticket["price"] == pytest.approx(99.99)
    assert child_ticket["price"] == pytest.approx(109.99)
    assert child_ticket["listingExchange"] == "SMART"
    assert child.exchange is None
    assert broker.data_source.get_conid_from_asset.call_args_list == [
        call(parent.asset, exchange="SMART"),
        call(child.asset, exchange="SMART"),
    ]


def test_bracket_payload_with_two_children_only_links_the_parent(broker):
    parent = _order(
        identifier="bracket-parent-two",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )

    payload = broker._get_order_data_for_submission(parent)

    assert len(payload["orders"]) == 3
    assert payload["orders"][0]["cOID"] == "bracket-parent-two"
    assert payload["orders"][1]["parentId"] == "bracket-parent-two"
    assert payload["orders"][2]["parentId"] == "bracket-parent-two"
    assert "cOID" not in payload["orders"][1]
    assert "cOID" not in payload["orders"][2]
    assert payload["orders"][1]["orderType"] == "LMT"
    assert payload["orders"][1]["price"] == pytest.approx(109.99)
    assert payload["orders"][2]["orderType"] == "STP"
    assert payload["orders"][2]["auxPrice"] == pytest.approx(94.99)


def test_advanced_parent_coid_is_stable_safe_and_within_ibkr_limit(broker):
    parent = _order(
        identifier="not IBKR safe/" + "x" * 100,
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
    )

    first_payload = broker._get_order_data_for_submission(parent)
    second_payload = broker._get_order_data_for_submission(parent)
    c_oid = first_payload["orders"][0]["cOID"]

    assert c_oid == second_payload["orders"][0]["cOID"]
    assert len(c_oid) <= 64
    assert c_oid.startswith("lumibot-")
    assert first_payload["orders"][1]["parentId"] == c_oid


def test_oto_payload_preserves_explicit_child_settings(broker):
    child = _order(
        identifier="explicit-oto-child",
        quantity=2,
        side=Order.OrderSide.SELL,
        limit_price=101.25,
        order_type=Order.OrderType.LIMIT,
        time_in_force="day",
        exchange="ARCA",
    )
    parent = _order(
        identifier="oto-parent",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.OTO,
        child_orders=[child],
    )

    payload = broker._get_order_data_for_submission(parent)

    assert payload["orders"][0]["cOID"] == "oto-parent"
    assert payload["orders"][1] == {
        "conid": 265598,
        "quantity": 2,
        "orderType": "LMT",
        "side": "SELL",
        "tif": "DAY",
        "price": pytest.approx(101.24),
        "listingExchange": "ARCA",
        "parentId": "oto-parent",
    }
    assert "cOID" not in payload["orders"][1]


def test_oco_payload_excludes_conceptual_parent_and_marks_both_children(broker):
    parent = _order(
        identifier="oco-parent",
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )

    payload = broker._get_order_data_for_submission(parent)

    assert len(payload["orders"]) == 2
    assert [ticket["orderType"] for ticket in payload["orders"]] == ["LMT", "STP"]
    assert all(ticket["isSingleGroup"] is True for ticket in payload["orders"])
    assert all("cOID" not in ticket for ticket in payload["orders"])
    assert all("parentId" not in ticket for ticket in payload["orders"])
    assert all(ticket["listingExchange"] == "SMART" for ticket in payload["orders"])


@pytest.mark.parametrize("order_class", [Order.OrderClass.BRACKET, Order.OrderClass.OTO, Order.OrderClass.OCO])
def test_invalid_advanced_child_count_prevents_package_serialization(broker, order_class):
    if order_class is Order.OrderClass.BRACKET:
        parent = _order(
            limit_price=100.0,
            order_type=Order.OrderType.LIMIT,
            order_class=order_class,
            secondary_limit_price=110.0,
        )
        parent.child_orders = []
        expected_count = "one or two"
    elif order_class is Order.OrderClass.OTO:
        parent = _order(
            limit_price=100.0,
            order_type=Order.OrderType.LIMIT,
            order_class=order_class,
            secondary_limit_price=110.0,
        )
        parent.child_orders.append(parent.child_orders[0])
        expected_count = "exactly one"
    else:
        parent = _order(
            side=Order.OrderSide.SELL,
            limit_price=110.0,
            stop_price=95.0,
            order_class=order_class,
        )
        parent.child_orders.pop()
        expected_count = "exactly two"

    with pytest.raises(ValueError, match=expected_count):
        broker._get_order_data_for_submission(parent)

    broker.data_source.get_conid_from_asset.assert_not_called()


def test_unserializable_advanced_child_prevents_complete_package(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
    )
    broker.data_source.get_conid_from_asset.side_effect = [265598, None]

    with pytest.raises(ValueError, match="Unable to serialize IBKR REST bracket child ticket"):
        broker._get_order_data_for_submission(parent)

    broker.data_source.execute_order.assert_not_called()
