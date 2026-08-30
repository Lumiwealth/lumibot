from datetime import datetime, timezone
from unittest.mock import Mock, call

import pytest

import lumibot.brokers.interactive_brokers_rest as ibkr_rest_module
from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST
from lumibot.data_sources.interactive_brokers_rest_data import InteractiveBrokersRESTData
from lumibot.entities import Asset, Order
from lumibot.trading_builtins.safe_list import SafeList


@pytest.fixture
def broker(monkeypatch):
    monkeypatch.setattr(ibkr_rest_module, "colored", lambda text, *args, **kwargs: text)
    broker = InteractiveBrokersREST.__new__(InteractiveBrokersREST)
    broker.data_source = Mock()
    broker.data_source.get_conid_from_asset.return_value = 265598
    broker.data_source.get_contract_rules.return_value = {"rules": {"increment": 0.01}}
    broker.name = InteractiveBrokersREST.NAME
    broker.logger = Mock()
    broker.logger.isEnabledFor.return_value = False
    broker._log_order_status = Mock()
    broker._hold_trade_events = False
    broker._held_trades = []
    broker._subscribers = SafeList(None)
    broker._unprocessed_orders = SafeList(None)
    broker._placeholder_orders = SafeList(None)
    broker._new_orders = SafeList(None)
    broker._canceled_orders = SafeList(None)
    broker._partially_filled_orders = SafeList(None)
    broker._filled_orders = SafeList(None)
    broker._error_orders = SafeList(None)
    broker._tracked_orders_cache_key = None
    broker._tracked_orders_cache_value = []
    broker._tracked_orders_filter_cache = {}
    broker._active_tracked_orders_filter_cache = {}
    broker._on_new_order = Mock()
    broker._strategy_name = "ibkr_rest_order_semantics"
    broker._first_iteration = False
    broker.sync_positions = Mock()
    broker.data_source.get_contract_details.return_value = {"instrument_type": "STK"}

    broker.dispatched_events = []

    def dispatch_and_process(event, **kwargs):
        order = kwargs["order"]
        broker.dispatched_events.append((event, order, order.status))
        if event == broker.NEW_ORDER:
            processed_order = broker._process_new_order(order)
            if processed_order:
                broker._on_new_order(processed_order)
        elif event == broker.PLACEHOLDER_ORDER:
            broker._process_placeholder_order(order)
        elif event == broker.FILLED_ORDER:
            order.status = broker.FILLED_ORDER
            order.set_filled()
        elif event == broker.CANCELED_ORDER:
            order.status = broker.CANCELED_ORDER
            order.set_canceled()
        elif event == broker.ERROR_ORDER:
            order.set_error(kwargs["error_msg"])

    broker._safe_stream_dispatch = Mock(side_effect=dispatch_and_process)
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


def _poll_order(order_id, status="Open", avg_price=None):
    response = {
        "orderId": order_id,
        "secType": "STK",
        "totalSize": 1,
        "conid": 265598,
        "ticker": "SPY",
        "cashCcy": "USD",
        "timeInForce": "GTC",
        "status": status,
        "side": "BUY",
        "price": 100.0,
    }
    if avg_price is not None:
        response["avgPrice"] = avg_price
    return response


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
    assert [ticket["cOID"] for ticket in payload["orders"]] == [
        child.identifier for child in parent.child_orders
    ]
    assert len({ticket["cOID"] for ticket in payload["orders"]}) == 2
    assert all("parentId" not in ticket for ticket in payload["orders"])
    assert all(ticket["listingExchange"] == "SMART" for ticket in payload["orders"])


def test_oco_duplicate_child_coids_fail_before_contract_lookup(broker):
    parent = _order(
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )
    parent.child_orders[1].identifier = parent.child_orders[0].identifier

    with pytest.raises(ValueError, match="distinct identifiers for cOID correlation"):
        broker._get_order_data_for_submission(parent)

    broker.data_source.get_conid_from_asset.assert_not_called()


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


def test_submit_simple_maps_raw_acknowledgement_and_tracks_once(broker):
    order = _order(
        identifier="local-simple",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
    )
    response = [{"order_id": 1001, "order_status": "Submitted"}]
    broker.data_source.execute_order.return_value = response

    result = broker._submit_order(order)

    assert result is order
    assert order.identifier == "1001"
    assert order._raw is response[0]
    assert order.was_transmitted()
    assert order in broker._new_orders
    assert order not in broker._unprocessed_orders
    assert order._new_event.is_set()
    assert broker.dispatched_events == [(broker.NEW_ORDER, order, Order.OrderStatus.SUBMITTED)]
    broker._on_new_order.assert_called_once_with(order)
    broker.data_source.execute_order.assert_called_once()
    assert broker.data_source.execute_order.call_args.kwargs == {"return_raw_response": True}


def test_execute_order_can_return_complete_mixed_response_without_network():
    data_source = InteractiveBrokersRESTData.__new__(InteractiveBrokersRESTData)
    data_source.base_url = "https://example.invalid"
    data_source.account_id = "test-account"
    data_source.ping_iserver = Mock()
    response = [{"error": "rejected"}, {"order_id": "1002"}]
    data_source.post_to_endpoint = Mock(return_value=response)

    result = data_source.execute_order({"orders": [{}, {}]}, return_raw_response=True)

    assert result is response
    data_source.ping_iserver.assert_called_once_with()
    data_source.post_to_endpoint.assert_called_once_with(
        "https://example.invalid/iserver/account/test-account/orders",
        {"orders": [{}, {}]},
        description="Executing order",
    )


def test_submit_bracket_maps_and_tracks_every_native_leg(broker):
    parent = _order(
        identifier="local-bracket",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    children = list(parent.child_orders)
    response = [
        {"order_id": 2001, "order_status": "Submitted"},
        {"order_id": "2002", "order_status": "Submitted"},
        {"order_id": 2003, "order_status": "Submitted"},
    ]
    broker.data_source.execute_order.return_value = response

    broker._submit_order(parent)

    native_orders = [parent, *children]
    assert parent.child_orders == children
    assert [native.identifier for native in native_orders] == ["2001", "2002", "2003"]
    assert [native._raw for native in native_orders] == response
    assert all(native.was_transmitted() for native in native_orders)
    assert all(child.parent_identifier == "2001" for child in children)
    assert all(native in broker._new_orders for native in native_orders)
    assert all(native not in broker._unprocessed_orders for native in native_orders)
    assert all(native._new_event.is_set() for native in native_orders)
    assert [event for event, _, _ in broker.dispatched_events] == [broker.NEW_ORDER] * 3
    assert [status for _, _, status in broker.dispatched_events] == [Order.OrderStatus.SUBMITTED] * 3
    assert [args.args[0] for args in broker._on_new_order.call_args_list] == native_orders


def test_submit_oto_maps_parent_and_child_in_request_order(broker):
    parent = _order(
        identifier="local-oto",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.OTO,
        secondary_limit_price=110.0,
    )
    child = parent.child_orders[0]
    response = [
        {"order_id": "3001", "order_status": "Submitted"},
        {"order_id": 3002, "order_status": "Submitted"},
    ]
    broker.data_source.execute_order.return_value = response

    broker._submit_order(parent)

    assert parent.identifier == "3001"
    assert child.identifier == "3002"
    assert child.parent_identifier == "3001"
    assert parent._raw is response[0]
    assert child._raw is response[1]
    assert parent in broker._new_orders
    assert child in broker._new_orders
    assert len(broker._unprocessed_orders) == 0
    assert broker._on_new_order.call_count == 2


def test_submit_oco_keeps_local_parent_placeholder_and_tracks_native_children(broker):
    parent = _order(
        identifier="local-oco",
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )
    children = list(parent.child_orders)
    child_coids = [child.identifier for child in children]
    response = [
        {
            "order_id": "4002",
            "order_status": "Submitted",
            "local_order_id": child_coids[1],
        },
        {"order_id": 4001, "order_status": "Submitted"},
    ]
    broker.data_source.execute_order.return_value = response

    broker._submit_order(parent)

    assert parent.identifier == "local-oco"
    assert not parent.was_transmitted()
    assert parent in broker._placeholder_orders
    assert parent not in broker._new_orders
    assert [child.identifier for child in children] == ["4001", "4002"]
    assert [child._raw for child in children] == [response[1], response[0]]
    assert all(child.parent_identifier == "local-oco" for child in children)
    assert all(child in broker._new_orders for child in children)
    assert all(child not in broker._unprocessed_orders for child in children)
    assert parent._new_event.is_set()
    assert [event for event, _, _ in broker.dispatched_events] == [
        broker.PLACEHOLDER_ORDER,
        broker.NEW_ORDER,
        broker.NEW_ORDER,
    ]
    assert [args.args[0] for args in broker._on_new_order.call_args_list] == children


def test_submit_oco_rejects_ambiguous_acknowledgement_order_and_cleans_up(broker):
    parent = _order(
        identifier="ambiguous-oco",
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )
    broker.data_source.execute_order.return_value = [
        {"order_id": "4011", "order_status": "Submitted"},
        {"order_id": "4012", "order_status": "Submitted"},
    ]

    broker._submit_order(parent)

    cleanup_orders = [args.args[0] for args in broker.data_source.delete_order.call_args_list]
    assert [cleanup.identifier for cleanup in cleanup_orders] == ["4011", "4012"]
    assert parent.status == Order.OrderStatus.ERROR
    assert all(child.status == Order.OrderStatus.ERROR for child in parent.child_orders)
    assert "does not identify enough OCO acknowledgements" in parent.error_message
    assert not broker.get_all_orders()


@pytest.mark.parametrize(
    ("response", "cleanup_ids"),
    [
        ([{"order_id": "5001"}, "malformed-entry"], ["5001"]),
        ([{"order_id": "5001"}], ["5001"]),
        (
            [{"order_id": "5001"}, {"order_id": "5002"}, {"order_id": "5003"}],
            ["5001", "5002", "5003"],
        ),
        (
            [{"order_id": "5001"}, {"order_id": "5002", "error": "rejected"}],
            ["5001", "5002"],
        ),
        ([{"error": "rejected"}, {"order_id": "5002"}], ["5002"]),
        ([{"order_id": "-1"}, {"order_id": "5002"}], ["5002"]),
    ],
    ids=["malformed", "short", "long", "mixed", "mixed-first-error", "placeholder"],
)
def test_invalid_acknowledgement_package_cleans_every_acknowledged_id(
    broker,
    response,
    cleanup_ids,
):
    parent = _order(
        identifier="failed-bracket",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
    )
    original_identifiers = [parent.identifier, parent.child_orders[0].identifier]
    broker.data_source.execute_order.return_value = response

    broker._submit_order(parent)

    cleanup_orders = [args.args[0] for args in broker.data_source.delete_order.call_args_list]
    assert [cleanup.identifier for cleanup in cleanup_orders] == cleanup_ids
    assert [parent.identifier, parent.child_orders[0].identifier] == original_identifiers
    assert parent.status == Order.OrderStatus.ERROR
    assert parent.child_orders[0].status == Order.OrderStatus.ERROR
    assert not parent.was_transmitted()
    assert not parent.child_orders[0].was_transmitted()
    assert "Invalid IBKR REST order acknowledgement package" in parent.error_message
    assert len(broker._unprocessed_orders) == 0
    assert len(broker._new_orders) == 0
    assert [event for event, _, _ in broker.dispatched_events] == [broker.ERROR_ORDER]
    broker._on_new_order.assert_not_called()


def test_cleanup_continues_after_an_earlier_cancel_fails(broker):
    parent = _order(
        identifier="cleanup-bracket",
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
    )
    broker.data_source.execute_order.return_value = [
        {"order_id": "6001"},
        {"order_id": "6002"},
        {"order_id": "6003"},
    ]
    broker.data_source.delete_order.side_effect = [RuntimeError("first cleanup failed"), None, None]

    broker._submit_order(parent)

    cleanup_orders = [args.args[0] for args in broker.data_source.delete_order.call_args_list]
    assert [cleanup.identifier for cleanup in cleanup_orders] == ["6001", "6002", "6003"]
    assert parent.status == Order.OrderStatus.ERROR
    assert broker._on_new_order.call_count == 0


def _set_broker_id(order, order_id):
    order.identifier = str(order_id)
    order.update_raw({"order_id": str(order_id)})


def test_cancel_simple_order_contacts_ibkr(broker):
    order = _order(limit_price=100.0, order_type=Order.OrderType.LIMIT)
    _set_broker_id(order, 7001)

    broker.cancel_order(order)

    broker.data_source.delete_order.assert_called_once_with(order)


def test_cancel_bracket_parent_attempts_parent_and_every_child(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    native_orders = [parent, *parent.child_orders]
    for order_id, native_order in zip((7101, 7102, 7103), native_orders):
        _set_broker_id(native_order, order_id)

    broker.cancel_order(parent)

    assert [args.args[0] for args in broker.data_source.delete_order.call_args_list] == native_orders


def test_cancel_oto_parent_attempts_parent_and_child(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.OTO,
        secondary_limit_price=110.0,
    )
    native_orders = [parent, *parent.child_orders]
    for order_id, native_order in zip((7201, 7202), native_orders):
        _set_broker_id(native_order, order_id)

    broker.cancel_order(parent)

    assert [args.args[0] for args in broker.data_source.delete_order.call_args_list] == native_orders


def test_cancel_oco_parent_excludes_local_container_id(broker):
    parent = _order(
        identifier="7300",
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )
    for order_id, child in zip((7301, 7302), parent.child_orders):
        _set_broker_id(child, order_id)

    broker.cancel_order(parent)

    canceled_orders = [args.args[0] for args in broker.data_source.delete_order.call_args_list]
    assert canceled_orders == parent.child_orders
    assert all(canceled.identifier != parent.identifier for canceled in canceled_orders)


def test_canceling_advanced_child_only_contacts_ibkr_for_that_child(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    _set_broker_id(parent, 7401)
    _set_broker_id(parent.child_orders[0], 7402)
    _set_broker_id(parent.child_orders[1], 7403)

    broker.cancel_order(parent.child_orders[1])

    broker.data_source.delete_order.assert_called_once_with(parent.child_orders[1])


def test_cancel_deduplicates_broker_ids(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    _set_broker_id(parent, 7501)
    _set_broker_id(parent.child_orders[0], 7501)
    _set_broker_id(parent.child_orders[1], 7502)

    broker.cancel_order(parent)

    canceled_orders = [args.args[0] for args in broker.data_source.delete_order.call_args_list]
    assert [canceled.identifier for canceled in canceled_orders] == ["7501", "7502"]


@pytest.mark.parametrize(
    "terminal_status",
    [
        Order.OrderStatus.CANCELLING,
        Order.OrderStatus.CANCELED,
        Order.OrderStatus.FILLED,
        Order.OrderStatus.ERROR,
    ],
)
def test_cancel_contacts_ibkr_regardless_of_terminal_local_status(broker, terminal_status):
    order = _order(limit_price=100.0, order_type=Order.OrderType.LIMIT)
    _set_broker_id(order, 7601)
    order.status = terminal_status

    broker.cancel_order(order)

    broker.data_source.delete_order.assert_called_once_with(order)


def test_cancel_skips_empty_local_and_malformed_identifiers(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    _set_broker_id(parent, 7701)
    parent.child_orders[0].identifier = ""
    parent.child_orders[1].identifier = "local-child-id"

    broker.cancel_order(parent)

    broker.data_source.delete_order.assert_called_once_with(parent)


def test_cancel_continues_after_exception_and_rejection(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    native_orders = [parent, *parent.child_orders]
    for order_id, native_order in zip((7801, 7802, 7803), native_orders):
        _set_broker_id(native_order, order_id)
    broker.data_source.delete_order.side_effect = [RuntimeError("cancel failed"), False, True]

    broker.cancel_order(parent)

    assert [args.args[0] for args in broker.data_source.delete_order.call_args_list] == native_orders


@pytest.mark.parametrize(
    ("submission_order_id", "poll_order_id"),
    [("8101", 8101), (8102, "8102")],
)
def test_polling_matches_simple_order_despite_order_id_type_changes(
    broker,
    submission_order_id,
    poll_order_id,
):
    order = _order(limit_price=100.0, order_type=Order.OrderType.LIMIT)
    broker.data_source.execute_order.return_value = [{"order_id": submission_order_id}]
    broker._submit_order(order)
    raw_order = _poll_order(poll_order_id, status="Open")
    broker.data_source.get_broker_all_orders.return_value = [raw_order]

    broker.do_polling()

    assert order.identifier == str(submission_order_id)
    assert order._raw is raw_order
    assert order.status == Order.OrderStatus.OPEN
    assert broker.get_all_orders() == [order]
    broker.sync_positions.assert_called_once_with(None)


def test_polling_updates_bracket_native_orders_without_erasing_child_relationships(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    children = list(parent.child_orders)
    broker.data_source.execute_order.return_value = [
        {"order_id": "8201"},
        {"order_id": "8202"},
        {"order_id": "8203"},
    ]
    broker._submit_order(parent)
    raw_orders = [_poll_order(8201), _poll_order(8202), _poll_order(8203)]
    broker.data_source.get_broker_all_orders.return_value = raw_orders

    broker.do_polling()

    assert parent.child_orders == children
    assert [child.parent_identifier for child in children] == ["8201", "8201"]
    assert [parent._raw, *(child._raw for child in children)] == raw_orders
    assert set(broker.get_all_orders()) == {parent, *children}


def test_polling_updates_oto_parent_and_child_in_place(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.OTO,
        secondary_limit_price=110.0,
    )
    child = parent.child_orders[0]
    broker.data_source.execute_order.return_value = [
        {"order_id": 8301},
        {"order_id": 8302},
    ]
    broker._submit_order(parent)
    raw_orders = [_poll_order("8301"), _poll_order("8302")]
    broker.data_source.get_broker_all_orders.return_value = raw_orders

    broker.do_polling()

    assert parent.child_orders == [child]
    assert child.parent_identifier == "8301"
    assert parent._raw is raw_orders[0]
    assert child._raw is raw_orders[1]
    assert set(broker.get_all_orders()) == {parent, child}


def test_polling_keeps_oco_placeholder_connected_to_known_children(broker):
    parent = _order(
        identifier="local-oco-container",
        side=Order.OrderSide.SELL,
        limit_price=110.0,
        stop_price=95.0,
        order_class=Order.OrderClass.OCO,
    )
    children = list(parent.child_orders)
    child_coids = [child.identifier for child in children]
    broker.data_source.execute_order.return_value = [
        {"order_id": "8401", "local_order_id": child_coids[0]},
        {"order_id": "8402", "local_order_id": child_coids[1]},
    ]
    broker._submit_order(parent)
    raw_orders = [_poll_order(8401), _poll_order(8402)]
    broker.data_source.get_broker_all_orders.return_value = raw_orders

    broker.do_polling()

    assert parent.identifier == "local-oco-container"
    assert parent.child_orders == children
    assert [child.parent_identifier for child in children] == [parent.identifier, parent.identifier]
    assert [child._raw for child in children] == raw_orders
    assert parent in broker._placeholder_orders
    assert set(broker.get_all_orders()) == {parent, *children}


def test_polling_dispatches_filled_and_canceled_child_status_changes(broker):
    parent = _order(
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=95.0,
    )
    first_child, second_child = parent.child_orders
    broker.data_source.execute_order.return_value = [
        {"order_id": "8501"},
        {"order_id": "8502"},
        {"order_id": "8503"},
    ]
    broker._submit_order(parent)
    broker.dispatched_events.clear()
    raw_orders = [
        _poll_order(8501, status="Open"),
        _poll_order(8502, status="Filled", avg_price=110.0),
        _poll_order(8503, status="Canceled"),
    ]
    broker.data_source.get_broker_all_orders.return_value = raw_orders

    broker.do_polling()

    assert first_child.status == Order.OrderStatus.FILLED
    assert second_child.status == Order.OrderStatus.CANCELED
    assert first_child._raw is raw_orders[1]
    assert second_child._raw is raw_orders[2]
    assert [event for event, _, _ in broker.dispatched_events] == [
        broker.FILLED_ORDER,
        broker.CANCELED_ORDER,
    ]


def _gtd_date():
    return datetime(2026, 9, 1, 15, 30, tzinfo=timezone.utc)


def test_rest_simple_gtd_fails_before_conid_or_execute_order(broker):
    order = _order(
        time_in_force="gtd",
        good_till_date=_gtd_date(),
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
    )

    with pytest.raises(NotImplementedError, match="exact-date GTD submission is not supported"):
        broker._submit_order(order)

    broker.data_source.get_conid_from_asset.assert_not_called()
    broker.data_source.get_contract_rules.assert_not_called()
    broker.data_source.execute_order.assert_not_called()


@pytest.mark.parametrize(
    "order_class, order_kwargs",
    [
        (Order.OrderClass.BRACKET, {"limit_price": 100.0, "secondary_limit_price": 110.0}),
        (Order.OrderClass.OTO, {"limit_price": 100.0, "secondary_limit_price": 110.0}),
        (Order.OrderClass.OCO, {"side": Order.OrderSide.SELL, "limit_price": 110.0, "stop_price": 95.0}),
    ],
)
def test_rest_advanced_parent_gtd_fails_before_package_serialization(broker, order_class, order_kwargs):
    parent = _order(
        order_class=order_class,
        time_in_force="gtd",
        good_till_date=_gtd_date(),
        **order_kwargs,
    )

    with pytest.raises(NotImplementedError, match="exact-date GTD submission is not supported"):
        broker._submit_order(parent)

    broker.data_source.get_conid_from_asset.assert_not_called()
    broker.data_source.execute_order.assert_not_called()


@pytest.mark.parametrize(
    "order_class, order_kwargs",
    [
        (Order.OrderClass.BRACKET, {"limit_price": 100.0, "secondary_limit_price": 110.0}),
        (Order.OrderClass.OTO, {"limit_price": 100.0, "secondary_limit_price": 110.0}),
        (Order.OrderClass.OCO, {"side": Order.OrderSide.SELL, "limit_price": 110.0, "stop_price": 95.0}),
    ],
)
def test_rest_advanced_child_gtd_fails_before_package_serialization(broker, order_class, order_kwargs):
    parent = _order(order_class=order_class, **order_kwargs)
    child = parent.child_orders[0]
    child.time_in_force = "gtd"
    child.good_till_date = _gtd_date()

    with pytest.raises(NotImplementedError, match="exact-date GTD submission is not supported"):
        broker._submit_order(parent)

    broker.data_source.get_conid_from_asset.assert_not_called()
    broker.data_source.execute_order.assert_not_called()


def test_rest_multileg_gtd_fails_before_conid_or_execute_order(broker):
    leg = _order(time_in_force="gtd", good_till_date=_gtd_date())

    with pytest.raises(NotImplementedError, match="exact-date GTD submission is not supported"):
        broker._submit_orders([leg], is_multileg=True)

    broker.data_source.get_conid_from_asset.assert_not_called()
    broker.data_source.execute_order.assert_not_called()


@pytest.mark.parametrize("order_class", [Order.OrderClass.SIMPLE, Order.OrderClass.BRACKET])
def test_rest_good_till_date_without_gtd_is_rejected(order_class, broker):
    order_kwargs = {
        "time_in_force": "gtc",
        "good_till_date": _gtd_date(),
        "limit_price": 100.0,
        "order_type": Order.OrderType.LIMIT,
    }
    if order_class is Order.OrderClass.BRACKET:
        order_kwargs.update(order_class=order_class, secondary_limit_price=110.0)
    order = _order(**order_kwargs)

    with pytest.raises(ValueError, match="good_till_date requires time_in_force='gtd'"):
        broker._submit_order(order)

    broker.data_source.get_conid_from_asset.assert_not_called()
    broker.data_source.execute_order.assert_not_called()


@pytest.mark.parametrize("time_in_force", ["day", "gtc"])
def test_rest_supported_time_in_force_payload_remains_unchanged(broker, time_in_force):
    order = _order(
        time_in_force=time_in_force,
        limit_price=100.0,
        order_type=Order.OrderType.LIMIT,
    )

    payload = broker._get_order_data_for_submission(order)

    assert payload["orders"][0]["tif"] == time_in_force.upper()
    assert "goodTillDate" not in payload["orders"][0]
