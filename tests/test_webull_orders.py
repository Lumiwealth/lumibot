"""Offline Webull v3 shapes from the individual order and account references."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers import Webull
from lumibot.entities import Asset, Order


def reply(data):
    return SimpleNamespace(json=lambda: data, raise_for_status=lambda: None)


@pytest.fixture
def venue():
    client = Mock()
    client.account_v2.get_account_list.return_value = reply([{"account_id": "sandbox-test"}])
    client.account_v2.get_account_balance.return_value = reply({
        "total_asset_currency": "USD",
        "total_cash_balance": "12000.50",
        "total_market_value": "250",
        "total_net_liquidation_value": "12250.50",
        "account_currency_assets": [{"currency": "USD", "buying_power": "23000"}],
    })
    client.order_v3.place_order.side_effect = lambda account, rows, **kwargs: reply({
        "client_order_id": rows[0]["client_order_id"], "order_id": "broker-generated-order",
    })
    client.order_v3.replace_order.return_value = reply({"order_id": "broker-generated-order"})
    client.order_v3.cancel_order.return_value = reply({})
    return client


@pytest.fixture
def broker(venue):
    return Webull({"ACCOUNT_ID": "sandbox-test"}, trading_client=venue,
                  connect_stream=False, start_orders_thread=False)


def order():
    return Order("qualification", Asset("SAMPLE"), 1, "buy", limit_price=20.50,
                 order_type="limit", time_in_force="day")


def detail(client_id, **updates):
    return {"combo_type": "NORMAL", "client_order_id": client_id, "orders": [{
        "client_order_id": client_id, "order_id": "broker-generated-order", "symbol": "SAMPLE",
        "instrument_type": "EQUITY", "side": "BUY", "total_quantity": "1", "order_type": "LIMIT",
        "time_in_force": "DAY", "status": "SUBMITTED", "limit_price": "20.50",
        "filled_quantity": "0", **updates,
    }]}


def test_stock_limit_lifecycle_preserves_native_id_and_waits_for_readback(broker, venue):
    placed = broker.submit_order(order())
    client_id = placed.identifier
    assert client_id != "broker-generated-order"
    _, payload = venue.order_v3.place_order.call_args.args
    assert payload == [{
        "client_order_id": client_id, "combo_type": "NORMAL", "instrument_type": "EQUITY",
        "market": "US", "symbol": "SAMPLE", "side": "BUY", "entrust_type": "QTY",
        "quantity": "1", "order_type": "LIMIT", "limit_price": "20.5", "time_in_force": "DAY",
        "support_trading_session": "CORE",
    }]
    venue.order_v3.get_order_detail.return_value = reply(detail(client_id))
    observed = broker._pull_order(client_id, "qualification")
    assert observed.limit_price == 20.50
    assert observed._raw["order_id"] == "broker-generated-order"
    broker._modify_order(placed, limit_price=20.60)
    assert venue.order_v3.replace_order.call_args.args[1][0]["limit_price"] == "20.6"
    assert placed.limit_price == 20.50
    venue.order_v3.get_order_detail.return_value = reply(detail(client_id, limit_price="20.60"))
    assert broker._pull_order(client_id, "qualification").limit_price == 20.60
    broker.cancel_order(placed)
    venue.order_v3.cancel_order.assert_called_once_with("sandbox-test", client_id)
    assert not placed.is_canceled()
    venue.order_v3.get_order_detail.return_value = reply(detail(client_id, status="CANCELLED"))
    assert broker._pull_order(client_id, "qualification").is_canceled()


def test_account_values_distinguish_zero_and_missing(broker, venue):
    assert broker._get_balances_at_broker(Asset("USD", asset_type="forex"), None) == (
        12000.50, 250, 12250.50
    )
    venue.account_v2.get_account_balance.return_value = reply({
        "total_asset_currency": "USD", "total_cash_balance": "0", "total_market_value": "0",
        "total_net_liquidation_value": "0",
    })
    assert broker._get_balances_at_broker(None, None) == (0, 0, 0)
    venue.account_v2.get_account_balance.return_value = reply({
        "total_asset_currency": "USD", "total_cash_balance": "", "total_market_value": "0",
        "total_net_liquidation_value": "",
    })
    with pytest.raises(ValueError, match="total_cash_balance"):
        broker._get_balances_at_broker(None, None)


def test_wrong_account_cannot_send_an_order(venue):
    other = Webull({"ACCOUNT_ID": "unlisted"}, trading_client=venue,
                   connect_stream=False, start_orders_thread=False)
    with pytest.raises(ValueError, match="account"):
        other.submit_order(order())
    venue.order_v3.place_order.assert_not_called()


def test_ambiguous_placement_reads_once_with_original_id(broker, venue):
    placed = order()
    venue.order_v3.place_order.side_effect = TimeoutError("synthetic message")
    venue.order_v3.get_order_detail.side_effect = lambda account, client_id: reply(detail(client_id))
    assert broker.submit_order(placed) is placed
    venue.order_v3.place_order.assert_called_once()
    venue.order_v3.get_order_detail.assert_called_once_with("sandbox-test", placed.identifier)


def test_open_orders_follows_opaque_cursor(broker, venue):
    venue.order_v3.list_order_open.side_effect = [
        reply({"data": [detail("client-one")], "pagination_key": "opaque"}),
        reply({"data": [detail("client-two")]}),
    ]
    assert [row["client_order_id"] for row in broker._pull_broker_all_orders()] == ["client-one", "client-two"]
    assert venue.order_v3.list_order_open.call_args.kwargs == {"pagination_key": "opaque"}


def test_equity_bracket_is_a_single_atomic_wire_request(broker, venue):
    entry = Order("qualification", Asset("SAMPLE"), 1, "buy", limit_price=20,
                  order_type="limit", order_class="bracket", secondary_limit_price=25,
                  secondary_stop_price=17, custom_params={"support_trading_session": "CORE"})
    assert broker.submit_order(entry) is entry
    account, legs = venue.order_v3.place_order.call_args.args
    assert account == "sandbox-test"
    assert [leg["combo_type"] for leg in legs] == ["MASTER", "OTOCO", "OTOCO"]
    assert [leg["side"] for leg in legs] == ["BUY", "SELL", "SELL"]
    assert [leg["order_type"] for leg in legs] == ["LIMIT", "LIMIT", "STOP_LOSS"]
    assert len({leg["client_order_id"] for leg in legs}) == 3
    assert legs[2]["stop_price"] == "17"
    assert venue.order_v3.place_order.call_args.kwargs["client_combo_order_id"]
    venue.order_v3.place_order.assert_called_once()


def test_native_option_vertical_transmits_one_package(broker, venue):
    from datetime import date

    expiry = date(2027, 1, 15)
    options = [Asset("SAMPLE", asset_type="option", right="CALL", strike=strike, expiration=expiry)
               for strike in (20, 25)]
    legs = [Order("qualification", asset, 2, side, order_type="market")
            for asset, side in zip(options, ("buy_to_open", "sell_to_open"))]
    orders = broker.submit_orders(legs, is_multileg=True, order_type="debit", duration="day", price="1.25")
    assert len(orders) == 1 and orders[0].quantity == 2
    venue.order_v3.place_order.assert_called_once()
    row, = venue.order_v3.place_order.call_args.args[1]
    assert row["option_strategy"] == "VERTICAL"
    assert row["side"] == "BUY"
    assert [row["legs"][0]["quantity"], row["legs"][1]["quantity"]] == ["2", "2"]


def test_http_reconciliation_does_not_apply_same_partial_twice(broker, venue):
    resting = Order("qualification", Asset("SAMPLE"), 3, "buy", limit_price=20.50, order_type="limit")
    broker.submit_order(resting)
    identifier = resting.identifier
    venue.order_v3.get_order_detail.return_value = reply(detail(
        identifier, status="PARTIALLY_FILLED", total_quantity="3", filled_quantity="1", filled_price="20.20"
    ))
    broker._refresh_tracked_order(resting)
    broker._refresh_tracked_order(resting)
    assert len(resting.transactions) == 1
    assert resting.transactions[0].quantity == 1
    venue.order_v3.get_order_detail.return_value = reply(detail(
        identifier, status="CANCELLED", total_quantity="3", filled_quantity="2", filled_price="20.25"
    ))
    broker._refresh_tracked_order(resting)
    assert resting.is_canceled()
    assert [round(float(t.price), 2) for t in resting.transactions] == [20.20, 20.30]
    broker._refresh_tracked_order(resting)
    assert sum(t.quantity for t in resting.transactions) == 2


def test_futures_cash_and_net_liquidation_keep_the_mark_to_market_difference(venue):
    venue.account_v2.get_account_list.return_value = reply([
        {"account_id": "sandbox-test", "account_class": "FUTURES"}
    ])
    venue.account_v2.get_account_balance.return_value = reply({
        "total_asset_currency": "USD",
        "total_cash_balance": "10000",
        "total_net_liquidation_value": "9987.5",
        "account_currency_assets": [{"currency": "USD", "buying_power": "8000"}],
    })
    broker = Webull({"ACCOUNT_ID": "sandbox-test"}, trading_client=venue,
                    connect_stream=False, start_orders_thread=False)
    assert broker._get_balances_at_broker(Asset("USD", "forex"), None) == (10000, -12.5, 9987.5)
    assert broker.get_buying_power() == 8000
