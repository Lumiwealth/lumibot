"""Selected-contract trading through the existing broker hooks.

Market/book fixture shapes were recorded from the demo API on 2026-10-26;
identifiers are replaced with neutral labels. External APIs alone are faked.
"""

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest

from lumibot.brokers import Kalshi
from lumibot.entities import Asset, Order
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient


@pytest.fixture
def market():
    return {
        "ticker": "SYNTHETIC-EVENT",
        "status": "active",
        "market_type": "binary",
        "exchange_index": 2,
        "notional_value_dollars": "1.0000",
        "last_price_dollars": "0.0000",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
    }


@pytest.fixture
def broker(market):
    client = Mock(spec=KalshiClient)

    def request(method, path, **kwargs):
        if path == "/markets/SYNTHETIC-EVENT":
            return {"market": market}
        if path.endswith("/orderbook"):
            return {"orderbook_fp": {"no_dollars": [["0.2300", "4.00"]], "yes_dollars": [["0.7400", "4.00"]]}}
        raise AssertionError((method, path, kwargs))

    client.request.side_effect = request
    client.path_id.side_effect = KalshiClient.path_id
    result = Kalshi(client=client, connect_stream=False)
    yield result
    result.close()


def contract(side):
    return Asset("SYNTHETIC-EVENT", "prediction_contract", outcome=side)


@pytest.mark.parametrize(
    "outcome,action,book,price",
    [
        ("yes", "buy", "bid", "0.2300"),
        ("yes", "sell", "ask", "0.2300"),
        ("no", "buy", "ask", "0.7700"),
        ("no", "sell", "bid", "0.7700"),
    ],
)
def test_four_directions_keep_original_contract(broker, outcome, action, book, price):
    order = Order("neutral", contract(outcome), Decimal("1.25"), action, limit_price=0.23, time_in_force="gtc")
    payload = broker._order_payload(order)
    assert (payload["side"], payload["price"], payload["count"]) == (book, price, "1.25")
    assert payload["exchange_index"] == 2
    assert "subaccount" not in payload
    assert (order.asset.outcome, order.side, order.limit_price) == (outcome, action, 0.23)


def test_no_quote_complements_and_swaps_book_sides(broker):
    quote = broker.get_quote(contract("no"))
    assert (quote.bid, quote.ask, quote.bid_size, quote.ask_size) == pytest.approx((0.23, 0.26, 4, 4))
    # A real zero trade/settlement price must not be treated as missing.
    assert broker.data_source.get_last_price(contract("yes")) == 0
    assert broker.data_source.get_last_price(contract("no")) == 1


def test_dynamic_ticks_and_provider_options_are_not_artificially_limited(broker, market):
    market.update(market_type="scalar", mve_collection_ticker="SYNTHETIC-COLLECTION")
    market["price_ranges"] = [{"start": "0", "end": "1", "step": "0.0001"}]
    order = Order(
        "neutral",
        contract("no"),
        0.25,
        "buy",
        limit_price=0.4321,
        time_in_force="ioc",
        custom_params={"reduce_only": True, "cancel_order_on_pause": True},
    )
    payload = broker._order_payload(order)
    assert payload["price"] == "0.5679"
    assert payload["reduce_only"] is True
    assert payload["cancel_order_on_pause"] is True


def test_provider_self_trade_option_is_configurable(broker):
    order = Order(
        "neutral",
        contract("yes"),
        1,
        "buy",
        limit_price=0.23,
        time_in_force="gtc",
        custom_params={"self_trade_prevention_type": "maker"},
    )
    assert broker._order_payload(order)["self_trade_prevention_type"] == "maker"


def test_caller_client_order_id_is_sent_and_checkpointed_once(broker, monkeypatch):
    order = Order("neutral", contract("yes"), .25, "buy", limit_price=.23,
                  custom_params={"client_order_id": "synthetic-owner-order"})
    monkeypatch.setattr(broker, "_refresh_after_mutation", lambda order: None)
    original = broker._client.request.side_effect

    def request(method, path, **kwargs):
        if method == "POST":
            assert kwargs["json"]["client_order_id"] == "synthetic-owner-order"
            return {**recorded("create_ack"), "client_order_id": kwargs["json"]["client_order_id"]}
        return original(method, path, **kwargs)

    broker._client.request.side_effect = request
    broker._submit_order(order)
    assert broker._state(order)["client_order_id"] == "synthetic-owner-order"
    assert Order.from_dict(order.to_dict()).custom_params["_kalshi"]["client_order_id"] == "synthetic-owner-order"
    with pytest.raises(ValueError, match="already been attempted"):
        broker._submit_order(order)
    assert sum(call.args[0] == "POST" for call in broker._client.request.call_args_list) == 1


@pytest.mark.parametrize("client_id", [None, "", " ", 42])
def test_invalid_client_order_ids_fail_before_submission(broker, client_id):
    order = Order("neutral", contract("yes"), 1, "buy", limit_price=.23,
                  custom_params={"client_order_id": client_id})
    with pytest.raises(ValueError, match="client_order_id must be a non-empty string"):
        broker._submit_order(order)
    assert not any(call.args[0] == "POST" for call in broker._client.request.call_args_list)


def test_market_order_uses_ioc_and_published_grid_extreme(broker):
    order = Order("neutral", contract("no"), 1, "buy", order_type="market")
    payload = broker._order_payload(order)
    assert payload["time_in_force"] == "immediate_or_cancel"
    assert (payload["side"], payload["price"]) == ("ask", "0.0100")
    assert order.order_type == "market"


def test_ambiguous_contract_is_rejected_before_any_trading_write(broker):
    order = Order("neutral", Asset("SYNTHETIC-EVENT", "prediction_contract"), 1, "buy", limit_price=0.2)
    with pytest.raises(ValueError, match="outcome"):
        broker.submit_order(order)
    assert all(call.args[0] == "GET" for call in broker._client.request.call_args_list)


def recorded(name):
    return json.loads((Path(__file__).parent / "fixtures/kalshi/trading_responses.json").read_text())[name]


def seed(broker, outcome="yes", action="buy", order_type="limit"):
    order = Order(
        "neutral",
        contract(outcome),
        1,
        action,
        order_type=order_type,
        limit_price=0.01 if order_type == "limit" else None,
        time_in_force="gtc",
    )
    order.identifier = "synthetic-order"
    order.update_raw(recorded("create_ack"))
    broker._state(order).update(client_order_id="synthetic-client", fill_count="0", fill_notional="0")
    broker._process_new_order(order)
    return order


def test_accepted_order_can_be_amended_during_recorded_read_lag(broker, monkeypatch):
    order = seed(broker, "no", "sell")
    monkeypatch.setattr(broker, "_pull_broker_order", lambda identifier: None)
    original = broker._client.request.side_effect

    def request(method, path, **kwargs):
        if path.endswith("/amend"):
            return {
                "order_id": "synthetic-order",
                "client_order_id": kwargs["json"]["updated_client_order_id"],
                "ts_ms": recorded("create_ack")["ts_ms"],
            }
        return original(method, path, **kwargs)

    broker._client.request.side_effect = request
    broker.modify_order(order, limit_price=0.98)
    sent = [c.kwargs["json"] for c in broker._client.request.call_args_list if c.args[0] == "POST"][0]
    assert (sent["side"], sent["price"], sent["count"]) == ("bid", "0.0200", "1.00")
    assert (order.asset.outcome, order.side) == ("no", "sell")


def test_read_lag_never_falsely_cancels_an_accepted_market_order(broker, monkeypatch):
    order = seed(broker, order_type="market")
    monkeypatch.setattr(broker, "_pull_broker_all_orders", lambda: [])
    monkeypatch.setattr(broker, "_pull_order", lambda *a: None)
    broker.sync_orders("neutral")
    assert order.is_active()


def test_read_observation_preserves_selected_no_and_serialized_checkpoint(broker):
    order = seed(broker, "no", "sell")
    order.limit_price = 0.99
    row = recorded("resting_order")
    broker._apply_snapshot(order, row)
    assert (order.asset.outcome, order.side, order.limit_price) == ("no", "sell", 0.99)
    restored = Order.from_dict(order.to_dict())
    assert restored.custom_params["_kalshi"] == order.custom_params["_kalshi"]
    with pytest.raises(ValueError, match="already been attempted"):
        broker.submit_order(restored)


def test_held_new_event_cannot_replace_original_no_intent(broker):
    broker._hold_trade_events = True
    original = broker._client.request.side_effect

    def request(method, path, **kwargs):
        if method == "POST":
            return recorded("create_ack")
        if path.startswith("/portfolio/orders/"):
            return {"order": recorded("resting_order")}
        return original(method, path, **kwargs)

    broker._client.request.side_effect = request
    order = Order("neutral", contract("no"), 1, "sell", limit_price=0.99, time_in_force="gtc")
    broker.submit_order(order)
    assert broker.get_tracked_order(order.identifier) is order
    assert order.asset.outcome == "no"


def test_partial_fills_are_deltas_and_callbacks_see_net_inventory(broker, monkeypatch):
    order = seed(broker, "no", "buy")
    order.quantity = 3
    row = recorded("no_order")
    row.update(initial_count_fp="3.00", remaining_count_fp="1.00", status="resting")
    fills = recorded("no_fills")
    positions = [recorded("yes_position")]
    # Synthetic quantities exercise the recorded API shapes; no database is faked.
    positions[0].update(position_fp="1.00", market_exposure_dollars="0.590000")

    def pages(path, key, **kwargs):
        return iter(fills if key == "fills" else positions)

    broker._client.pages.side_effect = pages
    callbacks = []
    monkeypatch.setattr(
        broker,
        "_on_partially_filled_order",
        lambda position, *args: callbacks.append(
            (
                position.asset.outcome,
                position.quantity,
                [(p.asset.outcome, p.quantity) for p in broker.get_tracked_positions("neutral")],
            )
        ),
    )
    broker._apply_snapshot(order, row)
    broker._apply_snapshot(order, row)
    assert callbacks == [("no", 0, [("yes", 1)])]
    assert sum(t.quantity for t in order.transactions) == 2
    assert order.avg_fill_price == 0.54
    assert order.trade_cost == 0.0348
    completion = Mock()
    monkeypatch.setattr(broker, "_on_filled_order", completion)
    extra = dict(
        fills[0],
        count_fp="1.00",
        yes_price_dollars="0.4700",
        fill_id="second-fill",
        trade_id="second-fill",
        fee_cost="0.010000",
    )
    fills.append(extra)
    positions[:] = [recorded("no_position")]
    row.update(fill_count_fp="3.00", remaining_count_fp="0.00", status="executed")
    broker._apply_snapshot(order, row)
    broker._apply_snapshot(order, row)
    assert completion.call_count == 1
    assert completion.call_args.args[0].asset.outcome == "no"
    assert completion.call_args.args[0].quantity == 1
    assert completion.call_args.args[2:4] == pytest.approx((0.53, 1))
    assert sum(t.quantity for t in order.transactions) == 3
    assert order.avg_fill_price == pytest.approx(1.61 / 3)
    assert order.trade_cost == 0.0448


def test_restored_fill_checkpoint_does_not_replay_fills(broker, monkeypatch):
    order = seed(broker, "yes", "buy")
    broker._new_orders.remove(order.identifier, key="identifier")
    order.quantity = 3
    order.avg_fill_price = 0.59
    order.status = Order.OrderStatus.FILLED
    broker._state(order).update(fill_count="3.00", fill_notional="1.770000")
    restored = Order.from_dict(order.to_dict())
    broker._filled_orders.append(restored)
    callback = Mock()
    monkeypatch.setattr(broker, "_on_filled_order", callback)
    broker._apply_snapshot(restored, recorded("yes_order"))
    callback.assert_not_called()
    broker._client.pages.assert_not_called()


def test_ambiguous_submit_reconciles_owned_terminal_fill_by_client_id(broker, monkeypatch):
    original = broker._client.request.side_effect

    def request(method, path, **kwargs):
        if method == "POST":
            raise KalshiAPIError("Kalshi request failed in transport; order outcome may be unknown")
        return original(method, path, **kwargs)

    broker._client.request.side_effect = request
    order = Order("neutral", contract("no"), 2, "buy", limit_price=0.54, time_in_force="ioc")
    with pytest.raises(KalshiAPIError):
        broker.submit_order(order)
    row = recorded("no_order")
    row["client_order_id"] = broker._state(order)["client_order_id"]

    def pages(path, key, **kwargs):
        return iter([row] if key == "orders" else recorded("no_fills") if key == "fills" else [recorded("no_position")])

    broker._client.pages.side_effect = pages
    broker._first_iteration = False
    callback = Mock()
    monkeypatch.setattr(broker, "_on_filled_order", callback)
    broker.sync_orders("neutral")
    assert broker.get_tracked_order(row["order_id"]) is order
    assert order.asset.outcome == "no" and order.side == "buy"
    assert order.is_filled()
    assert callback.call_count == 1
    assert len([c for c in broker._client.request.call_args_list if c.args[0] == "POST"]) == 1


@pytest.mark.parametrize("custom", [None, {"reduce_only": False}])
def test_standard_reduce_only_reaches_provider_without_legacy_override(broker, custom):
    order = Order("neutral", contract("no"), 5, "buy", limit_price=.23,
                  time_in_force="ioc", reduce_only=True, custom_params=custom)
    assert broker._order_payload(order)["reduce_only"] is True
    restored = Order.from_dict(order.to_dict())
    assert restored.reduce_only is True
    assert broker._order_payload(restored)["reduce_only"] is True


def test_standard_reduce_only_uses_the_existing_provider_time_in_force_rule(broker):
    order = Order("neutral", contract("yes"), 1, "sell", limit_price=.23, reduce_only=True)
    with pytest.raises(ValueError, match="reduce_only"):
        broker._order_payload(order)


def test_ordinary_order_exposes_false_reduce_only():
    assert Order("neutral", contract("yes"), 1, "buy").reduce_only is False


def test_direct_order_archive_fallback_filters_the_known_market(broker):
    order = Order("neutral", contract("yes"), 1, "buy", identifier="archived-order")
    broker._process_new_order(order)
    broker._client.request.side_effect = KalshiAPIError("Not found", status_code=404)
    row = json.loads((Path(__file__).parent / "fixtures/kalshi/trading_responses.json").read_text())["yes_order"].copy()
    row.update(order_id="archived-order", ticker=order.asset.symbol)
    broker._client.pages.return_value = iter([row])
    assert broker._pull_broker_order(order.identifier) == row
    assert broker._client.pages.call_args.kwargs["params"]["ticker"] == order.asset.symbol
