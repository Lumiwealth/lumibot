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


@pytest.mark.parametrize(("account", "paper"), [("another-test-account", True), ("sandbox-test", False)])
def test_restart_rejects_wrong_webull_account_or_environment(broker, venue, account, paper):
    broker.submit_order(order())
    checkpoint = broker.export_order_state()
    other = Webull({"ACCOUNT_ID": account, "PAPER": paper}, trading_client=venue,
                   connect_stream=False, start_orders_thread=False)
    with pytest.raises(ValueError, match="different account"):
        other.restore_order_state(checkpoint)
    assert not other.get_tracked_orders()


def test_option_qualification_selects_listed_expiry_inside_weekend_query():
    from datetime import date
    from decimal import Decimal
    from tests.test_webull_paper_apitest import _select_vertical_series

    def series(expiry, strike):
        return {"expiration_date": expiry, "strike_price": strike, "multiplier": "100"}

    rows = [series("2026-11-20", "100"), series("2026-11-30", "101"),
            series("2026-12-04", "100"), series("2026-12-04", "100.0"),
            series("2026-12-04", "105"), series("2026-12-14", "110")]
    selected = _select_vertical_series(rows, date(2026, 11, 28), date(2026, 12, 12), Decimal("101"))
    assert [(r["expiration_date"], Decimal(r["strike_price"])) for r in selected] == [
        ("2026-12-04", Decimal("100")), ("2026-12-04", Decimal("105"))]


@pytest.mark.parametrize(("side", "native"), [("sell_short", "SHORT"), ("buy_to_cover", "BUY")])
def test_equity_short_and_cover_use_webull_sides(broker, venue, side, native):
    value = Order("qualification", Asset("SAMPLE"), 1, side, limit_price=20, order_type="limit")
    assert broker._order_row(value)["side"] == native
    assert broker._order_row(value, combo_type="MASTER")["side"] == native
    if side == "sell_short":
        assert broker._parse_broker_order(detail("test", side=native), "qualification").side == side


@pytest.mark.parametrize(("params", "kind", "amount"), [
    ({"trail_percent": 0.025}, "PERCENTAGE", "0.025"), ({"trail_price": 1.5}, "AMOUNT", "1.5")
])
def test_equity_trail_keeps_same_units_when_placed_and_observed(broker, params, kind, amount):
    from decimal import Decimal
    value = Order("qualification", Asset("SAMPLE"), 1, "sell", time_in_force="day", **params)
    wire = broker._order_row(value)
    assert wire["trailing_type"] == kind and Decimal(wire["trailing_stop_step"]) == Decimal(amount)
    observed = broker._parse_broker_order(detail("test", order_type="TRAILING_STOP_LOSS", limit_price=None,
                                         trailing_type=kind, trailing_stop_step=amount), "qualification")
    assert getattr(observed, next(iter(params))) == pytest.approx(float(amount))


def test_crypto_fill_detail_reconstructs_the_quote_for_checkpoint_recovery(broker, venue):
    from decimal import Decimal
    resting = Order("qualification", Asset("BTC", "crypto"), Decimal("0.0001"), "buy",
                    quote=Asset("USD", "forex"), order_type="market", time_in_force="ioc")
    broker.submit_order(resting)
    checkpoint = broker.export_order_state(resting.strategy)
    recovered = Webull({"ACCOUNT_ID": "sandbox-test"}, trading_client=venue,
                       connect_stream=False, start_orders_thread=False)
    recovered.restore_order_state(checkpoint)
    venue.order_v3.get_order_detail.return_value = reply(detail(
        resting.identifier, instrument_type="CRYPTO", symbol="BTCUSD", currency="USD", order_type="MARKET",
        time_in_force="IOC", status="FILLED", total_quantity="0.0001", filled_quantity="0.0001",
        filled_price="75000.25", limit_price=None))
    order_from_checkpoint = recovered.get_tracked_order(resting.identifier)
    recovered._poll_tracked_orders()
    recovered._refresh_tracked_order(order_from_checkpoint)
    assert order_from_checkpoint.is_filled()
    assert order_from_checkpoint.quote == Asset("USD", "forex")
    assert [(Decimal(str(tx.quantity)), Decimal(str(tx.price))) for tx in order_from_checkpoint.transactions] == [
        (Decimal("0.0001"), Decimal("75000.25"))]


def test_wait_for_status_after_crypto_cancel_without_changing_local_order(broker, venue):
    resting = order()
    broker.submit_order(resting)
    incomplete = detail(resting.identifier)
    incomplete["orders"][0].pop("status")
    venue.order_v3.get_order_detail.return_value = reply(incomplete)
    assert broker._pull_order(resting.identifier, resting.strategy) is None
    assert resting.is_active()
    venue.order_v3.get_order_detail.return_value = reply(detail(resting.identifier, status="CANCELLED"))
    broker._refresh_tracked_order(resting)
    assert resting.is_canceled()


@pytest.mark.parametrize("asset_type,leg_sizes", [("option", [2]), ("multileg", [2, 4])])
def test_option_replace_preserves_broker_leg_ids_and_scales_ratios(broker, venue, asset_type, leg_sizes):
    from datetime import date

    asset = Asset("SAMPLE", asset_type=asset_type, expiration=date(2028, 1, 21), strike=50, right="CALL")
    o = Order("qualification", asset, 2, "buy", limit_price=1, time_in_force="gtc")
    legs = [{"id": f"broker-leg-{i}", "quantity": str(q)} for i, q in enumerate(leg_sizes)]
    venue.order_v3.get_order_detail.return_value = reply(detail(
        o.identifier, instrument_type="OPTION", total_quantity="2", filled_quantity="1", legs=legs,
    ))
    broker._modify_order(o, limit_price=1.15, quantity=3)
    _, [changed] = venue.order_v3.replace_order.call_args.args
    assert changed["client_order_id"] == o.identifier
    assert changed["quantity"] == "3" and changed["limit_price"] == "1.15"
    assert changed["legs"] == [{"id": leg["id"], "quantity": str(3 * q // 2)} for leg, q in zip(legs, leg_sizes)]
    with pytest.raises(ValueError, match="completed fills"):
        broker._modify_order(o, quantity=1)
    venue.order_v3.replace_order.assert_called_once()


def test_option_replace_needs_a_unique_provider_id_for_each_leg(broker, venue):
    from datetime import date

    asset = Asset("SAMPLE", asset_type="option", expiration=date(2028, 1, 21), strike=50, right="PUT")
    o = Order("qualification", asset, 1, "buy_to_open", limit_price=1)
    venue.order_v3.get_order_detail.return_value = reply(detail(
        o.identifier, instrument_type="OPTION", legs=[{"quantity": "1"}],
    ))
    with pytest.raises(ValueError, match="leg ID"):
        broker._modify_order(o, limit_price=1.15)
    venue.order_v3.replace_order.assert_not_called()


def test_native_vertical_reconciles_terminal_detail_on_repeated_poll(broker, venue):
    from datetime import date

    strikes = (100, 105)
    legs = [Order("qualification", Asset("SAMPLE", asset_type="option", right="CALL",
                                         expiration=date(2027, 1, 15), strike=k),
                  1, side, order_type="market")
            for k, side in zip(strikes, ("buy_to_open", "sell_to_open"))]
    [parent] = broker.submit_orders(legs, is_multileg=True, order_type="debit", price="1.25")
    response_legs = [{"id": str(index), "quantity": "1", "strike_price": str(strike), "side": side,
                      "option_contract_multiplier": "100", "option_type": "CALL", "option_expire_date": "2027-01-15"}
                     for index, (strike, side) in enumerate(zip(strikes, ("BUY", "SELL")))]
    venue.order_v3.get_order_detail.return_value = reply(detail(
        parent.identifier, status="FILLED", total_quantity="1", filled_quantity="1", filled_price="1.20",
        option_strategy="VERTICAL", instrument_type="OPTION", legs=response_legs))
    broker._refresh_tracked_order(parent)
    broker._refresh_tracked_order(parent)
    assert parent.is_filled()
    assert [(float(fill.quantity), float(fill.price)) for fill in parent.transactions] == [(1.0, 1.2)]


def test_positive_execution_cases_use_the_whole_exchange_session():
    import pandas as pd
    from tests.test_webull_paper_apitest import _require_equity_core_session

    local = "America/New_York"
    _require_equity_core_session(pd.Timestamp("2026-11-23 09:30", tz=local))
    for when in ("2026-11-21 10:00", "2026-11-23 09:29", "2026-11-23 16:00", "2026-11-27 13:01"):
        with pytest.raises(pytest.skip.Exception):
            _require_equity_core_session(pd.Timestamp(when, tz=local))


def test_bracket_child_signals_and_polling_use_independent_venue_order_ids(broker, venue):
    parent = Order("qualification", Asset("SAMPLE"), 1, "buy", limit_price=20,
                   order_type="limit", order_class="bracket", secondary_limit_price=22, secondary_stop_price=18)
    broker.submit_order(parent)
    target, stop = parent.child_orders
    assert broker.get_tracked_order(target.identifier) is target
    assert broker.get_tracked_order(stop.identifier) is stop
    venue.order_v3.get_order_detail.return_value = reply(detail(
        target.identifier, instrument_type="EQUITY", status="FILLED", side="SELL",
        total_quantity="1", limit_price="22", filled_quantity="1", filled_price="22.05"))
    broker._poll_signaled_order(target.identifier)
    broker._poll_signaled_order(target.identifier)
    assert target.is_filled() and len(target.transactions) == 1
    venue.order_v3.get_order_detail.return_value = reply(detail(
        stop.identifier, instrument_type="EQUITY", order_type="STOP_LOSS", status="CANCELLED",
        side="SELL", total_quantity="1", stop_price="18", limit_price=None, filled_quantity="0"))
    broker._poll_signaled_order(stop.identifier)
    assert stop.is_canceled() and not stop.transactions
    assert target.parent_identifier == stop.parent_identifier == parent.identifier


@pytest.mark.parametrize("right,group_qty,expected", [
    ("CALL", "2", {90: 2, 95: -2}), ("CALL", "-2", {90: -2, 95: 2}),
    ("PUT", "2", {90: -2, 95: 2}), ("PUT", "-2", {90: 2, 95: -2}),
])
def test_signed_vertical_holdings_net_with_single_contract_across_rows(broker, venue, right, group_qty, expected):
    def leg(strike):
        return dict(symbol="SAMPLE", option_expire_date="2027-01-15", option_type=right,
                    option_exercise_price=str(strike), option_contract_multiplier="100",
                    option_contract_deliverable="100", instrument_type="OPTION")
    spread = dict(symbol="SAMPLE", instrument_type="OPTION", option_strategy="VERTICAL",
                  quantity=group_qty, legs=[leg(95), leg(90)])
    single = dict(spread, option_strategy="SINGLE", quantity="1", legs=[leg(95)])
    venue.account_v2.get_account_position.return_value = reply([spread, single])
    positions = broker._pull_positions("holdings")
    assert {p.asset.strike: p.quantity for p in positions} == {90: expected[90], 95: expected[95] + 1}
    assert len(positions) == 2
    assert all(p.asset.right == right and p.asset.multiplier == 100 for p in positions)


def test_grouped_butterfly_positions_preserve_the_two_short_middle_contracts(broker, venue):
    legs = [
        dict(symbol="SAMPLE", instrument_type="OPTION", option_expire_date="2027-01-15",
             option_type="CALL", option_exercise_price=str(k), option_contract_multiplier="100")
        for k in (110, 100, 105)
    ]
    venue.account_v2.get_account_position.return_value = reply([dict(
        symbol="SAMPLE", option_strategy="BUTTERFLY", instrument_type="OPTION", quantity="2", legs=legs)])
    assert {p.asset.strike: p.quantity for p in broker._pull_positions("holdings")} == {100: 2, 105: -4, 110: 2}


@pytest.mark.parametrize("direction,side", [("LEG_OUT", "buy_to_close"), ("LEG_IN", "sell_to_open")])
def test_option_position_legging_uses_target_without_general_position_intent(broker, direction, side):
    from datetime import date

    asset = Asset("SAMPLE", "option", right="CALL", strike=45, expiration=date(2027, 1, 15))
    params = {"leg_in_or_out": direction}
    if direction == "LEG_IN":
        params["leg_in_strategy"] = "VERTICAL"
    order = Order("qualification", asset, 1, side, order_type="market", time_in_force="day",
                  custom_params=params, position_id="synthetic-position")
    row = broker._order_row(order)
    assert "position_intent" not in row
    assert row["position_id"] == "synthetic-position" and row["leg_in_or_out"] == direction
    assert row["side"] == ("BUY" if direction == "LEG_OUT" else "SELL")
    assert row.get("leg_in_strategy") == params.get("leg_in_strategy")
