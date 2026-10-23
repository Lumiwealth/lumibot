"""Shared CCXT spot contracts, using public unified structures and no credentials."""

from decimal import Decimal, getcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers.ccxt import Ccxt
from lumibot.entities import Asset, Order


@pytest.fixture
def make_broker(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")

    def build(exchange="phemex"):
        broker = Ccxt(
            {"exchange_id": exchange, "sandbox": False},
            connect_stream=False,
            start_orders_thread=False,
        )
        broker.api.set_markets([{
            "id": "BTCUSDT", "symbol": "BTC/USDT", "base": "BTC", "quote": "USDT",
            "baseId": "BTC", "quoteId": "USDT", "settle": None, "settleId": None,
            "type": "spot", "spot": True, "swap": False, "future": False,
            "option": False, "contract": False, "linear": False, "inverse": False,
            "active": True, "precision": {"amount": 0.005, "price": 0.5},
            "limits": {"amount": {"min": 0.005, "max": None},
                       "price": {"min": None, "max": None},
                       "cost": {"min": 1, "max": None}}, "info": {},
        }])
        broker.data_source._markets_loaded = True
        broker.api.create_order = Mock(return_value={"id": "test-order", "status": "open"})
        broker.data_source.get_last_price = Mock(return_value=20000)
        return broker

    return build


def order(**kwargs):
    return Order("test-strategy", Asset("BTC", asset_type="crypto"),
                 Decimal("2.123456"), "buy", quote=Asset("USDT", asset_type="crypto"), **kwargs)


@pytest.mark.parametrize("exchange", ["phemex", "coinbase", "kraken", "kucoin", "okx", "bybit"])
def test_unified_market_order_uses_ccxt_contract_without_exchange_allowlist(make_broker, exchange):
    broker = make_broker(exchange)
    submitted = order(custom_params={"clientOrderId": "test-client-id"})

    result = broker._submit_order(submitted)

    assert result is submitted
    assert submitted.identifier == "test-order"
    args, kwargs = broker.api.create_order.call_args
    assert args[:4] == ("BTC/USDT", "market", "buy", "2.12")
    assert args[4] == ("20000" if exchange == "coinbase" else None)
    assert kwargs["params"]["clientOrderId"] == "test-client-id"


def test_tick_precision_uses_exchange_rounding_instead_of_decimal_places(make_broker):
    broker = make_broker()
    submitted = order(limit_price=123.26)

    broker._submit_order(submitted)

    args, _ = broker.api.create_order.call_args
    assert Decimal(args[4]) == Decimal("123.5")
    assert submitted.quantity == Decimal("2.12")


def test_stop_order_preserves_trigger_and_custom_parameters(make_broker):
    broker = make_broker()
    submitted = order(stop_price=125, custom_params={"triggerDirection": "ascending"})

    broker._submit_order(submitted)

    args, kwargs = broker.api.create_order.call_args
    assert args[1] == "market"
    assert kwargs["params"]["triggerPrice"] == "125"
    assert kwargs["params"]["triggerDirection"] == "ascending"


@pytest.mark.parametrize("exchange", ["phemex", "coinbase", "kraken", "kucoin", "okx", "bybit"])
def test_unified_balances_and_spot_positions_work_without_exchange_allowlist(make_broker, exchange):
    broker = make_broker(exchange)
    broker.api.fetch_balance = Mock(return_value={
        "total": {"USDT": 1000, "BTC": 0.01},
        "free": {"USDT": 900, "BTC": 0.008},
        "used": {"USDT": 100, "BTC": 0.002},
        "info": {"provider": "opaque"},
    })
    broker.api.fetch_ticker = Mock(return_value={"last": 20000})
    quote = Asset("USDT", asset_type="crypto")

    assert broker._get_balances_at_broker(quote, None) == (1000.0, 200.0, 1200.0)
    positions = broker._pull_positions(SimpleNamespace(name="test-strategy", quote_asset=quote))
    assert len(positions) == 1
    assert positions[0].asset.symbol == "BTC"
    assert positions[0].quantity == 0.01
    assert positions[0].hold == Decimal("0.002")


def test_cancel_uses_unified_pair_and_processes_confirmed_cancellation(make_broker):
    broker = make_broker()
    submitted = order()
    submitted.identifier = "test-order"
    submitted.status = "new"
    broker.api.cancel_order = Mock(return_value={"id": "test-order", "status": "canceled"})

    broker.cancel_order(submitted)

    broker.api.cancel_order.assert_called_once_with("test-order", "BTC/USDT")
    assert submitted.is_canceled()


def test_closed_orders_pass_params_by_keyword(make_broker):
    broker = make_broker()
    broker.api.fetch_closed_orders = Mock(return_value=[])

    assert broker._pull_broker_closed_orders() == []

    broker.api.fetch_closed_orders.assert_called_once_with(params={})


def test_parse_unified_order_accepts_optional_fields(make_broker):
    broker = make_broker()

    parsed = broker._parse_broker_order({
        "id": "test-order", "symbol": "BTC/USDT", "type": "market",
        "amount": 0.01, "side": "buy", "status": "closed", "average": 20000,
    }, "test-strategy")

    assert parsed.asset.symbol == "BTC"
    assert parsed.quote.symbol == "USDT"
    assert parsed.identifier == "test-order"
    assert parsed.avg_fill_price == 20000


@pytest.mark.parametrize("trigger,expected_type", [(None, "Limit"), (125, "StopLimit")])
def test_phemex_native_spot_request_uses_base_units_and_trigger(make_broker, trigger, expected_type):
    broker = make_broker()
    # Run the actual installed CCXT serializer; replace only its HTTP transport.
    broker.api.markets["BTC/USDT"].update(valueScale=8, priceScale=8)
    broker.api.create_order = type(broker.api).create_order.__get__(broker.api)
    broker.api.request = Mock(return_value={"code": 0, "data": {
        "orderID": "native-order", "symbol": "BTCUSDT", "ordStatus": "New",
        "ordType": expected_type, "side": "Buy", "baseQtyEv": 212000000,
    }})
    submitted = order(limit_price=123.26, stop_price=trigger,
                      order_type="stop_limit" if trigger else "limit",
                      custom_params={"clientOrderId": "client-order"})

    broker._submit_order(submitted)

    assert submitted.identifier == "native-order", submitted.error_message
    request = broker.api.request.call_args.args[3]
    assert request["baseQtyEv"] == 212000000
    assert request["priceEp"] == 12350000000
    assert request["ordType"] == expected_type
    assert request["clOrdID"] == "client-order"
    if trigger:
        assert request["stopPxEp"] == 12500000000


def test_order_rounding_preserves_decimal_context(make_broker):
    broker = make_broker()
    before = getcontext().copy()

    broker._submit_order(order())

    assert getcontext().prec == before.prec
    assert getcontext().rounding == before.rounding


def test_precision_mode_comes_from_ccxt_metadata(make_broker):
    import ccxt

    broker = make_broker()
    broker.api.precisionMode = ccxt.DECIMAL_PLACES
    broker.api.markets["BTC/USDT"]["precision"] = {"amount": 3, "price": 2}
    submitted = order(limit_price=123.267)

    broker._submit_order(submitted)

    assert broker.api.create_order.call_args.args[3:5] == ("2.123", "123.27")


def test_cancel_acknowledgement_without_terminal_status_stays_pending(make_broker):
    broker = make_broker()
    submitted = order()
    submitted.identifier, submitted.status = "test-order", "new"
    broker.api.cancel_order = Mock(return_value={"id": "test-order"})

    broker.cancel_order(submitted)

    assert not submitted.is_canceled()


def test_per_currency_balances_and_dust_are_preserved(make_broker):
    broker = make_broker()
    broker.api.fetch_balance = Mock(return_value={
        "BTC": {"total": "0.000000001", "free": "0.0000000008", "used": "0.0000000002"},
    })

    raw = broker._pull_broker_position(Asset("BTC", asset_type="crypto"))
    parsed = broker._parse_broker_position(raw, "test-strategy")

    assert parsed._quantity == Decimal("0.000000001")
    assert parsed.quantity == 0.000000001
    assert parsed.hold == Decimal("0.0000000002")
    assert parsed.available == Decimal("0.0000000008")


@pytest.mark.parametrize("field,limits", [("price", {"max": 100}), ("cost", {"min": 1000})])
def test_reported_market_limits_are_preserved(make_broker, field, limits):
    broker = make_broker()
    broker.api.markets["BTC/USDT"]["limits"][field] = limits
    submitted = order(limit_price=123.26)

    broker._submit_order(submitted)

    assert submitted.status == "error"
    broker.api.create_order.assert_not_called()
