"""Cross-broker contract: every order is placed as asked, or refused with a plain error.

For each broker with a request builder we can drive offline, and for every order type
(market, limit, stop, stop-limit, trailing) and order class (simple, bracket, OTO, OCO),
submitting either

* builds a broker request that carries every price the order needs, or
* refuses with an error that says "not supported" and sends nothing.

It must never silently downgrade (a trailing stop sent as market, a bracket sent without
its stop, a stop sent without its trigger price). The network client is stubbed; no
order is ever placed. The resulting matrix is published in docsrc/brokers.order_support.rst.

Prices are chosen so each one is a distinct string in the recorded request.
"""

from __future__ import annotations

import json
from contextlib import ExitStack
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order

LIMIT, STOP, STOP_LIMIT = 101.5, 97.5, 96.5
TRAIL_PERCENT, TRAIL_PRICE = 2.75, 7.25
TARGET, LEG_STOP, LEG_STOP_LIMIT = 111.5, 89.5, 88.5

# name -> (Order kwargs, prices the request must carry)
CASES = {
    "market": ({}, []),
    "limit": ({"limit_price": LIMIT}, [LIMIT]),
    "stop": ({"stop_price": STOP}, [STOP]),
    "stop_limit": ({"stop_price": STOP, "stop_limit_price": STOP_LIMIT}, [STOP, STOP_LIMIT]),
    "trailing_percent": ({"trail_percent": TRAIL_PERCENT}, [TRAIL_PERCENT]),
    "trailing_price": ({"trail_price": TRAIL_PRICE}, [TRAIL_PRICE]),
    "bracket": (
        {"order_class": "bracket", "secondary_limit_price": TARGET, "secondary_stop_price": LEG_STOP},
        [TARGET, LEG_STOP],
    ),
    "bracket_limit_entry": (
        {"order_class": "bracket", "limit_price": LIMIT, "secondary_limit_price": TARGET,
         "secondary_stop_price": LEG_STOP},
        [LIMIT, TARGET, LEG_STOP],
    ),
    "bracket_stop_limit_leg": (
        {"order_class": "bracket", "secondary_limit_price": TARGET, "secondary_stop_price": LEG_STOP,
         "secondary_stop_limit_price": LEG_STOP_LIMIT},
        [TARGET, LEG_STOP, LEG_STOP_LIMIT],
    ),
    "bracket_trailing_leg": (
        {"order_class": "bracket", "secondary_limit_price": TARGET, "secondary_trail_percent": TRAIL_PERCENT},
        [TARGET, TRAIL_PERCENT],
    ),
    "oto_stop": ({"order_class": "oto", "secondary_stop_price": LEG_STOP}, [LEG_STOP]),
    "oto_trailing": ({"order_class": "oto", "secondary_trail_percent": TRAIL_PERCENT}, [TRAIL_PERCENT]),
    "oco": (
        {"order_class": "oco", "limit_price": TARGET, "stop_price": LEG_STOP, "position_filled": True},
        [TARGET, LEG_STOP],
    ),
}


def _alpaca(stack):
    from lumibot.brokers.alpaca import Alpaca

    broker = Alpaca.__new__(Alpaca)
    broker.api = MagicMock()
    broker.api.submit_order.return_value = MagicMock(id="a-1", status="new")
    broker._unprocessed_orders = []
    asset = Asset("SPY")

    def sent():
        if not broker.api.submit_order.called:
            return None
        return repr(broker.api.submit_order.call_args.kwargs["order_data"].to_request_fields())

    return broker, asset, sent, "buy"


def _bitunix(stack):
    from lumibot.brokers.bitunix import Bitunix
    from lumibot.tools.bitunix_helpers import BitUnixClient

    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client.get_trading_pairs = MagicMock(return_value={"code": 0, "data": [
        {"symbol": "BTCUSDT", "basePrecision": 4, "quotePrecision": 2, "minTradeVolume": "0.0001"}]})
    client.get_position_mode = MagicMock(return_value={"code": 0, "data": {"positionMode": "ONE_WAY"}})
    client.change_leverage = MagicMock(return_value={"code": 0})
    stack.enter_context(patch("lumibot.brokers.bitunix.BitUnixClient", return_value=client))
    stack.enter_context(patch("lumibot.brokers.bitunix.BitunixData"))
    request = stack.enter_context(patch("requests.request"))
    request.return_value.json.return_value = {"code": 0, "data": {"orderId": "b-1"}}
    broker = Bitunix({"API_KEY": "test-key", "API_SECRET": "test-secret"}, connect_stream=False)
    broker._process_trade_event = MagicMock()
    asset = Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE, leverage=2)

    def sent():
        return json.dumps(json.loads(request.call_args.kwargs["data"])) if request.called else None

    return broker, asset, sent, "buy"


def _tradovate(stack):
    from lumibot.brokers import Tradovate

    broker = Tradovate.__new__(Tradovate)
    broker.account_spec, broker.account_id, broker.trading_token = "TEST", 1, "token"
    broker.trading_api_url = "https://example.invalid/v1"
    broker._ensure_connected = MagicMock()
    broker._get_headers = MagicMock(return_value={})
    broker._process_trade_event = MagicMock()
    response = MagicMock()
    response.json.return_value = {"orderId": 7}
    broker._request = MagicMock(return_value=response)
    asset = Asset("MESZ5", asset_type=Asset.AssetType.FUTURE)

    def sent():
        return repr(broker._request.call_args.kwargs["json"]) if broker._request.called else None

    return broker, asset, sent, "buy"


def _ccxt(exchange):
    def build(stack):
        from lumibot.brokers.ccxt import Ccxt

        stack.enter_context(patch.dict("os.environ", {"LUMIBOT_TELEMETRY": "false"}))
        broker = Ccxt({"exchange_id": exchange, "sandbox": False}, connect_stream=False, start_orders_thread=False)
        broker.api.set_markets([{
            "id": "BTCUSDT", "symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "baseId": "BTC",
            "quoteId": "USDT", "settle": None, "settleId": None, "type": "spot", "spot": True, "swap": False,
            "future": False, "option": False, "contract": False, "linear": False, "inverse": False,
            "active": True, "precision": {"amount": 0.001, "price": 0.25},
            "limits": {"amount": {"min": 0.001, "max": None}, "price": {"min": None, "max": None},
                       "cost": {"min": None, "max": None}}, "info": {},
        }])
        broker.data_source._markets_loaded = True
        broker.api.create_order = MagicMock(return_value={"id": "c-1", "status": "open"})
        broker.data_source.get_last_price = MagicMock(return_value=100)
        asset = Asset("BTC", asset_type="crypto")

        def sent():
            if not broker.api.create_order.called:
                return None
            args, kwargs = broker.api.create_order.call_args
            return repr((args, kwargs))

        return broker, asset, sent, "buy"

    return build


def _tradier(stack):
    from lumibot.brokers.tradier import Tradier

    broker = Tradier.__new__(Tradier)
    broker.tradier = MagicMock()
    broker.tradier.orders.order.return_value = {"id": 1}
    broker.tradier.orders.advanced_order.return_value = {"id": 1}
    broker._unprocessed_orders = []
    broker._safe_stream_dispatch = MagicMock()
    broker._normalize_symbol_for_broker = MagicMock(side_effect=lambda symbol, asset_type=None: symbol)
    stack.enter_context(patch("lumibot.brokers.tradier._order_leg", side_effect=lambda **kw: kw))
    asset = Asset("SPY")

    def sent():
        calls = broker.tradier.orders.order.call_args_list + broker.tradier.orders.advanced_order.call_args_list
        return repr(calls) if calls else None

    return broker, asset, sent, "buy"


BROKERS = {
    "alpaca": _alpaca,
    "bitunix": _bitunix,
    "tradovate": _tradovate,
    "tradier": _tradier,
    "ccxt_okx": _ccxt("okx"),
    "ccxt_bybit": _ccxt("bybit"),
    "ccxt_coinbase": _ccxt("coinbase"),
}


def _submit(broker_name, case_name):
    """Return ("placed", request_text) or ("refused", message)."""
    kwargs, _ = CASES[case_name]
    with ExitStack() as stack:
        stack.enter_context(patch.object(Broker, "_launch_stream", lambda self: None))
        broker, asset, sent, side = BROKERS[broker_name](stack)
        quote = Asset("USDT", asset_type="crypto") if broker_name.startswith("ccxt") else None
        order = Order("contract-strategy", asset, Decimal("1"), side=side, quote=quote, **kwargs)
        try:
            result = broker._submit_order(order)
        except Exception as exc:  # a refusal may be raised
            assert sent() is None, f"{broker_name}/{case_name} raised after sending a request"
            return "refused", str(exc)
        request = sent()
        result = result if result is not None else order
        if request is None:
            return "refused", str(getattr(result, "error_message", "") or "")
        assert str(result.status).lower() != "error", f"{broker_name}/{case_name} sent a request but errored"
        return "placed", request


@pytest.mark.parametrize("case_name", list(CASES))
@pytest.mark.parametrize("broker_name", list(BROKERS))
def test_order_is_placed_as_asked_or_refused_plainly(broker_name, case_name):
    outcome, detail = _submit(broker_name, case_name)
    if outcome == "refused":
        assert "not supported" in detail.lower(), f"{broker_name}/{case_name} refused without a plain reason: {detail}"
        return
    for price in CASES[case_name][1]:
        assert str(price) in detail, f"{broker_name}/{case_name} dropped price {price}: {detail}"


# Pins today's matrix so a silent change in either direction is visible in review.
EXPECTED_REFUSED = {
    "alpaca": {"bracket_trailing_leg", "oto_trailing"},
    "bitunix": {"stop", "stop_limit", "trailing_percent", "trailing_price", "bracket_trailing_leg",
                "oto_trailing", "oco"},
    "tradovate": {"trailing_percent", "trailing_price", "bracket", "bracket_limit_entry",
                  "bracket_stop_limit_leg", "bracket_trailing_leg", "oto_stop", "oto_trailing", "oco"},
    "tradier": {"trailing_percent", "trailing_price", "bracket_trailing_leg", "oto_trailing"},
    "ccxt_okx": {"trailing_price", "bracket", "bracket_limit_entry", "bracket_stop_limit_leg",
                 "bracket_trailing_leg", "oto_stop", "oto_trailing", "oco"},
    "ccxt_bybit": {"trailing_percent", "bracket", "bracket_limit_entry", "bracket_stop_limit_leg",
                   "bracket_trailing_leg", "oto_stop", "oto_trailing", "oco"},
    "ccxt_coinbase": {"trailing_percent", "trailing_price", "bracket", "bracket_limit_entry",
                      "bracket_stop_limit_leg", "bracket_trailing_leg", "oto_stop", "oto_trailing", "oco"},
}


@pytest.mark.parametrize("broker_name", list(BROKERS))
def test_support_matrix_matches_published_docs(broker_name):
    refused = {case for case in CASES if _submit(broker_name, case)[0] == "refused"}
    assert refused == EXPECTED_REFUSED[broker_name]
