"""CCXT ships its own exchange partner IDs and, on some DEXes, an extra user-paid
builder fee. LumiBot removes both by default and only attaches IDs or fees that
are explicitly configured. These tests use the installed CCXT classes offline:
they call generated endpoint methods with the HTTP transport captured, or inspect
options set at construction. Both current and older installed adapters are covered."""

import json
import logging
from unittest.mock import patch

import ccxt
import pytest

from lumibot.tools import ccxt_partner_ids as partner
from lumibot.tools.ccxt_partner_ids import NEUTRAL_PREFIX, apply_partner_config

CREDS = {"uid": "u", "apiKey": "k" * 16, "secret": "c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0MTI=", "password": "p"}


def _exchange(exchange_id, **config):
    exchange = getattr(ccxt, exchange_id)({**CREDS, **config})
    return exchange


def _signed(exchange, endpoint, params):
    # Generated methods supply the SDK's own sign() arguments. CCXT 4.5.91
    # changed several API groups from strings to lists; hand-built arguments
    # bypassed that contract and sometimes exercised an unsigned/public branch.
    def capture(url, method="GET", headers=None, body=None):
        return {"url": url, "method": method, "headers": headers, "body": body}

    with patch.object(exchange, "fetch", side_effect=capture) as transport:
        signed = getattr(exchange, endpoint)(dict(params))
    transport.assert_called_once()
    assert signed["method"] == "POST"
    return signed


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    def reject_network(*args, **kwargs):
        raise AssertionError("Partner-ID tests must not make HTTP requests")

    monkeypatch.setattr(ccxt.Exchange, "fetch", reject_network)
    for name in (partner.ENV_PARTNER_IDS, partner.ENV_BUILDER_FEES, partner.ENV_REFERRAL_CODES):
        monkeypatch.delenv(name, raising=False)


def _all_text(signed):
    return json.dumps({"url": signed["url"], "body": signed.get("body"), "headers": signed.get("headers") or {}},
                      default=str)


# ---------------------------------------------------------------- defaults: CCXT's IDs are off


def test_lumibot_data_source_strips_gate_channel_header():
    from lumibot.data_sources import CcxtData

    data = CcxtData({"exchange_id": "gate", **CREDS})
    assert "X-Gate-Channel-Id" not in (data.api.headers or {})


@pytest.mark.parametrize(
    "exchange_id, endpoint, params, ccxt_marker",
    [
        ("bitget", "private_spot_post_v2_spot_trade_place_order", {"symbol": "BTCUSDT"}, "p4sve"),
        ("bingx", "spot_v1_private_post_trade_order", {"symbol": "BTC-USDT"}, "X-SOURCE-KEY"),
        ("bybit", "private_post_v5_order_create", {"symbol": "BTCUSDT"}, "Referer"),
        ("kucoin", "private_post_orders", {"symbol": "BTC-USDT"}, "KC-API-PARTNER"),
        ("mexc", "spot_private_post_order", {"symbol": "BTCUSDT"}, '"source"'),
        ("phemex", "private_post_orders", {"symbol": "BTCUSD"}, "CCXT123456"),
        ("binance", "fapiprivate_post_order", {"symbol": "BTCUSDT"}, "x-cvBPrNm9"),
        pytest.param(
            "bitmart", "private_post_spot_v2_submit_order", {"symbol": "BTC_USDT"}, "X-BM-BROKER-ID",
            marks=pytest.mark.skipif(
                "bitmart" not in ccxt.exchanges,
                reason="CCXT removed BitMart (upstream PR 29376); retained for older installed adapters",
            ),
        ),
        ("cryptocom", "v1_private_post_private_create_order", {"broker_id": "CCXT"}, "broker_id"),
        ("blofin", "private_post_trade_order", {"brokerId": "ec6dd3a7dd982d0b"}, "ec6dd3a7dd982d0b"),
    ],
)
def test_ccxt_partner_ids_are_off_by_default(exchange_id, endpoint, params, ccxt_marker):
    exchange = _exchange(exchange_id)
    raw = _all_text(_signed(exchange, endpoint, params))
    assert ccxt_marker in raw, "CCXT no longer sends this ID; update the partner table"

    exchange = _exchange(exchange_id)
    state = apply_partner_config(exchange)
    cleaned = _all_text(_signed(exchange, endpoint, params))
    assert ccxt_marker not in cleaned
    assert state["partner_id"] is None


@pytest.mark.parametrize(
    "exchange_id,option,ccxt_value",
    [
        ("phemex", "brokerId", "CCXT123456"),
        ("okx", "brokerId", "6b9ad766b55dBCDE"),
        ("coinbase", "brokerId", "ccxt"),
        pytest.param(
            "coinex", "brokerId", "x-167673045",
            marks=pytest.mark.skipif(
                "coinex" not in ccxt.exchanges,
                reason="CCXT removed CoinEx (upstream PR 30737); retained for older installed adapters",
            ),
        ),
        ("coinbaseinternational", "brokerId", "nfqkvdjp"),
    ],
)
def test_client_order_id_prefixes_become_neutral(exchange_id, option, ccxt_value):
    # Separate cases keep all available adapters covered when one is retired.
    exchange = _exchange(exchange_id)
    assert exchange.options[option] == ccxt_value
    apply_partner_config(exchange)
    assert exchange.options[option] == NEUTRAL_PREFIX


def test_binance_client_order_id_prefixes_become_neutral():
    binance = _exchange("binance")
    apply_partner_config(binance)
    assert set(binance.options["broker"].values()) == {NEUTRAL_PREFIX}


def test_unremovable_ids_are_reported_honestly():
    state = apply_partner_config(_exchange("woo"))
    assert "broker_id" in state["unremovable"]


# ---------------------------------------------------------------- our IDs, only when configured


def test_configured_partner_ids_replace_ccxt(monkeypatch):
    monkeypatch.setenv(partner.ENV_PARTNER_IDS, json.dumps({
        "gate": "lumigate", "bybit": "LUMIBYBIT", "okx": "LumiOkx1", "binance": "x-LUMI0001",
        "kucoin": {"id": "lumikc", "key": "partner-key"},
    }))
    gate = _exchange("gate")
    assert apply_partner_config(gate)["partner_id"] == "lumigate"
    assert gate.headers["X-Gate-Channel-Id"] == "lumigate"

    bybit = _exchange("bybit")
    apply_partner_config(bybit)
    assert _signed(bybit, "private_post_v5_order_create", {})["headers"]["Referer"] == "LUMIBYBIT"

    # OKX attaches clOrdId/tag in create_order from options["brokerId"] (its sign-time injection
    # edits a dict that is never serialised in CCXT 4.5.64).
    okx = _exchange("okx")
    apply_partner_config(okx)
    assert okx.options["brokerId"] == "LumiOkx1"

    binance = _exchange("binance")
    apply_partner_config(binance)
    assert set(binance.options["broker"].values()) == {"x-LUMI0001"}

    kucoin = _exchange("kucoin")
    apply_partner_config(kucoin)
    headers = _signed(kucoin, "private_post_orders", {})["headers"]
    assert headers["KC-API-PARTNER"] == "lumikc"


def test_header_only_exchange_gets_configured_id(monkeypatch):
    monkeypatch.setenv(partner.ENV_PARTNER_IDS, json.dumps({"backpack": "777"}))
    exchange = _exchange("backpack", secret="A" * 43 + "=")
    apply_partner_config(exchange)
    assert partner._post_sign_headers(exchange, {"X-Broker-Id": "1400"})["X-Broker-Id"] == "777"


def test_backpack_default_header_is_removed():
    exchange = _exchange("backpack")
    apply_partner_config(exchange)
    assert "X-Broker-Id" not in partner._post_sign_headers(exchange, {"X-Broker-Id": "1400", "X-API-Key": "k"})


def test_weex_client_order_prefix_is_rewritten():
    exchange = _exchange("weex")
    apply_partner_config(exchange)
    params = partner._pre_sign_params(exchange, {"newClientOrderId": "b-WEEX111125-abcdef"})
    assert not params["newClientOrderId"].startswith("b-WEEX111125")
    assert params["newClientOrderId"].endswith("-abcdef")
    assert len(params["newClientOrderId"]) == len("b-WEEX111125-abcdef")


def test_apply_is_idempotent():
    exchange = _exchange("bybit")
    apply_partner_config(exchange)
    apply_partner_config(exchange)
    assert "Referer" not in (_signed(exchange, "private_post_v5_order_create", {})["headers"] or {})


def test_partner_rules_only_reference_installed_or_known_retired_exchanges():
    # These upstream removals must not erase support for older installations:
    # https://github.com/ccxt/ccxt/pull/29376 (BitMart)
    # https://github.com/ccxt/ccxt/pull/30737 (CoinEx)
    retired = {"bitmart", "coinex"}
    listed = set(partner.PARTNER_RULES) | set(partner.BUILDER_FEE_DEFAULTS)
    unavailable = listed - set(ccxt.exchanges)
    assert unavailable <= retired  # unexpected removals/typos must still fail
    assert retired <= partner.PARTNER_RULES.keys()
    for exchange_id in listed - unavailable:
        assert callable(getattr(ccxt, exchange_id)), exchange_id


# ---------------------------------------------------------------- builder fees and referrers


@pytest.mark.parametrize("exchange_id", ["lighter", "grvt", "aster", "pacifica"])
def test_extra_builder_fee_is_off_by_default(exchange_id):
    exchange = getattr(ccxt, exchange_id)()
    assert exchange.options["builderFee"] is True
    state = apply_partner_config(exchange)
    assert exchange.options["builderFee"] is False
    assert state["builder_fee"] is None


def test_hyperliquid_never_approves_ccxt_builder_or_sets_ccxt_referrer():
    exchange = ccxt.hyperliquid()
    calls = []
    exchange.approve_builder_fee = lambda *a, **k: calls.append(("approve", a))
    exchange.privatePostExchange = lambda *a, **k: calls.append(("exchange", a))
    apply_partner_config(exchange)
    assert exchange.options["builderFee"] is False
    assert exchange.handle_builder_fee_approval() is True
    assert exchange.set_ref() is True
    assert calls == []
    assert not exchange.options.get("approvedBuilderFee")  # so create_order attaches no builder at all


def test_extended_builder_fee_is_zero_and_unattributed():
    exchange = ccxt.extended()
    apply_partner_config(exchange)
    assert exchange.options["builderFeeRate"] == "0"
    assert exchange.options["builderId"] is None


def test_configured_builder_fee_is_applied_and_logged(monkeypatch, caplog):
    monkeypatch.setenv(partner.ENV_BUILDER_FEES, json.dumps({
        "hyperliquid": {"builder": "0x" + "1" * 40, "feeRate": "0.01%", "feeInt": 10}}))
    monkeypatch.setenv(partner.ENV_REFERRAL_CODES, json.dumps({"hyperliquid": "LUMIREF"}))
    exchange = ccxt.hyperliquid()
    with caplog.at_level(logging.WARNING):
        state = apply_partner_config(exchange)
    assert exchange.options["builderFee"] is True
    assert exchange.options["builder"] == "0x" + "1" * 40
    assert exchange.options["ref"] == "LUMIREF" and not exchange.options.get("refSet")
    assert state["builder_fee"] == {"builder": "0x" + "1" * 40, "feeRate": "0.01%", "feeInt": 10}
    assert "costs the user extra" in caplog.text


def test_bad_json_config_fails_loudly(monkeypatch):
    monkeypatch.setenv(partner.ENV_PARTNER_IDS, "{not json")
    with pytest.raises(ValueError, match=partner.ENV_PARTNER_IDS):
        apply_partner_config(_exchange("gate"))


def test_async_pro_client_is_handled():
    import ccxt.pro

    exchange = ccxt.pro.hyperliquid()
    apply_partner_config(exchange)
    import asyncio
    assert asyncio.run(exchange.handle_builder_fee_approval()) is True
    gate = ccxt.pro.gate()
    apply_partner_config(gate)
    assert "X-Gate-Channel-Id" not in gate.headers


def test_real_okx_order_request_built_by_lumibot_carries_no_ccxt_id():
    """End to end through CCXT's own create_order: only the network call is faked."""
    from lumibot.data_sources import CcxtData

    data = CcxtData({"exchange_id": "okx", **CREDS, "sandbox": False})
    data.api.set_markets([{
        "id": "BTC-USDT", "symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "baseId": "BTC", "quoteId": "USDT",
        "type": "spot", "spot": True, "swap": False, "future": False, "option": False, "contract": False,
        "linear": None, "inverse": None, "settle": None, "active": True,
        "precision": {"amount": 0.0001, "price": 0.1}, "limits": {"amount": {"min": 0.0001}, "price": {}, "cost": {}},
        "info": {"instType": "SPOT"}}])
    sent = {}

    def fetch(url, method="GET", headers=None, body=None):
        sent["body"] = body
        return {"code": "0", "data": [{"ordId": "1", "clOrdId": "x", "sCode": "0"}]}

    data.api.fetch = fetch
    data.api.create_order("BTC/USDT", "limit", "buy", 0.001, 100)
    order = json.loads(sent["body"])[0]
    assert "6b9ad766b55dBCDE" not in sent["body"]
    assert order["clOrdId"].startswith(NEUTRAL_PREFIX) and order["tag"] == NEUTRAL_PREFIX
