"""Native venue serialization, exact price limits, accounting and remote signing."""

from decimal import Decimal, localcontext
from unittest.mock import Mock

import pytest
import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

from lumibot.tools.metamask.provider import MetaMaskError
from lumibot.tools.metamask.venue import HyperliquidVenue, action_typed_data, signature_parts

WALLET = "0x" + "1" * 40
META = {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]}


def reply(value):
    result = Mock(status_code=200)
    result.json.return_value = value
    return result


@pytest.fixture
def venue():
    return HyperliquidVenue("testnet", session=Mock(spec=requests.Session))


def test_price_reads_fixed_testnet_endpoint_without_wallet_credentials(venue):
    venue.session.request.return_value = reply([META, [{"markPx": "60000.125"}]])
    assert venue.price("BTC") == Decimal("60000.125")
    args, kwargs = venue.session.request.call_args
    assert args == ("POST", "https://api.hyperliquid-testnet.xyz/info")
    assert kwargs["json"] == {"type": "metaAndAssetCtxs"}
    assert kwargs["allow_redirects"] is False
    assert "Authorization" not in kwargs.get("headers", {})


def test_live_open_order_snapshot_is_read_from_bound_wallet_with_exact_remaining_size(venue):
    rows = [{"coin": "ETH", "oid": 42, "sz": "0.0002", "origSz": "0.0106", "reduceOnly": False}]
    venue.session.request.return_value = reply(rows)
    assert venue.frontend_open_orders(WALLET) == rows
    args, kwargs = venue.session.request.call_args
    assert args == ("POST", "https://api.hyperliquid-testnet.xyz/info")
    assert kwargs["json"] == {"type": "frontendOpenOrders", "user": WALLET}
    assert "Authorization" not in kwargs.get("headers", {})


@pytest.mark.parametrize("value", [None, {}, [None], ["not-an-order"]])
def test_malformed_live_open_order_snapshot_is_not_treated_as_empty(venue, value):
    venue.session.request.return_value = reply(value)
    with pytest.raises(MetaMaskError, match="INVALID_RESPONSE"):
        venue.frontend_open_orders(WALLET)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "0", "-1", None])
def test_unavailable_or_invalid_mark_is_not_a_synthetic_price(venue, value):
    venue.session.request.return_value = reply([META, [{"markPx": value}]])
    with pytest.raises(MetaMaskError):
        venue.price("BTC")


def test_exact_size_and_protective_limit_normalization(venue):
    venue.session.request.return_value = reply(META)
    plan = venue.order_action(
        "BTC", "buy", Decimal("0.001"), "limit", Decimal("60000.123"), "gtc", False, "0x" + "1" * 32
    )
    wire = plan["orders"][0]
    assert wire["s"] == "0.001"
    assert Decimal(wire["p"]) <= Decimal("60000.123")
    assert wire["t"] == {"limit": {"tif": "Gtc"}}
    assert wire["r"] is False
    with pytest.raises(MetaMaskError, match="SIZE_PRECISION"):
        venue.order_action("BTC", "buy", Decimal("0.000001"), "limit", Decimal("60000"), "gtc", False, "0x" + "1" * 32)


def test_price_normalization_must_not_weaken_sell_limit(venue):
    venue.session.request.return_value = reply(META)
    with pytest.raises(MetaMaskError, match="PRICE_PRECISION"):
        venue.order_action(
            "BTC", "sell", Decimal("0.001"), "limit", Decimal("60000.123"), "gtc", False, "0x" + "1" * 32
        )


def test_market_order_is_an_ioc_bounded_by_configured_slippage(venue):
    venue.session.request.side_effect = [reply(META), reply([META, [{"markPx": "60000"}]])]
    action = venue.order_action(
        "BTC", "buy", Decimal("0.001"), "market", None, "gtc", True, "0x" + "2" * 32, slippage_bps=50
    )
    wire = action["orders"][0]
    assert wire["t"] == {"limit": {"tif": "Ioc"}}
    assert wire["r"] is True
    assert Decimal(wire["p"]) <= Decimal("60300")


def test_l1_hash_matches_official_sdk_vector_and_does_not_confuse_chain_domains():
    action = {"type": "cancel", "cancels": [{"a": 0, "o": 42}]}
    testnet = action_typed_data(action, 1000, "testnet")
    assert testnet["domain"]["chainId"] == 1337
    assert testnet["message"] == {
        "source": "b",
        "connectionId": "0x8de8e7a260c10b93dbb8957884c17971de014cec0c668a7ca8c14ae2c802acf5",
    }
    mainnet = action_typed_data(action, 1000, "mainnet")
    assert mainnet["message"]["source"] == "a"
    assert action_typed_data(action, 1001, "testnet")["message"] != testnet["message"]


def test_remote_signature_must_recover_exact_bound_wallet():
    # Offline deterministic key is a test vector, never a funded runtime wallet.
    signer = Account.from_key("0x" + "01" * 32)
    data = action_typed_data({"type": "cancel", "cancels": []}, 1000, "testnet")
    signature = signer.sign_message(encode_typed_data(full_message=data)).signature.hex()
    signature = signature if signature.startswith("0x") else "0x" + signature
    parts = signature_parts(data, signature, signer.address)
    assert parts["v"] in (27, 28)
    with pytest.raises(MetaMaskError, match="SIGNER_MISMATCH"):
        signature_parts(data, signature, WALLET)


def test_standard_cash_uses_equity_margin_and_actual_withdrawable(venue):
    venue.session.request.side_effect = [
        reply("default"),
        reply(
            {
                "marginSummary": {"accountValue": "100", "totalMarginUsed": "15"},
                "withdrawable": "80",
                "assetPositions": [],
            }
        ),
    ]
    assert venue.account(WALLET) == {
        "cash": "80",
        "equity": "100",
        "positionsValue": "20",
        "currency": "USDC",
        "positions": [],
    }


def test_unified_account_uses_pooled_usdc_without_adding_pnl_again(venue):
    venue.session.request.side_effect = [
        reply("unifiedAccount"),
        reply(
            {
                "marginSummary": {"accountValue": "0", "totalMarginUsed": "0"},
                "withdrawable": "0",
                "assetPositions": [],
            }
        ),
        reply({"balances": [{"coin": "USDC", "token": 0, "total": "123.45", "hold": "3.45"}]}),
    ]
    account = venue.account(WALLET)
    assert account["equity"] == "123.45"
    assert account["cash"] == "120.00"
    assert account["positionsValue"] == "3.45"


def test_unknown_account_mode_is_not_valued_as_standard(venue):
    venue.session.request.return_value = reply("portfolioMargin")
    with pytest.raises(MetaMaskError, match="ACCOUNT_MODE_UNSUPPORTED"):
        venue.account(WALLET)


def test_candles_exclude_open_bar_and_preserve_requested_network(venue):
    venue.session.request.return_value = reply(
        [
            {"s": "BTC", "i": "1m", "t": 0, "T": 59999, "o": "1", "h": "3", "l": "1", "c": "2", "v": "4"},
            {"s": "BTC", "i": "1m", "t": 60000, "T": 119999, "o": "2", "h": "3", "l": "1", "c": "3", "v": "5"},
        ]
    )
    assert venue.candles("BTC", "1m", 0, 90000) == [[0, "1", "3", "1", "2", "4"]]


def test_exchange_transport_loss_is_unknown_without_retransmission(venue):
    venue.session.request.side_effect = requests.Timeout()
    with pytest.raises(MetaMaskError, match="VENUE_OUTCOME_UNKNOWN"):
        venue.submit_action({"type": "cancel", "cancels": []}, 1000, {"r": "0x1", "s": "0x2", "v": 27})
    assert venue.session.request.call_count == 1


def test_invalid_ecdsa_scalars_return_explicit_signature_error():
    data = action_typed_data({"type": "cancel", "cancels": []}, 1000, "testnet")
    with pytest.raises(MetaMaskError, match="INVALID_SIGNATURE"):
        signature_parts(data, "0x" + "00" * 64 + "1b", WALLET)


def test_leverage_read_verifies_wallet_and_market(venue):
    venue.session.request.return_value = reply(
        {"user": WALLET, "coin": "BTC", "leverage": {"type": "cross", "value": 3}}
    )
    assert venue.leverage(WALLET, "BTC") == {"type": "cross", "value": 3}
    assert venue.session.request.call_args.kwargs["json"] == {"type": "activeAssetData", "user": WALLET, "coin": "BTC"}


@pytest.mark.parametrize(
    "changes",
    [
        {"user": "0x" + "2" * 40},
        {"coin": "ETH"},
        {"leverage": {"type": "cross", "value": True}},
        {"leverage": {"type": "cross", "value": 0}},
        {"leverage": {"type": "unknown", "value": 1}},
    ],
)
def test_leverage_recovery_rejects_unrelated_or_malformed_state(venue, changes):
    venue.session.request.return_value = reply(
        {"user": WALLET, "coin": "BTC", "leverage": {"type": "cross", "value": 3}, **changes}
    )
    with pytest.raises(MetaMaskError):
        venue.leverage(WALLET, "BTC")


@pytest.mark.parametrize("kind,side", [("limit", "buy"), ("market", "buy"), ("market", "sell")])
def test_order_wire_ignores_strategy_decimal_precision(venue, kind, side):
    venue.session.request.side_effect = lambda *args, **kwargs: reply(
        META if kwargs["json"]["type"] == "meta" else [META, [{"markPx": "60000.123456789"}]]
    )
    args = ("BTC", side, Decimal("123.45678"), kind, Decimal("60000.123"), "gtc", False, "0x" + "1" * 32)
    expected = venue.order_action(*args)
    with localcontext() as context:
        context.prec = 6
        actual = venue.order_action(*args)
    assert actual == expected
    assert actual["orders"][0]["s"] == "123.45678"


def test_account_balances_ignore_strategy_decimal_precision(venue):
    venue.session.request.side_effect = [
        reply("default"),
        reply(
            {
                "marginSummary": {"accountValue": "123456.789012", "totalMarginUsed": "0.000012"},
                "withdrawable": "123456.789012",
                "assetPositions": [],
            }
        ),
    ]
    with localcontext() as context:
        context.prec = 6
        result = venue.account(WALLET)
    # Exact subtraction may retain insignificant zeroes from the venue's input scale.
    assert Decimal(result["cash"]) == Decimal("123456.789")
    assert result["positionsValue"] == "0.000012"
