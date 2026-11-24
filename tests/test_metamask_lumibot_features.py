"""Comprehensive tests for full LumiBot feature completeness on MetaMask."""

import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
from hyperliquid.utils.signing import action_hash as sdk_action_hash

from lumibot.brokers.metamask import MetaMask
from lumibot.data_sources.metamask_data import MetaMaskData
from lumibot.entities import Asset, Order, Position, Quote, SmartLimitConfig
from lumibot.strategies import Strategy
from lumibot.tools.metamask.engine import ExecutionEngine
from lumibot.tools.metamask.journal import Journal
from lumibot.tools.metamask.models import PerpIntent
from lumibot.tools.metamask.provider import Credentials, GuardPolicy, MetaMaskError, SignatureJob
from lumibot.tools.metamask.venue import HyperliquidVenue, Market, action_typed_data
from lumibot.tools.metamask_client import MetaMaskAPIError, MetaMaskClient

SIGNER = Account.from_key("0x" + "02" * 32)
WALLET = SIGNER.address.lower()
META_BTC_ETH = {
    "universe": [
        {"name": "BTC", "szDecimals": 5, "maxLeverage": 40},
        {"name": "ETH", "szDecimals": 4, "maxLeverage": 25},
    ]
}


def _reply(payload):
    resp = Mock(status_code=200)
    resp.json.return_value = payload
    return resp


def _make_venue():
    return HyperliquidVenue("testnet", session=Mock(spec=requests.Session))


def _make_broker(mode="perp", extra_config=None):
    client = Mock(spec=MetaMaskClient)
    client.verify.return_value = None
    client.orders.return_value = []
    client.account.return_value = {
        "currency": "USDC",
        "cash": "1000",
        "positionsValue": "0",
        "equity": "1000",
        "positions": [],
    }
    client.price.return_value = Decimal("50000")
    client.prices.return_value = {"BTC": Decimal("50000"), "ETH": Decimal("3000")}
    client.quote.return_value = {
        "symbol": "BTC",
        "bid": Decimal("49995"),
        "ask": Decimal("50005"),
        "bidSize": Decimal("1.5"),
        "askSize": Decimal("2.0"),
        "time": 1700000000000,
    }
    config = {"MODE": mode, "NETWORK": "testnet", **(extra_config or {})}
    with patch("lumibot.brokers.metamask.MetaMaskClient", return_value=client):
        broker = MetaMask(config, connect_stream=False)
    return broker, client


def _make_engine(tmp_path, max_order_usd="500", max_daily_usd="2000"):
    journal = Journal(tmp_path / "broker.sqlite")
    journal.bind(
        {
            "id": "agent",
            "wallet": WALLET,
            "network": "testnet",
            "enabled": True,
            "expiresAt": int(time.time() * 1000) + 86400000,
            "policyDigest": "b" * 64,
            "markets": ["BTC", "ETH"],
            "chains": [421614],
            "assets": [],
            "maxOrderUsd": max_order_usd,
            "maxDailyUsd": max_daily_usd,
            "maxLeverage": 10,
            "maxSlippageBps": 50,
        }
    )
    provider = Mock()
    provider.credentials.return_value = Credentials("project", "access")
    provider.policy.return_value = GuardPolicy("policy", "b" * 64)
    venue = HyperliquidVenue("testnet", session=Mock(spec=requests.Session))
    venue.market = Mock(return_value=Market("BTC", 0, 5, 40))
    venue.price = Mock(return_value=Decimal("50000"))
    venue.fee_rate = Mock(return_value=Decimal("0.00045"))
    venue.account = Mock(return_value={"accountMode": "standard", "currency": "USDC", "cash": "1000", "equity": "1000"})
    venue.submit_action = Mock()
    venue.order_status = Mock()
    venue.frontend_open_orders = Mock(return_value=[])
    venue.fills = Mock(return_value=[])

    def sign(request_id, wallet, network, data, action):
        sig = "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x")
        return SignatureJob(request_id, wallet, "SIGNED", sig)

    provider.submit_signature.side_effect = sign
    engine = ExecutionEngine(journal, provider, venue, "agent")
    return engine, journal, provider, venue


# ---------------------------------------------------------------------------
# 1. Hyperliquid SDK wire parity, l2Book, batched prices, and 14 intervals
# ---------------------------------------------------------------------------


def test_trigger_and_grouped_actions_match_official_sdk_action_hash():
    venue = _make_venue()

    def dispatch(method, url, **kwargs):
        payload = kwargs["json"]
        if payload["type"] == "meta":
            return _reply(META_BTC_ETH)
        if payload["type"] == "metaAndAssetCtxs":
            return _reply([META_BTC_ETH, [{"markPx": "50000"}, {"markPx": "3000"}]])
        raise AssertionError(f"Unexpected info payload: {payload}")

    venue.session.request.side_effect = dispatch
    cloid_entry = "0x" + "11" * 16
    cloid_tp = "0x" + "22" * 16
    cloid_sl = "0x" + "33" * 16

    # Standalone stop (market trigger)
    stop_action = venue.order_action(
        "BTC",
        "sell",
        Decimal("0.01"),
        "stop",
        None,
        "gtc",
        True,
        cloid_entry,
        slippage_bps=50,
        stop_price=Decimal("48000"),
        tpsl="sl",
    )
    assert stop_action["grouping"] == "na"
    wire = stop_action["orders"][0]
    assert list(wire.keys()) == ["a", "b", "p", "s", "r", "t", "c"]
    assert wire["t"] == {"trigger": {"isMarket": True, "triggerPx": "48000", "tpsl": "sl"}}
    assert Decimal(wire["p"]) < Decimal("48000")  # slippage-bounded sell below trigger
    typed = action_typed_data(stop_action, 1700000000000, "testnet", expires_after=1700000060000)
    assert (
        typed["message"]["connectionId"]
        == "0x" + sdk_action_hash(stop_action, None, 1700000000000, 1700000060000).hex()
    )

    # Standalone stop_limit
    sl_action = venue.order_action(
        "BTC",
        "sell",
        Decimal("0.01"),
        "stop_limit",
        Decimal("47800"),
        "gtc",
        True,
        cloid_entry,
        stop_price=Decimal("48000"),
        tpsl="sl",
    )
    sl_wire = sl_action["orders"][0]
    assert sl_wire["p"] == "47800"
    assert sl_wire["t"] == {"trigger": {"isMarket": False, "triggerPx": "48000", "tpsl": "sl"}}
    typed_sl = action_typed_data(sl_action, 1700000000001, "mainnet")
    assert typed_sl["message"]["connectionId"] == "0x" + sdk_action_hash(sl_action, None, 1700000000001, None).hex()

    # Grouped bracket (normalTpsl)
    bracket_action = venue.grouped_order_action(
        "BTC",
        [
            {
                "side": "buy",
                "quantity": Decimal("0.01"),
                "kind": "market",
                "limit": None,
                "stop_price": None,
                "tif": "ioc",
                "reduce_only": False,
                "client_id": cloid_entry,
                "tpsl": None,
                "as_trigger": False,
            },
            {
                "side": "sell",
                "quantity": Decimal("0.01"),
                "kind": "limit",
                "limit": Decimal("55000"),
                "stop_price": None,
                "tif": "gtc",
                "reduce_only": True,
                "client_id": cloid_tp,
                "tpsl": "tp",
                "as_trigger": True,
            },
            {
                "side": "sell",
                "quantity": Decimal("0.01"),
                "kind": "stop",
                "limit": None,
                "stop_price": Decimal("47000"),
                "tif": "gtc",
                "reduce_only": True,
                "client_id": cloid_sl,
                "tpsl": "sl",
                "as_trigger": True,
            },
        ],
        "normalTpsl",
        slippage_bps=50,
    )
    assert bracket_action["grouping"] == "normalTpsl"
    assert len(bracket_action["orders"]) == 3
    assert bracket_action["orders"][0]["t"] == {"limit": {"tif": "Ioc"}}
    assert bracket_action["orders"][1]["t"] == {"trigger": {"isMarket": False, "triggerPx": "55000", "tpsl": "tp"}}
    assert bracket_action["orders"][2]["t"] == {"trigger": {"isMarket": True, "triggerPx": "47000", "tpsl": "sl"}}
    typed_b = action_typed_data(bracket_action, 1700000000002, "testnet")
    assert typed_b["message"]["connectionId"] == "0x" + sdk_action_hash(bracket_action, None, 1700000000002, None).hex()

    # Grouped OCO (positionTpsl)
    oco_action = venue.grouped_order_action(
        "BTC",
        [
            {
                "side": "sell",
                "quantity": Decimal("0.01"),
                "kind": "limit",
                "limit": Decimal("55000"),
                "stop_price": None,
                "tif": "gtc",
                "reduce_only": True,
                "client_id": cloid_tp,
                "tpsl": "tp",
                "as_trigger": True,
            },
            {
                "side": "sell",
                "quantity": Decimal("0.01"),
                "kind": "stop_limit",
                "limit": Decimal("46800"),
                "stop_price": Decimal("47000"),
                "tif": "gtc",
                "reduce_only": True,
                "client_id": cloid_sl,
                "tpsl": "sl",
                "as_trigger": True,
            },
        ],
        "positionTpsl",
    )
    assert oco_action["grouping"] == "positionTpsl"
    assert len(oco_action["orders"]) == 2
    assert oco_action["orders"][1]["p"] == "46800"
    typed_oco = action_typed_data(oco_action, 1700000000003, "testnet")
    assert typed_oco["message"]["connectionId"] == "0x" + sdk_action_hash(oco_action, None, 1700000000003, None).hex()


def test_venue_prices_batched_single_http_call_and_l2_book():
    venue = _make_venue()
    venue.session.request.return_value = _reply([META_BTC_ETH, [{"markPx": "61000.5"}, {"markPx": "3100.25"}]])
    result = venue.prices(["BTC", "eth"])
    assert result == {"BTC": Decimal("61000.5"), "eth": Decimal("3100.25")}
    assert venue.session.request.call_count == 1

    venue.session.request.reset_mock()
    venue.session.request.side_effect = [
        _reply(META_BTC_ETH),
        _reply(
            {
                "coin": "BTC",
                "time": 1700000005000,
                "levels": [
                    [{"px": "60999.0", "sz": "2.5", "n": 3}],
                    [{"px": "61001.0", "sz": "1.25", "n": 2}],
                ],
            }
        ),
    ]
    book = venue.l2_book("BTC")
    assert book["bid"] == Decimal("60999.0")
    assert book["ask"] == Decimal("61001.0")
    assert book["bidSize"] == Decimal("2.5")
    assert book["askSize"] == Decimal("1.25")
    assert book["time"] == 1700000005000


@pytest.mark.parametrize(
    "interval,span_ms",
    [
        ("1m", 60_000),
        ("3m", 180_000),
        ("5m", 300_000),
        ("15m", 900_000),
        ("30m", 1_800_000),
        ("1h", 3_600_000),
        ("2h", 7_200_000),
        ("4h", 14_400_000),
        ("8h", 28_800_000),
        ("12h", 43_200_000),
        ("1d", 86_400_000),
        ("3d", 259_200_000),
        ("1w", 604_800_000),
        ("1M", 30 * 86_400_000),
    ],
)
def test_venue_candles_supports_all_14_hyperliquid_intervals(interval, span_ms):
    venue = _make_venue()
    candle = {
        "s": "BTC",
        "i": interval,
        "t": 1_700_000_000_000,
        "T": 1_700_000_000_000 + span_ms - 1,
        "o": "50000",
        "h": "51000",
        "l": "49500",
        "c": "50500",
        "v": "12.5",
    }
    venue.session.request.side_effect = [_reply(META_BTC_ETH), _reply([candle])]
    rows = venue.candles("BTC", interval, 1_700_000_000_000, 1_700_000_000_000 + span_ms + 10_000)
    assert rows == [[1_700_000_000_000, "50000", "51000", "49500", "50500", "12.5"]]


# ---------------------------------------------------------------------------
# 2. ExecutionEngine: stop, stop_limit, bracket, OTO, OCO, recovery & cancel
# ---------------------------------------------------------------------------


def test_perp_intent_validation_rejects_invalid_grouped_and_trigger_shapes():
    with pytest.raises(MetaMaskError, match="finite decimal"):
        PerpIntent.parse(
            {
                "kind": "perp",
                "id": "s1",
                "symbol": "BTC",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "leverage": 1,
                "reduceOnly": True,
                "tif": "gtc",
            }
        )
    with pytest.raises(MetaMaskError, match="child legs"):
        PerpIntent.parse(
            {
                "kind": "perp",
                "id": "b1",
                "symbol": "BTC",
                "side": "buy",
                "quantity": "0.01",
                "orderType": "market",
                "orderClass": "bracket",
                "leverage": 1,
                "reduceOnly": False,
                "tif": "ioc",
                "children": [],
            }
        )
    with pytest.raises(MetaMaskError, match="reduce-only"):
        PerpIntent.parse(
            {
                "kind": "perp",
                "id": "o1",
                "symbol": "BTC",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "limit",
                "orderClass": "oco",
                "leverage": 1,
                "reduceOnly": False,
                "tif": "gtc",
                "children": [
                    {
                        "id": "c1",
                        "side": "sell",
                        "quantity": "0.01",
                        "orderType": "limit",
                        "tpsl": "tp",
                        "limitPrice": "55000",
                        "reduceOnly": True,
                    },
                    {
                        "id": "c2",
                        "side": "sell",
                        "quantity": "0.01",
                        "orderType": "stop",
                        "tpsl": "sl",
                        "stopPrice": "45000",
                        "reduceOnly": True,
                    },
                ],
            }
        )


def test_engine_bracket_full_lifecycle_entry_fills_then_tp_fills_and_sl_cancels(tmp_path):
    engine, journal, provider, venue = _make_engine(tmp_path)
    bracket_req = {
        "kind": "perp",
        "id": "br-1",
        "symbol": "BTC",
        "side": "buy",
        "quantity": "0.005",
        "orderType": "limit",
        "orderClass": "bracket",
        "limitPrice": "50000",
        "leverage": 2,
        "reduceOnly": False,
        "tif": "gtc",
        "children": [
            {
                "id": "br-1-tp",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "limit",
                "tpsl": "tp",
                "limitPrice": "54000",
                "reduceOnly": True,
            },
            {
                "id": "br-1-sl",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "stop",
                "tpsl": "sl",
                "stopPrice": "47000",
                "reduceOnly": True,
            },
        ],
    }

    phase = {"step": "resting"}

    def submit_action(action, nonce, signature, expiry):
        if action["type"] == "updateLeverage":
            return {"status": "ok", "response": {"type": "default"}}
        assert action["grouping"] == "normalTpsl"
        assert len(action["orders"]) == 3
        return {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {"resting": {"oid": 100}},
                        "waitingForFill",
                        "waitingForFill",
                    ]
                },
            },
        }

    venue.submit_action.side_effect = submit_action

    def get_cloid(leg_id):
        plan = journal.effect("agent", "br-1", "plan")
        for leg in plan["legs"]:
            if leg["id"] == leg_id:
                return leg["clientOrderId"]
        return None

    def order_status(wallet, ref):
        c_entry, c_tp, c_sl = get_cloid("br-1"), get_cloid("br-1-tp"), get_cloid("br-1-sl")
        if ref in (100, c_entry):
            if phase["step"] == "resting":
                return {
                    "status": "order",
                    "order": {
                        "status": "open",
                        "order": {
                            "coin": "BTC",
                            "side": "B",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 100,
                            "cloid": c_entry,
                            "reduceOnly": False,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "origSz": "0.005",
                        "sz": "0",
                        "oid": 100,
                        "cloid": c_entry,
                        "reduceOnly": False,
                    },
                },
            }
        if ref in (101, c_tp):
            if phase["step"] == "resting":
                return {
                    "status": "order",
                    "order": {
                        "status": "waitingForFill",
                        "order": {
                            "coin": "BTC",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 101,
                            "cloid": c_tp,
                            "reduceOnly": True,
                        },
                    },
                }
            if phase["step"] == "entry_filled":
                return {
                    "status": "order",
                    "order": {
                        "status": "open",
                        "order": {
                            "coin": "BTC",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 101,
                            "cloid": c_tp,
                            "reduceOnly": True,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0",
                        "oid": 101,
                        "cloid": c_tp,
                        "reduceOnly": True,
                    },
                },
            }
        if ref in (102, c_sl):
            if phase["step"] == "resting":
                return {
                    "status": "order",
                    "order": {
                        "status": "waitingForFill",
                        "order": {
                            "coin": "BTC",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 102,
                            "cloid": c_sl,
                            "reduceOnly": True,
                        },
                    },
                }
            if phase["step"] == "entry_filled":
                return {
                    "status": "order",
                    "order": {
                        "status": "open",
                        "order": {
                            "coin": "BTC",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 102,
                            "cloid": c_sl,
                            "reduceOnly": True,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 102,
                        "cloid": c_sl,
                        "reduceOnly": True,
                    },
                },
            }
        return {"status": "unknownOid"}

    def open_orders(wallet):
        c_entry, c_tp, c_sl = get_cloid("br-1"), get_cloid("br-1-tp"), get_cloid("br-1-sl")
        if phase["step"] == "resting":
            return [
                {
                    "oid": 100,
                    "coin": "BTC",
                    "side": "B",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c_entry,
                    "reduceOnly": False,
                }
            ]
        if phase["step"] == "entry_filled":
            return [
                {
                    "oid": 101,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c_tp,
                    "reduceOnly": True,
                },
                {
                    "oid": 102,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c_sl,
                    "reduceOnly": True,
                },
            ]
        return []

    def fills(wallet, start, end):
        items = []
        if phase["step"] in ("entry_filled", "tp_filled"):
            items.append(
                {
                    "tid": 1,
                    "oid": 100,
                    "coin": "BTC",
                    "side": "B",
                    "sz": "0.005",
                    "px": "50000",
                    "fee": "0.05",
                    "feeToken": "USDC",
                }
            )
        if phase["step"] == "tp_filled":
            items.append(
                {
                    "tid": 2,
                    "oid": 101,
                    "coin": "BTC",
                    "side": "A",
                    "sz": "0.005",
                    "px": "54000",
                    "fee": "0.04",
                    "feeToken": "USDC",
                }
            )
        return items

    venue.order_status.side_effect = order_status
    venue.frontend_open_orders.side_effect = open_orders
    venue.fills.side_effect = fills

    # 1. Submit -> entry open, children waiting_for_fill
    op = engine.submit(bracket_req)
    assert op["state"] == "open"
    assert [c["state"] for c in op["childOperations"]] == ["waiting_for_fill", "waiting_for_fill"]

    # Duplicate child ID collision is rejected
    with pytest.raises(MetaMaskError, match="already belongs"):
        engine.submit(
            {
                "kind": "perp",
                "id": "br-1-tp",
                "symbol": "BTC",
                "side": "buy",
                "quantity": "0.001",
                "orderType": "market",
                "leverage": 1,
                "reduceOnly": False,
                "tif": "ioc",
            }
        )

    # 2. Entry fills -> children become live open legs
    phase["step"] = "entry_filled"
    op2 = engine.order("br-1")
    assert op2["state"] == "filled"
    assert op2["filledQuantity"] == "0.005"
    assert [c["state"] for c in op2["childOperations"]] == ["open", "open"]

    # Child lookup via engine.order(child_id) works
    tp_view = engine.order("br-1-tp")
    assert tp_view["id"] == "br-1-tp" and tp_view["parentId"] == "br-1" and tp_view["state"] == "open"

    # 3. TP fills and Hyperliquid cancels SL -> engine keeps reconciling even though entry was already filled!
    phase["step"] = "tp_filled"
    op3 = engine.orders()[0]
    assert op3["state"] == "filled"
    assert op3["childOperations"][0]["state"] == "filled"
    assert op3["childOperations"][0]["filledQuantity"] == "0.005"
    assert op3["childOperations"][0]["averagePrice"] == "54000"
    assert op3["childOperations"][1]["state"] == "canceled"


def test_engine_oco_container_cancel_and_child_cancel_and_local_pre_release_cancel(tmp_path):
    engine, journal, provider, venue = _make_engine(tmp_path)
    oco_req = {
        "kind": "perp",
        "id": "oco-1",
        "symbol": "BTC",
        "side": "sell",
        "quantity": "0.005",
        "orderType": "limit",
        "orderClass": "oco",
        "leverage": 1,
        "reduceOnly": True,
        "tif": "gtc",
        "children": [
            {
                "id": "oco-1-tp",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "limit",
                "tpsl": "tp",
                "limitPrice": "55000",
                "reduceOnly": True,
            },
            {
                "id": "oco-1-sl",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "stop",
                "tpsl": "sl",
                "stopPrice": "46000",
                "reduceOnly": True,
            },
        ],
    }
    canceled_oids = set()

    def submit_action(action, nonce, signature, expiry):
        if action["type"] == "cancel":
            for item in action["cancels"]:
                canceled_oids.add(item["o"])
            statuses = ["success"] * len(action["cancels"])
            return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": statuses}}}
        assert action["grouping"] == "positionTpsl"
        return {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 201}}, {"resting": {"oid": 202}}]}},
        }

    venue.submit_action.side_effect = submit_action
    venue.fills.return_value = []

    def cloid(leg_id):
        plan = journal.effect("agent", "oco-1", "plan")
        return next(leg["clientOrderId"] for leg in plan["legs"] if leg["id"] == leg_id)

    def order_status(wallet, ref):
        c1, c2 = cloid("oco-1-tp"), cloid("oco-1-sl")
        if ref in (201, c1):
            st = "canceled" if 201 in canceled_oids else "open"
            return {
                "status": "order",
                "order": {
                    "status": st,
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 201,
                        "cloid": c1,
                        "reduceOnly": True,
                    },
                },
            }
        if ref in (202, c2):
            st = "canceled" if 202 in canceled_oids else "open"
            return {
                "status": "order",
                "order": {
                    "status": st,
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 202,
                        "cloid": c2,
                        "reduceOnly": True,
                    },
                },
            }
        return {"status": "unknownOid"}

    def open_orders(wallet):
        c1, c2 = cloid("oco-1-tp"), cloid("oco-1-sl")
        rows = []
        if 201 not in canceled_oids:
            rows.append(
                {
                    "oid": 201,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c1,
                    "reduceOnly": True,
                }
            )
        if 202 not in canceled_oids:
            rows.append(
                {
                    "oid": 202,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c2,
                    "reduceOnly": True,
                }
            )
        return rows

    venue.order_status.side_effect = order_status
    venue.frontend_open_orders.side_effect = open_orders

    op = engine.submit(oco_req)
    assert op["state"] == "open"
    assert [c["state"] for c in op["childOperations"]] == ["open", "open"]

    # Cancel single child leg first
    child_canceled = engine.cancel("oco-1-sl")
    assert child_canceled["id"] == "oco-1-sl" and child_canceled["state"] == "canceled"
    assert 202 in canceled_oids and 201 not in canceled_oids

    # Cancel remaining OCO container -> cancels remaining open leg 201
    parent_canceled = engine.cancel("oco-1")
    assert parent_canceled["state"] == "canceled"
    assert 201 in canceled_oids

    # Local pre-release cancel on a prepared bracket proves not_released without touching provider/venue
    provider.submit_signature.reset_mock()
    venue.submit_action.reset_mock()
    br_local = {
        "kind": "perp",
        "id": "br-local",
        "symbol": "BTC",
        "side": "buy",
        "quantity": "0.005",
        "orderType": "limit",
        "orderClass": "bracket",
        "limitPrice": "49000",
        "leverage": 1,
        "reduceOnly": False,
        "tif": "gtc",
        "children": [
            {
                "id": "br-local-tp",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "limit",
                "tpsl": "tp",
                "limitPrice": "55000",
                "reduceOnly": True,
            },
        ],
    }
    with patch.object(engine, "_advance", side_effect=lambda x: x):
        prepared = engine.submit(br_local)
    assert prepared["state"] == "prepared"
    canceled = engine.cancel("br-local")
    assert canceled["state"] == "canceled"
    assert canceled["orderRelease"]["state"] == "not_released"
    assert canceled["childOperations"][0]["state"] == "canceled"
    provider.submit_signature.assert_not_called()
    venue.submit_action.assert_not_called()


# ---------------------------------------------------------------------------
# 3. MetaMaskData: get_quote, batched get_last_prices, and all 14 intervals
# ---------------------------------------------------------------------------


def test_metamask_data_quote_batched_prices_and_1M_interval():
    client = Mock(spec=MetaMaskClient)
    client.quote.return_value = {
        "symbol": "BTC",
        "bid": Decimal("49990"),
        "ask": Decimal("50010"),
        "bidSize": Decimal("3.0"),
        "askSize": Decimal("1.5"),
        "time": 1700000000000,
    }
    client.prices.return_value = {"BTC": Decimal("50000"), "ETH": Decimal("3000")}
    client.token_price.return_value = Decimal("2999.5")
    client.candles.return_value = [[1700000000000, "49000", "51000", "48500", "50500", "100"]]

    data = MetaMaskData({"NETWORK": "testnet"}, client=client)
    btc = Asset("BTC", asset_type="crypto_future")
    eth = Asset("ETH", asset_type="crypto_future")

    # Perpetual L2 quote
    quote = data.get_quote(btc)
    assert isinstance(quote, Quote)
    assert quote.bid == 49990.0 and quote.ask == 50010.0
    assert quote.bid_size == 3.0 and quote.ask_size == 1.5
    assert quote.price == 50000.0

    # Spot token quote fallback
    spot_eth = Asset("ETH", asset_type="crypto", chain_id=1, token_address="0x" + "0" * 40, decimals=18)
    spot_quote = data.get_quote(spot_eth)
    assert spot_quote.price == 2999.5

    # Batched get_last_prices preserves per-symbol availability
    prices = data.get_last_prices([btc, eth])
    client.prices.assert_called_once_with(["BTC", "ETH"], allow_missing=True)
    assert prices[btc] == 50000.0 and prices[eth] == 3000.0

    # Interval resolution including "1M" (month, NOT 1m!) and LumiBot aliases
    for timestep, expected_interval in [
        ("minute", "1m"),
        ("5 minutes", "5m"),
        ("15m", "15m"),
        ("4h", "4h"),
        ("hour", "1h"),
        ("day", "1d"),
        ("1w", "1w"),
        ("1M", "1M"),
    ]:
        client.candles.reset_mock()
        bars = data.get_historical_prices(btc, 1, timestep=timestep)
        assert bars is not None and len(bars.df) == 1
        assert client.candles.call_args.args[1] == expected_interval


# ---------------------------------------------------------------------------
# 4. MetaMask broker & Strategy integration
# ---------------------------------------------------------------------------


def test_strategy_defaults_quote_asset_to_usdc_for_metamask():
    broker, _ = _make_broker()

    class _Strat(Strategy):
        def on_trading_iteration(self):
            pass

    strat = _Strat(broker=broker)
    assert strat.quote_asset == Asset("USDC", Asset.AssetType.CRYPTO)

    # Explicit override still wins
    custom_quote = Asset("USD", Asset.AssetType.FOREX)
    strat2 = _Strat(broker=broker, quote_asset=custom_quote)
    assert strat2.quote_asset == custom_quote


@pytest.mark.parametrize(
    "side,expected_wire_side,expected_reduce_only",
    [
        (Order.OrderSide.BUY, "buy", False),
        (Order.OrderSide.BUY_TO_OPEN, "buy", False),
        (Order.OrderSide.BUY_TO_CLOSE, "buy", True),
        (Order.OrderSide.BUY_TO_COVER, "buy", True),
        (Order.OrderSide.SELL, "sell", False),
        (Order.OrderSide.SELL_TO_OPEN, "sell", False),
        (Order.OrderSide.SELL_SHORT, "sell", False),
        (Order.OrderSide.SELL_TO_CLOSE, "sell", True),
    ],
)
def test_order_side_variants_normalize_and_closing_sides_enforce_reduce_only(
    side, expected_wire_side, expected_reduce_only
):
    broker, client = _make_broker()
    client.submit.side_effect = lambda req: {"id": req["id"], "request": req, "state": "open", "filledQuantity": "0"}
    btc = Asset("BTC", asset_type="crypto_future", leverage=2)
    item = Order(
        "s",
        btc,
        Decimal("0.01"),
        side,
        order_type=Order.OrderType.MARKET,
        quote=Asset("USDC", asset_type="crypto"),
        custom_params={"reduce_only": False},
    )
    broker.submit_order(item)
    sent = client.submit.call_args.args[0]
    assert sent["side"] == expected_wire_side
    assert sent["reduceOnly"] is expected_reduce_only


def test_broker_bracket_and_oco_tracking_polling_restore_and_no_double_fill():
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future", leverage=3)
    usdc = Asset("USDC", asset_type="crypto")

    # Submit OCO order without position_filled=True so status transitions are genuinely tested
    oco = Order(
        "s",
        btc,
        Decimal("0.01"),
        Order.OrderSide.SELL,
        order_class=Order.OrderClass.OCO,
        limit_price=55000,
        stop_price=46000,
        quote=usdc,
    )
    assert len(oco.child_orders) == 2
    tp_child, sl_child = oco.child_orders
    assert broker._flatten_order(oco) == [oco, tp_child, sl_child]

    def submit_side_effect(req):
        return {
            "id": req["id"],
            "request": req,
            "state": "open",
            "filledQuantity": "0",
            "averagePrice": None,
            "childOperations": [
                {
                    **c,
                    "parentId": req["id"],
                    "state": "open",
                    "filledQuantity": "0",
                    "averagePrice": None,
                    "providerOrderId": str(300 + i),
                }
                for i, c in enumerate(req["children"])
            ],
        }

    client.submit.side_effect = submit_side_effect
    broker.submit_order(oco)
    assert tp_child.parent_identifier == str(oco.identifier)
    assert sl_child.parent_identifier == str(oco.identifier)
    assert broker.get_tracked_order(tp_child.identifier) is tp_child
    assert broker.get_tracked_order(sl_child.identifier) is sl_child
    # Once tracked, _flatten_order skips already-tracked orders so _wait_for_orders never duplicates them.
    assert broker._flatten_order(oco) == []
    # Container must be non-terminal before polling completes the fill.
    assert not oco.is_filled()
    assert oco.is_active()
    assert oco in broker._new_orders.get_list()

    # Polling reports TP filled and SL canceled -> ONLY TP emits a fill event (no duplicate on container!)
    fill_events = []
    orig_trade_event = broker._process_trade_event

    def spy_trade_event(order_obj, event, *args, **kwargs):
        if event == broker.FILLED_ORDER:
            fill_events.append((order_obj.identifier, kwargs.get("filled_quantity"), kwargs.get("price")))
        return orig_trade_event(order_obj, event, *args, **kwargs)

    broker._process_trade_event = spy_trade_event
    req = client.submit.call_args.args[0]
    client.orders.return_value = [
        {
            "id": req["id"],
            "request": req,
            "state": "filled",
            "filledQuantity": "0",
            "averagePrice": None,
            "childOperations": [
                {
                    **req["children"][0],
                    "parentId": req["id"],
                    "state": "filled",
                    "filledQuantity": "0.01",
                    "averagePrice": "55000",
                    "providerOrderId": "300",
                    "fees": [{"asset": "USDC", "amount": "0.05"}],
                },
                {
                    **req["children"][1],
                    "parentId": req["id"],
                    "state": "canceled",
                    "filledQuantity": "0",
                    "averagePrice": None,
                    "providerOrderId": "301",
                    "fees": [],
                },
            ],
        }
    ]
    broker.do_polling()
    assert fill_events == [(tp_child.identifier, Decimal("0.01"), Decimal("55000"))]
    assert tp_child.is_filled() and tp_child.trade_cost == pytest.approx(0.05)
    assert sl_child.is_canceled()
    assert oco.is_filled()
    assert oco.status == Order.OrderStatus.FILLED
    assert oco not in broker._new_orders.get_list()

    # Restart restore via _parse_broker_order preserves parent + linked children (with averagePrice=None)
    broker2, _ = _make_broker()
    restored = broker2._parse_broker_order(client.orders.return_value[0], "s")
    assert restored.order_class == Order.OrderClass.OCO
    assert len(restored.child_orders) == 2
    assert restored.child_orders[0].parent_identifier == str(restored.identifier)
    assert restored.child_orders[0].dependent_order is restored.child_orders[1]
    assert restored.child_orders[0].is_filled()
    assert restored.child_orders[1].is_canceled()


def test_smart_limit_submission_and_close_position_decimal_and_spot_guards():
    broker, client = _make_broker()
    client.submit.side_effect = lambda req: {"id": req["id"], "request": req, "state": "open", "filledQuantity": "0"}
    btc = Asset("BTC", asset_type="crypto_future", leverage=2)

    class _SmartStrat(Strategy):
        def on_trading_iteration(self):
            pass

    strat = _SmartStrat(broker=broker)
    smart = strat.create_order(
        btc,
        Decimal("0.01"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=SmartLimitConfig(step_seconds=5, final_price_pct=1.0),
    )
    assert broker.supports_order(smart).mode == "simulated"
    submitted = strat.submit_order(smart)
    assert submitted is not None
    # SMART_LIMIT converts to a LIMIT order priced from MetaMaskData.get_quote() bid/ask
    sent = client.submit.call_args.args[0]
    assert sent["orderType"] == "limit"
    assert Decimal("49995") <= Decimal(sent["limitPrice"]) <= Decimal("50005")
    with pytest.raises(NotImplementedError, match="Cancel and confirm"):
        broker._modify_order(submitted, limit_price=50002)

    # close_position with fractional Decimal quantity works without Decimal*float TypeError
    client.submit.reset_mock()
    broker._filled_positions.append(Position(strat._name, btc, Decimal("0.02")))
    closed = broker.close_position(strat._name, btc, fraction=0.5)
    assert closed is not None
    close_req = client.submit.call_args.args[0]
    assert close_req["side"] == "sell"
    assert close_req["reduceOnly"] is True
    assert Decimal(close_req["quantity"]) == Decimal("0.01")

    # cancel_order tolerates transport timeout without claiming canceled
    client.cancel.side_effect = requests.Timeout("timed out")
    returned = broker.cancel_order(submitted)
    assert returned is submitted and not submitted.is_canceled()

    # Spot mode guards close_position, sell_all, and do_polling
    spot_broker, spot_client = _make_broker(mode="spot")
    with pytest.raises(ValueError, match="swap"):
        spot_broker.close_position("s", btc)
    with pytest.raises(ValueError, match="swap"):
        spot_broker.sell_all("s")
    spot_broker.do_polling()
    spot_client.orders.assert_not_called()


def test_engine_stop_triggered_and_bracket_entry_canceled_unfilled(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)

    # 1. Standalone stop order moves through "triggered" (verified via frontendOpenOrders) -> "filled"
    stop_phase = {"step": "triggered"}

    def stop_submit(action, nonce, signature, expiry):
        assert action["grouping"] == "na"
        assert action["orders"][0]["t"]["trigger"]["isMarket"] is True
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 500}}]}}}

    venue.submit_action.side_effect = stop_submit

    def stop_cloid():
        return journal.effect("agent", "stp-1", "plan")["clientOrderId"]

    def stop_status(wallet, ref):
        c = stop_cloid()
        if stop_phase["step"] == "triggered":
            return {
                "status": "order",
                "order": {
                    "status": "triggered",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 500,
                        "cloid": c,
                        "reduceOnly": True,
                    },
                },
            }
        return {
            "status": "order",
            "order": {
                "status": "filled",
                "order": {
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0",
                    "oid": 500,
                    "cloid": c,
                    "reduceOnly": True,
                },
            },
        }

    def stop_open(wallet):
        c = stop_cloid()
        if stop_phase["step"] == "triggered":
            return [
                {
                    "oid": 500,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c,
                    "reduceOnly": True,
                }
            ]
        return []

    def stop_fills(wallet, start, end):
        if stop_phase["step"] == "filled":
            return [
                {
                    "tid": 10,
                    "oid": 500,
                    "coin": "BTC",
                    "side": "A",
                    "sz": "0.005",
                    "px": "47950",
                    "fee": "0.03",
                    "feeToken": "USDC",
                }
            ]
        return []

    venue.order_status.side_effect = stop_status
    venue.frontend_open_orders.side_effect = stop_open
    venue.fills.side_effect = stop_fills

    op = engine.submit(
        {
            "kind": "perp",
            "id": "stp-1",
            "symbol": "BTC",
            "side": "sell",
            "quantity": "0.005",
            "orderType": "stop",
            "stopPrice": "48000",
            "tpsl": "sl",
            "leverage": 1,
            "reduceOnly": True,
            "tif": "gtc",
        }
    )
    assert op["state"] == "open" and op["venueStatus"] == "triggered"

    stop_phase["step"] = "filled"
    filled_op = engine.order("stp-1")
    assert filled_op["state"] == "filled" and filled_op["averagePrice"] == "47950"

    # 2. Bracket entry canceled unfilled -> waiting child transitions to canceled
    br_phase = {"canceled": False}

    def br_submit(action, nonce, signature, expiry):
        if action["type"] == "updateLeverage":
            return {"status": "ok", "response": {"type": "default"}}
        if action["type"] == "cancel":
            br_phase["canceled"] = True
            return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}
        return {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 600}}, "waitingForFill"]}},
        }

    venue.submit_action.side_effect = br_submit
    venue.fills.side_effect = lambda *a, **kw: []

    def br_cloid(leg_id):
        plan = journal.effect("agent", "oto-1", "plan")
        return next(leg["clientOrderId"] for leg in plan["legs"] if leg["id"] == leg_id)

    def br_status(wallet, ref):
        ce, cs = br_cloid("oto-1"), br_cloid("oto-1-sl")
        if ref in (600, ce):
            st = "canceled" if br_phase["canceled"] else "open"
            return {
                "status": "order",
                "order": {
                    "status": st,
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "origSz": "0.004",
                        "sz": "0.004",
                        "oid": 600,
                        "cloid": ce,
                        "reduceOnly": False,
                    },
                },
            }
        if ref in (601, cs):
            return {
                "status": "order",
                "order": {
                    "status": "waitingForFill",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.004",
                        "sz": "0.004",
                        "oid": 601,
                        "cloid": cs,
                        "reduceOnly": True,
                    },
                },
            }
        return {"status": "unknownOid"}

    def br_open(wallet):
        ce = br_cloid("oto-1")
        if br_phase["canceled"]:
            return []
        return [
            {"oid": 600, "coin": "BTC", "side": "B", "origSz": "0.004", "sz": "0.004", "cloid": ce, "reduceOnly": False}
        ]

    venue.order_status.side_effect = br_status
    venue.frontend_open_orders.side_effect = br_open

    oto = engine.submit(
        {
            "kind": "perp",
            "id": "oto-1",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.004",
            "orderType": "limit",
            "orderClass": "oto",
            "limitPrice": "45000",
            "leverage": 1,
            "reduceOnly": False,
            "tif": "gtc",
            "children": [
                {
                    "id": "oto-1-sl",
                    "side": "sell",
                    "quantity": "0.004",
                    "orderType": "stop",
                    "tpsl": "sl",
                    "stopPrice": "43000",
                    "reduceOnly": True,
                },
            ],
        }
    )
    assert oto["state"] == "open"
    assert oto["childOperations"][0]["state"] == "waiting_for_fill"

    canceled_oto = engine.cancel("oto-1")
    assert canceled_oto["state"] == "canceled"
    assert canceled_oto["childOperations"][0]["state"] == "canceled"


def test_broker_sell_all_closes_long_and_short_and_polling_updates_feed_health():
    broker, client = _make_broker(extra_config={"MARKET": "us_futures"})
    assert broker.market == "us_futures"

    submitted = []

    def submit_ok(req):
        submitted.append(req)
        return {
            "id": req["id"],
            "request": req,
            "state": "filled",
            "filledQuantity": req["quantity"],
            "averagePrice": "50000",
            "fees": [],
        }

    client.submit.side_effect = submit_ok
    btc = Asset("BTC", asset_type="crypto_future")
    eth = Asset("ETH", asset_type="crypto_future")
    broker._filled_positions.append(Position("s", btc, Decimal("0.01")))
    broker._filled_positions.append(Position("s", eth, Decimal("-0.2")))
    broker.sell_all("s", cancel_open_orders=False)
    by_sym = {req["symbol"]: req for req in submitted}
    assert by_sym["BTC"]["side"] == "sell" and by_sym["BTC"]["reduceOnly"] is True
    assert by_sym["ETH"]["side"] == "buy" and by_sym["ETH"]["reduceOnly"] is True

    # Polling records order feed health true on success and false on error
    broker._record_order_feed = Mock()
    client.orders.return_value = []
    broker.do_polling()
    broker._record_order_feed.assert_called_with(True)

    broker._record_order_feed.reset_mock()
    client.orders.side_effect = RuntimeError("rpc down")
    with pytest.raises(RuntimeError, match="rpc down"):
        broker.do_polling()
    broker._record_order_feed.assert_called_with(False)


# ---------------------------------------------------------------------------
# 5. Deep review regression suite
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_tif", ["alo", "ioc"])
def test_trigger_tif_rejects_alo_and_ioc_before_signing(bad_tif):
    venue = _make_venue()
    venue.session.request.return_value = _reply(META_BTC_ETH)
    with pytest.raises(MetaMaskError) as exc:
        PerpIntent.parse(
            {
                "kind": "perp",
                "id": f"trig-{bad_tif}-1",
                "symbol": "BTC",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop_limit",
                "stopPrice": "48000",
                "limitPrice": "47900",
                "tpsl": "sl",
                "leverage": 1,
                "reduceOnly": True,
                "tif": bad_tif,
            }
        )
    assert exc.value.code == "INVALID_INTENT"

    with pytest.raises(MetaMaskError) as exc2:
        venue.order_action(
            "BTC",
            "sell",
            Decimal("0.01"),
            "stop_limit",
            Decimal("47900"),
            bad_tif,
            True,
            "0x" + "11" * 16,
            stop_price=Decimal("48000"),
            tpsl="sl",
        )
    assert exc2.value.code == "UNSUPPORTED_ORDER"

    # Regular alo limit orders and gtc triggers remain valid
    lim = venue.order_action("BTC", "buy", Decimal("0.01"), "limit", Decimal("45000"), "alo", False, "0x" + "22" * 16)
    assert lim["orders"][0]["t"] == {"limit": {"tif": "Alo"}}

    # Broker refuses stop_limit with non-gtc TIF before calling client.submit
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future")
    usdc = Asset("USDC", asset_type="crypto")
    bad_stop = Order(
        "s",
        btc,
        Decimal("0.01"),
        "sell",
        order_type="stop_limit",
        stop_price=48000,
        stop_limit_price=47900,
        time_in_force=bad_tif,
        quote=usdc,
        reduce_only=True,
    )
    with pytest.raises(ValueError, match="gtc"):
        broker._submit_order(bad_stop)
    client.submit.assert_not_called()


def test_strategy_get_historical_prices_native_timesteps_1M_int_timeshift_and_5000_bars():
    broker, client = _make_broker()
    client.candles.return_value = [[1700000000000, "49000", "51000", "48500", "50500", "100"]]
    data = MetaMaskData({"NETWORK": "testnet"}, client=client)
    broker.data_source = data
    btc = Asset("BTC", asset_type="crypto_future")

    class _Strat(Strategy):
        def on_trading_iteration(self):
            pass

    strat = _Strat(broker=broker)
    assert data.supports_native_timestep("4h")
    assert data.supports_native_timestep("1M")
    assert not data.supports_native_timestep("7 minutes")

    # Multi-bar interval uses native 4h directly (100 bars, NOT 24000 minute bars!)
    bars_4h = strat.get_historical_prices(btc, 100, timestep="4h")
    assert bars_4h is not None
    assert client.candles.call_args.args[1] == "4h"

    # "1M" preserves monthly interval through Strategy.get_historical_prices (NOT minute!)
    client.candles.reset_mock()
    bars_1M = strat.get_historical_prices(btc, 10, timestep="1M")
    assert bars_1M is not None
    assert client.candles.call_args.args[1] == "1M"

    # Lowercase "1m" stays 1-minute candles
    client.candles.reset_mock()
    bars_1m = strat.get_historical_prices(btc, 10, timestep="1m")
    assert bars_1m is not None
    assert client.candles.call_args.args[1] == "1m"

    # Integer timeshift works without TypeError, and length=5000 never overflows 5000 * interval_ms
    client.candles.reset_mock()
    bars_shift = data.get_historical_prices(btc, 5000, timestep="1h", timeshift=2)
    assert bars_shift is not None
    _, interval, start_ms, end_ms = client.candles.call_args.args
    assert interval == "1h"
    assert end_ms - start_ms <= 5000 * 3_600_000

    # Tuple spot asset raises NotImplementedError rather than AttributeError
    spot_eth = Asset("ETH", asset_type="crypto", chain_id=1, token_address="0x" + "0" * 40, decimals=18)
    usdc = Asset("USDC", asset_type="crypto")
    with pytest.raises(NotImplementedError, match="spot candles"):
        data.get_historical_prices((spot_eth, usdc), 5)


def test_strategy_spot_mode_keeps_usd_quote_and_contract_spec_and_order_size_rules():
    spot_broker, _ = _make_broker(mode="spot")

    class _Strat(Strategy):
        def on_trading_iteration(self):
            pass

    spot_strat = _Strat(broker=spot_broker)
    assert spot_strat.quote_asset == Asset("USD", Asset.AssetType.FOREX)
    assert spot_broker.get_order_size_rules(Asset("BTC", asset_type="crypto_future")) is None

    perp_broker, client = _make_broker(mode="perp")
    client.market.return_value = Market("BTC", 0, 5, 50)
    client.price.return_value = Decimal("50000")
    perp_broker.data_source = MetaMaskData({"NETWORK": "testnet"}, client=client)
    btc = Asset("BTC", asset_type="crypto_future", leverage=2)

    spec = perp_broker.get_contract_spec(btc)
    assert spec.base == "BTC" and spec.quote == "USDC" and spec.settle == "USDC"
    assert spec.quantity_unit == "base" and spec.contract_size == Decimal("1")
    exposure = perp_broker.get_contract_exposure(btc, Decimal("0.1"), Decimal("50000"))
    assert exposure["base"] == Decimal("0.1") and exposure["quote"] == Decimal("5000")
    assert perp_broker.get_quantity_for_notional(btc, Decimal("1000"), Decimal("50000")) == Decimal("0.02")

    # Hyperliquid enforces a price-dependent $10 min notional rather than a fixed base minimum,
    # so min_quantity is None while quantity_step reports the exact lot increment from szDecimals.
    rules = perp_broker.get_order_size_rules(btc)
    assert rules == {
        "min_quantity": None,
        "quantity_step": Decimal("0.00001"),
        "contract_size": Decimal("1"),
        "quantity_unit": "base",
    }
    assert perp_broker.polling_interval == perp_broker._poll_interval


def test_batched_prices_preserves_found_symbols_when_one_is_missing():
    venue = _make_venue()
    venue.session.request.return_value = _reply([META_BTC_ETH, [{"markPx": "50000"}, {"markPx": "3000"}]])
    found = venue.prices(["BTC", "UNKNOWN_COIN"], allow_missing=True)
    assert found == {"BTC": Decimal("50000")}
    with pytest.raises(MetaMaskError) as exc:
        venue.prices(["BTC", "UNKNOWN_COIN"])
    assert exc.value.code == "UNKNOWN_MARKET"


def test_engine_long_child_id_partial_fill_waiting_and_child_error_blocks_reserve(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)
    long_tp = "c" * 126
    long_sl = "d" * 126

    phase = {"entry": "partial", "child_err": False}

    def submit_fn(action, nonce, signature, expiry):
        if action["type"] == "updateLeverage":
            return {"status": "ok", "response": {"type": "default"}}
        return {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {"statuses": [{"resting": {"oid": 700}}, "waitingForFill", "waitingForFill"]},
            },
        }

    venue.submit_action.side_effect = submit_fn

    def cloid(leg_id):
        plan = journal.effect("agent", "br-long", "plan")
        return next(leg["clientOrderId"] for leg in plan["legs"] if leg["id"] == leg_id)

    def status_fn(wallet, ref):
        ce, ct, cs = cloid("br-long"), cloid(long_tp), cloid(long_sl)
        if ref in (700, ce):
            if phase["entry"] == "partial":
                return {
                    "status": "order",
                    "order": {
                        "status": "open",
                        "order": {
                            "coin": "BTC",
                            "side": "B",
                            "origSz": "0.005",
                            "sz": "0.003",
                            "oid": 700,
                            "cloid": ce,
                            "reduceOnly": False,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "origSz": "0.005",
                        "sz": "0.003",
                        "oid": 700,
                        "cloid": ce,
                        "reduceOnly": False,
                    },
                },
            }
        if ref in (701, ct):
            if phase["entry"] == "partial":
                return {"status": "unknownOid"}
            if phase["child_err"]:
                return {"status": "unknownOid"}
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 701,
                        "cloid": ct,
                        "reduceOnly": True,
                    },
                },
            }
        if ref in (702, cs):
            if phase["entry"] == "partial":
                return {
                    "status": "order",
                    "order": {
                        "status": "waitingForFill",
                        "order": {
                            "coin": "BTC",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 702,
                            "cloid": cs,
                            "reduceOnly": True,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 702,
                        "cloid": cs,
                        "reduceOnly": True,
                    },
                },
            }
        return {"status": "unknownOid"}

    def open_fn(wallet):
        if phase["entry"] == "partial":
            return [
                {
                    "oid": 700,
                    "coin": "BTC",
                    "side": "B",
                    "origSz": "0.005",
                    "sz": "0.003",
                    "cloid": cloid("br-long"),
                    "reduceOnly": False,
                }
            ]
        return []

    venue.order_status.side_effect = status_fn
    venue.frontend_open_orders.side_effect = open_fn
    venue.fills.return_value = [
        {
            "tid": 71,
            "oid": 700,
            "coin": "BTC",
            "side": "B",
            "sz": "0.002",
            "px": "50000",
            "fee": "0.05",
            "feeToken": "USDC",
        }
    ]

    # Partial fill on entry keeps dormant children in waiting_for_fill (even with 126-char child IDs!)
    op = engine.submit(
        {
            "kind": "perp",
            "id": "br-long",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.005",
            "orderType": "limit",
            "orderClass": "bracket",
            "limitPrice": "50000",
            "leverage": 1,
            "reduceOnly": False,
            "tif": "gtc",
            "children": [
                {
                    "id": long_tp,
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "limit",
                    "tpsl": "tp",
                    "limitPrice": "55000",
                    "reduceOnly": True,
                },
                {
                    "id": long_sl,
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "stop",
                    "tpsl": "sl",
                    "stopPrice": "45000",
                    "reduceOnly": True,
                },
            ],
        }
    )
    assert op["state"] == "partial_fill"
    assert [c["state"] for c in op["childOperations"]] == ["waiting_for_fill", "waiting_for_fill"]

    # After user cancel of partially-filled parent, child with unknownOid must NOT be inferred canceled
    # AND its reconciliation error must block new journal reservations!
    phase["entry"] = "canceled"
    phase["child_err"] = True
    polled = engine.order("br-long")
    assert polled["state"] == "canceled"
    assert polled["childOperations"][0]["state"] == "unknown"
    assert polled["childOperations"][0]["reconciliationErrorCode"] == "ORDER_UNRESOLVED"
    assert polled["childOperations"][1]["state"] == "canceled"

    with pytest.raises(MetaMaskError) as blocked:
        engine.submit(
            {
                "kind": "perp",
                "id": "blocked-after-child-err",
                "symbol": "BTC",
                "side": "buy",
                "quantity": "0.001",
                "orderType": "market",
                "leverage": 1,
                "reduceOnly": False,
                "tif": "ioc",
            }
        )
    assert blocked.value.code == "RECONCILIATION_REQUIRED"

    # Once child resolves cleanly from its own venue status, long-ID fill ledger saves cleanly without ValueError
    phase["child_err"] = False
    clean = engine.order("br-long")
    assert [c["state"] for c in clean["childOperations"]] == ["canceled", "canceled"]


def test_engine_waiting_for_trigger_in_transit_triggered_and_child_only_unreleased_cancel_refused(tmp_path):
    engine, journal, provider, venue = _make_engine(tmp_path)

    # 1. waitingForTrigger -> triggered (absent from frontendOpenOrders) -> filled
    phase = {"step": "waiting"}

    def submit_fn(action, nonce, signature, expiry):
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": ["waitingForTrigger"]}}}

    venue.submit_action.side_effect = submit_fn

    def status_fn(wallet, ref):
        c = journal.effect("agent", "trig-flow", "plan")["clientOrderId"]
        if phase["step"] == "waiting":
            return {
                "status": "order",
                "order": {
                    "status": "waitingForTrigger",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 800,
                        "cloid": c,
                        "reduceOnly": True,
                    },
                },
            }
        if phase["step"] == "in_transit":
            return {
                "status": "order",
                "order": {
                    "status": "triggered",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 800,
                        "cloid": c,
                        "reduceOnly": True,
                    },
                },
            }
        return {
            "status": "order",
            "order": {
                "status": "filled",
                "order": {
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0",
                    "oid": 800,
                    "cloid": c,
                    "reduceOnly": True,
                },
            },
        }

    def open_fn(wallet):
        c = journal.effect("agent", "trig-flow", "plan")["clientOrderId"]
        if phase["step"] == "waiting":
            return [
                {
                    "oid": 800,
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "cloid": c,
                    "reduceOnly": True,
                }
            ]
        return []

    def fills_fn(wallet, start, end):
        if phase["step"] == "filled":
            return [
                {
                    "tid": 88,
                    "oid": 800,
                    "coin": "BTC",
                    "side": "A",
                    "sz": "0.005",
                    "px": "47900",
                    "fee": "0.02",
                    "feeToken": "USDC",
                }
            ]
        return []

    venue.order_status.side_effect = status_fn
    venue.frontend_open_orders.side_effect = open_fn
    venue.fills.side_effect = fills_fn

    op = engine.submit(
        {
            "kind": "perp",
            "id": "trig-flow",
            "symbol": "BTC",
            "side": "sell",
            "quantity": "0.005",
            "orderType": "stop",
            "stopPrice": "48000",
            "tpsl": "sl",
            "leverage": 1,
            "reduceOnly": True,
            "tif": "gtc",
        }
    )
    assert op["state"] == "open" and op["venueStatus"] == "waitingForTrigger"

    phase["step"] = "in_transit"
    in_transit = engine.order("trig-flow")
    assert in_transit["state"] == "submitted" and in_transit["reconciliationErrorCode"] is None

    phase["step"] = "filled"
    done = engine.order("trig-flow")
    assert done["state"] == "filled" and done["averagePrice"] == "47900"

    # 2. Child-only cancel on an unreleased grouped order is refused; parent cancel succeeds locally
    def pending_mfa(request_id, wallet, network, typed_data, action):
        return SignatureJob(request_id, wallet, "AWAITING_MFA", None)

    provider.submit_signature.side_effect = pending_mfa
    provider.signature.side_effect = lambda req_id, wallet, network, origin="metamask": SignatureJob(
        req_id, wallet, "AWAITING_MFA", None
    )
    unreleased = engine.submit(
        {
            "kind": "perp",
            "id": "unrel-oco",
            "symbol": "BTC",
            "side": "sell",
            "quantity": "0.005",
            "orderType": "limit",
            "orderClass": "oco",
            "leverage": 1,
            "reduceOnly": True,
            "tif": "gtc",
            "children": [
                {
                    "id": "unrel-tp",
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "limit",
                    "tpsl": "tp",
                    "limitPrice": "55000",
                    "reduceOnly": True,
                },
                {
                    "id": "unrel-sl",
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "stop",
                    "tpsl": "sl",
                    "stopPrice": "45000",
                    "reduceOnly": True,
                },
            ],
        }
    )
    assert unreleased["state"] == "awaiting_approval"
    with pytest.raises(MetaMaskError) as child_cancel_err:
        engine.cancel("unrel-sl")
    assert child_cancel_err.value.code == "RECONCILIATION_REQUIRED"

    parent_canceled = engine.cancel("unrel-oco")
    assert parent_canceled["state"] == "canceled"
    assert [c["state"] for c in parent_canceled["childOperations"]] == ["canceled", "canceled"]


def test_engine_pending_cancel_not_released_when_target_child_errored_and_sibling_live(tmp_path):
    engine, journal, provider, venue = _make_engine(tmp_path)
    submit_calls = []

    def submit_fn(action, nonce, signature, expiry):
        submit_calls.append(action["type"])
        return {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 901}}, {"resting": {"oid": 902}}]}},
        }

    venue.submit_action.side_effect = submit_fn
    venue.fills.return_value = []
    errored_a = {"flag": False}

    def cloid(leg_id):
        plan = journal.effect("agent", "oco-pend", "plan")
        return next(leg["clientOrderId"] for leg in plan["legs"] if leg["id"] == leg_id)

    def status_fn(wallet, ref):
        ca, cb = cloid("oco-a"), cloid("oco-b")
        if ref in (901, ca):
            if errored_a["flag"]:
                # Identity mismatch on child A!
                return {
                    "status": "order",
                    "order": {
                        "status": "open",
                        "order": {
                            "coin": "ETH",
                            "side": "A",
                            "origSz": "0.005",
                            "sz": "0.005",
                            "oid": 901,
                            "cloid": ca,
                            "reduceOnly": True,
                        },
                    },
                }
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "order": {
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "oid": 901,
                        "cloid": ca,
                        "reduceOnly": True,
                    },
                },
            }
        return {
            "status": "order",
            "order": {
                "status": "open",
                "order": {
                    "coin": "BTC",
                    "side": "A",
                    "origSz": "0.005",
                    "sz": "0.005",
                    "oid": 902,
                    "cloid": cb,
                    "reduceOnly": True,
                },
            },
        }

    def open_fn(wallet):
        ca, cb = cloid("oco-a"), cloid("oco-b")
        return [
            {"oid": 901, "coin": "BTC", "side": "A", "origSz": "0.005", "sz": "0.005", "cloid": ca, "reduceOnly": True},
            {"oid": 902, "coin": "BTC", "side": "A", "origSz": "0.005", "sz": "0.005", "cloid": cb, "reduceOnly": True},
        ]

    venue.order_status.side_effect = status_fn
    venue.frontend_open_orders.side_effect = open_fn

    op = engine.submit(
        {
            "kind": "perp",
            "id": "oco-pend",
            "symbol": "BTC",
            "side": "sell",
            "quantity": "0.005",
            "orderType": "limit",
            "orderClass": "oco",
            "leverage": 1,
            "reduceOnly": True,
            "tif": "gtc",
            "children": [
                {
                    "id": "oco-a",
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "limit",
                    "tpsl": "tp",
                    "limitPrice": "55000",
                    "reduceOnly": True,
                },
                {
                    "id": "oco-b",
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "stop",
                    "tpsl": "sl",
                    "stopPrice": "45000",
                    "reduceOnly": True,
                },
            ],
        }
    )
    assert op["state"] == "open"

    # Start cancel on child A, pausing at AWAITING_MFA
    saved_submit = provider.submit_signature.side_effect
    provider.submit_signature.side_effect = lambda req_id, wallet, network, typed_data, action: SignatureJob(
        req_id, wallet, "AWAITING_MFA", None
    )
    engine.cancel("oco-a")
    assert submit_calls == ["order"]

    # Now provider signs cancel-901, BUT child A fails identity reconciliation while sibling B is still cleanly live!
    errored_a["flag"] = True
    provider.submit_signature.side_effect = saved_submit
    provider.signature.side_effect = lambda req_id, wallet, network, origin="metamask": SignatureJob(
        req_id, wallet, "SIGNED", "0x" + "11" * 65
    )
    polled = engine.order("oco-pend")
    # Pending cancel for child A MUST NOT be released to venue.submit_action!
    assert submit_calls == ["order"]
    assert polled["cancelErrorCode"] == "RECONCILIATION_REQUIRED"


def test_broker_bracket_restore_dependent_order_and_sequential_cancel_orders():
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future", leverage=2)
    usdc = Asset("USDC", asset_type="crypto")

    # Bracket restore preserves dependent_order between TP and SL legs
    bracket_snap = {
        "id": "br-rest",
        "state": "open",
        "filledQuantity": "0",
        "averagePrice": None,
        "request": {
            "kind": "perp",
            "id": "br-rest",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.01",
            "orderType": "limit",
            "orderClass": "bracket",
            "limitPrice": "45000",
            "leverage": 2,
            "reduceOnly": False,
            "tif": "gtc",
            "children": [
                {
                    "id": "br-tp",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "limit",
                    "tpsl": "tp",
                    "limitPrice": "55000",
                    "reduceOnly": True,
                },
                {
                    "id": "br-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "tpsl": "sl",
                    "stopPrice": "43000",
                    "reduceOnly": True,
                },
            ],
        },
        "childOperations": [
            {
                "id": "br-tp",
                "parentId": "br-rest",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "limit",
                "tpsl": "tp",
                "limitPrice": "55000",
                "state": "waiting_for_fill",
                "filledQuantity": "0",
                "averagePrice": None,
            },
            {
                "id": "br-sl",
                "parentId": "br-rest",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "tpsl": "sl",
                "stopPrice": "43000",
                "state": "waiting_for_fill",
                "filledQuantity": "0",
                "averagePrice": None,
            },
        ],
    }
    restored = broker._parse_broker_order(bracket_snap, "s")
    assert restored.child_orders[0].dependent_order is restored.child_orders[1]
    assert restored.child_orders[1].dependent_order is restored.child_orders[0]

    # cancel_orders runs parent before children and skips children already canceled by parent cancel
    canceled_ids = []

    def cancel_side_effect(oid):
        canceled_ids.append(oid)
        return {
            **bracket_snap,
            "state": "canceled",
            "childOperations": [{**c, "state": "canceled"} for c in bracket_snap["childOperations"]],
        }

    client.cancel.side_effect = cancel_side_effect
    # Pass children BEFORE parent to prove cancel_orders reorders parent first!
    broker.cancel_orders([restored.child_orders[0], restored.child_orders[1], restored])
    assert canceled_ids == ["br-rest"]
    assert restored.is_canceled() and all(c.is_canceled() for c in restored.child_orders)

    # cancel_order also handles native MetaMaskAPIError VENUE_OUTCOME_UNKNOWN safely
    other = Order("s", btc, Decimal("0.01"), "buy", quote=usdc)
    client.cancel.side_effect = MetaMaskAPIError("VENUE_OUTCOME_UNKNOWN", "lost response")
    assert broker.cancel_order(other) is other and not other.is_canceled()


def test_close_position_validates_decimal_fraction_without_float_rounding():
    broker, _ = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future")
    with pytest.raises(ValueError, match="fraction"):
        broker.close_position("s", btc, fraction=Decimal("1.0000000000000001"))
    with pytest.raises(ValueError, match="fraction"):
        broker.close_position("s", btc, fraction=Decimal("NaN"))


def test_cancel_orders_continues_after_individual_failure_and_preserves_status():
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future")
    usdc = Asset("USDC", asset_type="crypto")
    order_a = Order("s", btc, Decimal("0.01"), "buy", quote=usdc, identifier="ord-a")
    order_b = Order("s", btc, Decimal("0.01"), "buy", quote=usdc, identifier="ord-b")
    broker._new_orders.append(order_a)
    broker._new_orders.append(order_b)

    def cancel_side_effect(oid):
        if oid == "ord-a":
            raise MetaMaskAPIError("RECONCILIATION_REQUIRED", "cannot cancel A")
        return {
            "id": oid,
            "state": "canceled",
            "filledQuantity": "0",
            "averagePrice": None,
            "request": {"kind": "perp", "symbol": "BTC", "side": "buy", "quantity": "0.01"},
        }

    client.cancel.side_effect = cancel_side_effect
    broker.cancel_orders([order_a, order_b])
    assert not order_a.is_canceled()
    assert order_b.is_canceled()


def test_do_polling_records_failure_if_observe_raises():
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future")
    usdc = Asset("USDC", asset_type="crypto")
    order = Order("s", btc, Decimal("0.01"), "buy", quote=usdc, identifier="poll-reg")
    broker._new_orders.append(order)
    broker._observe(
        order,
        {
            "id": "poll-reg",
            "state": "partial_fill",
            "filledQuantity": "0.005",
            "averagePrice": "50000",
            "request": {"kind": "perp", "symbol": "BTC", "side": "buy", "quantity": "0.01"},
        },
    )
    broker._record_order_feed = Mock()
    client.orders.return_value = [
        {
            "id": "poll-reg",
            "state": "open",
            "filledQuantity": "0.001",
            "averagePrice": "50000",
            "request": {"kind": "perp", "symbol": "BTC", "side": "buy", "quantity": "0.01"},
        }
    ]
    with pytest.raises(MetaMaskAPIError, match="FILL_REGRESSION"):
        broker.do_polling()
    broker._record_order_feed.assert_called_once_with(False)


def test_data_source_get_last_prices_supports_tuple_keys_and_monthly_calendar_timeshift(monkeypatch):
    import lumibot.data_sources.metamask_data as mm_data_mod
    from lumibot.entities import AssetsMapping

    client = Mock()
    client.prices.return_value = {"BTC": Decimal("50000")}
    client.token_price.side_effect = [Decimal("3000"), Decimal("0.06")]
    ds = MetaMaskData({"NETWORK": "testnet"}, client=client)

    btc = Asset("BTC", asset_type="crypto_future")
    eth = Asset("ETH", asset_type="crypto", chain_id=1, token_address="0x" + "1" * 40, decimals=18)
    usdc = Asset("USDC", asset_type="crypto", chain_id=1, token_address="0x" + "2" * 40, decimals=6)
    wbtc = Asset("WBTC", asset_type="crypto", chain_id=1, token_address="0x" + "3" * 40, decimals=8)

    prices = ds.get_last_prices([(btc, usdc), (eth, usdc), (eth, wbtc)])
    assert isinstance(prices, AssetsMapping)
    assert prices[(btc, usdc)] == Decimal("50000")
    assert prices["BTC"] == Decimal("50000")
    assert prices[(eth, usdc)] == Decimal("3000")
    assert prices[(eth, wbtc)] == Decimal("0.06")
    # Ambiguous bare symbol for two distinct ETH pairs must not pick an arbitrary quote.
    assert "ETH" not in prices

    # Calendar-month shift: March 1, 2026 12:00 UTC shifted by 1 month lands on Feb 1, 2026 12:00 UTC.
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(mm_data_mod, "datetime", _FixedDatetime)
    client.candles.return_value = [
        [int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000), "1", "2", "1", "2", "10"]
    ]
    ds.get_historical_prices(btc, 1, timestep="1M", timeshift=1)
    _, _, _, end_ms = client.candles.call_args.args
    assert end_ms == int(datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)


def test_venue_prices_allow_missing_still_rejects_ambiguous_symbols():
    venue = _make_venue()
    ambig_meta = {
        "universe": [
            {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
            {"name": "btc", "szDecimals": 5, "maxLeverage": 50},
        ]
    }
    venue.session.request.return_value = _reply([ambig_meta, [{"markPx": "50000"}, {"markPx": "50001"}]])
    with pytest.raises(MetaMaskError) as exc:
        venue.prices(["BTC"], allow_missing=True)
    assert exc.value.code == "AMBIGUOUS_MARKET"


def test_engine_child_fill_step_h_prefix_disjoint_from_hashed_long_id():
    long_id = "c" * 125
    hashed = ExecutionEngine._child_fill_step(long_id)
    assert hashed.startswith("fills-h-")
    spoof_child = hashed.removeprefix("fills-")
    assert ExecutionEngine._child_fill_step(spoof_child) != hashed


def test_engine_pending_approval_release_uses_reservation_anchor_and_rejects_fee_increase(tmp_path):
    engine, journal, provider, venue = _make_engine(tmp_path, max_order_usd="1000", max_daily_usd="5000")
    real_sign = provider.submit_signature.side_effect
    venue.submit_action.return_value = {"status": "ok", "response": {"type": "default"}}

    def sign_or_pending(request_id, wallet, network, data, action):
        if action.get("type") == "updateLeverage":
            return real_sign(request_id, wallet, network, data, action)
        return SignatureJob("job-anchor-1", WALLET, "PENDING", None)

    provider.submit_signature.side_effect = sign_or_pending
    venue.price.return_value = Decimal("50000")
    venue.fee_rate.return_value = Decimal("0.00045")

    submitted = engine.submit(
        {
            "kind": "perp",
            "id": "stop-anchor-1",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.01",
            "orderType": "stop_limit",
            "stopPrice": "60000",
            "limitPrice": "59000",
            "tpsl": "sl",
            "leverage": 1,
            "reduceOnly": True,
            "tif": "gtc",
        }
    )
    assert submitted["state"] == "unknown"
    assert submitted["signatureOnly"] is True
    assert submitted["reservationAnchor"] == "60000"

    # Fee increases from 4.5 bps to 100 bps while approval is pending.
    # On executionLimitPrice (59000) alone it would fit inside reservedUsd (600.27),
    # but on reservationAnchor (60000) it is 606 > 600.27 -> must raise REQUOTE_REQUIRED!
    venue.fee_rate.return_value = Decimal("0.01")
    provider.signature.return_value = SignatureJob("job-anchor-1", WALLET, "SIGNED", "0x" + "11" * 65)
    after = engine.order("stop-anchor-1")
    assert after["state"] == "rejected"
    assert "REQUOTE_REQUIRED" in after["message"]
    assert journal.effect("agent", "stop-anchor-1", "order")["failureCode"] == "REQUOTE_REQUIRED"
    venue.submit_action.assert_not_called()


def test_engine_canceled_partial_entry_keeps_waiting_child_uncertain_and_blocks_reserve(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)
    op = {
        "id": "br-part-cancel",
        "agent": "agent",
        "request": {
            "kind": "perp",
            "id": "br-part-cancel",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.005",
            "orderType": "limit",
            "limitPrice": "50000",
            "tif": "gtc",
            "leverage": 2,
            "reduceOnly": False,
            "orderClass": "oto",
            "children": [
                {
                    "id": "br-part-tp",
                    "side": "sell",
                    "quantity": "0.005",
                    "orderType": "limit",
                    "limitPrice": "55000",
                    "tpsl": "tp",
                }
            ],
        },
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_000_000,
        "state": "open",
        "reservedUsd": "300",
        "filledQuantity": "0",
        "providerOrderId": "410",
        "fees": [],
        "hashes": [],
        "childOperations": [
            {
                "id": "br-part-tp",
                "parentId": "br-part-cancel",
                "side": "sell",
                "quantity": "0.005",
                "orderType": "limit",
                "tpsl": "tp",
                "limitPrice": "55000",
                "stopPrice": "55000",
                "state": "waiting_for_fill",
                "providerOrderId": "411",
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            }
        ],
    }
    journal.save(op)
    journal.save_effect(
        op,
        "plan",
        {
            "action": {"type": "order", "orders": [{}, {}], "grouping": "normalTpsl"},
            "clientOrderId": None,
            "ownerEpoch": engine.epoch,
            "binding": journal.binding("agent"),
            "wallet": WALLET,
            "network": "testnet",
            "projectId": "p",
            "marketIndex": 0,
            "isCross": True,
            "origin": "lumibot",
            "feeRate": "0.0005",
            "legs": [
                {"role": "entry", "id": "br-part-cancel", "clientOrderId": None},
                {"role": "child", "id": "br-part-tp", "clientOrderId": None},
            ],
        },
    )
    journal.save_effect(op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    venue.frontend_open_orders.return_value = []
    venue.fills.return_value = [
        {
            "oid": 410,
            "coin": "BTC",
            "side": "B",
            "sz": "0.002",
            "px": "50000",
            "tid": 9101,
            "fee": "0.05",
            "feeToken": "USDC",
        }
    ]

    def order_status(wallet, oid):
        if oid == 410:
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {
                        "oid": 410,
                        "coin": "BTC",
                        "side": "B",
                        "origSz": "0.005",
                        "sz": "0.003",
                        "reduceOnly": False,
                    },
                },
            }
        if oid == 411:
            return {
                "status": "order",
                "order": {
                    "status": "waitingForFill",
                    "order": {
                        "oid": 411,
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.005",
                        "sz": "0.005",
                        "reduceOnly": True,
                    },
                },
            }
        raise AssertionError(f"Unexpected oid: {oid}")

    venue.order_status.side_effect = order_status
    reconciled = engine.order("br-part-cancel")
    assert reconciled["state"] == "canceled"
    assert reconciled["filledQuantity"] == "0.002"
    # Child still reporting waitingForFill after partially-filled canceled parent must remain uncertain!
    assert reconciled["childOperations"][0]["state"] == "unknown"
    assert reconciled["childOperations"][0]["reconciliationErrorCode"] == "ORDER_STATUS_UNSUPPORTED"
    with pytest.raises(MetaMaskError) as exc:
        journal.reserve(
            "agent", {"kind": "perp", "id": "next-blocked"}, "100", expected_binding=journal.binding("agent")
        )
    assert exc.value.code == "RECONCILIATION_REQUIRED"


def test_engine_order_refreshes_clean_live_oids_between_pending_cancel_effects(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)
    op = {
        "id": "oco-multi-cancel",
        "agent": "agent",
        "request": {
            "kind": "perp",
            "id": "oco-multi-cancel",
            "symbol": "BTC",
            "side": "sell",
            "quantity": "0.01",
            "orderType": "limit",
            "tif": "gtc",
            "leverage": 2,
            "reduceOnly": True,
            "orderClass": "oco",
            "children": [
                {
                    "id": "oco-mc-tp",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "limit",
                    "limitPrice": "55000",
                    "tpsl": "tp",
                },
                {
                    "id": "oco-mc-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "stopPrice": "45000",
                    "tpsl": "sl",
                },
            ],
        },
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_000_000,
        "state": "open",
        "reservedUsd": "600",
        "filledQuantity": "0",
        "fees": [],
        "hashes": [],
        "childOperations": [
            {
                "id": "oco-mc-tp",
                "parentId": "oco-multi-cancel",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "limit",
                "tpsl": "tp",
                "state": "open",
                "providerOrderId": "501",
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            },
            {
                "id": "oco-mc-sl",
                "parentId": "oco-multi-cancel",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "tpsl": "sl",
                "state": "open",
                "providerOrderId": "502",
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            },
        ],
    }
    journal.save(op)
    journal.save_effect(
        op,
        "plan",
        {
            "action": {"type": "order", "orders": [{}, {}], "grouping": "positionTpsl"},
            "clientOrderId": None,
            "ownerEpoch": engine.epoch,
            "binding": journal.binding("agent"),
            "wallet": WALLET,
            "network": "testnet",
            "projectId": "p",
            "marketIndex": 0,
            "isCross": True,
            "origin": "lumibot",
            "feeRate": "0.0005",
            "legs": [
                {"role": "child", "id": "oco-mc-tp", "clientOrderId": None},
                {"role": "child", "id": "oco-mc-sl", "clientOrderId": None},
            ],
        },
    )
    journal.save_effect(op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    journal.save_effect(
        op,
        "cancel-501-1",
        {
            "phase": "provider",
            "state": "pending",
            "ownerEpoch": engine.epoch,
            "action": {"type": "cancel", "cancels": [{"a": 0, "o": 501}]},
        },
    )
    journal.save_effect(
        op,
        "cancel-501-502-2",
        {
            "phase": "provider",
            "state": "pending",
            "ownerEpoch": engine.epoch,
            "action": {"type": "cancel", "cancels": [{"a": 0, "o": 501}, {"a": 0, "o": 502}]},
        },
    )

    canceled_501 = [False]

    def open_orders(wallet):
        rows = [{"oid": 502, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0.01", "reduceOnly": True}]
        if not canceled_501[0]:
            rows.append({"oid": 501, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0.01", "reduceOnly": True})
        return rows

    def order_status(wallet, oid):
        if oid == 501:
            st = "canceled" if canceled_501[0] else "open"
            return {
                "status": "order",
                "order": {
                    "status": st,
                    "order": {
                        "oid": 501,
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.01",
                        "sz": "0.01",
                        "reduceOnly": True,
                    },
                },
            }
        if oid == 502:
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "order": {
                        "oid": 502,
                        "coin": "BTC",
                        "side": "A",
                        "origSz": "0.01",
                        "sz": "0.01",
                        "reduceOnly": True,
                    },
                },
            }
        raise AssertionError(f"Unexpected oid: {oid}")

    venue.frontend_open_orders.side_effect = open_orders
    venue.order_status.side_effect = order_status
    venue.fills.return_value = []
    ran_steps = []

    def fake_cancel_step(operation, plan, step, action):
        ran_steps.append(step)
        canceled_501[0] = True
        return operation

    engine._cancel_step = fake_cancel_step
    res = engine.order("oco-multi-cancel")
    # First cancel closed 501; second cancel targeting [501, 502] must NOT run because 501 is no longer cleanly live!
    assert ran_steps == ["cancel-501-1"]
    assert res["cancelErrorCode"] == "RECONCILIATION_REQUIRED"


def test_engine_orders_does_not_skip_when_ledger_is_ahead_of_snapshot(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)
    op = {
        "id": "br-ledger-ahead",
        "agent": "agent",
        "request": {
            "kind": "perp",
            "id": "br-ledger-ahead",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.01",
            "orderType": "limit",
            "limitPrice": "50000",
            "tif": "gtc",
            "leverage": 2,
            "reduceOnly": False,
            "orderClass": "oto",
            "children": [
                {
                    "id": "br-la-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "stopPrice": "45000",
                    "tpsl": "sl",
                }
            ],
        },
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_000_000,
        "state": "filled",
        "reservedUsd": "600",
        "filledQuantity": "0.01",
        "averagePrice": "50000",
        "providerOrderId": "601",
        "fees": [{"asset": "USDC", "amount": "0.1"}],
        "hashes": [],
        "childOperations": [
            {
                "id": "br-la-sl",
                "parentId": "br-ledger-ahead",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "tpsl": "sl",
                "state": "canceled",
                "providerOrderId": "602",
                # Stale snapshot (0 filled) while durable fill ledger already has a late 0.004 fill!
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            }
        ],
    }
    journal.save(op)
    journal.save_effect(
        op,
        "plan",
        {
            "action": {"type": "order", "orders": [{}, {}], "grouping": "normalTpsl"},
            "clientOrderId": None,
            "ownerEpoch": engine.epoch,
            "binding": journal.binding("agent"),
            "wallet": WALLET,
            "network": "testnet",
            "projectId": "p",
            "marketIndex": 0,
            "isCross": True,
            "origin": "lumibot",
            "feeRate": "0.0005",
            "legs": [
                {"role": "entry", "id": "br-ledger-ahead", "clientOrderId": None},
                {"role": "child", "id": "br-la-sl", "clientOrderId": None},
            ],
        },
    )
    journal.save_effect(op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    parent_fill = {
        "oid": 601,
        "coin": "BTC",
        "side": "B",
        "sz": "0.01",
        "px": "50000",
        "tid": 7101,
        "fee": "0.1",
        "feeToken": "USDC",
    }
    child_fill = {
        "oid": 602,
        "coin": "BTC",
        "side": "A",
        "sz": "0.004",
        "px": "45000",
        "tid": 7102,
        "fee": "0.05",
        "feeToken": "USDC",
    }
    journal.save_effect(op, "fills", {"wallet": WALLET, "network": "testnet", "items": [parent_fill]})
    journal.save_effect(
        op, engine._child_fill_step("br-la-sl"), {"wallet": WALLET, "network": "testnet", "items": [child_fill]}
    )
    # Omit child_fill from venue.fills so recovery must rely on the durable journal ledger.
    venue.fills.return_value = [parent_fill]
    venue.order_status.return_value = {
        "status": "order",
        "order": {
            "status": "canceled",
            "order": {"oid": 602, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0.006", "reduceOnly": True},
        },
    }

    listed = engine.orders()
    assert len(listed) == 1
    assert listed[0]["childOperations"][0]["filledQuantity"] == "0.004"
    assert listed[0]["childOperations"][0]["averagePrice"] == "45000"
    assert listed[0]["childOperations"][0]["fees"] == [{"asset": "USDC", "amount": "0.05"}]


def test_do_polling_isolates_child_regression_and_still_observes_subsequent_orders():
    broker, client = _make_broker()
    btc = Asset("BTC", asset_type="crypto_future")
    eth = Asset("ETH", asset_type="crypto_future")
    usdc = Asset("USDC", asset_type="crypto")

    # Track only the child of an OTO order (parent untracked) plus a second unrelated ETH order.
    child_order = Order("s", btc, Decimal("0.01"), "sell", quote=usdc, identifier="oto-child-reg")
    other_order = Order("s", eth, Decimal("0.1"), "buy", quote=usdc, identifier="eth-valid-fill")
    broker._new_orders.append(child_order)
    broker._new_orders.append(other_order)
    broker._observe(
        child_order,
        {
            "id": "oto-child-reg",
            "state": "partial_fill",
            "filledQuantity": "0.005",
            "averagePrice": "50000",
            "request": {"kind": "perp", "symbol": "BTC", "side": "sell", "quantity": "0.01"},
        },
    )
    broker._record_order_feed = Mock()
    client.orders.return_value = [
        {
            "id": "oto-parent-untracked",
            "state": "filled",
            "filledQuantity": "0.01",
            "averagePrice": "50000",
            "request": {"kind": "perp", "symbol": "BTC", "side": "buy", "quantity": "0.01"},
            "childOperations": [
                {
                    "id": "oto-child-reg",
                    "parentId": "oto-parent-untracked",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "limit",
                    "state": "open",
                    "filledQuantity": "0.001",  # regressed from 0.005
                    "averagePrice": "50000",
                }
            ],
        },
        {
            "id": "eth-valid-fill",
            "state": "filled",
            "filledQuantity": "0.1",
            "averagePrice": "3000",
            "request": {"kind": "perp", "symbol": "ETH", "side": "buy", "quantity": "0.1"},
        },
    ]
    with pytest.raises(MetaMaskAPIError, match="FILL_REGRESSION"):
        broker.do_polling()
    # Later unrelated order MUST still have been observed, and feed marked unhealthy.
    assert other_order.is_filled()
    broker._record_order_feed.assert_called_once_with(False)


def test_engine_orders_bounds_discovery_window_for_settled_closed_groups(tmp_path, monkeypatch):
    import lumibot.tools.metamask.engine as engine_mod

    engine, journal, _, venue = _make_engine(tmp_path)
    poll1_sec = 1_750_000_000.0
    poll2_sec = poll1_sec + 120.0
    poll3_sec = poll2_sec + 10.0
    now_sec = [poll1_sec]
    monkeypatch.setattr(engine_mod.time, "time", lambda: now_sec[0])
    old_ts = int(poll1_sec * 1000) - 10 * 86_400_000  # 10 days ago
    op = {
        "id": "br-settled-old",
        "agent": "agent",
        "request": {
            "kind": "perp",
            "id": "br-settled-old",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.01",
            "orderType": "limit",
            "limitPrice": "50000",
            "tif": "gtc",
            "leverage": 2,
            "reduceOnly": False,
            "orderClass": "bracket",
            "children": [
                {
                    "id": "br-so-tp",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "limit",
                    "limitPrice": "55000",
                    "tpsl": "tp",
                },
                {
                    "id": "br-so-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "stopPrice": "45000",
                    "tpsl": "sl",
                },
            ],
        },
        "createdAt": old_ts,
        "updatedAt": old_ts + 1000,
        "state": "filled",
        "reservedUsd": "600",
        "filledQuantity": "0.01",
        "averagePrice": "50000",
        "providerOrderId": "801",
        "fees": [{"asset": "USDC", "amount": "0.1"}],
        "hashes": [],
        "childOperations": [
            {
                "id": "br-so-tp",
                "parentId": "br-settled-old",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "limit",
                "tpsl": "tp",
                "state": "filled",
                "providerOrderId": "802",
                "filledQuantity": "0.01",
                "averagePrice": "55000",
                "fees": [{"asset": "USDC", "amount": "0.1"}],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            },
            {
                "id": "br-so-sl",
                "parentId": "br-settled-old",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "tpsl": "sl",
                "state": "canceled",
                "providerOrderId": "803",
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            },
        ],
    }
    journal.save(op)
    journal.save_effect(
        op,
        "plan",
        {
            "action": {"type": "order", "orders": [{}, {}, {}], "grouping": "normalTpsl"},
            "clientOrderId": None,
            "ownerEpoch": engine.epoch,
            "binding": journal.binding("agent"),
            "wallet": WALLET,
            "network": "testnet",
            "projectId": "p",
            "marketIndex": 0,
            "isCross": True,
            "origin": "lumibot",
            "feeRate": "0.0005",
            "legs": [
                {"role": "entry", "id": "br-settled-old", "clientOrderId": None},
                {"role": "child", "id": "br-so-tp", "clientOrderId": None},
                {"role": "child", "id": "br-so-sl", "clientOrderId": None},
            ],
        },
    )
    journal.save_effect(op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    p_fill = {
        "oid": 801,
        "coin": "BTC",
        "side": "B",
        "sz": "0.01",
        "px": "50000",
        "tid": 8101,
        "fee": "0.1",
        "feeToken": "USDC",
    }
    tp_fill = {
        "oid": 802,
        "coin": "BTC",
        "side": "A",
        "sz": "0.01",
        "px": "55000",
        "tid": 8102,
        "fee": "0.1",
        "feeToken": "USDC",
    }
    journal.save_effect(op, "fills", {"wallet": WALLET, "network": "testnet", "items": [p_fill]})
    journal.save_effect(
        op, engine._child_fill_step("br-so-tp"), {"wallet": WALLET, "network": "testnet", "items": [tp_fill]}
    )
    journal.save_effect(op, engine._child_fill_step("br-so-sl"), {"wallet": WALLET, "network": "testnet", "items": []})
    venue.fills.return_value = []

    # First poll scans and establishes the durable discovery cursor at poll1_ms.
    first = engine.orders()
    assert len(first) == 1
    assert venue.fills.call_count == 1
    saved_cursor = int(journal.metadata("closed_fill_cursor:agent"))
    assert saved_cursor == int(poll1_sec * 1000)
    venue.fills.reset_mock()

    # Second poll skips the fully-exited bracket completely (zero fills() calls for settled-only wallet!).
    now_sec[0] = poll2_sec
    second = engine.orders()
    assert len(second) == 1
    venue.fills.assert_not_called()

    # Now add a non-exited canceled-child OTO created 10 days ago: discovery window MUST start at
    # exactly saved_cursor - 60_000 (the 60s overlap) and end at poll2_ms!
    oto_op = {
        **op,
        "id": "oto-canceled-old",
        "request": {
            **op["request"],
            "id": "oto-canceled-old",
            "orderClass": "oto",
            "children": [
                {
                    "id": "oto-co-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "stopPrice": "45000",
                    "tpsl": "sl",
                }
            ],
        },
        "childOperations": [
            {**op["childOperations"][1], "id": "oto-co-sl", "parentId": "oto-canceled-old", "providerOrderId": "804"}
        ],
    }
    journal.save(oto_op)
    journal.save_effect(oto_op, "plan", journal.effect("agent", "br-settled-old", "plan"))
    journal.save_effect(oto_op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    journal.save_effect(oto_op, "fills", {"wallet": WALLET, "network": "testnet", "items": [p_fill]})
    journal.save_effect(
        oto_op, engine._child_fill_step("oto-co-sl"), {"wallet": WALLET, "network": "testnet", "items": []}
    )
    engine.orders()
    assert venue.fills.call_count == 1
    _, start_arg, end_arg = venue.fills.call_args.args
    assert start_arg == saved_cursor - 60_000
    assert end_arg == int(poll2_sec * 1000)

    # When venue.fills fails, settled fully-exited groups remain skipped in the fallback path.
    venue.fills.reset_mock()
    venue.fills.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "temporary outage")
    called_order_ids = []
    orig_order = engine.order

    def spy_order(op_id, **kwargs):
        called_order_ids.append(op_id)
        return orig_order(op_id, **kwargs)

    engine.order = spy_order
    venue.order_status.return_value = {
        "status": "order",
        "order": {
            "status": "canceled",
            "order": {"oid": 804, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0.01", "reduceOnly": True},
        },
    }
    engine.orders()
    assert "br-settled-old" not in called_order_ids
    assert "oto-canceled-old" in called_order_ids

    # If _execution is held so order() skips reconciliation of a new fill, cursor MUST NOT advance.
    cursor_before = journal.metadata("closed_fill_cursor:agent")
    now_sec[0] = poll3_sec
    new_fill = {
        "oid": 804,
        "coin": "BTC",
        "side": "A",
        "sz": "0.002",
        "px": "45000",
        "tid": 8199,
        "fee": "0.01",
        "feeToken": "USDC",
    }
    venue.fills.side_effect = None
    venue.fills.return_value = [new_fill]
    dispatched_obs = []

    def spy_locked_order(op_id, **kwargs):
        dispatched_obs.append((op_id, kwargs.get("_observation")))
        return orig_order(op_id, **kwargs)

    engine.order = spy_locked_order
    assert engine._execution.acquire(blocking=False)
    try:
        engine.orders()
    finally:
        engine._execution.release()
    assert any(op_id == "oto-canceled-old" and obs and new_fill in obs["fills"] for op_id, obs in dispatched_obs)
    assert journal.metadata("closed_fill_cursor:agent") == cursor_before

    # Once unlocked, the next scan records trade 8199 in the child ledger and then advances the cursor.
    engine.order = orig_order
    venue.fills.return_value = [p_fill, new_fill]

    def order_status_after_unlock(wallet, oid):
        if oid == 801:
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"oid": 801, "coin": "BTC", "side": "B", "origSz": "0.01", "sz": "0", "reduceOnly": False},
                },
            }
        return {
            "status": "order",
            "order": {
                "status": "canceled",
                "order": {"oid": 804, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0.008", "reduceOnly": True},
            },
        }

    venue.order_status.side_effect = order_status_after_unlock
    after_unlock = engine.orders()
    oto_after = next(row for row in after_unlock if row["id"] == "oto-canceled-old")
    assert oto_after["reconciliationErrorCode"] is None
    assert oto_after["childOperations"][0]["filledQuantity"] == "0.002"
    ledger = journal.effect("agent", "oto-canceled-old", engine._child_fill_step("oto-co-sl"))
    assert any(item.get("tid") == 8199 for item in ledger["items"])
    assert int(journal.metadata("closed_fill_cursor:agent")) == int(poll3_sec * 1000)

    # Backward clock step (2 min earlier than cursor): start falls back to conservative scan, start <= end.
    venue.fills.reset_mock()
    venue.fills.return_value = [p_fill, new_fill]
    now_sec[0] = poll3_sec - 120.0
    engine.orders()
    _, back_start, back_end = venue.fills.call_args.args
    assert back_start <= back_end
    assert back_start == old_ts - 5000


def test_engine_entry_lookup_error_still_reconciles_exit_and_blocks_cached_cancel(tmp_path):
    engine, journal, _, venue = _make_engine(tmp_path)
    op = {
        "id": "oto-entry-err",
        "agent": "agent",
        "request": {
            "kind": "perp",
            "id": "oto-entry-err",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.01",
            "orderType": "limit",
            "limitPrice": "50000",
            "tif": "gtc",
            "leverage": 2,
            "reduceOnly": False,
            "orderClass": "oto",
            "children": [
                {
                    "id": "oto-ee-sl",
                    "side": "sell",
                    "quantity": "0.01",
                    "orderType": "stop",
                    "stopPrice": "45000",
                    "tpsl": "sl",
                }
            ],
        },
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_000_000,
        "state": "canceled",
        "reservedUsd": "600",
        "filledQuantity": "0.01",
        "averagePrice": "50000",
        "providerOrderId": "901",
        "fees": [],
        "hashes": [],
        "childOperations": [
            {
                "id": "oto-ee-sl",
                "parentId": "oto-entry-err",
                "side": "sell",
                "quantity": "0.01",
                "orderType": "stop",
                "tpsl": "sl",
                "state": "open",
                "providerOrderId": "902",
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            }
        ],
    }
    journal.save(op)
    journal.save_effect(
        op,
        "plan",
        {
            "action": {"type": "order", "orders": [{}, {}], "grouping": "normalTpsl"},
            "clientOrderId": None,
            "ownerEpoch": engine.epoch,
            "binding": journal.binding("agent"),
            "wallet": WALLET,
            "network": "testnet",
            "projectId": "p",
            "marketIndex": 0,
            "isCross": True,
            "origin": "lumibot",
            "feeRate": "0.0005",
            "legs": [
                {"role": "entry", "id": "oto-entry-err", "clientOrderId": None},
                {"role": "child", "id": "oto-ee-sl", "clientOrderId": None},
            ],
        },
    )
    journal.save_effect(op, "order", {"phase": "venue", "state": "released", "ownerEpoch": engine.epoch})
    journal.save_effect(
        op,
        "cancel-902-1",
        {
            "phase": "provider",
            "state": "pending",
            "ownerEpoch": engine.epoch,
            "action": {"type": "cancel", "cancels": [{"a": 0, "o": 902}]},
        },
    )
    # 1. Shared venue read fails -> parent gets reconciliationErrorCode, cached open child must NOT resume cancel!
    venue.frontend_open_orders.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "rpc down")
    engine._cancel_step = Mock()
    failed = engine.order("oto-entry-err")
    assert failed["reconciliationErrorCode"] == "VENUE_UNAVAILABLE"
    assert failed["cancelErrorCode"] == "RECONCILIATION_REQUIRED"
    engine._cancel_step.assert_not_called()

    # 2. Shared venue read succeeds, entry lookup returns unknownOid (ORDER_UNRESOLVED),
    # while stop child 902 filled: child fill MUST still be reconciled and persisted!
    venue.frontend_open_orders.side_effect = None
    venue.frontend_open_orders.return_value = []
    venue.fills.return_value = [
        {
            "oid": 902,
            "coin": "BTC",
            "side": "A",
            "sz": "0.01",
            "px": "45000",
            "tid": 9201,
            "fee": "0.05",
            "feeToken": "USDC",
        }
    ]

    def order_status(wallet, oid):
        if oid == 901:
            return {"status": "unknownOid"}
        if oid == 902:
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"oid": 902, "coin": "BTC", "side": "A", "origSz": "0.01", "sz": "0", "reduceOnly": True},
                },
            }
        raise AssertionError(f"Unexpected oid: {oid}")

    venue.order_status.side_effect = order_status
    res = engine.order("oto-entry-err")
    assert res["reconciliationErrorCode"] == "ORDER_UNRESOLVED"
    assert res["childOperations"][0]["state"] == "filled"
    assert res["childOperations"][0]["filledQuantity"] == "0.01"


def test_observe_entry_and_sibling_child_regressions_still_observe_healthy_exit_legs():
    broker, client = _make_broker()
    try:
        btc = Asset("BTC", asset_type="crypto_future")
        usdc = Asset("USDC", asset_type="crypto")

        # 1. Tracked bracket where entry regresses (0.005 -> 0.004) while TP exit fills: exit fill MUST be observed.
        tp1 = Order(
            "s", btc, Decimal("0.01"), "sell", order_type="limit", limit_price=55000, quote=usdc, identifier="b-tp-1"
        )
        sl1 = Order(
            "s", btc, Decimal("0.01"), "sell", order_type="stop", stop_price=45000, quote=usdc, identifier="b-sl-1"
        )
        parent1 = Order(
            "s",
            btc,
            Decimal("0.01"),
            "buy",
            quote=usdc,
            order_class=Order.OrderClass.BRACKET,
            child_orders=[tp1, sl1],
            identifier="b-par-1",
        )
        broker._new_orders.append(parent1)
        broker._new_orders.append(tp1)
        broker._new_orders.append(sl1)
        broker._observe(
            parent1,
            {
                "id": "b-par-1",
                "state": "partial_fill",
                "filledQuantity": "0.005",
                "averagePrice": "50000",
                "request": {
                    "kind": "perp",
                    "symbol": "BTC",
                    "side": "buy",
                    "quantity": "0.01",
                    "orderClass": "bracket",
                },
                "childOperations": [],
            },
        )
        broker._record_order_feed = Mock()
        client.orders.return_value = [
            {
                "id": "b-par-1",
                "state": "partial_fill",
                "filledQuantity": "0.004",  # regressed from 0.005
                "averagePrice": "50000",
                "request": {
                    "kind": "perp",
                    "symbol": "BTC",
                    "side": "buy",
                    "quantity": "0.01",
                    "orderClass": "bracket",
                },
                "childOperations": [
                    {
                        "id": "b-tp-1",
                        "parentId": "b-par-1",
                        "side": "sell",
                        "quantity": "0.01",
                        "orderType": "limit",
                        "state": "filled",
                        "filledQuantity": "0.01",
                        "averagePrice": "55000",
                    }
                ],
            }
        ]
        with pytest.raises(MetaMaskAPIError, match="FILL_REGRESSION"):
            broker.do_polling()
        assert tp1.is_filled()
        broker._record_order_feed.assert_called_once_with(False)

        # 2. Valid entry, first exit regresses (0.005 -> 0.001), second exit fills: second MUST be observed.
        tp2 = Order(
            "s", btc, Decimal("0.01"), "sell", order_type="limit", limit_price=55000, quote=usdc, identifier="b-tp-2"
        )
        sl2 = Order(
            "s", btc, Decimal("0.01"), "sell", order_type="stop", stop_price=45000, quote=usdc, identifier="b-sl-2"
        )
        parent2 = Order(
            "s",
            btc,
            Decimal("0.01"),
            "buy",
            quote=usdc,
            order_class=Order.OrderClass.BRACKET,
            child_orders=[tp2, sl2],
            identifier="b-par-2",
        )
        broker._new_orders.append(parent2)
        broker._new_orders.append(tp2)
        broker._new_orders.append(sl2)
        broker._observe(
            tp2,
            {
                "id": "b-tp-2",
                "state": "partial_fill",
                "filledQuantity": "0.005",
                "averagePrice": "55000",
                "request": {"kind": "perp", "symbol": "BTC", "side": "sell", "quantity": "0.01"},
            },
        )
        broker._record_order_feed.reset_mock()
        client.orders.return_value = [
            {
                "id": "b-par-2",
                "state": "filled",
                "filledQuantity": "0.01",
                "averagePrice": "50000",
                "request": {
                    "kind": "perp",
                    "symbol": "BTC",
                    "side": "buy",
                    "quantity": "0.01",
                    "orderClass": "bracket",
                },
                "childOperations": [
                    {
                        "id": "b-tp-2",
                        "parentId": "b-par-2",
                        "side": "sell",
                        "quantity": "0.01",
                        "orderType": "limit",
                        "state": "open",
                        "filledQuantity": "0.001",  # regressed from 0.005
                        "averagePrice": "55000",
                    },
                    {
                        "id": "b-sl-2",
                        "parentId": "b-par-2",
                        "side": "sell",
                        "quantity": "0.01",
                        "orderType": "stop",
                        "state": "filled",
                        "filledQuantity": "0.01",
                        "averagePrice": "45000",
                    },
                ],
            }
        ]
        with pytest.raises(MetaMaskAPIError, match="FILL_REGRESSION"):
            broker.do_polling()
        assert sl2.is_filled()
        broker._record_order_feed.assert_called_once_with(False)
    finally:
        broker.close()
