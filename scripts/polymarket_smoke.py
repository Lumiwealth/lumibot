#!/usr/bin/env python3
"""Direct Polymarket CLOB smoke proof for local development.

This script is deliberately outside the LumiBot broker adapter. It verifies the raw
Polymarket SDK, Data API, CLOB HTTP endpoints, and WebSockets before blaming or
changing LumiBot mapping code.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
ENV_LOCAL = ROOT / ".env.local"
LOG_DIR = ROOT / "logs"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CLOB_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

KNOWN_ORDER_BLOCKERS = (
    "maker address not allowed",
    "deposit wallet flow",
    "order signer address has to be the address of the api key",
    "no deposit wallet found",
)


def main() -> int:
    args = parse_args()
    if ENV_LOCAL.exists():
        load_dotenv(ENV_LOCAL)

    result: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "env_file": str(ENV_LOCAL),
        "mode": {
            "read_only": True,
            "live_order": args.live_order,
            "limit_cancel": args.limit_cancel,
        },
        "env_present": redacted_env_summary(),
    }

    client, derived = build_clob_client()
    result["clob_credentials"] = {"derived_in_process": derived}

    token_id = os.environ.get("POLYMARKET_TEST_TOKEN_ID")
    if not token_id:
        raise SystemExit("Missing POLYMARKET_TEST_TOKEN_ID in .env.local or environment")

    result["account"] = prove_account(client)
    result["market"] = prove_market(token_id)
    result["websocket"] = asyncio.run(prove_websockets(token_id, timeout=args.ws_timeout))

    if args.live_order:
        result["live_market_order"] = prove_market_order(client, token_id, args.amount, args.max_price)

    if args.limit_cancel:
        result["limit_cancel"] = prove_limit_cancel(client, token_id, args.limit_price, args.limit_size)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    output_path = LOG_DIR / f"polymarket_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    output_path.write_text(json.dumps(redact_for_log(result), indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(redact_for_console(result, output_path), indent=2, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run direct Polymarket CLOB smoke checks with redacted output.")
    parser.add_argument("--live-order", action="store_true", help="Attempt one tiny FAK market BUY order.")
    parser.add_argument("--limit-cancel", action="store_true", help="Attempt one tiny GTC limit order and cancel it.")
    parser.add_argument("--amount", default=os.environ.get("POLYMARKET_TEST_ORDER_AMOUNT", "1.00"))
    parser.add_argument("--max-price", default=os.environ.get("POLYMARKET_TEST_MAX_PRICE", "0.99"))
    parser.add_argument("--limit-price", default=os.environ.get("POLYMARKET_TEST_LIMIT_PRICE", "0.01"))
    parser.add_argument("--limit-size", default=os.environ.get("POLYMARKET_TEST_LIMIT_SIZE", "5"))
    parser.add_argument("--ws-timeout", type=float, default=8.0)
    return parser.parse_args()


def build_clob_client():
    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
        from py_clob_client_v2.constants import POLYGON
    except Exception as exc:
        raise SystemExit("Install py-clob-client-v2 before running this script") from exc

    private_key = required_env("POLYMARKET_PRIVATE_KEY")
    signature_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "3"))
    chain_id = int(os.environ.get("POLYMARKET_CHAIN_ID", POLYGON))
    funder = os.environ.get("POLYMARKET_WALLET_ADDRESS")

    creds = None
    if all(
        os.environ.get(name)
        for name in ("POLYMARKET_CLOB_API_KEY", "POLYMARKET_CLOB_API_SECRET", "POLYMARKET_CLOB_API_PASSPHRASE")
    ):
        creds = ApiCreds(
            api_key=os.environ["POLYMARKET_CLOB_API_KEY"],
            api_secret=os.environ["POLYMARKET_CLOB_API_SECRET"],
            api_passphrase=os.environ["POLYMARKET_CLOB_API_PASSPHRASE"],
        )

    client = ClobClient(
        os.environ.get("POLYMARKET_CLOB_URL", CLOB_URL),
        chain_id=chain_id,
        key=private_key,
        creds=creds,
        signature_type=signature_type,
        funder=funder,
        retry_on_error=True,
    )
    if creds is None:
        derived = client.create_or_derive_api_key()
        client.set_api_creds(derived)
        return client, True
    return client, False


def prove_account(client) -> dict[str, Any]:
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

    balance_payload = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    open_orders = listify(client.get_open_orders())
    trades = listify(client.get_trades())
    addresses = query_addresses()
    positions_by_address = {}
    value_by_address = {}
    with httpx.Client(timeout=10.0) as http:
        for address in addresses:
            positions_by_address[redact_address(address)] = len(
                listify(http.get(f"{DATA_API_URL}/positions", params={"user": address}).json())
            )
            value_payload = http.get(f"{DATA_API_URL}/value", params={"user": address}).json()
            value_by_address[redact_address(address)] = listify(value_payload)

    balance_raw = first_present(balance_payload, "balance")
    return {
        "balance_raw_units": str(balance_raw),
        "balance_usdc": float(raw_usdc_to_decimal(balance_raw)),
        "allowance_present": first_present(balance_payload, "allowance") is not None,
        "open_orders_count": len(open_orders),
        "recent_trades_count": len(trades),
        "positions_count_by_address": positions_by_address,
        "value_by_address": value_by_address,
    }


def prove_market(token_id: str) -> dict[str, Any]:
    from lumibot.data_sources.polymarket_data import PolymarketData
    from lumibot.entities import Asset

    data = PolymarketData()
    asset = Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    book = data.get_order_book(token_id)
    quote = data.get_quote(asset)
    last_price = data.get_last_price(asset)
    history = data.get_historical_prices(asset, 5, timestep="minute")
    estimate = data.calculate_market_price(token_id, "buy", Decimal("1.00"))
    return {
        "token_id": redact_token(token_id),
        "book_bid_count": len(book.get("bids") or []),
        "book_ask_count": len(book.get("asks") or []),
        "tick_size": str(first_present(book, "tick_size", "minimum_tick_size") or data.get_tick_size(token_id)),
        "neg_risk": first_present(book, "neg_risk", "negRisk"),
        "quote": {"bid": quote.bid, "ask": quote.ask, "mid": quote.mid_price},
        "last_price": last_price,
        "history_points": len(history.df),
        "buy_one_dollar_estimate": {
            "avg_price": estimate["avg_price_estimate"],
            "shares": estimate["filled_shares_estimate"],
            "fully_satisfied": estimate["fully_satisfied"],
        },
    }


async def prove_websockets(token_id: str, timeout: float) -> dict[str, Any]:
    try:
        import websockets
    except Exception as exc:
        return {"error": f"websockets missing: {exc}"}

    public_events: list[Any] = []
    private_events: list[Any] = []

    async def public_ws():
        async with websockets.connect(MARKET_WS_URL, ping_interval=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"assets_ids": [token_id], "type": "market", "custom_feature_enabled": True}))
            end = asyncio.get_running_loop().time() + timeout
            while asyncio.get_running_loop().time() < end and len(public_events) < 3:
                try:
                    public_events.append(json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0)))
                except asyncio.TimeoutError:
                    continue

    async def private_ws():
        auth = websocket_auth()
        if not auth:
            return
        async with websockets.connect(USER_WS_URL, ping_interval=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"auth": auth, "type": "user"}))
            end = asyncio.get_running_loop().time() + timeout
            while asyncio.get_running_loop().time() < end and len(private_events) < 3:
                try:
                    private_events.append(json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0)))
                except asyncio.TimeoutError:
                    continue

    await asyncio.gather(public_ws(), private_ws(), return_exceptions=True)
    return {
        "public_event_count": len(public_events),
        "public_event_types": [event_type(event) for event in public_events],
        "private_event_count": len(private_events),
        "private_stream_connected": websocket_auth() is not None,
    }


def prove_market_order(client, token_id: str, amount: str, max_price: str) -> dict[str, Any]:
    ensure_live_enabled()
    enforce_notional_cap(amount)
    try:
        from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

    options = order_options(token_id)
    order_args = MarketOrderArgs(
        token_id=token_id,
        amount=float(amount),
        side="BUY",
        price=float(max_price),
        order_type=OrderType.FAK,
    )
    try:
        response = client.create_and_post_market_order(order_args, options=options, order_type=OrderType.FAK)
        return {"status": "submitted", "response": redact_for_log(to_plain(response))}
    except Exception as exc:
        return classify_order_exception(exc)


def prove_limit_cancel(client, token_id: str, price: str, size: str) -> dict[str, Any]:
    ensure_live_enabled()
    try:
        from py_clob_client_v2.clob_types import OrderArgs, OrderPayload, OrderType
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

    options = order_options(token_id)
    order_args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side="BUY")
    try:
        response = client.create_and_post_order(order_args, options=options, order_type=OrderType.GTC)
        order_id = first_present(response, "order_id", "orderID", "id")
        cancel_response = None
        if order_id:
            cancel_response = client.cancel_order(OrderPayload(orderID=str(order_id)))
        return {
            "status": "submitted",
            "order_response": redact_for_log(to_plain(response)),
            "cancel_response": redact_for_log(to_plain(cancel_response)),
        }
    except Exception as exc:
        return classify_order_exception(exc)


def order_options(token_id: str):
    from py_clob_client_v2.clob_types import PartialCreateOrderOptions
    from lumibot.data_sources.polymarket_data import PolymarketData

    data = PolymarketData()
    book = data.get_order_book(token_id)
    tick_size = first_present(book, "tick_size", "minimum_tick_size")
    neg_risk = first_present(book, "neg_risk", "negRisk")
    return PartialCreateOrderOptions(tick_size=str(tick_size) if tick_size else None, neg_risk=bool(neg_risk))


def ensure_live_enabled() -> None:
    enabled = os.environ.get("POLYMARKET_LIVE_TRADING_ENABLED", "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing live order: set POLYMARKET_LIVE_TRADING_ENABLED=true first")


def enforce_notional_cap(amount: str) -> None:
    amount_value = Decimal(str(amount))
    cap = Decimal(os.environ.get("POLYMARKET_TEST_MAX_NOTIONAL") or os.environ.get("POLYMARKET_MAX_MARKET_ORDER_NOTIONAL") or "5")
    if amount_value > cap:
        raise SystemExit(f"Refusing live order: amount {amount_value} exceeds cap {cap}")


def classify_order_exception(exc: Exception) -> dict[str, Any]:
    message = str(exc)
    status = "blocked" if any(marker in message.lower() for marker in KNOWN_ORDER_BLOCKERS) else "error"
    return {"status": status, "error": message}


def websocket_auth() -> dict | None:
    key = os.environ.get("POLYMARKET_CLOB_API_KEY")
    secret = os.environ.get("POLYMARKET_CLOB_API_SECRET")
    passphrase = os.environ.get("POLYMARKET_CLOB_API_PASSPHRASE")
    if not (key and secret and passphrase):
        return None
    return {"apiKey": key, "secret": secret, "passphrase": passphrase}


def query_addresses() -> list[str]:
    addresses = []
    for name in ("POLYMARKET_WALLET_ADDRESS", "POLYMARKET_OWNER_ADDRESS"):
        value = os.environ.get(name)
        if value and value.lower() not in {address.lower() for address in addresses}:
            addresses.append(value)
    return addresses


def redacted_env_summary() -> dict[str, Any]:
    names = [
        "TRADING_BROKER",
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_OWNER_ADDRESS",
        "POLYMARKET_WALLET_ADDRESS",
        "POLYMARKET_SIGNATURE_TYPE",
        "POLYMARKET_CLOB_API_KEY",
        "POLYMARKET_CLOB_API_SECRET",
        "POLYMARKET_CLOB_API_PASSPHRASE",
        "POLYMARKET_TEST_TOKEN_ID",
        "POLYMARKET_LIVE_TRADING_ENABLED",
        "POLYMARKET_TEST_MAX_NOTIONAL",
    ]
    return {name: bool(os.environ.get(name)) for name in names}


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing {name} in .env.local or environment")
    return value


def listify(value: Any) -> list:
    value = to_plain(value)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("items", "data", "orders", "positions", "trades", "results"):
            if isinstance(value.get(key), list):
                return value[key]
        return [value]
    return [value]


def first_present(mapping: Any, *keys: str) -> Any:
    mapping = to_plain(mapping)
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def raw_usdc_to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    decimal_value = Decimal(str(value))
    if decimal_value == decimal_value.to_integral_value():
        return decimal_value / Decimal("1000000")
    return decimal_value


def event_type(event: Any) -> str:
    if isinstance(event, list):
        return "list"
    return str(first_present(event, "event_type", "eventType", "type") or type(event).__name__)


def to_plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: to_plain(item) for key, item in vars(value).items() if not key.startswith("_")}
    return value


def redact_for_console(result: dict[str, Any], output_path: Path) -> dict[str, Any]:
    payload = redact_for_log(result)
    payload["log_path"] = str(output_path)
    return payload


def redact_for_log(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: redact_for_log(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    if isinstance(value, str):
        if value.startswith("0x") and len(value) == 42:
            return redact_address(value)
        if value.isdigit() and len(value) > 20:
            return redact_token(value)
    return value


def redact_address(value: str) -> str:
    return f"{value[:6]}...{value[-4:]}" if value and len(value) > 12 else value


def redact_token(value: str) -> str:
    return f"{value[:8]}...{value[-8:]}" if value and len(value) > 20 else value


if __name__ == "__main__":
    sys.exit(main())
