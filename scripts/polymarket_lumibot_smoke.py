#!/usr/bin/env python3
"""LumiBot-level Polymarket live smoke checks."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
ENV_LOCAL = ROOT / ".env.local"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    args = parse_args()
    load_dotenv(ENV_LOCAL, override=True)

    from lumibot.brokers.polymarket import Polymarket
    from lumibot.entities import Asset, Order
    from lumibot.strategies.strategy import Strategy

    class PolymarketSmokeStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1S"

    broker = Polymarket(connect_stream=args.websocket, use_websocket=args.websocket, polling_interval=1.0)
    try:
        strategy = PolymarketSmokeStrategy(broker=broker, budget=100000)
        broker.set_strategy_name(strategy._name)
        asset = Asset(required_env("POLYMARKET_TEST_TOKEN_ID"), asset_type=Asset.AssetType.PREDICTION_CONTRACT)
        stream_capture = install_stream_capture(broker, asset.symbol) if args.websocket else None

        result: dict[str, Any] = {
            "cash": strategy.get_cash(),
            "portfolio_value": strategy.get_portfolio_value(),
            "positions_count": len(strategy.get_positions()),
            "open_orders_count": len(broker._pull_broker_all_orders()),
        }

        if args.market_order:
            args.order_kind = "fak-buy"
        if args.limit_cancel:
            args.order_kind = "cancel-single"

        if args.order_kind:
            ensure_live_enabled()
            result[args.order_kind] = run_live_order_kind(strategy, broker, asset, args)

        if args.websocket:
            time.sleep(max(args.stream_wait_seconds, 0.0))
            broker.do_polling()
            result["websocket"] = summarize_stream_capture(broker, asset.symbol, stream_capture)

        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        broker.cleanup_streams()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LumiBot-level Polymarket smoke checks.")
    parser.add_argument("--market-order", action="store_true", help="Submit one capped FAK market BUY.")
    parser.add_argument("--amount", default=os.environ.get("POLYMARKET_TEST_ORDER_AMOUNT", "1.00"))
    parser.add_argument("--max-price", default=os.environ.get("POLYMARKET_TEST_MAX_PRICE", "0.99"))
    parser.add_argument("--limit-cancel", action="store_true", help="Submit and cancel one tiny GTC limit BUY.")
    parser.add_argument("--limit-price", default=os.environ.get("POLYMARKET_TEST_LIMIT_PRICE", "0.01"))
    parser.add_argument("--limit-size", default=os.environ.get("POLYMARKET_TEST_LIMIT_SIZE", "5"))
    parser.add_argument(
        "--order-kind",
        choices=[
            "fak-buy",
            "fak-sell",
            "fok-buy",
            "fok-sell",
            "gtc-buy",
            "gtc-sell",
            "gtd-buy",
            "gtd-sell",
            "post-only-buy",
            "post-only-sell",
            "cancel-single",
            "cancel-multiple",
            "cancel-all",
            "cancel-market",
        ],
        help="Run one explicit LumiBot live order/cancel matrix case.",
    )
    parser.add_argument("--market-id", default=os.environ.get("POLYMARKET_TEST_CONDITION_ID"))
    parser.add_argument("--gtd-seconds", type=int, default=int(os.environ.get("POLYMARKET_TEST_GTD_SECONDS", "300")))
    parser.add_argument("--websocket", action="store_true", help="Enable public/private Polymarket WebSockets.")
    parser.add_argument("--stream-wait-seconds", type=float, default=8.0)
    return parser.parse_args()


def run_live_order_kind(strategy, broker, asset, args) -> dict[str, Any]:
    from lumibot.entities import Order

    order_kind = args.order_kind
    if order_kind in {"fak-buy", "fok-buy"}:
        tif = order_kind.split("-")[0].upper()
        amount = Decimal(str(args.amount))
        enforce_notional_cap(amount)
        order = strategy.create_order(
            asset,
            quantity=Decimal("1"),
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            time_in_force=tif.lower(),
            custom_params={"amount": str(amount), "max_price": str(args.max_price), "order_type": tif},
        )
        return summarize_order(strategy.submit_order(order))

    if order_kind in {"fak-sell", "fok-sell"}:
        tif = order_kind.split("-")[0].upper()
        order = strategy.create_order(
            asset,
            quantity=Decimal(str(args.limit_size)),
            side=Order.OrderSide.SELL,
            order_type=Order.OrderType.MARKET,
            time_in_force=tif.lower(),
            custom_params={"min_price": str(args.limit_price), "order_type": tif},
        )
        return summarize_order(strategy.submit_order(order))

    if order_kind in {"gtc-buy", "gtc-sell", "gtd-buy", "gtd-sell", "post-only-buy", "post-only-sell"}:
        side = Order.OrderSide.SELL if order_kind.endswith("sell") else Order.OrderSide.BUY
        time_in_force = "gtd" if order_kind.startswith("gtd") else "gtc"
        custom_params = {}
        if side == Order.OrderSide.BUY:
            enforce_notional_cap(Decimal(str(args.limit_price)) * Decimal(str(args.limit_size)))
        if time_in_force == "gtd":
            custom_params["expiration"] = int((datetime.now(timezone.utc) + timedelta(seconds=args.gtd_seconds)).timestamp())
        if order_kind.startswith("post-only"):
            custom_params["post_only"] = True
        order = strategy.create_order(
            asset,
            quantity=Decimal(str(args.limit_size)),
            side=side,
            limit_price=Decimal(str(args.limit_price)),
            order_type=Order.OrderType.LIMIT,
            time_in_force=time_in_force,
            custom_params=custom_params,
        )
        return summarize_order(strategy.submit_order(order))

    if order_kind == "cancel-single":
        order = build_resting_buy_order(strategy, asset, args)
        submitted = strategy.submit_order(order)
        broker.cancel_order(submitted)
        return {**summarize_order(submitted), "open_orders_after_cancel": len(broker._pull_broker_all_orders())}

    if order_kind == "cancel-multiple":
        orders = [strategy.submit_order(build_resting_buy_order(strategy, asset, args)) for _ in range(2)]
        response = broker.cancel_orders(orders)
        return {
            "orders": [summarize_order(order) for order in orders],
            "cancel_response": redact_identifiers(response),
            "open_orders_after_cancel": len(broker._pull_broker_all_orders()),
        }

    if order_kind == "cancel-all":
        return {"cancel_response": redact_identifiers(broker.cancel_all_orders())}

    if order_kind == "cancel-market":
        return {"cancel_response": redact_identifiers(broker.cancel_market_orders(market=args.market_id, asset_id=asset))}

    raise SystemExit(f"Unsupported order kind: {order_kind}")


def build_resting_buy_order(strategy, asset, args):
    from lumibot.entities import Order

    enforce_notional_cap(Decimal(str(args.limit_price)) * Decimal(str(args.limit_size)))
    return strategy.create_order(
        asset,
        quantity=Decimal(str(args.limit_size)),
        side=Order.OrderSide.BUY,
        limit_price=Decimal(str(args.limit_price)),
        order_type=Order.OrderType.LIMIT,
        time_in_force="gtc",
    )


def summarize_order(order) -> dict[str, Any]:
    identifier = order.identifier or ""
    return {
        "identifier": f"{identifier[:8]}...{identifier[-8:]}" if len(identifier) > 16 else identifier,
        "status": str(order.status),
        "quantity": float(order.quantity or 0),
        "avg_fill_price": order.avg_fill_price,
        "limit_price": order.limit_price,
    }


def install_stream_capture(broker, token_id: str) -> dict[str, list[Any]]:
    capture: dict[str, list[Any]] = {"market_events": [], "user_events": []}
    stream = getattr(broker, "stream", None)
    if stream is None:
        return capture
    subscribe = getattr(stream, "subscribe_market_assets", None)
    if subscribe is not None:
        subscribe([token_id])

    original_market_handler = getattr(stream, "handle_market_event", None)
    original_user_handler = getattr(stream, "handle_user_event", None)

    if original_market_handler is not None:
        def capture_market_event(event):
            capture["market_events"].append(event_summary(event))
            return original_market_handler(event)

        stream.handle_market_event = capture_market_event

    if original_user_handler is not None:
        def capture_user_event(event):
            capture["user_events"].append(event_summary(event))
            return original_user_handler(event)

        stream.handle_user_event = capture_user_event

    return capture


def summarize_stream_capture(broker, token_id: str, capture: dict[str, list[Any]] | None) -> dict[str, Any]:
    capture = capture or {"market_events": [], "user_events": []}
    quote_cache = getattr(getattr(broker, "data_source", None), "_quote_cache", {})
    return {
        "market_event_count": len(capture["market_events"]),
        "market_event_types": [item.get("type") for item in capture["market_events"][:5]],
        "user_event_count": len(capture["user_events"]),
        "user_event_types": [item.get("type") for item in capture["user_events"][:5]],
        "trade_event_rows": len(getattr(broker, "_trade_event_log_rows", [])),
        "recent_order_cache_count": len(getattr(broker, "_recent_order_cache", {})),
        "recent_trade_cache_count": len(getattr(broker, "_recent_trade_cache", {})),
        "quote_cache_has_test_token": str(token_id) in quote_cache,
    }


def event_summary(event: Any) -> dict[str, Any]:
    if isinstance(event, list):
        return {"type": "list", "count": len(event)}
    if not isinstance(event, dict):
        return {"type": type(event).__name__}
    event_type = event.get("event_type") or event.get("eventType") or event.get("type") or event.get("status")
    identifier = event.get("id") or event.get("orderID") or event.get("order_id") or event.get("trade_id")
    return {
        "type": event_type,
        "identifier": redact_identifiers(str(identifier)) if identifier else None,
    }


def redact_identifiers(value):
    if isinstance(value, dict):
        return {key: redact_identifiers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_identifiers(item) for item in value]
    if isinstance(value, str) and len(value) > 16:
        return f"{value[:8]}...{value[-8:]}"
    return value


def ensure_live_enabled() -> None:
    if os.environ.get("POLYMARKET_LIVE_TRADING_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing live order: set POLYMARKET_LIVE_TRADING_ENABLED=true first")


def enforce_notional_cap(amount: Decimal) -> None:
    cap = Decimal(os.environ.get("POLYMARKET_TEST_MAX_NOTIONAL") or os.environ.get("POLYMARKET_MAX_MARKET_ORDER_NOTIONAL") or "5")
    if amount > cap:
        raise SystemExit(f"Refusing live order: amount {amount} exceeds cap {cap}")


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing {name} in {ENV_LOCAL}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
