#!/usr/bin/env python3
"""LumiBot-level Polymarket live smoke checks."""

from __future__ import annotations

import argparse
import json
import os
import sys
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

    broker = Polymarket(connect_stream=False)
    strategy = PolymarketSmokeStrategy(broker=broker, budget=100000)
    asset = Asset(required_env("POLYMARKET_TEST_TOKEN_ID"), asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    result: dict[str, Any] = {
        "cash": strategy.get_cash(),
        "portfolio_value": strategy.get_portfolio_value(),
        "positions_count": len(strategy.get_positions()),
        "open_orders_count": len(broker._pull_broker_all_orders()),
    }

    if args.market_order:
        ensure_live_enabled()
        amount = Decimal(str(args.amount))
        enforce_notional_cap(amount)
        order = strategy.create_order(
            asset,
            quantity=Decimal("1"),
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            time_in_force="fak",
            custom_params={"amount": str(amount), "max_price": str(args.max_price)},
        )
        result["market_order"] = summarize_order(strategy.submit_order(order))

    if args.limit_cancel:
        ensure_live_enabled()
        order = strategy.create_order(
            asset,
            quantity=Decimal(str(args.limit_size)),
            side=Order.OrderSide.BUY,
            limit_price=Decimal(str(args.limit_price)),
            order_type=Order.OrderType.LIMIT,
            time_in_force="gtc",
        )
        submitted = strategy.submit_order(order)
        broker.cancel_order(submitted)
        result["limit_cancel"] = {**summarize_order(submitted), "open_orders_after_cancel": len(broker._pull_broker_all_orders())}

    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LumiBot-level Polymarket smoke checks.")
    parser.add_argument("--market-order", action="store_true", help="Submit one capped FAK market BUY.")
    parser.add_argument("--amount", default=os.environ.get("POLYMARKET_TEST_ORDER_AMOUNT", "1.00"))
    parser.add_argument("--max-price", default=os.environ.get("POLYMARKET_TEST_MAX_PRICE", "0.99"))
    parser.add_argument("--limit-cancel", action="store_true", help="Submit and cancel one tiny GTC limit BUY.")
    parser.add_argument("--limit-price", default=os.environ.get("POLYMARKET_TEST_LIMIT_PRICE", "0.01"))
    parser.add_argument("--limit-size", default=os.environ.get("POLYMARKET_TEST_LIMIT_SIZE", "5"))
    return parser.parse_args()


def summarize_order(order) -> dict[str, Any]:
    identifier = order.identifier or ""
    return {
        "identifier": f"{identifier[:8]}...{identifier[-8:]}" if len(identifier) > 16 else identifier,
        "status": str(order.status),
        "quantity": float(order.quantity or 0),
        "avg_fill_price": order.avg_fill_price,
        "limit_price": order.limit_price,
    }


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
