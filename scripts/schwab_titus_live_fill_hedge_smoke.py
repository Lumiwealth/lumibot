#!/usr/bin/env python3
"""Live Schwab proof for Titus-style fill-to-hedge timing.

This intentionally creates and cleans up real exposure on Rob's Schwab account.
It uses a marketable limit for one TSLL option contract, watches that exact
order id, submits the stock hedge immediately on fill, then restores the stock
and option positions.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lumibot.entities import Asset, Order

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from schwab_titus_workflow_smoke import (  # noqa: E402
    _make_broker,
    _order_snapshot,
    _poll_until,
    _safe_cancel,
    _select_tsll_call_asset,
    _suffix,
)


TERMINAL = {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}


def _quote_dict(broker, asset: Asset) -> dict[str, Any]:
    quote = broker.data_source.get_quote(asset)
    return {
        "bid": float(getattr(quote, "bid", 0) or 0),
        "ask": float(getattr(quote, "ask", 0) or 0),
        "price": float(getattr(quote, "price", 0) or 0),
        "mid": float(getattr(quote, "mid_price", 0) or 0),
    }


def _submit_limit(
    broker,
    *,
    strategy_name: str,
    asset: Asset,
    quantity: int,
    side: str,
    limit_price: float,
) -> Order:
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.LIMIT,
        limit_price=round(float(limit_price), 2),
        time_in_force="day",
        tag=strategy_name,
    )
    submitted = broker._submit_order(order)
    if not submitted or not submitted.identifier:
        raise RuntimeError(f"Schwab did not return an order id for {side} {asset}.")
    return submitted


def _wait_for_terminal_or_active(broker, order: Order, strategy_name: str, timeout_seconds: float) -> dict[str, Any]:
    return _poll_until(
        broker,
        order.identifier,
        strategy_name,
        lambda snap: snap["status"] in TERMINAL or snap["filled"] or snap["canceled"],
        timeout_seconds=timeout_seconds,
        interval_seconds=0.5,
    )


def run_live_fill_hedge_smoke(
    broker,
    option_asset: Asset,
    *,
    option_qty: int,
    hedge_qty: int,
    max_option_ask: float,
    max_stock_ask: float,
    fill_timeout_seconds: float,
) -> dict[str, Any]:
    strategy_name = "rob-titus-live-fill-hedge-smoke"
    stock_asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    option_entry = None
    hedge_order = None
    option_exit = None
    hedge_restore = None
    events: list[dict[str, Any]] = []
    started = time.perf_counter()

    option_quote = _quote_dict(broker, option_asset)
    stock_quote = _quote_dict(broker, stock_asset)
    if option_quote["ask"] <= 0 or option_quote["ask"] > max_option_ask:
        raise RuntimeError(f"Option ask outside test bounds: {option_quote}")
    if stock_quote["ask"] <= 0 or stock_quote["ask"] > max_stock_ask:
        raise RuntimeError(f"Stock ask outside test bounds: {stock_quote}")

    try:
        option_entry = _submit_limit(
            broker,
            strategy_name=strategy_name,
            asset=option_asset,
            quantity=option_qty,
            side=Order.OrderSide.BUY_TO_OPEN,
            limit_price=option_quote["ask"] + 0.02,
        )
        events.append({
            "event": "option_entry_submitted",
            "elapsed": round(time.perf_counter() - started, 3),
            "order_suffix": _suffix(option_entry.identifier),
            "limit": round(option_quote["ask"] + 0.02, 2),
        })

        entry_final = _wait_for_terminal_or_active(broker, option_entry, strategy_name, fill_timeout_seconds)
        events.append({"event": "option_entry_final", "elapsed": round(time.perf_counter() - started, 3), **entry_final})

        if not entry_final.get("filled"):
            broker.cancel_order(option_entry)
            cancel_final = _wait_for_terminal_or_active(broker, option_entry, strategy_name, 8)
            return {
                "pass": False,
                "reason": "option_entry_did_not_fill",
                "events": events + [{"event": "option_entry_cleanup", **cancel_final}],
                "option_quote": option_quote,
                "stock_quote": stock_quote,
            }

        hedge_started = time.perf_counter()
        stock_quote_for_hedge = _quote_dict(broker, stock_asset)
        hedge_order = _submit_limit(
            broker,
            strategy_name=strategy_name,
            asset=stock_asset,
            quantity=hedge_qty,
            side=Order.OrderSide.SELL,
            limit_price=stock_quote_for_hedge["bid"] - 0.03,
        )
        hedge_submit_elapsed = time.perf_counter() - hedge_started
        events.append({
            "event": "hedge_submitted_after_fill",
            "elapsed": round(time.perf_counter() - started, 3),
            "seconds_after_fill_detection": round(hedge_submit_elapsed, 3),
            "order_suffix": _suffix(hedge_order.identifier),
            "limit": round(stock_quote_for_hedge["bid"] - 0.03, 2),
        })
        hedge_final = _wait_for_terminal_or_active(broker, hedge_order, strategy_name, 10)
        events.append({"event": "hedge_final", "elapsed": round(time.perf_counter() - started, 3), **hedge_final})

        # Restore the hedge shares first, then close the option contract.
        stock_quote_for_restore = _quote_dict(broker, stock_asset)
        hedge_restore = _submit_limit(
            broker,
            strategy_name=strategy_name,
            asset=stock_asset,
            quantity=hedge_qty,
            side=Order.OrderSide.BUY,
            limit_price=stock_quote_for_restore["ask"] + 0.05,
        )
        restore_final = _wait_for_terminal_or_active(broker, hedge_restore, strategy_name, 10)
        events.append({"event": "hedge_restore_final", "elapsed": round(time.perf_counter() - started, 3), **restore_final})

        option_quote_for_exit = _quote_dict(broker, option_asset)
        option_exit = _submit_limit(
            broker,
            strategy_name=strategy_name,
            asset=option_asset,
            quantity=option_qty,
            side=Order.OrderSide.SELL_TO_CLOSE,
            limit_price=max(0.01, option_quote_for_exit["bid"] - 0.03),
        )
        option_exit_final = _wait_for_terminal_or_active(broker, option_exit, strategy_name, 10)
        events.append({"event": "option_exit_final", "elapsed": round(time.perf_counter() - started, 3), **option_exit_final})

        return {
            "pass": bool(entry_final.get("filled") and hedge_final.get("filled")),
            "cleanup_pass": bool(restore_final.get("filled") and option_exit_final.get("filled")),
            "events": events,
            "option_quote": option_quote,
            "stock_quote": stock_quote,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
    finally:
        for order in (option_exit, hedge_restore, hedge_order, option_entry):
            _safe_cancel(broker, order, strategy_name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-suffix", default="4364")
    parser.add_argument("--token-path", default="schwab_token.json")
    parser.add_argument("--option-qty", type=int, default=1)
    parser.add_argument("--hedge-qty", type=int, default=100)
    parser.add_argument("--max-option-ask", type=float, default=0.75)
    parser.add_argument("--max-stock-ask", type=float, default=20.0)
    parser.add_argument("--fill-timeout-seconds", type=float, default=12.0)
    parser.add_argument("--summary-path", default=None)
    args = parser.parse_args()

    broker, account_meta = _make_broker(Path(args.token_path).expanduser().resolve(), None, args.account_suffix)
    option_asset = _select_tsll_call_asset(broker)
    result = run_live_fill_hedge_smoke(
        broker,
        option_asset,
        option_qty=args.option_qty,
        hedge_qty=args.hedge_qty,
        max_option_ask=args.max_option_ask,
        max_stock_ask=args.max_stock_ask,
        fill_timeout_seconds=args.fill_timeout_seconds,
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "account": account_meta,
        "asset": {
            "symbol": option_asset.symbol,
            "expiration": option_asset.expiration.isoformat() if option_asset.expiration else None,
            "strike": option_asset.strike,
            "right": option_asset.right,
        },
        "result": result,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    summary_path = Path(args.summary_path or f"tmp/schwab_titus_live_fill_hedge_{int(time.time())}.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote summary: {summary_path.resolve()}")
    return 0 if result.get("pass") and result.get("cleanup_pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
