#!/usr/bin/env python3
"""Live Schwab proof for the Titus fast-cancel strategy structure.

This is not a generic broker smoke. It proves the strategy-level behavior that
Titus needs: after a four-second option timeout, cancel by exact order id and
unlock on cancel accepted/canceling/canceled without broad get_orders scans.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lumibot.entities import Order

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


OLD_STRATEGY_ACTIVE_STATUSES = {
    "SUBMITTED",
    "OPEN",
    "NEW",
    "PARTIALLY_FILLED",
    "UNPROCESSED",
}

FAST_CANCEL_UNLOCK_STATUSES = {
    "CANCELED",
    "CANCELLED",
    "CANCELLING",
    "PENDING_CANCEL",
    "PENDING_REPLACE",
    "REPLACED",
}


def _submit_resting_option_order(broker, asset, limit_price: float, strategy_name: str) -> Order:
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=limit_price,
        time_in_force="day",
        tag=strategy_name,
    )
    submitted = broker._submit_order(order)
    if not submitted or not submitted.identifier:
        raise RuntimeError("Schwab did not return an order id for fast-cancel strategy smoke.")
    return submitted


def _time_broad_order_pull(broker) -> dict[str, Any]:
    started = time.perf_counter()
    raw_orders = broker._pull_broker_all_orders()
    elapsed = time.perf_counter() - started
    return {
        "elapsed_seconds": round(elapsed, 3),
        "raw_order_count": len(raw_orders or []),
    }


def run_fast_cancel_strategy_smoke(
    broker,
    asset,
    *,
    limit_price: float,
    wait_seconds: float,
    measure_broad_orders: bool,
) -> dict[str, Any]:
    strategy_name = "rob-titus-fast-cancel-strategy-smoke"
    submitted = None
    samples: list[dict[str, Any]] = []
    started = time.perf_counter()

    try:
        submitted = _submit_resting_option_order(broker, asset, limit_price, strategy_name)

        while time.perf_counter() - started < wait_seconds:
            snap = _order_snapshot(broker, submitted.identifier, strategy_name)
            snap["elapsed_seconds"] = round(time.perf_counter() - started, 3)
            samples.append(snap)
            time.sleep(0.5)

        before_cancel = _order_snapshot(broker, submitted.identifier, strategy_name)
        broker.cancel_order(submitted)

        # The fixed strategy decision is intentionally based on local cancel
        # acceptance plus bounded direct reads. It does not call get_orders().
        immediate_after_cancel = _order_snapshot(broker, submitted.identifier, strategy_name)
        final = _poll_until(
            broker,
            submitted.identifier,
            strategy_name,
            lambda snap: (
                snap["canceled"]
                or snap["status"] in FAST_CANCEL_UNLOCK_STATUSES
                or not snap["active"]
            ),
            timeout_seconds=8.0,
            interval_seconds=0.5,
        )

        post_status = str(immediate_after_cancel.get("status") or final.get("status") or "").upper()
        fixed_should_unlock = (
            submitted.is_canceled()
            or post_status in FAST_CANCEL_UNLOCK_STATUSES
            or bool(final.get("canceled"))
            or not bool(final.get("active"))
        )
        old_strategy_would_keep_lock = post_status in OLD_STRATEGY_ACTIVE_STATUSES

        broad_order_pull = _time_broad_order_pull(broker) if measure_broad_orders else None

        return {
            "name": "fast_cancel_strategy_unlock",
            "order_id_suffix": _suffix(submitted.identifier),
            "used_broad_get_orders_in_hot_path": False,
            "before_cancel": before_cancel,
            "immediate_after_cancel": immediate_after_cancel,
            "final": final,
            "samples": samples,
            "submitted_local_canceled_after_cancel_call": submitted.is_canceled(),
            "fixed_strategy_should_unlock": fixed_should_unlock,
            "old_strategy_would_keep_lock_on_immediate_status": old_strategy_would_keep_lock,
            "broad_order_pull": broad_order_pull,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "pass": bool(fixed_should_unlock),
        }
    finally:
        _safe_cancel(broker, submitted, strategy_name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-suffix", default="4364")
    parser.add_argument("--token-path", default="schwab_token.json")
    parser.add_argument("--token-payload", default=None)
    parser.add_argument("--wait-seconds", type=float, default=4.0)
    parser.add_argument("--option-limit", type=float, default=0.01)
    parser.add_argument("--measure-broad-orders", action="store_true")
    parser.add_argument("--summary-path", default=None)
    args = parser.parse_args()

    token_path = Path(args.token_path).expanduser().resolve()
    broker, account_meta = _make_broker(token_path, args.token_payload, args.account_suffix)
    asset = _select_tsll_call_asset(broker)

    result = run_fast_cancel_strategy_smoke(
        broker,
        asset,
        limit_price=args.option_limit,
        wait_seconds=args.wait_seconds,
        measure_broad_orders=args.measure_broad_orders,
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "account": account_meta,
        "asset": {
            "symbol": asset.symbol,
            "expiration": asset.expiration.isoformat() if asset.expiration else None,
            "strike": asset.strike,
            "right": asset.right,
        },
        "result": result,
    }

    print(json.dumps(summary, indent=2, sort_keys=True))
    summary_path = Path(args.summary_path or f"tmp/schwab_titus_fast_cancel_strategy_{int(time.time())}.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote summary: {summary_path.resolve()}")

    return 0 if result.get("pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
