#!/usr/bin/env python3
"""Rob-owned live Schwab smoke for Titus-style option cancel and hedge flows.

This script intentionally uses the real Schwab broker adapter and a real
account. It places low-priced option limit orders that should rest, then cancels
them and verifies broker state by direct order lookup.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order
from lumibot.tools.schwab_helper import SchwabHelper


TERMINAL_STATUSES = {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}


def _suffix(value: Any, length: int = 8) -> str:
    if value is None:
        return "<none>"
    text = str(value)
    return text[-length:] if len(text) > length else text


def _status(raw: dict | None) -> str:
    return str((raw or {}).get("status") or "").upper()


def _order_snapshot(broker: Schwab, order_id: str, strategy_name: str) -> dict[str, Any]:
    raw = broker._pull_broker_order(order_id)
    parsed = broker._parse_broker_order(raw, strategy_name) if raw else None
    return {
        "id_suffix": _suffix(order_id),
        "status": _status(raw),
        "active": bool(parsed and parsed.is_active()),
        "canceled": bool(parsed and parsed.is_canceled()),
        "filled": bool(parsed and parsed.is_filled()),
        "strategy_type": (raw or {}).get("orderStrategyType"),
        "child_count": len((raw or {}).get("childOrderStrategies") or []),
    }


def _poll_until(
    broker: Schwab,
    order_id: str,
    strategy_name: str,
    predicate,
    *,
    timeout_seconds: float = 8.0,
    interval_seconds: float = 0.5,
) -> dict[str, Any]:
    deadline = time.perf_counter() + timeout_seconds
    last = None
    while time.perf_counter() < deadline:
        last = _order_snapshot(broker, order_id, strategy_name)
        if predicate(last):
            return last
        time.sleep(interval_seconds)
    return last or _order_snapshot(broker, order_id, strategy_name)


def _select_account_by_suffix(broker: Schwab, account_suffix: str) -> dict[str, str]:
    response = broker.client.get_account_numbers()
    response.raise_for_status()
    accounts = response.json()
    matches = [
        row for row in accounts
        if str(row.get("accountNumber", "")).endswith(str(account_suffix))
    ]
    if len(matches) != 1:
        safe_suffixes = [_suffix(row.get("accountNumber"), 4) for row in accounts]
        raise RuntimeError(
            f"Expected exactly one Schwab account ending in {account_suffix}; "
            f"found {len(matches)} among suffixes {safe_suffixes}"
        )
    account = matches[0]
    broker.account_number = str(account["accountNumber"])
    broker.hash_value = account["hashValue"]
    if getattr(broker, "data_source", None) is not None:
        broker.data_source.client = broker.client
    return {
        "account_suffix": _suffix(account["accountNumber"], 4),
        "hash_suffix": _suffix(account["hashValue"], 6),
    }


def _make_broker(token_path: Path, token_payload: str | None, account_suffix: str) -> tuple[Schwab, dict[str, str]]:
    if token_payload:
        SchwabHelper._save_payload_str_to_token_file(token_payload, token_path)
        token_path.chmod(0o600)

    if not token_path.exists():
        raise RuntimeError(f"Missing Schwab token file: {token_path}")

    Schwab._launch_stream = lambda self: None
    broker = Schwab(
        config={
            "SCHWAB_TOKEN_PATH": str(token_path),
            "SCHWAB_ACCOUNT_NUMBER": account_suffix,
            "MARKET": "NASDAQ",
        }
    )
    account_meta = _select_account_by_suffix(broker, account_suffix)
    return broker, account_meta


def _select_tsll_call_asset(broker: Schwab) -> Asset:
    underlying = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    chains = broker.data_source.get_chains(underlying, strike_count=20)
    calls = (chains or {}).get("Chains", {}).get("CALL", {})
    if not calls:
        raise RuntimeError("No TSLL call option chain returned by Schwab.")

    today = date.today()
    expirations = sorted(exp for exp in calls if date.fromisoformat(exp) >= today)
    if not expirations:
        raise RuntimeError("No future TSLL call expiration returned by Schwab.")

    quote = broker.data_source.get_quote(underlying)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(underlying))
    if last_price <= 0:
        raise RuntimeError("Could not get TSLL last price.")

    expiration = expirations[0]
    strike = min(calls[expiration], key=lambda value: abs(float(value) - last_price))
    return Asset(
        "TSLL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date.fromisoformat(expiration),
        strike=float(strike),
        right="CALL",
    )


def _safe_cancel(broker: Schwab, order: Order | None, strategy_name: str) -> dict[str, Any] | None:
    if not order or not order.identifier:
        return None
    try:
        raw = broker._pull_broker_order(order.identifier)
        if _status(raw) not in TERMINAL_STATUSES:
            broker.cancel_order(order)
    except Exception as exc:
        return {"cleanup_error": str(exc), "id_suffix": _suffix(order.identifier)}
    return _poll_until(
        broker,
        order.identifier,
        strategy_name,
        lambda snap: snap["status"] in TERMINAL_STATUSES or snap["canceled"],
        timeout_seconds=8.0,
    )


def run_titus_style_cancel(broker: Schwab, asset: Asset, limit_price: float, wait_seconds: float) -> dict[str, Any]:
    strategy_name = "rob-titus-style-cancel-smoke"
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
    submitted = None
    started = time.perf_counter()
    samples: list[dict[str, Any]] = []
    try:
        submitted = broker._submit_order(order)
        if not submitted or not submitted.identifier:
            raise RuntimeError("Schwab did not return an order id for Titus-style cancel smoke.")

        while time.perf_counter() - started < wait_seconds:
            snap = _order_snapshot(broker, submitted.identifier, strategy_name)
            snap["elapsed_seconds"] = round(time.perf_counter() - started, 3)
            samples.append(snap)
            time.sleep(0.5)

        broker.cancel_order(submitted)
        final = _poll_until(
            broker,
            submitted.identifier,
            strategy_name,
            lambda snap: snap["canceled"] or snap["status"] in {"CANCELED", "CANCELLED"},
            timeout_seconds=8.0,
        )
        final["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        return {
            "name": "titus_style_cancel",
            "order_id_suffix": _suffix(submitted.identifier),
            "samples": samples,
            "final": final,
            "pass": bool(final["canceled"] or final["status"] in {"CANCELED", "CANCELLED"}),
        }
    finally:
        cleanup = _safe_cancel(broker, submitted, strategy_name)
        if cleanup:
            samples.append({"cleanup": cleanup})


def run_oto_structure(broker: Schwab, asset: Asset, parent_limit: float, child_limit: float) -> dict[str, Any]:
    strategy_name = "rob-titus-oto-hedge-smoke"
    child = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.LIMIT,
        limit_price=child_limit,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=parent_limit,
        order_class=Order.OrderClass.OTO,
        child_orders=[child],
        time_in_force="day",
        tag=strategy_name,
    )
    submitted = None
    try:
        submitted = broker._submit_order(order)
        if not submitted or not submitted.identifier:
            raise RuntimeError("Schwab did not return an order id for OTO smoke.")
        initial = _order_snapshot(broker, submitted.identifier, strategy_name)
        broker.cancel_order(submitted)
        local_child_active = [child_order.is_active() for child_order in submitted.child_orders]
        final = _poll_until(
            broker,
            submitted.identifier,
            strategy_name,
            lambda snap: snap["canceled"] or snap["status"] in {"CANCELED", "CANCELLED"},
            timeout_seconds=8.0,
        )
        return {
            "name": "oto_structure",
            "order_id_suffix": _suffix(submitted.identifier),
            "initial": initial,
            "final": final,
            "local_child_active_after_cancel": local_child_active,
            "pass": (
                initial["strategy_type"] == "TRIGGER"
                and initial["child_count"] == 1
                and not any(local_child_active)
                and bool(final["canceled"] or final["status"] in {"CANCELED", "CANCELLED"})
            ),
        }
    finally:
        _safe_cancel(broker, submitted, strategy_name)


def run_bracket_structure(broker: Schwab, asset: Asset, parent_limit: float, take_profit: float, stop_loss: float) -> dict[str, Any]:
    strategy_name = "rob-titus-bracket-hedge-smoke"
    take_profit_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.LIMIT,
        limit_price=take_profit,
        time_in_force="day",
        tag=strategy_name,
    )
    stop_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.STOP,
        stop_price=stop_loss,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=parent_limit,
        order_class=Order.OrderClass.BRACKET,
        child_orders=[take_profit_order, stop_order],
        time_in_force="day",
        tag=strategy_name,
    )
    submitted = None
    try:
        submitted = broker._submit_order(order)
        if not submitted or not submitted.identifier:
            raise RuntimeError("Schwab did not return an order id for bracket smoke.")
        initial = _order_snapshot(broker, submitted.identifier, strategy_name)
        broker.cancel_order(submitted)
        local_child_active = [child_order.is_active() for child_order in submitted.child_orders]
        final = _poll_until(
            broker,
            submitted.identifier,
            strategy_name,
            lambda snap: snap["canceled"] or snap["status"] in {"CANCELED", "CANCELLED"},
            timeout_seconds=8.0,
        )
        return {
            "name": "bracket_structure",
            "order_id_suffix": _suffix(submitted.identifier),
            "initial": initial,
            "final": final,
            "local_child_active_after_cancel": local_child_active,
            "pass": (
                initial["strategy_type"] == "TRIGGER"
                and initial["child_count"] == 1
                and not any(local_child_active)
                and bool(final["canceled"] or final["status"] in {"CANCELED", "CANCELLED"})
            ),
        }
    finally:
        _safe_cancel(broker, submitted, strategy_name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-suffix", default="4364", help="Schwab account suffix to trade in.")
    parser.add_argument("--token-path", default="schwab_token.json", help="Gitignored Schwab token JSON path.")
    parser.add_argument("--token-payload", default=None, help="Optional fresh BotSpot/LumiBot Schwab payload string.")
    parser.add_argument("--wait-seconds", type=float, default=4.0, help="Cancel timeout to mirror Titus workflow.")
    parser.add_argument("--option-limit", type=float, default=0.01, help="Low limit intended to rest, not fill.")
    parser.add_argument("--child-limit", type=float, default=0.50, help="OTO/bracket take-profit child limit.")
    parser.add_argument("--stop-loss", type=float, default=0.01, help="Bracket stop-loss child stop price.")
    parser.add_argument("--summary-path", default=None, help="Optional JSON summary path.")
    args = parser.parse_args()

    token_path = Path(args.token_path).expanduser().resolve()
    broker, account_meta = _make_broker(token_path, args.token_payload, args.account_suffix)
    asset = _select_tsll_call_asset(broker)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "account": account_meta,
        "asset": {
            "symbol": asset.symbol,
            "expiration": asset.expiration.isoformat() if asset.expiration else None,
            "strike": asset.strike,
            "right": asset.right,
        },
        "tests": [],
    }

    for runner in (
        lambda: run_titus_style_cancel(broker, asset, args.option_limit, args.wait_seconds),
        lambda: run_oto_structure(broker, asset, args.option_limit, args.child_limit),
        lambda: run_bracket_structure(broker, asset, args.option_limit, args.child_limit, args.stop_loss),
    ):
        result = runner()
        summary["tests"].append(result)
        print(json.dumps(result, indent=2, sort_keys=True))

    summary_path = Path(args.summary_path or f"tmp/schwab_titus_workflow_smoke_{int(time.time())}.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote summary: {summary_path.resolve()}")

    return 0 if all(test.get("pass") for test in summary["tests"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
