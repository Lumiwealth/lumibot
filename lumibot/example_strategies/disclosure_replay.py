"""Deterministic proof harness for point-in-time congressional disclosures."""

from __future__ import annotations

import csv
import hashlib
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from lumibot.components.disclosure_signals import visible_congress_disclosures


def _dt(value: Any) -> datetime:
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def replay_congress_disclosures(
    records: Iterable[dict[str, Any]],
    market_snapshots: dict[str, dict[str, Any]],
    *,
    as_of: Any,
    initial_cash: float,
    max_disclosure_age_days: int = 90,
    max_position_pct: float = 5,
    max_total_exposure_pct: float = 20,
    minimum_average_dollar_volume: float = 1_000_000,
) -> dict[str, Any]:
    """Replay public availability and deterministic risk gates without an LLM."""
    ceiling = _dt(as_of)
    visible = visible_congress_disclosures(records, as_of=ceiling)
    cash = float(initial_cash)
    positions: dict[str, int] = {}
    orders: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []

    for record in visible:
        published = _dt(record["published_at"])
        symbol = record["ticker"]
        trace.append(
            {
                "role": "disclosure_researcher",
                "at": published.isoformat(),
                "disclosure_id": record["id"],
                "evidence": {
                    "source": record["source"],
                    "published_at": record["published_at"],
                    "transaction_date": record["transaction_date"],
                    "ticker": symbol,
                    "transaction": record["transaction"],
                },
            }
        )
        reason = "accepted"
        snapshot = market_snapshots.get(symbol) or {}
        price = float(snapshot.get("price") or 0)
        volume = float(snapshot.get("average_daily_volume") or 0)
        average_dollar_volume = price * volume
        age_days = (ceiling - published).total_seconds() / 86_400
        if age_days > max(int(max_disclosure_age_days), 0):
            reason = "stale_disclosure"
        elif price <= 0:
            reason = "missing_price"
        elif average_dollar_volume < float(minimum_average_dollar_volume):
            reason = "liquidity_below_minimum"

        transaction = str(record.get("transaction") or "").strip().lower()
        side = "buy" if "purchase" in transaction or "buy" in transaction else "sell" if "sale" in transaction else None
        if reason == "accepted" and side is None:
            reason = "ambiguous_transaction"
        if reason == "accepted" and side == "sell" and positions.get(symbol, 0) <= 0:
            reason = "no_long_position_to_sell"

        equity = cash + sum(
            quantity * float((market_snapshots.get(position_symbol) or {}).get("price") or 0)
            for position_symbol, quantity in positions.items()
        )
        total_exposure = sum(
            max(quantity, 0) * float((market_snapshots.get(position_symbol) or {}).get("price") or 0)
            for position_symbol, quantity in positions.items()
        )
        quantity = 0
        if reason == "accepted" and side == "buy":
            position_cap = equity * float(max_position_pct) / 100
            total_cap_remaining = max(equity * float(max_total_exposure_pct) / 100 - total_exposure, 0)
            notional_cap = min(position_cap, total_cap_remaining, cash)
            quantity = int(notional_cap // price)
            if quantity <= 0:
                reason = "risk_cap_allows_no_shares"
        elif reason == "accepted" and side == "sell":
            quantity = positions.get(symbol, 0)

        decision = {
            "role": "trading_risk_manager",
            "at": published.isoformat(),
            "disclosure_id": record["id"],
            "ticker": symbol,
            "decision": "order" if reason == "accepted" else "hold",
            "reason": reason,
            "risk": {
                "price": price,
                "average_dollar_volume": average_dollar_volume,
                "max_position_pct": float(max_position_pct),
                "max_total_exposure_pct": float(max_total_exposure_pct),
                "available_cash_before": cash,
            },
        }
        if reason == "accepted":
            order_key = f"{record['id']}|{side}|{quantity}|{published.isoformat()}"
            order_id = hashlib.sha256(order_key.encode()).hexdigest()[:20]
            notional = round(quantity * price, 8)
            order = {
                "order_id": order_id,
                "origin_role": "trading_risk_manager",
                "disclosure_id": record["id"],
                "ticker": symbol,
                "side": side,
                "quantity": quantity,
                "fill_price": price,
                "notional": notional,
                "submitted_at": published.isoformat(),
                "status": "filled_fixture_replay",
            }
            if side == "buy":
                cash -= notional
                positions[symbol] = positions.get(symbol, 0) + quantity
            else:
                cash += notional
                positions[symbol] = max(positions.get(symbol, 0) - quantity, 0)
            orders.append(order)
            decision["order_id"] = order_id
        decisions.append(decision)
        trace.append(decision)

    ending_equity = cash + sum(
        quantity * float((market_snapshots.get(symbol) or {}).get("price") or 0)
        for symbol, quantity in positions.items()
    )
    return {
        "schema_version": 1,
        "as_of": ceiling.isoformat(),
        "availability_rule": "ReportDate/published_at, never TransactionDate",
        "reporting_lag_warning": "Congressional disclosures may arrive up to 45 days after the transaction.",
        "initial_cash": float(initial_cash),
        "ending_cash": cash,
        "ending_equity": ending_equity,
        "positions": positions,
        "orders": orders,
        "decisions": decisions,
        "trace": trace,
    }


def _artifact(path: Path) -> dict[str, str]:
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def write_replay_artifacts(result: dict[str, Any], output_dir: str | Path) -> dict[str, dict[str, str]]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    summary_path = output / "summary.json"
    trace_path = output / "agent-trace.json"
    trades_path = output / "trades.csv"
    tearsheet_path = output / "tearsheet.html"
    summary_path.write_text(
        json.dumps({key: value for key, value in result.items() if key != "trace"}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    trace_path.write_text(json.dumps(result["trace"], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with trades_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "order_id",
            "origin_role",
            "disclosure_id",
            "ticker",
            "side",
            "quantity",
            "fill_price",
            "notional",
            "submitted_at",
            "status",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result["orders"])
    rows = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(str(order[field]))}</td>"
            for field in ("ticker", "side", "quantity", "fill_price", "submitted_at")
        )
        + "</tr>"
        for order in result["orders"]
    )
    tearsheet_path.write_text(
        "<!doctype html><html><head><meta charset='utf-8'><title>Congress disclosure replay</title></head>"
        "<body><h1>Congress disclosure replay</h1>"
        f"<p>{html.escape(result['availability_rule'])}</p>"
        "<p><strong>45-day reporting-lag warning:</strong> a trade is never visible before its public disclosure.</p>"
        f"<p>Initial cash: ${result['initial_cash']:,.2f} &middot; Ending equity: ${result['ending_equity']:,.2f}</p>"
        "<table><thead><tr><th>Ticker</th><th>Side</th><th>Quantity</th><th>Price</th><th>Public time</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></body></html>\n",
        encoding="utf-8",
    )
    return {
        "summary": _artifact(summary_path),
        "trace": _artifact(trace_path),
        "trades": _artifact(trades_path),
        "tearsheet": _artifact(tearsheet_path),
    }
