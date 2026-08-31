"""Safe, portable provenance artifacts for completed backtests."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List


_OBSERVED_ROUTE_KEYS = (
    "assetClass",
    "symbol",
    "adapter",
    "vendor",
    "exchange",
    "feedType",
    "resolution",
    "windowStart",
    "windowEnd",
    "fallbackDecision",
)


def _policy_version() -> str | None:
    raw = os.environ.get("BOTSPOT_DATA_ROUTING_POLICY")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    version = parsed.get("version") if isinstance(parsed, dict) else None
    return str(version) if version else None


def _safe_routes(routes: Iterable[Any]) -> List[Dict[str, Any]]:
    safe: List[Dict[str, Any]] = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        item = {
            key: route[key]
            for key in _OBSERVED_ROUTE_KEYS
            if route.get(key) is not None and isinstance(route.get(key), (str, int, float, bool))
        }
        if item:
            safe.append(item)
    return safe


def _infer_vendor(adapter_name: str) -> str:
    normalized = adapter_name.lower()
    for token, vendor in (
        ("yahoo", "yahoo"),
        ("theta", "thetadata"),
        ("interactivebrokers", "interactive-brokers"),
        ("ibkr", "interactive-brokers"),
        ("ccxt", "ccxt"),
        ("coinbase", "coinbase"),
        ("polygon", "polygon"),
        ("alpaca", "alpaca"),
        ("databento", "databento"),
    ):
        if token in normalized:
            return vendor
    return normalized.removesuffix("databacktesting").removesuffix("backtesting")


def build_backtest_data_provenance(data_source: Any) -> Dict[str, Any]:
    observed = None
    getter = getattr(data_source, "get_data_provenance", None)
    if callable(getter):
        try:
            candidate = getter()
            if isinstance(candidate, dict):
                observed = candidate.get("observedRoutes")
        except Exception:
            observed = None

    routes = _safe_routes(observed or [])
    if not routes:
        adapter = type(data_source).__name__
        routes = [{"adapter": adapter, "vendor": _infer_vendor(adapter)}]

    policy_version = _policy_version()
    return {
        "policyVersion": policy_version,
        "selection": "BotSpot Auto" if policy_version else "Explicit data source",
        "observedRoutes": routes,
    }


def write_backtest_data_provenance(data_source: Any, directory: str | Path = "logs") -> Path:
    artifact = Path(directory) / "data_provenance.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(
        json.dumps(build_backtest_data_provenance(data_source), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact
