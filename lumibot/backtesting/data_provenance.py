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
    # Routed crypto adapters name the exchange feed actually served, so a reader
    # can tell Coinbase USD candles from Binance USDT candles and see where
    # perpetual funding and marks came from.
    "priceSource",
    "servedSymbol",
    "fundingSource",
    "markSource",
)

# Notices report known data gaps (for example funding not published yet).
_NOTICE_KEYS = ("kind", "source", "symbol", "through", "message")


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


def _safe_notices(notices: Iterable[Any]) -> List[Dict[str, Any]]:
    safe: List[Dict[str, Any]] = []
    for notice in notices:
        if not isinstance(notice, dict):
            continue
        item = {
            key: notice[key]
            for key in _NOTICE_KEYS
            if notice.get(key) is not None and isinstance(notice.get(key), (str, int, float, bool))
        }
        if item:
            safe.append(item)
    return safe


def build_backtest_data_provenance(data_source: Any, fill_price_counts: Dict[str, int] | None = None) -> Dict[str, Any]:
    observed = None
    synthetic = None
    notices: List[Dict[str, Any]] = []
    getter = getattr(data_source, "get_data_provenance", None)
    if callable(getter):
        try:
            candidate = getter()
            if isinstance(candidate, dict):
                observed = candidate.get("observedRoutes")
                synthetic = candidate.get("syntheticOptions")
                raw_notices = candidate.get("notices")
                if isinstance(raw_notices, list):
                    notices = _safe_notices(raw_notices)
        except Exception:
            observed = None

    routes = _safe_routes(observed or [])
    if not routes:
        adapter = type(data_source).__name__
        routes = [{"adapter": adapter, "vendor": _infer_vendor(adapter)}]

    policy_version = _policy_version()
    provenance = {
        "policyVersion": policy_version,
        "selection": "BotSpot Auto" if policy_version else "Explicit data source",
        "observedRoutes": routes,
    }
    if notices:
        provenance["notices"] = notices
    # How option fills were priced: "quote" and "trade" are real prices in the fill bar,
    # "estimated" is a model price (volatility from the expiration's recent trades when they
    # are loaded, otherwise from the contract's own last trade), used as computed and labeled.
    # "synthetic" is a price computed from the underlying only (the synthetic option route).
    if fill_price_counts:
        counts = {str(key): int(value) for key, value in fill_price_counts.items() if isinstance(value, int)}
        provenance["optionFillPrices"] = counts
        total = sum(counts.values())
        if total > 0:
            provenance["optionFillPriceShares"] = {key: round(value / total, 4) for key, value in counts.items()}
    if isinstance(synthetic, dict):
        # Says plainly that option prices were computed, and from which volatility input.
        provenance["syntheticOptions"] = {
            str(key): value
            for key, value in synthetic.items()
            if isinstance(value, (str, int, float, bool))
            or (isinstance(value, list) and all(isinstance(item, str) for item in value))
        }
    return provenance


def write_backtest_data_provenance(
    data_source: Any, directory: str | Path = "logs", fill_price_counts: Dict[str, int] | None = None
) -> Path:
    artifact = Path(directory) / "data_provenance.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(
        json.dumps(build_backtest_data_provenance(data_source, fill_price_counts), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact
