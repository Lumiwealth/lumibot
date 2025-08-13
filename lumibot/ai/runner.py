from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from datetime import date, datetime

from ..entities import Asset
from .model_client import ProviderRouter

# Minimal built-in engine stub: in v1 we keep single-shot JSON schema with retries
class BuiltInEngine:
    def __init__(self, strategy):
        self.strategy = strategy

    def make_prompts(self, handle, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        # In v1, we embed a minimal system prompt instructing JSON-only
        system = (
            "You are an autonomous trading assistant. Output strict JSON per schema."
        )
        user = handle.prompt
        return {"system": system, "user": user}

    def run(self, prompts: Dict[str, str], snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Placeholder: do nothing in this stub. Real implementation would call an LLM client.
        return None


def build_snapshot(strategy, symbols=None) -> Dict[str, Any]:
    # Compact snapshot with cash, positions, open orders, last prices for referenced symbols
    snapshot: Dict[str, Any] = {
        "cash": strategy.get_cash(),
        "portfolio_value": strategy.get_portfolio_value(),
        "positions": [
            {
                "symbol": p.asset.symbol if hasattr(p.asset, "symbol") else str(p.asset),
                "asset_type": getattr(p.asset, "asset_type", None),
                "quantity": float(p.quantity),
            }
            for p in strategy.get_positions()
        ],
        "open_orders": [
            {
                "symbol": getattr(o.asset, "symbol", None),
                "side": o.side,
                "qty": float(o.quantity) if getattr(o, "quantity", None) is not None else None,
                "order_type": o.order_type,
                "limit_price": getattr(o, "limit_price", None),
                "stop_price": getattr(o, "stop_price", None),
            }
            for o in strategy.get_orders()
        ],
    }

    # Add recent prices for requested symbols if provided
    if symbols:
        prices = {}
        for s in symbols:
            try:
                asset = s if isinstance(s, Asset) else Asset(symbol=s)
                prices[s] = strategy.get_last_price(asset)
            except Exception:
                prices[s] = None
        snapshot["last_prices"] = prices

    return snapshot


class AgentRunner:
    def __init__(self, strategy):
        self.strategy = strategy
        self.engine = BuiltInEngine(strategy)
        # Router is created per tick with possible per-agent overrides
        self._default_router = ProviderRouter()
        self._last_run_wallclock: Dict[str, float] = {}

    def tick(self, handle) -> Optional[Dict[str, Any]]:
        # Respect market hours
        if not self.strategy.broker.is_market_open():
            return None

        snapshot = build_snapshot(self.strategy, handle.symbols)
        prompts = self.engine.make_prompts(handle, snapshot)
        # Compose a minimal system + user prompt
        system = prompts.get("system", "")
        user = prompts.get("user", "")
        # Try LLM call with JSON-only; default search=True
        # Build a router with per-agent overrides if provided
        router = (
            ProviderRouter(provider=handle.provider, model=handle.model)
            if (handle.provider or handle.model) else self._default_router
        )
        decision, diags = router.complete_json(system, user, json_schema={}, search=getattr(handle, 'search', True))
        status = "MODEL_OK" if isinstance(decision, dict) else "MODEL_ERROR"
        reason = "ok" if status == "MODEL_OK" else (diags.raw_preview_on_error or "no decision")
        if decision is None:
            # Normalize to empty decision with reason baked into notes (runner level)
            decision = {"actions": [], "notes": "no decision"}
        # Attach diagnostics for manager to surface
        decision["_diagnostics"] = {
            "provider": diags.provider,
            "model": diags.model,
            "latency_ms": diags.latency_ms,
            "tokens_in": diags.tokens_in,
            "tokens_out": diags.tokens_out,
            "tokens_total": diags.tokens_total,
            "search_used": diags.search_used,
            "citations_count": diags.citations_count,
            "status": status,
            "reason": reason,
        }
        # Validate JSON shape minimally
        if decision is None:
            return None
        if not isinstance(decision, dict):
            return None
        if "actions" in decision and not isinstance(decision["actions"], list):
            return None
        return decision


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # Expect ISO like YYYY-MM-DD
    try:
        return date.fromisoformat(str(value))
    except Exception:
        return None


def _build_asset_from_action(a: Dict[str, Any]) -> Asset:
    symbol = a.get("symbol")
    asset_type = a.get("asset_type") or Asset.AssetType.STOCK
    # Normalize to enum string
    if isinstance(asset_type, str):
        asset_type = asset_type.lower()
    expiration = _parse_date(a.get("expiration"))
    strike = a.get("strike")
    right = a.get("right")
    auto_expiry = a.get("auto_expiry")
    # Construct Asset with available fields
    return Asset(
        symbol=symbol,
        asset_type=asset_type,
        expiration=expiration,
        strike=strike if strike is not None else 0.0,
        right=right,
        auto_expiry=auto_expiry,
    )


def _build_quote_asset_from_action(a: Dict[str, Any]) -> Optional[Asset]:
    # Accept either 'quote' (string) or structured quote dict {symbol, asset_type}
    quote = a.get("quote") or a.get("quote_symbol")
    if not quote:
        return None
    if isinstance(quote, dict):
        qsym = quote.get("symbol")
        qtype = quote.get("asset_type") or Asset.AssetType.FOREX
        return Asset(symbol=qsym, asset_type=qtype)
    # string
    # Heuristic: crypto pairs usually quote as crypto; forex as forex
    qtype = a.get("quote_asset_type") or Asset.AssetType.FOREX
    return Asset(symbol=str(quote), asset_type=qtype)


def apply_actions_on_strategy(strategy, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for a in actions:
        t = a.get("type")
        if t == "trade":
            symbol = a.get("symbol")
            qty = a.get("qty")
            side = a.get("side").lower()
            order_type = a.get("order_type", "market")
            limit_price = a.get("limit_price")
            stop_price = a.get("stop_price")
            if not symbol or not qty or not side:
                continue
            asset = _build_asset_from_action(a)
            quote_asset = _build_quote_asset_from_action(a)
            order = strategy.create_order(
                asset,
                qty,
                side,
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                quote=quote_asset,
            )
            strategy.submit_order(order)
            results.append({"type": "trade", "order_id": getattr(order, "identifier", None)})
        elif t == "cancel_orders":
            for o in strategy.get_orders():
                try:
                    strategy.cancel_order(o)
                except Exception:
                    pass
            results.append({"type": "cancel_ok"})
        elif t == "note":
            txt = a.get("text", "")
            strategy.log_message(f"[AI NOTE] {txt}")
            results.append({"type": "noted"})
    return {"results": results}
