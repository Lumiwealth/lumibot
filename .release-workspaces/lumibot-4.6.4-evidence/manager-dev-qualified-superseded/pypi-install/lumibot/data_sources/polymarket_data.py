from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

import httpx
import pandas as pd

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Bars, Quote


class PolymarketData(DataSource):
    """Public market-data source for Polymarket's international CLOB."""

    SOURCE = "POLYMARKET"
    MIN_TIMESTEP = "minute"
    CLOB_URL = "https://clob.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"
    DATA_API_URL = "https://data-api.polymarket.com"
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    _HISTORY_INTERVALS = {
        "minute": "1m",
        "1m": "1m",
        "hour": "1h",
        "1h": "1h",
        "day": "1d",
        "1d": "1d",
        "week": "1w",
        "1w": "1w",
        "all": "all",
        "max": "max",
    }

    def __init__(
        self,
        config: dict | None = None,
        *,
        client: Any | None = None,
        clob_url: str | None = None,
        gamma_url: str | None = None,
        data_api_url: str | None = None,
        timeout: float = 10.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = "polymarket"
        self.config = config or {}
        self.clob_url = (clob_url or self.config.get("CLOB_URL") or self.CLOB_URL).rstrip("/")
        self.gamma_url = (gamma_url or self.config.get("GAMMA_URL") or self.GAMMA_URL).rstrip("/")
        self.data_api_url = (data_api_url or self.config.get("DATA_API_URL") or self.DATA_API_URL).rstrip("/")
        self.timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None
        self._market_cache: dict[str, dict] = {}
        self._quote_cache: dict[str, Quote] = {}
        self._tick_size_cache: dict[str, Decimal] = {}

    def shutdown(self):
        if self._owns_client and hasattr(self._client, "close"):
            self._client.close()
        super().shutdown()

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        """Prediction contracts are not option chains."""
        return {}

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        **kwargs,
    ):
        token_id = self._token_id(asset)
        interval = self._HISTORY_INTERVALS.get((timestep or "minute").lower())
        if interval is None:
            raise ValueError(f"Unsupported Polymarket history timestep: {timestep!r}")

        start_arg = self._first_present(kwargs, "start", "start_dt", "start_date", "datetime_start")
        end_arg = self._first_present(kwargs, "end", "end_dt", "end_date", "datetime_end")

        if interval == "1m":
            end_dt = self._parse_timestamp(end_arg) if end_arg is not None else datetime.now(timezone.utc)
            if timeshift is not None:
                end_dt = end_dt - timeshift
            start_dt = (
                self._parse_timestamp(start_arg)
                if start_arg is not None
                else end_dt - timedelta(minutes=max(int(length or 1), 1))
            )
            params = {
                "market": token_id,
                "fidelity": 1,
                "startTs": int(start_dt.timestamp()),
                "endTs": int(end_dt.timestamp()),
            }
        else:
            params = {"market": token_id, "interval": interval}
            if start_arg is not None:
                params["startTs"] = int(self._parse_timestamp(start_arg).timestamp())
            if end_arg is not None:
                params["endTs"] = int(self._parse_timestamp(end_arg).timestamp())
        payload = self._request_json(self.clob_url, "/prices-history", params=params)
        rows = payload.get("history") if isinstance(payload, dict) else payload
        rows = rows or []

        records = []
        for row in rows:
            ts = self._first_present(row, "t", "timestamp", "time")
            price = self._first_present(row, "p", "price", "close")
            if ts is None or price is None:
                continue
            timestamp = self._parse_timestamp(ts)
            value = float(self._decimal(price))
            records.append(
                {
                    "datetime": timestamp,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "volume": float(self._first_present(row, "volume", "v") or 0.0),
                }
            )

        if length:
            records = records[-int(length):]

        df = pd.DataFrame(records)
        if df.empty:
            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            df.index = pd.DatetimeIndex([], tz=timezone.utc)
        else:
            df = df.set_index("datetime").sort_index()
        return Bars(df, source=self.SOURCE, asset=asset, quote=quote, raw=payload)

    def get_last_price(self, asset, quote=None, exchange=None):
        token_id = self._token_id(asset)
        try:
            payload = self._request_json(self.clob_url, "/last-trade-price", params={"token_id": token_id})
            price = self._first_present(payload, "price", "last_trade_price", "lastTradePrice")
            if price is not None:
                return float(self._decimal(price))
        except Exception:
            cached = self._quote_cache.get(token_id)
            if cached and cached.price is not None:
                return cached.price

        quote_obj = self.get_quote(asset, quote=quote, exchange=exchange)
        return quote_obj.price

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        token_id = self._token_id(asset)
        book = self.get_order_book(token_id)
        bids = self._levels(book.get("bids", []))
        asks = self._levels(book.get("asks", []))
        best_bid = max(bids, key=lambda level: level[0]) if bids else None
        best_ask = min(asks, key=lambda level: level[0]) if asks else None
        bid = float(best_bid[0]) if best_bid else None
        ask = float(best_ask[0]) if best_ask else None
        bid_size = float(best_bid[1]) if best_bid else None
        ask_size = float(best_ask[1]) if best_ask else None
        midpoint = (bid + ask) / 2 if bid is not None and ask is not None else None
        price = midpoint
        if price is None:
            cached = self._quote_cache.get(token_id)
            price = cached.price if cached else None

        quote_obj = Quote(
            asset=asset,
            price=price,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            mid_price=midpoint,
            raw_data=book,
        )
        self._quote_cache[token_id] = quote_obj
        return quote_obj

    def search_markets(self, query: str | None = None, limit: int = 20, **kwargs) -> list[dict]:
        """Search Polymarket Gamma markets/events.

        With ``query`` this uses Gamma public-search. Without ``query`` it falls
        back to the markets list endpoint using any supplied filters.
        """
        if query:
            params = dict(kwargs)
            params.setdefault("q", query)
            params.setdefault("limit_per_type", limit)
            payload = self._request_json(self.gamma_url, "/public-search", params=self._clean_params(params))
            markets = []
            if isinstance(payload, dict):
                markets.extend(self._listify(payload.get("markets")))
                for event in self._listify(payload.get("events")):
                    for market in self._listify(event.get("markets")):
                        normalized = self._normalize_market(market)
                        normalized.setdefault("eventSlug", event.get("slug"))
                        normalized.setdefault("eventTitle", event.get("title"))
                        markets.append(normalized)
            return [self._normalize_market(item) for item in markets[:limit]]

        params = dict(kwargs)
        params.setdefault("limit", limit)
        payload = self._request_json(self.gamma_url, "/markets", params=self._clean_params(params))
        return [self._normalize_market(item) for item in self._listify(payload)[:limit]]

    def get_event(self, event_id: str | int | None = None, slug: str | None = None, **kwargs) -> dict:
        if slug is None:
            slug = kwargs.get("event_slug")
        if slug:
            return self._model_to_dict(self._request_json(self.gamma_url, f"/events/slug/{slug}")) or {}
        if event_id is None:
            event_id = kwargs.get("id")
        if event_id is None:
            raise ValueError("event_id or slug is required")
        return self._model_to_dict(self._request_json(self.gamma_url, f"/events/{event_id}")) or {}

    def get_market_metadata(self, market=None, **kwargs) -> dict:
        if isinstance(market, dict):
            return self._normalize_market(market)
        if isinstance(market, Asset):
            attached = getattr(market, "polymarket_market", None)
            if attached:
                return self._normalize_market(attached)
            kwargs.setdefault("token_id", market.symbol)
        if isinstance(market, str) and not kwargs:
            kwargs["slug"] = market

        slug = kwargs.get("slug")
        url = kwargs.get("url")
        market_id = kwargs.get("market_id") or kwargs.get("id")
        condition_id = kwargs.get("condition_id") or kwargs.get("conditionId")
        token_id = kwargs.get("token_id") or kwargs.get("asset_id") or kwargs.get("assetId")

        if token_id and not any([slug, url, market_id, condition_id]):
            payload = self._request_json(self.gamma_url, f"/markets/token/{token_id}")
            return self._normalize_market(payload)
        return self.resolve_market(url=url, slug=slug, market_id=market_id, condition_id=condition_id)

    def get_market_rules(self, market=None, **kwargs) -> dict:
        metadata = self.get_market_metadata(market, **kwargs)
        tick_size = self._first_present(
            metadata,
            "orderPriceMinTickSize",
            "minimum_tick_size",
            "minimumTickSize",
            "tick_size",
            "tickSize",
        )
        min_order_size = self._first_present(metadata, "orderMinSize", "min_order_size", "minOrderSize")
        return {
            "question": metadata.get("question"),
            "description": metadata.get("description"),
            "resolution_source": metadata.get("resolutionSource") or metadata.get("resolution_source"),
            "end_date": self._first_present(metadata, "endDateIso", "endDate", "end_date"),
            "uma_end_date": self._first_present(metadata, "umaEndDateIso", "umaEndDate", "uma_end_date"),
            "outcomes": metadata.get("outcomes") or [],
            "clob_token_ids": metadata.get("clobTokenIds") or metadata.get("clob_token_ids") or [],
            "tick_size": tick_size,
            "min_order_size": min_order_size,
            "neg_risk": self._first_present(metadata, "negRisk", "neg_risk"),
            "enable_order_book": self._first_present(metadata, "enableOrderBook", "enable_order_book"),
            "accepting_orders": self._first_present(metadata, "acceptingOrders", "accepting_orders"),
            "active": metadata.get("active"),
            "closed": metadata.get("closed"),
            "archived": metadata.get("archived"),
            "category": metadata.get("category"),
            "tags": metadata.get("tags") or [],
            "raw": metadata,
        }

    def get_resolution_status(self, market=None, **kwargs) -> dict:
        metadata = self.get_market_metadata(market, **kwargs)
        status = self._first_present(metadata, "umaResolutionStatus", "resolutionStatus", "resolvedStatus")
        closed = self._optional_bool(metadata.get("closed"))
        resolved = bool(closed or (status and str(status).lower() not in {"", "pending", "unresolved", "not_started"}))
        outcomes = metadata.get("outcomes") or []
        tokens = metadata.get("tokens") or []
        winner = self._first_present(metadata, "winner", "winningOutcome", "winning_outcome")
        if winner is None:
            winner = self._winning_outcome_from_tokens(tokens, outcomes)
        return {
            "status": status or ("resolved" if resolved else "open"),
            "resolved": resolved,
            "winner": winner,
            "closed": closed,
            "condition_id": self._first_present(metadata, "conditionId", "condition_id"),
            "raw": metadata,
        }

    def get_spread(self, asset, quote=None, exchange=None):
        token_id = self._token_id(asset)
        try:
            payload = self._request_json(self.clob_url, "/spread", params={"token_id": token_id})
            value = self._first_present(payload, "spread")
            if value is not None:
                return float(self._decimal(value))
        except Exception:
            pass
        return super().get_spread(asset, quote=quote, exchange=exchange)

    def get_midpoint(self, asset, quote=None, exchange=None):
        token_id = self._token_id(asset)
        try:
            payload = self._request_json(self.clob_url, "/midpoint", params={"token_id": token_id})
            value = self._first_present(payload, "mid", "midpoint", "midpoint_price", "midpointPrice")
            if value is not None:
                return float(self._decimal(value))
        except Exception:
            pass
        return super().get_midpoint(asset, quote=quote, exchange=exchange)

    def get_recent_trades(self, market=None, limit: int = 100, **kwargs) -> list[dict]:
        params = dict(kwargs)
        params.setdefault("limit", limit)
        if market is not None:
            condition_id = self._condition_id(market)
            if condition_id:
                params.setdefault("market", condition_id)
        payload = self._request_json(self.data_api_url, "/trades", params=self._clean_params(params))
        return self._listify(payload)

    def get_open_interest(self, market=None, **kwargs):
        params = dict(kwargs)
        if market is not None:
            condition_id = self._condition_id(market)
            if condition_id:
                params.setdefault("market", condition_id)
        payload = self._request_json(self.data_api_url, "/oi", params=self._clean_params(params))
        if market is not None:
            rows = self._listify(payload)
            if not rows:
                return None
            value = self._first_present(rows[0], "value", "open_interest", "openInterest")
            return float(self._decimal(value)) if value is not None else None
        return payload

    def get_holders(self, market=None, limit: int = 20, **kwargs) -> list[dict]:
        params = dict(kwargs)
        params.setdefault("limit", limit)
        if market is not None:
            condition_id = self._condition_id(market)
            token_id = None if condition_id else self._safe_token_id(market)
            if condition_id:
                params.setdefault("market", condition_id)
            elif token_id:
                params.setdefault("token", token_id)
        payload = self._request_json(self.data_api_url, "/holders", params=self._clean_params(params))
        return self._listify(payload)

    def get_market_close(self, market=None, **kwargs) -> datetime | None:
        rules = self.get_market_rules(market, **kwargs)
        value = rules.get("end_date") or rules.get("uma_end_date")
        return self._parse_timestamp(value) if value else None

    def get_resolution_source(self, market=None, **kwargs) -> str | None:
        rules = self.get_market_rules(market, **kwargs)
        return rules.get("resolution_source")

    def get_min_order_size(self, market=None, **kwargs) -> Decimal | None:
        rules = self.get_market_rules(market, **kwargs)
        value = rules.get("min_order_size")
        return self._decimal(value) if value is not None else None

    def get_settlement_price(self, asset: Asset, market=None, **kwargs) -> float | None:
        status = self.get_resolution_status(market or asset, **kwargs)
        if not status.get("resolved"):
            return None
        winner = status.get("winner")
        outcome = getattr(asset, "polymarket_outcome", None)
        if outcome is not None and winner is not None:
            return 1.0 if str(outcome).lower() == str(winner).lower() else 0.0
        metadata = status.get("raw") or {}
        token_ids = metadata.get("clobTokenIds") or metadata.get("clob_token_ids") or []
        outcomes = metadata.get("outcomes") or []
        try:
            index = [str(token) for token in token_ids].index(str(asset.symbol))
        except ValueError:
            return None
        if winner is None:
            return None
        return 1.0 if index < len(outcomes) and str(outcomes[index]).lower() == str(winner).lower() else 0.0

    def resolve_market(
        self,
        *,
        url: str | None = None,
        slug: str | None = None,
        market_id: str | None = None,
        condition_id: str | None = None,
    ) -> dict:
        if url and not slug:
            slug = self._slug_from_url(url)

        if market_id:
            cache_key = f"id:{market_id}"
            if cache_key not in self._market_cache:
                self._market_cache[cache_key] = self._normalize_market(
                    self._request_json(self.gamma_url, f"/markets/{market_id}")
                )
            return self._market_cache[cache_key]

        if slug:
            cache_key = f"slug:{slug}"
            if cache_key not in self._market_cache:
                self._market_cache[cache_key] = self._normalize_market(
                    self._request_json(self.gamma_url, f"/markets/slug/{slug}")
                )
            return self._market_cache[cache_key]
        elif condition_id:
            params = {}
            params["condition_ids"] = condition_id
            cache_key = f"condition:{condition_id}"
        else:
            raise ValueError("One of url, slug, market_id, or condition_id is required")

        if cache_key not in self._market_cache:
            payload = self._request_json(self.gamma_url, "/markets", params=params)
            item = payload[0] if isinstance(payload, list) and payload else payload
            self._market_cache[cache_key] = self._normalize_market(item)
        return self._market_cache[cache_key]

    def resolve_contract(self, market: dict, *, outcome: str | None = None, token_id: str | None = None) -> Asset:
        market = self._normalize_market(market)
        outcomes = market.get("outcomes") or []
        token_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []

        if token_id is None:
            if outcome is None:
                raise ValueError("outcome or token_id is required")
            for idx, label in enumerate(outcomes):
                if str(label).lower() == str(outcome).lower():
                    if idx >= len(token_ids):
                        raise ValueError(f"No Polymarket token id found for outcome {outcome!r}")
                    token_id = str(token_ids[idx])
                    break
            if token_id is None:
                raise ValueError(f"Outcome {outcome!r} was not found in market")

        asset = Asset(str(token_id), asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")
        asset.polymarket_market = market
        if outcome:
            asset.polymarket_outcome = outcome
        return asset

    def get_order_book(self, token_id: str | Asset) -> dict:
        token_id = self._token_id(token_id)
        payload = self._request_json(self.clob_url, "/book", params={"token_id": token_id})
        if not isinstance(payload, dict):
            raise ValueError("Polymarket order book response was not an object")
        return payload

    def get_tick_size(self, token_id: str | Asset) -> Decimal | None:
        token_id = self._token_id(token_id)
        if token_id in self._tick_size_cache:
            return self._tick_size_cache[token_id]
        try:
            payload = self._request_json(self.clob_url, "/tick-size", params={"token_id": token_id})
            value = self._first_present(payload, "tick_size", "tickSize", "minimum_tick_size", "minimumTickSize")
        except Exception:
            value = None
        if value is None:
            return None
        tick_size = self._decimal(value)
        self._tick_size_cache[token_id] = tick_size
        return tick_size

    def calculate_market_price(self, token_id: str | Asset, side: str, amount: str | Decimal) -> dict:
        """Return a lightweight pre-trade estimate from the current book."""
        token_id = self._token_id(token_id)
        amount = self._decimal(amount)
        side = str(side).lower()
        book = self.get_order_book(token_id)
        levels = self._levels(book.get("asks" if side == "buy" else "bids", []))
        levels = sorted(levels, key=lambda level: level[0], reverse=(side != "buy"))
        remaining = amount
        notional = Decimal("0")
        shares = Decimal("0")
        worst_price = None
        for price, size in levels:
            if side == "buy":
                level_notional = price * size
                take_notional = min(remaining, level_notional)
                if take_notional <= 0:
                    break
                take_shares = take_notional / price
                remaining -= take_notional
            else:
                take_shares = min(remaining, size)
                take_notional = take_shares * price
                remaining -= take_shares
            notional += take_notional
            shares += take_shares
            worst_price = price
            if remaining <= 0:
                break
        avg_price = notional / shares if shares else None
        return {
            "token_id": token_id,
            "side": side,
            "requested": float(amount),
            "filled_shares_estimate": float(shares),
            "notional_estimate": float(notional),
            "avg_price_estimate": float(avg_price) if avg_price is not None else None,
            "worst_price_estimate": float(worst_price) if worst_price is not None else None,
            "fully_satisfied": remaining <= 0,
            "raw_book": book,
        }

    def apply_market_event(self, event: Any) -> None:
        payload = self._model_to_dict(event)
        if isinstance(payload, list):
            for item in payload:
                self.apply_market_event(item)
            return
        if isinstance(payload, dict) and isinstance(payload.get("price_changes"), list):
            for item in payload["price_changes"]:
                change = dict(item)
                change.setdefault("event_type", "price_change")
                change.setdefault("market", payload.get("market"))
                change.setdefault("timestamp", payload.get("timestamp"))
                self.apply_market_event(change)
            return
        token_id = str(self._first_present(payload, "asset_id", "assetId", "token_id", "tokenId") or "")
        if not token_id:
            return
        event_type = str(self._first_present(payload, "event_type", "eventType", "type") or "").lower()
        if event_type in {"best_bid_ask", "book", "price_change", "last_trade_price"}:
            quote = self._quote_cache.get(token_id)
            bid = self._first_present(payload, "best_bid", "bestBid", "bid")
            ask = self._first_present(payload, "best_ask", "bestAsk", "ask")
            price = self._first_present(payload, "price", "last_trade_price", "lastTradePrice")
            asset = quote.asset if quote else Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT)
            self._quote_cache[token_id] = Quote(
                asset=asset,
                price=float(self._decimal(price)) if price is not None else (quote.price if quote else None),
                bid=float(self._decimal(bid)) if bid is not None else (quote.bid if quote else None),
                ask=float(self._decimal(ask)) if ask is not None else (quote.ask if quote else None),
                raw_data=payload,
            )
        if event_type == "tick_size_change":
            tick_size = self._first_present(payload, "tick_size", "tickSize")
            if tick_size is not None:
                self._tick_size_cache[token_id] = self._decimal(tick_size)

    @staticmethod
    def _slug_from_url(url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        return path.split("/")[-1]

    def _request_json(self, base_url: str, path: str, params: dict | None = None) -> Any:
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        response = self._client.get(url, params=self._clean_params(params or {}))
        if isinstance(response, (dict, list)):
            return response
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        return response.json()

    @staticmethod
    def _token_id(asset_or_token: str | Asset) -> str:
        if isinstance(asset_or_token, Asset):
            if asset_or_token.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
                raise ValueError("PolymarketData requires a prediction_contract asset")
            return str(asset_or_token.symbol)
        return str(asset_or_token)

    @classmethod
    def _normalize_market(cls, payload: Any) -> dict:
        market = cls._model_to_dict(payload)
        for key in ("outcomes", "outcomePrices", "clobTokenIds", "clob_token_ids", "shortOutcomes"):
            if isinstance(market.get(key), str):
                try:
                    market[key] = json.loads(market[key])
                except json.JSONDecodeError:
                    pass
        return market

    @classmethod
    def _listify(cls, value: Any) -> list:
        value = cls._model_to_dict(value)
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, dict):
            for key in ("items", "data", "results", "markets", "events", "holders", "trades"):
                if key in value and isinstance(value[key], (list, tuple)):
                    return list(value[key])
            return [value]
        return [value]

    @staticmethod
    def _model_to_dict(value: Any) -> Any:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if hasattr(value, "dict"):
            return value.dict()
        if isinstance(value, dict):
            return value
        if isinstance(value, (list, tuple)):
            return [PolymarketData._model_to_dict(item) for item in value]
        if hasattr(value, "__dict__"):
            return {k: v for k, v in vars(value).items() if not k.startswith("_")}
        return value

    @staticmethod
    def _first_present(mapping: Any, *keys: str) -> Any:
        if not isinstance(mapping, dict):
            return None
        for key in keys:
            if key in mapping and mapping[key] is not None:
                return mapping[key]
        return None

    @classmethod
    def _condition_id(cls, market: Any) -> str | None:
        if isinstance(market, Asset):
            attached = getattr(market, "polymarket_market", None)
            return cls._condition_id(attached) if attached else None
        if isinstance(market, dict):
            value = cls._first_present(market, "conditionId", "condition_id", "market")
            return str(value) if value else None
        text = str(market) if market is not None else ""
        if text.startswith("0x") and len(text) >= 10:
            return text
        return None

    @classmethod
    def _safe_token_id(cls, value: Any) -> str | None:
        try:
            return cls._token_id(value)
        except Exception:
            return None

    @staticmethod
    def _clean_params(params: dict | None) -> dict:
        clean = {}
        for key, value in (params or {}).items():
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                clean[key] = ",".join(str(item) for item in value)
            else:
                clean[key] = value
        return clean

    @classmethod
    def _winning_outcome_from_tokens(cls, tokens: list, outcomes: list) -> str | None:
        for token in tokens or []:
            item = cls._model_to_dict(token)
            if not isinstance(item, dict):
                continue
            winner = cls._first_present(item, "winner", "winning", "isWinner")
            if cls._optional_bool(winner):
                outcome = cls._first_present(item, "outcome", "name")
                if outcome is not None:
                    return str(outcome)
                outcome_index = cls._first_present(item, "outcomeIndex", "outcome_index")
                try:
                    index = int(outcome_index)
                    return str(outcomes[index]) if 0 <= index < len(outcomes) else None
                except Exception:
                    return None
        return None

    @staticmethod
    def _optional_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"Invalid Polymarket decimal value: {value!r}") from exc

    @classmethod
    def _levels(cls, values: list) -> list[tuple[Decimal, Decimal]]:
        levels = []
        for row in values or []:
            if isinstance(row, dict):
                price = cls._first_present(row, "price", "p")
                size = cls._first_present(row, "size", "s")
            else:
                try:
                    price, size = row[0], row[1]
                except Exception:
                    continue
            levels.append((cls._decimal(price), cls._decimal(size)))
        return levels

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 10_000_000_000:
                ts = ts / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        return pd.to_datetime(value, utc=True).to_pydatetime()
