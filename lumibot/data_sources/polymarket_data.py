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
        timeout: float = 10.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = "polymarket"
        self.config = config or {}
        self.clob_url = (clob_url or self.config.get("CLOB_URL") or self.CLOB_URL).rstrip("/")
        self.gamma_url = (gamma_url or self.config.get("GAMMA_URL") or self.GAMMA_URL).rstrip("/")
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

        if interval == "1m":
            end_dt = datetime.now(timezone.utc)
            if timeshift is not None:
                end_dt = end_dt - timeshift
            start_dt = end_dt - timedelta(minutes=max(int(length or 1), 1))
            params = {
                "market": token_id,
                "fidelity": 1,
                "startTs": int(start_dt.timestamp()),
                "endTs": int(end_dt.timestamp()),
            }
        else:
            params = {"market": token_id, "interval": interval}
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

        params = {}
        if slug:
            params["slug"] = slug
            cache_key = f"slug:{slug}"
        elif condition_id:
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
        response = self._client.get(url, params=params or {})
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
        for key in ("outcomes", "clobTokenIds", "clob_token_ids"):
            if isinstance(market.get(key), str):
                try:
                    market[key] = json.loads(market[key])
                except json.JSONDecodeError:
                    pass
        return market

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
