"""Kalshi selected-contract prices through the existing DataSource interface."""

from collections import deque
from decimal import Decimal

import pandas as pd

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.helpers import parse_timestep_qty_and_unit
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value


class KalshiData(DataSource):
    """USD prediction contracts, selected by market ticker and YES/NO outcome."""

    SOURCE = "KALSHI"
    MIN_TIMESTEP = "minute"
    _HISTORY_WINDOW = 4999

    def __init__(self, config=None, *, client=None, **kwargs):
        super().__init__(**kwargs)
        config = config or {}
        self.name = "kalshi"
        configured = config.get("CLIENT", config.get("KALSHI_CLIENT"))
        self._client = client if client is not None else configured
        self._owns_client = self._client is None
        if self._owns_client:
            self._client = KalshiClient(config)

    def get_market_metadata(self, market=None, **kwargs):
        if isinstance(market, dict):
            return self._discovery_market(market)
        ticker = market.symbol if isinstance(market, Asset) else market
        ticker = ticker or kwargs.get("market_id") or kwargs.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            raise ValueError("Kalshi metadata requires a market ticker")
        try:
            payload = self._client.request("GET", f"/markets/{KalshiClient.path_id(ticker)}", authenticated=False)
        except KalshiAPIError as exc:
            if exc.status_code != 404:
                raise
            payload = self._client.request(
                "GET", f"/historical/markets/{KalshiClient.path_id(ticker)}", authenticated=False
            )
        row = payload.get("market")
        if not isinstance(row, dict) or row.get("ticker") != ticker:
            raise KalshiAPIError("Kalshi returned an invalid market response")
        return row

    @staticmethod
    def _discovery_market(row):
        """Keep native metadata and serialize the existing outcome-specific Assets."""
        if not isinstance(row, dict) or not isinstance(row.get("ticker"), str) or not row["ticker"]:
            raise KalshiAPIError("Kalshi returned invalid market metadata")
        result = dict(row)
        result["outcomes"] = ["yes", "no"]
        result["outcome_assets"] = [
            Asset(row["ticker"], "prediction_contract", outcome=outcome).to_dict() for outcome in result["outcomes"]
        ]
        return result

    def search_markets(self, query=None, limit=20, **kwargs):
        """Discover native markets, using series metadata for topic searches.

        Kalshi's market endpoint has no text-search parameter. A topic matches
        the public series title/ticker/tags, then its market pages supply actual
        executable tickers. Results stop at limit and are not a claim of
        exhaustive venue coverage. Native market filters remain configurable.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("Kalshi search limit must be a nonnegative integer")
        if limit == 0:
            return []
        if query is not None and not isinstance(query, str):
            raise ValueError("Kalshi search query must be a string")
        terms = (query or "").casefold().split()
        params = dict(kwargs)
        category, tags = params.pop("category", None), params.pop("tags", None)
        params.setdefault("status", "open")
        params["limit"] = min(limit, 1000)
        params = {key: value for key, value in params.items() if value is not None}
        scoped = bool(params.get("series_ticker") or params.get("event_ticker") or params.get("tickers"))

        def matches(row):
            text = " ".join(str(row.get(key, "")) for key in ("ticker", "title", "subtitle", "tags")).casefold()
            return all(term in text for term in terms)

        if (terms or category or tags) and not scoped:
            filters = {"include_volume": True}
            if category:
                filters["category"] = category
            if tags:
                filters["tags"] = ",".join(tags) if isinstance(tags, (list, tuple)) else tags
            payload = self._client.request("GET", "/series", params=filters, authenticated=False)
            series = payload.get("series")
            if not isinstance(series, list) or any(
                not isinstance(row, dict) or not isinstance(row.get("ticker"), str) or not row["ticker"]
                for row in series
            ):
                raise KalshiAPIError("Kalshi response is missing the series list")
            selected = [row for row in series if matches(row)]
            # Provider volume ranks active series ahead of old empty templates.
            selected.sort(key=lambda row: decimal_value(row.get("volume_fp") or 0, "series volume"), reverse=True)
            scopes = [(dict(params, series_ticker=row["ticker"]), row) for row in selected]
        else:
            scopes = [(params, None)]
        found, seen = [], set()
        # Diversify a bounded topic result across matching native series. A
        # high-volume daily template can have hundreds of empty new contracts;
        # it must not hide a different matching series with executable quotes.
        active = deque(
            (iter(self._client.pages("/markets", "markets", params=filters, authenticated=False)), series)
            for filters, series in scopes
        )
        while active:
            rows, series = active.popleft()
            try:
                row = next(rows)
            except StopIteration:
                continue
            active.append((rows, series))
            market = self._discovery_market(row)
            if scoped and terms and not matches(market):
                continue
            if market["ticker"] in seen:
                continue
            seen.add(market["ticker"])
            if series is not None:
                market.setdefault("series_ticker", series["ticker"])
                for key in ("category", "tags", "settlement_sources", "contract_terms_url"):
                    if key in series:
                        market.setdefault(key, series[key])
            found.append(market)
            if len(found) >= limit:
                return found
        return found

    def get_event(self, event_id=None, slug=None, **kwargs):
        """Read an event by its native ticker, preserving its child market identities."""
        ticker = event_id or slug or kwargs.pop("event_ticker", None) or kwargs.pop("id", None)
        if ticker is None or not str(ticker).strip():
            raise ValueError("Kalshi event_id requires a native event ticker")
        params = dict(kwargs)
        if not params.get("exclude_markets"):
            params.setdefault("with_nested_markets", True)
        payload = self._client.request(
            "GET", f"/events/{KalshiClient.path_id(ticker)}", params=params, authenticated=False
        )
        event = payload.get("event")
        if not isinstance(event, dict) or event.get("event_ticker") != str(ticker):
            raise KalshiAPIError("Kalshi returned an invalid event response")
        result = dict(event)
        rows = result.get("markets", payload.get("markets", []))
        if not isinstance(rows, list):
            raise KalshiAPIError("Kalshi response is missing the event markets list")
        result["markets"] = [self._discovery_market(row) for row in rows]
        return result

    def get_market_rules(self, market=None, **kwargs):
        """Return payout criteria, source documents, price grid and separate dates."""
        metadata = self.get_market_metadata(market, **kwargs)
        event = self.get_event(metadata["event_ticker"], exclude_markets=True) if metadata.get("event_ticker") else {}
        ticker = metadata.get("series_ticker") or event.get("series_ticker")
        series = {}
        if ticker:
            payload = self._client.request("GET", f"/series/{KalshiClient.path_id(ticker)}", authenticated=False)
            series = payload.get("series")
            if not isinstance(series, dict) or series.get("ticker") != ticker:
                raise KalshiAPIError("Kalshi returned an invalid series response")
        primary, secondary = metadata.get("rules_primary"), metadata.get("rules_secondary")
        return {
            "question": metadata.get("title"),
            "description": "\n\n".join(value for value in (primary, secondary) if value),
            "rules_primary": primary,
            "rules_secondary": secondary,
            "resolution_source": series.get("settlement_sources", []),
            "settlement_sources": series.get("settlement_sources", []),
            "contract_terms_url": series.get("contract_terms_url"),
            "contract_url": series.get("contract_url"),
            "end_date": metadata.get("close_time"),
            "expiration_time": metadata.get("expiration_time"),
            "expected_expiration_time": metadata.get("expected_expiration_time"),
            "early_close_condition": metadata.get("early_close_condition"),
            "can_close_early": metadata.get("can_close_early"),
            "price_ranges": metadata.get("price_ranges", []),
            "outcomes": ["yes", "no"],
            "outcome_assets": self._discovery_market(metadata)["outcome_assets"],
            "raw": metadata,
        }

    def get_chains(self, asset, quote=None):
        return {}

    @staticmethod
    def _validate_asset(asset, quote=None, exchange=None):
        if not isinstance(asset, Asset) or asset.asset_type != "prediction_contract":
            raise ValueError("Kalshi requires a prediction_contract Asset")
        if asset.outcome not in ("yes", "no"):
            raise ValueError("Kalshi requires an explicit Asset outcome='yes' or outcome='no'")
        if quote is not None and (quote.symbol != "USD" or quote.asset_type != "forex"):
            raise ValueError("Kalshi prediction contracts are quoted in USD")

    def _market(self, asset, quote=None, exchange=None):
        self._validate_asset(asset, quote, exchange)
        return self.get_market_metadata(asset)

    @staticmethod
    def _selected_price(market, outcome):
        value = market.get("last_price_dollars")
        if value is None:
            return None
        price = decimal_value(value, "last price")
        if not 0 <= price <= 1:
            raise KalshiAPIError("Kalshi returned an invalid prediction price")
        return float(price if outcome == "yes" else Decimal(1) - price)

    def get_last_price(self, asset, quote=None, exchange=None):
        return self._selected_price(self._market(asset, quote, exchange), asset.outcome)

    def get_quote(self, asset, quote=None, exchange=None):
        market = self._market(asset, quote, exchange)
        payload = self._client.request(
            "GET", f"/markets/{KalshiClient.path_id(asset.symbol)}/orderbook", authenticated=False
        )
        book = payload.get("orderbook_fp")
        if not isinstance(book, dict):
            raise KalshiAPIError("Kalshi returned an invalid fixed-point order book")

        def best(side):
            levels = book.get(side + "_dollars")
            if not isinstance(levels, list):
                raise KalshiAPIError("Kalshi order book is missing a side")
            parsed = []
            for level in levels:
                if not isinstance(level, (list, tuple)) or len(level) != 2:
                    raise KalshiAPIError("Kalshi returned an invalid book level")
                price, size = (decimal_value(value, "book level") for value in level)
                if not 0 <= price <= 1 or size < 0:
                    raise KalshiAPIError("Kalshi returned an invalid book level")
                if size:
                    parsed.append((price, size))
            return max(parsed, key=lambda value: value[0]) if parsed else None

        own = best(asset.outcome)
        other = best("no" if asset.outcome == "yes" else "yes")
        return Quote(
            asset=asset,
            price=self._selected_price(market, asset.outcome),
            bid=float(own[0]) if own else None,
            ask=float(Decimal(1) - other[0]) if other else None,
            bid_size=float(own[1]) if own else None,
            ask_size=float(other[1]) if other else None,
            raw_data=payload,
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs
    ):
        self._validate_asset(asset, quote, exchange)
        period, native = self._history_period(timestep)
        if length is not None and (isinstance(length, bool) or int(length) != length or length <= 0):
            raise ValueError("Kalshi history length must be a positive integer or None for a date range")
        end_arg = next(
            (kwargs[k] for k in ("end", "end_dt", "end_date", "datetime_end") if kwargs.get(k) is not None),
            self.get_datetime(adjust_for_delay=True),
        )
        end = self._utc(end_arg)
        if timeshift is not None:
            shift = (
                pd.Timedelta(minutes=period * timeshift)
                if isinstance(timeshift, (int, float))
                else pd.Timedelta(timeshift)
            )
            end -= shift
        end = end.floor(f"{period}min")
        start_arg = next(
            (kwargs[k] for k in ("start", "start_dt", "start_date", "datetime_start") if kwargs.get(k) is not None),
            None,
        )
        if start_arg is not None:
            start = self._utc(start_arg)
        elif length is not None:
            start = end - pd.Timedelta(minutes=period * (int(length) - 1))
        else:
            start = self._utc(self.get_market_metadata(asset)["open_time"])
        columns = ["open", "high", "low", "close", "volume", "bid", "ask", "open_interest"]
        records, raw = [], []
        # The API labels bars by their inclusive END, unlike start-stamped Data.
        # Include all constituent bars of the first requested aggregate.
        first = int((start - pd.Timedelta(minutes=period - native)).timestamp())
        last = int(end.timestamp())
        step = native * 60
        for left in range(first, last + 1, self._HISTORY_WINDOW * step):
            right = min(last, left + (self._HISTORY_WINDOW - 1) * step)
            candles = self._candlesticks(asset.symbol, left, right, native)
            raw.extend(candles)
            for candle in candles:
                ts = int(candle["end_period_ts"])
                if not left <= ts <= right:
                    continue
                prices = candle.get("price") or {}
                values = {name: self._candle_number(prices, name) for name in ("open", "high", "low", "close")}
                bid = self._candle_number(candle.get("yes_bid") or {}, "close")
                ask = self._candle_number(candle.get("yes_ask") or {}, "close")
                if asset.outcome == "no":
                    values = {
                        "open": self._complement(values["open"]),
                        "high": self._complement(values["low"]),
                        "low": self._complement(values["high"]),
                        "close": self._complement(values["close"]),
                    }
                    bid, ask = self._complement(ask), self._complement(bid)
                records.append(
                    {
                        "datetime": pd.Timestamp(ts, unit="s", tz="UTC"),
                        **values,
                        "bid": bid,
                        "ask": ask,
                        "volume": self._candle_count(candle, "volume"),
                        "open_interest": self._candle_count(candle, "open_interest"),
                    }
                )
        if records:
            frame = pd.DataFrame(records).set_index("datetime").sort_index()
            frame = frame[~frame.index.duplicated(keep="last")]
            if period != native:
                frame = (
                    frame.resample(f"{period}min", label="right", closed="right")
                    .agg(
                        {
                            "open": "first",
                            "high": "max",
                            "low": "min",
                            "close": "last",
                            "volume": lambda values: values.sum(min_count=1),
                            "bid": "last",
                            "ask": "last",
                            "open_interest": "last",
                        }
                    )
                    .dropna(how="all")
                )
            frame = frame[(frame.index >= start) & (frame.index <= end)]
            if length is not None:
                frame = frame.tail(int(length))
        else:
            frame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([], name="datetime", tz="UTC"))
        frame.attrs["bar_timestamp"] = "end"
        return Bars(frame, self.SOURCE, asset, quote=quote, raw=raw)

    @staticmethod
    def _utc(value):
        value = pd.Timestamp(value)
        return value.tz_localize("UTC") if value.tzinfo is None else value.tz_convert("UTC")

    @staticmethod
    def _history_period(timestep):
        quantity, unit = parse_timestep_qty_and_unit(timestep or "minute")
        multiplier = {"minute": 1, "hour": 60, "day": 1440, "week": 10080}.get(unit)
        if multiplier is None or quantity <= 0 or int(quantity) != quantity:
            raise ValueError("Kalshi history supports positive minute/hour/day/week timesteps")
        period = int(quantity) * multiplier
        native = 1440 if period % 1440 == 0 else 60 if period % 60 == 0 else 1
        return period, native

    @staticmethod
    def _complement(value):
        return None if value is None else float(Decimal(1) - Decimal(str(value)))

    @staticmethod
    def _candle_number(row, name):
        value = row.get(name + "_dollars", row.get(name))
        if value is None:
            return None
        value = decimal_value(value, "candle price")
        if not 0 <= value <= 1:
            raise KalshiAPIError("Kalshi returned a candle price outside zero to one")
        return float(value)

    @staticmethod
    def _candle_count(row, name):
        value = row.get(name + "_fp", row.get(name))
        if value is None:
            return None
        value = decimal_value(value, "candle count")
        if value < 0:
            raise KalshiAPIError("Kalshi returned a negative candle count")
        return float(value)

    def _candlesticks(self, ticker, start, end, interval):
        params = {"start_ts": start, "end_ts": end, "period_interval": interval}
        live = None
        try:
            payload = self._client.request(
                "GET", "/markets/candlesticks", params={**params, "market_tickers": ticker}, authenticated=False
            )
            markets = payload.get("markets")
            if not isinstance(markets, list):
                raise KalshiAPIError("Kalshi candlestick response is missing markets")
            for market in markets:
                if market.get("market_ticker", market.get("ticker")) == ticker:
                    live = market.get("candlesticks")
                    if not isinstance(live, list):
                        raise KalshiAPIError("Kalshi returned invalid candlesticks")
                    if live:
                        return live
        except KalshiAPIError as exc:
            if exc.status_code != 404:
                raise
        # Archival is a market-settlement partition, not a candle-date split.
        try:
            archived = self._client.request(
                "GET",
                f"/historical/markets/{KalshiClient.path_id(ticker)}/candlesticks",
                params=params,
                authenticated=False,
            )
        except KalshiAPIError as exc:
            if exc.status_code == 404 and live is not None:
                return live
            raise
        if archived.get("ticker") != ticker or not isinstance(archived.get("candlesticks"), list):
            raise KalshiAPIError("Kalshi returned an invalid historical candle response")
        return archived["candlesticks"]

    def shutdown(self):
        super().shutdown()
        if self._owns_client:
            self._client.close()
