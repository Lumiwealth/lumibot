from __future__ import annotations

import time
from threading import Lock

from lumibot._lazy_imports import LazyLogger, LazyModule, lazy_class

from .data_source import DataSource

logger = LazyLogger(__name__)
datetime = LazyModule("datetime")
pd = LazyModule("pandas")
Asset = lazy_class("lumibot.entities", "Asset")


class CcxtData(DataSource):
    SOURCE = "CCXT"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m"]},
        {"timestep": "day", "representations": ["1d"]},
    ]
    IS_BACKTESTING_DATA_SOURCE = False

    """Common base class for data_sources/ccxt and brokers/ccxt"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        super().__init__(**kwargs)
        self.name = "ccxt"
        self.max_workers = min(max_workers, 200)
        self._markets_loaded = False
        self._markets_lock = Lock()
        self._stream = None

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        try:
            import ccxt

            exchange_class = getattr(ccxt, config["exchange_id"])
        except (ImportError, AttributeError, KeyError):
            raise Exception(
                "Could not find exchange named '{}'. Are you sure you are spelling the exchange_id correctly?".format(
                    config["exchange_id"]
                )
            )

        self.config = config
        self.api = exchange_class(config)
        # Remove CCXT's own partner IDs and builder fees; attach only LumiBot's configured ones.
        from lumibot.tools.ccxt_partner_ids import apply_partner_config

        apply_partner_config(self.api)
        is_sandbox = True if "sandbox" not in config else config["sandbox"]
        self.api.set_sandbox_mode(is_sandbox)
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True

    def _ensure_markets_loaded(self):
        if self._markets_loaded:
            return
        with self._markets_lock:
            if self._markets_loaded:
                return
            try:
                self.api.load_markets()
                from lumibot.tools.ccxt_phemex import normalize_markets
                normalize_markets(self.api)
                self._markets_loaded = True
            except Exception as exc:
                logger.warning(
                    "[CCXT] load_markets() failed for exchange_id=%s sandbox=%s: %s",
                    self.config.get("exchange_id"),
                    self.config.get("sandbox", True),
                    exc,
                )
                raise

    def get_market(self, asset, quote=None):
        """Resolve a spot or contract instrument without losing settlement."""
        from lumibot.tools.symbol_normalization import resolve_ccxt_instrument_market

        self._ensure_markets_loaded()
        if getattr(asset, "asset_type", None) == "option":
            return self._get_option_market(asset, quote)
        from lumibot.tools.ccxt_phemex import normalize_market
        return normalize_market(self.api, resolve_ccxt_instrument_market(self.api.markets, asset, quote,
                                              exchange_id=self.config.get("exchange_id")))

    def _get_option_market(self, asset, quote=None):
        """Find the CCXT option market for a LumiBot option asset (base, expiry, strike, right)."""
        from decimal import Decimal

        right = str(getattr(asset, "right", "") or "").lower()
        expiration = getattr(asset, "expiration", None)
        strike = getattr(asset, "strike", None)
        if right not in ("call", "put") or expiration is None or strike is None:
            raise ValueError("A CCXT option needs expiration, strike and right (call or put)")
        quote_symbol = getattr(quote, "symbol", quote)
        candidates = []
        for market in self.api.markets.values():
            if not market.get("option") or market.get("base") != asset.symbol:
                continue
            if str(market.get("optionType") or "").lower() != right:
                continue
            if market.get("strike") is None or Decimal(str(market["strike"])) != Decimal(str(strike)):
                continue
            expiry = market.get("expiry")
            if expiry is None or datetime.datetime.fromtimestamp(
                    expiry / 1000, tz=datetime.timezone.utc).date() != expiration:
                continue
            candidates.append(market)
        if quote_symbol and len(candidates) > 1:
            narrowed = [m for m in candidates if quote_symbol in (m.get("quote"), m.get("settle"))]
            candidates = narrowed or candidates
        if len(candidates) == 1:
            return candidates[0]
        if not candidates:
            raise ValueError(f"No CCXT option market for {asset.symbol} {expiration} {strike} {right}")
        raise ValueError(f"Multiple CCXT option markets for {asset.symbol} {expiration} {strike} {right}; "
                         "pass the quote or settlement currency")

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for CcxtData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, quote=quote)
        return response[asset]

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=True
    ):
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs = dict(limit=length)
        if timeshift:
            kwargs["end"] = datetime.datetime.now(datetime.timezone.utc) - timeshift

        result = {}
        for asset in assets:
            if isinstance(asset, tuple):
                symbol = self.get_market(asset[0], asset[1])["symbol"]
            else:
                symbol = self.get_market(asset, quote)["symbol"]
            data = self.get_barset_from_api(self.api, symbol, parsed_timestep, **kwargs)
            result[asset] = data

        return result

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        raise NotImplementedError(
            "Lumibot CcxtData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, return_polars: bool = False
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars

    def get_barset_from_api(self, api, symbol, freq, limit=None, end=None):
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.

        Reads go through the shared bar cache (one series per exchange
        connection, symbol and bar size), so only uncached ranges are paged
        from the exchange.
        """
        from lumibot.tools import shared_bar_cache

        if not api.has.get("fetchOHLCV"):
            # Some CCXT exchanges publish no historical candles at all. Say so
            # plainly instead of failing later with a provider NotSupported.
            raise NotImplementedError(
                f"CCXT exchange '{getattr(api, 'id', 'unknown')}' does not provide historical "
                "candles (fetchOHLCV). Live prices, balances and orders still work; use another "
                "DATA_SOURCE for price history."
            )

        self._ensure_markets_loaded()
        market = self.api.markets.get(symbol, None)
        if market is None:
            logger.error(
                f"A request for market data for {symbol} was submitted. " f"The market for that pair does not exist"
            )
            return None

        if limit is None:
            limit = 300

        if end is None:
            end = datetime.datetime.now(datetime.timezone.utc)
        else:
            # Legacy naive endpoints denote UTC; aware endpoints retain their instant.
            end = pd.Timestamp(end)
            end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
        end = pd.Timestamp(end).tz_convert("UTC")

        buffer = 10  # A few extra datapoints in the download then trim the df.
        timeframe_delta, _ = self.convert_timestep_str_to_timedelta(freq)
        timeframe_ms = int(timeframe_delta.total_seconds() * 1000)
        loop_limit = 300
        rate_limit = 10  # Requests per second in burst.
        page_span_ms = loop_limit * timeframe_ms

        def fetch(range_start, range_end):
            """Page CCXT candles covering [range_start, range_end]. Exchange errors raise."""
            curr_start = int(range_start.value // 1_000_000)
            endunix = int(range_end.value // 1_000_000)
            frames = []
            cnt = 0
            while curr_start <= endunix:
                cnt += 1
                from lumibot.tools.ccxt_phemex import fetch_ohlcv
                candles = fetch_ohlcv(self.api, symbol, freq, since=curr_start, limit=loop_limit, params={})
                df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume"])
                df["datetime"] = pd.to_datetime(df["datetime"], unit="ms", utc=True)
                df = df.set_index("datetime")
                next_start = curr_start + page_span_ms
                last_curr_end = None
                if len(df) > 0:
                    frames.append(df)
                    last_curr_end = int(df.index[-1].value // 1_000_000)
                    next_start = max(last_curr_end + timeframe_ms, curr_start + timeframe_ms)
                if last_curr_end is not None and last_curr_end >= endunix:
                    break
                if next_start <= curr_start:
                    break
                curr_start = next_start
                if cnt % rate_limit == 0:
                    time.sleep(1)
                if cnt > 500:
                    break
            if not frames:
                return None
            out = pd.concat(frames).sort_index()
            return out[~out.index.duplicated(keep="first")]

        exchange_id = (getattr(self, "config", None) or {}).get("exchange_id")
        sandbox = (getattr(self, "config", None) or {}).get("sandbox", True)
        unit = {"m": "minute", "h": "hour", "d": "day", "w": "week", "M": "month"}.get(str(freq)[-1:], "minute")
        series = shared_bar_cache.BarSeries.make(
            "ccxt",
            symbol,
            shared_bar_cache.normalize_timestep(int(str(freq)[:-1] or 1), unit),
            scope=self.shared_bars_scope
            or shared_bar_cache.credential_scope(
                "ccxt", exchange_id, "sandbox" if sandbox else "live", (getattr(self, "config", None) or {}).get("apiKey")
            ),
            # Crypto candles are traded prices; exchanges do not re-adjust them.
            adjusted=False,
            exchange=exchange_id,
            sandbox=bool(sandbox),
        )

        df_ret = None
        for lookback_multiplier in (1, 4, 16, 64):
            start = end - timeframe_delta * (limit + buffer) * lookback_multiplier
            df_ret = shared_bar_cache.read_bars(series, start, end, fetch)
            if len(df_ret) >= limit:
                break

        if df_ret is None or df_ret.empty:
            raise ValueError(f"CCXT returned no {freq} bars for {symbol}; {limit} were requested")

        attrs = dict(df_ret.attrs)
        df_ret = df_ret[~df_ret.index.duplicated(keep="first")]
        df_ret = df_ret.loc[:end]
        df_ret = df_ret.iloc[-limit:]
        df_ret.index.name = "datetime"
        df_ret.attrs = attrs
        if len(df_ret) < limit:
            raise ValueError(
                f"CCXT returned only {len(df_ret)} of {limit} requested {freq} bars for {symbol}"
            )

        return df_ret

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        from lumibot.entities import Bars

        # Parse the dataframe returned from CCXT.
        bars = Bars(response, self.SOURCE, asset, quote=quote, raw=response)
        return bars

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        symbol = self.get_market(asset, quote)["symbol"]
        ticker = self._live_read("watch_ticker", symbol, lambda: self.api.fetch_ticker(symbol))
        price = ticker["last"]

        return price

    def _live_read(self, method, symbol, rest, **kwargs):
        if self._stream is not None:
            import ccxt

            try:
                return self._stream.snapshot(method, symbol=symbol, **kwargs)
            except (TimeoutError, ccxt.NetworkError, ccxt.NotSupported):
                logger.debug("CCXT %s unavailable for %s; reading REST", method, symbol)
        return rest()

    def get_order_book(self, asset, quote=None, limit=None):
        symbol = self.get_market(asset, quote)["symbol"]
        kwargs = {"limit": limit} if limit is not None else {}
        return self._live_read("watch_order_book", symbol, lambda: self.api.fetch_order_book(symbol, **kwargs), **kwargs)

    def get_trades(self, asset, quote=None, since=None, limit=None):
        symbol = self.get_market(asset, quote)["symbol"]
        kwargs = {key: value for key, value in (("since", since), ("limit", limit)) if value is not None}
        return self._live_read("watch_trades", symbol, lambda: self.api.fetch_trades(symbol, **kwargs), **kwargs)

    def get_live_candles(self, asset, quote=None, timeframe="1m", limit=None):
        symbol = self.get_market(asset, quote)["symbol"]
        kwargs = {"timeframe": timeframe}
        if limit is not None:
            kwargs["limit"] = limit
        return self._live_read("watch_ohlcv", symbol, lambda: self.api.fetch_ohlcv(symbol, **kwargs), **kwargs)

    def get_quote(self, asset, quote=None, exchange=None):
        from lumibot.entities import Quote

        symbol = self.get_market(asset, quote)["symbol"]
        ticker = self._live_read("watch_ticker", symbol, lambda: self.api.fetch_ticker(symbol))
        book = self.get_order_book(asset, quote)
        bids, asks = book.get("bids") or [], book.get("asks") or []
        timestamp = ticker.get("timestamp")
        source_time = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.timezone.utc) if timestamp is not None else None
        book_timestamp = book.get("timestamp")
        book_time = datetime.datetime.fromtimestamp(book_timestamp / 1000, datetime.timezone.utc) if book_timestamp is not None else None
        return Quote(asset, price=ticker.get("last"), bid=bids[0][0] if bids else ticker.get("bid"),
                     ask=asks[0][0] if asks else ticker.get("ask"),
                     bid_size=bids[0][1] if bids else None, ask_size=asks[0][1] if asks else None,
                     volume=ticker.get("baseVolume"), timestamp=source_time, quote_time=source_time,
                     bid_time=book_time, ask_time=book_time, raw_data={"ticker": ticker, "order_book": book})
