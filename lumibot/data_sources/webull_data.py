"""Non-display Webull OpenAPI market data."""
from datetime import timedelta
from threading import Lock

import pandas as pd

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.webull_helpers import WebullAPIError, make_client, paper_mode, request_json, require_number


class WebullData(DataSource):
    SOURCE = "WEBULL"
    AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES = True

    def __init__(self, config, *, data_client=None, **kwargs):
        self._config = dict(config)
        self.is_paper = paper_mode(self._config.get("PAPER"))
        self._client = data_client
        self._client_lock = Lock()
        super().__init__(**kwargs)
        self.name = "webull"
        self._market_stream = None
        self._futures_terms = {}
        self._option_symbols = {}

    @property
    def client(self):
        with self._client_lock:
            if self._client is None:
                self._client = make_client(self._config, trading=False)
            return self._client

    @staticmethod
    def _realtime_instrument(asset):
        if isinstance(asset, tuple):
            base, quote = asset
            if base.asset_type == Asset.AssetType.CRYPTO:
                return base.symbol + quote.symbol, "US_CRYPTO"
            asset = base
        if asset.asset_type == Asset.AssetType.STOCK:
            return asset.symbol, "US_STOCK"
        if asset.asset_type == Asset.AssetType.FUTURE:
            return asset.symbol, "US_FUTURES"
        raise ValueError("Webull realtime bars require a stock, ETF, dated future, or (crypto, quote) pair")

    def start_realtime_bars(self, asset, keep_bars=30):
        from lumibot.data_sources.webull_stream import WebullMarketStream

        symbol, category = self._realtime_instrument(asset)
        with self._client_lock:
            if self._market_stream is None:
                self._market_stream = WebullMarketStream(self._config, paper=self.is_paper)
            self._market_stream.start(symbol, category, keep_bars)

    def get_realtime_bars(self, asset):
        stream = self._market_stream
        if stream is None:
            return None
        return stream.bars(*self._realtime_instrument(asset))

    def cancel_realtime_bars(self, asset):
        with self._client_lock:
            stream = self._market_stream
            if stream is not None and stream.cancel(*self._realtime_instrument(asset)):
                self._market_stream = None
                stream.close()

    def shutdown(self):
        if self._market_stream is not None:
            self._market_stream.close()
        super().shutdown()

    @staticmethod
    def _stock_symbol(asset):
        if isinstance(asset, str):
            asset = Asset(asset)
        if asset.asset_type != Asset.AssetType.STOCK:
            raise ValueError("This Webull data candidate currently handles US stocks and ETFs")
        return asset

    def option_contracts(self, asset, **filters):
        """Read Webull's current listed option terms, including provider symbol and expiry."""
        cursor = None
        seen = set()
        rows = []
        while True:
            page = request_json("option contracts", self.client.instrument.list_option_contracts,
                                underlying_symbols=asset.symbol, pagination_key=cursor, **filters)
            rows.extend(page["data"])
            cursor = page.get("pagination_key")
            if not cursor:
                return rows
            if cursor in seen:
                raise WebullAPIError("option contracts returned a repeated cursor")
            seen.add(cursor)

    def option_symbol(self, asset):
        if asset.expiration is None or not asset.right:
            raise ValueError("Webull option data requires expiry, strike and right")
        expiry, right = asset.expiration.isoformat(), str(asset.right).upper()
        strike = require_number(asset.strike, "option strike")
        terms = asset.symbol, expiry, right, strike
        if terms not in self._option_symbols:
            contracts = self.option_contracts(asset, start_date=expiry, root_symbol=asset.symbol,
                                             option_type=right, strike_price_gte=str(strike), strike_price_lte=str(strike))
            matches = [row for row in contracts if row.get("expiration_date") == expiry
                       and row.get("option_type") == right
                       and require_number(row.get("strike_price"), "option strike") == strike]
            if len(matches) != 1:
                raise ValueError("Webull option data requires exactly one matching listed contract")
            self._option_symbols[terms] = matches[0]["symbol"]
        return self._option_symbols[terms]

    def get_chains(self, asset, quote=None):
        chains = {"CALL": {}, "PUT": {}}
        multipliers = set()
        for row in self.option_contracts(asset, root_symbol=asset.symbol):
            right = row["option_type"]
            expiry = row["expiration_date"]
            chains[right].setdefault(expiry, set()).add(float(require_number(row["strike_price"], "strike")))
            multipliers.add(require_number(row["multiplier"], "option multiplier"))
        if len(multipliers) > 1:
            raise ValueError("Webull returned mixed option contract multipliers for the selected root")
        return {
            "Multiplier": next(iter(multipliers), None),
            "Chains": {right: {expiry: sorted(strikes) for expiry, strikes in terms.items()}
                       for right, terms in chains.items()},
            "Exchange": "SMART",
        }

    def futures_contract(self, symbol):
        symbol = symbol.upper()
        if symbol not in self._futures_terms:
            rows = request_json("futures contract", self.client.instrument.get_futures_instrument,
                                symbol, "US_FUTURES")
            exact = next((row for row in rows if row.get("symbol") == symbol), None)
            if exact is None:
                raise ValueError("Webull did not return the exact dated futures contract")
            self._futures_terms[symbol] = exact
        return self._futures_terms[symbol]

    def _futures_snapshot(self, symbol):
        operation = self.client.futures_market_data.get_futures_snapshot
        if symbol.endswith("MAIN"):
            rows = request_json("snapshot", operation, symbol[:-4] + "main", "US_FUTURES")
            if len(rows) == 1:
                return rows[0]
            return None
        try:
            rows = request_json("snapshot", operation, symbol, "US_FUTURES")
        except WebullAPIError as error:
            if (error.status, error.code) != (417, "ILLEGAL_PARAMETER"):
                raise
            code = self.futures_contract(symbol)["code"]
            # PaperTrade may expose prices only through the continuous alias.
            # Accept this fallback only if the feed resolves to the same expiry.
            rows = request_json("snapshot", operation, code + "main", "US_FUTURES")
        return next((row for row in rows if row.get("symbol") == symbol), None)

    def get_quote(self, asset, quote=None, exchange=None):
        if isinstance(asset, str):
            asset = Asset(asset)
        wire_symbol = asset.symbol
        if asset.asset_type == Asset.AssetType.STOCK:
            operation, category = self.client.market_data.get_snapshot, "US_STOCK"
        elif asset.asset_type == Asset.AssetType.CRYPTO:
            if quote is None:
                raise ValueError("Webull crypto quotes require an explicit quote currency")
            operation, category = self.client.crypto_market_data.get_crypto_snapshot, "US_CRYPTO"
            wire_symbol += quote.symbol
        elif asset.asset_type in {Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE}:
            return self._quote_from_snapshot(asset, self._futures_snapshot(wire_symbol))
        elif asset.asset_type == Asset.AssetType.OPTION:
            wire_symbol = self.option_symbol(asset)
            operation, category = self.client.option_market_data.get_option_snapshot, "US_OPTION"
        else:
            raise ValueError("Webull snapshot is unavailable for the requested asset type")
        records = request_json("snapshot", operation, wire_symbol, category)
        row = next((record for record in records if record.get("symbol") == wire_symbol), None)
        return self._quote_from_snapshot(asset, row)

    @staticmethod
    def _quote_from_snapshot(asset, row):
        if row is None:
            raise ValueError("Webull did not return a snapshot for the requested symbol")
        quote_time = _timestamp(row.get("quote_time"))
        timestamp = _timestamp(row.get("last_trade_time")) or quote_time
        if timestamp is None:
            raise ValueError("Webull did not provide a snapshot timestamp")
        prices = {field: float(require_number(row[field], field))
                  for field in ("price", "bid", "ask", "bid_size", "ask_size", "volume")
                  if row.get(field) not in (None, "")}
        return Quote(asset, timestamp=timestamp, quote_time=quote_time, **prices)

    def get_last_price(self, asset, quote=None, exchange=None):
        return self.get_quote(asset, quote, exchange).price

    def get_equity_daily_range(self, symbol, start, end):
        return self._daily_range(symbol, start, end, self.client.market_data.get_batch_history_bar,
                                 "US_STOCK", symbol_arg=[symbol])

    def get_option_daily_range(self, symbol, start, end):
        return self._daily_range(symbol, start, end, self.client.option_market_data.get_option_history_bars,
                                 "US_OPTION", symbol_arg=symbol)

    def get_crypto_daily_range(self, symbol, start, end):
        return self._daily_range(symbol, start, end, self.client.crypto_market_data.get_crypto_history_bar,
                                 "US_CRYPTO", symbol_arg=symbol, continuous=True)

    @staticmethod
    def _daily_range(symbol, start, end, endpoint, category, *, symbol_arg, continuous=False):
        from lumibot.tools.helpers import get_trading_days

        ny = "America/New_York"
        start, end = (pd.Timestamp(value) for value in (start, end))
        start = start.tz_localize(ny) if start.tzinfo is None else start.tz_convert(ny)
        end = end.tz_localize(ny) if end.tzinfo is None else end.tz_convert(ny)
        first, last = start.normalize(), end.normalize()
        until = last + pd.DateOffset(days=1)
        cursor = until
        rows = []
        count = 200 if continuous else 1_200
        window = pd.DateOffset(days=count - 1) if continuous else pd.DateOffset(years=4)
        while cursor > first:
            window_start = max(first, cursor - window)
            response = request_json(
                "daily history", endpoint, symbol_arg, category, "D", count=count,
                start_time=int(window_start.timestamp() * 1_000), end_time=int(cursor.timestamp() * 1_000),
            )
            series = response["result"] if isinstance(response, dict) else response
            page = next((row["result"] for row in series if row.get("symbol") == symbol), [])
            rows.extend(page)
            if len(page) >= count:
                oldest = min(_timestamp(row["time"]) for row in page)
                if oldest >= cursor:
                    raise WebullAPIError("daily history cursor did not advance")
                cursor = oldest - timedelta(milliseconds=1)
            else:
                cursor = window_start
        if not rows:
            return None
        frame = pd.DataFrame(rows)
        opens = pd.DatetimeIndex([_timestamp(value) for value in frame.pop("time")]).tz_convert(ny).normalize()
        frame.index = opens
        frame = frame.loc[(first <= frame.index) & (frame.index <= last)]
        if continuous:
            frame.index = frame.index + pd.DateOffset(days=1)
        else:
            schedule = get_trading_days("NYSE", first.date().isoformat(), until.date().isoformat())
            closes = dict(zip(schedule.index.date, schedule["market_close"]))
            frame = frame.loc[pd.Index(frame.index.date).isin(closes)]
            frame.index = pd.DatetimeIndex([closes[day] for day in frame.index.date])
        frame.index.name = "datetime"
        frame = frame[~frame.index.duplicated(keep="last")].sort_index()
        # When Webull omits volume, preserve it as missing.
        return frame.reindex(columns=["open", "high", "low", "close", "volume"]).astype(float)

    def get_historical_prices(self, asset, length, timestep="", timeshift=None, quote=None,
                              exchange=None, include_after_hours=True, **kwargs):
        if isinstance(asset, str):
            asset = Asset(asset)
        if timestep not in ("", "minute", "day"):
            raise ValueError("Webull history supports minute and daily bars for eligible products")
        if not isinstance(length, int) or length <= 0:
            raise ValueError("Webull history requires a positive bar count")
        end = pd.Timestamp(self.get_datetime()) - (timeshift or timedelta())
        if timestep == "day":
            start = end - timedelta(days=length * 2 + 7)
            if asset.asset_type == Asset.AssetType.CRYPTO:
                if quote is None:
                    raise ValueError("Webull crypto history requires an explicit quote currency")
                frame = self.get_crypto_daily_range(asset.symbol + quote.symbol, start, end)
            elif asset.asset_type == Asset.AssetType.OPTION:
                expiry = pd.Timestamp(asset.expiration, tz="America/New_York")
                frame = self.get_option_daily_range(self.option_symbol(asset), start, min(end, expiry))
            else:
                self._stock_symbol(asset)
                frame = self.get_equity_daily_range(asset.symbol, start, end)
            if frame is None:
                return None
            frame = frame[frame.index <= end].tail(length)
            return Bars(frame, self.SOURCE, asset, quote=quote) if not frame.empty else None
        frame = self._minute_bars(asset, end, quote, include_after_hours, length=length)
        return Bars(frame, self.SOURCE, asset, quote=quote) if frame is not None else None

    def get_minute_range(self, asset, start, end, *, quote=None, include_after_hours=True):
        """Load completed minute bars in [start, end) for a pandas-backed replay."""
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        if start.tz is None or end.tz is None or end <= start:
            raise ValueError("Webull minute ranges need ordered, timezone-aware start and end times")
        asset = Asset(asset) if isinstance(asset, str) else asset
        return self._minute_bars(asset, end, quote, include_after_hours, start=start)

    def _minute_bars(self, asset, end, quote, include_after_hours, *, start=None, length=None):
        extra = {}
        symbol = asset.symbol
        if asset.asset_type == Asset.AssetType.STOCK:
            endpoint, category, argument = self.client.market_data.get_batch_history_bar, "US_STOCK", [symbol]
            extra["trading_sessions"] = "PRE,RTH,ATH" if include_after_hours else None
            page_size = 1_650
        elif asset.asset_type == Asset.AssetType.FUTURE:
            endpoint, category, argument = self.client.futures_market_data.get_futures_history_bars, "US_FUTURES", symbol
            page_size = 1_200
        elif asset.asset_type == Asset.AssetType.OPTION:
            symbol = self.option_symbol(asset)
            endpoint, category, argument = self.client.option_market_data.get_option_history_bars, "US_OPTION", symbol
            page_size = 1_200
        elif asset.asset_type == Asset.AssetType.CRYPTO:
            if quote is None:
                raise ValueError("Webull crypto history requires an explicit quote currency")
            symbol += quote.symbol
            endpoint, category, argument = self.client.crypto_market_data.get_crypto_history_bar, "US_CRYPTO", symbol
            page_size = 200
        else:
            raise ValueError("Webull minute history requires an equity, listed option, future or spot crypto pair")
        cursor, remaining, frames = end, length, []
        while (start is None or cursor >= start) and (remaining is None or remaining > 0):
            if start is not None:
                window_start = max(start, cursor.floor("min") - pd.Timedelta(minutes=page_size - 1))
                extra["start_time"] = int(window_start.timestamp() * 1_000)
            count = page_size if remaining is None else min(remaining + 1, page_size)
            response = request_json("minute history", endpoint, argument, category, "M1",
                                    count=count, end_time=int(cursor.timestamp() * 1_000), **extra)
            groups = response["result"] if isinstance(response, dict) else response
            rows = next((row["result"] for row in groups if row.get("symbol") == symbol), [])
            if not rows:
                if start is None:
                    break
                cursor = window_start - pd.Timedelta(milliseconds=1)
                continue
            frame = pd.DataFrame(rows)
            frame.index = pd.DatetimeIndex([_timestamp(t) for t in frame.pop("time")], name="datetime")
            if frame.index.hasnans:
                raise ValueError("Webull returned a bar without a timestamp")
            oldest = frame.index.min()
            if oldest > cursor:
                # PaperTrade crypto can ignore both time arguments and return
                # its latest N bars. Never shift that data into a prior date.
                if category == "US_CRYPTO":
                    break
                raise ValueError("Webull bars did not advance the historical cursor")
            frame = frame.loc[(frame.index <= cursor) & (frame.index + pd.Timedelta(minutes=1) <= end)]
            if start is not None:
                frame = frame.loc[frame.index >= start]
            frame = frame[~frame.index.duplicated(keep="last")]
            frames.append(frame)
            if remaining is not None:
                remaining -= len(frame)
            cursor = oldest - pd.Timedelta(milliseconds=1)
        if not frames or not any(len(frame) for frame in frames):
            return None
        prices = pd.concat(frames).sort_index()
        prices = prices.reindex(columns=["open", "high", "low", "close", "volume"]).astype(float)
        return prices.tail(length) if length is not None else prices


def _timestamp(value):
    if value in (None, ""):
        return None
    text = str(value)
    if text.isdigit():
        return pd.to_datetime(int(text), unit="ms", utc=True)
    return pd.to_datetime(value, utc=True)
