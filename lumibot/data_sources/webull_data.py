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
        if self._market_stream is None:
            return None
        return self._market_stream.bars(*self._realtime_instrument(asset))

    def cancel_realtime_bars(self, asset):
        if self._market_stream is not None:
            self._market_stream.cancel(*self._realtime_instrument(asset))

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
            terms = self.option_contracts(
                asset, start_date=asset.expiration.isoformat(), root_symbol=asset.symbol,
                option_type=str(asset.right).upper(), strike_price_gte=asset.strike, strike_price_lte=asset.strike,
            )
            terms = [item for item in terms if item.get("expiration_date") == asset.expiration.isoformat()
                     and item.get("option_type") == str(asset.right).upper()
                     and require_number(item.get("strike_price"), "option strike") == require_number(asset.strike, "asset strike")]
            if len(terms) != 1:
                raise ValueError("Webull option snapshot requires exactly one matching contract")
            wire_symbol = terms[0]["symbol"]
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
        from lumibot.tools.helpers import get_trading_days

        ny = "America/New_York"
        start, end = (pd.Timestamp(value) for value in (start, end))
        start = start.tz_localize(ny) if start.tzinfo is None else start.tz_convert(ny)
        end = end.tz_localize(ny) if end.tzinfo is None else end.tz_convert(ny)
        # History clients pass inclusive trade dates. Request the full last
        # exchange day even when end was supplied as local midnight.
        until = end.normalize() + pd.DateOffset(days=1)
        cursor = until
        rows = []
        while cursor > start:
            window_start = max(start.normalize(), cursor - pd.DateOffset(years=4))
            response = request_json(
                "US equity daily history", self.client.market_data.get_batch_history_bar,
                [symbol], "US_STOCK", "D", count=1_200, start_time=int(window_start.timestamp() * 1_000),
                end_time=int(cursor.timestamp() * 1_000),
            )
            page = next((row["result"] for row in response["result"] if row.get("symbol") == symbol), [])
            rows.extend(page)
            if len(page) >= 1_200:
                oldest = min(_timestamp(row["time"]) for row in page)
                if oldest >= cursor:
                    raise WebullAPIError("stock daily history cursor did not advance")
                cursor = oldest - timedelta(milliseconds=1)
            else:
                cursor = window_start
        if not rows:
            return None
        frame = pd.DataFrame(rows)
        frame.index = pd.DatetimeIndex([_timestamp(value) for value in frame.pop("time")]).tz_convert(ny).date
        schedule = get_trading_days("NYSE", start.date().isoformat(), until.date().isoformat())
        closes = dict(zip(schedule.index.date, schedule["market_close"]))
        frame = frame.loc[frame.index.isin(closes)]
        frame.index = pd.DatetimeIndex([closes[day] for day in frame.index], name="datetime")
        frame = frame.loc[~frame.index.duplicated(keep="last")].sort_index()
        return frame[["open", "high", "low", "close", "volume"]].astype(float)

    def get_crypto_daily_range(self, symbol, start, end):
        ny = "America/New_York"
        start, end = (pd.Timestamp(value) for value in (start, end))
        start = start.tz_localize(ny) if start.tzinfo is None else start.tz_convert(ny)
        end = end.tz_localize(ny) if end.tzinfo is None else end.tz_convert(ny)
        cursor = end.normalize() + pd.DateOffset(days=1)
        rows = []
        count = 200
        while cursor > start:
            window_start = max(start.normalize(), cursor - pd.DateOffset(days=count - 1))
            response = request_json(
                "crypto daily history", self.client.crypto_market_data.get_crypto_history_bar,
                symbol, "US_CRYPTO", "D", count=count, start_time=int(window_start.timestamp() * 1_000),
                end_time=int(cursor.timestamp() * 1_000),
            )
            page = next((row["result"] for row in response if row.get("symbol") == symbol), [])
            rows.extend(page)
            if len(page) >= count:
                oldest = min(_timestamp(row["time"]) for row in page)
                if oldest >= cursor:
                    raise WebullAPIError("crypto daily history cursor did not advance")
                cursor = oldest - timedelta(milliseconds=1)
            else:
                cursor = window_start
        if not rows:
            return None
        frame = pd.DataFrame(rows)
        opens = pd.DatetimeIndex([_timestamp(value) for value in frame.pop("time")]).tz_convert(ny)
        opens = opens.normalize()
        first = start.normalize()
        last = end.normalize()
        frame.index = pd.DatetimeIndex(opens + pd.DateOffset(days=1), name="datetime")
        frame = frame.loc[(opens >= first) & (opens <= last)]
        frame = frame[~frame.index.duplicated(keep="last")].sort_index()
        # The sandbox occasionally omits volume. Preserve it as missing.
        return frame.reindex(columns=["open", "high", "low", "close", "volume"]).astype(float)

    def get_historical_prices(self, asset, length, timestep="", timeshift=None, quote=None,
                              exchange=None, include_after_hours=True, **kwargs):
        asset = self._stock_symbol(asset)
        if timestep not in ("", "minute", "day"):
            raise ValueError("Webull equity history supports minute and daily bars")
        if not isinstance(length, int) or length <= 0:
            raise ValueError("Webull history requires a positive bar count")
        end = pd.Timestamp(self.get_datetime()) - (timeshift or timedelta())
        if timestep == "day":
            frame = self.get_equity_daily_range(asset.symbol, end - timedelta(days=length * 2 + 7), end)
            if frame is None:
                return None
            frame = frame[frame.index <= end].tail(length)
            return Bars(frame, self.SOURCE, asset, quote=quote) if not frame.empty else None
        cursor = end
        frames = []
        sessions = "PRE,RTH,POST" if include_after_hours else None
        remaining = length
        while remaining:
            response = request_json("US equity bars", self.client.market_data.get_batch_history_bar,
                                    [asset.symbol], "US_STOCK", "M1", count=min(remaining + 1, 1_650),
                                    trading_sessions=sessions, end_time=int(cursor.timestamp() * 1_000))
            series = next((row["result"] for row in response["result"] if row.get("symbol") == asset.symbol), [])
            if not series:
                break
            frame = pd.DataFrame(series)
            frame.index = pd.DatetimeIndex([_timestamp(value) for value in frame.pop("time")], name="datetime")
            if frame.index.hasnans:
                raise ValueError("Webull returned a bar without a timestamp")
            oldest = frame.index.min()
            if oldest > cursor:
                raise ValueError("Webull bars did not advance the historical cursor")
            frame = frame.loc[(frame.index <= cursor) & (frame.index + timedelta(minutes=1) <= end)]
            frame = frame[~frame.index.duplicated(keep="last")]
            frames.append(frame)
            remaining -= len(frame)
            if remaining <= 0:
                break
            cursor = oldest - timedelta(milliseconds=1)
        if not frames or not any(len(frame) for frame in frames):
            return None
        prices = pd.concat(frames).sort_index().tail(length)
        for field in ("open", "high", "low", "close", "volume"):
            prices[field] = pd.to_numeric(prices[field])
        return Bars(prices, self.SOURCE, asset, quote=quote)


def _timestamp(value):
    if value in (None, ""):
        return None
    text = str(value)
    if text.isdigit():
        return pd.to_datetime(int(text), unit="ms", utc=True)
    return pd.to_datetime(value, utc=True)
