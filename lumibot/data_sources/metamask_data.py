"""Native Python market data on the network configured for execution."""

import calendar
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import AssetsMapping, Quote
from lumibot.tools.metamask_client import MetaMaskClient, token_payload


class _PairAssetsMapping(AssetsMapping):
    """AssetsMapping that also accepts ``(base, quote)`` tuple keys without losing pair identity."""

    def __init__(self, mapping):
        data = dict(mapping)
        super(AssetsMapping, self).__init__(data)
        symbols_mapping = {}
        tuple_symbol_counts: dict[str, int] = {}
        for key in data:
            if isinstance(key, tuple) and key and hasattr(key[0], "symbol"):
                tuple_symbol_counts[key[0].symbol] = tuple_symbol_counts.get(key[0].symbol, 0) + 1
        for key, value in data.items():
            if isinstance(key, tuple):
                if key and hasattr(key[0], "symbol") and tuple_symbol_counts.get(key[0].symbol) == 1:
                    symbols_mapping.setdefault(key[0].symbol, value)
            elif hasattr(key, "symbol"):
                symbols_mapping[key.symbol] = value
        self._symbols_mapping = symbols_mapping


def _shift_calendar_months(dt: datetime, months: int) -> datetime:
    total_months = dt.year * 12 + (dt.month - 1) - months
    year, zero_month = divmod(total_months, 12)
    if year < 1:
        raise ValueError("timeshift exceeds valid calendar date range")
    month = zero_month + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


class MetaMaskData(DataSource):
    SOURCE = "METAMASK"
    MIN_TIMESTEP = "minute"
    _INTERVAL_SPECS = {
        "minute": ("1m", 60),
        "3 minutes": ("3m", 180),
        "5 minutes": ("5m", 300),
        "15 minutes": ("15m", 900),
        "30 minutes": ("30m", 1800),
        "hour": ("1h", 3600),
        "2 hours": ("2h", 7200),
        "4 hours": ("4h", 14400),
        "8 hours": ("8h", 28800),
        "12 hours": ("12h", 43200),
        "day": ("1d", 86400),
        "3 days": ("3d", 259200),
        "week": ("1w", 604800),
        "month": ("1M", 2678400),
    }
    TIMESTEP_MAPPING = [
        {"timestep": key, "representations": [interval, key]} for key, (interval, _) in _INTERVAL_SPECS.items()
    ]

    def __init__(self, config=None, *, client=None, **kwargs):
        super().__init__(**kwargs)
        self.client = client or MetaMaskClient(config or {})
        self.name = "metamask"

    def _resolve_interval(self, timestep):
        if isinstance(timestep, str) and timestep.strip() == "1M":
            return self._INTERVAL_SPECS["month"]
        from lumibot.tools.helpers import canonicalize_timestep

        canonical = canonicalize_timestep(timestep)
        if canonical in self._INTERVAL_SPECS:
            return self._INTERVAL_SPECS[canonical]
        supported = ", ".join(self._INTERVAL_SPECS)
        raise ValueError(f"Unsupported MetaMask timestep {timestep!r}; use one of: {supported}")

    def supports_native_timestep(self, timestep) -> bool:
        """Return whether Hyperliquid serves ``timestep`` candles directly."""
        try:
            self._resolve_interval(timestep)
            return True
        except ValueError:
            return False

    def get_contract_spec(self, asset, quote=None):
        """Return Hyperliquid linear USDC perpetual contract terms in base units."""
        from lumibot.entities import ContractSpec

        if isinstance(asset, tuple):
            asset, tuple_quote = asset
            if quote is None:
                quote = tuple_quote
        if getattr(asset, "asset_type", None) != "crypto_future":
            raise ValueError("Contract specifications are only defined for Hyperliquid crypto_future assets")
        if quote is not None and getattr(quote, "symbol", quote) != "USDC":
            raise ValueError("Hyperliquid perpetuals settle in USDC")
        market = self.client.market(asset.symbol)
        return ContractSpec(
            base=market.symbol,
            quote="USDC",
            settle="USDC",
            contract_size=Decimal("1"),
            inverse=False,
            quantity_unit="base",
        )

    def get_last_price(self, asset, quote=None, exchange=None):
        if isinstance(asset, tuple):
            asset, tuple_quote = asset
            if quote is None:
                quote = tuple_quote
        if asset.asset_type == "crypto_future":
            if quote is not None and quote.symbol != "USDC":
                raise ValueError("Hyperliquid prices are denominated in USDC")
            return self.client.price(asset.symbol)
        elif asset.asset_type == "crypto":
            return self.client.token_price(token_payload(asset), token_payload(quote) if quote is not None else None)
        else:
            raise ValueError("MetaMask supports onchain crypto and Hyperliquid crypto_future assets")

    def get_last_prices(self, assets, quote=None, exchange=None):
        result = {}
        perp_assets = []
        other_assets = []
        for entry in assets:
            asset = entry[0] if isinstance(entry, tuple) else entry
            item_quote = entry[1] if isinstance(entry, tuple) and quote is None else quote
            if asset.asset_type == "crypto_future":
                if item_quote is not None and item_quote.symbol != "USDC":
                    raise ValueError("Hyperliquid prices are denominated in USDC")
                perp_assets.append((entry, asset))
            else:
                other_assets.append((entry, asset, item_quote))
        if perp_assets:
            batched = self.client.prices([asset.symbol for _, asset in perp_assets], allow_missing=True)
            for entry, asset in perp_assets:
                result[entry] = batched.get(asset.symbol)
        for entry, asset, item_quote in other_assets:
            result[entry] = self.get_last_price(asset, quote=item_quote, exchange=exchange)
        return _PairAssetsMapping(result)

    def get_quote(self, asset, quote=None, exchange=None):
        if isinstance(asset, tuple):
            asset, tuple_quote = asset
            if quote is None:
                quote = tuple_quote
        if asset.asset_type == "crypto_future":
            if quote is not None and quote.symbol != "USDC":
                raise ValueError("Hyperliquid prices are denominated in USDC")
            book = self.client.quote(asset.symbol)
            bid = float(book["bid"]) if book.get("bid") is not None else None
            ask = float(book["ask"]) if book.get("ask") is not None else None
            bid_size = float(book["bidSize"]) if book.get("bidSize") is not None else None
            ask_size = float(book["askSize"]) if book.get("askSize") is not None else None
            if bid is not None and ask is not None:
                price = (bid + ask) / 2
            else:
                price = float(self.client.price(asset.symbol))
            stamp = book.get("time")
            ts = (
                datetime.fromtimestamp(stamp / 1000, tz=timezone.utc)
                if isinstance(stamp, int) and not isinstance(stamp, bool)
                else datetime.now(timezone.utc)
            )
            return Quote(
                asset=asset,
                price=price,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                timestamp=ts,
                quote_time=ts,
                raw_data=book,
            )
        if asset.asset_type == "crypto":
            price = float(
                self.client.token_price(token_payload(asset), token_payload(quote) if quote is not None else None)
            )
            return Quote(asset=asset, price=price, timestamp=datetime.now(timezone.utc))
        raise ValueError("MetaMask supports onchain crypto and Hyperliquid crypto_future assets")

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="minute",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        **kwargs,
    ):
        if isinstance(asset, tuple):
            asset, tuple_quote = asset
            if quote is None:
                quote = tuple_quote
        if asset.asset_type != "crypto_future":
            raise NotImplementedError(
                "MetaMask spot candles are not available; configure a separate historical data source"
            )
        if quote is not None and quote.symbol != "USDC":
            raise ValueError("Hyperliquid prices are denominated in USDC")
        if isinstance(length, bool) or not isinstance(length, int) or not 1 <= length <= 5000:
            raise ValueError("Length must be an integer between 1 and 5000 bars")

        import pandas as pd

        from lumibot.entities import Bars

        interval, seconds = self._resolve_interval(timestep)
        now = datetime.now(timezone.utc)
        if timeshift is None:
            end = now
        elif isinstance(timeshift, timedelta):
            end = now - timeshift
        elif isinstance(timeshift, int) and not isinstance(timeshift, bool):
            if interval == "1M":
                end = _shift_calendar_months(now, timeshift)
            else:
                end = now - timedelta(seconds=seconds * timeshift)
        else:
            raise ValueError("timeshift must be None, an integer number of bars, or a timedelta")
        lookback_bars = min(length + 1, 5000)
        end_ms = int(end.timestamp() * 1000)
        if interval == "1M":
            try:
                start_ms = max(0, int(_shift_calendar_months(end, lookback_bars).timestamp() * 1000))
            except ValueError:
                start_ms = 0
        else:
            start_ms = max(0, end_ms - seconds * 1000 * lookback_bars)
        response = self.client.candles(asset.symbol, interval, start_ms, end_ms)
        frame = pd.DataFrame(response, columns=["timestamp", "open", "high", "low", "close", "volume"])
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
        frame = frame.set_index("timestamp").tail(length)
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column])
        return Bars(frame, self.SOURCE, asset, quote=quote, raw=response)

    def get_chains(self, asset, quote=None):
        raise NotImplementedError("MetaMask does not offer option chains")
