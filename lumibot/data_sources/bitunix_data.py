from __future__ import annotations

from lumibot._lazy_imports import LazyModule, LazyPytzTimezoneRef, lazy_class
from lumibot.data_sources.data_source import DataSource

TYPE_CHECKING = False
pd = LazyModule("pandas")
pytz = LazyModule("pytz")
Asset = lazy_class("lumibot.entities", "Asset")

if TYPE_CHECKING:
    from lumibot.entities import Bars


def _get_bars_class():
    from lumibot.entities import Bars

    return Bars


def _get_bitunix_client_class():
    from lumibot.tools.bitunix_helpers import BitUnixClient

    return BitUnixClient


class BitunixData(DataSource):
    SOURCE = "BITUNIX"
    DEFAULT_TIMEZONE = "UTC"
    DEFAULT_PYTZ = LazyPytzTimezoneRef(DEFAULT_TIMEZONE)
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1", "1m", "minute"]},
        {"timestep": "3 minutes", "representations": ["3", "3m"]},
        {"timestep": "5 minutes", "representations": ["5", "5m"]},
        {"timestep": "15 minutes", "representations": ["15", "15m"]},
        {"timestep": "30 minutes", "representations": ["30", "30m"]},
        {"timestep": "hour", "representations": ["60", "60m", "1h", "hour"]},
        {"timestep": "2 hours", "representations": ["120", "120m", "2h"]},
        {"timestep": "4 hours", "representations": ["240", "240m", "4h"]},
        {"timestep": "day", "representations": ["D", "1d", "day"]},
    ]
    MAX_KLINE_LIMIT = 200
    _INTERVAL_MILLISECONDS = {
        "1m": 60_000,
        "3m": 3 * 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "30m": 30 * 60_000,
        "1h": 60 * 60_000,
        "2h": 2 * 60 * 60_000,
        "4h": 4 * 60 * 60_000,
        "1d": 24 * 60 * 60_000,
    }

    def __init__(self, config: dict, max_workers: int = 1, chunk_size: int = 100, tzinfo: Optional[pytz.timezone] = None):
        super().__init__(delay=0, tzinfo=tzinfo)
        self.name = "bitunix"
        self.chunk_size = chunk_size
        # Parse API keys
        if isinstance(config, dict):
            try:
                self.api_key = config["API_KEY"]
                self.api_secret = config["API_SECRET"]
            except KeyError:
                raise ValueError("API_KEY and API_SECRET must be provided in config")
        else:
            self.api_key = getattr(config, "API_KEY", None)
            self.api_secret = getattr(config, "API_SECRET", None)
            if not self.api_key or not self.api_secret:
                raise ValueError("API_KEY and API_SECRET must be provided in config")
        self.client = _get_bitunix_client_class()(self.api_key, self.api_secret)
        # Track symbols we're interested in for WebSocket subscriptions
        self.client_symbols = set()

    def _sanitize_base_and_quote_asset(self, base_asset, quote_asset) -> tuple[Asset, Asset]:
        """Ensure base and quote are Asset and set defaults for spot/futures."""
        if isinstance(base_asset, tuple):
            asset, quote = base_asset
        else:
            asset, quote = base_asset, quote_asset

        if not isinstance(asset, Asset):
            asset = Asset(symbol=str(asset), asset_type=Asset.AssetType.CRYPTO)
        if quote and not isinstance(quote, Asset):
            quote = Asset(symbol=str(quote), asset_type=Asset.AssetType.CRYPTO)

        if asset.asset_type == Asset.AssetType.FUTURE:
            # futures do not need explicit quote asset
            quote = None
        elif asset.asset_type == Asset.AssetType.CRYPTO and quote is None:
            # default spot quote
            quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
        return asset, quote

    def get_last_price(self, asset: Asset, quote: Asset = None, **kwargs) -> Optional[float]:
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if asset.asset_type == Asset.AssetType.FUTURE:
            symbol = asset.symbol
        else:
            symbol = f"{asset.symbol}USDT"

        # For futures, use mark price
        try:
            resp = self.client.get_funding_rate(symbol)
            if resp and resp.get("code") == 0:
                price_str = resp.get("data", {}).get("markPrice")
                return float(price_str) if price_str else None
        except Exception as e:
            print(e)
            return None

        return None

    def _parse_source_timestep(self, timestep: str) -> str:
        """Convert Lumibot timestep to BitUnix interval format."""
        normalized = self.get_timestep_from_string(timestep)

        if normalized == "minute":
            return "1m"
        elif normalized == "3 minutes":
            return "3m"
        elif normalized == "5 minutes":
            return "5m"
        elif normalized == "15 minutes":
            return "15m"
        elif normalized == "30 minutes":
            return "30m"
        elif normalized == "hour":
            return "1h"
        elif normalized == "2 hours":
            return "2h"
        elif normalized == "4 hours":
            return "4h"
        elif normalized == "day":
            return "1d"
        else:
            # Default to 1m if unknown
            return "1m"

    def supports_native_timestep(self, timestep: str) -> bool:
        """Return whether Bitunix can serve the requested interval directly."""
        normalized = str(timestep or "").lower().strip()
        return any(
            normalized == str(mapping["timestep"]).lower()
            or normalized in {str(value).lower() for value in mapping["representations"]}
            for mapping in self.TIMESTEP_MAPPING
        )

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "",
        timeshift=None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True
    ) -> Optional[Bars]:
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if not timestep:
            timestep = self.get_timestep()

        # Determine symbol format based on asset type
        if asset.asset_type in (Asset.AssetType.FUTURE, Asset.AssetType.CRYPTO_FUTURE):
            symbol = asset.symbol
        else:
            symbol = f"{asset.symbol}{quote.symbol}"

        # Add to tracked symbols
        self.client_symbols.add(symbol)

        # Convert Lumibot timestep to BitUnix interval format
        interval = self._parse_source_timestep(timestep)

        try:
            interval_ms = self._INTERVAL_MILLISECONDS[interval]
            end = pd.Timestamp(self.get_datetime())
            if timeshift is not None:
                if isinstance(timeshift, int):
                    end = end - pd.Timedelta(milliseconds=timeshift * interval_ms)
                else:
                    end = end - timeshift
            end_ms = int(end.timestamp() * 1000)

            # Bitunix caps each response at 200 candles. Query bounded forward
            # windows so response ordering cannot strand the request on one page.
            buffer = 2
            start_ms = end_ms - (length + buffer) * interval_ms
            cursor = start_ms
            bars_data = []
            while cursor < end_ms:
                page_end = min(end_ms + 1, cursor + (self.MAX_KLINE_LIMIT + 1) * interval_ms)
                resp = self.client.get_kline(
                    symbol=symbol,
                    interval=interval,
                    start_time=cursor,
                    end_time=page_end,
                    limit=self.MAX_KLINE_LIMIT,
                )
                if not resp or resp.get("code") != 0:
                    break

                page = resp.get("data", []) or []
                bars_data.extend(page)
                page_timestamps = []
                for candle in page:
                    raw_timestamp = candle.get("t", candle.get("time"))
                    try:
                        page_timestamps.append(int(raw_timestamp))
                    except (TypeError, ValueError):
                        continue

                if page_timestamps:
                    next_cursor = max(page_timestamps)
                    if next_cursor <= cursor:
                        next_cursor = page_end
                else:
                    next_cursor = page_end
                if next_cursor <= cursor:
                    break
                cursor = next_cursor

            if not bars_data:
                return None

            # Construct DataFrame from candle data
            df = pd.DataFrame(bars_data)

            # Expected format from documentation - adjust if needed
            if "t" in df.columns:  # Timestamp
                df["ts"] = df["t"]
            elif "time" in df.columns:  # Also handle 'time' column
                df["ts"] = df["time"]
            if "o" in df.columns:  # Open
                df["open"] = df["o"]
            if "h" in df.columns:  # High
                df["high"] = df["h"]
            if "l" in df.columns:  # Low
                df["low"] = df["l"]
            if "c" in df.columns:  # Close
                df["close"] = df["c"]
            if "baseVol" in df.columns:  # Volume
                df["volume"] = df["baseVol"]

            # Ensure numeric columns
            for col in ("open", "high", "low", "close", "volume"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Set timestamp as index
            if "ts" in df.columns:
                df.index = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True)
                df.index = df.index.tz_convert(self.tzinfo)
                df = df[~df.index.duplicated(keep="last")]

            # Select only required columns
            required_cols = ["open", "high", "low", "close", "volume"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = 0.0

            # Limit to the requested length and fail loudly if the exchange
            # cannot supply it, rather than silently starving the strategy.
            df = df.sort_index()
            if len(df) < length:
                raise ValueError(
                    f"Bitunix returned only {len(df)} of {length} requested {interval} bars for {symbol}"
                )
            if len(df) > length:
                df = df.tail(length)

            # Wrap in Bars object
            return self._parse_source_symbol_bars(
                df[required_cols],
                asset,
                quote=None if asset.asset_type in (Asset.AssetType.FUTURE, Asset.AssetType.CRYPTO_FUTURE) else quote,
                length=length
            )

        except ValueError:
            raise
        except Exception:
            import traceback
            traceback.print_exc()
            return None


    def _parse_source_symbol_bars(self, df: pd.DataFrame, asset: Asset, quote: Asset = None, length: int = None) -> Bars:
        """
        Wraps the raw DataFrame into a Bars entity with source metadata.
        """
        return _get_bars_class()(df, self.SOURCE, asset, raw=df, quote=quote)

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 100) -> dict:
        """Option chains not supported by BitUnix."""
        return {"Multiplier": 1, "Exchange": exchange or "", "Chains": {}}

    def get_timestep_from_string(self, timestep: str) -> str:
        """
        Maps a string representation of a timestep to the normalized timestep.
        """
        ts = timestep.lower().strip()
        for mapping in self.TIMESTEP_MAPPING:
            if ts in [r.lower() for r in mapping["representations"]]:
                return mapping["timestep"]
        # Default to "minute" if not found
        return "minute"
