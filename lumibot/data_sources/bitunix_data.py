from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, TypeAlias, cast

import pytz

from lumibot.data_sources.data_source import ChainMap, DataSource
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

if TYPE_CHECKING:
    from lumibot.entities.bars import Bars

PandasDataFrame: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.
AssetInput: TypeAlias = Asset | str | tuple[Asset | str, Asset | str | None]  # noqa: UP040
QuoteInput: TypeAlias = Asset | str | None  # noqa: UP040

logger = get_logger(__name__)


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        self._module_name = module_name
        self._module = None

    def _load(self) -> ModuleType:
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
_MISSING = object()


class _BitunixClientProtocol(Protocol):
    def get_funding_rate(self, symbol: str) -> dict[str, Any]: ...

    def get_kline(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
        kline_type: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]: ...


class _BitunixClientFactory(Protocol):
    def __call__(self, api_key: str, secret_key: str) -> _BitunixClientProtocol: ...


BitUnixClient: _BitunixClientFactory | None = None
_bars_class_cache: type[Bars] | None = None


def _bitunix_client_class() -> _BitunixClientFactory:
    global BitUnixClient
    if BitUnixClient is None:
        from lumibot.tools.bitunix_helpers import BitUnixClient as _BitUnixClient

        BitUnixClient = cast(_BitunixClientFactory, _BitUnixClient)
    return BitUnixClient


def _bars_class() -> type[Bars]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    return _bars_class_cache


def _read_config_value(config: object, key: str) -> str:
    if isinstance(config, Mapping):
        config_map = cast(Mapping[str, object], config)
        value = config_map.get(key, _MISSING)
        if value is _MISSING:
            raise ValueError("API_KEY and API_SECRET must be provided in config")
    else:
        value = getattr(config, key, None)
        if not value:
            raise ValueError("API_KEY and API_SECRET must be provided in config")
    return str(value)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return None


class BitunixData(DataSource):
    SOURCE: str = "BITUNIX"
    MIN_TIMESTEP: str = "minute"
    TIMESTEP_MAPPING: list[dict[str, Any]] = [
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
    _FUTURE_ASSET_TYPES: ClassVar[frozenset[Asset.AssetType]] = frozenset(
        {Asset.AssetType.FUTURE, Asset.AssetType.CRYPTO_FUTURE}
    )

    def __init__(self, config: object, max_workers: int = 1, chunk_size: int = 100, tzinfo: Any | None = None) -> None:
        super().__init__(delay=0, tzinfo=tzinfo, max_workers=max_workers)
        if self.tzinfo is None:
            self.tzinfo = pytz.utc

        self.name = "bitunix"
        self.chunk_size = chunk_size
        self.api_key = _read_config_value(config, "API_KEY")
        self.api_secret = _read_config_value(config, "API_SECRET")
        self.client: _BitunixClientProtocol = _bitunix_client_class()(
            api_key=self.api_key,
            secret_key=self.api_secret,
        )
        self.client_symbols: set[str] = set()

    @classmethod
    def _is_futures_asset(cls, asset: Asset) -> bool:
        return asset.asset_type in cls._FUTURE_ASSET_TYPES

    @classmethod
    def _asset_symbol(cls, asset: Asset) -> str:
        symbol = asset.symbol
        if not symbol:
            raise ValueError("BitUnix assets must have a symbol")
        return symbol

    @classmethod
    def _symbol_for_asset(cls, asset: Asset, quote: Asset | None) -> str:
        if cls._is_futures_asset(asset):
            return cls._asset_symbol(asset)
        if quote is None:
            raise ValueError(f"BitUnix spot asset {asset} requires a quote asset")
        return f"{cls._asset_symbol(asset)}{cls._asset_symbol(quote)}"

    def _sanitize_base_and_quote_asset(self, base_asset: AssetInput, quote_asset: QuoteInput) -> tuple[Asset, Asset | None]:
        """Ensure base and quote are Asset and set defaults for spot/futures."""
        if isinstance(base_asset, tuple):
            asset, quote = base_asset
        else:
            asset, quote = base_asset, quote_asset

        if not isinstance(asset, Asset):
            asset = Asset(symbol=str(asset), asset_type=Asset.AssetType.CRYPTO)
        if quote is not None and not isinstance(quote, Asset):
            quote = Asset(symbol=str(quote), asset_type=Asset.AssetType.CRYPTO)

        if self._is_futures_asset(asset):
            quote = None
        elif asset.asset_type == Asset.AssetType.CRYPTO and quote is None:
            quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
        return asset, quote

    def get_last_price(self, asset: Asset, quote: Any = None, exchange: str | None = None) -> float | None:
        if quote is None:
            quote = Asset("USDT", Asset.AssetType.CRYPTO)
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        symbol = self._symbol_for_asset(asset, quote)

        try:
            resp = self.client.get_funding_rate(symbol)
            if resp and resp.get("code") == 0:
                data = resp.get("data", {})
                if not isinstance(data, Mapping):
                    return None
                data_map = cast(Mapping[str, object], data)
                return _float_or_none(data_map.get("markPrice"))
        except Exception:
            logger.debug("Failed to get BitUnix last price for %s", symbol, exc_info=True)
            return None

        return None

    def _parse_source_timestep(self, timestep: str, reverse: bool = False) -> str:
        """Convert Lumibot timestep to BitUnix interval format."""
        normalized = self.get_timestep_from_string(timestep)
        if reverse:
            return normalized

        if normalized == "minute":
            return "1m"
        if normalized == "3 minutes":
            return "3m"
        if normalized == "5 minutes":
            return "5m"
        if normalized == "15 minutes":
            return "15m"
        if normalized == "30 minutes":
            return "30m"
        if normalized == "hour":
            return "1h"
        if normalized == "2 hours":
            return "2h"
        if normalized == "4 hours":
            return "4h"
        if normalized == "day":
            return "1d"
        return "1m"

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "",
        timeshift: timedelta | None = None,
        quote: Any = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        **kwargs: Any,
    ) -> Bars | None:
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if not timestep:
            timestep = self.get_timestep()

        symbol = self._symbol_for_asset(asset, quote)
        self.client_symbols.add(symbol)
        interval = self._parse_source_timestep(timestep)

        try:
            limit = min(1000, length * 2)
            resp = self.client.get_kline(symbol=symbol, interval=interval, limit=limit)
            if resp and resp.get("code") == 0:
                bars_data = resp.get("data", [])
                if not bars_data:
                    return None

                df: PandasDataFrame = pd.DataFrame(bars_data)
                if "t" in df.columns:
                    df["ts"] = df["t"]
                elif "time" in df.columns:
                    df["ts"] = df["time"]
                if "o" in df.columns:
                    df["open"] = df["o"]
                if "h" in df.columns:
                    df["high"] = df["h"]
                if "l" in df.columns:
                    df["low"] = df["l"]
                if "c" in df.columns:
                    df["close"] = df["c"]
                if "baseVol" in df.columns:
                    df["volume"] = df["baseVol"]

                for col in ("open", "high", "low", "close", "volume"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                if "ts" in df.columns:
                    df.index = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms")
                    df.index = df.index.tz_localize(pytz.utc).tz_convert(self.tzinfo)

                required_cols = ["open", "high", "low", "close", "volume"]
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = 0.0

                df = df.sort_index()
                if len(df) > length:
                    df = df.tail(length)

                return self._parse_source_symbol_bars(
                    df[required_cols],
                    asset,
                    quote=None if self._is_futures_asset(asset) else quote,
                )
        except Exception:
            logger.exception("Failed to fetch BitUnix historical prices for %s", symbol)
            return None

        return None

    def _parse_source_symbol_bars(
        self,
        response: PandasDataFrame,
        asset: Asset,
        quote: Any = None,
    ) -> Bars:
        """
        Wraps the raw DataFrame into a Bars entity with source metadata.
        """
        quote_asset = cast(Asset | None, quote)
        return _bars_class()(response, self.SOURCE, asset, raw=response, quote=quote_asset)

    def get_chains(
        self,
        asset: Any,
        quote: Any = None,
        exchange: str | None = None,
        strike_count: int = 100,
    ) -> ChainMap:
        """Option chains not supported by BitUnix."""
        return {"Multiplier": 1, "Exchange": exchange or "", "Chains": {}}

    def get_timestep_from_string(self, timestep: str) -> str:
        """
        Maps a string representation of a timestep to the normalized timestep.
        """
        ts = timestep.lower().strip()
        for mapping in self.TIMESTEP_MAPPING:
            representations = cast(list[str], mapping["representations"])
            if ts in [representation.lower() for representation in representations]:
                return cast(str, mapping["timestep"])
        return "minute"
