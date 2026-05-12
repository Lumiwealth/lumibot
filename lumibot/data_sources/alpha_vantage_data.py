from __future__ import annotations

import os.path
import time
from collections.abc import Iterable, Mapping
from datetime import timedelta
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

from .data_source import ChainMap, DataSource

if TYPE_CHECKING:
    from lumibot.entities.bars import Bars

PandasDataFrame: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.
BarsResultMap: TypeAlias = dict[Asset, PandasDataFrame | None]  # noqa: UP040

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
_bars_class_cache: type[Bars] | None = None


def _bars_class() -> type[Bars]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    return _bars_class_cache


def _api_key_from_config(config: object | None) -> str | None:
    if config is None:
        return None
    if isinstance(config, str):
        return config
    if isinstance(config, Mapping):
        config_map = cast(Mapping[str, object], config)
        value = config_map.get("API_KEY")
    else:
        value = getattr(config, "API_KEY", None)
    return str(value) if value else None


def _close_value(data: PandasDataFrame) -> float | None:
    if data is None or len(data) == 0 or "close" not in data.columns:
        return None
    value = data.iloc[-1]["close"]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class AlphaVantageData(DataSource):
    SOURCE = "ALPHA_VANTAGE"
    MIN_TIMESTEP = "minute"
    DATA_STALE_AFTER = timedelta(days=1)

    def __init__(self, config: object | None = None, auto_adjust: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.name = "alpha vantage"
        self.auto_adjust = auto_adjust
        self._data_store: dict[Asset, PandasDataFrame] = {}
        self.config = config
        self.api_key = _api_key_from_config(config)

    def _require_api_key(self) -> str:
        if not self.api_key:
            raise ValueError("AlphaVantageData requires a config with API_KEY to download uncached data")
        return self.api_key

    def _append_data(self, asset: Asset, data: PandasDataFrame) -> PandasDataFrame:
        result: PandasDataFrame = data.rename(
            columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            }
        )
        self._data_store[asset] = result
        return result

    def get_chains(self, asset: Any, quote: Any = None, exchange: str | None = None) -> ChainMap:
        """AlphaVantage does not support options chains."""
        raise NotImplementedError(
            "Lumibot AlphaVantage does not support get_chains options data. If you need this "
            "feature, please use a different data source."
        )

    def _download_symbol_bars(self, symbol: str, timestep: str, filename: str) -> PandasDataFrame | None:
        api_key = self._require_api_key()

        if timestep == "minute":
            interval = "1min"
            frames: list[PandasDataFrame] = []
            logger.info("Downloading minute data for %s, this can take 6 minutes or more per symbol", symbol)
            for year_number in range(1, 3):
                for month_number in range(1, 13):
                    time_slice = f"year{year_number}month{month_number}"
                    url = (
                        "https://www.alphavantage.co/query?"
                        "function=TIME_SERIES_INTRADAY_EXTENDED"
                        f"&symbol={symbol}"
                        f"&interval={interval}"
                        f"&slice={time_slice}"
                        f"&apikey={api_key}"
                    )
                    frames.append(pd.read_csv(url))
                    time.sleep(13)
            data: PandasDataFrame = pd.concat(frames)
            data.to_csv(filename)
            return data

        if timestep == "day":
            url = (
                "https://www.alphavantage.co/query?"
                "function=TIME_SERIES_DAILY_ADJUSTED"
                f"&symbol={symbol}"
                "&outputsize=full"
                "&datatype=csv"
                f"&apikey={api_key}"
            )
            data = pd.read_csv(url)
            data = data.set_index("timestamp")
            data.to_csv(filename)
            return data

        logger.warning("Unsupported AlphaVantage timestep %s for %s", timestep, symbol)
        return None

    def _read_cached_symbol_bars(self, filename: str) -> PandasDataFrame | None:
        if not os.path.exists(filename):
            return None

        modified_time = os.path.getmtime(filename)
        if self.get_datetime() - self.DATA_STALE_AFTER >= self.to_default_timezone(pd.Timestamp(modified_time, unit="s")):
            return None

        data: PandasDataFrame = pd.read_csv(filename)
        if "timestamp" in data.columns:
            return data.set_index("timestamp")
        if "time" in data.columns:
            return data.set_index("time")
        return data

    def _normalize_index(self, data: PandasDataFrame) -> PandasDataFrame:
        index = pd.to_datetime(data.index)
        if getattr(index, "tz", None) is None:
            index = index.tz_localize(tz=LUMIBOT_DEFAULT_PYTZ)
        else:
            index = index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
        data.index = index.astype("O")
        return data

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | None = None,
        quote: Any = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> PandasDataFrame | None:
        if exchange is not None:
            logger.warning("the exchange parameter is not implemented for AlphaVantageData, but %s was passed", exchange)

        symbol = asset.symbol
        if not symbol:
            raise ValueError("AlphaVantage assets must have a symbol")

        filename = f"{symbol}_{timestep}.csv"
        data = self._data_store.get(asset)
        if data is None:
            data = self._read_cached_symbol_bars(filename)
        if data is None:
            data = self._download_symbol_bars(symbol, timestep, filename)
        if data is None:
            return None

        data = self._normalize_index(data)
        self._data_store[asset] = data

        end = self._datetime if self._datetime is not None else self.get_datetime()
        if timeshift is not None:
            end = self.get_datetime() - timeshift
        filtered = data[data.index <= end]
        return filtered.tail(length)

    def _pull_source_bars(
        self,
        assets: Iterable[Asset],
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | None = None,
        quote: Any = None,
    ) -> BarsResultMap:
        """Pull broker bars for a list of assets."""
        return {
            asset: self._pull_source_symbol_bars(asset, length, timestep, timeshift, quote=quote)
            for asset in assets
        }

    def _parse_source_symbol_bars(
        self,
        response: PandasDataFrame,
        asset: Asset,
        quote: Any = None,
    ) -> Bars:
        quote_asset = cast(Asset | None, quote)
        return _bars_class()(response, self.SOURCE, asset, raw=response, quote=quote_asset)

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
        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep or self.MIN_TIMESTEP,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if response is None:
            return None
        return self._parse_source_symbol_bars(response, asset, quote=quote)

    def get_last_price(self, asset: Asset, quote: Any = None, exchange: str | None = None) -> float | None:
        data = self._pull_source_symbol_bars(asset, 1, timestep=self.MIN_TIMESTEP, quote=quote, exchange=exchange)
        if data is None:
            return None
        return _close_value(data)
