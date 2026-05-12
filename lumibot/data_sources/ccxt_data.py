from __future__ import annotations

# pyright: reportIncompatibleMethodOverride=false, reportMissingTypeStubs=false
import datetime
import time
from decimal import Decimal
from importlib import import_module
from typing import Any, TypeAlias, cast

from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

from .data_source import DataSource

logger = get_logger(__name__)

PandasDataFrame: TypeAlias = Any  # noqa: UP040
BarsEntity: TypeAlias = Any  # noqa: UP040
AssetInput: TypeAlias = Asset | str | tuple[Asset, Asset]  # noqa: UP040


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        self._module_name = module_name
        self._module: Any | None = None

    def _load(self) -> Any:
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
_bars_class_cache: type[Any] | None = None


def _bars_class() -> type[Any]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    return _bars_class_cache


def _asset_symbol(asset: Asset | str) -> str:
    if isinstance(asset, str):
        return asset.upper()
    return str(asset.symbol or "").upper()


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None

    def _load(self):
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
_BARS_CLASS = None


def _bars_class():
    global _BARS_CLASS
    if _BARS_CLASS is None:
        from lumibot.entities import Bars

        _BARS_CLASS = Bars
    return _BARS_CLASS


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
    def _format_datetime(dt: datetime.datetime) -> str:
        return str(pd.Timestamp(dt).isoformat())

    def __init__(self, config: dict[str, Any], max_workers: int = 20, chunk_size: int = 100, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.name = "ccxt"
        self.max_workers: int = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size: int = min(chunk_size, 100)

        import ccxt

        import ccxt

        try:
            exchange_id = str(config["exchange_id"])
            exchange_class = getattr(ccxt, exchange_id)
        except Exception:
            raise Exception(
                "Could not find exchange named '{}'. Are you sure you are spelling the exchange_id correctly?".format(
                    config["exchange_id"]
                )
            ) from None

        self.config: dict[str, Any] = config
        self.api: Any = exchange_class(config)
        is_sandbox = bool(config.get("sandbox", True))
        self.api.set_sandbox_mode(is_sandbox)
        # NOTE (unit-test + offline safety):
        # `load_markets()` performs a public network call (e.g., Kraken AssetPairs). Some environments
        # (CI sandboxes, offline dev machines, firewalled networks) block outbound traffic, which
        # would make broker initialization fail even when no market data is needed immediately.
        #
        # Keep initialization robust by logging-and-continuing on transient network failures. Any
        # subsequent call that actually needs markets can retry or raise as appropriate.
        try:
            self.api.load_markets()
        except Exception as exc:
            logger.warning(
                "[CCXT] load_markets() failed during init for exchange_id=%s sandbox=%s: %s",
                config.get("exchange_id"),
                is_sandbox,
                exc,
            )
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True

    def _pull_source_symbol_bars(
        self,
        asset: AssetInput,
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: datetime.timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> Any:
        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for CcxtData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, quote=quote)
        return response[asset]

    def _pull_source_bars(
        self,
        assets: list[AssetInput],
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: datetime.timedelta | None = None,
        quote: Asset | None = None,
        include_after_hours: bool = True,
    ) -> dict[AssetInput, Any]:
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs: dict[str, Any] = {"limit": length}
        if timeshift:
            end = datetime.datetime.now() - timeshift
            kwargs["end"] = self.to_default_timezone(end)

        result: dict[AssetInput, Any] = {}
        for asset in assets:
            if isinstance(asset, tuple):
                symbol = f"{_asset_symbol(asset[0])}/{_asset_symbol(asset[1])}"
            elif quote is not None:
                symbol = f"{_asset_symbol(asset)}/{_asset_symbol(quote)}"
            else:
                symbol = _asset_symbol(asset)
            data = self.get_barset_from_api(self.api, symbol, parsed_timestep, **kwargs)
            result[asset] = data

        return result

    def get_chains(
        self, asset: Asset, quote: Asset | None = None, exchange: str | None = None
    ) -> dict[str, Any]:
        raise NotImplementedError(
            "Lumibot CcxtData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self,
        asset: AssetInput,
        length: int,
        timestep: str = "",
        timeshift: datetime.timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
    ) -> Any:
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

    def get_barset_from_api(
        self,
        api: Any,
        symbol: str,
        freq: str,
        limit: int | None = None,
        end: datetime.datetime | None = None,
    ) -> PandasDataFrame | None:
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.
        """
        if not api.has["fetchOHLCV"]:
            logger.error("Exchange does not support fetching OHLCV data")

        market = self.api.markets.get(symbol, None)
        if market is None:
            logger.error(
                f"A request for market data for {symbol} was submitted. The market for that pair does not exist"
            )
            return None

        if limit is None:
            limit = 300

        if end is None:
            end = datetime.datetime.now(datetime.UTC)

        endunix = self.api.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))
        buffer = 10  # A few extra datapoints in the download then trim the df.
        if freq == "1m":
            start = end - datetime.timedelta(minutes=limit + buffer)
        else:
            start = end - datetime.timedelta(days=limit + buffer)
        df_ret: Any = None
        curr_start = self.api.parse8601(start.strftime("%Y-%m-%d %H:%M:%S"))
        cnt = 0
        last_curr_end = None
        # loop_limit = 300 if limit > 300 else limit
        loop_limit = 300
        rate_limit = 10  # Requests per second in burst.

        while True:
            cnt += 1
            candles = self.api.fetch_ohlcv(symbol, freq, since=curr_start, limit=loop_limit, params={})

            df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
            df = df.set_index("datetime")

            if df_ret is None:
                df_ret = df
            else:
                df_ret = pd.concat([df_ret, df])

            df_ret = df_ret.sort_index()

            if len(df) > 0:
                last_curr_end = self.api.parse8601(df.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            else:
                last_curr_end = None

            if len(df_ret) >= limit:
                break
            elif last_curr_end is None:
                break
            elif last_curr_end > endunix:
                break

            if curr_start == last_curr_end:
                break
            else:
                curr_start = last_curr_end

            # Sleep for half a second every rate_limit requests to prevent rate limiting issues
            if cnt % rate_limit == 0:
                time.sleep(1)

            # Catch if endless loop.
            if cnt > 500:
                break

        if df_ret is None:
            return None

        df_ret = df_ret[~df_ret.index.duplicated(keep="first")]
        df_ret = df_ret.loc[:end]
        df_ret = df_ret.iloc[-limit:]

        return df_ret

    def _parse_source_symbol_bars(
        self,
        response: PandasDataFrame,
        asset: AssetInput,
        quote: Asset | None = None,
        length: int | None = None,
    ) -> BarsEntity:
        # Parse the dataframe returned from CCXT.
        bars = _bars_class()(response, self.SOURCE, asset, quote=quote, raw=response)
        return bars

    def get_last_price(
        self,
        asset: Asset,
        quote: Asset | None = None,
        exchange: str | None = None,
        **kwargs: Any,
    ) -> float | Decimal | None:
        if quote is not None:
            symbol = f"{asset.symbol}/{quote.symbol}"
        else:
            symbol = asset.symbol

        ticker = self.api.fetch_ticker(symbol)
        price = ticker["last"]

        return cast(float | Decimal | None, price)
