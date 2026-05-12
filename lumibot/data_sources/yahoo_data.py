from __future__ import annotations

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportMissingTypeArgument=false
# pyright: reportInvalidTypeForm=false, reportUnnecessaryComparison=false, reportArgumentType=false
# pyright: reportOptionalMemberAccess=false, reportOptionalSubscript=false, reportAttributeAccessIssue=false
# pyright: reportPrivateUsage=false, reportUnknownLambdaType=false, reportConstantRedefinition=false
# pyright: reportIncompatibleMethodOverride=false, reportUnnecessaryIsInstance=false
from datetime import datetime, timedelta
from decimal import Decimal
from importlib import import_module
from types import ModuleType
from typing import Any, TypeAlias, cast

from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)
PandasDataFrame: TypeAlias = Any  # noqa: UP040
BarsEntity: TypeAlias = Any  # noqa: UP040
_BARS_CLASS = None
_YAHOO_HELPER = None


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self) -> ModuleType:
        module = cast(ModuleType | None, object.__getattribute__(self, "_module"))
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


numpy = _LazyModule("numpy")
pd = _LazyModule("pandas")


def _bars_class() -> Any:
    global _BARS_CLASS
    if _BARS_CLASS is None:
        from lumibot.entities import Bars

        _BARS_CLASS = Bars
    return _BARS_CLASS


def _yahoo_helper() -> Any:
    global _YAHOO_HELPER
    if _YAHOO_HELPER is None:
        from lumibot.tools import YahooHelper

        _YAHOO_HELPER = YahooHelper
    return _YAHOO_HELPER


class YahooData(DataSourceBacktesting):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "15 minutes", "representations": ["15m", "15 minutes"]},
        {"timestep": "minute", "representations": ["1m", "1 minute"]},
    ]

    def __init__(
        self,
        auto_adjust: bool = False,
        datetime_start: datetime | None = None,
        datetime_end: datetime | None = None,
        **kwargs: Any,
    ) -> None:
        # Log received parameters BEFORE applying defaults
        logger.info(f"YahooData.__init__ received: datetime_start={datetime_start}, datetime_end={datetime_end}")

        # Set default date range if not provided
        if datetime_start is None:
            logger.info("YahooData.__init__: datetime_start is None, using default.")
            datetime_start = datetime.now() - timedelta(days=365)
        if datetime_end is None:
            logger.info("YahooData.__init__: datetime_end is None, using default.")
            datetime_end = datetime.now()

        # Log the dates being passed to super().__init__
        logger.info(
            f"YahooData.__init__ calling super().__init__ with: datetime_start={datetime_start}, datetime_end={datetime_end}"
        )

        # Pass datetime_start and datetime_end as keyword arguments only, not as positional args
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
        self.name = "yahoo"
        self.auto_adjust = auto_adjust
        self._data_store: dict[Any, PandasDataFrame] = {}
        self._data_index_values: dict[Any, Any] = {}
        self._data_open_values: dict[Any, Any] = {}
        # Initialize last-price cache here to avoid per-call hasattr checks
        self._last_price_cache: dict[tuple[Any, ...], float | Decimal | None] = {}
        self._last_price_cache_datetime: datetime | None = None
        self._daily_last_price_cache: dict[tuple[Any, ...], float | Decimal | None] = {}
        self._daily_last_price_cache_date: Any | None = None

    def _append_data(self, asset: Asset, data: PandasDataFrame) -> PandasDataFrame:
        """

        Parameters
        ----------
        asset : Asset
        data

        Returns
        -------

        """
        if "Adj Close" in data:
            del data["Adj Close"]
        data = data.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividend",
                "Stock Splits": "stock_splits",
            },
        )

        data["price_change"] = data["close"].pct_change()
        data["dividend_yield"] = data["dividend"] / data["close"]
        data["return"] = data["dividend_yield"] + data["price_change"]
        self._data_store[asset] = data
        self._data_index_values[asset] = data.index.values
        self._data_open_values[asset] = data["open"].to_numpy(copy=False)
        return data

    def _format_futures_symbol(self, symbol: str) -> list[str]:
        """
        Format the futures symbol for Yahoo Finance.

        Yahoo Finance futures symbols can be in different formats:
        - Continuous contracts: Root symbol + "=F" (e.g., "ES=F", "CL=F", "GC=F")
        - Specific expiry contracts: Symbol + month code + year + exchange
          (e.g., "ESH25.CME" for the March 2025 E-mini S&P 500 contract)

        Parameters
        ----------
        symbol : str
            The futures symbol

        Returns
        -------
        list
            A list of properly formatted futures symbols to try in order of preference
        """
        # Strip any $ prefix if present (sometimes used in futures symbols)
        if symbol.startswith("$"):
            symbol = symbol[1:]

        formatted_symbols = []

        # If already contains a dot (like ESZ23.CME), it's likely already properly formatted
        if "." in symbol:
            # Already has exchange suffix, keep as is
            formatted_symbols.append(symbol)

            # Also try continuous contract (extract root symbol and add =F)
            parts = symbol.split(".")
            base_symbol = parts[0]
            # Extract root symbol (usually first 2 characters for most futures)
            root_symbol = "".join([c for c in base_symbol if c.isalpha()])[:2]
            formatted_symbols.append(f"{root_symbol}=F")

        # If it has =F already, it's formatted for continuous contract
        elif "=F" in symbol:
            formatted_symbols.append(symbol)

        # No special formatting, try to determine if it's a specific contract or continuous
        else:
            # Check if it looks like a specific contract (e.g., ESH25)
            # Extract letters (should be root symbol + month code) and numbers (year)
            letters = "".join([c for c in symbol if c.isalpha()])
            numbers = "".join([c for c in symbol if c.isdigit()])

            # If it follows pattern of root + month code + year digits
            if len(letters) >= 3 and len(numbers) >= 1:
                # This looks like a specific contract, try both with and without exchange
                root_symbol = letters[:2]  # First two letters usually the root

                # Add with common exchanges
                for exchange in ["CME", "NYMEX", "CBOT", "COMEX", "NYBOT"]:
                    formatted_symbols.append(f"{symbol}.{exchange}")

                # Also try as continuous
                formatted_symbols.append(f"{root_symbol}=F")

                # Add original as fallback
                formatted_symbols.append(symbol)
            else:
                # Looks like a root symbol, try as continuous
                formatted_symbols.append(f"{symbol}=F")
                formatted_symbols.append(symbol)

        # Log the potential symbols we'll try
        logger.info(f"Trying futures symbols for Yahoo Finance: {formatted_symbols}")

        return formatted_symbols

    def _format_index_symbol(self, symbol: str) -> list[str]:
        """
        Format the index symbol for Yahoo Finance.

        Yahoo Finance index symbols typically use the "^" prefix:
        - SPX -> ^SPX (S&P 500 Index)
        - DJI -> ^DJI (Dow Jones Industrial Average)
        - IXIC -> ^IXIC (NASDAQ Composite)
        - RUT -> ^RUT (Russell 2000)

        Parameters
        ----------
        symbol : str
            The index symbol

        Returns
        -------
        list
            A list of properly formatted index symbols to try in order of preference
        """
        formatted_symbols = []

        # If already has ^ prefix, keep as is
        if symbol.startswith("^"):
            formatted_symbols.append(symbol)
            # Also try without prefix as fallback
            formatted_symbols.append(symbol[1:])
        else:
            # Try with ^ prefix first
            formatted_symbols.append(f"^{symbol}")
            # Also try original symbol as fallback
            formatted_symbols.append(symbol)

        # Log the potential symbols we'll try
        logger.info(f"Trying index symbols for Yahoo Finance: {formatted_symbols}")

        return formatted_symbols

    def _pull_source_symbol_bars(
        self,
        asset: Asset | str,
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | int | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> PandasDataFrame | None:
        logger.info(
            "Inside _pull_source_symbol_bars for %s: self._datetime = %s, requesting length %s",
            asset.symbol,
            self._datetime,
            length,
        )

        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for YahooData, but {exchange} was passed as the exchange"
            )

        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        data = self._get_source_symbol_data(asset, timestep)
        if data is None:
            return None

        end_idx = self._get_filtered_end_index(data, timestep, timeshift=timeshift, asset=asset)
        start_idx = max(0, end_idx - length)
        result = data.iloc[start_idx:end_idx].copy()

        if len(result) < length:
            logger.warning(
                f"Insufficient historical data for {asset.symbol} "
                f"to satisfy length {length}. Available: {len(result)}. "
                f"Check backtest start date and data availability."
            )

        return result

    def _get_source_symbol_data(self, asset: Asset | str, timestep: str) -> PandasDataFrame | None:
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        interval = self._parse_source_timestep(timestep, reverse=True)
        symbol = asset.symbol
        if symbol is None:
            logger.error("YahooData cannot fetch data for an asset without a symbol: %s", asset)
            return None
        symbols_to_try = [symbol]

        if asset.asset_type == "futures" or getattr(asset, "asset_type", None) == Asset.AssetType.FUTURE:
            symbols_to_try = self._format_futures_symbol(symbol)
        elif asset.asset_type == "index" or getattr(asset, "asset_type", None) == Asset.AssetType.INDEX:
            symbols_to_try = self._format_index_symbol(symbol)

        if asset in self._data_store:
            return self._data_store[asset]

        data = None
        successful_symbol = None
        for sym in symbols_to_try:
            logger.info("Attempting to fetch data for symbol: %s", sym)
            try:
                data = _yahoo_helper().get_symbol_data(
                    sym,
                    interval=interval,
                    auto_adjust=self.auto_adjust,
                    last_needed_datetime=self.datetime_end,
                )
                if data is not None and data.shape[0] > 0:
                    logger.info("Successfully fetched data for symbol: %s", sym)
                    successful_symbol = sym
                    break
            except Exception as e:
                logger.warning("_pull_source_symbol_bars: Error fetching data for symbol %s: %s", sym, str(e))
                import traceback

                traceback.print_exc()

        if data is None or data.shape[0] == 0:
            message = (
                f"{self.SOURCE} did not return data for symbol {asset.symbol}. Tried: {symbols_to_try}. "
                f"Make sure this symbol is valid and data exists for the period {self.datetime_start} "
                f"to {self.datetime_end}."
            )
            logger.error(message)
            return None

        data = self._append_data(asset, data)
        if successful_symbol and successful_symbol != asset.symbol:
            logger.info("Updating asset symbol from %s to successful format: %s", asset.symbol, successful_symbol)
        return data

    def _get_filtered_end_index(
        self, data: PandasDataFrame, timestep: str, timeshift: timedelta | int | None = None, asset: Any = None
    ) -> int:
        current_dt = self.to_default_timezone(self._datetime)

        if timestep == "day":
            dt = current_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            end_filter = dt - timedelta(days=1)
        else:
            end_filter = current_dt

        if timeshift:
            if isinstance(timeshift, int):
                timeshift = timedelta(days=timeshift)
            end_filter = end_filter - timeshift

        index_array = self._data_index_values.get(asset) if asset is not None else None
        if index_array is None:
            index_array = data.index.values

        if hasattr(end_filter, "to_numpy"):
            end_filter_np = end_filter.to_numpy()
        elif hasattr(end_filter, "asm8"):
            end_filter_np = end_filter.asm8
        else:
            end_filter_np = pd.Timestamp(end_filter).asm8

        return index_array.searchsorted(end_filter_np, side="left")

    def _pull_source_bars(
        self,
        assets: list[Asset],
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | int | None = None,
        quote: Asset | None = None,
        include_after_hours: bool = False,
    ) -> dict[Asset, PandasDataFrame | None]:
        """pull broker bars for a list assets"""

        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        interval = self._parse_source_timestep(timestep, reverse=True)
        missing_symbols: list[str] = []
        missing_symbol_map: dict[Asset, list[str]] = {}

        # Check for futures and index symbols and properly format them
        for asset in assets:
            if asset not in self._data_store:
                if asset.symbol is None:
                    logger.warning("Skipping Yahoo data fetch for asset without symbol: %s", asset)
                    continue
                if asset.asset_type == Asset.AssetType.FUTURE:
                    symbols = self._format_futures_symbol(asset.symbol)
                elif asset.asset_type == Asset.AssetType.INDEX:
                    symbols = self._format_index_symbol(asset.symbol)
                else:
                    symbols = [asset.symbol]
                missing_symbol_map[asset] = symbols
                missing_symbols.extend(symbols)

        if missing_symbols:
            # Fetch data using the helper without restricting dates here
            dfs = _yahoo_helper().get_symbols_data(missing_symbols, interval=interval, auto_adjust=self.auto_adjust)
            for symbol, df in dfs.items():
                # Find the corresponding asset for this symbol
                for asset, asset_symbols in missing_symbol_map.items():
                    if symbol in asset_symbols:
                        self._append_data(asset, df)
                        break

        result: dict[Asset, PandasDataFrame | None] = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(asset, length, timestep=timestep, timeshift=timeshift)
        return result

    def _parse_source_symbol_bars(
        self, response: PandasDataFrame, asset: Asset, quote: Asset | None = None, length: int | None = None
    ) -> BarsEntity:
        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        bars = _bars_class()(response, self.SOURCE, asset, raw=response)
        return bars

    def get_last_price(
        self,
        asset: Asset | str,
        timestep: str | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        **kwargs: Any,
    ) -> float | Decimal | None:
        """Takes an asset and returns the last known price"""
        if timestep is None:
            timestep = self.get_timestep()

        # OPTIMIZATION: Cache last price lookups to avoid redundant get_historical_prices calls
        current_datetime = self._datetime
        if isinstance(timestep, str) and "day" in timestep.lower():
            current_date = current_datetime.date() if current_datetime is not None else None
            cache_key = (asset, timestep, quote, exchange, current_date)
            if self._daily_last_price_cache_date != current_date:
                self._daily_last_price_cache.clear()
                self._daily_last_price_cache_date = current_date
            if cache_key in self._daily_last_price_cache:
                return self._daily_last_price_cache[cache_key]
            price = self._get_last_daily_open_price(asset, timestep=timestep, quote=quote, exchange=exchange)
            self._daily_last_price_cache[cache_key] = price
            return price

        cache_key = (asset, timestep, quote, exchange, current_datetime)

        # Clear cache if datetime changed
        if self._last_price_cache_datetime != current_datetime:
            self._last_price_cache.clear()
            self._last_price_cache_datetime = current_datetime

        # Check cache first
        if cache_key in self._last_price_cache:
            return self._last_price_cache[cache_key]

        # Daily bars are stamped at the session close. Leaving the timeshift unset for daily
        # requests ensures we only reference the most recent fully closed bar (no lookahead).
        # Intraday paths still step back one interval to avoid peeking ahead.
        if isinstance(timestep, str) and "day" in timestep.lower():
            timeshift_delta = None
        else:
            timeshift_delta = timedelta(days=-1)

        bars = self.get_historical_prices(asset, 1, timestep=timestep, quote=quote, timeshift=timeshift_delta)

        if isinstance(bars, float):
            return bars
        elif bars is None:
            return None

        df_local = bars.df
        if hasattr(df_local, "iloc"):
            open_ = df_local["open"].iat[0]
        else:
            open_ = df_local["open"][0]
        if isinstance(open_, numpy.int64):
            open_ = Decimal(open_.item())
        self._last_price_cache[cache_key] = open_
        return open_

    def _get_last_daily_open_price(
        self, asset: Asset | str, timestep: str, quote: Asset | None = None, exchange: str | None = None
    ) -> float | Decimal | None:
        if exchange is not None:
            logger.warning(
                "the exchange parameter is not implemented for YahooData, but %s was passed as the exchange",
                exchange,
            )

        if quote is not None:
            logger.warning("quote is not implemented for YahooData, but %s was passed as the quote", quote)

        data = self._get_source_symbol_data(asset, timestep)
        if data is None or data.empty:
            return None

        end_idx = self._get_filtered_end_index(data, timestep, timeshift=None, asset=asset)
        if end_idx <= 0:
            return None

        open_values = self._data_open_values.get(asset)
        if open_values is None:
            open_ = data["open"].iat[end_idx - 1]
        else:
            open_ = open_values[end_idx - 1]
        if isinstance(open_, numpy.int64):
            open_ = Decimal(open_.item())
        return open_

    def get_chains(
        self, asset: Asset, quote: Asset | None = None, exchange: str | None = None
    ) -> dict[str, Any]:
        """
        Get the chains for a given asset.  This is not implemented for YahooData becuase Yahoo does not support
        historical options data.

        yfinance module does support getting some of the info for current options chains, but it is not implemented.
        See yf methods:
        >>>    import yfinance as yf
        >>>    spy = yf.Ticker("SPY")
        >>>    expirations = spy.options
        >>>    chain_data = spy.option_chain()
        """
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_strikes(self, asset: Asset) -> list[float]:
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self,
        asset: Asset | str,
        length: int,
        timestep: str = "",
        timeshift: timedelta | int | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> BarsEntity | float | None:
        """Get bars for a given asset"""
        if isinstance(asset, str):
            # Create Asset with futures type if it appears to be a futures symbol
            if "." in asset and any(exchange in asset for exchange in ["CME", "NYMEX", "CBOT", "NYBOT", "COMEX"]):
                asset = Asset(symbol=asset, asset_type="futures")
            else:
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
