from __future__ import annotations

import os
import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from logging import Logger
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities.asset import Asset, AssetsMapping
from lumibot.entities.quote import Quote
from lumibot.tools import black_scholes

from .exceptions import UnavailabeTimestep

if TYPE_CHECKING:
    from lumibot.entities.bars import Bars

PandasDataFrame: TypeAlias = Any  # noqa: UP040
AssetInput: TypeAlias = Asset | str | tuple[Asset, Asset]  # noqa: UP040
BarsResultMap: TypeAlias = dict[Any, Any]  # noqa: UP040
ChainMap: TypeAlias = dict[str, Any]  # noqa: UP040
GreeksMap: TypeAlias = dict[str, Any]  # noqa: UP040


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
_create_options_symbol_func: Any | None = None


class _LazyLogger:
    _logger: Logger | None

    __slots__ = ("_logger",)

    def __init__(self) -> None:
        self._logger = None

    def _load(self) -> Logger:
        if self._logger is None:
            from lumibot.tools.lumibot_logger import get_logger

            self._logger = get_logger(__name__)
        return self._logger

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


logger = _LazyLogger()


def _create_options_symbol(*args: Any, **kwargs: Any) -> str:
    global _create_options_symbol_func
    create_symbol = _create_options_symbol_func
    if create_symbol is None:
        from lumibot.tools import create_options_symbol

        create_symbol = create_options_symbol
        _create_options_symbol_func = create_symbol
    return cast(str, create_symbol(*args, **kwargs))


class DataSource(ABC):
    SOURCE: str = ""
    IS_BACKTESTING_DATA_SOURCE: bool = False
    MIN_TIMESTEP: str = "minute"
    TIMESTEP_MAPPING: list[dict[str, Any]] = []
    DEFAULT_TIMEZONE: str = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ: Any = LUMIBOT_DEFAULT_PYTZ
    option_quote_fallback_allowed: bool = False

    def __init__(
        self,
        api_key: str | None = None,
        delay: int | None = None,
        tzinfo: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """

        Parameters
        ----------
        api_key : str
            The API key to use for the data source
        delay : int
            The number of minutes to delay the data by. This is useful for paper trading data sources that
            provide delayed data (i.e. 15m delayed data).
        """
        self.name: str = "data_source"
        self._timestep: str | None = None
        self._api_key = api_key
        self._datetime: Any | None = None
        self.datetime_start: Any | None = None
        self.datetime_end: Any | None = None

        # Use DATA_SOURCE_DELAY environment variable if it exists and delay is not explicitly provided
        if delay is None:
            env_delay = os.environ.get("DATA_SOURCE_DELAY")
            if env_delay is not None:
                try:
                    delay = int(env_delay)
                except ValueError:
                    # If the environment variable is not a valid integer, ignore it
                    pass
            else:
                # Default to 0 if no environment variable is set
                delay = 0

        self._delay = timedelta(minutes=delay) if delay is not None else None

        if tzinfo is None:
            tzinfo = pytz.timezone(self.DEFAULT_TIMEZONE)
        self.tzinfo: Any = tzinfo

        # Initialize caches centrally (avoid ad-hoc hasattr checks in methods)
        self._greeks_cache: dict[tuple[Any, ...], GreeksMap] = {}

        # Thread pool for parallel operations - reuse to avoid creation/destruction overhead
        self._thread_pool: ThreadPoolExecutor | None = None
        self._thread_pool_max_workers: int = int(kwargs.get("max_workers", 10))

        # Dividend cache for backtest performance
        self._dividend_cache: dict[Any, dict[Any, Any]] = {}  # {asset: {date: dividend_value}}
        self._dividend_cache_enabled: bool = bool(kwargs.get("cache_dividends", True))

        # Ensure the instance has an explicit attribute for fallback behaviour
        if not hasattr(self, "option_quote_fallback_allowed"):
            self.option_quote_fallback_allowed = False

    def _get_or_create_thread_pool(self) -> ThreadPoolExecutor:
        """Get or create the thread pool for parallel operations"""
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(max_workers=self._thread_pool_max_workers)
        return self._thread_pool

    def shutdown(self) -> None:
        """Cleanup thread pool resources"""
        if self._thread_pool is not None:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

    # ========Required Implementations ======================
    @abstractmethod
    def get_chains(self, asset: Any, quote: Any = None) -> ChainMap:
        """
        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset
            The asset to get the option chains for
        quote : Asset | None
            The quote asset to get the option chains for

        Returns
        -------
        dict
            Mapping with keys such as ``Multiplier`` (e.g. ``"100"``) and ``Chains``.
            ``Chains`` is a nested dictionary where expiration dates map to strike lists,
            e.g. ``chains['Chains']['CALL']['2023-07-31'] = [strike1, strike2, ...]``.
        """
        pass

    @abstractmethod
    def get_historical_prices(
        self,
        asset: Any,
        length: int,
        timestep: str = "",
        timeshift: timedelta | None = None,
        quote: Any = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        **kwargs: Any,
    ) -> Bars | None:
        """
        Get bars for a given asset, going back in time from now, getting length number of bars by timestep.
        For example, with a length of 10 and a timestep of "day", and no timeshift, this
        would return the last 10 daily bars.

        - Higher-level method that returns a `Bars` object
        - Handles timezone conversions automatically
        - Includes additional metadata and processing
        - Preferred for strategy development and backtesting
        - Returns normalized data with consistent format across data sources

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. Accepts "day" "hour" or "minute".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        return_polars : bool (deprecated)
            Deprecated. Do not use in strategy code. This keyword will be removed in a future release.
            Strategy logic should use pandas operations on ``bars.pandas_df`` and should not depend on
            the underlying DataFrame backend.

        Returns
        -------
        Bars
            The bars for the asset. For strategy code, prefer ``bars.pandas_df`` for a pandas DataFrame.
        """
        pass

    @abstractmethod
    def get_last_price(self, asset: Any, quote: Any = None, exchange: str | None = None) -> float | Decimal | None:
        """
        Takes an asset and returns the last known price

        Parameters
        ----------
        asset : Asset
            The asset to get the price of.
        quote : Asset
            The quote asset to get the price of.
        exchange : str
            The exchange to get the price of.

        Returns
        -------
        float or Decimal or None
            The last known price of the asset.
        """
        pass

    # ========Python datetime helpers======================

    def get_datetime(self, adjust_for_delay: bool = False) -> datetime:
        """
        Returns the current datetime in the default timezone

        Parameters
        ----------
        adjust_for_delay : bool
            Whether to adjust the current time for the delay. This is useful for paper trading data sources that
            provide delayed data.

        Returns
        -------
        datetime
        """
        current_time = self.to_default_timezone(datetime.now())
        if adjust_for_delay and self._delay:
            current_time -= self._delay
        return current_time

    def get_timestamp(self) -> float:
        """
        Returns the current timestamp in the default timezone
        Returns
        -------
        float
        """
        return self.get_datetime().timestamp()

    def get_round_minute(self, timeshift: int = 0) -> datetime:
        """
        Returns the current datetime rounded to the minute and applies a timeshift in minutes
        Parameters
        ----------
        timeshift: int
            The number of minutes to shift the datetime by

        Returns
        -------
        datetime
            Rounded datetime with the timeshift applied
        """
        current = self.get_datetime().replace(second=0, microsecond=0)
        return current - timedelta(minutes=timeshift)

    def get_last_minute(self) -> datetime:
        return self.get_round_minute(timeshift=1)

    def get_round_day(self, timeshift: int = 0) -> datetime:
        """
        Returns the current datetime rounded to the day and applies a timeshift in days
        Parameters
        ----------
        timeshift: int
            The number of days to shift the datetime by

        Returns
        -------
        datetime
            Rounded datetime with the timeshift applied
        """
        current = self.get_datetime().replace(hour=0, minute=0, second=0, microsecond=0)
        return current - timedelta(days=timeshift)

    def get_last_day(self) -> datetime:
        return self.get_round_day(timeshift=1)

    def get_datetime_range(
        self,
        length: int,
        timestep: str = "minute",
        timeshift: timedelta | None = None,
    ) -> tuple[datetime, datetime]:
        if timestep == "minute":
            period_length = length * timedelta(minutes=1)
            end_date = self.get_last_minute()
        else:
            period_length = length * timedelta(days=1)
            end_date = self.get_last_day()

        if timeshift:
            end_date -= timeshift

        start_date = end_date - period_length
        return start_date, end_date

    def localize_datetime(self, dt: datetime) -> datetime:
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            return self.to_default_timezone(dt)
        else:
            return self.tzinfo.localize(dt, is_dst=None)

    def to_default_timezone(self, dt: datetime) -> datetime:
        return dt.astimezone(self.tzinfo)

    def get_timestep(self) -> str:
        return self._timestep if self._timestep else self.MIN_TIMESTEP

    @staticmethod
    def convert_timestep_str_to_timedelta(timestep: str) -> tuple[timedelta, str]:
        """
        Convert a timestep string to a timedelta object. For example, "1minute" will be converted to a
        timedelta of 1 minute.

        Parameters
        ----------
        timestep : str
            The timestep string to convert. For example, "1minute" or "1hour" or "1day".

        Returns
        -------
        timedelta
            A timedelta object representing the timestep.
        unit : str
            The unit of the timestep. For example, "minute" or "hour" or "day".
        """
        timestep = timestep.lower()
        if not timestep:
            raise ValueError("timestep cannot be empty")

        # Define mapping from timestep units to equivalent minutes
        time_unit_map = {
            "minute": 1,
            "min": 1,  # Common shorthand (e.g., "15min")
            "hour": 60,
            "day": 24 * 60,
            "m": 1,  # "M" is for minutes
            "h": 60,  # "H" is for hours
            "d": 24 * 60,  # "D" is for days
        }

        # Define default values
        quantity = 1
        unit = ""

        # Check if timestep string has a number at the beginning
        if timestep[0].isdigit():
            for i, char in enumerate(timestep):
                if not char.isdigit():
                    # Get the quantity (number of units)
                    quantity = int(timestep[:i])
                    # Get the unit (minute, hour, or day)
                    # IBRK uses "minutes" instead of "minute" when 'quantity' > 1, for some reason, so handle
                    # that behavior here so backtest is comptiable with IBRK
                    unit = timestep[i:].strip().rstrip("s")  # Remove extra whitespace and IBKR's extra pluralization
                    break
        else:
            unit = timestep

        # Check if the unit is valid
        if unit in time_unit_map:
            # Convert quantity to minutes
            quantity_in_minutes = quantity * time_unit_map[unit]
            # Convert minutes to timedelta
            delta = timedelta(minutes=quantity_in_minutes)
            canonical_unit = {
                "m": "minute",
                "min": "minute",
                "minute": "minute",
                "h": "hour",
                "hour": "hour",
                "d": "day",
                "day": "day",
            }.get(unit, unit)
            return delta, canonical_unit
        else:
            raise ValueError(f"Unknown unit: {unit}. Valid units are minute, hour, day, M, H, D")

    # ========Internal Market Data Methods===================

    def _parse_source_timestep(self, timestep: str, reverse: bool = False) -> str:
        """transform the data source timestep variable
        into lumibot representation. set reverse to True
        for opposite direction"""
        for item in self.TIMESTEP_MAPPING:
            if reverse:
                if timestep == item["timestep"]:
                    return item["representations"][0]
            else:
                if timestep in item["representations"]:
                    return item["timestep"]

        raise UnavailabeTimestep(self.SOURCE, timestep)

    def _parse_source_symbol_bars(self, response: Any, asset: Any, quote: Any = None) -> Any:
        raise NotImplementedError("DataSource subclasses must implement _parse_source_symbol_bars")

    def _parse_source_bars(self, response: Mapping[Any, Any], quote: Any = None) -> BarsResultMap:
        result: BarsResultMap = {}
        for asset, data in response.items():
            if data is None or isinstance(data, float):
                result[asset] = data
                continue
            result[asset] = self._parse_source_symbol_bars(data, asset, quote=quote)
        return result

    # =================Public Market Data Methods==================

    def get_bars(
        self,
        assets: AssetInput | Iterable[AssetInput],
        length: int,
        timestep: str = "minute",
        timeshift: timedelta | None = None,
        chunk_size: int = 2,
        max_workers: int = 2,
        quote: Any = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        sleep_time: float | None = None,
    ) -> BarsResultMap:
        """Get bars for the list of assets"""
        del max_workers
        if isinstance(assets, list):
            raw_assets = assets
        else:
            raw_assets = [cast(AssetInput, assets)]

        effective_sleep_time = sleep_time
        if effective_sleep_time is None:
            effective_sleep_time = 0.0 if getattr(self, "IS_BACKTESTING_DATA_SOURCE", False) else 0.1

        def process_chunk(chunk: list[AssetInput]) -> BarsResultMap:
            chunk_result: BarsResultMap = {}
            for asset in chunk:
                if isinstance(asset, tuple):
                    base_asset: AssetInput = asset[0]
                    quote_asset: Any = asset[1]
                else:
                    base_asset = asset
                    quote_asset = quote
                try:
                    chunk_result[asset] = self.get_historical_prices(
                        asset=base_asset,
                        length=length,
                        timestep=timestep,
                        timeshift=timeshift,
                        quote=quote_asset,
                        exchange=exchange,
                        include_after_hours=include_after_hours,
                    )

                    # Sleep to prevent rate limiting
                    if effective_sleep_time:
                        time.sleep(effective_sleep_time)
                except Exception as e:
                    # Log once per asset to avoid spamming with a huge traceback
                    base_symbol = getattr(base_asset, "symbol", base_asset)
                    logger.warning(f"Error retrieving data for {base_symbol}: {e}")
                    tb = traceback.format_exc()
                    logger.warning(tb)  # This prints the traceback
                    chunk_result[asset] = None
            return chunk_result

        # Convert strings to Asset objects
        asset_list: list[AssetInput] = [Asset(symbol=a) if isinstance(a, str) else a for a in raw_assets]

        # Chunk the assets
        chunks = [asset_list[i : i + chunk_size] for i in range(0, len(asset_list), chunk_size)]

        results: BarsResultMap = {}
        # Reuse thread pool to avoid creation/destruction overhead
        from concurrent.futures import as_completed

        executor = self._get_or_create_thread_pool()
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            results.update(future.result())

        return results

    def get_last_prices(self, assets: Iterable[Any], quote: Any = None, exchange: str | None = None) -> Any:
        """Takes a list of assets and returns the last known prices"""

        result: dict[Any, float | Decimal | None] = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)

        if self.SOURCE == "CCXT":
            return result
        else:
            return AssetsMapping(result)

    def get_strikes(self, asset: Asset) -> list[Any]:
        """Return a set of strikes for a given asset"""
        chains = self.get_chains(asset)
        all_strikes: set[Any] = set()
        for right in chains["Chains"]:
            for _exp_date, strike_values in chains["Chains"][right].items():
                all_strikes.update(strike_values)

        return sorted(all_strikes)

    def get_yesterday_dividend(self, asset: Asset, quote: Any = None) -> Any:
        """Return dividend per share for a given
        asset for the day before"""
        bars = self.get_historical_prices(asset, 1, timestep="day", quote=quote)
        if bars is None:
            return 0
        return bars.get_last_dividend()

    def get_yesterday_dividends(self, assets: Iterable[Asset], quote: Any = None) -> AssetsMapping:
        """Return dividend per share for a list of assets for the day before.

        For backtesting, this method caches all dividend data to avoid repeated API calls.
        On the first call for an asset, it fetches ALL historical dividend data and caches it.
        Subsequent calls use the cache.
        """
        result: dict[Any, Any] = {}

        # For backtesting with dividends, use an efficient caching strategy
        current_datetime = getattr(self, "_datetime", None)
        if current_datetime:
            current_date = current_datetime.date() if hasattr(current_datetime, "date") else current_datetime

            # Process each asset
            for asset in assets:
                # Check if we've already cached ALL dividends for this asset
                if asset not in self._dividend_cache:
                    # First time seeing this asset - fetch ALL its historical data and cache dividends
                    # Get enough bars to cover the entire backtest period
                    # Most backtests are < 1000 days; limit the fetch to what's needed for this backtest
                    # to avoid slow API calls (especially Polygon) and keep CI fast.
                    try:
                        # Default to a conservative upper bound, but shrink for short backtests.
                        length = 2000
                        if (
                            hasattr(self, "datetime_start")
                            and hasattr(self, "datetime_end")
                            and getattr(self, "datetime_start", None) is not None
                            and getattr(self, "datetime_end", None) is not None
                        ):
                            try:
                                datetime_end = self.datetime_end
                                datetime_start = self.datetime_start
                                if datetime_end is None or datetime_start is None:
                                    raise ValueError("datetime_start and datetime_end must be set")
                                span_days = (datetime_end.date() - datetime_start.date()).days + 1
                                # Add a small cushion for weekends/holidays; ensure at least ~1 month.
                                length = min(2000, max(span_days + 10, 30))
                            except Exception:
                                length = 2000

                        bars = self.get_bars([asset], length, timestep="day", quote=quote).get(asset)

                        # Extract all dividends from the bars and store by date
                        asset_dividends: dict[Any, Any] = {}
                        if bars is not None and hasattr(bars, "df") and "dividend" in bars.df.columns:
                            # Store dividend for each date
                            for idx, row in bars.df.iterrows():
                                date = idx.date() if hasattr(idx, "date") else idx
                                dividend_val = row.get("dividend", 0)
                                if dividend_val and dividend_val > 0:
                                    asset_dividends[date] = dividend_val

                        # Cache the dividend dict for this asset
                        self._dividend_cache[asset] = asset_dividends
                        if asset_dividends:
                            logger.debug(
                                "[DIVIDEND][CACHE] Cached %d entries for %s (%s -> %s)",
                                len(asset_dividends),
                                getattr(asset, "symbol", asset),
                                min(asset_dividends.keys()),
                                max(asset_dividends.keys()),
                            )
                        else:
                            logger.debug(
                                "[DIVIDEND][CACHE] No dividend entries available for %s",
                                getattr(asset, "symbol", asset),
                            )
                    except Exception:
                        # If fetching fails, cache empty dict to avoid repeated failures
                        self._dividend_cache[asset] = {}

                # Now look up the dividend for the current trading date. Daily bars already align
                # dividends with the ex-date, so there's no need to subtract a day here.
                asset_dividends = self._dividend_cache.get(asset, {})
                dividend = asset_dividends.get(current_date, 0)
                if dividend:
                    logger.debug(
                        "[DIVIDEND][APPLY] %s -> %s pays %.4f on %s",
                        getattr(asset, "symbol", asset),
                        getattr(self, "_name", "strategy"),
                        dividend,
                        current_date,
                    )
                result[asset] = dividend

            return AssetsMapping(result)

        # Fallback to normal flow for non-backtesting
        assets_bars = self.get_bars(assets, 1, timestep="day", quote=quote)
        for asset, bars in assets_bars.items():
            if bars is not None:
                result[asset] = bars.get_last_dividend()

        return AssetsMapping(result)

    def get_chain_full_info(
        self,
        asset: Asset,
        expiry: Any,
        chains: ChainMap | None = None,
        underlying_price: float | Decimal | None = None,
        risk_free_rate: float | Decimal | None = None,
        strike_min: float | None = None,
        strike_max: float | None = None,
    ) -> PandasDataFrame:
        """
        Get the full chain information for an option asset, including: greeks, bid/ask, open_interest, etc. For
        brokers that do not support this, greeks will be calculated locally. For brokers like Tradier this function
        is much faster as only a single API call can be done to return the data for all options simultaneously.

        Parameters
        ----------
        asset : Asset
            The option asset to get the chain information for.
        expiry : datetime.date | datetime.datetime
            The expiry date of the option chain.
        chains : dict
            The chains dictionary created by `get_chains` method. This is used
            to get the list of strikes needed to calculate the greeks.
        underlying_price : float
            Price of the underlying asset.
        risk_free_rate : float
            The risk-free rate used in interest calculations.
        strike_min : float
            The minimum strike price to return in the chain. If None, will return all strikes.
            Providing this will speed up execution by limiting the number of strikes queried.
        strike_max : float
            The maximum strike price to return in the chain. If None, will return all strikes.
            Providing this will speed up execution by limiting the number of strikes queried.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the full chain information for the option asset. Greeks columns will be named as
            'greeks.delta', 'greeks.theta', etc.
        """
        start_t = time.perf_counter()
        # Base level DataSource assumes that the data source does not support this and the greeks will be calculated
        # locally. Subclasses can override this method to provide a more efficient implementation.
        if isinstance(expiry, str):
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d").date()
        elif isinstance(expiry, datetime):
            expiry_dt = expiry.date()
        elif isinstance(expiry, date):
            expiry_dt = expiry
        else:
            raise TypeError("expiry must be a string, datetime.date, or datetime.datetime instance")

        expiry_str = expiry_dt.strftime("%Y-%m-%d")
        if chains is None:
            chains = self.get_chains(asset)
        if asset.symbol is None:
            raise ValueError("Cannot build option chain rows for an asset without a symbol")

        rows: list[dict[str, Any]] = []
        query_total = 0
        for right in chains["Chains"]:
            expirations_map = chains["Chains"].get(right, {})
            if expiry_str not in expirations_map:
                raise KeyError(f"Expiry {expiry_str} not available for option type {right}")
            for strike in expirations_map[expiry_str]:
                # Skip strikes outside the requested range. Saves querying time.
                if (strike_min is not None and strike < strike_min) or (
                    strike_max is not None and strike > strike_max
                ):
                    continue

                # Build the option asset and query for the price
                opt_asset = Asset(
                    asset.symbol,
                    asset_type="option",
                    expiration=expiry_dt,
                    strike=strike,
                    right=right,
                )
                query_t = time.perf_counter()
                option_symbol = _create_options_symbol(opt_asset.symbol, expiry_dt, right, strike)
                opt_price = self.get_last_price(opt_asset)
                greeks = self.calculate_greeks(opt_asset, opt_price, underlying_price, risk_free_rate)
                if greeks is None:
                    greeks = {}
                query_total += time.perf_counter() - query_t

                # Build the row. Match the Tradier column naming conventions.
                row = {
                    "symbol": option_symbol,
                    "last": opt_price,
                    "expiration_date": expiry_dt,
                    "strike": strike,
                    "option_type": right,
                    "underlying": opt_asset.symbol,
                    "open_interest": 0,
                    "bid": 0.0,
                    "ask": 0.0,
                    "bidsize": 0,
                    "asksize": 0,
                    "volume": 0,
                    "last_volume": 0,
                    "average_volume": 0,
                    "type": "option",
                }
                # Add in the greeks. Format: greeks.delta, greeks.theta, etc.
                row.update({f"greeks.{col}": val for col, val in greeks.items()})
                rows.append(row)

        logger.info(
            f"Chain Full Info Query Total: {query_total:.2f}s. "
            f"Total Time: {time.perf_counter() - start_t:.2f}s, "
            f"Rows: {len(rows)}"
        )
        return pd.DataFrame(rows).sort_values("strike") if rows else pd.DataFrame()

    def calculate_greeks(
        self,
        asset: Asset,
        # API Querying for prices and rates are expensive, so we'll pass them in as arguments most of the time
        asset_price: float | Decimal | None,
        underlying_price: float | Decimal | None,
        risk_free_rate: float | Decimal | None,
    ) -> GreeksMap | None:
        """Returns Greeks in backtesting."""
        # Handle None values - don't cache or calculate if inputs are invalid
        if asset_price is None or underlying_price is None or risk_free_rate is None:
            return None

        # Optimization: Cache Greeks calculations based on key parameters
        # Round prices to 2 decimal places for cache key to handle minor price fluctuations
        current_date = self.get_datetime()
        asset_symbol = asset.symbol
        asset_expiration = asset.expiration
        asset_strike = asset.strike
        asset_right = asset.right

        if asset_expiration is None:
            raise ValueError(f"Cannot calculate greeks for {asset}: expiration is required")
        if asset_strike is None:
            raise ValueError(f"Cannot calculate greeks for {asset}: strike is required")
        if asset_right is None:
            raise ValueError(f"Cannot calculate greeks for {asset}: right is required")

        asset_price_float = float(asset_price)
        underlying_price_float = float(underlying_price)
        risk_free_rate_float = float(risk_free_rate)

        cache_key = (
            asset_symbol,
            asset_strike,
            asset_right,
            asset_expiration,
            round(asset_price_float, 2),
            round(underlying_price_float, 2),
            round(risk_free_rate_float, 4),
            current_date.date()
            if hasattr(current_date, "date")
            else current_date,  # Cache per day to handle time decay
        )

        # Check cache
        if cache_key in self._greeks_cache:
            return self._greeks_cache[cache_key]

        # Keep cache size limited to prevent memory issues
        if len(self._greeks_cache) > 10000:
            # Clear oldest half of cache
            keys_to_remove = list(self._greeks_cache.keys())[:5000]
            for key in keys_to_remove:
                del self._greeks_cache[key]

        opt_price = asset_price_float
        und_price = underlying_price_float
        interest = risk_free_rate_float * 100

        # If asset expiration is a datetime object, convert it to date
        expiration = asset_expiration
        if isinstance(expiration, datetime):
            expiration = expiration.date()

        # Convert the expiration to be a datetime with 4pm New York time
        expiration = datetime.combine(expiration, datetime.min.time())
        expiration = self.tzinfo.localize(expiration)
        expiration = expiration.astimezone(self.tzinfo)
        expiration = expiration.replace(hour=16, minute=0, second=0, microsecond=0)

        # Calculate the days to expiration, but allow for fractional days
        days_to_expiration = (expiration - current_date).total_seconds() / (60 * 60 * 24)

        right = str(asset_right).upper()
        strike = float(asset_strike)

        if right == "CALL":
            is_call = True
            iv = black_scholes.BS(
                [und_price, strike, interest, days_to_expiration],
                callPrice=opt_price,
            )
        elif right == "PUT":
            is_call = False
            iv = black_scholes.BS(
                [und_price, strike, interest, days_to_expiration],
                putPrice=opt_price,
            )
        else:
            raise ValueError(f"Invalid option type {asset.right}, cannot get option greeks")

        c = black_scholes.BS(
            [und_price, strike, interest, days_to_expiration],
            volatility=iv.impliedVolatility,
        )

        greeks = dict(
            implied_volatility=iv.impliedVolatility,
            delta=c.callDelta if is_call else c.putDelta,
            option_price=c.callPrice if is_call else c.putPrice,
            pv_dividend=None,  # (No equiv )
            gamma=c.gamma,
            vega=c.vega,
            theta=c.callTheta if is_call else c.putTheta,
            underlying_price=und_price,
        )

        # Cache the result
        self._greeks_cache[cache_key] = greeks

        return greeks

    def query_greeks(self, asset: Asset) -> GreeksMap:
        """Query for the Greeks as it can be more accurate than calculating locally."""
        logger.info(
            f"Querying Options Greeks for {asset.symbol} is not supported for this data source {self.__class__}."
        )
        return {}

    def get_quote(self, asset: Asset, quote: Asset | None = None, exchange: str | None = None) -> Quote | None:
        """
        Get the latest quote for an asset (stock, option, or crypto).
        Returns a Quote object with bid, ask, last, and other fields if available.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.

        Returns
        -------
        Quote
            A Quote object with the quote information, eg. bid, ask, etc.
        """
        raise NotImplementedError("get_quote method not implemented")
