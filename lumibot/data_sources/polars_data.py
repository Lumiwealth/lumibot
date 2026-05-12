from __future__ import annotations

from collections import OrderedDict, defaultdict
from collections.abc import Iterable
from datetime import timedelta
from decimal import Decimal
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, cast

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

if TYPE_CHECKING:
    from lumibot.entities.quote import Quote

logger = get_logger(__name__)


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

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
_bars_class_cache: type[Any] | None = None
_USD_FOREX = Asset("USD", "forex")


def _bars_class() -> type[Any]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    assert _bars_class_cache is not None
    return _bars_class_cache


class PolarsData(DataSourceBacktesting):
    """
    PolarsData is a Backtesting-only DataSource that will be optimized to use Polars DataFrames.
    Currently identical to PandasData as a baseline. Will be incrementally converted to use Polars.
    """

    SOURCE: ClassVar[str] = "POLARS"
    TIMESTEP_MAPPING: ClassVar[list[dict[str, list[str] | str]]] = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    option_quote_fallback_allowed: bool
    name: str
    pandas_data: OrderedDict[Any, Any]
    auto_adjust: bool
    _data_store: OrderedDict[Any, Any]
    _date_index: Any | None
    _date_supply: Any | None
    _timestep: str
    _history_window_bars: int
    _future_window_bars: int
    _trim_frequency_bars: int
    _trim_iteration_count: int
    _aggregated_cache: OrderedDict[Any, Any]
    _max_storage_bytes: int

    def __init__(
        self,
        *args: Any,
        pandas_data: Any | None = None,
        auto_adjust: bool = True,
        allow_option_quote_fallback: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)  # pyright: ignore[reportUnknownMemberType]
        self.option_quote_fallback_allowed = allow_option_quote_fallback
        self.name = "polars"
        self.pandas_data = self._set_pandas_data_keys(pandas_data)
        self.auto_adjust = auto_adjust
        self._data_store = self.pandas_data
        self._date_index = None
        self._date_supply = None
        self._timestep = "minute"

        # Sliding window configuration (always-on, optimized for speed)
        self._history_window_bars = 5000  # Fixed window size
        self._future_window_bars = 1000  # Look-ahead buffer for efficiency
        self._trim_frequency_bars = 1000  # Trim every 1000 iterations
        self._trim_iteration_count = 0  # Counter for periodic trimming

        # Aggregated bars cache (separate from pandas_data)
        # Uses existing OrderedDict infrastructure for LRU tracking
        self._aggregated_cache = OrderedDict()

        # Memory limits (1 GB hard cap)
        self._max_storage_bytes = 1_000_000_000

    @property
    def MAX_STORAGE_BYTES(self) -> int:
        return self._max_storage_bytes

    @MAX_STORAGE_BYTES.setter
    def MAX_STORAGE_BYTES(self, value: int) -> None:
        self._max_storage_bytes = int(value)

    def _trim_cached_data(self) -> None:
        """Periodically trim cached data to maintain sliding window.

        Called every trim-frequency interval to remove old bars
        that are outside the sliding window. This keeps memory usage low
        while maintaining enough history for lookback calculations.

        This is always-on and requires no user configuration.
        """
        # Increment iteration counter
        self._trim_iteration_count += 1

        # Only trim every TRIM_FREQUENCY_BARS iterations
        if self._trim_iteration_count < self._trim_frequency_bars:
            return

        # Reset counter
        self._trim_iteration_count = 0

        # Get current datetime for window calculation
        current_dt = self.get_datetime()

        # Trim each DataPolars object in the data store
        # CRITICAL: Use each data object's own timestep, not global self._timestep
        # A backtest can have mixed timeframes (1m, 5m, 1h, 1d for same asset)
        trimmed_count = 0
        for asset_key, data in self._data_store.items():
            # Only trim if data is a DataPolars object (has trim_before method)
            if not hasattr(data, "trim_before"):
                continue

            try:
                # Get this data object's timestep (not the global self._timestep!)
                data_timestep = getattr(data, "timestep", "minute")

                # Use convert_timestep_str_to_timedelta for robust conversion
                base_delta, _ = cast(tuple[timedelta, str], self.convert_timestep_str_to_timedelta(data_timestep))

                # Calculate cutoff for this specific data object
                # Keep HISTORY_WINDOW_BARS bars of this timestep before current time
                window_delta = base_delta * self._history_window_bars
                cutoff_dt = current_dt - window_delta

                # Trim with the correct per-asset cutoff
                data.trim_before(cutoff_dt)

                trimmed_count += 1

            except Exception as e:
                logger.warning(f"Failed to trim data for {asset_key}: {e}")

        if trimmed_count > 0:
            logger.debug(f"[SLIDING WINDOW] Trimmed {trimmed_count} assets at iteration {self._trim_frequency_bars}")

    def _get_aggregation_cache_key(self, asset: Any, quote: Any, timestep: str) -> tuple[Any, Any, str]:
        """Generate a unique cache key for aggregated bars.

        Parameters
        ----------
        asset : Asset
            The asset
        quote : Asset
            The quote asset
        timestep : str
            The timestep (e.g., "5 minutes", "15 minutes", "hour", "day")

        Returns
        -------
        tuple
            Cache key (asset, quote, timestep)
        """
        asset_key: Any = asset
        quote_key: Any = quote
        if isinstance(asset_key, tuple):
            asset_tuple = cast(tuple[Any, ...], asset_key)
            asset_key, quote_key = asset_tuple
        return (asset_key, quote_key, timestep)

    def _aggregate_polars_bars(self, source_data: Any, target_timestep: str) -> Any | None:
        """Aggregate minute-level polars data to higher timeframes.

        This is a critical performance optimization - aggregating once and caching
        is much faster than re-aggregating every iteration.

        Parameters
        ----------
        source_data : DataPolars
            Source data (typically 1-minute bars)
        target_timestep : str
            Target timestep ("5 minutes", "15 minutes", "hour", "day")

        Returns
        -------
        polars.DataFrame or None
            Aggregated data, or None if aggregation not possible
        """
        try:
            pl_module: Any = import_module("polars")

            # Get the polars DataFrame from DataPolars
            if not hasattr(source_data, "polars_df"):
                return None

            df: Any = source_data.polars_df
            if df.height == 0:
                return None

            # Map timestep to polars interval
            interval_mapping = {
                "5 minutes": "5m",
                "15 minutes": "15m",
                "30 minutes": "30m",
                "hour": "1h",
                "2 hours": "2h",
                "4 hours": "4h",
                "day": "1d",
            }

            interval = interval_mapping.get(target_timestep)
            if not interval:
                logger.warning(f"Unsupported aggregation timestep: {target_timestep}")
                return None

            # Aggregate using polars group_by_dynamic (fast!)
            # This is the core optimization - polars aggregation is 10-100x faster than pandas
            aggregated = df.group_by_dynamic("datetime", every=interval, closed="left", label="left").agg(
                [
                    pl_module.col("open").first(),
                    pl_module.col("high").max(),
                    pl_module.col("low").min(),
                    pl_module.col("close").last(),
                    pl_module.col("volume").sum(),
                ]
            )

            logger.debug(
                f"[AGGREGATION] {source_data.asset.symbol}: {df.height} rows ({source_data.timestep}) → {aggregated.height} rows ({target_timestep})"
            )
            return aggregated

        except Exception as e:
            logger.error(f"Error aggregating data: {e}")
            return None

    def _get_or_aggregate_bars(
        self,
        asset: Asset,
        quote: Asset | None,
        length: int,
        source_timestep: str,
        target_timestep: str,
    ) -> Any | None:
        """Get aggregated bars from cache or create them.

        This method implements the aggregated bars cache to avoid re-aggregating
        5m/15m/1h bars from 1-minute data on every iteration.

        Parameters
        ----------
        asset : Asset
            The asset
        quote : Asset
            The quote asset
        length : int
            Number of bars requested
        source_timestep : str
            Source timestep (typically "minute")
        target_timestep : str
            Target timestep (e.g., "5 minutes", "15 minutes", "hour")

        Returns
        -------
        polars.DataFrame or None
            Aggregated bars, or None if not available
        """
        # Generate cache key
        cache_key = self._get_aggregation_cache_key(asset, quote, target_timestep)

        # Check if we already have aggregated data cached
        if cache_key in self._aggregated_cache:
            # Move to end (LRU tracking)
            self._aggregated_cache.move_to_end(cache_key)
            logger.debug(f"[AGG CACHE HIT] {asset.symbol} {target_timestep}")
            return self._aggregated_cache[cache_key]

        # Need to aggregate from source data
        asset_key = self.find_asset_in_data_store(asset, quote)
        if not asset_key or asset_key not in self._data_store:
            return None

        source_data: Any = self._data_store[asset_key]

        # Only aggregate from DataPolars objects (has polars_df)
        if not hasattr(source_data, "polars_df"):
            logger.warning(f"Cannot aggregate - source data is not DataPolars: {type(source_data)}")
            return None

        # Perform aggregation
        aggregated_df = self._aggregate_polars_bars(source_data, target_timestep)
        if aggregated_df is None:
            return None

        # Cache the result (LRU cache)
        self._aggregated_cache[cache_key] = aggregated_df
        logger.debug(f"[AGG CACHE MISS] {asset.symbol} {target_timestep} - cached {aggregated_df.height} rows")

        # Note: Memory limits are enforced periodically in get_historical_prices()
        # Don't enforce here to avoid immediate eviction after caching

        return aggregated_df

    def _enforce_memory_limits(self) -> None:
        """Enforce memory limits using LRU eviction.

        This method ensures total memory usage stays under the configured 1GB cap
        by evicting least-recently-used items from both _data_store and _aggregated_cache.

        Uses the proven LRU pattern from polygon_backtesting_pandas.py.

        PERFORMANCE: Only checks every trim-frequency interval (same as trim).
        Checking memory on every get_historical_prices() call is expensive!
        """
        # Use the same periodic counter as _trim_cached_data
        # Only check memory limits when we actually trim (every 1000 iterations)
        # This avoids iterating all data on every get_historical_prices call
        if self._trim_iteration_count != 0:
            return  # Not time to check yet

        try:
            # Calculate total memory usage
            storage_used = 0

            # Memory from _data_store (DataPolars objects)
            for data in self._data_store.values():
                if hasattr(data, "polars_df"):
                    # Estimate polars DataFrame memory
                    df: Any = data.polars_df
                    if df.height > 0:
                        # Polars estimated_size() returns bytes
                        storage_used += int(df.estimated_size())

            # Memory from _aggregated_cache (polars DataFrames)
            for agg_df in self._aggregated_cache.values():
                if agg_df is not None and hasattr(agg_df, "estimated_size"):
                    storage_used += int(agg_df.estimated_size())

            if storage_used <= self._max_storage_bytes:
                return  # Under limit, nothing to do

            logger.debug(
                f"[MEMORY] Storage used: {storage_used:,} bytes ({len(self._data_store)} data + {len(self._aggregated_cache)} aggregated)"
            )
            logger.warning(f"[MEMORY] Exceeds limit of {self._max_storage_bytes:,} bytes, evicting LRU items...")

            # Evict from aggregated cache first (less critical than source data)
            while storage_used > self._max_storage_bytes and len(self._aggregated_cache) > 0:
                # popitem(last=False) removes oldest (LRU)
                k, agg_df = self._aggregated_cache.popitem(last=False)
                if agg_df is not None and hasattr(agg_df, "estimated_size"):
                    freed = int(agg_df.estimated_size())
                    storage_used -= freed
                    logger.debug(f"[MEMORY] Evicted aggregated cache for {k}: freed {freed:,} bytes")
                else:
                    # Item has no size - assume 0 bytes freed but continue evicting
                    logger.warning(f"[MEMORY] Evicted aggregated cache for {k}: no estimated_size(), assuming 0 bytes")

            # If still over limit, evict from data_store (more aggressive)
            evicted_data_items = 0
            while storage_used > self._max_storage_bytes and len(self._data_store) > 0:
                # popitem(last=False) removes oldest (LRU)
                k, data = self._data_store.popitem(last=False)
                if hasattr(data, "polars_df"):
                    df = data.polars_df
                    if df.height > 0:
                        freed = int(df.estimated_size())
                        storage_used -= freed
                        evicted_data_items += 1
                        logger.warning(f"[MEMORY] Evicted data_store for {k}: freed {freed:,} bytes")
                    else:
                        # DataFrame is empty - assume 0 bytes
                        evicted_data_items += 1
                        logger.warning(f"[MEMORY] Evicted data_store for {k}: empty DataFrame, 0 bytes freed")
                else:
                    # Not a DataPolars object - assume 0 bytes
                    logger.warning(f"[MEMORY] Evicted data_store for {k}: no polars_df, assuming 0 bytes")

            if evicted_data_items > 0:
                logger.warning(
                    f"[MEMORY] Evicted {evicted_data_items} data items to stay under {self._max_storage_bytes:,} bytes"
                )

            logger.debug(
                f"[MEMORY] After eviction: {storage_used:,} bytes ({len(self._data_store)} data + {len(self._aggregated_cache)} aggregated)"
            )

        except Exception as e:
            logger.error(f"Error enforcing memory limits: {e}")

    @staticmethod
    def _set_pandas_data_keys(pandas_data: Any) -> OrderedDict[Any, Any]:
        # OrderedDict tracks the LRU dataframes for when it comes time to do evictions.
        new_pandas_data: OrderedDict[Any, Any] = OrderedDict()

        def _get_new_pandas_data_key(data: Any) -> Any:
            # Always save the asset as a tuple of Asset and quote
            data_asset: Any = data.asset
            data_quote: Any = data.quote
            if isinstance(data_asset, tuple):
                return cast(tuple[Any, ...], data_asset)
            elif isinstance(data_asset, Asset):
                # If quote is not specified, use USD as the quote
                if data_quote is None:
                    # Warn that USD is being used as the quote
                    logger.warning(f"No quote specified for {data_asset}. Using USD as the quote.")
                    return data_asset, _USD_FOREX
                return data_asset, data_quote
            else:
                raise ValueError("Asset must be an Asset or a tuple of Asset and quote")

        # Check if pandas_data is a dictionary
        if isinstance(pandas_data, dict):
            for _k, data in cast(dict[Any, Any], pandas_data).items():
                key = _get_new_pandas_data_key(data)
                new_pandas_data[key] = data

        # Check if pandas_data is a list
        elif isinstance(pandas_data, list):
            for data in cast(list[Any], pandas_data):
                key = _get_new_pandas_data_key(data)
                new_pandas_data[key] = data

        return new_pandas_data

    def load_data(self) -> Any:
        self._data_store = self.pandas_data
        self._date_index = self.update_date_index()

        if len(self._data_store.values()) > 0:
            first_data = next(iter(self._data_store.values()))
            self._timestep = first_data.timestep

        pcal = self.get_trading_days_pandas()
        self._date_index = self.clean_trading_times(self._date_index, pcal)
        for _, data in self._data_store.items():
            data.repair_times_and_fill(self._date_index)
        return pcal

    def clean_trading_times(self, dt_index: Any, pcal: Any) -> Any:
        """Fill gaps within trading days using the supplied market calendar.

        Parameters
        ----------
        dt_index : pandas.DatetimeIndex
            Original datetime index.
        pcal : pandas.DataFrame
            Calendar with ``market_open`` and ``market_close`` columns indexed by date.

        Returns
        -------
        pandas.DatetimeIndex
            Cleaned index with one-minute frequency during market hours.
        """
        # Ensure the datetime index is timezone-aware and drop duplicates
        dt_index = pd.to_datetime(dt_index)
        original_tz = dt_index.tz
        if original_tz is None:
            original_tz = LUMIBOT_DEFAULT_PYTZ
            dt_index = dt_index.tz_localize(original_tz, ambiguous="infer", nonexistent="shift_forward")

        dt_index = dt_index.sort_values().drop_duplicates()
        dt_index_utc = dt_index.tz_convert("UTC")

        # Normalize calendar boundaries to UTC to match the resample space
        pcal_utc: Any = pcal.copy()
        if isinstance(pcal_utc.index, pd.DatetimeIndex):
            calendar_index: Any = pcal_utc.index
            if getattr(calendar_index, "tz", None) is not None:
                calendar_index = calendar_index.tz_convert(original_tz).normalize().tz_localize(None)
            else:
                calendar_index = (
                    calendar_index.tz_localize(None) if hasattr(calendar_index, "tz_localize") else calendar_index
                )
            pcal_utc.index = calendar_index
        for column in ("market_open", "market_close"):
            pcal_utc[column] = pd.to_datetime(pcal_utc[column])
            if getattr(pcal_utc[column].dt, "tz", None) is None:
                pcal_utc[column] = pcal_utc[column].dt.tz_localize(
                    original_tz, ambiguous="infer", nonexistent="shift_forward"
                )
            pcal_utc[column] = pcal_utc[column].dt.tz_convert("UTC")

        # Create a DataFrame in UTC to avoid DST duplication
        df = pd.DataFrame(range(len(dt_index_utc)), index=dt_index_utc)
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()
        df["dates"] = df.index.tz_convert(original_tz).normalize().tz_localize(None)

        df = df.merge(pcal_utc[["market_open", "market_close"]], left_on="dates", right_index=True, how="left")

        if self._timestep == "minute":
            df = df.asfreq("1min", method="pad")
            mask = (df.index >= df["market_open"]) & (df.index <= df["market_close"])
            result_index = df.loc[mask].index
        else:
            result_index = df.index

        result_index = result_index.tz_convert(original_tz)
        return result_index

    def get_trading_days_pandas(self) -> Any:
        pcal = pd.DataFrame(self._date_index)

        if pcal.empty:
            # Create a dummy dataframe that spans the entire date range with market_open and market_close
            # set to 00:00:00 and 23:59:59 respectively.
            result = pd.DataFrame(
                index=pd.date_range(start=self.datetime_start, end=self.datetime_end, freq="D"),
                columns=["market_open", "market_close"],
            )
            result["market_open"] = result.index.floor("D")
            result["market_close"] = result.index.ceil("D") - pd.Timedelta("1s")
            return result

        else:
            pcal.columns = ["datetime"]
            # Normalize to date but keep as datetime64 type (not date objects)
            pcal["date"] = pcal["datetime"].dt.normalize()
            result = pcal.groupby("date").agg(
                market_open=(
                    "datetime",
                    "first",
                ),
                market_close=(
                    "datetime",
                    "last",
                ),
            )
            return result

    def get_assets(self) -> list[Any]:
        return list(self._data_store.keys())

    def get_asset_by_name(self, name: str) -> list[Any]:
        return [asset for asset in self.get_assets() if asset.name == name]

    def get_asset_by_symbol(self, symbol: str, asset_type: str | None = None) -> list[Any]:
        """Finds the assets that match the symbol. If type is specified
        finds the assets matching symbol and type.

        Parameters
        ----------
        symbol : str
            The symbol of the asset.
        asset_type : str
            Asset type. One of:
            - stock
            - future
            - option
            - forex

        Returns
        -------
        list of Asset
        """
        store_assets = self.get_assets()
        if asset_type is None:
            return [asset for asset in store_assets if asset.symbol == symbol]
        else:
            return [asset for asset in store_assets if (asset.symbol == symbol and asset.asset_type == asset_type)]

    def update_date_index(self) -> Any:
        dt_index: Any | None = None
        for _asset, data in self._data_store.items():
            if dt_index is None:
                df: Any = data.df
                dt_index = df.index
            else:
                dt_index = dt_index.join(data.df.index, how="outer")

        if dt_index is None:
            # Build a dummy index
            freq = "1min" if self._timestep == "minute" else "1D"
            dt_index = pd.date_range(start=self.datetime_start, end=self.datetime_end, freq=freq)

        else:
            if self.datetime_end < dt_index[0]:
                raise ValueError(
                    f"The ending date for the backtest was set for {self.datetime_end}. "
                    f"The earliest data entered is {dt_index[0]}. \nNo backtest can "
                    f"be run since there is no data before the backtest end date."
                )
            elif self.datetime_start > dt_index[-1]:
                raise ValueError(
                    f"The starting date for the backtest was set for {self.datetime_start}. "
                    f"The latest data entered is {dt_index[-1]}. \nNo backtest can "
                    f"be run since there is no data after the backtest start date."
                )

        return dt_index

    def get_last_price(
        self,
        asset: Asset,
        quote: Asset | None = None,
        exchange: Any | None = None,
    ) -> float | Decimal | None:
        # Takes an asset and returns the last known price
        tuple_to_find = self.find_asset_in_data_store(asset, quote)

        if tuple_to_find in self._data_store:
            # LRU tracking - mark this data as recently used
            self._data_store.move_to_end(tuple_to_find)
            data: Any = self._data_store[tuple_to_find]
            try:
                dt = self.get_datetime()
                price: Any = data.get_last_price(dt)

                # Check if price is NaN
                if pd.isna(price):
                    # Provide more specific error message for index assets
                    if hasattr(asset, "asset_type") and asset.asset_type == Asset.AssetType.INDEX:
                        logger.warning(
                            "Index asset `%s` returned a NaN price. This data source did not provide usable "
                            "index data for the requested window. Verify that index coverage and any required "
                            "subscriptions are enabled for the active provider.",
                            asset.symbol,
                        )
                    else:
                        logger.debug(f"Error getting last price for {tuple_to_find}: price is NaN")
                    return None

                return cast(float | Decimal | None, price)
            except Exception as e:
                logger.debug(f"Error getting last price for {tuple_to_find}: {e}")
                return None
        else:
            # Provide more specific error message when asset not found in data store
            if hasattr(asset, "asset_type") and asset.asset_type == Asset.AssetType.INDEX:
                logger.warning(
                    "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                    "active provider supports index data for this symbol and that any required subscriptions "
                    "are enabled.",
                    asset.symbol,
                )
            return None

    def get_quote(self, asset: Asset, quote: Asset | None = None, exchange: Any | None = None) -> Quote:
        """
        Get the latest quote for an asset.
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
            A Quote object with the quote information.
        """
        from lumibot.entities.quote import Quote

        # Takes an asset and returns the last known price
        tuple_to_find = self.find_asset_in_data_store(asset, quote)

        if tuple_to_find in self._data_store:
            # LRU tracking - mark this data as recently used
            self._data_store.move_to_end(tuple_to_find)
            data: Any = self._data_store[tuple_to_find]
            dt = self.get_datetime()
            ohlcv_bid_ask_dict: Any = data.get_quote(dt)

            # Check if ohlcv_bid_ask_dict is NaN
            if pd.isna(ohlcv_bid_ask_dict):
                logger.debug(f"Error getting ohlcv_bid_ask for {tuple_to_find}: ohlcv_bid_ask_dict is NaN")
                return Quote(asset=asset)

            # Convert dictionary to Quote object
            return Quote(
                asset=asset,
                price=ohlcv_bid_ask_dict.get("close"),
                bid=ohlcv_bid_ask_dict.get("bid"),
                ask=ohlcv_bid_ask_dict.get("ask"),
                volume=ohlcv_bid_ask_dict.get("volume"),
                timestamp=dt,
                bid_size=ohlcv_bid_ask_dict.get("bid_size"),
                ask_size=ohlcv_bid_ask_dict.get("ask_size"),
                raw_data=ohlcv_bid_ask_dict,
            )
        else:
            return Quote(asset=asset)

    def get_last_prices(
        self,
        assets: Iterable[Asset],
        quote: Asset | None = None,
        exchange: Any | None = None,
        **kwargs: Any,
    ) -> dict[Asset, float | Decimal | None]:
        result: dict[Asset, float | Decimal | None] = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)
        return result

    def _get_polars_data_entry(self, asset: Asset, quote: Asset | None, timestep: str) -> Any | None:
        """Retrieve a cached DataPolars entry for a specific timestep if available."""
        polars_cache = cast(dict[Any, Any], getattr(self, "_polars_data", {}))

        # Build candidate quotes: exact match first, then USD fallback (default storage)
        quote_candidates: list[Asset] = []
        if quote is not None:
            quote_candidates.append(quote)
        quote_candidates.append(_USD_FOREX)

        for candidate_quote in quote_candidates:
            key = (asset, candidate_quote, timestep)
            entry = polars_cache.get(key)
            if entry is not None:
                return entry

        # Final attempt: linear scan to cope with differing Asset instances
        for (cached_asset, cached_quote, cached_timestep), entry in polars_cache.items():
            if cached_asset == asset and cached_timestep == timestep:
                if quote is None or cached_quote == quote:
                    return entry
        return None

    def find_asset_in_data_store(
        self,
        asset: Asset | Any,
        quote: Asset | None = None,
        timestep: str | None = None,
    ) -> Any | None:
        """
        Locate the cache key for an asset, preferring timestep-aware keys but
        gracefully falling back to legacy (asset, quote) entries for backward
        compatibility.
        """
        candidates: list[Any] = []

        if timestep is not None:
            base_quote = quote if quote is not None else _USD_FOREX
            candidates.append((asset, base_quote, timestep))
            # If a quote was explicitly supplied, also consider the USD fallback to
            # match historical cache entries that were stored with USD.
            if quote is not None:
                candidates.append((asset, _USD_FOREX, timestep))

        if quote is not None:
            candidates.append((asset, quote))

        if isinstance(asset, Asset):
            candidates.append((asset, _USD_FOREX))

        candidates.append(asset)

        for key in candidates:
            if key in self._data_store:
                return key
        return None

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "",
        timeshift: int | timedelta | None = 0,
        quote: Asset | None = None,
        exchange: Any | None = None,
        include_after_hours: bool = True,
    ) -> Any | None:
        timestep = timestep if timestep else self.MIN_TIMESTEP
        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for PandasData, but {exchange} was passed as the exchange"
            )

        if not timeshift:
            timeshift = 0

        asset_to_find = self.find_asset_in_data_store(asset, quote, timestep)

        if asset_to_find in self._data_store:
            # LRU tracking - mark this data as recently used
            self._data_store.move_to_end(asset_to_find)
            data: Any = self._data_store[asset_to_find]
        else:
            if hasattr(asset, "asset_type") and asset.asset_type == Asset.AssetType.INDEX:
                logger.warning(
                    "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                    "active provider supports index data for this symbol and that any required subscriptions "
                    "are enabled.",
                    asset.symbol,
                )
            else:
                logger.warning(f"The asset: `{asset}` does not exist or does not have data.")
            return

        desired_timestep = timestep

        # Prefer a direct DataPolars match for the requested timestep (if available) to
        # avoid aggregating from trimmed minute windows.
        current_timestep = getattr(data, "timestep", None)
        if desired_timestep and current_timestep != desired_timestep:
            direct_match = self._get_polars_data_entry(asset, quote, desired_timestep)
            if direct_match is not None:
                data = direct_match
                current_timestep = data.timestep

        # OPTIMIZATION: Use aggregated bars cache for different timesteps
        # This avoids re-aggregating 5m/15m/1h bars from minute data every iteration
        source_timestep = current_timestep
        can_aggregate = (
            source_timestep == "minute"
            and timestep != source_timestep
            and hasattr(data, "polars_df")  # Only for DataPolars objects
            and timestep in ["5 minutes", "15 minutes", "30 minutes", "hour", "2 hours", "4 hours", "day"]
        )

        if can_aggregate:
            # Try to get aggregated bars from cache
            aggregated_df: Any | None = self._get_or_aggregate_bars(
                asset, quote, length, str(source_timestep), timestep
            )
            if aggregated_df is not None:
                # We have aggregated data - now filter and tail it like get_bars would
                pl_module: Any = import_module("polars")

                now = self.get_datetime()
                # Apply timeshift if specified
                # CRITICAL: Integer timeshift represents BAR offsets, not minute deltas!
                # Must calculate adjustment based on the actual timestep being requested.
                if timeshift:
                    if isinstance(timeshift, int):
                        # Calculate timedelta for one bar of this timestep
                        timestep_delta, _ = self.convert_timestep_str_to_timedelta(timestep)
                        # Multiply by timeshift to get total adjustment
                        # Example: timestep="5 minutes", timeshift=-2 → adjustment = -10 minutes
                        now = now + (timestep_delta * timeshift)
                    else:
                        # Timeshift is already a timedelta - use it directly
                        now = now + timeshift

                # Filter to current time and take last 'length' bars
                # Convert now to match polars DataFrame timezone
                import pytz

                if now.tzinfo is None:
                    now_aware = pytz.utc.localize(now)
                else:
                    now_aware = now

                polars_tz: Any | None = getattr(aggregated_df["datetime"].dtype, "time_zone", None)
                if polars_tz:
                    now_compat = pd.Timestamp(now_aware).tz_convert(polars_tz)
                else:
                    now_compat = now_aware

                filtered: Any = aggregated_df.filter(pl_module.col("datetime") <= now_compat)
                result: Any = filtered.tail(length)

                if result.height >= length:
                    logger.debug(f"[AGG CACHE] {asset.symbol} {timestep}: returning {result.height} bars from cache")
                    return result

                # Aggregated slice is insufficient—evict this cache entry and try to fall back
                logger.warning(
                    "[AGG CACHE] %s %s: insufficient rows (requested=%s, filtered=%s, returning=%s); falling back",
                    asset.symbol,
                    timestep,
                    length,
                    filtered.height,
                    result.height,
                )
                cache_key = self._get_aggregation_cache_key(asset, quote, timestep)
                self._aggregated_cache.pop(cache_key, None)

                direct_match = self._get_polars_data_entry(asset, quote, timestep)
                if direct_match is not None:
                    data = direct_match
                    source_timestep = data.timestep
                # Fall through to regular get_bars

        # Regular path - use data.get_bars() which handles timestep conversion internally
        now = self.get_datetime()

        try:
            res = data.get_bars(now, length=length, timestep=timestep, timeshift=timeshift)
        # Return None if data.get_bars returns a ValueError
        except ValueError as e:
            logger.debug(f"Error getting bars for {asset}: {e}")
            return None

        return res

    def _pull_source_symbol_bars_between_dates(
        self,
        asset: Asset,
        timestep: str = "",
        quote: Asset | None = None,
        exchange: Any | None = None,
        include_after_hours: bool = True,
        start_date: Any | None = None,
        end_date: Any | None = None,
    ) -> Any | None:
        """Pull all bars for an asset"""
        timestep = timestep if timestep else self.MIN_TIMESTEP
        asset_to_find = self.find_asset_in_data_store(asset, quote)

        if asset_to_find in self._data_store:
            # LRU tracking - mark this data as recently used
            self._data_store.move_to_end(asset_to_find)
            data: Any = self._data_store[asset_to_find]
        else:
            if hasattr(asset, "asset_type") and asset.asset_type == Asset.AssetType.INDEX:
                logger.warning(
                    "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                    "active provider supports index data for this symbol and that any required subscriptions "
                    "are enabled.",
                    asset.symbol,
                )
            else:
                logger.warning(f"The asset: `{asset}` does not exist or does not have data.")
            return

        try:
            res = data.get_bars_between_dates(start_date=start_date, end_date=end_date, timestep=timestep)
        # Return None if data.get_bars returns a ValueError
        except ValueError as e:
            logger.debug(f"Error getting bars for {asset}: {e}")
            res = None
        return res

    def _pull_source_bars(
        self,
        assets: Iterable[Asset],
        length: int,
        timestep: str = "",
        timeshift: int | timedelta | None = None,
        quote: Asset | None = None,
        include_after_hours: bool = True,
    ) -> dict[Asset, Any]:
        """pull broker bars for a list assets"""
        timestep = timestep if timestep else self.MIN_TIMESTEP
        self._parse_source_timestep(timestep, reverse=True)

        result: dict[Asset, Any] = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift, quote=quote
            )
            # remove assets that have no data from the result
            if result[asset] is None:
                result.pop(asset)

        return result

    def _parse_source_symbol_bars(
        self,
        response: Any,
        asset: Asset | tuple[Any, Any],
        quote: Asset | None = None,
        length: int | None = None,
        return_polars: bool = False,
    ) -> Any:
        """parse broker response for a single asset

        CRITICAL: return_polars defaults to False for backwards compatibility.
        Existing strategies expect pandas DataFrames!
        """
        asset1: Any = asset
        asset2: Any = quote
        if isinstance(asset, tuple):
            asset1, asset2 = asset
        bars = _bars_class()(response, self.SOURCE, asset1, quote=asset2, raw=response, return_polars=return_polars)
        return bars

    def get_yesterday_dividend(self, asset: Asset, quote: Asset | None = None) -> None:
        pass

    def get_yesterday_dividends(self, assets: Iterable[Asset], quote: Asset | None = None) -> None:
        pass

    # =======Options methods.=================
    def get_chains(
        self,
        asset: Asset,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> dict[str, Any]:
        """Returns option chains.

        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset object
            The stock whose option chain is being fetched. Represented
            as an asset object.
        quote : Asset object, optional
            The quote asset. Default is None.
        exchange : str, optional
            The exchange to fetch the option chains from. For PandasData, will only use "SMART".

        Returns
        -------
        dict
            Mapping with keys such as ``Multiplier`` (e.g. ``"100"``) and ``Chains``.
            ``Chains`` is a nested dictionary where expiration dates map to strike lists,
            e.g. ``chains['Chains']['CALL']['2023-07-31'] = [strike1, strike2, ...]``.
        """
        chains: dict[str, Any] = dict(
            Multiplier=100,
            Exchange="SMART",
            Chains={"CALL": defaultdict[str, list[Any]](list), "PUT": defaultdict[str, list[Any]](list)},
        )

        for store_item, _data in self._data_store.items():
            if not isinstance(store_item, tuple) or not store_item:
                continue
            store_asset: Any = cast(tuple[Any, ...], store_item)[0]
            if store_asset.asset_type != "option":
                continue
            if store_asset.symbol != asset.symbol:
                continue
            chains["Chains"][store_asset.right][store_asset.expiration].append(store_asset.strike)

        return chains

    def get_start_datetime_and_ts_unit(
        self,
        length: int,
        timestep: str,
        start_dt: Any | None = None,
        start_buffer: timedelta = timedelta(days=5),
    ) -> tuple[Any, str]:
        """
        Get the start datetime for the data.

        Parameters
        ----------
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".


        Returns
        -------
        datetime
            The start datetime.
        str
            The timestep unit.
        """
        # Convert timestep string to timedelta and get start datetime
        td, ts_unit = cast(tuple[timedelta, str], self.convert_timestep_str_to_timedelta(timestep))

        if ts_unit == "day":
            weeks_requested = length // 5  # Full trading week is 5 days
            extra_padding_days = weeks_requested * 3  # to account for 3day weekends
            td = timedelta(days=length + extra_padding_days)
        else:
            td *= length

        if start_dt is not None:
            start_datetime = start_dt - td
        else:
            start_datetime = self.datetime_start - td

        # Subtract an extra 5 days to the start datetime to make sure we have enough
        # data when it's a sparsely traded asset, especially over weekends
        start_datetime = start_datetime - start_buffer

        return start_datetime, ts_unit

    def get_historical_prices(
        self,
        asset: Asset | str,
        length: int,
        timestep: str | None = None,
        timeshift: int | timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        # PolarsData supports return_polars to enable polars-backed Bars for performance.
        # When True, returns Bars with polars DataFrame internally (lazy conversion to pandas).
        # CRITICAL: Default MUST be False for backwards compatibility with existing strategies!
        return_polars: bool = False,
    ) -> Any:
        """Get bars for a given asset"""
        # Periodically trim cached data to maintain sliding window
        self._trim_cached_data()

        # Enforce memory limits after trimming (same periodic frequency)
        # This ensures total memory usage stays under 1GB cap
        self._enforce_memory_limits()

        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        timestep_value = timestep or str(self.get_timestep())
        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep_value,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length, return_polars=return_polars)
        return bars
