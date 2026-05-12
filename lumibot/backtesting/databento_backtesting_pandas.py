from __future__ import annotations

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportMissingTypeArgument=false
# pyright: reportConstantRedefinition=false, reportInvalidTypeForm=false, reportOptionalMemberAccess=false
# pyright: reportUnnecessaryComparison=false, reportGeneralTypeIssues=false, reportArgumentType=false
import traceback
from datetime import datetime, timedelta
from importlib import import_module
from types import ModuleType
from typing import Any, TypeAlias, cast

from termcolor import colored

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources.pandas_data import PandasData
from lumibot.entities.asset import Asset
from lumibot.tools.helpers import to_datetime_aware
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)
AssetInput: TypeAlias = Asset | tuple[Asset, Asset]  # noqa: UP040
SearchAsset: TypeAlias = tuple[Asset, Asset]  # noqa: UP040

START_BUFFER = timedelta(days=5)


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

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._load(), name, value)

    def __delattr__(self, name: str) -> None:
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


pd = _LazyModule("pandas")
np = _LazyModule("numpy")
databento_helper = _LazyModule("lumibot.tools.databento_helper")
_data_class_cache: type[Any] | None = None
_databento_auth_error_class_cache: type[Exception] | None = None


def _data_class() -> type[Any]:
    global _data_class_cache
    if _data_class_cache is None:
        from lumibot.entities import Data

        _data_class_cache = Data
    assert _data_class_cache is not None
    return _data_class_cache


def _databento_auth_error_class() -> type[Exception]:
    global _databento_auth_error_class_cache
    if _databento_auth_error_class_cache is None:
        from lumibot.tools.databento_helper import DataBentoAuthenticationError

        _databento_auth_error_class_cache = DataBentoAuthenticationError
    return _databento_auth_error_class_cache


class DataBentoDataBacktestingPandas(PandasData):
    """
    Backtesting implementation of DataBento data source

    This class extends PandasData to provide DataBento-specific backtesting functionality,
    including data retrieval, caching, and time-based filtering for historical simulations.
    """

    def __init__(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        pandas_data: dict[Any, Any] | list[Any] | None = None,
        api_key: str | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> None:
        """
        Initialize DataBento backtesting data source

        Parameters
        ----------
        datetime_start : datetime
            Start datetime for backtesting period
        datetime_end : datetime
            End datetime for backtesting period
        pandas_data : dict, optional
            Pre-loaded pandas data
        api_key : str
            DataBento API key
        timeout : int, optional
            API request timeout in seconds, default 30
        max_retries : int, optional
            Maximum number of API retry attempts, default 3
        **kwargs
            Additional parameters passed to parent class
        """
        super().__init__(
            datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, api_key=api_key, **kwargs
        )

        # Store DataBento-specific configuration
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries

        # Track which assets we've already fetched to avoid redundant requests
        self._prefetched_assets: set[Any] = set()
        # Track data requests to avoid repeated log messages
        self._logged_requests: set[str] = set()

        # OPTIMIZATION: Iteration-level caching to avoid redundant filtering
        # Cache filtered DataFrames per iteration (datetime)
        self._filtered_bars_cache: dict[Any, Any] = {}  # {(asset_key, length, timestep, timeshift, dt): DataFrame}
        self._last_price_cache: dict[Any, float] = {}  # {(asset_key, dt): price}
        self._cache_datetime: datetime | None = None  # Track when to invalidate cache

        # Track which futures assets we've fetched multipliers for (to avoid redundant API calls)
        self._multiplier_fetched_assets: set[Any] = set()
        # Cache UTC nanosecond datetime indexes by asset/timestep for positional slicing.
        self._datetime_ns_cache: dict[Any, Any] = {}

        # Verify DataBento availability
        if not databento_helper.DATABENTO_AVAILABLE:
            logger.error("DataBento package not available. Please install with: pip install databento")
            raise ImportError("DataBento package not available")

        logger.debug(f"DataBento backtesting initialized for period: {datetime_start} to {datetime_end}")

    def _check_and_clear_cache(self) -> None:
        """
        OPTIMIZATION: Clear iteration caches when datetime changes.
        This ensures fresh filtering for each new iteration while reusing
        results within the same iteration.
        """
        current_dt = self.get_datetime()
        if self._cache_datetime != current_dt:
            self._filtered_bars_cache.clear()
            self._last_price_cache.clear()
            self._cache_datetime = current_dt

    @staticmethod
    def _split_asset(asset: AssetInput, quote: Asset | None = None) -> tuple[SearchAsset, Asset, Asset]:
        quote_asset = quote if quote is not None else Asset("USD", "forex")
        if isinstance(asset, tuple):
            asset_separated, tuple_quote = asset
            return (asset_separated, tuple_quote), asset_separated, tuple_quote
        return (asset, quote_asset), asset, quote_asset

    @staticmethod
    def _bar_delta(timestep: str) -> timedelta:
        if timestep == "hour":
            return timedelta(hours=1)
        if timestep == "day":
            return timedelta(days=1)
        return timedelta(minutes=1)

    @staticmethod
    def _apply_timeshift(current_dt: datetime, timeshift: int | timedelta | None) -> tuple[datetime, float]:
        if not timeshift:
            return current_dt, 0.0
        if isinstance(timeshift, int):
            return current_dt - timedelta(minutes=timeshift), float(timeshift * 60)
        return current_dt - timeshift, float(timeshift.total_seconds())

    def _cache_datetime_series(self, search_asset: SearchAsset, data_obj: Any) -> None:
        """Cache the data index as UTC nanoseconds for fast repeated slicing."""
        try:
            timestep = getattr(data_obj, "timestep", "minute")
            cache_key = (search_asset, timestep)
            df_index = data_obj.df.index
            if getattr(df_index, "tz", None) is None:
                df_index = df_index.tz_localize(LUMIBOT_DEFAULT_PYTZ)
            else:
                df_index = df_index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            utc_index = df_index.tz_convert("UTC").tz_localize(None)
            self._datetime_ns_cache[cache_key] = pd.DatetimeIndex(utc_index).astype("datetime64[ns]").view("int64")
        except Exception as exc:
            logger.debug(f"Failed to cache datetime series for {search_asset}: {exc}")

    @staticmethod
    def _datetime_to_utc_ns(dt_obj: datetime) -> int:
        ts = pd.Timestamp(dt_obj)
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return int(ts.value)

    def _datetime_ns_for(self, search_asset: SearchAsset, data_obj: Any) -> Any | None:
        cache_key = (search_asset, data_obj.timestep)
        datetime_ns = self._datetime_ns_cache.get(cache_key)
        if datetime_ns is None:
            self._cache_datetime_series(search_asset, data_obj)
            datetime_ns = self._datetime_ns_cache.get(cache_key)
        return datetime_ns

    def _last_close_from_data(self, search_asset: SearchAsset, data_obj: Any, current_dt: datetime) -> float | None:
        df = data_obj.df
        datetime_ns = self._datetime_ns_for(search_asset, data_obj)
        if df.empty or "close" not in df.columns or datetime_ns is None or len(datetime_ns) == 0:
            return None

        current_dt_aware = to_datetime_aware(current_dt)
        cutoff_dt = current_dt_aware - self._bar_delta(data_obj.timestep)
        last_pos = int(np.searchsorted(datetime_ns, self._datetime_to_utc_ns(cutoff_dt), side="right")) - 1

        if last_pos < 0:
            last_pos = int(np.searchsorted(datetime_ns, self._datetime_to_utc_ns(current_dt_aware), side="right")) - 1

        if last_pos < 0:
            return None

        valid_closes = df["close"].iloc[: last_pos + 1].dropna()
        if valid_closes.empty:
            return None
        return float(valid_closes.iloc[-1])

    def _slice_cached_bars(
        self,
        search_asset: SearchAsset,
        data_obj: Any,
        *,
        length: int,
        current_dt: datetime,
        timeshift: int | timedelta | None = None,
        include_current: bool = False,
        step_back_for_positive_shift: bool = False,
    ) -> Any | None:
        df = data_obj.df
        datetime_ns = self._datetime_ns_for(search_asset, data_obj)
        if df.empty or datetime_ns is None or len(datetime_ns) == 0:
            return None

        shifted_dt, shift_seconds = self._apply_timeshift(current_dt, timeshift)
        current_dt_aware = to_datetime_aware(shifted_dt)
        allow_current = include_current or shift_seconds > 0
        target_dt = current_dt_aware
        if step_back_for_positive_shift and shift_seconds > 0:
            target_dt = current_dt_aware - self._bar_delta(data_obj.timestep)

        side = "right" if allow_current else "left"
        last_pos = int(np.searchsorted(datetime_ns, self._datetime_to_utc_ns(target_dt), side=side)) - 1
        if last_pos < 0:
            return None

        start_pos = max(0, last_pos - (int(length) - 1))
        result_df = df.iloc[start_pos : last_pos + 1]
        return None if result_df.empty else result_df

    def _ensure_futures_multiplier(self, asset: Asset) -> None:
        """
        Ensure futures asset has correct multiplier set.

        This method is idempotent and cached - safe to call multiple times.
        Only fetches multiplier once per unique asset.

        Design rationale:
        - Futures multipliers must be fetched from data provider (e.g., DataBento)
        - Asset class defaults to multiplier=1
        - Data source is responsible for updating multiplier on first use
        - Lazy fetching is more efficient than prefetching all possible assets

        Parameters
        ----------
        asset : Asset
            The asset to ensure has correct multiplier
        """
        # Skip if not a futures asset
        if asset.asset_type not in (Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE):
            return

        # Skip if multiplier already set to non-default value
        if asset.multiplier != 1:
            return

        # Create cache key to track which assets we've already processed
        # Use symbol + asset_type + expiration to handle different contracts
        cache_key = (asset.symbol, asset.asset_type, getattr(asset, "expiration", None))

        # Check if we already tried to fetch for this asset
        if cache_key in self._multiplier_fetched_assets:
            return  # Already attempted (even if failed, don't retry every time)

        # Mark as attempted to avoid redundant API calls
        self._multiplier_fetched_assets.add(cache_key)

        # Fetch and set multiplier from DataBento
        try:
            client = databento_helper.DataBentoClient(self._api_key)

            # Resolve symbol based on asset type
            if asset.asset_type == Asset.AssetType.CONT_FUTURE:
                resolved_symbol = databento_helper._format_futures_symbol_for_databento(
                    asset, reference_date=self.datetime_start
                )
            else:
                resolved_symbol = databento_helper._format_futures_symbol_for_databento(asset)

            # Fetch multiplier from DataBento instrument definition
            databento_helper._fetch_and_update_futures_multiplier(
                client=client,
                asset=asset,
                resolved_symbol=resolved_symbol,
                dataset="GLBX.MDP3",
                reference_date=self.datetime_start,
            )

            logger.debug(f"Successfully set multiplier for {asset.symbol}: {asset.multiplier}")

        except _databento_auth_error_class() as e:
            logger.error(
                colored(f"DataBento authentication failed while fetching multiplier for {asset.symbol}: {e}", "red")
            )
            raise
        except Exception as e:
            logger.warning(f"Could not fetch multiplier for {asset.symbol}: {e}")

    def prefetch_data(self, assets: list[Asset], timestep: str = "minute") -> None:
        """
        Prefetch all required data for the specified assets for the entire backtest period.
        This reduces redundant API calls and log spam during backtesting.

        Parameters
        ----------
        assets : list of Asset
            List of assets to prefetch data for
        timestep : str, optional
            Timestep to fetch (default: "minute")
        """
        if not assets:
            return

        logger.debug(f"Prefetching DataBento data for {len(assets)} assets...")

        for asset in assets:
            # Create search key for the asset
            quote_asset = Asset("USD", "forex")
            search_asset = (asset, quote_asset)

            # Skip if already prefetched
            if search_asset in self._prefetched_assets:
                continue

            try:
                # Calculate start with buffer for better data coverage
                start_datetime = self.datetime_start - START_BUFFER
                end_datetime = self.datetime_end + timedelta(days=1)

                logger.debug(f"Fetching {asset.symbol} data from {start_datetime.date()} to {end_datetime.date()}")

                # Get data from DataBento for entire period
                df = databento_helper.get_price_data_from_databento(
                    api_key=self._api_key,
                    asset=asset,
                    start=start_datetime,
                    end=end_datetime,
                    timestep=timestep,
                    venue=None,
                    force_cache_update=False,
                )

                if df is None or df.empty:
                    # For empty data, create an empty Data object with proper timezone handling
                    empty_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
                    # Create an empty DatetimeIndex with proper timezone
                    empty_df.index = pd.DatetimeIndex([], tz=LUMIBOT_DEFAULT_PYTZ, name="datetime")

                    data_obj = _data_class()(
                        asset,
                        df=empty_df,
                        timestep=timestep,
                        quote=quote_asset,
                        # Explicitly set dates to avoid timezone issues
                        date_start=None,
                        date_end=None,
                    )
                    self.pandas_data[search_asset] = data_obj
                    self._cache_datetime_series(search_asset, data_obj)
                else:
                    # Create Data object and store
                    data_obj = _data_class()(
                        asset,
                        df=df,
                        timestep=timestep,
                        quote=quote_asset,
                    )
                    self.pandas_data[search_asset] = data_obj
                    self._cache_datetime_series(search_asset, data_obj)
                    logger.debug(f"Cached {len(df)} rows for {asset.symbol}")

                # Mark as prefetched
                self._prefetched_assets.add(search_asset)

            except _databento_auth_error_class() as e:
                logger.error(colored(f"DataBento authentication failed while prefetching {asset.symbol}: {e}", "red"))
                raise
            except Exception as e:
                logger.error(f"Error prefetching data for {asset.symbol}: {str(e)}")
                logger.error(traceback.format_exc())

    def _update_pandas_data(
        self,
        asset: AssetInput,
        quote: Asset | None,
        length: int,
        timestep: str,
        start_dt: datetime | None = None,
    ) -> None:
        """
        Get asset data and update the self.pandas_data dictionary.

        This method retrieves historical data from DataBento and caches it for backtesting use.
        If data has already been prefetched, it skips redundant API calls.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. For DataBento, this is typically not used.
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "minute", "hour", or "day".
        start_dt : datetime, optional
            The start datetime to use. If None, the current self.datetime_start will be used.
        """
        search_asset, asset_separated, quote_asset = self._split_asset(asset, quote)

        # Ensure futures have correct multiplier set
        self._ensure_futures_multiplier(asset_separated)

        # If this asset was already prefetched, we don't need to do anything
        if search_asset in self._prefetched_assets:
            return

        # Check if we already have adequate data for this asset
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df

            # Only check if we have actual data (not empty DataFrame)
            if not asset_data_df.empty and len(asset_data_df.index) > 0:
                data_start_datetime = asset_data_df.index[0]
                data_end_datetime = asset_data_df.index[-1]

                # Get the timestep of the existing data
                data_timestep = asset_data.timestep

                # If the timestep matches, check if we have sufficient coverage
                if data_timestep == timestep:
                    # Ensure both datetimes are timezone-aware for comparison
                    data_start_tz = to_datetime_aware(data_start_datetime)
                    data_end_tz = to_datetime_aware(data_end_datetime)

                    # Get the start datetime with buffer
                    start_datetime, _ = self.get_start_datetime_and_ts_unit(
                        length, timestep, start_dt, start_buffer=START_BUFFER
                    )
                    start_tz = to_datetime_aware(start_datetime)

                    # Check if existing data covers the needed time range with buffer
                    needed_start = start_tz - START_BUFFER
                    needed_end = self.datetime_end

                    if data_start_tz <= needed_start and data_end_tz >= needed_end:
                        # Data is already sufficient - return silently
                        return

        # We need to fetch new data from DataBento
        # Create a unique key for logging to avoid spam
        log_key = f"{asset_separated.symbol}_{timestep}"

        try:
            # Only log fetch message once per asset/timestep combination
            if log_key not in self._logged_requests:
                logger.debug(f"Fetching {timestep} data for {asset_separated.symbol}")
                self._logged_requests.add(log_key)

            # Get the start datetime and timestep unit
            start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
                length, timestep, start_dt, start_buffer=START_BUFFER
            )

            # Calculate end datetime (use current backtest end or a bit beyond)
            end_datetime = self.datetime_end + timedelta(days=1)

            # Get data from DataBento
            df = databento_helper.get_price_data_from_databento(
                api_key=self._api_key,
                asset=asset_separated,
                start=start_datetime,
                end=end_datetime,
                timestep=ts_unit,
                venue=None,  # Could add venue support later
                force_cache_update=False,
            )

            if df is None or df.empty:
                # For empty data, create an empty Data object with proper timezone handling
                # to maintain backward compatibility with tests
                empty_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
                # Create an empty DatetimeIndex with proper timezone
                empty_df.index = pd.DatetimeIndex([], tz=LUMIBOT_DEFAULT_PYTZ, name="datetime")

                data_obj = _data_class()(
                    asset_separated,
                    df=empty_df,
                    timestep=ts_unit,
                    quote=quote_asset,
                    # Use timezone-aware dates to avoid timezone issues
                    date_start=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2000, 1, 1)),
                    date_end=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2000, 1, 1)),
                )
                self.pandas_data[search_asset] = data_obj
                self._cache_datetime_series(search_asset, data_obj)
                return

            # Ensure the DataFrame has a datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                logger.error(f"DataBento data for {asset_separated.symbol} doesn't have datetime index")
                return

            # Create Data object and store in pandas_data
            data_obj = _data_class()(
                asset_separated,
                df=df,
                timestep=ts_unit,
                quote=quote_asset,
            )

            self.pandas_data[search_asset] = data_obj
            self._cache_datetime_series(search_asset, data_obj)

        except _databento_auth_error_class() as e:
            logger.error(colored(f"DataBento authentication failed for {asset_separated.symbol}: {e}", "red"))
            raise
        except Exception as e:
            logger.error(f"Error updating pandas data for {asset_separated.symbol}: {str(e)}")
            logger.error(traceback.format_exc())

    def get_last_price(
        self,
        asset: AssetInput,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> float | None:
        """
        Get the last price for an asset at the current backtest time

        Parameters
        ----------
        asset : Asset
            Asset to get the price for
        quote : Asset, optional
            Quote asset (not typically used with DataBento)
        exchange : str, optional
            Exchange filter

        Returns
        -------
        float, Decimal, or None
            Last price at current backtest time
        """
        asset_separated = asset[0] if isinstance(asset, tuple) else asset
        try:
            # OPTIMIZATION: Check cache first
            self._check_and_clear_cache()
            current_dt = self.get_datetime()
            current_dt_aware = to_datetime_aware(current_dt)

            # Try to get data from our cached pandas_data first
            search_asset, asset_separated, quote_asset = self._split_asset(asset, quote)

            # Ensure futures have correct multiplier set
            self._ensure_futures_multiplier(asset_separated)

            # OPTIMIZATION: Check iteration cache
            cache_key = (search_asset, current_dt)
            if cache_key in self._last_price_cache:
                return self._last_price_cache[cache_key]

            if search_asset in self.pandas_data:
                asset_data = self.pandas_data[search_asset]
                price = self._last_close_from_data(search_asset, asset_data, current_dt)
                if price is not None:
                    self._last_price_cache[cache_key] = price
                    return price

            # If no cached data, try to load it for the backtest window
            try:
                fetched_bars = self.get_historical_prices(
                    asset_separated,
                    length=1,
                    quote=quote_asset,
                    timestep="minute",
                )
                if fetched_bars is not None:
                    asset_data = self.pandas_data.get(search_asset)
                    if asset_data is not None:
                        price = self._last_close_from_data(search_asset, asset_data, current_dt)
                        if price is not None:
                            self._last_price_cache[cache_key] = price
                            return price
            except Exception as exc:
                logger.debug(
                    "Attempted to hydrate Databento cache for %s but hit error: %s",
                    asset_separated.symbol,
                    exc,
                )

            # If still no data, fall back to direct fetch (live-style)
            logger.warning(f"No cached data for {asset_separated.symbol}, attempting direct fetch")
            return databento_helper.get_last_price_from_databento(
                api_key=self._api_key, asset=asset_separated, venue=exchange, reference_date=current_dt_aware
            )

        except _databento_auth_error_class() as e:
            logger.error(
                colored(
                    f"DataBento authentication failed while getting last price for {asset_separated.symbol}: {e}",
                    "red",
                )
            )
            raise
        except Exception as e:
            logger.error(f"Error getting last price for {asset}: {e}")
            return None

    def get_chains(
        self,
        asset: Asset,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> dict[str, Any]:
        """
        Get option chains for an asset

        DataBento doesn't provide options chain data, so this returns an empty dict.

        Parameters
        ----------
        asset : Asset
            Asset to get chains for
        quote : Asset, optional
            Quote asset

        Returns
        -------
        dict
            Empty dictionary
        """
        logger.warning("DataBento does not provide options chain data")
        return {}

    def _get_bars_dict(
        self,
        assets: list[Asset],
        length: int,
        timestep: str,
        timeshift: int | timedelta | None = None,
    ) -> dict[Asset, Any | None]:
        """
        Override parent method to handle DataBento-specific data retrieval

        Parameters
        ----------
        assets : list
            List of assets to get data for
        length : int
            Number of bars to retrieve
        timestep : str
            Timestep for the data
        timeshift : timedelta, optional
            Time shift to apply

        Returns
        -------
        dict
            Dictionary mapping assets to their bar data
        """
        result: dict[Asset, Any | None] = {}

        for asset in assets:
            try:
                # Update pandas data if needed
                self._update_pandas_data(asset, None, length, timestep)

                search_asset, _, _ = self._split_asset(asset)

                if search_asset in self.pandas_data:
                    asset_data = self.pandas_data[search_asset]
                    result_df = self._slice_cached_bars(
                        search_asset,
                        asset_data,
                        length=length,
                        current_dt=self.get_datetime(),
                        timeshift=timeshift,
                        include_current=bool(getattr(self, "_include_current_bar_for_orders", False)),
                    )

                    if result_df is not None:
                        result[asset] = result_df
                    else:
                        logger.warning(f"No data available for {asset.symbol} at {self.get_datetime()}")
                        result[asset] = None
                else:
                    logger.warning(f"No data found for {asset.symbol}")
                    result[asset] = None

            except _databento_auth_error_class() as e:
                logger.error(colored(f"DataBento authentication failed while getting bars for {asset}: {e}", "red"))
                raise
            except Exception as e:
                logger.error(f"Error getting bars for {asset}: {e}")
                result[asset] = None

        return result

    def _pull_source_symbol_bars(
        self,
        asset: AssetInput,
        length: int,
        timestep: str = "",
        timeshift: int | timedelta | None = 0,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> Any | None:
        """
        Override parent method to fetch data from DataBento instead of pre-loaded data store

        This method is called by get_historical_prices and is responsible for actually
        fetching the data from the DataBento API.
        """
        timestep = timestep if timestep else "minute"

        # OPTIMIZATION: Check iteration cache first
        self._check_and_clear_cache()
        current_dt = self.get_datetime()

        # Get data from our cached pandas_data
        search_asset, asset_separated, _ = self._split_asset(asset, quote)

        # OPTIMIZATION: Build cache key and check cache
        # Convert timeshift to consistent format for caching
        timeshift_key = 0
        if timeshift:
            if isinstance(timeshift, int):
                timeshift_key = timeshift
            else:
                timeshift_key = int(timeshift.total_seconds() / 60)

        cache_key = (search_asset, length, timestep, timeshift_key, current_dt)
        if cache_key in self._filtered_bars_cache:
            return self._filtered_bars_cache[cache_key]

        # Check if we need to fetch data by calling _update_pandas_data first
        # This will only fetch if data is not already cached or prefetched
        self._update_pandas_data(asset, quote, length, timestep)

        # Check if we have data in pandas_data cache
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            result_df = self._slice_cached_bars(
                search_asset,
                asset_data,
                length=length,
                current_dt=current_dt,
                timeshift=timeshift,
                step_back_for_positive_shift=True,
            )

            if result_df is not None:
                shifted_dt, shift_seconds = self._apply_timeshift(current_dt, timeshift)
                logger.debug(
                    f"[TIMESHIFT_PANDAS] asset={asset_separated.symbol} broker_dt={self.get_datetime()} "
                    f"timeshift={timeshift} shift_seconds={shift_seconds} "
                    f"shifted_dt={to_datetime_aware(shifted_dt)} returned_bar={result_df.index[-1]}"
                )
                self._filtered_bars_cache[cache_key] = result_df
                return result_df

            self._filtered_bars_cache[cache_key] = None
            return None
        else:
            return None

    def initialize_data_for_backtest(self, strategy_assets: list[Asset | str], timestep: str = "minute") -> None:
        """
        Convenience method to prefetch all required data for a backtest strategy.
        This should be called during strategy initialization to load all data up front.

        Parameters
        ----------
        strategy_assets : list of Asset or list of str
            List of assets or asset symbols that the strategy will use
        timestep : str, optional
            Primary timestep for the data (default: "minute")
        """
        # Convert string symbols to Asset objects if needed
        assets = []
        for asset in strategy_assets:
            if isinstance(asset, str):
                # Try to determine asset type from symbol format
                if any(month in asset for month in ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]):
                    # Looks like a futures symbol
                    assets.append(Asset(asset, "future"))
                else:
                    # Default to stock
                    assets.append(Asset(asset, "stock"))
            else:
                assets.append(asset)

        # Prefetch data for all assets
        self.prefetch_data(assets, timestep)

        logger.debug(f"Initialized DataBento backtesting with prefetched data for {len(assets)} assets")
