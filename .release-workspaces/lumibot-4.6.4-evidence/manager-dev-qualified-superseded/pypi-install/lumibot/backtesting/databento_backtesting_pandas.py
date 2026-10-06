import traceback
from datetime import datetime, timedelta

import pandas as pd

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import databento_helper
from lumibot.tools.databento_helper import DataBentoAuthenticationError
from lumibot.tools.helpers import to_datetime_aware
from termcolor import colored

from lumibot.tools.lumibot_logger import get_logger
logger = get_logger(__name__)

START_BUFFER = timedelta(days=5)


class DataBentoDataBacktestingPandas(PandasData):
    """
    Backtesting implementation of DataBento data source
    
    This class extends PandasData to provide DataBento-specific backtesting functionality,
    including data retrieval, caching, and time-based filtering for historical simulations.
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        timeout=30,
        max_retries=3,
        **kwargs,
    ):
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
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data,
            api_key=api_key,
            **kwargs
        )

        # Store DataBento-specific configuration
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        
        # Track which assets we've already fetched to avoid redundant requests
        self._prefetched_assets = set()
        # Track data requests to avoid repeated log messages
        self._logged_requests = set()

        # OPTIMIZATION: Iteration-level caching to avoid redundant filtering
        # Cache filtered DataFrames per iteration (datetime)
        self._filtered_bars_cache = {}  # {(asset_key, length, timestep, timeshift, dt): DataFrame}
        self._last_price_cache = {}     # {(asset_key, dt): price}
        self._cache_datetime = None     # Track when to invalidate cache

        # Track which futures assets we've fetched multipliers for (to avoid redundant API calls)
        self._multiplier_fetched_assets = set()

        # Verify DataBento availability
        if not databento_helper.DATABENTO_AVAILABLE:
            logger.error("DataBento package not available. Please install with: pip install databento")
            raise ImportError("DataBento package not available")

        logger.debug(f"DataBento backtesting initialized for period: {datetime_start} to {datetime_end}")

    def _check_and_clear_cache(self):
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

    def _ensure_futures_multiplier(self, asset):
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
        cache_key = (asset.symbol, asset.asset_type, getattr(asset, 'expiration', None))

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
                reference_date=self.datetime_start
            )

            logger.debug(f"Successfully set multiplier for {asset.symbol}: {asset.multiplier}")

        except DataBentoAuthenticationError as e:
            logger.error(colored(f"DataBento authentication failed while fetching multiplier for {asset.symbol}: {e}", "red"))
            raise
        except Exception as e:
            logger.warning(f"Could not fetch multiplier for {asset.symbol}: {e}")

    def prefetch_data(self, assets, timestep="minute"):
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
                    force_cache_update=False
                )

                if df is None or df.empty:
                    # For empty data, create an empty Data object with proper timezone handling
                    empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                    # Create an empty DatetimeIndex with proper timezone
                    empty_df.index = pd.DatetimeIndex([], tz=LUMIBOT_DEFAULT_PYTZ, name='datetime')
                    
                    data_obj = Data(
                        asset,
                        df=empty_df,
                        timestep=timestep,
                        quote=quote_asset,
                        # Explicitly set dates to avoid timezone issues
                        date_start=None,
                        date_end=None
                    )
                    self.pandas_data[search_asset] = data_obj
                else:
                    # Create Data object and store
                    data_obj = Data(
                        asset,
                        df=df,
                        timestep=timestep,
                        quote=quote_asset,
                    )
                    self.pandas_data[search_asset] = data_obj
                    logger.debug(f"Cached {len(df)} rows for {asset.symbol}")
                
                # Mark as prefetched
                self._prefetched_assets.add(search_asset)
                
            except DataBentoAuthenticationError as e:
                logger.error(colored(f"DataBento authentication failed while prefetching {asset.symbol}: {e}", "red"))
                raise
            except Exception as e:
                logger.error(f"Error prefetching data for {asset.symbol}: {str(e)}")
                logger.error(traceback.format_exc())

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
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
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        # Handle tuple assets (asset, quote pairs)
        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

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
                force_cache_update=False
            )

            if df is None or df.empty:
                # For empty data, create an empty Data object with proper timezone handling
                # to maintain backward compatibility with tests
                empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                # Create an empty DatetimeIndex with proper timezone
                empty_df.index = pd.DatetimeIndex([], tz=LUMIBOT_DEFAULT_PYTZ, name='datetime')
                
                data_obj = Data(
                    asset_separated,
                    df=empty_df,
                    timestep=ts_unit,
                    quote=quote_asset,
                    # Use timezone-aware dates to avoid timezone issues
                    date_start=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2000, 1, 1)),
                    date_end=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2000, 1, 1))
                )
                self.pandas_data[search_asset] = data_obj
                return

            # Ensure the DataFrame has a datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                logger.error(f"DataBento data for {asset_separated.symbol} doesn't have datetime index")
                return

            # Create Data object and store in pandas_data
            data_obj = Data(
                asset_separated,
                df=df,
                timestep=ts_unit,
                quote=quote_asset,
            )
            
            self.pandas_data[search_asset] = data_obj

        except DataBentoAuthenticationError as e:
            logger.error(colored(f"DataBento authentication failed for {asset_separated.symbol}: {e}", "red"))
            raise
        except Exception as e:
            logger.error(f"Error updating pandas data for {asset_separated.symbol}: {str(e)}")
            logger.error(traceback.format_exc())

    def get_last_price(self, asset, quote=None, exchange=None):
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
        try:
            # OPTIMIZATION: Check cache first
            self._check_and_clear_cache()
            current_dt = self.get_datetime()
            current_dt_aware = to_datetime_aware(current_dt)

            # Try to get data from our cached pandas_data first
            search_asset = asset
            quote_asset = quote if quote is not None else Asset("USD", "forex")

            if isinstance(search_asset, tuple):
                asset_separated, quote_asset = search_asset
            else:
                search_asset = (search_asset, quote_asset)
                asset_separated = asset

            # Ensure futures have correct multiplier set
            self._ensure_futures_multiplier(asset_separated)

            # OPTIMIZATION: Check iteration cache
            cache_key = (search_asset, current_dt)
            if cache_key in self._last_price_cache:
                return self._last_price_cache[cache_key]

            if search_asset in self.pandas_data:
                asset_data = self.pandas_data[search_asset]
                df = asset_data.df

                if not df.empty and 'close' in df.columns:
                        # Ensure current_dt is timezone-aware for comparison
                        # Step back one bar so only fully closed bars are visible
                        bar_delta = timedelta(minutes=1)
                        if asset_data.timestep == "hour":
                            bar_delta = timedelta(hours=1)
                        elif asset_data.timestep == "day":
                            bar_delta = timedelta(days=1)

                        cutoff_dt = current_dt_aware - bar_delta

                        # Filter to data up to current backtest time (exclude current bar unless broker overrides)
                        filtered_df = df[df.index <= cutoff_dt]

                        # If we have no prior bar (e.g., first iteration), allow the current timestamp
                        if filtered_df.empty:
                            filtered_df = df[df.index <= current_dt_aware]

                        if not filtered_df.empty:
                            valid_closes = filtered_df['close'].dropna()
                            if not valid_closes.empty:
                                price = float(valid_closes.iloc[-1])
                                # OPTIMIZATION: Cache the result
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
                        df = asset_data.df
                        if not df.empty and 'close' in df.columns:
                            valid_closes = df[df.index <= current_dt_aware]['close'].dropna()
                            if not valid_closes.empty:
                                price = float(valid_closes.iloc[-1])
                                self._last_price_cache[cache_key] = price
                                return price
            except Exception as exc:
                logger.debug(
                    "Attempted to hydrate Databento cache for %s but hit error: %s",
                    asset.symbol,
                    exc,
                )

            # If still no data, fall back to direct fetch (live-style)
            logger.warning(f"No cached data for {asset.symbol}, attempting direct fetch")
            return databento_helper.get_last_price_from_databento(
                api_key=self._api_key,
                asset=asset_separated,
                venue=exchange,
                reference_date=current_dt_aware
            )
            
        except DataBentoAuthenticationError as e:
            logger.error(colored(f"DataBento authentication failed while getting last price for {asset.symbol}: {e}", "red"))
            raise
        except Exception as e:
            logger.error(f"Error getting last price for {asset.symbol}: {e}")
            return None

    def get_chains(self, asset, quote=None):
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

    def _get_bars_dict(self, assets, length, timestep, timeshift=None):
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
        result = {}
        
        for asset in assets:
            try:
                # Update pandas data if needed
                self._update_pandas_data(asset, None, length, timestep)
                
                # Get data from pandas_data
                search_asset = asset
                if not isinstance(search_asset, tuple):
                    search_asset = (search_asset, Asset("USD", "forex"))
                
                if search_asset in self.pandas_data:
                    asset_data = self.pandas_data[search_asset]
                    df = asset_data.df
                    
                    if not df.empty:
                        # Apply timeshift if specified
                        current_dt = self.get_datetime()
                        shift_seconds = 0
                        if timeshift:
                            if isinstance(timeshift, int):
                                shift_seconds = timeshift * 60
                                current_dt = current_dt - timedelta(minutes=timeshift)
                            else:
                                shift_seconds = timeshift.total_seconds()
                                current_dt = current_dt - timeshift
                        
                        # Ensure current_dt is timezone-aware for comparison
                        current_dt_aware = to_datetime_aware(current_dt)
                        
                        # Filter data up to current backtest time (exclude current bar unless broker overrides)
                        include_current = getattr(self, "_include_current_bar_for_orders", False)
                        allow_current = include_current or shift_seconds > 0
                        mask = df.index <= current_dt_aware if allow_current else df.index < current_dt_aware
                        filtered_df = df[mask]
                        
                        # Take the last 'length' bars
                        result_df = filtered_df.tail(length)
                        
                        if not result_df.empty:
                            result[asset] = result_df
                        else:
                            logger.warning(f"No data available for {asset.symbol} at {current_dt}")
                            result[asset] = None
                    else:
                        logger.warning(f"Empty data for {asset.symbol}")
                        result[asset] = None
                else:
                    logger.warning(f"No data found for {asset.symbol}")
                    result[asset] = None
                    
            except DataBentoAuthenticationError as e:
                logger.error(colored(f"DataBento authentication failed while getting bars for {asset}: {e}", "red"))
                raise
            except Exception as e:
                logger.error(f"Error getting bars for {asset}: {e}")
                result[asset] = None
        
        return result

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=0,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
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
        search_asset = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)
            asset_separated = asset

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
            df = asset_data.df

            if not df.empty:
                # ========================================================================
                # CRITICAL: NEGATIVE TIMESHIFT ARITHMETIC FOR LOOKAHEAD
                # ========================================================================
                # Negative timeshift allows broker to "peek ahead" for realistic fills.
                #
                # Example with timeshift=-2 at broker_dt=09:30:
                #   - Arithmetic: current_dt - timeshift
                #                = 09:30 - timedelta(minutes=-2)
                #                = 09:30 - (-2 minutes)
                #                = 09:30 + 2 minutes
                #                = 09:32
                #   - Data source returns bars up to 09:32: [..., 09:29, 09:30, 09:31, 09:32]
                #   - Broker filters to future bars (>= 09:30): [09:30, 09:31, 09:32]
                #   - Broker uses FIRST future bar (09:31) and its OPEN price for fills
                #
                # Why this is necessary:
                #   - Real world: Order placed at 09:30:30 fills at 09:31:00 open
                #   - Backtesting: Broker at 09:30 needs to see 09:31 bar for realistic fills
                #
                # DO NOT change this arithmetic! "current_dt - timeshift" with negative
                # timeshift is CORRECT and INTENTIONAL.
                # ========================================================================
                shift_seconds = 0
                if timeshift:
                    if isinstance(timeshift, int):
                        shift_seconds = timeshift * 60
                        current_dt = current_dt - timedelta(minutes=timeshift)
                    else:
                        shift_seconds = timeshift.total_seconds()
                        current_dt = current_dt - timeshift

                # Ensure current_dt is timezone-aware for comparison
                current_dt_aware = to_datetime_aware(current_dt)

                # Step back one bar to avoid exposing the in-progress bar
                bar_delta = timedelta(minutes=1)
                if asset_data.timestep == "hour":
                    bar_delta = timedelta(hours=1)
                elif asset_data.timestep == "day":
                    bar_delta = timedelta(days=1)

                cutoff_dt = current_dt_aware - bar_delta

                # INSTRUMENTATION: Log timeshift application and filtering
                broker_dt_orig = self.get_datetime()
                filter_branch = "shift_seconds > 0" if shift_seconds > 0 else "shift_seconds <= 0"

                # Filter data up to current backtest time (exclude current bar unless broker overrides)
                filtered_df = df[df.index <= cutoff_dt] if shift_seconds > 0 else df[df.index < current_dt_aware]

                # Log what bar we're returning
                if not filtered_df.empty:
                    returned_bar_dt = filtered_df.index[-1]
                    logger.debug(f"[TIMESHIFT_PANDAS] asset={asset_separated.symbol} broker_dt={broker_dt_orig} "
                               f"timeshift={timeshift} shift_seconds={shift_seconds} "
                               f"shifted_dt={current_dt_aware} cutoff_dt={cutoff_dt} "
                               f"filter={filter_branch} returned_bar={returned_bar_dt}")

                # Take the last 'length' bars
                result_df = filtered_df.tail(length)

                # OPTIMIZATION: Cache the result before returning
                if not result_df.empty:
                    self._filtered_bars_cache[cache_key] = result_df
                    return result_df
                else:
                    self._filtered_bars_cache[cache_key] = None
                    return None
            else:
                return None
        else:
            return None
    
    def initialize_data_for_backtest(self, strategy_assets, timestep="minute"):
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
                if any(month in asset for month in ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']):
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
