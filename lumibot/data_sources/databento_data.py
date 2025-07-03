import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union, Optional

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper

logger = logging.getLogger(__name__)


class DataBentoData(DataSourceBacktesting):
    """
    DataBento data source for historical market data
    
    This data source provides access to DataBento's institutional-grade market data,
    with a focus on futures data and support for multiple asset types.
    """
    
    SOURCE = "DATABENTO"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m", "minute", "1 minute"]},
        {"timestep": "hour", "representations": ["1h", "hour", "1 hour"]},
        {"timestep": "day", "representations": ["1d", "day", "1 day"]},
    ]

    def __init__(
        self, 
        api_key: str,
        datetime_start: datetime = None,
        datetime_end: datetime = None,
        timeout: int = 30,
        max_retries: int = 3,
        **kwargs
    ):
        """
        Initialize DataBento data source
        
        Parameters
        ----------
        api_key : str
            DataBento API key
        datetime_start : datetime, optional
            Start datetime for backtesting. If not provided, defaults to 1 year ago.
        datetime_end : datetime, optional
            End datetime for backtesting. If not provided, defaults to now.
        timeout : int, optional
            API request timeout in seconds, default 30
        max_retries : int, optional
            Maximum number of API retry attempts, default 3
        **kwargs
            Additional parameters passed to parent class
        """
        # Set default date range if not provided
        if datetime_start is None:
            datetime_start = datetime.now() - timedelta(days=365)
        if datetime_end is None:
            datetime_end = datetime.now()
        
        # Initialize parent class
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
        
        self.name = "databento"
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._data_store = {}
        
        # Backtesting mode is determined by the parent class
        self.is_backtesting_mode = self.IS_BACKTESTING_DATA_SOURCE
        
        # Verify DataBento availability
        if not databento_helper.DATABENTO_AVAILABLE:
            logger.error("DataBento package not available. Please install with: pip install databento")
            raise ImportError("DataBento package not available")

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: timedelta = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True
    ) -> Bars:
        """
        Get historical price data for an asset
        
        Parameters
        ----------
        asset : Asset
            The asset to get historical prices for
        length : int
            Number of bars to retrieve
        timestep : str, optional
            Timestep for the data ('minute', 'hour', 'day'), default 'minute'
        timeshift : timedelta, optional
            Time shift to apply to the data retrieval
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
        include_after_hours : bool, optional
            Whether to include after-hours data, default True
            
        Returns
        -------
        Bars
            Historical price data as Bars object
        """
        logger.info(f"Getting historical prices for {asset.symbol}, length={length}, timestep={timestep}")
        
        # Use backtesting method if in backtesting mode
        if self.is_backtesting_mode:
            return self._pull_source_symbol_bars(
                asset,
                length,
                timestep=timestep,
                timeshift=timeshift,
                quote=quote,
                exchange=exchange,
                include_after_hours=include_after_hours,
            )
        
        # For live trading, use current approach
        # Calculate the date range for data retrieval
        current_dt = datetime.now()
        logging.info(f"Using current datetime for live trading: {current_dt}")
        
        # Apply timeshift if specified
        if timeshift:
            current_dt = current_dt - timeshift
        
        # Calculate start date based on length and timestep
        if timestep == "day":
            buffer_days = max(10, length // 2)  # Buffer for live trading
            start_dt = current_dt - timedelta(days=length + buffer_days)
            # For live trading, end should be current time (no future data available)
            end_dt = current_dt
        elif timestep == "hour":
            buffer_hours = max(24, length // 2)  # Buffer for live trading  
            start_dt = current_dt - timedelta(hours=length + buffer_hours)
            # For live trading, end should be current time (no future data available)
            end_dt = current_dt
        else:  # minute or other
            buffer_minutes = max(1440, length)  # Buffer for live trading
            start_dt = current_dt - timedelta(minutes=length + buffer_minutes)
            # For live trading, end should be current time (no future data available)
            end_dt = current_dt
        
        # Ensure both dates have the same timezone awareness
        if start_dt.tzinfo is not None and end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=start_dt.tzinfo)
        elif start_dt.tzinfo is None and end_dt.tzinfo is not None:
            start_dt = start_dt.replace(tzinfo=end_dt.tzinfo)
        
        # Ensure we always have a valid date range (start < end)
        if start_dt >= end_dt:
            # If dates are equal or start is after end, adjust end date
            if timestep == "day":
                end_dt = start_dt + timedelta(days=max(1, length))
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=max(1, length))
            else:  # minute or other
                end_dt = start_dt + timedelta(minutes=max(1, length))
        
        # Final safety check: ensure end is always after start
        if start_dt >= end_dt:
            logger.error(f"Invalid date range after adjustment: start={start_dt}, end={end_dt}")
            if timestep == "day":
                end_dt = start_dt + timedelta(days=1)
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=1)
            else:
                end_dt = start_dt + timedelta(minutes=1)
        
        # Get data from DataBento
        logging.info(f"Requesting DataBento data for asset: {asset} (type: {asset.asset_type})")
        logging.info(f"Date range: {start_dt} to {end_dt}")
        
        df = databento_helper.get_price_data_from_databento(
            api_key=self._api_key,
            asset=asset,
            start=start_dt,
            end=end_dt,
            timestep=timestep,
            venue=exchange
        )
        
        if df is None or df.empty:
            logging.error(f"No data returned from DataBento for {asset.symbol}. This could be due to:")
            logging.error("1. Incorrect symbol format")
            logging.error("2. Wrong dataset selection")
            logging.error("3. Data not available for the requested time range")
            logging.error("4. API authentication issues")
            return None
        
        # Filter data to the current time (for live trading)
        df_filtered = df[df.index <= current_dt]
        
        # Take the last 'length' bars
        df_result = df_filtered.tail(length)
        
        if df_result.empty:
            logger.warning(f"No data available for {asset.symbol} up to {current_dt}")
            return None
        
        # Create and return Bars object
        bars = Bars(
            df=df_result,
            source=self.SOURCE,
            asset=asset,
            quote=quote
        )
        
        logger.info(f"Retrieved {len(df_result)} bars for {asset.symbol}")
        return bars

    def get_last_price(
        self,
        asset: Asset,
        quote: Asset = None,
        exchange: str = None
    ) -> Union[float, Decimal, None]:
        """
        Get the last known price for an asset
        
        Parameters
        ----------
        asset : Asset
            The asset to get the last price for
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
            
        Returns
        -------
        float, Decimal, or None
            Last known price of the asset
        """
        logger.info(f"Getting last price for {asset.symbol}")
        
        try:
            last_price = databento_helper.get_last_price_from_databento(
                api_key=self._api_key,
                asset=asset,
                venue=exchange
            )
            
            if last_price is not None:
                logger.info(f"Last price for {asset.symbol}: {last_price}")
                return last_price
            else:
                logger.warning(f"No last price available for {asset.symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting last price for {asset.symbol}: {e}")
            return None

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        """
        Get option chains for an asset
        
        Note: DataBento primarily focuses on market data rather than options chains.
        This method returns an empty dict as DataBento doesn't provide options chain data.
        
        Parameters
        ----------
        asset : Asset
            The asset to get option chains for
        quote : Asset, optional
            Quote asset
            
        Returns
        -------
        dict
            Empty dictionary as DataBento doesn't provide options chains
        """
        logger.warning("DataBento does not provide options chain data")
        return {}

    # ===== BACKTESTING METHODS =====
    
    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: timedelta = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
    ) -> Bars:
        """
        Pull historical bars for a specific symbol during backtesting
        
        Parameters
        ----------
        asset : Asset
            The asset to get historical prices for
        length : int
            Number of bars to retrieve
        timestep : str, optional
            Timestep for the data ('minute', 'hour', 'day'), default 'minute'
        timeshift : timedelta, optional
            Time shift to apply to the data retrieval
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
        include_after_hours : bool, optional
            Whether to include after-hours data, default True
            
        Returns
        -------
        Bars
            Historical price data as Bars object
        """
        logger.info(f"Backtesting mode: Getting historical prices for {asset.symbol}, length={length}, timestep={timestep}")
        
        # Get current backtest time from strategy
        current_dt = self.get_datetime()
        if current_dt is None:
            current_dt = datetime.now()
        
        # Check if we're actually in a live trading scenario by comparing current_dt to now
        # If current_dt is more than a few hours old, we're likely in a live trading scenario
        # where the data source was initialized with old default dates
        now = datetime.now()
        
        # Handle timezone-aware vs timezone-naive datetime comparison
        if current_dt.tzinfo is not None and now.tzinfo is None:
            # Convert now to timezone-aware using the same timezone as current_dt
            now = now.replace(tzinfo=current_dt.tzinfo)
        elif current_dt.tzinfo is None and now.tzinfo is not None:
            # Convert current_dt to timezone-aware using the same timezone as now
            current_dt = current_dt.replace(tzinfo=now.tzinfo)
        
        time_diff = abs((now - current_dt).total_seconds())
        
        # If the difference is more than 4 hours, treat this as live trading
        if time_diff > 4 * 3600:  # 4 hours in seconds
            logger.info("Detected live trading mode despite backtesting data source - using current time")
            current_dt = now
        
        # Apply timeshift if specified
        if timeshift:
            current_dt = current_dt - timeshift
        
        # Calculate start date based on length and timestep with buffer
        # We need a substantial buffer to ensure we get enough data to return the requested number of bars
        if timestep == "day":
            buffer_days = max(30, length * 2)  # Ensure sufficient buffer for requested bars
            start_dt = current_dt - timedelta(days=length + buffer_days)
            # For backtesting, end should be current time (don't request future data)
            end_dt = current_dt
        elif timestep == "hour":
            buffer_hours = max(48, length * 2)  # Ensure sufficient buffer for requested bars
            start_dt = current_dt - timedelta(hours=length + buffer_hours)
            # For backtesting, end should be current time (don't request future data)
            end_dt = current_dt
        else:  # minute or other
            buffer_minutes = max(2880, length * 2)  # Ensure sufficient buffer for requested bars
            start_dt = current_dt - timedelta(minutes=length + buffer_minutes)
            # For backtesting, end should be current time (don't request future data)
            end_dt = current_dt
        
        # Ensure start date is not before the backtesting start date
        if self.datetime_start is not None:
            # Handle timezone-aware vs timezone-naive datetime comparison
            if self.datetime_start.tzinfo is not None and start_dt.tzinfo is None:
                # Convert start_dt and end_dt to timezone-aware using the same timezone as datetime_start
                start_dt = start_dt.replace(tzinfo=self.datetime_start.tzinfo)
                end_dt = end_dt.replace(tzinfo=self.datetime_start.tzinfo)
            elif self.datetime_start.tzinfo is None and start_dt.tzinfo is not None:
                # Convert datetime_start to timezone-aware using the same timezone as start_dt
                datetime_start_aware = self.datetime_start.replace(tzinfo=start_dt.tzinfo)
            else:
                datetime_start_aware = self.datetime_start
                
            # Use the properly handled datetime_start for comparison
            if start_dt < (datetime_start_aware if 'datetime_start_aware' in locals() else self.datetime_start):
                start_dt = datetime_start_aware if 'datetime_start_aware' in locals() else self.datetime_start
                # Recalculate end_dt to ensure proper range
                if timestep == "day":
                    end_dt = start_dt + timedelta(days=max(1, length + 30))
                elif timestep == "hour":
                    end_dt = start_dt + timedelta(hours=max(1, length + 48))
                else:  # minute or other
                    end_dt = start_dt + timedelta(minutes=max(1, length + 2880))
        
        # Ensure both dates have the same timezone awareness
        if start_dt.tzinfo is not None and end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=start_dt.tzinfo)
        elif start_dt.tzinfo is None and end_dt.tzinfo is not None:
            start_dt = start_dt.replace(tzinfo=end_dt.tzinfo)
        
        # Ensure we always have a valid date range (start < end)
        if start_dt >= end_dt:
            # If dates are equal or start is after end, adjust end date
            if timestep == "day":
                end_dt = start_dt + timedelta(days=max(1, length))
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=max(1, length))
            else:  # minute or other
                end_dt = start_dt + timedelta(minutes=max(1, length))
        
        # Final safety check: ensure end is always after start
        if start_dt >= end_dt:
            logger.error(f"Invalid date range after adjustment: start={start_dt}, end={end_dt}")
            if timestep == "day":
                end_dt = start_dt + timedelta(days=1)
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=1)
            else:
                end_dt = start_dt + timedelta(minutes=1)
        
        # Get data from DataBento
        logging.info(f"Backtesting: Requesting DataBento data for asset: {asset} (type: {asset.asset_type})")
        logging.info(f"Date range: {start_dt} to {end_dt}")
        
        df = databento_helper.get_price_data_from_databento(
            api_key=self._api_key,
            asset=asset,
            start=start_dt,
            end=end_dt,
            timestep=timestep,
            venue=exchange
        )
        
        if df is None or df.empty:
            logging.error(f"No data returned from DataBento for {asset.symbol} during backtesting")
            return None
        
        # Filter data to the current backtest time
        df_filtered = df[df.index <= current_dt]
        
        # Take the last 'length' bars
        df_result = df_filtered.tail(length)
        
        if df_result.empty:
            logger.warning(f"No data available for {asset.symbol} up to {current_dt} during backtesting")
            return None
        
        # Create and return Bars object
        bars = Bars(
            df=df_result,
            source=self.SOURCE,
            asset=asset,
            quote=quote
        )
        
        logger.info(f"Backtesting: Retrieved {len(df_result)} bars for {asset.symbol}")
        return bars

    def _pull_source_bars(
        self,
        assets: list,
        length: int,
        timestep: str = "minute",
        timeshift: timedelta = None,
        chunk_size: int = 100,
        max_workers: int = 200,
    ) -> dict:
        """
        Pull historical bars for multiple assets during backtesting
        
        Parameters
        ----------
        assets : list
            List of assets to get historical prices for
        length : int
            Number of bars to retrieve
        timestep : str, optional
            Timestep for the data ('minute', 'hour', 'day'), default 'minute'
        timeshift : timedelta, optional
            Time shift to apply to the data retrieval
        chunk_size : int, optional
            Chunk size for processing multiple assets, default 100
        max_workers : int, optional
            Maximum number of workers for parallel processing, default 200
            
        Returns
        -------
        dict
            Dictionary mapping assets to their historical Bars objects
        """
        logger.info(f"Backtesting mode: Getting historical prices for {len(assets)} assets")
        
        result = {}
        
        # Process each asset individually for DataBento
        for asset in assets:
            try:
                bars = self._pull_source_symbol_bars(
                    asset=asset,
                    length=length,
                    timestep=timestep,
                    timeshift=timeshift
                )
                if bars is not None:
                    result[asset] = bars
                else:
                    logger.warning(f"No bars retrieved for {asset.symbol}")
                    
            except Exception as e:
                logger.error(f"Error retrieving bars for {asset.symbol}: {e}")
                continue
        
        logger.info(f"Successfully retrieved bars for {len(result)} out of {len(assets)} assets")
        return result

    def _parse_source_symbol_bars(self, symbol_response: dict, asset: Asset) -> Bars:
        """
        Parse response from DataBento into Bars object
        
        This method is kept for compatibility but DataBento responses
        are typically handled directly in the helper functions.
        
        Parameters
        ----------
        symbol_response : dict
            Response data from DataBento API
        asset : Asset
            The asset the data corresponds to
            
        Returns
        -------
        Bars
            Parsed Bars object
        """
        logger.warning("_parse_source_symbol_bars called - DataBento responses are handled in helper functions")
        
        # For DataBento, parsing is typically done in the helper functions
        # This method is kept for compatibility with the DataSource interface
        if symbol_response is None:
            return None
        
        # If we receive a DataFrame directly, convert to Bars
        if hasattr(symbol_response, 'index'):  # DataFrame-like object
            # Check if DataFrame is empty or missing required columns
            if symbol_response.empty:
                logger.warning("Empty DataFrame provided to _parse_source_symbol_bars")
                return None
                
            # Check for required columns
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in symbol_response.columns]
            
            if missing_columns:
                logger.error(f"DataFrame missing required columns: {missing_columns}")
                return None
            
            return Bars(
                df=symbol_response,
                source=self.SOURCE,
                asset=asset
            )
        
        return None
