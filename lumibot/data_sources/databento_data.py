import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union, Optional

from lumibot.data_sources import DataSource
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper

logger = logging.getLogger(__name__)


class DataBentoData(DataSource):
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
        timeout : int, optional
            API request timeout in seconds, default 30
        max_retries : int, optional
            Maximum number of API retry attempts, default 3
        **kwargs
            Additional parameters passed to parent class
        """
        # Initialize parent class
        super().__init__(api_key=api_key, **kwargs)
        
        self.name = "databento"
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._data_store = {}
        
        # For live trading, this is a live data source
        self.is_backtesting_mode = False
        
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
        
        # Validate asset type - DataBento primarily supports futures
        supported_asset_types = [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]
        if asset.asset_type not in supported_asset_types:
            error_msg = f"DataBento data source only supports futures assets. Received asset type '{asset.asset_type}' for symbol '{asset.symbol}'. Supported types: {[t.value for t in supported_asset_types]}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Additional logging for debugging
        logger.info(f"DataBento request - Asset: {asset.symbol}, Type: {asset.asset_type}, Length: {length}, Timestep: {timestep}")
        logger.info(f"DataBento live trading mode: Requesting data for futures asset {asset.symbol}")
        
        # Calculate the date range for data retrieval
        # Use timezone-naive datetime for consistency
        current_dt = datetime.now()
        if current_dt.tzinfo is not None:
            current_dt = current_dt.replace(tzinfo=None)
        
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
        
        # Ensure both dates are timezone-naive for consistency
        if start_dt.tzinfo is not None:
            start_dt = start_dt.replace(tzinfo=None)
        if end_dt.tzinfo is not None:
            end_dt = end_dt.replace(tzinfo=None)
        
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
        
        try:
            df = databento_helper.get_price_data_from_databento(
                api_key=self._api_key,
                asset=asset,
                start=start_dt,
                end=end_dt,
                timestep=timestep,
                venue=exchange
            )
        except Exception as e:
            logger.error(f"Error getting data from DataBento for {asset.symbol}: {e}")
            return None
        
        if df is None or df.empty:
            logging.error(f"No data returned from DataBento for {asset.symbol}. This could be due to:")
            logging.error("1. Incorrect symbol format")
            logging.error("2. Wrong dataset selection")
            logging.error("3. Data not available for the requested time range")
            logging.error("4. API authentication issues")
            return None
        
        # Filter data to the current time (for live trading)
        # Handle timezone consistency for comparison
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            # DataFrame has timezone-aware index, convert current_dt to match
            if current_dt.tzinfo is None:
                import pytz
                current_dt = current_dt.replace(tzinfo=pytz.UTC)
        else:
            # DataFrame has timezone-naive index, ensure current_dt is also naive
            if current_dt.tzinfo is not None:
                current_dt = current_dt.replace(tzinfo=None)
        
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

    def get_quote(self, asset: Asset, quote: Asset = None) -> Union[float, Decimal, None]:
        """
        Get current quote for an asset
        
        For DataBento, this returns the last known price since real-time quotes
        may not be available for all assets.
        
        Parameters
        ----------
        asset : Asset
            The asset to get the quote for
        quote : Asset, optional
            Quote asset (not used for DataBento)
            
        Returns
        -------
        float, Decimal, or None
            Current quote/last price of the asset
        """
        return self.get_last_price(asset, quote=quote)

    def _parse_source_symbol_bars(self, response, asset, quote=None):
        """
        Parse source data for a single asset into Bars format
        
        Parameters
        ----------
        response : pd.DataFrame
            Raw data from DataBento API
        asset : Asset
            The asset the data is for
        quote : Asset, optional
            Quote asset (not used for DataBento)
            
        Returns
        -------
        Bars or None
            Parsed bars data or None if parsing fails
        """
        try:
            if response is None or response.empty:
                return None
                
            # Check if required columns exist
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in response.columns for col in required_columns):
                logger.warning(f"Missing required columns in DataBento data for {asset.symbol}")
                return None
            
            # Create Bars object
            bars = Bars(
                df=response,
                source=self.SOURCE,
                asset=asset,
                quote=quote
            )
            
            return bars
            
        except Exception as e:
            logger.error(f"Error parsing DataBento data for {asset.symbol}: {e}")
            return None
