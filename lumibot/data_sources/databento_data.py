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
        timeout: int = 30,
        max_retries: int = 3,
        datetime_start: datetime = None,
        datetime_end: datetime = None,
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
        datetime_start : datetime, optional
            Start datetime for backtesting
        datetime_end : datetime, optional
            End datetime for backtesting
        **kwargs
            Additional parameters passed to parent class
        """
        # Set default date range if not provided for backtesting
        if datetime_start is None:
            datetime_start = datetime.now() - timedelta(days=365)
        if datetime_end is None:
            datetime_end = datetime.now()
        
        # Initialize parent class
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            api_key=api_key,
            **kwargs
        )
        
        self.name = "databento"
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._data_store = {}
        
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
        
        # Calculate the date range for data retrieval
        current_dt = self.get_datetime()
        
        # Apply timeshift if specified
        if timeshift:
            current_dt = current_dt - timeshift
        
        # Calculate start date based on length and timestep
        if timestep == "day":
            start_dt = current_dt - timedelta(days=length + 10)  # Add buffer
        elif timestep == "hour":
            start_dt = current_dt - timedelta(hours=length + 24)  # Add buffer
        else:  # minute or other
            start_dt = current_dt - timedelta(minutes=length + 1440)  # Add buffer
        
        # Get data from DataBento
        df = databento_helper.get_price_data_from_databento(
            api_key=self._api_key,
            asset=asset,
            start=start_dt,
            end=current_dt,
            timestep=timestep,
            venue=exchange
        )
        
        if df is None or df.empty:
            logger.warning(f"No data returned from DataBento for {asset.symbol}")
            return None
        
        # Filter data to the current backtest time
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

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True
    ):
        """
        Pull raw data for a single asset from DataBento
        
        This is used internally by the backtesting framework.
        """
        logger.info(f"Pulling source data for {asset.symbol}")
        
        # Calculate time range
        current_dt = self.get_datetime()
        if timeshift:
            current_dt = current_dt - timeshift
        
        # Add substantial buffer to ensure we have enough data
        if timestep == "day":
            start_dt = current_dt - timedelta(days=length * 2)
        elif timestep == "hour":
            start_dt = current_dt - timedelta(hours=length * 2)
        else:
            start_dt = current_dt - timedelta(minutes=length * 2)
        
        # Get data from DataBento
        df = databento_helper.get_price_data_from_databento(
            api_key=self._api_key,
            asset=asset,
            start=start_dt,
            end=current_dt,
            timestep=timestep,
            venue=exchange
        )
        
        if df is None or df.empty:
            logger.warning(f"No data returned for {asset.symbol}")
            return None
        
        # Filter to current backtest time and take last 'length' bars
        df_filtered = df[df.index <= current_dt].tail(length)
        
        return df_filtered

    def _pull_source_bars(
        self,
        assets: list,
        length: int,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta = None,
        quote: Asset = None,
        include_after_hours: bool = True
    ):
        """
        Pull raw data for multiple assets from DataBento
        
        This is used internally by the backtesting framework.
        """
        result = {}
        
        for asset in assets:
            try:
                exchange = None
                # Extract exchange from asset if it's a tuple
                if isinstance(asset, tuple):
                    asset, exchange = asset[0], asset[1] if len(asset) > 1 else None
                
                data = self._pull_source_symbol_bars(
                    asset=asset,
                    length=length,
                    timestep=timestep,
                    timeshift=timeshift,
                    quote=quote,
                    exchange=exchange,
                    include_after_hours=include_after_hours
                )
                
                result[asset] = data
                
            except Exception as e:
                logger.error(f"Error pulling data for {asset}: {e}")
                result[asset] = None
        
        return result

    def _parse_source_symbol_bars(self, df, asset: Asset, quote: Asset = None):
        """
        Parse and validate source data for a single asset
        
        This ensures the data is in the correct format for Lumibot.
        """
        if df is None or df.empty:
            return None
        
        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"Missing columns in data for {asset.symbol}: {missing_cols}")
            return None
        
        # Create Bars object
        bars = Bars(
            df=df,
            source=self.SOURCE,
            asset=asset,
            quote=quote
        )
        
        return bars
