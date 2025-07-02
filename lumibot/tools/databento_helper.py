# This file contains helper functions for getting data from DataBento
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset
from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.error_logger import ErrorLogger

# DataBento imports (will be installed as dependency)
try:
    import databento as db
    from databento import Historical
    DATABENTO_AVAILABLE = True
except ImportError:
    DATABENTO_AVAILABLE = False
    logging.warning("DataBento package not available. Please install with: pip install databento")

# Cache settings
CACHE_SUBFOLDER = "databento"
LUMIBOT_DATABENTO_CACHE_FOLDER = os.path.join(LUMIBOT_CACHE_FOLDER, CACHE_SUBFOLDER)
RECENT_FILE_TOLERANCE_DAYS = 14
MAX_DATABENTO_DAYS = 365  # DataBento can handle larger date ranges than some providers

# Error logging
error_logger = ErrorLogger()

# Create cache directory if it doesn't exist
if not os.path.exists(LUMIBOT_DATABENTO_CACHE_FOLDER):
    try:
        os.makedirs(LUMIBOT_DATABENTO_CACHE_FOLDER)
    except Exception as e:
        logging.warning(f"Could not create DataBento cache folder: {e}")


class DataBentoClient:
    """DataBento client wrapper for handling API connections and requests"""
    
    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 3):
        if not DATABENTO_AVAILABLE:
            raise ImportError("DataBento package not available. Please install with: pip install databento")
        
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self._client = None
        
    @property
    def client(self):
        """Lazy initialization of DataBento client"""
        if self._client is None:
            if not DATABENTO_AVAILABLE:
                raise ImportError("DataBento package not available")
            self._client = Historical(key=self.api_key)
        return self._client
    
    def get_historical_data(
        self,
        dataset: str,
        symbols: Union[str, List[str]],
        schema: str,
        start: Union[str, datetime, date],
        end: Union[str, datetime, date],
        venue: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Get historical data from DataBento with retry logic
        
        Parameters
        ----------
        dataset : str
            DataBento dataset identifier (e.g., 'GLBX.MDP3', 'XNAS.ITCH')
        symbols : str or list of str
            Symbol(s) to retrieve data for
        schema : str
            DataBento schema (e.g., 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d')
        start : str, datetime, or date
            Start date/time for data retrieval
        end : str, datetime, or date
            End date/time for data retrieval
        venue : str, optional
            Venue filter
        **kwargs
            Additional parameters for DataBento API
            
        Returns
        -------
        pd.DataFrame
            Historical data from DataBento
        """
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                logging.info(f"Requesting DataBento data: {symbols} from {start} to {end}")
                
                # Use DataBento's time_series API
                data = self.client.timeseries.get_range(
                    dataset=dataset,
                    symbols=symbols,
                    schema=schema,
                    start=start,
                    end=end,
                    **kwargs
                )
                
                # Convert to DataFrame if not already
                if hasattr(data, 'to_df'):
                    df = data.to_df()
                else:
                    df = pd.DataFrame(data)
                
                logging.info(f"Successfully retrieved {len(df)} rows from DataBento")
                return df
                
            except Exception as e:
                retry_count += 1
                error_msg = f"DataBento API error (attempt {retry_count}/{self.max_retries}): {str(e)}"
                logging.warning(error_msg)
                
                if error_logger:
                    error_logger.log_error(
                        severity="ERROR",
                        error_code="API_ERROR",
                        message=f"DataBento API error: {str(e)}",
                        details=f"Symbols: {symbols}, Start: {start}, End: {end}"
                    )
                
                if retry_count >= self.max_retries:
                    logging.error(f"DataBento API failed after {self.max_retries} attempts")
                    raise e
                
                # Exponential backoff
                time.sleep(2 ** retry_count)
        
        return pd.DataFrame()


def _format_futures_symbol_for_databento(asset: Asset) -> str:
    """
    Format a futures Asset object for DataBento symbol conventions
    
    DataBento expects futures symbols in YYYYMM format for specific contracts.
    
    Parameters
    ----------
    asset : Asset
        Lumibot Asset object with asset_type='future'
        
    Returns
    -------
    str
        DataBento-formatted futures symbol
    """
    symbol = asset.symbol
    
    # For continuous contracts, DataBento often uses the root symbol
    if not asset.expiration:
        return symbol
    
    # For specific contracts, format with expiration in YYYYMM format
    if asset.expiration:
        # DataBento uses YYYYMM format for specific contracts
        year = asset.expiration.year
        month = asset.expiration.month
        
        # Format as SYMBOLYYYYMM (e.g., ES202503 for March 2025)
        formatted_symbol = f"{symbol}{year:04d}{month:02d}"
        
        logging.debug(f"Formatted futures symbol: {asset.symbol} {asset.expiration} -> {formatted_symbol}")
        return formatted_symbol
    
    return symbol


def _determine_databento_dataset(asset: Asset, venue: Optional[str] = None) -> str:
    """
    Determine the appropriate DataBento dataset based on asset type and venue
    
    Parameters
    ----------
    asset : Asset
        Lumibot Asset object
    venue : str, optional
        Specific venue/exchange
        
    Returns
    -------
    str
        DataBento dataset identifier
    """
    # Default datasets for different asset types and venues
    if asset.asset_type in ['future', 'futures']:
        if venue:
            venue_upper = venue.upper()
            if venue_upper in ['CME', 'CBOT', 'NYMEX', 'COMEX']:
                return 'GLBX.MDP3'
            elif venue_upper in ['ICE']:
                return 'IFEU.IMPACT'
        # Default to CME group for futures
        return 'GLBX.MDP3'
    
    elif asset.asset_type in ['stock', 'equity']:
        # Default to NASDAQ for equities
        return 'XNAS.ITCH'
    
    # Default fallback
    return 'GLBX.MDP3'


def _determine_databento_schema(timestep: str) -> str:
    """
    Map Lumibot timestep to DataBento schema
    
    Parameters
    ----------
    timestep : str
        Lumibot timestep ('minute', 'hour', 'day')
        
    Returns
    -------
    str
        DataBento schema identifier
    """
    schema_mapping = {
        'minute': 'ohlcv-1m',
        'hour': 'ohlcv-1h', 
        'day': 'ohlcv-1d',
        '1minute': 'ohlcv-1m',
        '1hour': 'ohlcv-1h',
        '1day': 'ohlcv-1d',
        '1m': 'ohlcv-1m',
        '1h': 'ohlcv-1h',
        '1d': 'ohlcv-1d',
    }
    
    return schema_mapping.get(timestep.lower(), 'ohlcv-1m')


def _build_cache_filename(asset: Asset, start: datetime, end: datetime, timestep: str) -> Path:
    """Build a cache filename for the given parameters"""
    symbol = asset.symbol
    if asset.expiration:
        symbol += f"_{asset.expiration.strftime('%Y%m%d')}"
    
    start_str = start.strftime('%Y%m%d')
    end_str = end.strftime('%Y%m%d')
    filename = f"{symbol}_{timestep}_{start_str}_{end_str}.feather"
    
    return Path(LUMIBOT_DATABENTO_CACHE_FOLDER) / filename


def _load_cache(cache_file: Path) -> Optional[pd.DataFrame]:
    """Load data from cache file"""
    try:
        if cache_file.exists():
            df = pd.read_feather(cache_file)
            # Ensure datetime index
            if 'ts_event' in df.columns:
                df.set_index('ts_event', inplace=True)
            elif not isinstance(df.index, pd.DatetimeIndex):
                # Try to find a datetime column to use as index
                datetime_cols = df.select_dtypes(include=['datetime64']).columns
                if len(datetime_cols) > 0:
                    df.set_index(datetime_cols[0], inplace=True)
            
            return df
    except Exception as e:
        logging.warning(f"Error loading cache file {cache_file}: {e}")
        # Remove corrupted cache file
        try:
            cache_file.unlink()
        except:
            pass
    
    return None


def _save_cache(df: pd.DataFrame, cache_file: Path) -> None:
    """Save data to cache file"""
    try:
        # Ensure directory exists
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Reset index if needed to ensure it's saved properly
        df_to_save = df.copy()
        if isinstance(df_to_save.index, pd.DatetimeIndex):
            df_to_save.reset_index(inplace=True)
        
        df_to_save.to_feather(cache_file)
        logging.debug(f"Cached data saved to {cache_file}")
    except Exception as e:
        logging.warning(f"Error saving cache file {cache_file}: {e}")


def _normalize_databento_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataBento DataFrame to Lumibot standard format
    
    Parameters
    ----------
    df : pd.DataFrame
        Raw DataBento DataFrame
        
    Returns
    -------
    pd.DataFrame
        Normalized DataFrame with standard OHLCV columns
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying original
    df_norm = df.copy()
    
    # DataBento timestamp column mapping
    timestamp_cols = ['ts_event', 'timestamp', 'time']
    timestamp_col = None
    for col in timestamp_cols:
        if col in df_norm.columns:
            timestamp_col = col
            break
    
    if timestamp_col:
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df_norm[timestamp_col]):
            df_norm[timestamp_col] = pd.to_datetime(df_norm[timestamp_col])
        
        # Set as index
        df_norm.set_index(timestamp_col, inplace=True)
    
    # Standardize column names to Lumibot format
    column_mapping = {
        'open': 'open',
        'high': 'high', 
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'vwap': 'vwap',  # Keep if available
    }
    
    # Apply column mapping
    df_norm = df_norm.rename(columns=column_mapping)
    
    # Ensure we have the required OHLCV columns
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df_norm.columns]
    
    if missing_cols:
        logging.warning(f"Missing required columns in DataBento data: {missing_cols}")
        # Fill missing columns with NaN or appropriate defaults
        for col in missing_cols:
            if col == 'volume':
                df_norm[col] = 0
            else:
                df_norm[col] = None
    
    # Ensure numeric data types
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        if col in df_norm.columns:
            df_norm[col] = pd.to_numeric(df_norm[col], errors='coerce')
    
    # Sort by index (datetime)
    if isinstance(df_norm.index, pd.DatetimeIndex):
        df_norm.sort_index(inplace=True)
    
    return df_norm


def get_price_data_from_databento(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timestep: str = "minute",
    venue: Optional[str] = None,
    force_cache_update: bool = False,
    **kwargs
) -> Optional[pd.DataFrame]:
    """
    Get historical price data from DataBento for the given asset
    
    Parameters
    ----------
    api_key : str
        DataBento API key
    asset : Asset
        Lumibot Asset object
    start : datetime
        Start datetime for data retrieval
    end : datetime
        End datetime for data retrieval
    timestep : str, optional
        Data timestep ('minute', 'hour', 'day'), default 'minute'
    venue : str, optional
        Specific exchange/venue filter
    force_cache_update : bool, optional
        Force refresh of cached data, default False
    **kwargs
        Additional parameters for DataBento API
        
    Returns
    -------
    pd.DataFrame or None
        Historical price data in standard OHLCV format, None if no data
    """
    if not DATABENTO_AVAILABLE:
        logging.error("DataBento package not available. Please install with: pip install databento")
        return None
    
    # Build cache filename
    cache_file = _build_cache_filename(asset, start, end, timestep)
    
    # Try to load from cache first
    if not force_cache_update:
        cached_data = _load_cache(cache_file)
        if cached_data is not None and not cached_data.empty:
            logging.debug(f"Loaded DataBento data from cache: {cache_file}")
            return cached_data
    
    # Initialize DataBento client
    try:
        client = DataBentoClient(api_key=api_key)
        
        # Format symbol for DataBento
        symbol = _format_futures_symbol_for_databento(asset)
        
        # Determine dataset and schema
        dataset = _determine_databento_dataset(asset, venue)
        schema = _determine_databento_schema(timestep)
        
        logging.info(f"Fetching DataBento data: symbol={symbol}, dataset={dataset}, schema={schema}")
        
        # Get data from DataBento
        df = client.get_historical_data(
            dataset=dataset,
            symbols=symbol,
            schema=schema,
            start=start,
            end=end,
            **kwargs
        )
        
        if df.empty:
            logging.warning(f"No data returned from DataBento for {symbol}")
            return None
        
        # Normalize the data
        df_normalized = _normalize_databento_dataframe(df)
        
        # Cache the data
        _save_cache(df_normalized, cache_file)
        
        logging.debug(f"Successfully retrieved and cached {len(df_normalized)} rows for {symbol}")
        return df_normalized
        
    except Exception as e:
        error_msg = f"Error fetching DataBento data for {asset.symbol}: {str(e)}"
        logging.error(error_msg)
        
        if error_logger:
            error_logger.log_error(
                severity="ERROR",
                error_code="DATA_FETCH_ERROR",
                message=f"DataBento data fetch error: {str(e)}",
                details=f"Asset: {asset.symbol}, Start: {start}, End: {end}"
            )
        
        return None


def get_last_price_from_databento(
    api_key: str,
    asset: Asset,
    venue: Optional[str] = None,
    **kwargs
) -> Optional[Union[float, Decimal]]:
    """
    Get the last/current price for an asset from DataBento
    
    Parameters
    ----------
    api_key : str
        DataBento API key
    asset : Asset
        Lumibot Asset object
    venue : str, optional
        Specific exchange/venue filter
    **kwargs
        Additional parameters
        
    Returns
    -------
    float, Decimal, or None
        Last price of the asset, None if unavailable
    """
    if not DATABENTO_AVAILABLE:
        logging.error("DataBento package not available")
        return None
    
    try:
        # For last price, we'll get the most recent daily data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        df = get_price_data_from_databento(
            api_key=api_key,
            asset=asset,
            start=start_date,
            end=end_date,
            timestep="minute",
            venue=venue,
            **kwargs
        )
        
        if df is not None and not df.empty and 'close' in df.columns:
            last_price = df['close'].iloc[-1]
            return float(last_price) if not pd.isna(last_price) else None
            
    except Exception as e:
        logging.error(f"Error getting last price from DataBento for {asset.symbol}: {e}")
    
    return None
