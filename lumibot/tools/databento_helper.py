# This file contains helper functions for getting data from DataBento
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset
from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.error_logger import ErrorLogger

# Set up module-specific logger
logger = logging.getLogger(__name__)

# DataBento imports (will be installed as dependency)
try:
    import databento as db
    from databento import Historical
    DATABENTO_AVAILABLE = True
except ImportError:
    DATABENTO_AVAILABLE = False
    logger.warning("DataBento package not available. Please install with: pip install databento")

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
        logger.warning(f"Could not create DataBento cache folder: {e}")


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
    
    def get_available_range(self, dataset: str) -> Dict[str, str]:
        """Get the available date range for a dataset"""
        try:
            return self.client.metadata.get_dataset_range(dataset=dataset)
        except Exception as e:
            logger.warning(f"Could not get dataset range for {dataset}: {e}")
            return {}

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
        Get historical data from DataBento (no retry logic - fail fast on parameter errors)
        
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
        # Get available range to clamp end date
        available_range = self.get_available_range(dataset)
        if available_range and 'end' in available_range:
            available_end = pd.to_datetime(available_range['end'])
            request_end = pd.to_datetime(end)
            
            # Clamp end date to available range
            if request_end > available_end:
                logger.info(f"Clamping end date from {end} to available end: {available_end}")
                end = available_end
        
        logger.info(f"Requesting DataBento data: {symbols} from {start} to {end}")
        logger.info(f"Making DataBento API call with: dataset={dataset}, symbols={symbols}, schema={schema}")
        
        try:
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
            
            logger.info(f"Successfully retrieved {len(df)} rows from DataBento for symbols: {symbols}")
            return df
            
        except Exception as e:
            error_msg = f"DataBento API error: {str(e)}"
            logger.error(error_msg)
            
            if error_logger:
                error_logger.log_error(
                    severity="ERROR",
                    error_code="API_ERROR", 
                    message=f"DataBento API error: {str(e)}",
                    details=f"Symbols: {symbols}, Start: {start}, End: {end}"
                )
            
            raise e


def _format_futures_symbol_for_databento(asset: Asset) -> str:
    """
    Format a futures Asset object for DataBento symbol conventions
    
    This function handles the complexity of DataBento's futures symbology, which may
    differ from standard CME formats. It provides multiple fallback strategies
    when symbols don't resolve.
    
    DataBento Symbol Format Notes:
    - Some symbols in GLBX.MDP3 dataset use different formats than expected
    - Standard CME formats like MESU25 may not work
    - This function tries multiple formats and provides actionable error messages
    
    For continuous futures (CONT_FUTURE), automatically resolve to the current active contract.
    For specific contracts (FUTURE), format with month code and year if expiration is provided.
    
    Parameters
    ----------
    asset : Asset
        Lumibot Asset object with asset_type='future' or 'cont_future'
        
    Returns
    -------
    str
        DataBento-formatted futures symbol (specific contract for cont_future, or raw symbol for regular future)
        
    Raises
    ------
    ValueError
        If symbol resolution fails with actionable error message
    """
    symbol = asset.symbol
    
    # For continuous contracts, resolve to current active contract using Asset class methods
    if asset.asset_type == Asset.AssetType.CONT_FUTURE:
        logger.info(f"Resolving continuous futures symbol: {symbol}")
        
        # Use the Asset class method for contract resolution
        try:
            active_contract = asset.resolve_continuous_futures_contract()
            logger.info(f"Resolved {symbol} continuous -> {active_contract}")
            
            # Generate alternative symbol formats for DataBento
            alternatives = _generate_databento_symbol_alternatives(symbol, active_contract)
            logger.info(f"Generated DataBento symbol alternatives: {alternatives}")
            
            return alternatives[0]  # Return primary format, alternatives will be tried in get_price_data_from_databento
            
        except Exception as e:
            error_msg = f"Error resolving continuous futures contract for {symbol}: {e}"
            logger.error(error_msg)
            
            # Provide actionable error message
            raise ValueError(
                f"Failed to resolve continuous futures contract for {symbol}. "
                f"Error: {e}. "
                f"Please check that the asset symbol '{symbol}' is valid for continuous futures. "
                f"Consult DataBento documentation at https://databento.com/docs/api-reference-historical/basics/symbology "
                f"for correct symbol formats."
            )
    
    # For specific futures contracts, format with expiration if provided
    if asset.asset_type == Asset.AssetType.FUTURE and asset.expiration:
        # DataBento uses month codes for specific contracts
        month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        year = asset.expiration.year % 100  # Last 2 digits of year
        month_code = month_codes.get(asset.expiration.month, 'H')
        
        # Format as SYMBOL{MONTH_CODE}{YY} (e.g., ESH25 for March 2025)
        formatted_symbol = f"{symbol}{month_code}{year:02d}"
        
        logger.info(f"Formatted specific futures symbol: {asset.symbol} {asset.expiration} -> {formatted_symbol}")
        return formatted_symbol
    
    # For regular futures without expiration, return raw symbol (no resolution)
    logger.info(f"Using raw futures symbol: {symbol}")
    return symbol


def _determine_databento_dataset_from_symbol(root_symbol: str) -> str:
    """
    Determine DataBento dataset from root symbol
    
    Parameters
    ----------
    root_symbol : str
        Root futures symbol
        
    Returns
    -------
    str
        DataBento dataset name
    """
    # Most futures are on CME and use GLBX.MDP3
    cme_symbols = ['ES', 'MES', 'NQ', 'MNQ', 'RTY', 'M2K', 'YM', 'MYM']
    
    if root_symbol in cme_symbols:
        return "GLBX.MDP3"
    
    # Default to CME
    return "GLBX.MDP3"


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
    # For futures (ES, MES, etc.), use GLBX.MDP3 (CME Group data)
    if asset.asset_type in ['future', 'futures', 'cont_future']:
        if venue:
            venue_upper = venue.upper()
            if venue_upper in ['CME', 'CBOT', 'NYMEX', 'COMEX']:
                return 'GLBX.MDP3'
            elif venue_upper in ['ICE']:
                return 'IFEU.IMPACT'
        
        # Default for futures is CME Group data
        logger.info("Using GLBX.MDP3 dataset for futures (CME Group)")
        return 'GLBX.MDP3'
    
    elif asset.asset_type in ['stock', 'equity']:
        # Default to NASDAQ for equities
        logger.info("Using XNAS.ITCH dataset for equities")
        return 'XNAS.ITCH'
    
    # Default fallback for other asset types
    logger.info("Using GLBX.MDP3 as default dataset")
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
        logger.warning(f"Error loading cache file {cache_file}: {e}")
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
        logger.debug(f"Cached data saved to {cache_file}")
    except Exception as e:
        logger.warning(f"Error saving cache file {cache_file}: {e}")


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
        logger.warning(f"Missing required columns in DataBento data: {missing_cols}")
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
        logger.error("DataBento package not available. Please install with: pip install databento")
        return None
    
    # Build cache filename
    cache_file = _build_cache_filename(asset, start, end, timestep)
    
    # Try to load from cache first
    if not force_cache_update:
        cached_data = _load_cache(cache_file)
        if cached_data is not None and not cached_data.empty:
            logger.debug(f"Loaded DataBento data from cache: {cache_file}")
            return cached_data
    
    # Initialize DataBento client
    try:
        client = DataBentoClient(api_key=api_key)
        
        # Determine dataset and schema
        dataset = _determine_databento_dataset(asset, venue)
        schema = _determine_databento_schema(timestep)
        
        # For continuous futures, resolve to a specific contract FIRST
        # DataBento does not support continuous futures directly - we must resolve to actual contracts
        if asset.asset_type == Asset.AssetType.CONT_FUTURE:
            # Use Asset class method to resolve continuous futures to actual contract (returns string)
            resolved_symbol = asset.resolve_continuous_futures_contract()
            if resolved_symbol is None:
                logger.error(f"Could not resolve continuous futures contract for {asset.symbol}")
                return None
                
            # Generate the correct DataBento symbol format (should be single result)
            symbols_to_try = _generate_databento_symbol_alternatives(asset.symbol, resolved_symbol)
            logger.info(f"Resolved continuous future {asset.symbol} to specific contract: {resolved_symbol}")
            logger.info(f"DataBento symbol format: {symbols_to_try[0]}")
        else:
            # For specific contracts, just use the formatted symbol
            symbol = _format_futures_symbol_for_databento(asset)
            symbols_to_try = [symbol]
        
        # Use the correct symbol format (no more guessing/multiple attempts)
        symbol_to_use = symbols_to_try[0]
        
        try:
            logger.info(f"Fetching DataBento data for symbol: {symbol_to_use}")
            logger.info(f"DataBento request details: dataset={dataset}, symbol={symbol_to_use}, schema={schema}, start={start}, end={end}")
            
            df = client.get_historical_data(
                dataset=dataset,
                symbols=symbol_to_use,
                schema=schema,
                start=start,
                end=end,
                **kwargs
            )
            
            if df is not None and not df.empty:
                logger.info(f"✓ SUCCESS: Retrieved {len(df)} rows for symbol: {symbol_to_use}")
                
                # Normalize the data
                df_normalized = _normalize_databento_dataframe(df)
                
                # Cache the data
                _save_cache(df_normalized, cache_file)
                
                logger.debug(f"Successfully retrieved and cached {len(df_normalized)} rows")
                return df_normalized
            else:
                logger.warning(f"✗ No data returned for symbol: {symbol_to_use}")
                return None
                
        except Exception as e:
            error_str = str(e).lower()
            if "symbology_invalid_request" in error_str or "none of the symbols could be resolved" in error_str:
                logger.error(f"❌ DataBento symbol resolution FAILED for {asset.symbol}")
                logger.error(f"Symbol used: {symbol_to_use}")
                logger.error("This indicates:")
                logger.error("1. Contract may not be available in DataBento GLBX.MDP3 dataset")
                logger.error("2. Data may not be available for the requested time range")
                logger.error("3. Markets may be closed (weekend/holiday)")
                logger.error("Check DataBento documentation: https://databento.com/docs/api-reference-historical/basics/symbology")
                logger.error(f"Last error encountered: {e}")
                return None
            else:
                # Non-symbol related error, re-raise
                logger.error(f"DataBento API error (not symbol-related): {e}")
                raise e
        
    except Exception as e:
        error_msg = f"Error fetching DataBento data for {asset.symbol}: {str(e)}"
        logger.error(error_msg)
        
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
        logger.error("DataBento package not available")
        return None
    
    try:
        # For last price, get the most recent available data
        dataset = _determine_databento_dataset(asset, venue)
        
        # For continuous futures, resolve to the current active contract
        if asset.asset_type == Asset.AssetType.CONT_FUTURE:
            # Use Asset class method to resolve continuous futures to actual contract (returns string)
            resolved_symbol = asset.resolve_continuous_futures_contract()
            if resolved_symbol is None:
                logger.error(f"Could not resolve continuous futures contract for {asset.symbol}")
                return None
            # Generate the correct DataBento symbol format (should be single result)
            symbols_to_try = _generate_databento_symbol_alternatives(asset.symbol, resolved_symbol)
            logger.info(f"Resolved continuous future {asset.symbol} to specific contract: {resolved_symbol}")
            logger.info(f"DataBento symbol format for last price: {symbols_to_try[0]}")
        else:
            # For specific contracts, just use the formatted symbol
            symbol = _format_futures_symbol_for_databento(asset)
            symbols_to_try = [symbol]
        
        # Get available range first
        client = Historical(api_key)
        try:
            range_result = client.metadata.get_dataset_range(dataset=dataset)
            # Handle different response formats
            if hasattr(range_result, 'end') and range_result.end:
                if hasattr(range_result.end, 'tz_localize'):
                    # Already a pandas Timestamp
                    available_end = range_result.end if range_result.end.tz else range_result.end.tz_localize('UTC')
                else:
                    # Convert to pandas Timestamp
                    available_end = pd.to_datetime(range_result.end).tz_localize('UTC')
            elif isinstance(range_result, dict) and 'end' in range_result:
                available_end = pd.to_datetime(range_result['end']).tz_localize('UTC')
            else:
                logger.warning(f"Could not parse dataset range for {dataset}: {range_result}")
                # Fallback: use a recent date that's likely to have data
                available_end = datetime.now(tz=timezone.utc) - timedelta(days=1)
        except Exception as e:
            logger.warning(f"Could not get dataset range for {dataset}: {e}")
            # Fallback: use a recent date that's likely to have data
            available_end = datetime.now(tz=timezone.utc) - timedelta(days=1)
        
        # Request the most recent available data (work backwards from available end)
        end_date = available_end
        start_date = end_date - timedelta(hours=6)  # Get last 6 hours of available data
        
        # Ensure we don't go too far back
        min_start = end_date - timedelta(days=7)
        if start_date < min_start:
            start_date = min_start
        
        # Use the correct symbol format (no more guessing/multiple attempts)
        symbol_to_use = symbols_to_try[0]
        
        try:
            logger.info(f"Getting last price for {asset.symbol} -> using symbol {symbol_to_use}")
            
            # Get recent data to extract last price
            data = client.timeseries.get_range(
                dataset=dataset,
                symbols=symbol_to_use,
                schema='ohlcv-1m',  # Use minute data for most recent price
                start=start_date,
                end=end_date,
                **kwargs
            )
            
            if data is not None:
                # Convert to DataFrame if needed
                if hasattr(data, 'to_df'):
                    df = data.to_df()
                else:
                    df = pd.DataFrame(data)
                
                if not df.empty:
                    # Get the last available price (close price of most recent bar)
                    if 'close' in df.columns:
                        price = df['close'].iloc[-1]
                        if pd.notna(price):
                            logger.info(f"✓ SUCCESS: Got last price for {symbol_to_use}: {price}")
                            return float(price)
                    
                    logger.warning(f"✗ No valid close price found for symbol '{symbol_to_use}'")
                    return None
                else:
                    logger.warning(f"✗ No data returned for symbol '{symbol_to_use}'")
                    return None
            else:
                logger.warning(f"✗ No data object returned for symbol '{symbol_to_use}'")
                return None
                
        except Exception as e:
            error_str = str(e).lower()
            if "symbology_invalid_request" in error_str or "none of the symbols could be resolved" in error_str:
                logger.error(f"❌ DataBento symbol resolution FAILED for last price: {asset.symbol}")
                logger.error(f"Symbol used: {symbol_to_use}")
                logger.error("This indicates the contract may not be available in DataBento GLBX.MDP3 dataset")
                return None
            else:
                # Non-symbol related error, re-raise
                logger.error(f"DataBento API error (not symbol-related): {e}")
                raise e
            
    except Exception as e:
        logger.error(f"Error getting last price from DataBento for {asset.symbol}: {e}")
        return None
    return None


def _generate_databento_symbol_alternatives(base_symbol: str, resolved_contract: str) -> List[str]:
    """
    Format futures symbol for DataBento using the correct symbology.
    
    DataBento GLBX.MDP3 dataset uses the format: {ROOT}{MONTH_CODE}{YEAR_DIGIT}
    Examples: ESM2 (E-mini June 2022), MESU5 (Micro E-mini September 2025)
    
    This function no longer tries multiple alternatives - it uses the correct format
    on the first attempt based on DataBento's documented symbology standards.
    
    Parameters
    ----------
    base_symbol : str
        Base futures symbol (e.g., 'MES', 'ES')
    resolved_contract : str
        Resolved contract from Asset class (e.g., 'MESU25')
        
    Returns
    -------
    List[str]
        Single-item list with the correct DataBento symbol format
    """
    # Extract month and year from resolved contract (e.g., MESU25 -> U, 5)
    if len(resolved_contract) >= len(base_symbol) + 3:
        # For contracts like MESU25: month=U, year=5 (from 25)
        month_char = resolved_contract[len(base_symbol)]  # Month code after base symbol
        year_digits = resolved_contract[len(base_symbol) + 1:]  # Year part (e.g., "25")
        year_char = year_digits[-1]  # Last digit of year (e.g., "5" from "25")
        
        # DataBento format: {ROOT}{MONTH_CODE}{YEAR_DIGIT}
        databento_symbol = f"{base_symbol}{month_char}{year_char}"
        
        logger.info(f"DataBento symbol format: {resolved_contract} -> {databento_symbol}")
        return [databento_symbol]
    else:
        # Fallback for unexpected contract format - use original contract
        logger.warning(f"Unexpected contract format: {resolved_contract}, using as-is")
        return [resolved_contract]
