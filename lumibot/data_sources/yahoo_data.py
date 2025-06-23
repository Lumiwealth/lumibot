import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union

import numpy

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot.tools import YahooHelper

logger = logging.getLogger(__name__)


class YahooData(DataSourceBacktesting):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "15 minutes", "representations": ["15m", "15 minutes"]},
        {"timestep": "minute", "representations": ["1m", "1 minute"]},
    ]

    def __init__(self, *args, auto_adjust=False, datetime_start=None, datetime_end=None, **kwargs):
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
        logger.info(f"YahooData.__init__ calling super().__init__ with: datetime_start={datetime_start}, datetime_end={datetime_end}")
        
        # Pass datetime_start and datetime_end as keyword arguments only, not as positional args
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
        self.name = "yahoo"
        self.auto_adjust = auto_adjust
        self._data_store = {}

    def _append_data(self, asset, data):
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
        return data

    def _format_futures_symbol(self, symbol):
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
        if symbol.startswith('$'):
            symbol = symbol[1:]
        
        formatted_symbols = []
        
        # If already contains a dot (like ESZ23.CME), it's likely already properly formatted
        if '.' in symbol:
            # Already has exchange suffix, keep as is
            formatted_symbols.append(symbol)
            
            # Also try continuous contract (extract root symbol and add =F)
            parts = symbol.split('.')
            base_symbol = parts[0]
            # Extract root symbol (usually first 2 characters for most futures)
            root_symbol = ''.join([c for c in base_symbol if c.isalpha()])[:2]
            formatted_symbols.append(f"{root_symbol}=F")
            
        # If it has =F already, it's formatted for continuous contract
        elif '=F' in symbol:
            formatted_symbols.append(symbol)
            
        # No special formatting, try to determine if it's a specific contract or continuous
        else:
            # Check if it looks like a specific contract (e.g., ESH25)
            # Extract letters (should be root symbol + month code) and numbers (year)
            letters = ''.join([c for c in symbol if c.isalpha()])
            numbers = ''.join([c for c in symbol if c.isdigit()])
            
            # If it follows pattern of root + month code + year digits
            if len(letters) >= 3 and len(numbers) >= 1:
                # This looks like a specific contract, try both with and without exchange
                root_symbol = letters[:2]  # First two letters usually the root
                
                # Add with common exchanges
                for exchange in ['CME', 'NYMEX', 'CBOT', 'COMEX', 'NYBOT']:
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

    def _format_index_symbol(self, symbol):
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
        if symbol.startswith('^'):
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
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        # Log the current backtest datetime being processed
        logger.info(f"Inside _pull_source_symbol_bars for {asset.symbol}: self._datetime = {self._datetime}, requesting length {length}")
        
        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for YahooData, but {exchange} was passed as the exchange"
            )

        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        interval = self._parse_source_timestep(timestep, reverse=True)
        
        # Check if the asset is a futures contract or index and format the symbol accordingly
        symbol = asset.symbol
        symbols_to_try = [symbol]  # Default to just trying the original symbol
        
        if asset.asset_type == 'futures' or getattr(asset, 'asset_type', None) == Asset.AssetType.FUTURE:
            symbols_to_try = self._format_futures_symbol(symbol)
            if not isinstance(symbols_to_try, list):
                symbols_to_try = [symbols_to_try]
        elif asset.asset_type == 'index' or getattr(asset, 'asset_type', None) == Asset.AssetType.INDEX:
            symbols_to_try = self._format_index_symbol(symbol)
            if not isinstance(symbols_to_try, list):
                symbols_to_try = [symbols_to_try]
        
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            # Try each symbol format until we get data
            data = None
            successful_symbol = None
            
            for sym in symbols_to_try:
                logger.info(f"Attempting to fetch data for symbol: {sym}")
                try:
                    # Fetch data using the helper without restricting dates here
                    data = YahooHelper.get_symbol_data(
                        sym,
                        interval=interval,
                        auto_adjust=self.auto_adjust,
                        last_needed_datetime=self.datetime_end, # Keep this if needed for caching logic
                    )
                    if data is not None and data.shape[0] > 0:
                        logger.info(f"Successfully fetched data for symbol: {sym}")
                        successful_symbol = sym
                        break
                except Exception as e:
                    logger.warning(f"_pull_source_symbol_bars: Error fetching data for symbol {sym}: {str(e)}")
                    # Print the traceback for debugging
                    import traceback
                    traceback.print_exc()

                
            
            if data is None or data.shape[0] == 0:
                # Use self.datetime_start and self.datetime_end in the error message for clarity
                message = f"{self.SOURCE} did not return data for symbol {asset.symbol}. Tried: {symbols_to_try}. Make sure this symbol is valid and data exists for the period {self.datetime_start} to {self.datetime_end}."
                logging.error(message)
                return None
                
            data = self._append_data(asset, data)
            
            # Update the asset symbol to the successful one for future reference
            if successful_symbol and successful_symbol != asset.symbol:
                logger.info(f"Updating asset symbol from {asset.symbol} to successful format: {successful_symbol}")
                # We don't modify the asset directly, but we store the successful format for reference

        # --- Revised Filtering Logic ---
        # Use the current backtest datetime as the reference point
        current_dt = self.to_default_timezone(self._datetime)

        if timestep == "day":
            # For daily data, we want bars up to and including the current backtest day.
            # Filter data strictly *before* the start of the *next* day.
            dt = self._datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
            end_filter = dt - timedelta(days=1)
        else:
            # For intraday, filter up to the current datetime
            end_filter = current_dt

        if timeshift:
            # Ensure timeshift is a timedelta object
            if isinstance(timeshift, int):
                timeshift = timedelta(days=timeshift)
            end_filter = end_filter - timeshift

        # Filter the data store. Use '<' to get data strictly before the end_filter.
        result_data = data[data.index < end_filter]

        # Log if insufficient data is available before taking the tail
        if len(result_data) < length:
            logger.warning(
                f"Insufficient historical data for {asset.symbol} before {end_filter} "
                f"to satisfy length {length}. Available: {len(result_data)}. "
                f"Check backtest start date and data availability."
            )

        result = result_data.tail(length)
        return result

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=False
    ):
        """pull broker bars for a list assets"""

        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        interval = self._parse_source_timestep(timestep, reverse=True)
        missing_assets = []
        
        # Check for futures and index symbols and properly format them
        for asset in assets:
            if asset not in self._data_store:
                if asset.asset_type == Asset.AssetType.FUTURE:
                    symbol = self._format_futures_symbol(asset.symbol)
                    missing_assets.append(symbol)
                elif asset.asset_type == Asset.AssetType.INDEX:
                    symbol = self._format_index_symbol(asset.symbol)
                    missing_assets.append(symbol)
                else:
                    missing_assets.append(asset.symbol)

        if missing_assets:
            # Fetch data using the helper without restricting dates here
            dfs = YahooHelper.get_symbols_data(
                missing_assets, 
                interval=interval, 
                auto_adjust=self.auto_adjust
            )
            for symbol, df in dfs.items():
                # Find the corresponding asset for this symbol
                for asset in assets:
                    asset_symbol = asset.symbol
                    if asset.asset_type == Asset.AssetType.FUTURE:
                        asset_symbol = self._format_futures_symbol(asset_symbol)
                    elif asset.asset_type == Asset.AssetType.INDEX:
                        asset_symbol = self._format_index_symbol(asset_symbol)
                    
                    if asset_symbol == symbol:
                        self._append_data(asset, df)
                        break

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(asset, length, timestep=timestep, timeshift=timeshift)
        return result

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        """Takes an asset and returns the last known price"""
        if timestep is None:
            timestep = self.get_timestep()

        # Use -1 timeshift to get the price for the current bar (otherwise gets yesterdays prices)
        bars = self.get_historical_prices(asset, 1, timestep=timestep, quote=quote, timeshift=timedelta(days=-1))

        if isinstance(bars, float):
            return bars
        elif bars is None:
            return None

        open_ = bars.df.iloc[0].open
        if isinstance(open_, numpy.int64):
            open_ = Decimal(open_.item())
        return open_

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
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

    def get_strikes(self, asset):
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            # Create Asset with futures type if it appears to be a futures symbol
            if '.' in asset and any(exchange in asset for exchange in ['CME', 'NYMEX', 'CBOT', 'NYBOT', 'COMEX']):
                asset = Asset(symbol=asset, asset_type='futures')
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
