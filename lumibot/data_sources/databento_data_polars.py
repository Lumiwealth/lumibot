"""DataBento data source using Polars with proper Live API integration.

This implementation uses:
- Live API for data <24 hours old (real-time + intraday replay)
- Historical API for data >24 hours old
- Raw symbol format (ESZ5, NQZ5) for direct contract specification
- Trade aggregation for building minute bars with <1 minute lag
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional, Union
import time
import threading
from collections import defaultdict

import polars as pl
import databento as db

from lumibot.data_sources import DataSource
from lumibot.data_sources.polars_mixin import PolarsMixin
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper_polars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class DataBentoDataPolars(PolarsMixin, DataSource):
    """
    DataBento data source optimized with Polars and proper Live API usage.
    
    Uses Live API for <24 hour data to achieve <1 minute lag.
    Falls back to Historical API only for data >24 hours old.
    """
    
    SOURCE = "DATABENTO"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = {
        "minute": "1m",
        "day": "1d",
        "hour": "1h"
    }

    def __init__(
        self,
        api_key: str,
        has_paid_subscription: bool = False,
        enable_cache: bool = True,
        cache_duration_minutes: int = 60,
        enable_live_stream: bool = True,
        timeout: int = None,  # For backwards compatibility
        max_retries: int = None  # For backwards compatibility
    ):
        """Initialize DataBento data source with Live API support"""
        super().__init__(api_key=api_key, has_paid_subscription=has_paid_subscription)
        
        self._api_key = api_key
        self.has_paid_subscription = has_paid_subscription
        self.enable_cache = enable_cache
        self.cache_duration = timedelta(minutes=cache_duration_minutes)
        self.enable_live_stream = enable_live_stream
        
        # Initialize caches
        self._last_price_cache = {}
        self._eager_cache = {}
        self._filtered_data_cache = {}
        self._cache_metadata = {}
        self._cache_timestamps = {}
        
        # Live streaming components
        self._live_client = None
        self._live_thread = None
        self._stop_streaming = False
        self._minute_bars = defaultdict(dict)  # symbol -> minute -> bar
        self._current_bars = {}  # symbol -> current building bar
        self._finalized_minutes = defaultdict(set)  # symbol -> set of finalized minute timestamps
        self._published_bars = defaultdict(dict)  # symbol -> minute -> first published version
        self._subscribed_symbols = set()
        
        # Initialize Live API if enabled
        if self.enable_live_stream:
            self._init_live_streaming()
    
    def _init_live_streaming(self):
        """Initialize DataBento Live API client for real-time data"""
        try:
            self._live_client = db.Live(key=self._api_key)
            self._stop_streaming = False
            
            # Start live stream worker thread
            self._live_thread = threading.Thread(target=self._live_stream_worker, daemon=True)
            self._live_thread.start()
            
            logger.debug("DataBento Live API initialized for real-time streaming")
            
        except Exception as e:
            logger.error(f"Failed to initialize Live API: {e}")
            self.enable_live_stream = False
    
    def _live_stream_worker(self):
        """Worker thread that processes live streaming data"""
        try:
            logger.debug("Live stream worker started")
            
            # Keep thread alive - actual processing happens when subscribe is called
            while not self._stop_streaming:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Live stream worker error: {e}")
    
    def _subscribe_to_symbol(self, symbol: str, start_time: datetime = None, min_bars: int = 10):
        """Subscribe to a symbol on the Live API with replay for historical data"""
        if not self._live_client or symbol in self._subscribed_symbols:
            return
        
        try:
            # Ensure we get enough historical data
            # Request extra time to ensure we have enough bars
            if start_time is None:
                # Request 3x the bars we need with larger minimum to ensure we have enough data
                # This accounts for market gaps, low volume periods, and ensures full coverage
                start_time = datetime.now(timezone.utc) - timedelta(minutes=max(60, min_bars * 3))
            
            # Ensure we have enough buffer for the requested bars
            # DataBento Live API might limit replay, so request extra
            min_replay_minutes = max(60, min_bars * 5)  # 5x buffer for safety
            earliest_start = datetime.now(timezone.utc) - timedelta(minutes=min_replay_minutes)
            if start_time > earliest_start:
                start_time = earliest_start
            
            logger.debug(f"Subscribing to {symbol} via Live API (replay from {start_time})")
            
            # Subscribe with raw symbol format
            self._live_client.subscribe(
                dataset="GLBX.MDP3",
                schema="trades",
                stype_in="raw_symbol",
                symbols=[symbol],
                start=start_time.isoformat()
            )
            
            self._subscribed_symbols.add(symbol)
            
            # Start processing trades
            self._process_live_trades(symbol)
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {symbol}: {e}")
    
    def _process_live_trades(self, symbol: str):
        """Process live trades in a separate thread"""
        def _worker():
            try:
                logger.debug(f"Starting trade processor for {symbol}")
                processed_count = 0
                symbol_map = {}  # instrument_id -> symbol
                
                for record in self._live_client:
                    if self._stop_streaming:
                        break
                    
                    # Track symbol mappings
                    if isinstance(record, db.SymbolMappingMsg):
                        symbol_map[record.instrument_id] = record.stype_in_symbol
                        logger.debug(f"Symbol mapping: {record.instrument_id} -> {record.stype_in_symbol}")
                        
                    elif isinstance(record, db.TradeMsg):
                        # Map instrument_id to symbol
                        if record.instrument_id in symbol_map:
                            trade_symbol = symbol_map[record.instrument_id]
                        else:
                            trade_symbol = symbol  # Fallback to requested symbol
                            # Also try to extract from record if available
                            if hasattr(record, 'symbol'):
                                trade_symbol = record.symbol
                        
                        # Aggregate with the mapped symbol
                        self._aggregate_trade(record, trade_symbol)
                        processed_count += 1
                        
                        # Log progress
                        if processed_count % 1000 == 0:
                            logger.debug(f"Processed {processed_count} trades")
                        elif processed_count <= 5:
                            logger.debug(f"Trade #{processed_count}: {trade_symbol} @ {record.price}")
                
                logger.debug(f"Trade processor stopped after {processed_count} trades")
                    
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in trade processor: {e}")
        
        # Start worker in new thread
        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        logger.debug(f"Trade processor thread started for {symbol}")
    
    def _aggregate_trade(self, trade, symbol: str = None):
        """Aggregate a trade into minute bars"""
        # Use provided symbol or try to get from trade
        if symbol is None:
            symbol = getattr(trade, 'symbol', 'UNKNOWN')
        
        # Handle price scaling if needed (DataBento uses fixed-point)
        if hasattr(trade, 'price'):
            # DataBento prices may be in fixed-point format
            if trade.price > 1e10:  # Likely fixed-point
                price = float(trade.price) / 1e9
            else:
                price = float(trade.price)
        else:
            return
            
        size = int(trade.size) if hasattr(trade, 'size') else 0
        
        # Convert nanosecond timestamp to datetime
        ts = datetime.fromtimestamp(trade.ts_event / 1e9, tz=timezone.utc)
        minute = ts.replace(second=0, microsecond=0)
        
        # Check if this minute has been finalized - if so, don't update it
        if minute in self._finalized_minutes[symbol]:
            # Bar exists and is finalized, skip update
            return
        
        # Get current time to check if bar should be finalized
        current_time = datetime.now(timezone.utc)
        current_minute = current_time.replace(second=0, microsecond=0)
        
        # Get or create bar for this minute
        if symbol not in self._minute_bars:
            self._minute_bars[symbol] = {}
            logger.debug(f"Created new symbol entry for {symbol}")
        
        if minute not in self._minute_bars[symbol]:
            # New minute bar - create it even if it's old (for replay data)
            self._minute_bars[symbol][minute] = {
                'datetime': minute,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': size
            }
            logger.debug(f"New minute bar for {symbol} at {minute}: O={price}")
            
            # If this is an old bar (completed minute), finalize it immediately
            # A minute is complete once we've moved to the next minute
            if minute < current_minute:
                self._finalized_minutes[symbol].add(minute)
        else:
            # Update existing bar only if it's the current minute
            # We only update bars for the current incomplete minute
            if minute == current_minute:
                bar = self._minute_bars[symbol][minute]
                bar['high'] = max(bar['high'], price)
                bar['low'] = min(bar['low'], price)
                bar['close'] = price
                bar['volume'] += size
            # Otherwise finalize it if not already finalized
            elif minute not in self._finalized_minutes[symbol]:
                self._finalized_minutes[symbol].add(minute)
    
    def _finalize_old_bars(self, symbol: str, current_time: datetime):
        """Mark bars older than 1 minute as finalized"""
        if symbol not in self._minute_bars:
            return
            
        current_minute = current_time.replace(second=0, microsecond=0)
        cutoff = current_minute - timedelta(minutes=1)
        
        for minute in list(self._minute_bars[symbol].keys()):
            if minute < cutoff and minute not in self._finalized_minutes[symbol]:
                self._finalized_minutes[symbol].add(minute)
    
    def _get_live_bars(self, symbol: str, length: int, current_time: datetime) -> Optional[pl.DataFrame]:
        """Get minute bars from live aggregated data"""
        # First finalize any old bars
        for tracked_symbol in list(self._minute_bars.keys()):
            self._finalize_old_bars(tracked_symbol, current_time)
        
        # Check all tracked symbols, not just the requested one
        logger.debug(f"Checking for bars: symbol={symbol}, tracked symbols={list(self._minute_bars.keys())}")
        
        # Check if we have any bars at all
        if not self._minute_bars:
            logger.debug("No minute bars aggregated yet")
            return None
            
        # Find bars for this symbol or any matching symbol
        bars_dict = None
        for tracked_symbol in self._minute_bars.keys():
            if symbol in tracked_symbol or tracked_symbol in symbol:
                bars_dict = self._minute_bars[tracked_symbol]
                logger.debug(f"Found bars under symbol {tracked_symbol}")
                break
        
        if not bars_dict:
            logger.debug(f"No bars found for {symbol}")
            return None
        
        # Convert bars to list and sort by time
        # Exclude the current incomplete minute from historical data
        current_minute = current_time.replace(second=0, microsecond=0)
        bars = []
        for minute, bar_data in sorted(bars_dict.items()):
            # Don't include the current minute as it's still forming
            if minute < current_minute:
                bars.append(bar_data)
        
        logger.debug(f"Found {len(bars)} total bars for {symbol}")
        
        # Take the most recent bars
        if len(bars) > length:
            bars = bars[-length:]
        
        if bars:
            df = pl.DataFrame(bars)
            
            # Log latency
            latest_time = df['datetime'].max()
            lag_seconds = (current_time - latest_time).total_seconds()
            logger.debug(f"Live bars: {len(df)} bars, lag: {lag_seconds:.0f} seconds")
            
            return df
        
        return None
    
    def _should_use_live_api(self, start_time: datetime, end_time: datetime) -> bool:
        """Determine if we should use Live API based on 24-hour cutoff"""
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(hours=24)
        
        # Use Live API if any part of the requested range is within 24 hours
        return end_time > cutoff_time
    
    def _resolve_futures_symbol(self, asset: Asset, reference_date: datetime = None) -> str:
        """Resolve asset to specific futures contract symbol"""
        if asset.asset_type in [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]:
            # Use existing resolution logic
            if hasattr(asset, 'resolve_continuous_futures_contract'):
                resolved = asset.resolve_continuous_futures_contract(reference_date)
                # DataBento uses single digit year for CME futures (ESZ5 not ESZ25)
                # Check if it's a 5-character symbol ending with 2 digits (year)
                if len(resolved) >= 5:
                    # Extract base and check if last 2 chars are digits (year)
                    base = resolved[:-2]
                    year_part = resolved[-2:]
                    if year_part.isdigit() and int(year_part) >= 20:
                        # Convert to single digit year (e.g., 25 -> 5, 24 -> 4)
                        single_year = resolved[-1]
                        return base + single_year
                return resolved
            
            # Fallback to manual resolution for common futures
            symbol = asset.symbol.upper()
            
            # Determine current contract month and year
            month = reference_date.month if reference_date else datetime.now().month
            year = reference_date.year if reference_date else datetime.now().year
            
            # Quarterly contracts: H(Mar), M(Jun), U(Sep), Z(Dec)
            if month <= 3:
                month_code = 'H'
            elif month <= 6:
                month_code = 'M'
            elif month <= 9:
                month_code = 'U'
            else:
                month_code = 'Z'
            
            # Use single digit year for CME (5 for 2025, 4 for 2024)
            year_digit = year % 10
            
            # Handle various futures symbols
            if symbol in ["ES", "NQ", "RTY", "YM"]:  # E-mini futures
                return f"{symbol}{month_code}{year_digit}"
            elif symbol in ["MES", "MNQ", "MYM", "M2K"]:  # Micro futures
                return f"{symbol}{month_code}{year_digit}"
            elif symbol in ["CL", "GC", "SI"]:  # Commodities
                return f"{symbol}{month_code}{year_digit}"
            
            # For unknown symbols, return as-is
            return asset.symbol
    
    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
        return_polars: bool = False
    ) -> Optional[Bars]:
        """Get historical prices using appropriate API based on time range"""
        
        # Validate asset type
        supported_types = [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]
        if asset.asset_type not in supported_types:
            logger.error(f"DataBento only supports futures. Got: {asset.asset_type}")
            return None
        
        # Calculate time range
        current_time = datetime.now(timezone.utc)
        if timeshift:
            current_time = current_time - timeshift
        
        # Determine how far back we need data
        step = timestep.lower()
        if step == "minute":
            # For minute bars, request significantly more data to ensure we have enough bars
            # Futures markets can have gaps, so we need a larger buffer
            time_needed = timedelta(minutes=max(length * 3, 30))
        elif step == "hour":
            time_needed = timedelta(hours=max(length * 2, 12))
        elif step == "day":
            time_needed = timedelta(days=max(length * 2, 10))
        else:
            time_needed = timedelta(days=length * 2)
        
        start_time = current_time - time_needed
        
        # Resolve to specific contract
        symbol = self._resolve_futures_symbol(asset, current_time)
        logger.debug(f"Resolved {asset.symbol} to {symbol}")
        
        # Determine which API to use
        cutoff_time = current_time - timedelta(hours=24)
        
        if start_time > cutoff_time and self.enable_live_stream:
            # All data is within 24 hours - use Live API
            logger.debug(f"Using Live API for {symbol} (all data <24h old)")
            
            # Subscribe if not already, ensuring we get enough bars
            if symbol not in self._subscribed_symbols:
                # Request extra bars to ensure we have enough after filtering
                self._subscribe_to_symbol(symbol, start_time, min_bars=length * 2)
                # Wait longer for initial data processing
                logger.debug(f"Waiting for Live API data to process {length} bars...")
                time.sleep(5)
            else:
                # Already subscribed, but check if we have enough bars
                # If not, we might need to wait for more data to accumulate
                logger.debug(f"Already subscribed to {symbol}, checking available bars")
            
            # Try to get bars from live aggregation multiple times
            for attempt in range(3):
                live_bars = self._get_live_bars(symbol, length, current_time)
                
                if live_bars is not None and len(live_bars) >= length:
                    # We have enough bars from Live API
                    logger.debug(f"Got {len(live_bars)} bars from Live API on attempt {attempt + 1}")
                    return Bars(
                        df=live_bars,
                        source=self.SOURCE,
                        asset=asset,
                        quote=quote,
                        return_polars=return_polars
                    )
                elif live_bars is not None and len(live_bars) > 0:
                    # We have some bars but not enough
                    logger.debug(f"Got {len(live_bars)} bars from Live API, but need {length}")
                    if len(live_bars) >= length * 0.5:  # If we have at least half
                        logger.debug(f"Returning {len(live_bars)} bars (partial)")
                        return Bars(
                            df=live_bars,
                            source=self.SOURCE,
                            asset=asset,
                            quote=quote,
                            return_polars=return_polars
                        )
                
                if attempt < 2:
                    logger.debug(f"Waiting for more data... (attempt {attempt + 1}/3)")
                    time.sleep(2)
            
            # Fallback to Historical if not enough live data
            if live_bars is not None and len(live_bars) > 0:
                logger.warning(f"Only got {len(live_bars)} bars from Live API, falling back to Historical")
            else:
                logger.warning("No live bars available after 3 attempts, falling back to Historical API")
        
        # Use Historical API (either for old data or as fallback)
        logger.debug(f"Using Historical API for {symbol}")
        
        df = databento_helper_polars.get_price_data_from_databento_polars(
            api_key=self._api_key,
            asset=asset,
            start=start_time,
            end=current_time,
            timestep=step,
            venue=exchange,
            force_cache_update=True if step in ("minute", "hour") else False
        )
        
        if df is not None and not df.is_empty():
            # Take only requested length
            df = df.tail(length)
            
            return Bars(
                df=df,
                source=self.SOURCE,
                asset=asset,
                quote=quote,
                return_polars=return_polars
            )
        
        return None
    
    def get_last_price(self, asset: Asset, quote: Optional[Asset] = None, exchange: Optional[str] = None) -> Optional[float]:
        """Get the last price for an asset"""
        # Try to get from live data first
        if self.enable_live_stream:
            symbol = self._resolve_futures_symbol(asset)
            if symbol in self._minute_bars and self._minute_bars[symbol]:
                # Get the most recent bar
                latest_minute = max(self._minute_bars[symbol].keys())
                latest_bar = self._minute_bars[symbol][latest_minute]
                return float(latest_bar['close'])
        
        # Fallback to getting last bar
        bars = self.get_historical_prices(asset, 1, "minute", exchange=exchange)
        if bars and len(bars) > 0:
            return float(bars.df['close'].tail(1).item())
        
        return None
    
    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None) -> dict:
        """Get option chains - not supported for futures data source"""
        logger.warning("DataBento is a futures data source and does not support option chains")
        return {"Chains": {}, "Multiplier": 1, "Exchange": exchange or ""}
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, '_stop_streaming'):
            self._stop_streaming = True
        if hasattr(self, '_live_thread') and self._live_thread:
            self._live_thread.join(timeout=1)