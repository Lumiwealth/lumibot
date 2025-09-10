"""DataBento data source using Polars with proper Live API integration - FIXED VERSION.

This implementation uses:
- Live API for real-time data streaming
- Historical API for data >24 hours old
- Proper handling of DataBento message types
- Correct price conversion from fixed-point format
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional, Union
import time
import threading
import queue
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
    
    Uses Live API for real-time trade streaming to achieve <1 minute lag.
    Falls back to Historical API for older data.
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

        # Core configuration
        self._api_key = api_key
        self.has_paid_subscription = has_paid_subscription
        self.enable_cache = enable_cache
        self.cache_duration = timedelta(minutes=cache_duration_minutes)
        self.enable_live_stream = enable_live_stream

        # Caches
        self._last_price_cache = {}
        self._eager_cache = {}
        self._filtered_data_cache = {}
        self._cache_metadata = {}
        self._cache_timestamps = {}

        # Live streaming state
        self._live_client = None
        self._producer_threads = {}  # Map symbol to producer thread
        self._consumer_thread = None
        self._finalizer_thread = None
        self._stop_streaming = False
        self._minute_bars = defaultdict(dict)
        self._bars_lock = threading.Lock()
        self._finalized_minutes = defaultdict(set)
        self._subscribed_symbols = set()
        self._last_trade_time = {}
        self._last_ts_event = {}  # Track last timestamp per symbol for reconnection
        self._symbol_mapping = {}  # Maps instrument_id to symbol
        self._record_queue = queue.Queue(maxsize=10000)
        self._reconnect_backoff = 1.0
        
        # Configuration
        self._finalize_grace_seconds = 3  # Wait 3 seconds after minute ends to finalize
        self._prune_older_minutes = 720  # Remove bars older than 12 hours
        self._resub_overlap_seconds = 5  # Overlap on reconnection

        if self.enable_live_stream:
            self._init_live_streaming()
    
    def _init_live_streaming(self):
        """Initialize DataBento Live API client for real-time data"""
        try:
            self._stop_streaming = False
            
            # Start consumer thread to process records from queue
            self._consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)
            self._consumer_thread.start()
            
            # Start finalizer thread to mark old bars as complete
            self._finalizer_thread = threading.Thread(target=self._finalizer_loop, daemon=True)
            self._finalizer_thread.start()
            
            logger.debug("[DATABENTO][LIVE] Live streaming threads initialized")
            
        except Exception as e:
            logger.error(f"[DATABENTO][LIVE] Failed to initialize Live streaming: {e}", exc_info=True)
            self.enable_live_stream = False
    
    def _live_stream_worker(self, symbol: str, start_time: datetime):
        """Producer thread that subscribes and iterates in the same context"""
        logger.debug(f"[DATABENTO][PRODUCER] Starting for {symbol}")
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        backoff_seconds = 1
        
        while not self._stop_streaming and reconnect_attempts < max_reconnect_attempts:
            try:
                # Create a new client for this producer
                client = db.Live(key=self._api_key)
                
                logger.debug(f"[DATABENTO][PRODUCER] Subscribing to {symbol} from {start_time.isoformat()}")
                
                # Subscribe - must happen in same context as iteration
                client.subscribe(
                    dataset="GLBX.MDP3",
                    schema="trades",
                    stype_in="raw_symbol",
                    symbols=[symbol],
                    start=start_time.isoformat()
                )
                
                # Immediately iterate in the SAME context
                record_count = 0
                error_count = 0
                
                for record in client:
                    if self._stop_streaming:
                        break
                    
                    record_count += 1
                    
                    # Handle ErrorMsg records
                    if hasattr(record, '__class__') and record.__class__.__name__ == 'ErrorMsg':
                        error_count += 1
                        err_msg = getattr(record, 'err', 'Unknown error')
                        logger.error(f"[DATABENTO][PRODUCER] Error from server: {err_msg}")
                        if error_count > 3:
                            logger.error(f"[DATABENTO][PRODUCER] Too many errors, will reconnect")
                            break
                        continue
                    
                    # Reset error count on successful records
                    error_count = 0
                    
                    # Put record in queue for consumer
                    try:
                        self._record_queue.put((symbol, record), timeout=0.1)
                        
                        # Track last event timestamp for reconnection
                        if hasattr(record, 'ts_event'):
                            self._last_ts_event[symbol] = getattr(record, 'ts_event')
                        
                        # Log progress (only first few)
                        if record_count <= 3:
                            logger.debug(f"[DATABENTO][PRODUCER] {symbol} record #{record_count}: {record.__class__.__name__}")
                    
                    except queue.Full:
                        logger.warning(f"[DATABENTO][PRODUCER] Queue full, dropping record")
                
                # Clean exit
                logger.info(f"[DATABENTO][PRODUCER] {symbol} stopped after {record_count} records")
                break  # Successful completion
                
            except Exception as e:
                logger.error(f"[DATABENTO][PRODUCER] {symbol} error: {e}")
                reconnect_attempts += 1
                
                if reconnect_attempts < max_reconnect_attempts:
                    sleep_time = backoff_seconds * (2 ** reconnect_attempts)
                    logger.info(f"[DATABENTO][PRODUCER] Reconnecting {symbol} in {sleep_time}s (attempt {reconnect_attempts})")
                    time.sleep(sleep_time)
                    
                    # Update start time for reconnection to avoid duplicate data
                    if symbol in self._last_ts_event:
                        # Start from last received timestamp (databento timestamps are in nanoseconds)
                        ts_ns = self._last_ts_event[symbol]
                        if ts_ns > 0:
                            start_time = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
                        logger.info(f"[DATABENTO][PRODUCER] Resuming from last event: {start_time.isoformat()}")
                else:
                    logger.error(f"[DATABENTO][PRODUCER] {symbol} max reconnection attempts reached")
    
    def _subscribe_to_symbol(self, symbol: str, start_time: datetime = None, min_bars: int = 10):
        """Start a producer thread for a symbol"""
        if symbol in self._subscribed_symbols:
            logger.debug(f"[DATABENTO][LIVE] {symbol} already subscribed")
            return
        
        try:
            # Calculate start time for replay
            if start_time is None:
                # Request enough history to build minute bars
                start_time = datetime.now(timezone.utc) - timedelta(minutes=max(30, min_bars * 2))
            
            logger.debug(f"[DATABENTO][LIVE] Starting producer for {symbol}")
            logger.debug(f"[DATABENTO][LIVE]   Replay from: {start_time.isoformat()}")
            
            # Start producer thread for this symbol
            producer_thread = threading.Thread(
                target=self._live_stream_worker,
                args=(symbol, start_time),
                daemon=True,
                name=f"databento-producer-{symbol}"
            )
            producer_thread.start()
            
            self._subscribed_symbols.add(symbol)
            self._producer_threads[symbol] = producer_thread
            logger.debug(f"[DATABENTO][LIVE] Producer started for {symbol}")
            
        except Exception as e:
            logger.error(f"[DATABENTO][LIVE] Failed to start producer for {symbol}: {e}", exc_info=True)
    
    def _consumer_loop(self):
        """Consumer thread that processes records from the queue"""
        logger.debug("[DATABENTO][CONSUMER] Started")
        trade_count = 0
        
        while not self._stop_streaming:
            try:
                # Get record from queue with timeout
                symbol, record = self._record_queue.get(timeout=1.0)
                
                # Handle symbol mappings
                if hasattr(record, '__class__') and record.__class__.__name__ == 'SymbolMappingMsg':
                    instrument_id = getattr(record, 'instrument_id', None)
                    if instrument_id:
                        for attr in ['raw_symbol', 'stype_out_symbol', 'symbol']:
                            mapped_symbol = getattr(record, attr, None)
                            if mapped_symbol:
                                self._symbol_mapping[instrument_id] = mapped_symbol
                                logger.debug(f"[DATABENTO][CONSUMER] Symbol mapping: {instrument_id} -> {mapped_symbol}")
                                break
                
                # Process trade messages
                elif hasattr(record, '__class__') and record.__class__.__name__ == 'TradeMsg':
                    instrument_id = getattr(record, 'instrument_id', None)
                    
                    # Try to get symbol from mapping or use provided symbol
                    actual_symbol = self._symbol_mapping.get(instrument_id, symbol)
                    
                    # Process the trade
                    if actual_symbol:
                        self._last_trade_time[actual_symbol] = datetime.now(timezone.utc)
                        trade_count += 1
                        
                        # Log only first few trades for verification
                        if trade_count <= 3:
                            raw_price = getattr(record, 'price', 0)
                            price = raw_price / 1e9 if raw_price > 1e10 else raw_price
                            size = getattr(record, 'size', 0)
                            logger.debug(f"[DATABENTO][CONSUMER] Trade #{trade_count} {actual_symbol} @ {price:.2f} size={size}")
                        
                        # Aggregate the trade into minute bars
                        self._aggregate_trade(record, actual_symbol)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"[DATABENTO][CONSUMER] Error processing record: {e}")
        
        logger.info(f"[DATABENTO][CONSUMER] Stopped after {trade_count} trades")
    
    def _finalizer_loop(self):
        """Finalizer thread that marks old bars as complete"""
        logger.debug("[DATABENTO][FINALIZER] Started")
        
        while not self._stop_streaming:
            try:
                time.sleep(5)  # Check every 5 seconds
                
                current_time = datetime.now(timezone.utc)
                cutoff_time = current_time - timedelta(seconds=self._finalize_grace_seconds)
                cutoff_minute = cutoff_time.replace(second=0, microsecond=0)
                
                with self._bars_lock:
                    for symbol in list(self._minute_bars.keys()):
                        # Finalize minutes that are complete
                        for minute_dt in list(self._minute_bars[symbol].keys()):
                            if minute_dt < cutoff_minute and minute_dt not in self._finalized_minutes[symbol]:
                                self._finalized_minutes[symbol].add(minute_dt)
                                bar = self._minute_bars[symbol][minute_dt]
                                logger.debug(f"[DATABENTO][FINALIZER] Finalized {symbol} bar at {minute_dt}: OHLC={bar['open']:.2f}/{bar['high']:.2f}/{bar['low']:.2f}/{bar['close']:.2f} vol={bar['volume']}")
                        
                        # Prune old bars to prevent unlimited memory growth
                        prune_before = current_time - timedelta(minutes=self._prune_older_minutes)
                        old_minutes = [dt for dt in self._minute_bars[symbol].keys() if dt < prune_before]
                        for old_dt in old_minutes:
                            del self._minute_bars[symbol][old_dt]
                            self._finalized_minutes[symbol].discard(old_dt)
                        
                        if old_minutes:
                            logger.debug(f"[DATABENTO][FINALIZER] Pruned {len(old_minutes)} old bars for {symbol}")
                
            except Exception as e:
                logger.error(f"[DATABENTO][FINALIZER] Error: {e}")
        
        logger.info("[DATABENTO][FINALIZER] Stopped")
    
    def _aggregate_trade(self, trade, symbol: str):
        """Aggregate a trade into minute bars"""
        # Get trade data
        raw_price = getattr(trade, 'price', 0)
        size = getattr(trade, 'size', 0)
        ts_event = getattr(trade, 'ts_event', 0)
        
        # Convert price from fixed-point format (DataBento multiplies by 1e9)
        price = float(raw_price) / 1e9 if raw_price > 1e10 else float(raw_price)
        
        # Convert nanosecond timestamp to datetime
        trade_time = datetime.fromtimestamp(ts_event / 1e9, tz=timezone.utc)
        minute = trade_time.replace(second=0, microsecond=0)
        
        # Skip if already finalized
        if minute in self._finalized_minutes[symbol]:
            return
        
        # Get current time to check if bar should be finalized
        current_time = datetime.now(timezone.utc)
        current_minute = current_time.replace(second=0, microsecond=0)
        
        # Initialize symbol's bar dict if needed
        if symbol not in self._minute_bars:
            self._minute_bars[symbol] = {}
        
        # Create or update the minute bar
        if minute not in self._minute_bars[symbol]:
            # New minute bar
            self._minute_bars[symbol][minute] = {
                'datetime': minute,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': size
            }
            logger.debug(f"[DATABENTO][LIVE] New minute bar: {symbol} {minute} @ {price:.2f}")
        else:
            # Update existing bar
            bar = self._minute_bars[symbol][minute]
            bar['high'] = max(bar['high'], price)
            bar['low'] = min(bar['low'], price)
            bar['close'] = price
            bar['volume'] += size
        
        # Finalize old bars (anything older than current minute)
        for bar_minute in list(self._minute_bars[symbol].keys()):
            if bar_minute < current_minute and bar_minute not in self._finalized_minutes[symbol]:
                self._finalized_minutes[symbol].add(bar_minute)
                logger.debug(f"[DATABENTO][LIVE] Finalized bar: {symbol} {bar_minute}")
    
    def _get_live_tail(self, symbol: str, after_dt: datetime) -> Optional[pl.DataFrame]:
        """Get finalized live bars newer than after_dt"""
        if symbol not in self._minute_bars or not self._minute_bars[symbol]:
            return None
        
        current_minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        # Get finalized bars newer than after_dt
        tail_bars = []
        for minute, bar_data in sorted(self._minute_bars[symbol].items()):
            if minute > after_dt and minute < current_minute:
                # Only include core OHLCV data to match historical schema
                simple_bar = {
                    'datetime': bar_data['datetime'],
                    'open': bar_data['open'],
                    'high': bar_data['high'],
                    'low': bar_data['low'],
                    'close': bar_data['close'],
                    'volume': bar_data['volume']
                }
                tail_bars.append(simple_bar)
        
        if not tail_bars:
            return None
        
        df = pl.DataFrame(tail_bars).sort('datetime')
        logger.debug(f"[DATABENTO][LIVE] Collected {len(df)} tail bars after {after_dt}")
        return df
    
    def _resolve_futures_symbol(self, asset: Asset, reference_date: datetime = None) -> str:
        """Resolve asset to specific futures contract symbol"""
        if asset.asset_type in [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]:
            # For continuous futures, resolve to specific contract
            if asset.asset_type == Asset.AssetType.CONT_FUTURE:
                if hasattr(asset, 'resolve_continuous_futures_contract'):
                    resolved = asset.resolve_continuous_futures_contract(reference_date)
                    # DataBento uses single digit year for CME futures (ESZ5 not ESZ25)
                    if len(resolved) >= 5 and resolved[-2:].isdigit() and int(resolved[-2:]) >= 20:
                        # Convert to single digit year (e.g., 25 -> 5)
                        return resolved[:-2] + resolved[-1]
                    return resolved
            
            # Manual resolution for common futures
            symbol = asset.symbol.upper()
            month = reference_date.month if reference_date else datetime.now().month
            year = reference_date.year if reference_date else datetime.now().year
            
            # Quarterly contracts
            if month <= 3:
                month_code = 'H'
            elif month <= 6:
                month_code = 'M'
            elif month <= 9:
                month_code = 'U'
            else:
                month_code = 'Z'
            
            year_digit = year % 10
            
            if symbol in ["ES", "NQ", "RTY", "YM", "MES", "MNQ", "MYM", "M2K", "CL", "GC", "SI"]:
                return f"{symbol}{month_code}{year_digit}"
            
            return asset.symbol
        
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
        """Get historical prices with live tail merge"""
        
        # Validate asset type
        if asset.asset_type not in [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]:
            logger.error(f"DataBento only supports futures. Got: {asset.asset_type}")
            return None
        
        # Calculate time range
        current_time = datetime.now(timezone.utc)
        if timeshift:
            current_time = current_time - timeshift
        
        # Determine time range
        if timestep == "minute":
            time_needed = timedelta(minutes=max(length * 3, 30))
        elif timestep == "hour":
            time_needed = timedelta(hours=max(length * 2, 12))
        else:
            time_needed = timedelta(days=max(length * 2, 10))
        
        start_time = current_time - time_needed
        
        # Resolve to specific contract
        symbol = self._resolve_futures_symbol(asset, current_time)
        logger.debug(f"Resolved {asset.symbol} to {symbol}")
        
        # Subscribe to live stream if enabled (only for recent data gap)
        if self.enable_live_stream and symbol not in self._subscribed_symbols:
            # Live API can only replay recent data (last ~30 minutes)
            live_start_time = current_time - timedelta(minutes=30)
            self._subscribe_to_symbol(symbol, live_start_time, min_bars=30)
            # Give it a moment to start receiving data
            time.sleep(0.5)
        
        # Get historical data
        logger.debug(f"[DATABENTO][HIST] Fetching {symbol} from {start_time} to {current_time}")
        
        df = databento_helper_polars.get_price_data_from_databento_polars(
            api_key=self._api_key,
            asset=asset,
            start=start_time,
            end=current_time,
            timestep=timestep,
            venue=exchange,
            force_cache_update=False
        )
        
        if df is not None and not df.is_empty():
            # Try to append live tail if available
            if self.enable_live_stream and 'datetime' in df.columns:
                try:
                    hist_last = df['datetime'].max()
                    # Ensure hist_last is timezone-aware
                    if not hasattr(hist_last, 'tzinfo') or hist_last.tzinfo is None:
                        from datetime import timezone as tz
                        hist_last = hist_last.replace(tzinfo=tz.utc)
                    tail_df = self._get_live_tail(symbol, hist_last)
                    
                    # Debug: check live bar status
                    if symbol in self._minute_bars:
                        live_bar_count = len(self._minute_bars[symbol])
                        finalized_count = len(self._finalized_minutes.get(symbol, []))
                        logger.debug(f"[DATABENTO][DEBUG] {symbol} has {live_bar_count} total bars, {finalized_count} finalized")
                    else:
                        logger.debug(f"[DATABENTO][DEBUG] No live bars for {symbol}")
                    
                    if tail_df is not None and not tail_df.is_empty():
                        # Make sure both dataframes have the same columns and types
                        try:
                            # Ensure timezone compatibility
                            hist_tz_info = df['datetime'].dtype
                            tail_tz_info = tail_df['datetime'].dtype
                            
                            logger.debug(f"[DATABENTO][MERGE] Historical datetime: {hist_tz_info}, Live datetime: {tail_tz_info}")
                            
                            # Make timezone consistent - prefer naive (historical format)
                            if 'UTC' in str(tail_tz_info) and 'UTC' not in str(hist_tz_info):
                                # Convert live to naive to match historical
                                tail_df = tail_df.with_columns(pl.col('datetime').dt.replace_time_zone(None))
                                logger.debug("[DATABENTO][MERGE] Converted live datetime to naive")
                            elif 'UTC' not in str(tail_tz_info) and 'UTC' in str(hist_tz_info):
                                # Convert historical to naive to match live
                                df = df.with_columns(pl.col('datetime').dt.replace_time_zone(None))
                                logger.debug("[DATABENTO][MERGE] Converted historical datetime to naive")
                            
                            # Only keep columns that exist in both dataframes
                            common_columns = [col for col in df.columns if col in tail_df.columns]
                            df_subset = df.select(common_columns)
                            tail_subset = tail_df.select(common_columns)
                            
                            # Ensure numeric columns have compatible types
                            for col in common_columns:
                                if col != 'datetime':  # Don't modify datetime
                                    df_dtype = df_subset[col].dtype
                                    tail_dtype = tail_subset[col].dtype
                                    
                                    # Convert both to Float64 for compatibility
                                    if df_dtype != tail_dtype:
                                        logger.debug(f"[DATABENTO][MERGE] Converting {col}: {df_dtype} vs {tail_dtype} -> Float64")
                                        df_subset = df_subset.with_columns(pl.col(col).cast(pl.Float64))
                                        tail_subset = tail_subset.with_columns(pl.col(col).cast(pl.Float64))
                            
                            # Merge the data
                            merged_df = pl.concat([df_subset, tail_subset]).sort('datetime')
                            
                            # If original df had more columns, merge them back
                            if len(df.columns) > len(common_columns):
                                extra_cols = [col for col in df.columns if col not in common_columns]
                                df_extra = df.select(['datetime'] + extra_cols)
                                merged_df = merged_df.join(df_extra, on='datetime', how='left')
                            
                            df = merged_df
                            logger.info(f"[DATABENTO][MERGE] Successfully appended {len(tail_df)} live bars")
                        
                        except Exception as merge_e:
                            logger.error(f"[DATABENTO][MERGE] All merge attempts failed: {merge_e}")
                            # Last resort - just log what we have
                            hist_latest = df['datetime'].max() if 'datetime' in df.columns else None  
                            tail_latest = tail_df['datetime'].max() if 'datetime' in tail_df.columns else None
                            logger.error(f"[DATABENTO][MERGE] Historical latest: {hist_latest}, Live latest: {tail_latest}")
                            # Continue with historical data only
                    else:
                        lag = (current_time - hist_last).total_seconds()
                        logger.debug(f"[DATABENTO][MERGE] No live tail bars (lag={lag:.0f}s)")
                        
                except Exception as e:
                    logger.warning(f"[DATABENTO][MERGE] Failed to merge live tail: {e}")
            
            # Trim to requested length
            df = df.tail(length)
            return Bars(df=df, source=self.SOURCE, asset=asset, quote=quote, return_polars=return_polars)
        
        return None
    
    def get_last_price(self, asset: Asset, quote: Optional[Asset] = None, exchange: Optional[str] = None) -> Optional[float]:
        """Get the last price for an asset"""
        # Try live data first
        if self.enable_live_stream:
            symbol = self._resolve_futures_symbol(asset)
            if symbol in self._minute_bars and self._minute_bars[symbol]:
                latest_minute = max(self._minute_bars[symbol].keys())
                latest_bar = self._minute_bars[symbol][latest_minute]
                return float(latest_bar['close'])
        
        # Fallback to historical
        bars = self.get_historical_prices(asset, 1, "minute", exchange=exchange)
        if bars and len(bars) > 0:
            return float(bars.df['close'].tail(1).item())
        
        return None
    
    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None) -> dict:
        """Get option chains - not supported for futures"""
        logger.warning("DataBento does not support option chains")
        return {"Chains": {}, "Multiplier": 1, "Exchange": exchange or ""}
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, '_stop_streaming'):
            self._stop_streaming = True
        
        # Stop all producer threads
        if hasattr(self, '_producer_threads'):
            for symbol, thread in self._producer_threads.items():
                if thread and thread.is_alive():
                    thread.join(timeout=1)
        
        # Stop consumer thread
        if hasattr(self, '_consumer_thread') and self._consumer_thread:
            self._consumer_thread.join(timeout=1)
        
        # Stop finalizer thread
        if hasattr(self, '_finalizer_thread') and self._finalizer_thread:
            self._finalizer_thread.join(timeout=1)