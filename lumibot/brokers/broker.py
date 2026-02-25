import logging
import json
import os
import time
import threading
from collections import deque
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from queue import Queue
from threading import RLock, Thread
from typing import Union

import pandas as pd
import pandas_market_calendars as mcal
from dateutil import tz
from termcolor import colored

from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

from lumibot.tools.lumibot_logger import get_logger, get_strategy_logger
from lumibot.tools.parquet_utils import (
    coerce_object_columns_to_json_strings,
    is_parquet_required,
    write_parquet_with_logging,
)
from ..data_sources import DataSource
from ..entities import Asset, Order, Position, Quote
from ..entities.chains import normalize_option_chains
from ..trading_builtins import SafeList, SafeOrderDict

DEFAULT_CLEANUP_CONFIG = {
    "enabled": True,
    "cleanup_interval_iterations": 100,  # Clean up every 100 trading iterations
    "retention_policies": {
        "filled_orders": {
            "max_age_days": 30,      # Keep orders for 30 days
            "max_count": 10000,      # Keep max 10,000 orders
            "min_keep": 100          # Always keep at least 100 recent orders
        },
        "canceled_orders": {
            "max_age_days": 7,       # Keep canceled orders for 7 days
            "max_count": 1000,       # Keep max 1,000 canceled orders  
            "min_keep": 50           # Always keep at least 50 recent orders
        },
        "error_orders": {
            "max_age_days": 30,      # Keep error orders for 30 days
            "max_count": 1000,       # Keep max 1,000 error orders
            "min_keep": 50           # Always keep at least 50 recent orders
        },
        "filled_positions": {
            "max_age_days": 30,      # Keep positions for 30 days
            "max_count": 5000,       # Keep max 5,000 positions
            "min_keep": 100          # Always keep at least 100 recent positions
        }
    }
}

# PERF: Trade event logs are append-only and can be very large in backtests. Building a full
# per-event dict (dozens of keys) is expensive and dominates hot-path CPU when a strategy submits
# many orders. For the common case where audits are disabled, store a compact tuple instead and
# materialize a DataFrame lazily with a fixed schema.
TRADE_EVENT_LOG_COLUMNS = (
    "time",
    "strategy",
    "exchange",
    "identifier",
    "symbol",
    "side",
    "type",
    "status",
    "price",
    "filled_quantity",
    "multiplier",
    "trade_cost",
    "trade_slippage",
    "time_in_force",
    "asset.right",
    "asset.strike",
    "asset.multiplier",
    "asset.expiration",
    "asset.asset_type",
    "price_source",
)

# Consolidate errors from different brokers into a single class that can be easily caught even
# if the user decides to switch brokers.
class LumibotBrokerAPIError(Exception):
    pass

class Broker(ABC):
    # Metainfo
    IS_BACKTESTING_BROKER = False

    # Trading events flags
    NEW_ORDER = "new"
    CANCELED_ORDER = "canceled"
    FILLED_ORDER = "fill"
    MODIFIED_ORDER = "modified"
    PARTIALLY_FILLED_ORDER = "partial_fill"
    CASH_SETTLED = "cash_settled"
    ASSIGNED = "assigned"
    EXERCISED = "exercised"
    EXPIRED_OPTION = "expired"
    ERROR_ORDER = "error"
    PLACEHOLDER_ORDER = "placeholder"

    @staticmethod
    def _truthy_env(value: str | None) -> bool:
        return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    def __init__(self, name="", connect_stream=True, data_source: DataSource = None, option_source: DataSource = None,
                 config=None, max_workers=20, extended_trading_minutes=0, cleanup_config=None):
        """Broker constructor"""
        # Shared Variables between threads
        self.name = name
        self._lock = RLock()
        self._stop_event = threading.Event()  # Add stop event for clean shutdown
        self._runtime_telemetry = None
        # PERF: Backtesting is single-threaded, but SafeList lock acquisition shows up as
        # a measurable overhead in minute-level backtests. Use lock-free SafeLists in backtests
        # while keeping thread-safe lists for live brokers.
        safelist_lock = None if self.IS_BACKTESTING_BROKER else self._lock
        # PERF: Backtests remove orders by `identifier` extremely frequently as they move through
        # the order-state buckets (unprocessed -> new -> partial -> filled/canceled/error). A
        # list-backed container makes those removals O(n) scans and shows up as a dominant CPU
        # cost in high-churn minute backtests.
        #
        # Use a dict-backed container for the "active" buckets in backtesting only (live brokers
        # remain list-backed for compatibility and because their order counts are usually small).
        order_bucket = SafeOrderDict if self.IS_BACKTESTING_BROKER else SafeList
        self._unprocessed_orders = order_bucket(safelist_lock)
        self._placeholder_orders = order_bucket(safelist_lock)
        self._new_orders = order_bucket(safelist_lock)
        self._canceled_orders = SafeList(safelist_lock)
        self._partially_filled_orders = order_bucket(safelist_lock)
        self._filled_orders = SafeList(safelist_lock)
        self._error_orders = SafeList(safelist_lock)
        self._filled_positions = SafeList(safelist_lock)
        self._subscribers = SafeList(safelist_lock)
        self._is_stream_subscribed = False
        # PERF: appending to a pandas DataFrame with concat on every trade event is extremely slow
        # (option-heavy intraday backtests can generate 100k+ events). Store rows in a Python list
        # and materialize a DataFrame lazily when needed.
        self._trade_event_log_enabled = True
        # In live mode we must avoid unbounded growth in long-running workers (Render/ECS).
        # Backtests keep full history for analysis.
        self._trade_event_log_max_rows = None if self.IS_BACKTESTING_BROKER else 5000
        self._trade_event_log_rows = (
            deque(maxlen=self._trade_event_log_max_rows) if self._trade_event_log_max_rows else []
        )
        self._trade_event_log_df_cache = pd.DataFrame()
        self._trade_event_log_columns = TRADE_EVENT_LOG_COLUMNS
        self._hold_trade_events = False
        self._held_trades = []
        self._config = config
        self._strategy_name = ""
        self.data_source = data_source
        self.option_source = option_source
        self.max_workers = min(max_workers, 200)
        self.quote_assets = set()  # Quote positions will never be removed from tracking during sync operations

        # Brokers like Tradier allows SPY option trading for 15 additional min after market close
        # This will need to be set directly by the strategy
        self.extended_trading_minutes = extended_trading_minutes

        # Set the state of first iteration to True. This will later be updated to False by the strategy executor
        self._first_iteration = True

        # Initialize cleanup configuration and tracking
        self._cleanup_config = self._initialize_cleanup_config(cleanup_config)
        self._iteration_counter = 0
        self._last_cleanup_time = None

        # Create an adapter with 'strategy_name' set to the instance's name
        self.logger = get_strategy_logger(__name__, "unknown")

        # --- Market calendar setting ---
        # StrategyExecutor relies on broker.market to decide whether trading is
        # 24/7 or should follow an exchange calendar.  Derive it from config or
        # env, else default to "NASDAQ" which is compatible with pandas-market-calendars.
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "NASDAQ"
        # Auto-adjust market for certain known non-equity data sources when still at generic default.
        try:
            if self.market in ("NASDAQ", "NYSE", "stock") and self.data_source is not None:
                ds_name = self.data_source.__class__.__name__
                if ds_name in ("ProjectXData", "TradovateData"):
                    self.market = "us_futures"
                    logger.debug(f"Auto-set broker.market to 'us_futures' for data source {ds_name}")
                elif ds_name in ("CcxtData", "CcxtBacktestingData", "CCXTData"):
                    self.market = "24/7"
                    logger.debug(f"Auto-set broker.market to '24/7' for data source {ds_name}")
        except Exception as _auto_mkt_exc:
            logger.debug(f"Auto market inference skipped: {_auto_mkt_exc}")

        if self.data_source is None:
            raise ValueError("Broker must have a data source")

        # setting the orders queue and threads
        if not self.IS_BACKTESTING_BROKER:
            self._orders_queue = Queue()
            self._orders_thread = None
            self._start_orders_thread()

        # setting the stream object
        if connect_stream:
            self.stream = self._get_stream_object()
            if self.stream is not None:
                self._launch_stream()

        # Trading calendar placeholder; StrategyExecutor will initialize.
        self._trading_days = None

        self._start_runtime_telemetry()

    def _telemetry_snapshot(self) -> dict:
        """Lightweight, best-effort broker snapshot for runtime telemetry."""
        snap: dict[str, object] = {}
        try:
            snap["orders_unprocessed"] = int(len(self._unprocessed_orders))
            snap["orders_placeholder"] = int(len(self._placeholder_orders))
            snap["orders_new"] = int(len(self._new_orders))
            snap["orders_partial"] = int(len(self._partially_filled_orders))
            snap["orders_filled"] = int(len(self._filled_orders))
            snap["orders_canceled"] = int(len(self._canceled_orders))
            snap["orders_error"] = int(len(self._error_orders))
        except Exception:
            pass

        try:
            snap["positions_tracked"] = int(len(self._filled_positions))
        except Exception:
            pass

        try:
            snap["trade_events_rows"] = int(len(self._trade_event_log_rows))
        except Exception:
            pass

        for key in (
            "_telemetry_polls_total",
            "_telemetry_events_dispatched_total",
            "_telemetry_orders_seen_max",
        ):
            try:
                if hasattr(self, key):
                    snap[key.removeprefix("_telemetry_")] = int(getattr(self, key))
            except Exception:
                continue

        return snap

    def _start_runtime_telemetry(self) -> None:
        """Start always-on runtime memory telemetry (best-effort)."""
        try:
            from lumibot.tools.runtime_telemetry import RuntimeTelemetryConfig, RuntimeTelemetryEmitter

            cfg = RuntimeTelemetryConfig.from_env(is_backtesting=bool(self.IS_BACKTESTING_BROKER))
            if not cfg.enabled:
                return

            self._runtime_telemetry = RuntimeTelemetryEmitter(
                broker=self,
                stop_event=self._stop_event,
                config=cfg,
                logger=getattr(self, "logger", None),
            )
            self._runtime_telemetry.start()
        except Exception:
            return

    # --- Trading calendar initialization ---
    def initialize_market_calendars(self, trading_days_df):
        """Initialize broker trading calendar from a DataFrame.

        Sorts by market_close and sets index for fast lookups; stores
        DataFrame on the broker as `_trading_days`.
        """
        if trading_days_df is None:
            self._trading_days = None
            return
        try:
            df = trading_days_df.sort_values('market_close').copy()
            df.set_index('market_close', inplace=True)
            self._trading_days = df
        except Exception:
            # If trading_days_df is already indexed/sorted, accept as-is
            self._trading_days = trading_days_df

    def _update_attributes_from_config(self, config):
        value_dict = config
        if not isinstance(config, dict):
            value_dict = config.__dict__

        for key in value_dict:
            attr = "is_paper" if "paper" in key.lower() else key.lower()
            if hasattr(self, attr):
                setattr(self, attr, config[key])

    # =================================================================================
    # ================================ Cleanup Methods ===============================

    def _initialize_cleanup_config(self, cleanup_config):
        """Initialize cleanup configuration with defaults."""
        if cleanup_config is None:
            return DEFAULT_CLEANUP_CONFIG.copy()
        
        # Start with defaults and merge user config
        import copy
        config = copy.deepcopy(DEFAULT_CLEANUP_CONFIG)
        
        if cleanup_config:
            # Update top-level settings
            for key in ["enabled", "cleanup_interval_iterations"]:
                if key in cleanup_config:
                    config[key] = cleanup_config[key]
            
            # Merge retention policies
            if "retention_policies" in cleanup_config:
                for policy_name, policy_config in cleanup_config["retention_policies"].items():
                    if policy_name in config["retention_policies"]:
                        # Merge individual policy settings, preserving defaults
                        config["retention_policies"][policy_name].update(policy_config)
                    else:
                        config["retention_policies"][policy_name] = policy_config
        
        return config

    def _cleanup_old_tracking_data(self):
        """Perform cleanup of old orders and positions based on configured policies."""
        if not self._cleanup_config.get("enabled", True):
            return
            
        current_time = self.data_source.get_datetime()
        cleanup_stats = {}
        
        # Clean up each type of tracking data
        for list_name, policy in self._cleanup_config["retention_policies"].items():
            list_obj = getattr(self, f"_{list_name}", None)
            if list_obj is None:
                continue
                
            initial_count = len(list_obj)
            removed_count = self._cleanup_tracking_list(list_obj, policy, current_time)
            cleanup_stats[list_name] = {
                "initial_count": initial_count,
                "removed_count": removed_count, 
                "final_count": len(list_obj)
            }
        
        # Log cleanup results if any items were removed
        if any(stats["removed_count"] > 0 for stats in cleanup_stats.values()):
            self.logger.info(f"Memory cleanup completed: {cleanup_stats}")
        
        self._last_cleanup_time = current_time

    def _cleanup_tracking_list(self, safe_list, policy, current_time):
        """Clean up a specific SafeList based on retention policy."""
        items = safe_list.get_list()
        if len(items) <= policy.get("min_keep", 0):
            return 0  # Don't clean up if below minimum threshold
        
        max_age_days = policy.get("max_age_days")
        max_count = policy.get("max_count")
        min_keep = policy.get("min_keep", 0)

        # PERF (backtesting): these tracking lists are append-only in deterministic chronological
        # order. Sorting (O(n log n)) and repeated `remove()` scans are major hot spots in long
        # backtests, especially for strategies that trade frequently.
        #
        # In backtesting, prefer a cheap short-circuit when nothing could be removed, and use
        # `trim_to_last()` for count-based retention.
        if getattr(self, "IS_BACKTESTING_BROKER", False):
            item_count = len(items)

            # Short-circuit: under max_count AND oldest item within age window => nothing to do.
            if max_count and item_count <= int(max_count or 0) and max_age_days:
                try:
                    oldest_ts = self._get_item_timestamp(items[0]) if items else None
                    if oldest_ts is not None and (current_time - oldest_ts).days < int(max_age_days):
                        return 0
                except Exception:
                    pass
            if max_count and item_count <= int(max_count or 0) and not max_age_days:
                return 0

            removed_total = 0

            # Age retention (when configured): drop oldest until cutoff, keeping `min_keep`.
            if max_age_days:
                cutoff = current_time - timedelta(days=int(max_age_days))
                drop = 0
                for item in items:
                    try:
                        ts = self._get_item_timestamp(item)
                    except Exception:
                        ts = None
                    if ts is None or ts >= cutoff:
                        break
                    drop += 1
                keep_last_age = max(int(min_keep or 0), item_count - drop)
                removed_total += safe_list.trim_to_last(keep_last_age)

            # Count retention (when configured): keep the most recent `max_count` items (but never
            # fewer than `min_keep`).
            if max_count:
                keep_last_count = max(int(min_keep or 0), int(max_count or 0))
                removed_total += safe_list.trim_to_last(keep_last_count)

            return removed_total

        # Default (live + unknown ordering): sort by age (newest first) to preserve recent items.
        items_to_remove = []
        sorted_items = sorted(items, key=self._get_item_timestamp, reverse=True)

        for i, item in enumerate(sorted_items):
            should_remove = False

            # Always keep minimum number of recent items
            if i < min_keep:
                continue

            # Remove by age
            if max_age_days and self._is_item_too_old(item, current_time, max_age_days):
                should_remove = True

            # Remove by count (keep most recent)
            if max_count and i >= max_count:
                should_remove = True

            if should_remove:
                items_to_remove.append(item)

        # Remove items (thread-safe)
        for item in items_to_remove:
            try:
                safe_list.remove(item)
            except ValueError:
                # Item might have been removed by another thread
                pass

        return len(items_to_remove)

    def _get_item_timestamp(self, item):
        """Get the timestamp to use for age-based cleanup."""
        if hasattr(item, 'broker_update_date') and item.broker_update_date:
            return item.broker_update_date
        elif hasattr(item, 'broker_create_date') and item.broker_create_date:
            return item.broker_create_date
        elif hasattr(item, '_date_created') and item._date_created:
            return item._date_created
        else:
            # Fallback to current time (won't be cleaned up)
            return self.data_source.get_datetime()

    def _is_item_too_old(self, item, current_time, max_age_days):
        """Check if an item is too old based on retention policy."""
        item_time = self._get_item_timestamp(item)
        if item_time is None:
            return False
        
        age_delta = current_time - item_time
        return age_delta.days >= max_age_days

    def _trigger_periodic_cleanup(self):
        """Trigger cleanup based on iteration counter."""
        self._iteration_counter += 1
        cleanup_interval = self._cleanup_config.get("cleanup_interval_iterations", 100)
        
        if self._iteration_counter % cleanup_interval == 0:
            try:
                self._cleanup_old_tracking_data()
            except Exception as e:
                self.logger.warning(f"Memory cleanup failed: {e}")

    def force_cleanup(self):
        """Force immediate cleanup of old tracking data (for testing or manual cleanup)."""
        try:
            self._cleanup_old_tracking_data()
            self.logger.info("Manual cleanup completed successfully")
        except Exception as e:
            self.logger.error(f"Manual cleanup failed: {e}")

    def cleanup_streams(self):
        """Clean up stream threads properly to prevent segmentation faults"""
        # Set stop event to signal all threads to stop
        if hasattr(self, '_stop_event'):
            self._stop_event.set()

        # Stop the stream
        if hasattr(self, 'stream') and self.stream:
            try:
                self.stream.stop()
            except Exception as e:
                if hasattr(self, 'logger'):
                    self.logger.warning(f"Error stopping stream: {e}")

        # Clean up the orders queue
        if hasattr(self, '_orders_queue'):
            try:
                # Clear the queue to unblock any waiting threads
                while not self._orders_queue.empty():
                    self._orders_queue.get_nowait()
                    self._orders_queue.task_done()
            except:
                pass

        # Wait for threads to finish
        if hasattr(self, '_orders_thread') and self._orders_thread:
            try:
                self._orders_thread.join(timeout=1)
            except:
                pass

        if getattr(self, "_runtime_telemetry", None) is not None:
            try:
                self._runtime_telemetry.join(timeout=1)  # type: ignore[union-attr]
            except Exception:
                pass

    def __del__(self):
        """Cleanup when broker is destroyed"""
        try:
            self.cleanup_streams()
        except:
            pass  # Suppress any errors during cleanup

    # =================================================================================
    # ================================ Required Implementations========================
    # =========Order Handling=======================
    @abstractmethod
    def cancel_order(self, order: Order) -> None:
        """Cancel an order at the broker"""
        pass

    @abstractmethod
    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        """
        pass

    @abstractmethod
    def _submit_order(self, order: Order) -> Order:
        """Submit an order to the broker"""
        pass

    # =========Account functions=======================
    @abstractmethod
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Get the actual cash balance at the broker.
        Parameters
        ----------
        quote_asset : Asset
            The quote asset to get the balance of.

        Returns
        -------
        tuple of float
            A tuple containing (cash, positions_value, total_liquidation_value).
            Cash = cash in the account (whatever the quote asset is).
            Positions value = the value of all the positions in the account.
            Portfolio value = the total equity value of the account (aka. portfolio value).
        """
        pass

    @abstractmethod
    def get_historical_account_value(self) -> dict:
        """
        Get the historical account value of the account.
        TODO: Fill out the docstring with more information.
        """
        pass

    # =========Streaming functions=======================

    @abstractmethod
    def _get_stream_object(self):
        """
        Get the broker stream connection
        """
        pass

    @abstractmethod
    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        pass

    @abstractmethod
    def _run_stream(self):
        pass

    # =========Broker Positions=======================

    @abstractmethod
    def _pull_positions(self, strategy: 'Strategy') -> list[Position]:
        """
        Get the account positions. return a list of position objects

        Parameters
        ----------
        strategy : Strategy
            The strategy object to pull the positions for

        Returns
        -------
        list[Position]
            A list of position objects
        """
        pass

    @abstractmethod
    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Position:
        """
        Pull a single position from the broker that matches the asset and strategy. If no position is found, None is
        returned.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull
        asset: Asset
            The asset to pull the position for

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None
        """
        pass

    # =========Broker Orders=======================

    @abstractmethod
    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        """
        Parse a broker order representation to an order object

        Parameters
        ----------
        response : dict
            The broker order representation
        strategy_name : str
            The name of the strategy that placed the order

        Returns
        -------
        Order
            The order object
        """
        pass

    @abstractmethod
    def _pull_broker_order(self, identifier: str) -> Order:
        """
        Get a broker order representation by its id

        Parameters
        ----------
        identifier : str
            The identifier of the order to pull

        Returns
        -------
        Order
            The order object
        """
        pass

    @abstractmethod
    def _pull_broker_all_orders(self) -> list[dict]:
        """
        Get the broker open orders

        Returns
        -------
        list[dict]
            A list of order responses from the broker query. These will be passed to _parse_broker_order() to
             be converted to Order objects.
        """
        pass

    def sync_positions(self, strategy):
        """
        Sync the broker positions with the lumibot positions. Remove any lumibot positions that are not at the broker.
        """
        positions_broker = self._pull_positions(strategy)
        for position in positions_broker:
            # Check if the position is None
            if position is None:
                continue

            # Check against existing position.
            position_lumi = [
                pos_lumi
                for pos_lumi in self._filled_positions.get_list()
                if pos_lumi.asset == position.asset
            ]
            position_lumi = position_lumi[0] if len(position_lumi) > 0 else None

            if position_lumi:
                # Compare to existing lumi position.
                if position_lumi.quantity != position.quantity:
                    position_lumi.quantity = position.quantity

                # No current brokers have any way to distinguish between strategies for an open position.
                # Therefore, we will just update the strategy to the current strategy.
                # This is added here because with initial polling, no strategy is set for the positions so we
                # can create ones that have no strategy attached. This will ensure that all stored positions have a
                # strategy with subsequent updates.
                if strategy:
                    position_lumi.strategy = strategy.name if not isinstance(strategy, str) else strategy
            else:
                # Add to positions in lumibot, position does not exist
                # in lumibot.
                if position.quantity != 0.0:
                    self._filled_positions.append(position)

        # Now iterate through lumibot positions.
        # Remove lumibot position if not at the broker.
        for position in self._filled_positions.get_list():
            found = False
            for position_broker in positions_broker:
                if position_broker.asset == position.asset:
                    found = True
                    break
            if not found and (position.asset not in self.quote_assets):
                self._filled_positions.remove(position)

    # =========Market functions=======================

    def get_last_price(self, asset: Asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        Takes an asset and returns the last known price

        Parameters
        ----------
        asset : Asset
            The asset to get the price of.
        quote : Asset
            The quote asset to get the price of.
        exchange : str
            The exchange to get the price of.

        Returns
        -------
        float or Decimal or None
            The last known price of the asset.
        """
        if self.option_source and "option" == asset.asset_type:
            return self.option_source.get_last_price(asset, quote=quote, exchange=exchange)
        else:
            return self.data_source.get_last_price(asset, quote=quote, exchange=exchange)

    def get_last_prices(self, assets, quote=None, exchange=None):
        """
        Takes a list of assets and returns the last known prices

        Parameters
        ----------
        assets : list
            The assets to get the prices of.
        quote : Asset
            The quote asset to get the prices of.
        exchange : str
            The exchange to get the prices of.

        Returns
        -------
        dict
            The last known prices of the assets.
        """
        return self.data_source.get_last_prices(assets=assets, quote=quote, exchange=exchange)

    # =================================================================================
    # ================================ Common functions ================================
    @property
    def _tracked_orders(self):
        orders: list[Order] = []
        orders.extend(self._unprocessed_orders.get_list())
        orders.extend(self._new_orders.get_list())
        orders.extend(self._partially_filled_orders.get_list())
        orders.extend(self._filled_orders.get_list())
        orders.extend(self._error_orders.get_list())
        orders.extend(self._canceled_orders.get_list())
        orders.extend(self._placeholder_orders.get_list())
        return orders

    def is_backtesting_broker(self):
        return self.IS_BACKTESTING_BROKER

    def get_chains(self, asset) -> dict:
        """Returns option chains.

        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset
            The stock whose option chain is being fetched. Represented
            as an asset object.

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strke info to guarentee that the stikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        """
        # PERF: Strategies can call `get_chains()` hundreds of times per trading day. Normalizing the
        # chain (copying strike maps + sorting) is expensive and, in backtests, the chain for a given
        # (asset, date, constraints) is immutable. Cache the normalized result within a backtest to
        # avoid repeating that work.
        cache_key = None
        if getattr(self, "IS_BACKTESTING_BROKER", False):
            try:
                cache_date = self.data_source.get_datetime().date()
            except Exception:
                cache_date = None

            constraints = getattr(self.data_source, "_chain_constraints", None) or {}
            try:
                constraints_key = json.dumps(constraints, sort_keys=True, default=str)
            except Exception:
                constraints_key = str(constraints)

            cache_key = (
                getattr(asset, "symbol", str(asset)),
                str(getattr(asset, "asset_type", "")),
                cache_date,
                constraints_key,
            )

            if getattr(self, "_chains_cache_date", None) != cache_date:
                self._chains_cache_date = cache_date
                self._chains_cache = {}

            cached = getattr(self, "_chains_cache", {}).get(cache_key)
            if cached is not None:
                return cached

        raw_chains = self.data_source.get_chains(asset)
        normalized_chains = normalize_option_chains(raw_chains)

        # PERF: ThetaData historical chains can contain hundreds of expirations. Eagerly fetching
        # strike lists for each expiration (option/list/strikes fanout) makes cold backtests
        # unusably slow. For ThetaData backtests, enable lazy strike loading so we only fetch
        # strikes for the expirations a strategy actually accesses.
        if (
            getattr(self, "IS_BACKTESTING_BROKER", False)
            and isinstance(raw_chains, dict)
            and raw_chains.get("_chain_cache_version") is not None
        ):
            try:
                from datetime import date as _date, datetime as _dt

                from lumibot.tools import thetadata_helper

                underlying_symbol = getattr(asset, "symbol", None) or normalized_chains.underlying_symbol or str(asset)
                symbol_upper = str(underlying_symbol).upper()

                # If this underlying has future splits relative to the chain's as-of date, Theta's
                # strike lists can be on the pre-split scale. Reuse the same per-strike selection
                # heuristic as build_historical_chain so strategies continue to see split-adjusted
                # strikes when using lazy strike loading.
                strike_normalizer = None
                try:
                    is_stock_underlying = str(getattr(asset, "asset_type", "")).lower() == "stock"
                    as_of_date = self.data_source.get_datetime().date()
                except Exception:
                    is_stock_underlying = False
                    as_of_date = None

                if is_stock_underlying and isinstance(as_of_date, _date):
                    try:
                        splits = thetadata_helper._get_theta_splits(asset, as_of_date, _date.today())
                        if splits is not None and not splits.empty and "event_date" in splits.columns:
                            import math
                            import pandas as _pd

                            as_of_datetime = _pd.Timestamp(as_of_date)
                            if splits["event_date"].dtype != "datetime64[ns]":
                                splits["event_date"] = _pd.to_datetime(splits["event_date"])
                            future_splits = splits[splits["event_date"] > as_of_datetime]
                            if not future_splits.empty:
                                cumulative_split_factor = float(future_splits["ratio"].prod())
                                if cumulative_split_factor != 1.0:
                                    reference_price = None
                                    try:
                                        ref_asset = Asset(asset.symbol, asset_type="stock")
                                        ref_dt = _dt(as_of_date.year, as_of_date.month, as_of_date.day)
                                        ref_df = thetadata_helper.get_price_data(
                                            asset=ref_asset,
                                            start=ref_dt,
                                            end=ref_dt,
                                            timespan="day",
                                            datastyle="ohlc",
                                            include_after_hours=False,
                                        )
                                        if ref_df is not None and not ref_df.empty:
                                            for col in ("close", "Close", "adj_close", "Adj Close"):
                                                if col in ref_df.columns:
                                                    reference_price = float(ref_df[col].iloc[-1])
                                                    break
                                    except Exception:
                                        reference_price = None

                                    def _select_normalized_strike(raw_strike: float) -> float:
                                        adjusted = raw_strike / cumulative_split_factor
                                        if reference_price and reference_price > 0:
                                            try:
                                                raw_score = abs(math.log(raw_strike / reference_price))
                                                adjusted_score = abs(math.log(adjusted / reference_price))
                                            except (ValueError, ZeroDivisionError):
                                                return adjusted
                                            return adjusted if adjusted_score < raw_score else raw_strike
                                        return adjusted

                                    strike_normalizer = _select_normalized_strike
                    except Exception:
                        strike_normalizer = None

                def _load_strikes(expiry_key: str) -> list[float]:
                    try:
                        exp_date = _dt.strptime(str(expiry_key), "%Y-%m-%d").date()
                    except Exception:
                        return []

                    strike_symbol = str(underlying_symbol)
                    if symbol_upper == "SPX":
                        strike_symbol = "SPX" if thetadata_helper._is_third_friday(exp_date) else "SPXW"

                    try:
                        strikes = thetadata_helper.get_strikes(strike_symbol, _dt.combine(exp_date, _dt.min.time()))
                        if strike_normalizer is not None and strikes:
                            normalized = {
                                round(float(strike_normalizer(float(s))), 5)
                                for s in strikes
                                if s is not None
                            }
                            return sorted(normalized)
                        return strikes
                    except Exception:
                        return []

                # NOTE: Lazy strike fetching can trigger `option/list/strikes` fanout. In
                # backtesting/CI acceptance runs we enforce a strict warm-cache invariant (no
                # downloader queue submissions), so never enable lazy strike hydration there.
                if not bool(getattr(self, "IS_BACKTESTING_BROKER", False)):
                    normalized_chains.enable_lazy_strikes(_load_strikes)
            except Exception:
                pass

        if not normalized_chains:
            logger.warning(
                colored(
                    f"Option chains unavailable for {getattr(asset, 'symbol', asset)}; continuing without options data.",
                    "yellow",
                )
            )

        if cache_key is not None:
            try:
                self._chains_cache[cache_key] = normalized_chains
            except Exception:
                pass

        return normalized_chains

    def get_chain(self, chains, exchange="SMART") -> dict:
        """Returns option chain for a particular exchange.

        Takes in a full set of chains for all the exchanges and returns
        on chain for a given exchange. The full chains are returned
        from `get_chains` method.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strke info to guarentee that the stikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        """
        return chains[exchange] if exchange in chains else chains

    def get_chain_full_info(self, asset: Asset, expiry: str, chains=None, underlying_price=None, risk_free_rate=None,
                            strike_min=None, strike_max=None) -> pd.DataFrame:
        """
        Get the full chain information for an option asset, including: greeks, bid/ask, open_interest, etc. For
        brokers that do not support this, greeks will be calculated locally. For brokers like Tradier this function
        is much faster as only a single API call can be done to return the data for all options simultaneously.

        Parameters
        ----------
        asset : Asset
            The option asset to get the chain information for.
        expiry
            The expiry date of the option chain.
        chains
            The chains dictionary created by `get_chains` method. This is used
            to get the list of strikes needed to calculate the greeks.
        underlying_price
            Price of the underlying asset.
        risk_free_rate
            The risk-free rate used in interest calculations.
        strike_min
            The minimum strike price to return in the chain. If None, will return all strikes.
        strike_max
            The maximum strike price to return in the chain. If None, will return all strikes.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the full chain information for the option asset. Greeks columns will be named as
            'greeks.delta', 'greeks.theta', etc.
        """
        return self.data_source.get_chain_full_info(asset, expiry, chains, underlying_price, risk_free_rate,
                                                    strike_min, strike_max)

    def get_greeks(self, asset, asset_price, underlying_price, risk_free_rate, query_greeks=False):
        """
        Get the greeks of an option asset.

        Parameters
        ----------
        asset : Asset
            The option asset to get the greeks of.
        asset_price : float, optional
            The price of the option asset, by default None
        underlying_price : float, optional
            The price of the underlying asset, by default None
        risk_free_rate : float, optional
            The risk-free rate used in interest calculations, by default None
        query_greeks : bool, optional
            Whether to query the greeks from the broker. By default, the greeks are calculated locally, but if the
            broker supports it, they can be queried instead which could theoretically be more precise.

        Returns
        -------
        dict
            A dictionary containing the greeks of the option asset.
        """
        if query_greeks:
            greeks = self.data_source.query_greeks(asset)

            # If greeks could not be queried, continue and calculate them locally
            if greeks:
                return greeks
            self.logger.info("Greeks could not be queried from the broker. Calculating locally instead.")

        return self.data_source.calculate_greeks(asset, asset_price, underlying_price, risk_free_rate)

    def get_multiplier(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all the multipliers for the option chains on a given
        exchange.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        int
            The multiplier for the option chain.
        """
        return self.get_chain(chains, exchange)["Multiplier"]

    def get_expiration(self, chains):
        """Returns expiration dates for an option chain for a particular
        exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all the expiry dates for the option chains on a given
        exchange. The return list is sorted.

        Parameters
        ---------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        Returns
        -------
        list of datetime.date
            Sorted list of option expiry dates.
        """
        return sorted(set(chains["Chains"]["CALL"].keys()) | set(chains["Chains"]["PUT"].keys()))

    def get_strikes(self, asset, chains=None):
        """Returns the strikes for an option asset with right and expiry."""
        # If provided chains, use them. It is faster than querying the data source.
        if chains and "Chains" in chains:
            if "option" == asset.asset_type:
                return chains["Chains"][asset.right][asset.expiration]
            else:
                strikes = set()
                for right in chains["Chains"]:
                    for exp in chains["Chains"][right]:
                        strikes |= set(chains["Chains"][right][exp])
        else:
            strikes = self.data_source.get_strikes(asset)

        return sorted(strikes)

    def _start_orders_thread(self):
        self._orders_thread = Thread(target=self._wait_for_orders, daemon=True, name=f"{self.name}_orders_thread")
        self._orders_thread.start()

    def _wait_for_orders(self):
        import queue as queue_module
        while not self._stop_event.is_set():
            try:
                # Use timeout to periodically check stop event
                block = self._orders_queue.get(timeout=0.1)
            except queue_module.Empty:
                continue
            except Exception as e:
                if self._stop_event.is_set():
                    break
                self.logger.error(f"Error in _wait_for_orders: {e}")
                continue

            try:
                if isinstance(block, Order):
                    result = [self._submit_order(block)]
                else:
                    result = self._submit_orders(block)

                for order in result:
                    if order is None:
                        continue

                    if order.was_transmitted():
                        flat_orders = self._flatten_order(order)
                        for flat_order in flat_orders:
                            if self.logger.isEnabledFor(logging.INFO):
                                self.logger.info(
                                    colored(
                                        f"Order {flat_order} was sent to broker {self.name}",
                                        color="green",
                                    )
                                )
                            self._unprocessed_orders.append(flat_order)

                # Trigger periodic cleanup after processing orders
                self._trigger_periodic_cleanup()

                self._orders_queue.task_done()
            except Exception as e:
                if self._stop_event.is_set():
                    break
                self.logger.error(f"Error processing order: {e}")
                self._orders_queue.task_done()

    # =========Internal functions==============

    def _set_initial_positions(self, strategy):
        """Set initial positions"""
        positions = self._pull_positions(strategy)
        for pos in positions:
            if pos.quantity != 0.0:
                self._filled_positions.append(pos)

    def _process_new_order(self, order):
        # Check if this order already exists in self._new_orders based on the identifier
        if order in self._new_orders:
            return order

        self._unprocessed_orders.remove(order.identifier, key="identifier")
        order.status = self.NEW_ORDER
        order.set_new()
        self._new_orders.append(order)
        return order

    def _process_placeholder_order(self, order):
        """Used to track a placeholder order that never gets filled. I.e. OCO parent order"""
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        order.status = self.NEW_ORDER
        order.set_new()
        self._placeholder_orders.append(order)
        return order

    def _process_canceled_order(self, order):
        self._new_orders.remove(order.identifier, key="identifier")
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.status = self.CANCELED_ORDER
        order.set_canceled()
        self._canceled_orders.append(order)
        return order

    def _process_partially_filled_order(self, order, price, quantity):
        self._new_orders.remove(order.identifier, key="identifier")
        order.add_transaction(price, quantity)
        order.status = self.PARTIALLY_FILLED_ORDER
        order.set_partially_filled()
        if order not in self._partially_filled_orders:
            self._partially_filled_orders.append(order)

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is None:
            # Create new position for this given strategy and asset
            position = order.to_position(quantity)
        else:
            # Add the order to the already existing position
            position.add_order(order)

        if "crypto" == order.asset.asset_type:
            self._process_crypto_quote(order, quantity, price)

        return order, position

    def _process_filled_order(self, order, price, quantity):
        self._new_orders.remove(order.identifier, key="identifier")
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.add_transaction(price, quantity)
        order.status = self.FILLED_ORDER
        order.set_filled()
        self._filled_orders.append(order)

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is None:
            # Create new position for this given strategy and asset
            position = order.to_position(quantity)
            if position is None:
                # Order is invalid, skip processing
                logger.error(f"Skipping filled order processing - could not create position from order {order.identifier}")
                return
        else:
            # Add the order to the already existing position
            position.add_order(order)  # Don't update quantity here, it's handled by querying broker

        if order.asset and "crypto" == order.asset.asset_type:
            self._process_crypto_quote(order, quantity, price)

        return position

    def _process_error_order(self, order, error):
        self._new_orders.remove(order.identifier, key="identifier")
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        self._filled_orders.remove(order.identifier, key="identifier")
        order.status = self.ERROR_ORDER
        order.set_error(error)
        self._error_orders.append(order)
        return order

    def _process_option_lifecycle_event(self, order, price, quantity, lifecycle_status, lifecycle_label):
        price_value = 0.0 if price is None else float(price)
        quantity_value = 0.0 if quantity is None else float(quantity)

        self.logger.info(
            colored(
                f"{lifecycle_label}: {order.side} {quantity_value} of {order.asset.symbol} at {price_value:,.8f} USD per share",
                color="green",
            )
        )
        self._new_orders.remove(order.identifier, key="identifier")
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.add_transaction(price_value, quantity_value)
        order.status = lifecycle_status
        order.set_filled()
        self._filled_orders.append(order)

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is not None:
            # Live brokers will update positions via polling, but in backtesting there is no broker-side
            # reconciliation. Cash settlement must therefore adjust the position quantity immediately so
            # expired contracts don't remain open forever (which can cause long backtests to "freeze"
            # due to exploding open positions).
            if getattr(self, "IS_BACKTESTING_BROKER", False):
                try:
                    position.add_order(order, Decimal(str(quantity_value)))
                except Exception:
                    # Fallback: still record the settlement order even if quantity normalization failed.
                    position.add_order(order)

                if position.quantity == 0:
                    try:
                        self._filled_positions.remove(position)
                    except Exception:
                        # SafeList/remove semantics vary by broker; leaving a 0-qty position is acceptable.
                        pass
                    try:
                        # Expired/settled contracts should not leave active close orders behind.
                        self._cancel_open_orders_for_asset(order.strategy, order.asset, {order.identifier})
                    except Exception:
                        pass
            else:
                # Add the order to the already existing position; don't update quantity here because it's
                # handled by querying broker positions in live trading.
                position.add_order(order)

    def _process_cash_settlement(self, order, price, quantity):
        self._process_option_lifecycle_event(
            order=order,
            price=price,
            quantity=quantity,
            lifecycle_status=self.CASH_SETTLED,
            lifecycle_label="Cash Settled",
        )

    def _process_assigned_option(self, order, price, quantity):
        self._process_option_lifecycle_event(
            order=order,
            price=price,
            quantity=quantity,
            lifecycle_status=self.ASSIGNED,
            lifecycle_label="Assigned",
        )

    def _process_exercised_option(self, order, price, quantity):
        self._process_option_lifecycle_event(
            order=order,
            price=price,
            quantity=quantity,
            lifecycle_status=self.EXERCISED,
            lifecycle_label="Exercised",
        )

    def _process_expired_option(self, order, price, quantity):
        self._process_option_lifecycle_event(
            order=order,
            price=price,
            quantity=quantity,
            lifecycle_status=self.EXPIRED_OPTION,
            lifecycle_label="Expired",
        )

    def _process_crypto_quote(self, order, quantity, price):
        """Used to process the quote side of a crypto trade."""
        # Handle cases where price might be None (can happen with some filled orders)
        if price is None:
            # Try to use the limit price if available, otherwise skip processing
            if hasattr(order, 'limit_price') and order.limit_price is not None:
                price = order.limit_price
                logger.debug(f"Using limit_price {price} for crypto quote processing since avg_fill_price was None for order {order.identifier}")
            else:
                logger.debug(f"Skipping crypto quote processing for order {order.identifier} - both avg_fill_price and limit_price are None")
                return

        quote_quantity = Decimal(quantity) * Decimal(price)
        if order.side == "buy":
            quote_quantity = -quote_quantity
        position = self.get_tracked_position(order.strategy, order.quote)
        if position is None:
            position = Position(
                order.strategy,
                order.quote,
                quote_quantity,
            )
            self._filled_positions.append(position)
        else:
            position._quantity += quote_quantity

    # =========Clock functions=====================

    def utc_to_local(self, utc_dt):
        return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())

    def market_hours(self, market="NASDAQ", close=True, next=False, date=None):
        """[summary]

        Parameters
        ----------
        market : str, optional
            Which market to test, by default "NASDAQ"
        close : bool, optional
            Choose open or close to check, by default True
        next : bool, optional
            Check current day or next day, by default False
        date : [type], optional
            Date to check, `None` for today, by default None

        Returns
        -------
        market open or close: Timestamp
            Timestamp of the market open or close time depending on the parameters passed

        """

        market = self.market if self.market is not None else market
        mkt_cal = mcal.get_calendar(market)

        # Get the current datetime in UTC (because the market hours are in UTC)
        dt_now_utc = datetime.now(timezone.utc)

        date = date if date is not None else dt_now_utc
        trading_hours = mkt_cal.schedule(start_date=date, end_date=date + pd.DateOffset(weeks=1)).head(2)

        row = 0 if not next else 1
        th = trading_hours.iloc[row, :]
        market_open, market_close = th.iloc[0], th.iloc[1]

        if close:
            return market_close + timedelta(minutes=self.extended_trading_minutes)
        else:
            return market_open

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit timestamp was reached"""
        return True

    def market_close_time(self):
        return self.utc_to_local(self.market_hours(close=True))

    def market_open_time(self):
        return self.utc_to_local(self.market_hours(close=False))

    def _is_continuous_market(self, market_name):
        """
        Determine if a market trades continuously (24/7 or near-24/7) by checking its trading schedule.
        
        This method uses pandas_market_calendars to check actual trading hours and caches results
        to avoid expensive repeated lookups.
        
        Args:
            market_name (str): Name of the market (e.g., 'NYSE', 'us_futures', '24/7')
            
        Returns:
            bool: True if market trades continuously (>=20 hours per day), False otherwise
        """

        if not hasattr(self, '_market_type_cache'):
            self._market_type_cache = {}
            
        if market_name in self._market_type_cache:
            result = self._market_type_cache[market_name]
            
            return result
            
        try:
            # Special cases that are definitely continuous
            if market_name == '24/7':
                self._market_type_cache[market_name] = True
                
                return True
                
            # Get market calendar
            import pandas as pd
            import pandas_market_calendars as mcal
            cal = mcal.get_calendar(market_name)
            
            # Test with a recent Monday (typical trading day)
            test_date = pd.Timestamp('2025-01-13', tz='UTC')  # Monday
            schedule = cal.schedule(start_date=test_date, end_date=test_date)
            
            if schedule.empty:
                # No trading on this date, assume it's not continuous
                self._market_type_cache[market_name] = False
                
                return False
                
            # Calculate trading hours duration
            market_open = schedule.iloc[0, 0]
            market_close = schedule.iloc[0, 1]
            duration_hours = (market_close - market_open).total_seconds() / 3600
            
            # Consider markets with 20+ hours per day as continuous
            # This catches futures markets that trade ~22-24 hours
            is_continuous = duration_hours >= 20.0
            
            self._market_type_cache[market_name] = is_continuous
            
            return is_continuous
            
        except Exception as e:
            # If we can't determine market type, default to non-continuous for safety
            # Log the error for debugging
            
            if hasattr(self, 'logger'):
                self.logger.warning(f'Could not determine market type for {market_name}: {e}')
            self._market_type_cache[market_name] = False
            return False

    def is_market_open(self):
        """Determines if the market is open.

        Parameters
        ----------
        None

        Returns
        -------
        boolean
            True if market is open, false if the market is closed.

        Examples
        --------
        >>> self.is_market_open()
        True
        """

        # Handle 24/7 markets immediately
        if self.market == '24/7':
            
            return True
            
        # Check if this is a continuous market (futures, forex, crypto)
        if self._is_continuous_market(self.market):
            
            return True
            
        current_time = datetime.now().astimezone(tz=tz.tzlocal())

        # For ANY market, check both today's and tomorrow's sessions since trading sessions 
        # can span multiple calendar days (futures: 6pm Thu -> 6pm Fri, forex: Sun 5pm -> Fri 5pm, 
        # crypto sessions, international markets, etc.)
        
        # Check today's session
        try:
            open_time_today = self.utc_to_local(self.market_hours(close=False, next=False))
            close_time_today = self.utc_to_local(self.market_hours(close=True, next=False))
            
            if (current_time >= open_time_today) and (close_time_today >= current_time):
                return True
        except:
            pass  # Today might not have a session
        
        # Check tomorrow's session (which might have started today)
        try:
            open_time_tomorrow = self.utc_to_local(self.market_hours(close=False, next=True))
            close_time_tomorrow = self.utc_to_local(self.market_hours(close=True, next=True))
            
            if (current_time >= open_time_tomorrow) and (close_time_tomorrow >= current_time):
                return True
        except:
            pass  # Tomorrow might not have a session
        
        return False

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        open_time_this_day = self.utc_to_local(self.market_hours(close=False, next=False))
        open_time_next_day = self.utc_to_local(self.market_hours(close=False, next=True))
        now = self.utc_to_local(datetime.now())
        open_time = open_time_this_day if open_time_this_day > now else open_time_next_day
        current_time = datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return 0
        else:
            result = open_time.timestamp() - current_time.timestamp()
            return result

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        market_hours = self.market_hours(close=True)
        close_time = self.utc_to_local(market_hours)
        current_time = datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            result = close_time.timestamp() - current_time.timestamp()
            return result
        else:
            return 0

    def sleep(self, sleeptime):
        """The broker custom method for sleeping.
        Needs to be overloaded depending whether strategy is
        running live or in backtesting mode"""
        time.sleep(sleeptime)

    def _await_market_to_open(self, timedelta=None, strategy=None):
        """Executes infinite loop until market opens"""
        isOpen = self.is_market_open()
        if not isOpen:
            time_to_open = self.get_time_to_open()
            if timedelta is not None:
                time_to_open -= 60 * timedelta

            sleeptime = max(0, time_to_open)
            self.logger.info("Sleeping until the market opens")
            self.sleep(sleeptime)

    def _await_market_to_close(self, timedelta=None, strategy=None):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            if timedelta is not None:
                time_to_close -= 60 * timedelta

            sleeptime = max(0, time_to_close)
            self.logger.info("Sleeping until the market closes")
            self.sleep(sleeptime)

    # =========Positions functions==================
    def get_tracked_position(self, strategy, asset):
        """get a tracked position given an asset and
        a strategy"""
        for position in self._filled_positions:
            if position.asset == asset and (not strategy or position.strategy == strategy):
                return position
        return None

    def get_tracked_positions(self, strategy=None):
        """get all tracked positions for a given strategy"""
        result = [position for position in self._filled_positions if strategy is None or position.strategy == strategy]
        return result

    # =========Orders and assets functions=================

    def get_tracked_order(self, identifier, use_placeholders=False):
        """get a tracked order given an identifier"""
        tracked_orders = list(self._tracked_orders)
        if use_placeholders:
            tracked_orders.extend(self._placeholder_orders.get_list())
        for order in tracked_orders:
            if order.identifier == identifier:
                return order
        return None

    def get_tracked_orders(self, strategy=None, asset=None) -> list[Order]:
        """get all tracked orders for a given strategy"""
        # Allow filtering by Strategy instance or by name
        if strategy is not None and not isinstance(strategy, str):
            strategy_name = getattr(strategy, "name", getattr(strategy, "_name", None))
        else:
            strategy_name = strategy
        result = []
        for order in self._tracked_orders:
            if (strategy_name is None or order.strategy == strategy_name) and (asset is None or order.asset == asset):
                result.append(order)
        return result

    def get_active_tracked_orders(self, strategy=None, asset=None) -> list[Order]:
        """Return only active (open) tracked orders for a strategy/asset.

        This is intentionally faster than `get_tracked_orders()` because it only scans
        the internal lists that can contain active orders (unprocessed/new/partially-filled
        plus placeholders), avoiding the much larger filled/canceled/error histories.
        """
        # Allow filtering by Strategy instance or by name
        if strategy is not None and not isinstance(strategy, str):
            strategy_name = getattr(strategy, "name", getattr(strategy, "_name", None))
        else:
            strategy_name = strategy

        result: list[Order] = []
        for bucket in (
            self._unprocessed_orders,
            self._new_orders,
            self._partially_filled_orders,
            self._placeholder_orders,
        ):
            for order in bucket.get_list():
                if not order.is_active():
                    continue
                if strategy_name is not None and order.strategy != strategy_name:
                    continue
                if asset is not None and order.asset != asset:
                    continue
                result.append(order)
        return result

    def get_all_orders(self) -> list[Order]:
        """get all tracked and completed orders"""
        orders = self._tracked_orders
        return orders

    def get_order(self, identifier) -> Order:
        """get a tracked order given an identifier"""
        for order in self.get_all_orders():
            if order.identifier == identifier:
                return order
        return None

    def get_tracked_assets(self, strategy):
        """Get the list of assets for positions
        and open orders for a given strategy"""
        orders = self.get_tracked_orders(strategy)
        positions = self.get_tracked_positions(strategy)
        result = [o.asset for o in orders] + [p.asset for p in positions]
        return list(set(result))

    def get_asset_potential_total(self, strategy, asset):
        """given a strategy and a asset, check the ongoing
        position and the tracked order and returns the total
        number of shares provided all orders went through"""
        quantity = 0
        position = self.get_tracked_position(strategy, asset)
        if position is not None:
            quantity = position.quantity

        # Get all tracked orders for the strategy and asset
        orders = self.get_tracked_orders(strategy, asset)

        # Add the quantity of the order to the total
        for order in orders:
            # Check if the order status is new (only new orders are considered because they are not filled yet
            # and the quantity does not include what will be filled)
            if order.status == Order.OrderStatus.NEW:
                # If the order is not filled, add the quantity of the order to the total
                quantity += float(order.get_increment())

        if type(quantity) == Decimal:
            if quantity.as_tuple().exponent > -4:
                quantity = float(quantity)  # has less than 5 decimal places, use float

        return quantity

    def _parse_broker_orders(self, broker_orders, strategy_name, strategy_object=None):
        """parse a list of broker orders into a list of order objects"""
        result = []
        if broker_orders is not None:
            for broker_order in broker_orders:
                order = self._parse_broker_order(broker_order, strategy_name, strategy_object=strategy_object)
                # skip if parsing returned None
                if order is None:
                    continue

                # this seems very broker specific by assuming "leg" is how they package multileg orders
                # it should be removed if the broker specific _parse_broker_order() function handles multileg orders
                # Check if it is a multileg order and Parse the legs
                if isinstance(broker_order, dict) and "leg" in broker_order and isinstance(broker_order["leg"], list):
                    parsed_legs = []
                    for leg in broker_order["leg"]:
                        order_leg = self._parse_broker_order(leg, strategy_name, strategy_object=strategy_object)
                        if order_leg is not None:  # Additional None check for legs
                            order_leg.parent_identifier = order.identifier
                            parsed_legs.append(order_leg)

                    # Add the legs to the parent order
                    order.child_orders = parsed_legs

                # Add the parent order to the result
                result.append(order)

        else:
            self.logger.warning("No orders found in broker._parse_broker_orders: the broker_orders object is None")

        return result

    def _pull_order(self, identifier, strategy_name):
        """pull and parse a broker order by id"""
        response = self._pull_broker_order(identifier)
        if response:
            order = self._parse_broker_order(response, strategy_name)
            return order
        return None

    def _pull_all_orders(self, strategy_name, strategy_object) -> list[Order]:
        """Get a list of order objects representing the open
        orders"""
        response = self._pull_broker_all_orders()
        result = self._parse_broker_orders(response, strategy_name, strategy_object=strategy_object)
        return result

    def modify_order(self, order, stop_price: Union[float, None] = None, limit_price: Union[float, None] = None):
        """Modify an order"""
        return self._modify_order(order, stop_price=stop_price, limit_price=limit_price)

    def submit_order(self, order) -> Order:
        """Conform an order for an asset to broker constraints and submit it."""
        self._conform_order(order)
        return self._submit_order(order)

    def _conform_order(self, order):
        """Conform an order to broker constraints. Derived brokers should implement this method."""
        pass

    def submit_orders(self, orders, **kwargs) -> Union[Order, list[Order]]:
        """Submit orders"""
        if hasattr(self, '_submit_orders'):
            return self._submit_orders(orders, **kwargs)
        else:
            #if kwargs indicates multileg orders with a limit price, and broker does not support it, we should error out instead of submitting legs individually
            if kwargs.get('is_multileg') and kwargs.get('order_type') == Order.OrderType.LIMIT:
                raise NotImplementedError("Multileg limit orders are not supported by this broker")

            with ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix=f"{self.name}_submitting_orders",
            ) as executor:
                tasks = []
                for order in orders:
                    tasks.append(executor.submit(self._submit_order, order))

                result = []
                for task in as_completed(tasks):
                    result.append(task.result())
                return result

    def wait_for_order_registration(self, order):
        """Wait for the order to be registered by the broker"""
        order.wait_to_be_registered()

    def wait_for_order_execution(self, order):
        """Wait for the order to execute/be canceled"""
        order.wait_to_be_closed()

    def wait_for_orders_registration(self, orders):
        """Wait for the orders to be registered by the broker"""
        for order in orders:
            order.wait_to_be_registered()

    def wait_for_orders_execution(self, orders):
        """Wait for the orders to execute/be canceled"""
        for order in orders:
            order.wait_to_be_closed()

    def cancel_orders(self, orders):
        """cancel orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                tasks.append(executor.submit(self.cancel_order, order))

    def cancel_open_orders(self, strategy, orders: list[Order] | None = None):
        """Cancel all open orders for a given strategy.

        Parameters
        ----------
        strategy : str
            Strategy name whose orders should be canceled.
        orders : list[Order] | None
            Optional pre-filtered orders list. When provided, the broker
            skips re-fetching tracked orders and cancels the supplied
            active orders immediately. If None, the broker will gather
            the active orders itself.
        """
        if orders is None:
            orders = [o for o in self.get_tracked_orders(strategy) if o.is_active()]
        else:
            orders = [o for o in orders if o.is_active()]
        if not orders:
            self.logger.debug("cancel_open_orders(strategy=%s) -> no active orders", strategy)
            return []
        order_ids = [
            getattr(order, "identifier", None)
            or getattr(order, "id", None)
            or getattr(order, "order_id", None)
            for order in orders
        ]
        self.logger.debug(
            "cancel_open_orders(strategy=%s) -> active=%d ids=%s",
            strategy,
            len(orders),
            order_ids,
        )
        self.cancel_orders(orders)

    def wait_orders_clear(self, strategy, max_loop=5):
        # Returns true if outstanding orders for a strategy are complete.

        while max_loop > 0:
            outstanding_orders = [
                order
                for bucket in (self._unprocessed_orders, self._new_orders, self._partially_filled_orders)
                for order in bucket.get_list()
                if order.strategy == strategy
            ]

            if len(outstanding_orders) > 0:
                time.sleep(0.25)
                max_loop -= 1
                continue
            else:
                return 1
        return 0

    def sell_all(self, strategy_name, cancel_open_orders=True, strategy=None, is_multileg=False):
        """sell all positions"""
        self.logger.warning(f"Selling all positions for {strategy_name} strategy")
        if cancel_open_orders:
            self.cancel_open_orders(strategy_name)

        if not self.IS_BACKTESTING_BROKER:
            orders_result = self.wait_orders_clear(strategy_name)
            if not orders_result:
                self.logger.info("From sell_all, orders were still outstanding before the sell all event")

        orders = []
        positions = self.get_tracked_positions(strategy_name)
        for position in positions:
            if position.quantity == 0:
                continue

            if strategy is not None:
                if strategy.quote_asset != position.asset:
                    order = position.get_selling_order(quote_asset=strategy.quote_asset)
                    orders.append(order)
            else:
                order = position.get_selling_order()
                orders.append(order)

        self.submit_orders(orders, is_multileg=is_multileg)

    def close_position(self, strategy_name: str, asset: Asset, fraction: float = 1.00):
        """
        Close a position for a given strategy and asset by submitting a sell order.

        Parameters
        ----------
        strategy_name : str
            Name of the strategy that owns the position.
        asset : Asset
            The asset whose position should be closed.
        fraction : float, optional
            Fraction of the position to close, between 0 and 1.0 (default is 1.0, meaning the full position).

        Returns
        -------
        Order or None
            The sell order submitted to close the position, or None if no open position exists
            or the position quantity is zero.
        """
        pos = self.get_tracked_position(strategy_name, asset)
        if pos and pos.quantity != 0:
            self.logger.info(
                "close_position(strategy=%s, asset=%s, fraction=%s) -> qty=%s",
                strategy_name,
                getattr(asset, "symbol", asset),
                fraction,
                pos.quantity,
            )
            order = pos.get_selling_order(quote_asset=self.quote_assets and next(iter(self.quote_assets)))
            if fraction != 1.00:
                order.quantity = order.quantity * fraction
            order_id = getattr(order, "identifier", None) or getattr(order, "id", None) or getattr(order, "order_id", None)
            self.logger.info(
                "close_position(strategy=%s) submitting order %s qty=%s side=%s type=%s",
                strategy_name,
                order_id,
                getattr(order, "quantity", None),
                getattr(order, "side", None),
                getattr(order, "order_type", None),
            )
            return self.submit_order(order)
        self.logger.info(
            "close_position(strategy=%s, asset=%s) -> no tracked position or zero quantity",
            strategy_name,
            getattr(asset, "symbol", asset),
        )
        return None

    # =========Subscribers/Strategies functions==============

    def _add_subscriber(self, subscriber):
        """Adding a new strategy as a subscriber for the broker"""
        self._subscribers.append(subscriber)

    def _get_subscriber(self, name):
        """get a subscriber/strategy by name"""
        for subscriber in self._subscribers:
            if subscriber.name == name:
                return subscriber

        return None

    def _on_new_order(self, order):
        """notify relevant subscriber/strategy about
        new order event"""

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(colored(f"New order was created: {order}", color="green"))

        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            # PERF: Many strategies do not override `Strategy.on_new_order()` (base impl is `pass`).
            # Avoid enqueueing/processing a no-op event in high-churn backtests.
            try:
                strategy_obj = getattr(subscriber, "strategy", None)
                handler = getattr(strategy_obj, "on_new_order", None)
                func = getattr(handler, "__func__", handler)
                if getattr(func, "__qualname__", "") == "Strategy.on_new_order":
                    return
            except Exception:
                pass
            subscriber.add_event(subscriber.NEW_ORDER, payload)

    def _on_canceled_order(self, order):
        """notify relevant subscriber/strategy about
        canceled order event"""

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(colored(f"Order was canceled: {order}", color="green"))

        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            # PERF: Many strategies do not override `Strategy.on_canceled_order()` (base impl is `pass`).
            # Avoid enqueueing/processing a no-op event in high-churn backtests.
            try:
                strategy_obj = getattr(subscriber, "strategy", None)
                handler = getattr(strategy_obj, "on_canceled_order", None)
                func = getattr(handler, "__func__", handler)
                if getattr(func, "__qualname__", "") == "Strategy.on_canceled_order":
                    return
            except Exception:
                pass
            subscriber.add_event(subscriber.CANCELED_ORDER, payload)

    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        """notify relevant subscriber/strategy about
        partially filled order event"""

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(colored(f"Order was partially filled: {order}", color="green"))

        payload = dict(
            position=position,
            order=order,
            price=price,
            quantity=quantity,
            multiplier=multiplier,
        )
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.PARTIALLY_FILLED_ORDER, payload)
        else:
            self.logger.error(f"Subscriber {order.strategy} not found", color="red")

    def _on_filled_order(self, position, order, price, quantity, multiplier):
        """notify relevant subscriber/strategy about
        filled order event"""

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(colored(f"Order was filled: {order}", color="green"))

        payload = dict(
            position=position,
            order=order,
            price=price,
            quantity=quantity,
            multiplier=multiplier,
        )
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.FILLED_ORDER, payload)
        else:
            self.logger.error(colored(f"Subscriber {order.strategy} not found", color="red"))

    # ==========Processing streams data=======================

    def _stream_established(self):
        self._is_stream_subscribed = True

    @property
    def _trade_event_log_df(self) -> pd.DataFrame:
        """Trade event log as a DataFrame (materialized lazily for performance)."""
        if not getattr(self, "_trade_event_log_enabled", True):
            return getattr(self, "_trade_event_log_df_cache", pd.DataFrame())
        cache = getattr(self, "_trade_event_log_df_cache", None)
        if cache is None:
            rows = getattr(self, "_trade_event_log_rows", [])
            # `rows` may be a deque in live mode.
            if not rows:
                cache = pd.DataFrame()
            else:
                first = next(iter(rows), None)
                if isinstance(first, dict):
                    cache = pd.DataFrame(list(rows))
                else:
                    cols = getattr(self, "_trade_event_log_columns", None) or None
                    cache = pd.DataFrame(list(rows), columns=list(cols) if cols else None)
            self._trade_event_log_df_cache = cache
        return cache

    @_trade_event_log_df.setter
    def _trade_event_log_df(self, value) -> None:
        # Preserve legacy test behavior where callers set this to None to disable logging.
        if value is None:
            self._trade_event_log_enabled = False
            self._trade_event_log_rows = []
            self._trade_event_log_df_cache = pd.DataFrame()
            return
        self._trade_event_log_enabled = True
        self._trade_event_log_df_cache = value
        # Preserve bounded live behavior even if a caller sets the DF explicitly.
        if getattr(self, "_trade_event_log_max_rows", None):
            self._trade_event_log_rows = deque(maxlen=self._trade_event_log_max_rows)
        else:
            self._trade_event_log_rows = []

    def process_held_trades(self):
        """Processes any held trade notifications."""
        while len(self._held_trades) > 0:
            th = self._held_trades.pop(0)

            # Unpack the held trade event
            stored_order = th[0]
            type_event = th[1]
            price = th[2]
            filled_quantity = th[3]
            multiplier = th[4]

            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(
                    f"Processing held trade event. Trade event received for stored_order: {stored_order}, "
                    f"type_event: {type_event}, ID: {stored_order.identifier}, price: {price}, "
                    f"filled_quantity: {filled_quantity}, multiplier: {multiplier}"
                )

            # Process the trade event
            self._process_trade_event(
                stored_order,
                type_event,
                price=price,
                filled_quantity=filled_quantity,
                multiplier=multiplier,
            )

    def _process_trade_event(self, stored_order, type_event, price=None, filled_quantity=None, multiplier=1, error=None): # Add error parameter
        """process an occurred trading event and update the
        corresponding order"""
        # PERF: This method is called once per broker event. In backtests, high-churn strategies
        # can emit hundreds of thousands of events, so minimize per-call overhead in the common
        # backtesting path.
        is_backtesting = getattr(self, "IS_BACKTESTING_BROKER", False)
        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                f"Processing trade event. Trade event received for {stored_order.strategy} strategy: {type_event} "
                f"{stored_order.symbol} ID={stored_order.identifier}, processed by broker {self.name}"
            )

        if self._hold_trade_events and not is_backtesting:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(
                    f"Trade event held for {stored_order.strategy} strategy: {type_event} {stored_order.symbol} "
                    f"ID={stored_order.identifier}, processed by broker {self.name}. "
                    f"self._hold_trade_events is {self._hold_trade_events}"
                )

            # Hold the trade event
            self._held_trades.append(
                (
                    stored_order,
                    type_event,
                    price,
                    filled_quantity,
                    multiplier,
                )
            )
            return

        # for fill and partial_fill events, price and filled_quantity must be specified
        if (type_event in (self.FILLED_ORDER, self.PARTIALLY_FILLED_ORDER) and
                stored_order.order_class != Order.OrderClass.OCO and
                (price is None or filled_quantity is None)):
            raise ValueError(
                f"""For filled_order and partially_filled_order event,
                price and filled_quantity must be specified.
                Received respectively {price} and {filled_quantity}"""
            )

        if filled_quantity is not None and not isinstance(filled_quantity, float):
            try:
                filled_quantity = float(filled_quantity)
            except (TypeError, ValueError):
                raise ValueError(
                    f"filled_quantity must be an integer or float, received {filled_quantity} instead"
                ) from None

        if price is not None and not isinstance(price, float):
            try:
                price = float(price)
            except ValueError:
                raise ValueError(f"price must be a positive float, received {price} instead") from None

        # PERF: Backtesting emits the canonical event types from Broker constants, so we can avoid
        # the expensive "equivalent status" normalization logic intended for live brokers.
        if is_backtesting:
            if type_event == self.NEW_ORDER:
                order = self._process_new_order(stored_order)
                if order:
                    self._on_new_order(order)
            elif type_event == self.PLACEHOLDER_ORDER:
                self._process_placeholder_order(stored_order)
                # No notification needed for placeholder
            elif type_event == self.CANCELED_ORDER:
                order = self._process_canceled_order(stored_order)
                if order:
                    self._on_canceled_order(order)
            elif type_event == self.ERROR_ORDER:
                order = self._process_error_order(stored_order, error or LumibotBrokerAPIError("Unknown order error"))
                if order:
                    subscriber = self._get_subscriber(order.strategy)
                    if subscriber:
                        payload = dict(order=order, error=error)
                        subscriber.add_event(subscriber.ERROR_ORDER, payload)
            elif type_event == self.MODIFIED_ORDER:
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(colored(f"Order was modified: {stored_order}", color="yellow"))
            elif type_event == self.PARTIALLY_FILLED_ORDER:
                stored_order, position = self._process_partially_filled_order(stored_order, price, filled_quantity)
                if position:
                    self._on_partially_filled_order(position, stored_order, price, filled_quantity, multiplier)
            elif type_event == self.FILLED_ORDER:
                position = self._process_filled_order(stored_order, price, filled_quantity)
                if position:
                    self._on_filled_order(position, stored_order, price, filled_quantity, multiplier)
            elif type_event == self.CASH_SETTLED:
                self._process_cash_settlement(stored_order, price, filled_quantity)
                stored_order.order_type = self.CASH_SETTLED
            elif type_event == self.ASSIGNED:
                self._process_assigned_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.ASSIGNED
            elif type_event == self.EXERCISED:
                self._process_exercised_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.EXERCISED
            elif type_event == self.EXPIRED_OPTION:
                self._process_expired_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.EXPIRED_OPTION
            else:
                self.logger.warning(f"Unknown trade event type: {type_event}")
        else:
            if Order.is_equivalent_status(type_event, self.NEW_ORDER):
                order = self._process_new_order(stored_order)
                if order:
                    self._on_new_order(order)
            elif Order.is_equivalent_status(type_event, self.PLACEHOLDER_ORDER):
                self._process_placeholder_order(stored_order)
                # No notification needed for placeholder
            elif Order.is_equivalent_status(type_event, self.CANCELED_ORDER):
                order = self._process_canceled_order(stored_order)
                if order:
                    self._on_canceled_order(order)
            elif Order.is_equivalent_status(type_event, self.ERROR_ORDER):
                order = self._process_error_order(stored_order, error or LumibotBrokerAPIError("Unknown order error"))
                if order:
                    # Notify subscriber about the error event
                    subscriber = self._get_subscriber(order.strategy)
                    if subscriber:
                        payload = dict(order=order, error=error)
                        subscriber.add_event(subscriber.ERROR_ORDER, payload)
            elif Order.is_equivalent_status(type_event, self.MODIFIED_ORDER):
                # TODO: Implement modification logic and notification if needed
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(colored(f"Order was modified: {stored_order}", color="yellow"))
                pass
            elif Order.is_equivalent_status(type_event, self.PARTIALLY_FILLED_ORDER):
                stored_order, position = self._process_partially_filled_order(stored_order, price, filled_quantity)
                if position:
                    self._on_partially_filled_order(position, stored_order, price, filled_quantity, multiplier)
            elif Order.is_equivalent_status(type_event, self.FILLED_ORDER):
                position = self._process_filled_order(stored_order, price, filled_quantity)
                if position:
                    self._on_filled_order(position, stored_order, price, filled_quantity, multiplier)
            elif Order.is_equivalent_status(type_event, self.CASH_SETTLED):
                self._process_cash_settlement(stored_order, price, filled_quantity)
                stored_order.order_type = self.CASH_SETTLED
            elif Order.is_equivalent_status(type_event, self.ASSIGNED):
                self._process_assigned_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.ASSIGNED
            elif Order.is_equivalent_status(type_event, self.EXERCISED):
                self._process_exercised_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.EXERCISED
            elif Order.is_equivalent_status(type_event, self.EXPIRED_OPTION):
                self._process_expired_option(stored_order, price, filled_quantity)
                stored_order.order_type = self.EXPIRED_OPTION
            else:
                self.logger.warning(f"Unknown trade event type: {type_event}")

        # PERF: backtesting data sources often store the current dt on `_datetime`. Avoid the extra
        # method call overhead in the hot-path trade-event logger, but fall back to `get_datetime()`
        # for stubbed sources used in unit tests.
        if is_backtesting:
            current_dt = getattr(self.data_source, "_datetime", None) or self.data_source.get_datetime()
        else:
            current_dt = self.data_source.get_datetime()
        # Cache the audit-enabled flag when available (BacktestingBroker sets this in __init__).
        audit_enabled = getattr(self, "_backtest_audit_enabled", None)
        if audit_enabled is None:
            audit_enabled = self._truthy_env(os.environ.get("LUMIBOT_BACKTEST_AUDIT"))

        asset = stored_order.asset
        price_source = getattr(stored_order, "_price_source", None)

        if audit_enabled:
            new_row = {
                "time": current_dt,
                "strategy": stored_order.strategy,
                "exchange": stored_order.exchange,
                "identifier": stored_order.identifier,
                "symbol": stored_order.symbol,
                "side": stored_order.side,
                "type": stored_order.order_type,
                "status": stored_order.status,
                "price": price,
                "filled_quantity": filled_quantity,
                "multiplier": multiplier,
                "trade_cost": stored_order.trade_cost,
                "trade_slippage": getattr(stored_order, "trade_slippage", None),
                "time_in_force": stored_order.time_in_force,
                "asset.right": asset.right if asset is not None else None,
                "asset.strike": asset.strike if asset is not None else None,
                "asset.multiplier": asset.multiplier if asset is not None else None,
                "asset.expiration": asset.expiration if asset is not None else None,
                "asset.asset_type": asset.asset_type if asset is not None else None,
            }
            if price_source:
                new_row["price_source"] = price_source
        else:
            # PERF: In backtests, trade events are appended in large volumes. Avoid property
            # lookups where we can safely access the backing fields.
            new_row = (
                current_dt,
                stored_order.strategy,
                stored_order.exchange,
                stored_order._identifier,
                stored_order.symbol,
                stored_order.side,
                stored_order.order_type,
                stored_order._status,
                price,
                filled_quantity,
                multiplier,
                stored_order.trade_cost,
                getattr(stored_order, "trade_slippage", None),
                stored_order.time_in_force,
                asset.right if asset is not None else None,
                asset.strike if asset is not None else None,
                asset.multiplier if asset is not None else None,
                asset.expiration if asset is not None else None,
                asset.asset_type if asset is not None else None,
                price_source,
            )

        # Backtest-only trade audit telemetry.
        #
        # WHY: NVDA/SPX investigations require a bulletproof, per-fill record of the *inputs used
        # to decide* a fill (bar OHLC, quote bid/ask, underlying quote, slippage model, etc.).
        # We keep this behind an env flag because it increases CSV width and can add overhead.
        if audit_enabled:
            audit = getattr(stored_order, "_audit", None)
            if isinstance(audit, dict) and audit:
                for key, value in audit.items():
                    # Namespace audit fields to avoid collisions with existing columns.
                    new_row[f"audit.{key}"] = value
            try:
                delattr(stored_order, "_audit")
            except Exception:
                pass

        if price_source:
            try:
                delattr(stored_order, "_price_source")
            except AttributeError:
                pass
        if getattr(self, "_trade_event_log_enabled", True):
            # PERF: Avoid pandas concat per event; append to list and materialize once.
            self._trade_event_log_rows.append(new_row)
            # Invalidate cached DataFrame representation.
            self._trade_event_log_df_cache = None

        # Ensure cleanup runs even when a strategy rarely submits orders.
        try:
            self._trigger_periodic_cleanup()
        except Exception:
            pass

        return

    def _launch_stream(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""
        self._register_stream_events()
        t = Thread(target=self._run_stream, daemon=True, name=f"broker_{self.name}_thread")
        t.start()
        if not self.IS_BACKTESTING_BROKER:
            self.logger.info(
                """Waiting for the socket stream connection to be established,
                method _stream_established must be called"""
            )
            timeout = 30  # 30 second timeout
            start_time = time.time()
            while not self._stop_event.is_set():
                if self._is_stream_subscribed is True:
                    break
                if time.time() - start_time > timeout:
                    self.logger.warning("Timeout waiting for stream to be established")
                    break
                time.sleep(0.1)
        return

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        """
        Get the latest quote for an asset.
        Returns a Quote object with bid, ask, last, and other fields if available.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.

        Returns
        -------
        Quote
            A Quote object with the quote information.
        """
        return self.data_source.get_quote(asset, quote, exchange)

    def export_trade_events_to_csv(self, filename):
        safe_logger = getattr(self, "logger", None) or logger

        df = self._trade_event_log_df
        if df is None:
            df = pd.DataFrame()

        if df.empty:
            columns = getattr(self, "_trade_event_log_columns", None) or TRADE_EVENT_LOG_COLUMNS
            df = pd.DataFrame(columns=list(columns))
            df.to_csv(filename, index=False)
        else:
            # Preserve legacy "time as index" CSV formatting for readability.
            output_df = df.set_index("time") if "time" in df.columns else df
            output_df.to_csv(filename)

        parquet_filename = (
            filename[:-4] + ".parquet" if str(filename).lower().endswith(".csv") else str(filename) + ".parquet"
        )
        required = bool(self.IS_BACKTESTING_BROKER) and is_parquet_required()
        write_parquet_with_logging(
            df=df,
            path=parquet_filename,
            artifact="trade_events",
            logger=safe_logger,
            index=False,
            required=required,
            compression="zstd",
            sanitizer=coerce_object_columns_to_json_strings,
        )

    def set_strategy_name(self, strategy_name):
        """
        Let's the broker know the name of the strategy that is using it for logging purposes.

        Parameters
        ----------
        strategy_name : str
            The name of the strategy that is using the broker.
        """
        self._strategy_name = strategy_name

        # Update the strategy name in the logger
        self.logger.update_strategy_name(strategy_name)

    def _perform_cleanup(self):
        """Perform cleanup actions based on the configured strategy."""
        # Call our new comprehensive cleanup method
        try:
            self._cleanup_old_tracking_data()
        except Exception as e:
            self.logger.warning(f"Memory cleanup failed: {e}")
