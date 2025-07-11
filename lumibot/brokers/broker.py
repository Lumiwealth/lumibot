import os
import logging
import time
from abc import ABC, abstractmethod
from asyncio.log import logger
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

from ..data_sources import DataSource
from ..entities import Asset, Order, Position, Quote
from ..trading_builtins import SafeList

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

# Consolidate errors from different brokers into a single class that can be easily caught even
# if the user decides to switch brokers.
class LumibotBrokerAPIError(Exception):
    pass


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Check if the level is enabled to avoid formatting costs if not necessary
        if self.logger.isEnabledFor(kwargs.get('level', logging.INFO)):
            # Lazy formatting of the message
            return f'[{self.extra["strategy_name"]}] {msg}', kwargs
        else:
            return msg, kwargs

    def update_strategy_name(self, new_strategy_name):
        self.extra['strategy_name'] = new_strategy_name
        # Pre-format part of the log message that's static or changes infrequently
        self.formatted_prefix = f'[{new_strategy_name}]'


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
    ERROR_ORDER = "error"
    PLACEHOLDER_ORDER = "placeholder"

    def __init__(self, name="", connect_stream=True, data_source: DataSource = None, option_source: DataSource = None,
                 config=None, max_workers=20, extended_trading_minutes=0, cleanup_config=None):
        """Broker constructor"""
        # Shared Variables between threads
        self.name = name
        self._lock = RLock()
        self._unprocessed_orders = SafeList(self._lock)
        self._placeholder_orders = SafeList(self._lock)
        self._new_orders = SafeList(self._lock)
        self._canceled_orders = SafeList(self._lock)
        self._partially_filled_orders = SafeList(self._lock)
        self._filled_orders = SafeList(self._lock)
        self._error_orders = SafeList(self._lock)
        self._filled_positions = SafeList(self._lock)
        self._subscribers = SafeList(self._lock)
        self._is_stream_subscribed = False
        self._trade_event_log_df = pd.DataFrame()
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
        self.logger = CustomLoggerAdapter(logger, {'strategy_name': "unknown"})

        # --- Market calendar setting ---
        # StrategyExecutor relies on broker.market to decide whether trading is
        # 24/7 or should follow an exchange calendar.  Derive it from config or
        # env, else default to "NASDAQ" which is compatible with pandas-market-calendars.
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "NASDAQ"

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
        
        items_to_remove = []
        max_age_days = policy.get("max_age_days")
        max_count = policy.get("max_count")
        min_keep = policy.get("min_keep", 0)
        
        # Sort items by age (newest first) to preserve recent items
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
        if self.option_source and asset.asset_type == "option":
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
        return (self._unprocessed_orders.get_list() + self._new_orders.get_list() +
                self._partially_filled_orders.get_list() + self._filled_orders.get_list() +
                self._error_orders.get_list() + self._canceled_orders.get_list() + self._placeholder_orders.get_list())

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
        return self.data_source.get_chains(asset)

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
        list of str
            Sorted list of dates in the form of `2022-10-13`.
        """
        return sorted(set(chains["Chains"]["CALL"].keys()) | set(chains["Chains"]["PUT"].keys()))

    def get_strikes(self, asset, chains=None):
        """Returns the strikes for an option asset with right and expiry."""
        # If provided chains, use them. It is faster than querying the data source.
        if chains and "Chains" in chains:
            if asset.asset_type == "option":
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
        while True:
            # at first, block maybe a list of orders or just one order
            block = self._orders_queue.get()
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

        if order.asset.asset_type == "crypto":
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
        else:
            # Add the order to the already existing position
            position.add_order(order)  # Don't update quantity here, it's handled by querying broker

        if order.asset.asset_type == "crypto":
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

    def _process_cash_settlement(self, order, price, quantity):
        self.logger.info(
            colored(
                f"Cash Settled: {order.side} {quantity} of {order.asset.symbol} at {price:,.8f} {'USD'} per share",
                color="green",
            )
        )

        self._new_orders.remove(order.identifier, key="identifier")
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.add_transaction(price, quantity)
        order.status = self.CASH_SETTLED
        order.set_filled()
        self._filled_orders.append(order)

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is not None:
            # Add the order to the already existing position
            position.add_order(order)  # Don't update quantity here, it's handled by querying broker

    def _process_crypto_quote(self, order, quantity, price):
        """Used to process the quote side of a crypto trade."""
        # Handle cases where price might be None (can happen with some filled orders)
        if price is None:
            # Try to use the limit price if available, otherwise skip processing
            if hasattr(order, 'limit_price') and order.limit_price is not None:
                price = order.limit_price
                logging.debug(f"Using limit_price {price} for crypto quote processing since avg_fill_price was None for order {order.identifier}")
            else:
                logging.debug(f"Skipping crypto quote processing for order {order.identifier} - both avg_fill_price and limit_price are None")
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
        if self.market == "24/7":
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
        tracked_orders = list(self._tracked_orders) + (self._placeholder_orders.get_list() if use_placeholders else [])
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

    def cancel_open_orders(self, strategy):
        """cancel all open orders for a given strategy"""
        orders = [o for o in self.get_tracked_orders(strategy) if o.is_active()]
        self.cancel_orders(orders)

    def wait_orders_clear(self, strategy, max_loop=5):
        # Returns true if outstanding orders for a strategy are complete.

        while max_loop > 0:
            outstanding_orders = [
                order
                for order in (
                    self._unprocessed_orders.get_list()
                    + self._new_orders.get_list()
                    + self._partially_filled_orders.get_list()
                )
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
            order = pos.get_selling_order(quote_asset=self.quote_assets and next(iter(self.quote_assets)))
            if fraction != 1.00:
                order.quantity = order.quantity * fraction
            return self.submit_order(order)
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

        self.logger.info(colored(f"New order was created: {order}", color="green"))

        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.NEW_ORDER, payload)

    def _on_canceled_order(self, order):
        """notify relevant subscriber/strategy about
        canceled order event"""

        self.logger.info(colored(f"Order was canceled: {order}", color="green"))

        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.CANCELED_ORDER, payload)

    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        """notify relevant subscriber/strategy about
        partially filled order event"""

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

            # Log that the trade event was received
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
        # Log that the trade event was received
        self.logger.info(
            f"Processing trade event. Trade event received for {stored_order.strategy} strategy: {type_event} "
            f"{stored_order.symbol} ID={stored_order.identifier}, processed by broker {self.name}"
        )

        if self._hold_trade_events and not self.IS_BACKTESTING_BROKER:
            # Log that the trade event was held
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
        if (type_event in [self.FILLED_ORDER, self.PARTIALLY_FILLED_ORDER] and
                stored_order.order_class != Order.OrderClass.OCO and
                (price is None or filled_quantity is None)):
            raise ValueError(
                f"""For filled_order and partially_filled_order event,
                price and filled_quantity must be specified.
                Received respectively {price} and {filled_quantity}"""
            )

        if filled_quantity is not None:
            error = ValueError(
                f"filled_quantity must be an integer or float, received {filled_quantity} instead")
            try:
                if not isinstance(filled_quantity, float):
                    filled_quantity = float(filled_quantity)
            except ValueError:
                raise error

        if price is not None and not isinstance(price, float):
            try:
                price = float(price)
            except ValueError:
                raise ValueError(f"price must be a positive float, received {price} instead") from None

        if Order.is_equivalent_status(type_event, self.NEW_ORDER):
            order = self._process_new_order(stored_order)
            if order:
                self._on_new_order(order)
        elif Order.is_equivalent_status(type_event, self.PLACEHOLDER_ORDER):
            order = self._process_placeholder_order(stored_order)
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
            self.logger.info(colored(f"Order was modified: {stored_order}", color="yellow"))
            # Update raw data if modification response is available (might need adjustment)
            # stored_order.update_raw(modification_response_data)
            # self._on_modified_order(stored_order) # Need to implement _on_modified_order
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
        else:
            self.logger.warning(f"Unknown trade event type: {type_event}")

        current_dt = self.data_source.get_datetime()
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
            "time_in_force": stored_order.time_in_force,
            "asset.right": stored_order.asset.right if stored_order.asset is not None else None,
            "asset.strike": stored_order.asset.strike if stored_order.asset is not None else None,
            "asset.multiplier": stored_order.asset.multiplier if stored_order.asset is not None else None,
            "asset.expiration": stored_order.asset.expiration if stored_order.asset is not None else None,
            "asset.asset_type": stored_order.asset.asset_type if stored_order.asset is not None else None,
        }
        # Create a DataFrame with the new row
        new_row_df = pd.DataFrame(new_row, index=[0])

        # Filter out empty or all-NA columns from new_row_df
        new_row_df = new_row_df.dropna(axis=1, how="all")

        # Concatenate the filtered new_row_df with the existing _trade_event_log_df
        self._trade_event_log_df = pd.concat([self._trade_event_log_df, new_row_df], axis=0)

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
            while True:
                if self._is_stream_subscribed is True:
                    break
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
        if len(self._trade_event_log_df) > 0:
            output_df = self._trade_event_log_df.set_index("time")
            output_df.to_csv(filename)

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
