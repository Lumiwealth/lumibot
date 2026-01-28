import json
import math
import os
import time
import traceback
import threading
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
import polars as pl
import pytz

from lumibot.brokers import Broker
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Order, Position, SmartLimitConfig, TradingFee
from lumibot.tools.smart_limit_utils import build_price_ladder, compute_final_price, compute_mid, expected_fill_price, infer_tick_size, round_to_tick
from lumibot.tools.lumibot_logger import get_logger
from lumibot.trading_builtins import CustomStream

try:
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
except Exception:  # pragma: no cover - optional dependency
    ThetaDataBacktestingPandas = None

logger = get_logger(__name__)


# Typical initial margin requirements for common futures contracts
# Used for backtesting to simulate margin deduction/release
TYPICAL_FUTURES_MARGINS = {
    # CME Micro E-mini Futures
    "MES": 1300,      # Micro E-mini S&P 500 (~$1,300)
    "MNQ": 1700,      # Micro E-mini Nasdaq-100 (~$1,700)
    "MYM": 1100,      # Micro E-mini Dow (~$1,100)
    "M2K": 800,       # Micro E-mini Russell 2000 (~$800)
    "MCL": 1500,      # Micro Crude Oil (~$1,500)
    "MGC": 1200,      # Micro Gold (~$1,200)

    # CME Standard E-mini Futures
    "ES": 13000,      # E-mini S&P 500 (~$13,000)
    "NQ": 17000,      # E-mini Nasdaq-100 (~$17,000)
    "YM": 11000,      # E-mini Dow (~$11,000)
    "RTY": 8000,      # E-mini Russell 2000 (~$8,000)

    # CME Full-Size Futures
    "CL": 8000,       # Crude Oil (~$8,000)
    "GC": 10000,      # Gold (~$10,000)
    "SI": 14000,      # Silver (~$14,000)
    "NG": 3000,       # Natural Gas (~$3,000)
    "HG": 4000,       # Copper (~$4,000)

    # CME Currency Futures
    "6E": 2500,       # Euro FX (~$2,500)
    "6J": 3000,       # Japanese Yen (~$3,000)
    "6B": 2800,       # British Pound (~$2,800)
    "6C": 2000,       # Canadian Dollar (~$2,000)

    # CME Interest Rate Futures
    "ZB": 4000,       # 30-Year T-Bond (~$4,000)
    "ZN": 2000,       # 10-Year T-Note (~$2,000)
    "ZF": 1500,       # 5-Year T-Note (~$1,500)
    "ZT": 800,        # 2-Year T-Note (~$800)

    # CME Agricultural Futures
    "ZC": 2000,       # Corn (~$2,000)
    "ZS": 3000,       # Soybeans (~$3,000)
    "ZW": 2500,       # Wheat (~$2,500)
    "ZL": 1500,       # Soybean Oil (~$1,500)

    # Default for unknown futures
    "DEFAULT": 5000,  # Conservative default
}


def get_futures_margin_requirement(asset: Asset) -> float:
    """
    Get the initial margin requirement for a futures contract.

    This is used in backtesting to simulate the margin deduction when opening
    a futures position and margin release when closing.

    Args:
        asset: The futures Asset object

    Returns:
        float: Initial margin requirement in dollars

    Note:
        These are TYPICAL values and may not match current broker requirements.
        For live trading, brokers handle margin internally.
    """
    symbol = asset.symbol.upper()

    # Try exact match first
    if symbol in TYPICAL_FUTURES_MARGINS:
        return TYPICAL_FUTURES_MARGINS[symbol]

    # Try base symbol (remove month/year codes like "ESH4" -> "ES")
    # Most futures symbols are 2-3 characters followed by month/year
    base_symbol = ''.join(c for c in symbol if c.isalpha())
    if base_symbol in TYPICAL_FUTURES_MARGINS:
        return TYPICAL_FUTURES_MARGINS[base_symbol]

    # Unknown contract - use conservative default
    logger.warning(
        f"Unknown futures contract '{symbol}'. Using default margin of "
        f"${TYPICAL_FUTURES_MARGINS['DEFAULT']:.2f}. "
        f"Consider adding this contract to TYPICAL_FUTURES_MARGINS."
    )
    return TYPICAL_FUTURES_MARGINS["DEFAULT"]


class BacktestingBroker(Broker):
    # Metainfo
    IS_BACKTESTING_BROKER = True

    def __init__(self, data_source, option_source=None, connect_stream=True, max_workers=20, config=None, **kwargs):
        super().__init__(name="backtesting", data_source=data_source,
                         option_source=option_source, connect_stream=connect_stream, **kwargs)
        # Calling init methods
        self.max_workers = max_workers
        self.option_source = option_source

        # Legacy strategy.backtest code will always pass in a config even for Brokers that don't need it, so
        # catch it here and ignore it in this class. Child classes that need it should error check it themselves.
        # self._config = config

        # Check if data source is a backtesting data source
        if not (isinstance(self.data_source, DataSourceBacktesting) or
                (hasattr(self.data_source, 'IS_BACKTESTING_DATA_SOURCE') and
                 self.data_source.IS_BACKTESTING_DATA_SOURCE)):
            raise ValueError("Must provide a backtesting data_source to run with a BacktestingBroker")

        # Market session caching for performance optimization
        self._market_session_cache = OrderedDict()  # LRU-style cache
        self._cache_max_size = 500

        # Simple day-based session dict for O(1) lookup
        self._daily_sessions = {}  # {date: [(start, end), ...]}
        self._sessions_built = False

        # Prefetchers (optional). Some builds/tests won't configure these.
        # Initialize to None so attribute checks are safe in processing code.
        self.prefetcher = None
        self.hybrid_prefetcher = None
        self._last_cache_clear = None
        # Market open lookup cache (populated when calendars are initialized)
        self._market_open_cache = {}
        # Track per-strategy futures lots for accurate margin/P&L when flipping
        self._futures_lot_ledgers = defaultdict(list)

        # Backtest-only trade audit telemetry (NVDA/SPX investigations).
        #
        # WHY: Investigations sometimes require a per-fill record of the inputs used to decide fills
        # (OHLC bars, quote bid/ask, spread gating, SMART_LIMIT model inputs, underlying quotes, etc.).
        # This can add overhead and widen CSV outputs, so keep it disabled by default and gate behind
        # `LUMIBOT_BACKTEST_AUDIT=1`.
        self._backtest_audit_enabled = self._truthy_env(os.environ.get("LUMIBOT_BACKTEST_AUDIT"))
        # Track end-of-data to prevent infinite loops when end date is in the future
        self._end_of_trading_days_reached = False

    def _mark_end_of_trading_days(self, now):
        if self._end_of_trading_days_reached:
            return

        self._end_of_trading_days_reached = True
        logger.warning(
            "Backtesting reached end of available trading days data; stopping at %s",
            now,
        )
        if self.data_source.datetime_end > now:
            self.data_source.datetime_end = now
        if self.option_source and hasattr(self.option_source, "datetime_end"):
            if self.option_source.datetime_end > now:
                self.option_source.datetime_end = now
    def initialize_market_calendars(self, trading_days_df):
        """Initialize trading calendar and eagerly build caches for backtesting."""
        super().initialize_market_calendars(trading_days_df)
        # Prepare caches when calendar is set
        self._market_open_cache = {}
        self._daily_sessions = {}
        self._sessions_built = False
        if self._trading_days is None or len(self._trading_days) == 0:
            return
        self._market_open_cache = self._trading_days['market_open'].to_dict()
        for close_time in self._trading_days.index:
            open_time = self._market_open_cache[close_time]
            for dt in (open_time, close_time):
                day = dt.date()
                sess = (open_time, close_time)
                self._daily_sessions.setdefault(day, [])
                if sess not in self._daily_sessions[day]:
                    self._daily_sessions[day].append(sess)
        self._sessions_built = True

    def _build_daily_sessions(self):
        """Build day-based session dict for fast O(1) day lookup."""
        if (
            self._trading_days is None or
            len(self._trading_days) == 0 or
            self._sessions_built
        ):
            return

        # Optimize: Convert market_open column to dict once to avoid many .at calls
        if not self._market_open_cache:
            self._market_open_cache = self._trading_days['market_open'].to_dict()

        # Group sessions by day for fast lookup
        for close_time in self._trading_days.index:
            open_time = self._market_open_cache[close_time]

            # Add to both days the session might span
            for dt in [open_time, close_time]:
                day = dt.date()
                if day not in self._daily_sessions:
                    self._daily_sessions[day] = []
                if (open_time, close_time) not in self._daily_sessions[day]:
                    self._daily_sessions[day].append((open_time, close_time))

        self._sessions_built = True

    def _ensure_market_open_cache(self):
        """Populate the market-open cache if it has not been initialized."""
        if self._market_open_cache or self._trading_days is None:
            return
        self._market_open_cache = self._trading_days['market_open'].to_dict()

    def _get_market_open_for_close(self, close_time):
        self._ensure_market_open_cache()
        return self._market_open_cache.get(close_time)

    def _contiguous_session_time(self, now, idx):
        """Return remaining time when sessions share a boundary (e.g., futures, crypto)."""
        if not self.is_market_open():
            return None

        next_idx = idx + 1
        if next_idx >= len(self._trading_days):
            return None

        next_close = self._trading_days.index[next_idx]
        next_open = self._get_market_open_for_close(next_close)
        if next_open is None:
            return None

        if next_open <= now < next_close:
            return (next_close - now).total_seconds()

        return None

    def _is_market_open_dict(self, now):
        """Fast O(1) day lookup then check few sessions."""
        if not self._sessions_built:
            self._build_daily_sessions()

        # O(1) lookup by day, then check just a few sessions
        day = now.date()
        sessions = self._daily_sessions.get(day, [])

        for start, end in sessions:
            # Daily backtests (PandasData w/ timestep="day") often produce calendars where
            # ``market_open == market_close`` (single timestamp per day). Treat that instant
            # as "open" so StrategyExecutor doesn't skip all iterations.
            if start == end:
                if now == start:
                    return True
                continue
            if start <= now < end:
                return True
        return False

    @property
    def datetime(self):
        return self.data_source.get_datetime()

    def _get_balances_at_broker(self, quote_asset, strategy):
        """
        Get the balances of the broker
        """
        # return self._data_source.get_balances()
        pass

    def _get_tick(self, order: Order):
        """TODO: Review this function with Rob"""
        pass

    def get_historical_account_value(self):
        pass

    def get_active_tracked_orders(self, strategy: str) -> list[Order]:
        """Return active (open/submitted/new) orders for the given strategy.

        Backtests can accumulate tens of thousands of filled orders over long windows.
        Scanning all tracked orders and calling ``Order.is_active()`` each bar is a major
        performance bottleneck, so this method sources active orders directly from the
        broker's active-order buckets.
        """
        active_orders: list[Order] = []
        try:
            buckets = (
                self._unprocessed_orders.get_list(),
                self._new_orders.get_list(),
                self._partially_filled_orders.get_list(),
            )
        except Exception:
            # Fallback to the slower path if internal buckets are unavailable.
            orders = self.get_tracked_orders(strategy=strategy)
            return [o for o in orders if o.is_active()] if orders else []

        for bucket in buckets:
            for order in bucket:
                if getattr(order, "strategy", None) != strategy:
                    continue
                # Buckets should already contain only active orders, but keep a defensive check.
                if order.is_active():
                    active_orders.append(order)
        return active_orders

    # =========Internal functions==================

    def _update_datetime(self, update_dt, cash=None, portfolio_value=None, positions=None, initial_budget=None, orders=None):
        """Works with either timedelta or datetime input
        and updates the datetime of the broker.

        Parameters
        ----------
        update_dt : timedelta, int, float, or datetime
            The time to advance by (if timedelta/int/float) or the new datetime
        cash : float, optional
            Current cash balance
        portfolio_value : float, optional
            Current portfolio value
        positions : list, optional
            List of minimal position dicts from Position.to_minimal_dict()
        initial_budget : float, optional
            Initial budget for calculating return percentage
        orders : list, optional
            List of minimal order dicts from Order.to_minimal_dict()
        """
        tz = self.datetime.tzinfo
        is_pytz = isinstance(tz, (pytz.tzinfo.StaticTzInfo, pytz.tzinfo.DstTzInfo))

        previous_datetime = self.datetime

        if isinstance(update_dt, timedelta):
            new_datetime = self.datetime + update_dt
        elif isinstance(update_dt, int) or isinstance(update_dt, float):
            new_datetime = self.datetime + timedelta(seconds=update_dt)
        else:
            new_datetime = update_dt

        # This is needed to handle Daylight Savings Time changes
        new_datetime = tz.normalize(new_datetime) if is_pytz else new_datetime

        # Guard against non-advancing timestamps (e.g., DST ambiguity)
        if new_datetime <= previous_datetime:
            new_datetime = previous_datetime + timedelta(minutes=1)
            if is_pytz:
                new_datetime = tz.normalize(new_datetime)

        self.data_source._update_datetime(
            new_datetime,
            cash=cash,
            portfolio_value=portfolio_value,
            positions=positions,
            initial_budget=initial_budget,
            orders=orders
        )
        if self.option_source:
            self.option_source._update_datetime(new_datetime, cash=cash, portfolio_value=portfolio_value)

    # =========Clock functions=====================

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit datetime was reached"""

        if self._end_of_trading_days_reached:
            return False

        # If we are at the end of the data source, we should stop
        if self.datetime >= self.data_source.datetime_end:
            return False

        # All other cases we should continue
        return True

    def is_market_open(self):
        """Return True if market is open else false"""
        now = self.datetime

        # Handle 24/7 markets immediately
        if self.market == "24/7":
            return True

        # Simple, fast cache with timestamp key
        cache_key = int(now.timestamp() * 1000)

        # Check cache first
        if cache_key in self._market_session_cache:
            self._market_session_cache.move_to_end(cache_key)
            return self._market_session_cache[cache_key]

        # Use fast day-based dict lookup
        result = self._is_market_open_dict(now)

        # Cache result with LRU eviction
        self._market_session_cache[cache_key] = result
        if len(self._market_session_cache) > self._cache_max_size:
            self._market_session_cache.popitem(last=False)

        return result

    def _get_next_trading_day(self):
        now = self.datetime
        search = self._trading_days[now < self._trading_days.market_open]
        if search.empty:
            self._mark_end_of_trading_days(now)
            return None

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self.datetime

        search = self._trading_days[now < self._trading_days.index]
        if search.empty:
            self._mark_end_of_trading_days(now)
            return None

        trading_day = search.iloc[0]
        open_time = trading_day.market_open

        # DEBUG: Log what's happening
        logger.debug(f"[BROKER DEBUG] get_time_to_open: now={now}, next_trading_day={trading_day.name}, "
                     f"open_time={open_time}")

        # For Backtesting, sometimes the user can just pass in dates (i.e. 2023-08-01) and not datetimes
        # In this case the "now" variable is starting at midnight, so we need to adjust the open_time to be actual
        # market open time.  In the case where the user passes in a valid trading day, use that time
        # as the start of trading instead of market open.
        # BUT: Only do this if the current day (now.date()) is actually a trading day
        if self.IS_BACKTESTING_BROKER and now > open_time:
            # Check if now.date() is in trading days before overriding
            now_date = now.date() if hasattr(now, 'date') else now
            trading_day_dates = self._trading_days.index.date
            if now_date in trading_day_dates:
                logger.debug(f"[BROKER DEBUG] Overriding open_time to datetime_start because now ({now}) is on a "
                             f"trading day but after market open")
                open_time = self.data_source.datetime_start
            else:
                logger.debug(f"[BROKER DEBUG] NOT overriding open_time because now ({now}) is NOT a trading day")

        if now >= open_time:
            logger.debug(f"[BROKER DEBUG] Market already open: now={now} >= open_time={open_time}, returning 0")
            return 0

        delta = open_time - now
        logger.debug(f"[BROKER DEBUG] Market opens in {delta.total_seconds()} seconds")
        return delta.total_seconds()

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        now = self.datetime

        # Use searchsorted for efficient searching and reduce unnecessary DataFrame access
        idx = self._trading_days.index.searchsorted(now, side='left')

        if idx >= len(self._trading_days):
            logger.warning(f"Backtest has reached the end of available trading days data. Current time: {now}, Last trading day: {self._trading_days.index[-1] if len(self._trading_days) > 0 else 'No data'}")
            # Return None to signal that backtesting should stop
            return None

        # Directly access the data needed using more efficient methods
        market_close_time = self._trading_days.index[idx]
        market_open = self._get_market_open_for_close(market_close_time)
        if market_open is None:
            logger.warning("Missing market_open for %s; cannot compute time_to_close", market_close_time)
            return None
        market_close = market_close_time  # Assuming this is a scalar value directly from the index

        # If we're before the market opens for the found trading day,
        # count the whole time until that day's market close so the clock
        # can advance instead of stalling.
        if now < market_open:
            delta = market_close - now
            return delta.total_seconds()

        delta_seconds = (market_close - now).total_seconds()
        if delta_seconds <= 0:
            contiguous_seconds = self._contiguous_session_time(now, idx)
            if contiguous_seconds is not None:
                return contiguous_seconds

            logger.debug(
                "Backtesting clock reached or passed market close (%s >= %s); returning 0 seconds.",
                now,
                market_close,
            )
            return 0.0

        return delta_seconds

    def _await_market_to_open(self, timedelta=None, strategy=None):
        # Process outstanding orders first before waiting for market to open
        # or else they don't get processed until the next day
        logger.debug(f"[BROKER DEBUG] _await_market_to_open called, current "
                     f"datetime={self.datetime}, timedelta={timedelta}")
        self.process_pending_orders(strategy=strategy)

        time_to_open = self.get_time_to_open()
        logger.debug(f"[BROKER DEBUG] get_time_to_open returned: {time_to_open}")

        # If None is returned, it means we've reached the end of available trading days
        if time_to_open is None:
            logger.debug(f"[BROKER DEBUG] time_to_open is None, returning early")
            return

        # Allow the caller to specify a buffer (in minutes) before the actual open
        if timedelta:
            time_to_open -= 60 * timedelta
            logger.debug(f"[BROKER DEBUG] Adjusted time_to_open for timedelta buffer: {time_to_open}")

        # Only advance time if there is something positive to advance;
        # prevents zero or negative time updates.
        if time_to_open <= 0:
            logger.debug(f"[BROKER DEBUG] time_to_open <= 0 ({time_to_open}), returning without advancing time")
            return

        logger.debug(f"[BROKER DEBUG] Advancing time by {time_to_open} seconds")
        self._update_datetime(time_to_open)

    def _await_market_to_close(self, timedelta=None, strategy=None):
        """Wait until market closes or specified time before close"""
        # Process outstanding orders first before waiting for market to close
        # or else they don't get processed until the next day
        self.process_pending_orders(strategy=strategy)

        result = self.get_time_to_close()

        # If get_time_to_close returned None (e.g., market already closed or error), do nothing.
        if result is None:
            return

        time_to_close = result

        if timedelta is not None:
            time_to_close -= 60 * timedelta

        # Only advance time if there is positive time remaining.
        if time_to_close > 0:
            self._update_datetime(time_to_close)
        # If the calculated time is non-positive (e.g., exactly `minutes_before_closing` before
        # the close), do nothing. Nudging by 1 second can shift the simulated timestamp off
        # bar boundaries (e.g., 17:59:01), which breaks parity and can cause subtle lookups.
        # Otherwise (result <= 0 initially), the market is already closed.

    # =========Positions functions==================
    def _pull_broker_position(self, asset):
        """Given an asset, get the broker representation
        of the corresponding asset"""
        orders = []
        quantity = 0
        for position in self._filled_positions:
            if position.asset == asset:
                orders.extend(position.orders)
                quantity += position.quantity

        response = Position("", asset, quantity, orders=orders)
        return response

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        response = self._filled_positions.__items
        return response

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        broker_position.strategy = strategy
        return broker_position

    # =======Orders and assets functions=========

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        order = response
        return order

    def _pull_broker_order(self, identifier):
        """Get a broker order representation by its id"""
        for order in self._tracked_orders:
            if order.id == identifier:
                return order
        return None

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
        orders = self.get_all_orders()
        return orders

    def _flatten_order(self, order):
        """Some submitted orders may triggers other orders.
        _flatten_order returns a list containing the derived orders"""
        # OCO order does not include the main parent (entry) order becuase that has been placed earlier. Only the
        # child (exit) orders are included in the list
        orders = []
        if order.order_class is not Order.OrderClass.OCO:
            orders.append(order)

        if order.is_parent():
            for child_order in order.child_orders:
                orders.extend(self._flatten_order(child_order))

        # This entire else block should be depricated as child orders should be built in the Order.__init__()
        # to ensure that the proper orders are created up front.
        else:
            # David M - Note sure what case this "empty" block is supposed to support.  Why is it adding itself and
            # a stop loss order?  But not a potential limit order?
            if order.order_class == "" or order.order_class is None:
                orders.append(order)
                if order.stop_price:
                    stop_limit_price = getattr(order, "stop_limit_price", None)
                    trail_price = getattr(order, "trail_price", None)
                    trail_percent = getattr(order, "trail_percent", None)

                    if stop_limit_price is not None:
                        child_order_type = Order.OrderType.STOP_LIMIT
                    elif trail_price is not None or trail_percent is not None:
                        child_order_type = Order.OrderType.TRAIL
                    else:
                        child_order_type = Order.OrderType.STOP

                    stop_loss_order = Order(
                        order.strategy,
                        order.asset,
                        order.quantity,
                        order.side,
                        stop_price=order.stop_price,
                        stop_limit_price=stop_limit_price,
                        trail_price=trail_price,
                        trail_percent=trail_percent,
                        quote=order.quote,
                        order_type=child_order_type,
                    )
                    stop_loss_order = self._parse_broker_order(stop_loss_order, order.strategy)
                    orders.append(stop_loss_order)

            elif order.order_class is Order.OrderClass.OCO:
                stop_limit_price = getattr(order, "stop_limit_price", None)
                stop_child_type = Order.OrderType.STOP_LIMIT if stop_limit_price else Order.OrderType.STOP
                stop_loss_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    order.side,
                    stop_price=order.stop_price,
                    stop_limit_price=stop_limit_price,
                    quote=order.quote,
                    order_type=stop_child_type,
                )
                orders.append(stop_loss_order)

                limit_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    order.side,
                    limit_price=order.limit_price,
                    quote=order.quote,
                    order_type=Order.OrderType.LIMIT,
                )
                orders.append(limit_order)

                stop_loss_order.dependent_order = limit_order
                limit_order.dependent_order = stop_loss_order

            elif order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
                side = Order.OrderSide.SELL if order.is_buy_order() else Order.OrderSide.BUY
                if (order.order_class is Order.OrderClass.BRACKET or
                        (order.order_class is Order.OrderClass.OTO and order.secondary_stop_price)):
                    secondary_stop_limit_price = getattr(order, "secondary_stop_limit_price", None)
                    secondary_trail_price = getattr(order, "secondary_trail_price", None)
                    secondary_trail_percent = getattr(order, "secondary_trail_percent", None)

                    if secondary_stop_limit_price is not None:
                        child_order_type = Order.OrderType.STOP_LIMIT
                    elif secondary_trail_price is not None or secondary_trail_percent is not None:
                        child_order_type = Order.OrderType.TRAIL
                    else:
                        child_order_type = Order.OrderType.STOP

                    stop_loss_order = Order(
                        order.strategy,
                        order.asset,
                        order.quantity,
                        side,
                        stop_price=order.secondary_stop_price,
                        stop_limit_price=secondary_stop_limit_price,
                        trail_price=secondary_trail_price,
                        trail_percent=secondary_trail_percent,
                        quote=order.quote,
                        order_type=child_order_type,
                    )
                    orders.append(stop_loss_order)

                if (order.order_class is Order.OrderClass.BRACKET or
                        (order.order_class is Order.OrderClass.OTO and order.secondary_limit_price)):
                    limit_order = Order(
                        order.strategy,
                        order.asset,
                        order.quantity,
                        side,
                        limit_price=order.secondary_limit_price,
                        quote=order.quote,
                        order_type=Order.OrderType.LIMIT,
                    )
                    orders.append(limit_order)

                if order.order_class is Order.OrderClass.BRACKET:
                    stop_loss_order.dependent_order = limit_order
                    limit_order.dependent_order = stop_loss_order

        return orders

    def _cancel_open_orders_for_asset(
        self,
        strategy_name: str,
        asset: Asset,
        exclude_identifiers: set | None = None,
        *,
        cancel_sides: set | None = None,
    ):
        """Cancel any still-active orders for the given asset in backtesting.

        When a position is force-closed (manual exit or cash settlement) we need to ensure any
        remaining bracket/OTO child orders do not continue to execute against a zero position.
        """

        if exclude_identifiers is None:
            exclude_identifiers = set()

        if strategy_name is None or asset is None:
            return

        in_stream_thread = threading.current_thread().name.startswith(f"broker_{self.name}")

        # Track which orders have been canceled to avoid duplicate processing
        canceled_identifiers = set()

        def _cancel_inline(order: Order):
            if order.identifier in canceled_identifiers:
                return

            # BUGFIX: Only process CANCELED event if the order is actually active
            # Don't try to cancel orders that are already filled or canceled
            if order.is_active():
                canceled_identifiers.add(order.identifier)
                self._process_trade_event(order, self.CANCELED_ORDER)
            else:
                logger.debug(f"Order {order.identifier} not active (status={order.status}), skipping cancel event")
                canceled_identifiers.add(order.identifier)

            for child in order.child_orders:
                _cancel_inline(child)

        # PERF: Only active orders can be canceled; scanning full order history is
        # extremely expensive in long intraday option strategies.
        open_orders = self.get_active_tracked_orders(strategy=strategy_name)

        # Build a set of all child order identifiers to skip them in the main loop
        # (they will be handled by their parent orders)
        child_order_identifiers = set()
        for tracked_order in open_orders:
            for child in getattr(tracked_order, "child_orders", []) or []:
                child_order_identifiers.add(child.identifier)

        def _matches_cancel_sides(order: Order) -> bool:
            if cancel_sides is None:
                return True
            if getattr(order, "side", None) in cancel_sides:
                return True
            for child in getattr(order, "child_orders", []) or []:
                if _matches_cancel_sides(child):
                    return True
            return False

        for tracked_order in open_orders:
            if tracked_order.identifier in exclude_identifiers:
                continue
            if tracked_order.identifier in canceled_identifiers:
                continue
            # Skip child orders - they will be handled by their parent
            if tracked_order.identifier in child_order_identifiers:
                continue
            if tracked_order.asset != asset:
                continue
            if not _matches_cancel_sides(tracked_order):
                continue
            if in_stream_thread:
                _cancel_inline(tracked_order)
            else:
                self.cancel_order(tracked_order)

    def _process_filled_order(self, order, price, quantity):
        """
        BackTesting needs to create/update positions when orders are filled becuase there is no broker to do it
        """
        # This is a parent order, typically for a Multileg strategy. The parent order itself is expected to be
        # filled after all child orders are filled.
        if order.is_parent() and order.order_class in [Order.OrderClass.MULTILEG, Order.OrderClass.OCO]:
            order.avg_fill_price = price
            order.quantity = quantity
            order.add_transaction(price, quantity)
            order.status = Order.OrderStatus.FILLED
            order.set_filled()

            self._new_orders.remove(order.identifier, key="identifier")
            self._unprocessed_orders.remove(order.identifier, key="identifier")
            self._partially_filled_orders.remove(order.identifier, key="identifier")

            self._track_filled_order(order)

            return None

        existing_position = self.get_tracked_position(order.strategy, order.asset)

        # Currently perfect fill price in backtesting!
        order.avg_fill_price = price

        position = super()._process_filled_order(order, price, quantity)
        if existing_position:
            position.add_order(order, quantity)  # Add will update quantity, but not double count the order
            if position.quantity == 0:
                logger.info(f"Position {position} liquidated")
                self._filled_positions.remove(position)
                # If the position is flat after this fill, ensure any remaining close-only orders
                # (stops/trailing stops/limits) do not continue to trade against a zero position.
                #
                # WHY: Some strategies (including example strategies) submit multiple exit orders
                # without explicitly wrapping them in a BRACKET/OCO. In broker reality, "SELL"
                # is treated as closing a long position unless the user explicitly requests a short
                # side (SELL_SHORT / SELL_TO_OPEN). When a long position is liquidated, remaining
                # close-only SELL orders should be canceled rather than opening an unintended short.
                cancel_sides = None
                if getattr(order, "side", None) in {Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE}:
                    cancel_sides = {Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE}
                elif getattr(order, "side", None) in {Order.OrderSide.BUY_TO_COVER, Order.OrderSide.BUY_TO_CLOSE}:
                    cancel_sides = {Order.OrderSide.BUY_TO_COVER, Order.OrderSide.BUY_TO_CLOSE}

                if cancel_sides is not None:
                    self._cancel_open_orders_for_asset(
                        order.strategy,
                        order.asset,
                        {order.identifier},
                        cancel_sides=cancel_sides,
                    )
        else:
            self._filled_positions.append(position)  # New position, add it to the tracker

        # If this is a child order, update the parent order status if all children are filled or cancelled.
        if order.parent_identifier:
            parent_order = self.get_tracked_order(order.parent_identifier, use_placeholders=True)
            self._update_parent_order_status(parent_order)
        return position

    def _track_filled_order(self, order: Order) -> None:
        """Record a filled order without incurring O(n) SafeList membership scans."""
        if order is None:
            return
        identifier = getattr(order, "identifier", None)
        if identifier is None:
            self._filled_orders.append(order)
            return

        filled_ids = getattr(self, "_filled_order_identifiers", None)
        if filled_ids is None:
            filled_ids = set()
            setattr(self, "_filled_order_identifiers", filled_ids)

        if identifier in filled_ids:
            return
        filled_ids.add(identifier)
        self._filled_orders.append(order)

    def _process_partially_filled_order(self, order, price, quantity):
        """
        BackTesting needs to create/update positions when orders are partially filled becuase there is no broker
        to do it
        """
        existing_position = self.get_tracked_position(order.strategy, order.asset)
        stored_order, position = super()._process_partially_filled_order(order, price, quantity)
        if existing_position:
            position.add_order(stored_order, quantity)  # Add will update quantity, but not double count the order
        return stored_order, position

    def _process_cash_settlement(self, order, price, quantity):
        """
        BackTesting needs to create/update positions when orders are filled becuase there is no broker to do it
        """
        existing_position = self.get_tracked_position(order.strategy, order.asset)
        super()._process_cash_settlement(order, price, quantity)
        if existing_position:
            existing_position.add_order(order, quantity)  # Add will update quantity, but not double count the order
            if existing_position.quantity == 0:
                logger.info("Position %r liquidated" % existing_position)
                self._filled_positions.remove(existing_position)
                self._cancel_open_orders_for_asset(order.strategy, order.asset, {order.identifier})

    def _update_parent_order_status(self, order: Order):
        """Update the status of a parent order based on the status of its child orders."""
        if order is None or not order.is_parent():
            return

        child_states = [
            (child.is_active(), child.is_filled(), child.is_canceled())
            for child in order.child_orders
        ]

        if any(active for active, _, _ in child_states):
            return

        if all(cancelled for _, _, cancelled in child_states):
            self.cancel_order(order)
            return

        if any(filled for _, filled, _ in child_states):
            filled_children = [child for child in order.child_orders if child.is_filled()]

            if filled_children:
                # Aggregate quantity across all legs using absolute values to ensure totals remain positive.
                aggregated_qty = sum(
                    Decimal(str(abs(float(child.quantity)))) for child in filled_children
                )

                # Compute a net price similar to the legacy logic used when synthesising parent fills.
                net_price = Decimal("0")
                for child in filled_children:
                    fill_price = child.get_fill_price()
                    if fill_price is None:
                        continue

                    signed = Decimal(str(fill_price))
                    if child.is_sell_order():
                        signed *= Decimal("-1")
                    net_price += signed

                order.quantity = aggregated_qty
                order.avg_fill_price = net_price
                order.trade_cost = 0.0

            order.status = Order.OrderStatus.FILLED
            order.set_filled()
            self._new_orders.remove(order.identifier, key="identifier")
            self._unprocessed_orders.remove(order.identifier, key="identifier")
            self._partially_filled_orders.remove(order.identifier, key="identifier")

            self._track_filled_order(order)

    def _submit_order(self, order):
        """Submit an order for an asset"""

        # Optional audit trail (submission-time context).
        #
        # Invariant: audit collection must never break backtests; any errors must be swallowed.
        audit_enabled = self._audit_enabled()
        if audit_enabled:
            try:
                self._audit_merge(order, self._audit_submit_fields(order), overwrite=False)
            except Exception:
                pass

        # Submit regular and Bracket/OTO orders now.
        # OCO orders have no parent orders, so do not submit this "main" order. The children of an OCO will be
        # submitted below. Bracket/OTO orders will be submitted here, but their child orders will not be submitted
        # until the parent order is filled
        if order.order_class is not Order.OrderClass.OCO:
            order.update_raw(order)
            self.stream.dispatch(
                self.NEW_ORDER,
                wait_until_complete=True,
                order=order,
            )

        # Only an OCO order submits the child orders immediately. Bracket/OTO child orders are not submitted until
        # the parent order is filled
        else:
            # Keep the OCO parent as a placeholder order so it can still be looked up by ID.
            self.stream.dispatch(
                self.PLACEHOLDER_ORDER,
                wait_until_complete=True,
                order=order,
            )
            for child in order.child_orders:
                # Ensure OCO child orders get the same submission-time audit context (and do not
                # rely solely on the parent placeholder).
                if audit_enabled:
                    try:
                        self._audit_merge(child, self._audit_submit_fields(child), overwrite=False)
                    except Exception:
                        pass

                child.parent_identifier = order.identifier
                child.update_raw(child)
                self.stream.dispatch(
                    self.NEW_ORDER,
                    wait_until_complete=True,
                    order=child,
                )

        return order

    def _submit_orders(self, orders, is_multileg=False, **kwargs):
        """Submit multiple orders for an asset"""

        # Check that orders is a list and not zero
        if not orders or not isinstance(orders, list) or len(orders) == 0:
            # Log an error and return an empty list
            logger.error("No orders to submit to broker when calling submit_orders")
            return []

        results = []
        for order in orders:
            results.append(self.submit_order(order))

        if is_multileg:
            # Each leg uses a different option asset, just use the base symbol.
            symbol = orders[0].asset.symbol
            # Multileg parents are scheduling containers (they do not represent a real tradable).
            #
            # Using a concrete asset_type like STOCK here causes subtle issues for index option
            # combos (e.g. SPX): the parent would be a "stock" symbol with no OHLC/quotes, which
            # can trigger SMART_LIMIT timeouts/cancellations and block strategies that wait for
            # orders to fully complete. Use the dedicated MULTILEG asset_type instead.
            parent_asset = Asset(symbol=symbol, asset_type=Asset.AssetType.MULTILEG)
            parent_order_type = kwargs.get("order_type", orders[0].order_type)
            parent_smart_limit = None

            if str(parent_order_type) == str(Order.OrderType.SMART_LIMIT):
                # Package SMART_LIMIT: treat the multileg parent as the smart order and fill the
                # child legs atomically when the parent becomes executable.
                parent_order_type = Order.OrderType.SMART_LIMIT
                parent_smart_limit = (
                    kwargs.get("smart_limit")
                    or getattr(orders[0], "smart_limit", None)
                    or SmartLimitConfig()
                )
                for child in orders:
                    setattr(child, "_smart_limit_managed_by_parent", True)

            # Tradier multileg orders pass broker-specific order types like "credit"/"debit"/"even"
            # which are not executable order types in LumiBot. The multileg parent is a placeholder,
            # so normalize to a standard type to avoid raising.
            if parent_smart_limit is None:
                if isinstance(parent_order_type, str) and parent_order_type.lower() in {"credit", "debit", "even"}:
                    parent_order_type = Order.OrderType.LIMIT
                else:
                    try:
                        parent_order_type = (
                            parent_order_type
                            if isinstance(parent_order_type, Order.OrderType) or parent_order_type is None
                            else Order.OrderType(parent_order_type)
                        )
                    except ValueError:
                        parent_order_type = Order.OrderType.MARKET
            parent_order = Order(
                asset=parent_asset,
                strategy=orders[0].strategy,
                order_class=Order.OrderClass.MULTILEG,
                side=orders[0].side,
                quantity=orders[0].quantity,
                order_type=parent_order_type,
                tag=orders[0].tag,
                status=Order.OrderStatus.SUBMITTED
            )
            if parent_smart_limit is not None:
                parent_order.smart_limit = parent_smart_limit

            for o in orders:
                o.parent_identifier = parent_order.identifier

            parent_order.child_orders = orders
            self._unprocessed_orders.append(parent_order)
            # Backtesting must be deterministic: process the NEW_ORDER event inline so the parent
            # multileg order is tracked consistently before the simulation clock advances.
            self.stream.dispatch(self.NEW_ORDER, wait_until_complete=True, order=parent_order)
            return [parent_order]

        return results

    def cancel_order(self, order):
        """Cancel an order"""
        self.stream.dispatch(
            self.CANCELED_ORDER,
            wait_until_complete=True,
            order=order,
        )
        # Cancel all child orders as well
        for child in order.child_orders or []:
            if child is None:
                continue
            # Never overwrite already-closed child orders (e.g., OCO winners).
            if not child.is_active():
                continue
            self.cancel_order(child)

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """Modify an order. Only limit/stop price is allowed to be modified by most brokers."""
        price = None
        if order.order_type == order.OrderType.LIMIT:
            price = limit_price
        elif order.order_type == order.OrderType.STOP:
            price = stop_price

        self.stream.dispatch(
            self.MODIFIED_ORDER,
            order=order,
            price=price,
            wait_until_complete=True,
        )

    def cash_settle_options_contract(self, position, strategy):
        """Cash settle an options contract position. This method will calculate the
        profit/loss of the position and add it to the cash position of the strategy. This
        method will not actually sell the contract, it will just add the profit/loss to the
        cash position and set the position to 0. Note: only for backtesting"""

        # Check to make sure we are in backtesting mode
        if not self.IS_BACKTESTING_BROKER:
            logger.error("Cannot cash settle options contract in live trading")
            return

        # Check that the position is an options contract
        if position.asset.asset_type != "option":
            logger.error(f"Cannot cash settle non-option contract {position.asset}")
            return

        def _infer_underlying_asset_from_strategy(symbol: str) -> Asset | None:
            """Best-effort: reuse the strategy's own underlying Asset object when available.

            Some Strategy Library demos construct option Assets without `underlying_asset=...`.
            In that case we should NOT guess based on symbol strings; instead, look for an
            explicit non-option Asset already attached to the strategy (e.g. `self.vars.underlying_asset`).
            """
            try:
                symbol_norm = (symbol or "").upper()
            except Exception:
                symbol_norm = symbol

            vars_obj = getattr(strategy, "vars", None)
            if vars_obj is None:
                return None

            candidates: list[Asset] = []
            try:
                if hasattr(vars_obj, "all"):
                    values = list(vars_obj.all().values())
                else:
                    values = list(getattr(vars_obj, "_vars_dict", {}).values())
            except Exception:
                values = []

            def _maybe_add(value: object) -> None:
                if not isinstance(value, Asset):
                    return
                if getattr(value, "symbol", None) != symbol_norm:
                    return
                if getattr(value, "asset_type", None) == Asset.AssetType.OPTION:
                    return
                candidates.append(value)

            for value in values:
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        _maybe_add(item)
                else:
                    _maybe_add(value)

            if not candidates:
                return None
            if len(candidates) == 1:
                return candidates[0]

            for candidate in candidates:
                if getattr(candidate, "asset_type", None) == Asset.AssetType.INDEX:
                    return candidate
            return candidates[0]

        # First check if the option asset has an underlying asset, otherwise try to reuse the
        # strategy's explicit underlying Asset (keeps asset_type deterministic for indices).
        underlying_asset = position.asset.underlying_asset
        if underlying_asset is None:
            underlying_asset = _infer_underlying_asset_from_strategy(getattr(position.asset, "symbol", None))
        if underlying_asset is None:
            underlying_asset = Asset(symbol=position.asset.symbol, asset_type="stock")

        # Get the price of the underlying asset.
        underlying_price = None
        last_price_error = None

        def _try_last_price(asset: Asset) -> None:
            nonlocal underlying_price, last_price_error, underlying_asset
            try:
                underlying_price = self.get_last_price(asset)
                underlying_asset = asset
                last_price_error = None
            except Exception as exc:
                underlying_price = None
                last_price_error = exc

        _try_last_price(underlying_asset)

        # Index options can arrive without an explicit underlying_asset. In that case we initially
        # try the underlying as a stock (historical behavior), but SPX/NDX/VIX-style index symbols
        # are not valid stocks in ThetaData and can produce placeholder-only minute series. When that
        # happens (or when the price is None), retry as an index before failing.
        if underlying_price is None and getattr(underlying_asset, "asset_type", None) == Asset.AssetType.STOCK:
            symbol_upper = str(getattr(underlying_asset, "symbol", "") or "").upper()
            index_root_aliases = {
                "SPXW": "SPX",
                "RUTW": "RUT",
                "VIXW": "VIX",
                "NDXP": "NDX",
            }
            index_root = index_root_aliases.get(symbol_upper, symbol_upper)
            index_like_symbols = {
                "SPX", "SPXW",
                "NDX", "NDXP",
                "VIX", "VIXW",
                "RUT", "RUTW",
                "XSP", "DJX", "OEX", "XEO",
            }
            if symbol_upper in index_like_symbols:
                _try_last_price(Asset(symbol=index_root, asset_type="index"))

        if underlying_price is None and last_price_error is not None:
            # Common production failure mode: ThetaData returns placeholder-only minute bars for an
            # index while daily close remains available. Use day-close as the settlement proxy.
            message = str(last_price_error)
            if "[THETA][COVERAGE]" in message:
                try:
                    bars = strategy.get_historical_prices(underlying_asset, length=1, timestep="day")
                    df = getattr(bars, "df", None)
                    if df is not None and not df.empty and "close" in df.columns:
                        underlying_price = float(df["close"].iloc[-1])
                        logger.warning(
                            "[CASH_SETTLE][FALLBACK] get_last_price(%s) failed (%s); settling using daily close=%s",
                            underlying_asset,
                            message.splitlines()[0],
                            underlying_price,
                        )
                except Exception:
                    pass

        if underlying_price is None:
            if last_price_error is not None:
                raise last_price_error
            raise ValueError(f"Unable to price underlying {underlying_asset} for cash settlement of {position.asset}")

        # Calculate profit/loss per contract
        if position.asset.right == "CALL":
            profit_loss_per_contract = underlying_price - position.asset.strike
        else:
            profit_loss_per_contract = position.asset.strike - underlying_price

        # Calculate profit/loss for the position
        profit_loss = profit_loss_per_contract * position.quantity * position.asset.multiplier

        # Adjust profit/loss based on the option type and position
        if position.quantity > 0 and profit_loss < 0:
            profit_loss = 0  # Long position can't lose more than the premium paid
        elif position.quantity < 0 and profit_loss > 0:
            profit_loss = 0  # Short position can't gain more than the cash collected

        # Add the profit/loss to the cash position
        current_cash = strategy.get_cash()
        if current_cash is None:
            # self.strategy.logger.warning("strategy.get_cash() returned None during cash_settle_options_contract. Defaulting to 0.")
            current_cash = Decimal(0)
        else:
            current_cash = Decimal(str(current_cash)) # Ensure it's Decimal

        new_cash = current_cash + Decimal(str(profit_loss))

        # Update the cash position
        strategy._set_cash_position(float(new_cash)) # _set_cash_position expects float

        # Set the side
        if position.quantity > 0:
            side = "sell"
        else:
            side = "buy"

        # Create offsetting order
        order = strategy.create_order(position.asset, abs(position.quantity), side)

        # Send filled order event
        self.stream.dispatch(
            self.CASH_SETTLED,
            wait_until_complete=True,
            order=order,
            price=abs(profit_loss / position.quantity / position.asset.multiplier),
            filled_quantity=abs(position.quantity),
        )

    def process_expired_option_contracts(self, strategy):
        """Checks if options or futures contracts have expried and converts
        to cash.

        Parameters
        ----------
        strategy : Strategy object.
            Strategy object.

        Returns
        --------
            List of orders
        """
        if self.data_source.SOURCE != "PANDAS":
            return

        # If it's the same day as the expiration, we need to check the time to see if it's after market close
        time_to_close = self.get_time_to_close()

        # If the time to close is None, then the market is not open so we should not sell the contracts
        if time_to_close is None:
            return

        # Calculate the number of seconds before market close
        seconds_before_closing = strategy.minutes_before_closing * 60

        positions = self.get_tracked_positions(strategy.name)
        for position in positions:
            if position.asset.expiration is not None and position.asset.expiration <= self.datetime.date():
                # If the contract has expired, we should sell it
                if position.asset.expiration == self.datetime.date() and time_to_close > seconds_before_closing:
                    continue

                logger.info(f"Automatically selling expired contract for asset {position.asset}")

                # If there are still active orders working this asset (e.g., a market order that never
                # filled due to missing bid/ask/trades data), a live broker would not leave them
                # active after expiration. Cancel them and proceed to settlement so positions cannot
                # get "stuck" indefinitely in long daily-cadence backtests.
                self._cancel_open_orders_for_asset(strategy.name, position.asset, set())

                # Cash settle the options contract
                self.cash_settle_options_contract(position, strategy)

    def _apply_trade_cost(self, strategy, trade_cost: Decimal) -> None:
        if not trade_cost:
            return

        current_cash = strategy.cash
        strategy._set_cash_position(current_cash - float(trade_cost))

    def _execute_filled_order(
        self,
        order: Order,
        price: float,
        filled_quantity: Decimal,
        strategy,
    ) -> None:
        parent_identifier = getattr(order, "parent_identifier", None)

        if order.dependent_order:
            order.dependent_order.dependent_order_filled = True
            strategy.broker.cancel_order(order.dependent_order)

        if order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
            for child_order in order.child_orders:
                logger.info(
                    f"{child_order} was sent to broker {self.name} now that the parent Bracket/OTO order has been filled"
                )
                self._new_orders.append(child_order)

        is_multileg_parent = order.is_parent() and order.order_class is Order.OrderClass.MULTILEG

        trade_cost = Decimal("0") if is_multileg_parent else self.calculate_trade_cost(order, strategy, price)
        order.trade_cost = float(trade_cost)

        # Handle cash updates based on asset types
        asset_type = getattr(order.asset, "asset_type", None)
        quote_asset_type = getattr(order.quote, "asset_type", None) if hasattr(order, "quote") and order.quote else None

        # For futures, use margin-based cash management (not full notional value)
        # Futures don't tie up full contract value - only margin requirement
        if (
            not is_multileg_parent
            and asset_type in (Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE)
        ):
            self._process_futures_fill(strategy, order, float(price), float(filled_quantity))

        # For crypto base with forex quote (like BTC/USD where USD is forex), use cash
        # For crypto base with crypto quote (like BTC/USDT where both are crypto), use positions
        elif (
            not is_multileg_parent
            and asset_type == Asset.AssetType.CRYPTO
            and quote_asset_type == Asset.AssetType.FOREX
        ):
            trade_amount = float(filled_quantity) * price
            if hasattr(order.asset, 'multiplier') and order.asset.multiplier:
                trade_amount *= order.asset.multiplier

            current_cash = strategy.cash

            if order.is_buy_order():
                # Deduct cash for buy orders (trade amount + fees)
                new_cash = current_cash - trade_amount - float(trade_cost)
            else:
                # Add cash for sell orders (trade amount - fees)
                new_cash = current_cash + trade_amount - float(trade_cost)

            strategy._set_cash_position(new_cash)

        multiplier = 1
        if hasattr(order, "asset") and getattr(order.asset, "multiplier", None):
            multiplier = order.asset.multiplier

        # PERF: BacktestingBroker commonly uses `Decimal` quantities. Convert once at the dispatch
        # boundary so the trade-event hot path doesn't re-cast on every fill event.
        filled_quantity_f = filled_quantity
        if filled_quantity_f is not None and not isinstance(filled_quantity_f, float):
            filled_quantity_f = float(filled_quantity_f)

        self.stream.dispatch(
            self.FILLED_ORDER,
            wait_until_complete=True,
            order=order,
            price=price,
            filled_quantity=filled_quantity_f,
            quantity=filled_quantity_f,
            multiplier=multiplier,
        )

        # Only apply trade cost if it's not crypto with forex quote (already handled above)
        if (
            not is_multileg_parent
            and not (asset_type == Asset.AssetType.CRYPTO and quote_asset_type == Asset.AssetType.FOREX)
        ):
            self._apply_trade_cost(strategy, trade_cost)

        # If this was an OCO child fill, mark the OCO parent filled after the child
        # fill is recorded so legacy ordering expectations remain stable.
        if parent_identifier:
            parent_order = self.get_tracked_order(parent_identifier, use_placeholders=True)
            if parent_order is not None and parent_order.order_class is Order.OrderClass.OCO and not parent_order.is_filled():
                try:
                    self._placeholder_orders.remove(parent_order.identifier, key="identifier")
                except Exception:
                    pass
                parent_order.status = self.FILLED_ORDER
                parent_order.set_filled()
                if parent_order not in self._filled_orders:
                    self._filled_orders.append(parent_order)

    def _process_crypto_quote(self, order, quantity, price):
        """Override to skip quote processing for assets that use direct cash updates or margin-based trading."""
        # Check asset types
        asset_type = getattr(order.asset, "asset_type", None)
        quote_asset_type = getattr(order.quote, "asset_type", None) if hasattr(order, "quote") and order.quote else None

        # Skip position-based quote processing for:
        # 1. Crypto+forex trades (handled with direct cash updates)
        # 2. Futures contracts (use margin, only realize P&L on close, not full notional)
        if asset_type == Asset.AssetType.CRYPTO and quote_asset_type == Asset.AssetType.FOREX:
            return

        if asset_type in (Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE):
            return

        # For other asset types (crypto+crypto, stocks, etc.), use the original position-based processing
        super()._process_crypto_quote(order, quantity, price)

    def _get_futures_ledger_key(self, strategy, asset):
        strategy_name = getattr(strategy, "_name", None) or getattr(strategy, "name", "unknown_strategy")
        asset_symbol = getattr(asset, "symbol", "unknown_asset")
        asset_type = getattr(asset, "asset_type", "unknown_type")
        # Futures contract identity must include expiration.
        #
        # Strategies commonly construct futures as Asset(symbol="CL", expiration=YYYYMM or date),
        # which means the root symbol alone is not unique. Calendar spreads (front/next month)
        # would otherwise collide in the ledger and incorrectly net margin + realized PnL.
        asset_expiration = getattr(asset, "expiration", None)
        return (strategy_name, asset_symbol, asset_type, asset_expiration)

    def _realize_futures_pnl(self, ledger, closing_qty, exit_price, multiplier):
        remaining = closing_qty
        realized_pnl = 0.0

        while remaining > 1e-9 and ledger:
            lot = ledger[0]
            lot_qty = abs(lot["qty"])
            take_qty = min(lot_qty, remaining)
            entry_price = lot["price"]

            if lot["qty"] > 0:
                realized_pnl += (exit_price - entry_price) * take_qty * multiplier
                lot["qty"] -= take_qty
            else:
                realized_pnl += (entry_price - exit_price) * take_qty * multiplier
                lot["qty"] += take_qty  # Negative qty for shorts

            if abs(lot["qty"]) < 1e-9:
                ledger.pop(0)

            remaining -= take_qty

        if remaining > 1e-6:
            logger.warning("Attempted to close more futures contracts than currently open. Remaining qty: %.6f", remaining)

        return realized_pnl

    def _process_futures_fill(self, strategy, order, price, filled_quantity):
        multiplier = getattr(order.asset, "multiplier", 1)
        margin_per_contract = get_futures_margin_requirement(order.asset)

        key = self._get_futures_ledger_key(strategy, order.asset)
        ledger = self._futures_lot_ledgers[key]

        net_before = sum(lot["qty"] for lot in ledger)
        signed_fill_qty = filled_quantity if order.is_buy_order() else -filled_quantity

        closing_qty = 0.0
        if net_before and signed_fill_qty and net_before * signed_fill_qty < 0:
            closing_qty = min(abs(net_before), filled_quantity)

        opening_qty = filled_quantity - closing_qty

        current_cash = strategy.cash or 0.0
        new_cash = current_cash

        if closing_qty > 0:
            realized_pnl = self._realize_futures_pnl(ledger, closing_qty, price, multiplier)
            new_cash += margin_per_contract * closing_qty
            new_cash += realized_pnl

        if opening_qty > 0:
            opening_sign = 1 if signed_fill_qty > 0 else -1
            ledger.append({
                "qty": opening_sign * opening_qty,
                "price": price,
            })
            new_cash -= margin_per_contract * opening_qty

        if not ledger:
            self._futures_lot_ledgers.pop(key, None)

        strategy._set_cash_position(new_cash)

    def calculate_trade_cost(self, order: Order, strategy, price: float):
        """Calculate the trade cost of an order for a given strategy"""
        # PERF: Trade fees are frequently empty in backtests/benchmarks. Avoid per-fill string
        # normalization and Decimal math when there are no configured fees.
        buy_fees = getattr(strategy, "buy_trading_fees", None) or []
        sell_fees = getattr(strategy, "sell_trading_fees", None) or []
        if not buy_fees and not sell_fees:
            return Decimal("0")

        trade_cost = Decimal("0")
        trading_fees = []
        side_value = str(order.side).lower() if order.side is not None else ""
        order_type_attr = getattr(order, "order_type", None)
        if hasattr(order_type_attr, "value"):
            order_type_value = str(order_type_attr.value).lower()
        else:
            order_type_value = str(order_type_attr).lower() if order_type_attr is not None else ""
        if side_value in ("buy", "buy_to_open", "buy_to_cover"):
            trading_fees = buy_fees
        elif side_value in ("sell", "sell_to_close", "sell_short", "sell_to_open"):
            trading_fees = sell_fees

        for trading_fee in trading_fees:
            if trading_fee.taker is True and order_type_value in {"market", "stop"}:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(str(price)) * Decimal(str(order.quantity)) * trading_fee.percent_fee
            elif trading_fee.maker is True and order_type_value in {"limit", "stop_limit", "smart_limit"}:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(str(price)) * Decimal(str(order.quantity)) * trading_fee.percent_fee

        return trade_cost
        

    def process_pending_orders(self, strategy):
        """Used to evaluate and execute open orders in backtesting.

        This method will evaluate the open orders at the beginning of every new bar to
        determine if any of the open orders should have been filled. This method will
        execute order events as needed, mostly fill events.

        Parameters
        ----------
        strategy : Strategy object

        """

        # This function is called once per bar (minute-level: ~100k/year). Avoid allocating lists
        # or copying buckets when there are no pending orders.
        strategy_name = strategy.name

        unprocessed_bucket = getattr(self, "_unprocessed_orders", None)
        new_bucket = getattr(self, "_new_orders", None)
        if not unprocessed_bucket and not new_bucket:
            return

        pending_orders: list[Order] = []
        if unprocessed_bucket:
            for order in unprocessed_bucket:
                if getattr(order, "strategy", None) == strategy_name:
                    pending_orders.append(order)
        if new_bucket:
            for order in new_bucket:
                if getattr(order, "strategy", None) == strategy_name:
                    pending_orders.append(order)

        if not pending_orders:
            return

        # Prefetching: Track assets and schedule prefetch
        current_dt = self.datetime
        audit_enabled = self._audit_enabled()
        # PERF: Used in multiple branches below; avoid recomputing per order.
        data_source_name = str(getattr(self.data_source, "SOURCE", "") or "").upper()

        if self.hybrid_prefetcher:
            # Use advanced hybrid prefetcher
            try:
                import asyncio

                # Record access patterns for all pending orders
                for order in pending_orders:
                    asset = order.asset if order.asset.asset_type != "crypto" else order.asset
                    timestep = getattr(strategy, 'timestep', 'minute')
                    lookback = getattr(strategy, 'bars_lookback', 100)

                    # Record this access for pattern learning
                    self.hybrid_prefetcher.record_access(asset, current_dt, timestep, lookback)

                # Get predictions and prefetch
                predictions = self.hybrid_prefetcher.get_predictions(current_dt, horizon=30)

                # Execute prefetch asynchronously if possible
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(self.hybrid_prefetcher.prefetch_parallel(predictions))
                    else:
                        loop.run_until_complete(self.hybrid_prefetcher.prefetch_parallel(predictions))
                except:
                    # Fall back to sync if async not available
                    pass

                # Periodic cleanup
                if hasattr(self, '_last_cache_clear'):
                    if (current_dt - self._last_cache_clear).days > 1:
                        self.hybrid_prefetcher.cleanup(max_age_hours=48)
                        self._last_cache_clear = current_dt
                        # Log stats
                        stats = self.hybrid_prefetcher.get_stats()
                        logger.debug(f"Hybrid prefetch stats: {stats}")
                else:
                    self._last_cache_clear = current_dt

            except Exception as e:
                logger.debug(f"Hybrid prefetching error (non-critical): {e}")

        elif self.prefetcher:
            # Use standard aggressive prefetcher
            try:
                # Track all assets from pending orders
                for order in pending_orders:
                    asset = order.asset if order.asset.asset_type != "crypto" else order.asset
                    timestep = getattr(strategy, 'timestep', 'minute')
                    lookback = getattr(strategy, 'bars_lookback', 100)
                    self.prefetcher.track_asset(asset, timestep=timestep, lookback=lookback)

                # Schedule aggressive prefetch for future iterations
                self.prefetcher.schedule_prefetch(current_dt)

                # Clear old cache periodically to prevent memory bloat
                if hasattr(self, '_last_cache_clear'):
                    if (current_dt - self._last_cache_clear).days > 1:
                        self.prefetcher.clear_old_cache(current_dt, max_age_days=3)
                        self._last_cache_clear = current_dt
                else:
                    self._last_cache_clear = current_dt

            except Exception as e:
                logger.debug(f"Standard prefetching error (non-critical): {e}")

        for order in pending_orders:
            if not order.is_active():
                continue
            if order.dependent_order_filled:
                continue
            # No need to check status since we already filtered for pending orders only

            # OCO parent orders do not get filled.
            # PERF: `OrderClass` is a StrEnum; use identity comparisons in backtesting hot loops.
            if order.order_class is Order.OrderClass.OCO:
                continue

            # SMART_LIMIT multileg children should be filled atomically by their parent order.
            if getattr(order, "_smart_limit_managed_by_parent", False):
                continue

            # Multileg parent orders are placeholders unless they are explicitly
            # configured as a package SMART_LIMIT (i.e. have a SmartLimitConfig).
            if order.order_class is Order.OrderClass.MULTILEG and getattr(order, "smart_limit", None) is None:
                # If this is the final fill for a multileg order, mark the parent order as filled
                if all([o.is_filled() for o in order.child_orders]):
                    parent_qty = sum([abs(o.quantity) for o in order.child_orders])
                    child_prices = [o.get_fill_price() if o.is_buy_order() else -o.get_fill_price()
                                    for o in order.child_orders]
                    parent_price = sum(child_prices)
                    parent_multiplier = 1
                    if hasattr(order, "asset") and getattr(order.asset, "multiplier", None):
                        parent_multiplier = order.asset.multiplier

                    if audit_enabled:
                        legs = []
                        for leg in order.child_orders:
                            legs.append(
                                {
                                    "identifier": getattr(leg, "identifier", None),
                                    "symbol": getattr(getattr(leg, "asset", None), "symbol", None),
                                    "asset_type": getattr(getattr(leg, "asset", None), "asset_type", None),
                                    "right": getattr(getattr(leg, "asset", None), "right", None),
                                    "strike": getattr(getattr(leg, "asset", None), "strike", None),
                                    "expiration": getattr(getattr(leg, "asset", None), "expiration", None),
                                    "side": getattr(leg, "side", None),
                                    "quantity": float(getattr(leg, "quantity", 0) or 0),
                                    "fill_price": leg.get_fill_price(),
                                }
                            )
                        self._audit_merge(
                            order,
                            {
                                "fill.model": "multileg_parent_placeholder",
                                "fill.price": float(parent_price),
                                "multileg.legs": legs,
                            },
                            overwrite=True,
                        )

                    self.stream.dispatch(
                        self.FILLED_ORDER,
                        wait_until_complete=True,
                        order=order,
                        price=parent_price,
                        filled_quantity=parent_qty,
                        multiplier=parent_multiplier,
                    )

                continue

            # Check validity if current date > valid date, cancel order. todo valid date
            # TODO: One day... I will purge all this crypto tuple stuff.
            asset = order.asset if order.asset.asset_type != "crypto" else (order.asset, order.quote)

            price = None
            filled_quantity = order.quantity
            timeshift = None
            dt = None
            open = high = low = close = volume = None
            fast_bid = None
            fast_ask = None

            # PERF: MARKET orders dominate many warm-cache backtests (minute strategies, speed-burner).
            # When bid/ask are already present in the cached Data series, we can fill immediately
            # without constructing a `Quote` object or running the full quote normalization stack.
            if (
                not audit_enabled
                and order.order_type == Order.OrderType.MARKET
                and (order.is_buy_order() or order.is_sell_order())
                and not self._is_option_asset(getattr(order, "asset", None))
            ):
                fast_bid, fast_ask = self._fast_get_bid_ask_for_fill(
                    order.asset,
                    order.quote,
                    getattr(order, "exchange", None),
                )
                fast_bid = self._coerce_price(fast_bid)
                fast_ask = self._coerce_price(fast_ask)
                if fast_bid is not None and self._is_invalid_price(fast_bid):
                    fast_bid = None
                if fast_ask is not None and self._is_invalid_price(fast_ask):
                    fast_ask = None

                required_price = fast_ask if order.is_buy_order() else fast_bid
                if required_price is not None and not self._is_invalid_price(required_price):
                    setattr(order, "_price_source", "quote")
                    self._execute_filled_order(
                        order=order,
                        price=required_price,
                        filled_quantity=filled_quantity,
                        strategy=strategy,
                    )
                    continue

            # PERF (IBKR futures/crypto, Trades history): When bid/ask are not available, calling
            # `get_quote()` is pure overhead and eventually falls back to the OHLC model anyway.
            # Detect that scenario and skip the quote path entirely.
            skip_quote_fills = (
                (not audit_enabled)
                and data_source_name in {"INTERACTIVEBROKERSREST"}
                and order.order_type == Order.OrderType.MARKET
                and (order.is_buy_order() or order.is_sell_order())
                and not self._is_option_asset(getattr(order, "asset", None))
                and fast_bid is None
                and fast_ask is None
            )

            # PERFORMANCE: Prefer quote-based fills when bid/ask are present so we avoid
            # fetching trade-only OHLC bars (which can be sparse/missing, especially for
            # options). When bid/ask are unavailable we fall back to the OHLC-based model.
            if (
                order.order_type in (Order.OrderType.MARKET, Order.OrderType.LIMIT)
                and (order.is_buy_order() or order.is_sell_order())
            ):
                quote = None
                if not skip_quote_fills:
                    try:
                        quote = self.get_quote(order.asset, quote=order.quote)
                    except Exception:
                        quote = None

                bid = self._coerce_price(getattr(quote, "bid", None)) if quote is not None else None
                ask = self._coerce_price(getattr(quote, "ask", None)) if quote is not None else None
                if bid is not None and self._is_invalid_price(bid):
                    bid = None
                if ask is not None and self._is_invalid_price(ask):
                    ask = None

                snap = None
                # ThetaData daily-cadence backtests frequently return day-aligned option quotes without
                # NBBO (bid/ask). For execution (especially MARKET exits), prefer a point-in-time
                # quote snapshot so we can fill against realistic bid/ask instead of falling through
                # to sparse trade-only OHLC (which can leave orders unfilled and canceled at EOD).
                if (
                    (bid is None or ask is None)
                    and getattr(order.asset, "asset_type", None) == Asset.AssetType.OPTION
                    and self.data_source is not None
                    and self.data_source.__class__.__name__ == "ThetaDataBacktestingPandas"
                ):
                    try:
                        snap = self.data_source.get_quote(order.asset, quote=order.quote, snapshot_only=True)
                    except TypeError:
                        snap = None
                    except Exception:
                        snap = None
                    if snap is not None:
                        snap_bid = self._coerce_price(getattr(snap, "bid", None))
                        snap_ask = self._coerce_price(getattr(snap, "ask", None))
                        if bid is None and snap_bid is not None and not self._is_invalid_price(snap_bid):
                            bid = snap_bid
                        if ask is None and snap_ask is not None and not self._is_invalid_price(snap_ask):
                            ask = snap_ask

                is_buy = order.is_buy_order()

                if order.order_type == Order.OrderType.MARKET:
                    required_price = ask if is_buy else bid
                    if required_price is not None and not self._is_invalid_price(required_price):
                        if audit_enabled:
                            payload: dict[str, Any] = {
                                "fill.model": "quote_market",
                                "fill.price": float(required_price),
                                "asset_quote.final_bid": bid,
                                "asset_quote.final_ask": ask,
                                "asset_quote.snapshot_only_used": bool(snap is not None),
                            }
                            payload.update(self._audit_quote_fields("asset_quote", quote))
                            if snap is not None:
                                payload.update(self._audit_quote_fields("asset_quote.snapshot", snap))
                            self._audit_merge(order, payload, overwrite=False)
                            self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)
                        setattr(order, "_price_source", "quote")
                        self._execute_filled_order(
                            order=order,
                            price=required_price,
                            filled_quantity=filled_quantity,
                            strategy=strategy,
                        )
                        continue

                elif bid is not None and ask is not None:
                    fill_price: Optional[float] = None
                    limit_price = self._coerce_price(order.limit_price)
                    crossed = False
                    if limit_price is not None:
                        if is_buy:
                            if limit_price >= ask:
                                fill_price = ask
                                crossed = True
                            elif limit_price >= bid:
                                fill_price = limit_price
                        else:
                            if limit_price <= bid:
                                fill_price = bid
                                crossed = True
                            elif limit_price <= ask:
                                fill_price = limit_price

                    if fill_price is not None and not self._is_invalid_price(fill_price):
                        spread_key = "max_spread_buy_pct" if is_buy else "max_spread_sell_pct"
                        spread_limit = self._get_spread_limit(strategy, spread_key)
                        if spread_limit is None:
                            spread_limit = self._get_spread_limit(strategy, "max_spread_pct")
                        spread_pct = None
                        # Only apply spread gating to *inside-spread* limit fills.
                        # Marketable limits (>=ask for buys, <=bid for sells) should fill regardless
                        # of spread width, otherwise strategies can get stuck with uncloseable legs.
                        if spread_limit is not None and not crossed and fill_price == limit_price:
                            mid = (ask + bid) / 2
                            if mid > 0:
                                spread_pct = (ask - bid) / mid
                                if spread_pct > spread_limit:
                                    fill_price = None

                    if fill_price is not None and not self._is_invalid_price(fill_price):
                        if audit_enabled:
                            payload = {
                                "fill.model": "quote_limit",
                                "fill.price": float(fill_price),
                                "fill.crossed": bool(crossed),
                                "order.limit_price": limit_price,
                                "asset_quote.final_bid": bid,
                                "asset_quote.final_ask": ask,
                                "asset_quote.snapshot_only_used": bool(snap is not None),
                                "spread.limit": spread_limit,
                                "spread.pct": spread_pct,
                            }
                            payload.update(self._audit_quote_fields("asset_quote", quote))
                            if snap is not None:
                                payload.update(self._audit_quote_fields("asset_quote.snapshot", snap))
                            self._audit_merge(order, payload, overwrite=False)
                            self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)
                        setattr(order, "_price_source", "quote")
                        self._execute_filled_order(
                            order=order,
                            price=fill_price,
                            filled_quantity=filled_quantity,
                            strategy=strategy,
                        )
                        continue

                    # Quote was available but did not satisfy fill conditions; keep the order open
                    # and retry on the next bar without forcing an OHLC download.
                    if bid is not None and ask is not None:
                        continue

            #############################
            # Get OHLCV data for the asset
            #############################

            # Get the OHLCV data for the asset if we're using the YAHOO, CCXT data source
            if data_source_name in ["CCXT", "YAHOO", "ALPACA", "DATABENTO", "DATABENTO_POLARS"]:
                # Negative deltas here are intentional: _pull_source_symbol_bars subtracts the offset, so
                # passing -1 minute yields an effective +1 minute guard that keeps us on the previously
                # completed bar. See tests/*_lookahead for regression coverage.
                timeshift = timedelta(minutes=-1)
                if data_source_name in {"DATABENTO", "DATABENTO_POLARS"}:
                    # DataBento feeds can skip minutes around maintenance windows. Giving it a two-minute
                    # cushion mirrors the legacy Polygon behaviour and avoids falling through gaps.
                    timeshift = timedelta(minutes=-2)
                elif data_source_name == "YAHOO":
                    # Yahoo daily bars are stamped at the close (16:00). A one-day backstep keeps fills on
                    # the previous session so we never peek at the in-progress bar.
                    timeshift = timedelta(days=-1)
                elif data_source_name == "ALPACA":
                    # Alpaca minute bars line up with our clock already; no offset needed.
                    timeshift = None

                ohlc = self.data_source.get_historical_prices(
                    asset=asset,
                    length=1,
                    quote=order.quote,
                    timeshift=timeshift,
                )

                if (
                    ohlc is None
                    or getattr(ohlc, "df", None) is None
                    or (hasattr(ohlc.df, "empty") and ohlc.df.empty)
                ):
                    if strategy is not None:
                        display_symbol = getattr(order.asset, "symbol", order.asset)
                        order_identifier = getattr(order, "identifier", None)
                        if order_identifier is None:
                            order_identifier = getattr(order, "id", "<unknown>")
                        strategy.log_message(
                            f"[DIAG] No historical bars returned for {display_symbol} at {self.datetime}; "
                            f"pending {order.order_type} id={order_identifier}",
                            color="yellow",
                        )
                    continue

                # Handle both pandas and polars DataFrames
                if hasattr(ohlc.df, 'index'):  # pandas
                    dt = ohlc.df.index[-1]
                    open = ohlc.df['open'].iloc[-1]
                    high = ohlc.df['high'].iloc[-1]
                    low = ohlc.df['low'].iloc[-1]
                    close = ohlc.df['close'].iloc[-1]
                    volume = ohlc.df['volume'].iloc[-1]
                else:  # polars
                    # Find datetime column
                    dt_cols = [col for col in ohlc.df.columns if 'date' in col.lower() or 'time' in col.lower()]
                    if dt_cols:
                        dt = ohlc.df[dt_cols[0]][-1]
                    else:
                        dt = None
                    open = ohlc.df['open'][-1]
                    high = ohlc.df['high'][-1]
                    low = ohlc.df['low'][-1]
                    close = ohlc.df['close'][-1]
                    volume = ohlc.df['volume'][-1]

            # Get the OHLCV data for the asset if we're using a Pandas-backed data source.
            #
            # IMPORTANT: IBKR REST backtesting is implemented as a PandasData subclass but its
            # `SOURCE` is not literally "PANDAS". It must still use the Pandas order-fill path,
            # otherwise `open/high/low/close` remain unset and orders never fill.
            elif self.data_source.SOURCE == "PANDAS" or data_source_name in {"INTERACTIVEBROKERSREST"}:
                # Market orders: prefer quote-based fills when bid/ask are available to avoid
                # expensive OHLC downloads and reflect real-world execution more closely.
                if order.order_type == Order.OrderType.MARKET and not skip_quote_fills:
                    quote_fill_price = self._try_fill_with_quote(order, strategy, None, None, None)
                    if quote_fill_price is not None:
                        self._execute_filled_order(
                            order=order,
                            price=quote_fill_price,
                            filled_quantity=filled_quantity,
                            strategy=strategy,
                        )
                        continue

                # LIMIT orders: if the limit is immediately marketable against the current NBBO,
                # fill from quotes and skip the OHLC fetch. This is especially important for options,
                # where OHLC bars can be sparse/missing and would otherwise trigger a slow downloader call.
                if order.order_type == Order.OrderType.LIMIT:
                    try:
                        quote = self.get_quote(order.asset, quote=order.quote)
                    except Exception:
                        quote = None

                    bid = self._coerce_price(getattr(quote, "bid", None)) if quote is not None else None
                    ask = self._coerce_price(getattr(quote, "ask", None)) if quote is not None else None
                    limit_price = self._coerce_price(getattr(order, "limit_price", None))

                    if limit_price is not None:
                        if order.is_buy_order():
                            if ask is not None and not self._is_invalid_price(ask) and limit_price >= ask:
                                if audit_enabled:
                                    self._audit_merge(
                                        order,
                                        {
                                            "fill.model": "quote_limit_marketable",
                                            "fill.price": float(ask),
                                            "order.limit_price": limit_price,
                                            "asset_quote.final_bid": bid,
                                            "asset_quote.final_ask": ask,
                                        },
                                        overwrite=False,
                                    )
                                    self._audit_merge(order, self._audit_quote_fields("asset_quote", quote), overwrite=False)
                                    self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)
                                setattr(order, "_price_source", "quote")
                                self._execute_filled_order(
                                    order=order,
                                    price=ask,
                                    filled_quantity=filled_quantity,
                                    strategy=strategy,
                                )
                                continue
                        elif order.is_sell_order():
                            if bid is not None and not self._is_invalid_price(bid) and limit_price <= bid:
                                if audit_enabled:
                                    self._audit_merge(
                                        order,
                                        {
                                            "fill.model": "quote_limit_marketable",
                                            "fill.price": float(bid),
                                            "order.limit_price": limit_price,
                                            "asset_quote.final_bid": bid,
                                            "asset_quote.final_ask": ask,
                                        },
                                        overwrite=False,
                                    )
                                    self._audit_merge(order, self._audit_quote_fields("asset_quote", quote), overwrite=False)
                                    self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)
                                setattr(order, "_price_source", "quote")
                                self._execute_filled_order(
                                    order=order,
                                    price=bid,
                                    filled_quantity=filled_quantity,
                                    strategy=strategy,
                                )
                                continue

                # This is a hack to get around the fact that we need to get the previous day's data to prevent lookahead bias.
                # Multileg parent orders are placeholders and often have no backing OHLC stream
                # (especially for ThetaData where multileg assets are unsupported). For package
                # SMART_LIMIT orders, we fill from the child legs' quotes instead of attempting
                # to fetch OHLC for the parent.
                if (
                    order.order_class is Order.OrderClass.MULTILEG
                    and order.child_orders
                    and getattr(order, "smart_limit", None) is not None
                ):
                    ohlc = None
                else:
                    timestep = getattr(self.data_source, "_timestep", "minute")
                    # PandasData's bar slicing is already careful about day-vs-minute lookahead
                    # (see Data._get_bars_dict). For daily bars, a negative `timeshift` can
                    # accidentally *advance* the slice into future sessions.
                    #
                    # For intraday bars, we still need a deterministic bar available for order fills.
                    #
                    # Default (`-1`) includes the current bar while avoiding pulling far-future bars.
                    #
                    # NOTE (IBKR futures): we intentionally avoid the older "force next bar" behavior
                    # here. Correctness is driven by data availability (no synthetic bars) and by
                    # ensuring orders do not fill during gaps (handled below).
                    timeshift = 0 if str(timestep) == "day" else -1
                    ohlc = self.data_source.get_historical_prices(
                        asset=asset,
                        length=2,
                        quote=order.quote,
                        timeshift=timeshift,
                        timestep=timestep,
                    )
                    # ThetaData daily bars are timestamped at the end of the trading session (e.g.
                    # 16:00 NY / 21:00 UTC). At intraday times (or midnight), PandasData slicing
                    # (Data.get_iter_count) returns the *previous* session as "last bar <= now",
                    # which can cause fills to incorrectly use yesterday's open.
                    #
                    # For ORDER FILLS ONLY, we need the current session's bar available so market
                    # orders price off the correct day's open. If the last bar we received is from
                    # an earlier NY date than `self.datetime`, retry once including the next bar.
                    if str(timestep) == "day" and ohlc is not None and not ohlc.empty:
                        try:
                            df_check = getattr(ohlc, "df", None)
                            last_dt = None
                            if df_check is not None and hasattr(df_check, "index"):
                                last_dt = df_check.index.max()
                            elif df_check is not None and hasattr(df_check, "columns"):
                                dt_col = None
                                for col in df_check.columns:
                                    try:
                                        if df_check[col].dtype in [pl.Datetime, pl.Date]:
                                            dt_col = col
                                            break
                                    except Exception:
                                        continue
                                if dt_col is None and "datetime" in df_check.columns:
                                    dt_col = "datetime"
                                if dt_col is not None:
                                    try:
                                        last_dt = df_check[dt_col].max()
                                    except Exception:
                                        last_dt = None

                            def _ny_date(value):
                                ts = pd.Timestamp(value)
                                if ts.tz is None:
                                    ts = ts.tz_localize("America/New_York")
                                else:
                                    ts = ts.tz_convert("America/New_York")
                                return ts.date()

                            if last_dt is not None and _ny_date(last_dt) < _ny_date(self.datetime):
                                ohlc_next = self.data_source.get_historical_prices(
                                    asset=asset,
                                    length=2,
                                    quote=order.quote,
                                    timeshift=-1,
                                    timestep=timestep,
                                )
                                if ohlc_next is not None and not ohlc_next.empty:
                                    ohlc = ohlc_next
                        except Exception:
                            pass
                # Check if we got any ohlc data
                if ohlc is None or ohlc.empty:
                    # SmartLimit should attempt quote-based fills regardless of asset type or data source.
                    if str(order.order_type) == str(Order.OrderType.SMART_LIMIT):
                        smart_price, smart_timed_out = self._smart_limit_backtest_price(
                            order, strategy, None, None, None
                        )
                        if smart_timed_out:
                            self.cancel_order(order)
                            continue

                        # Package SMART_LIMIT orders (multileg parents) must never fill on their own.
                        # When OHLC is missing (common for placeholder multileg assets), use quotes to
                        # fill the child legs atomically and then mark the parent filled for logging.
                        if (
                            order.order_class is Order.OrderClass.MULTILEG
                            and order.child_orders
                            and getattr(order, "smart_limit", None) is not None
                        ):
                            if smart_price is None or self._is_invalid_price(smart_price):
                                # If we can't compute a net quote (and the parent asset has no OHLC),
                                # downgrade to a market-style fill for each leg instead of silently
                                # timing out and canceling the entire combo.
                                filled = self._fill_multileg_children_at_market_open(order, strategy)
                                if filled:
                                    parent_qty = sum(abs(o.quantity) for o in order.child_orders)
                                    child_prices = [
                                        o.get_fill_price() if o.is_buy_order() else -o.get_fill_price()
                                        for o in order.child_orders
                                    ]
                                    parent_price = sum(child_prices)
                                    parent_multiplier = getattr(getattr(order, "asset", None), "multiplier", 1) or 1

                                    if audit_enabled:
                                        legs = []
                                        for leg in order.child_orders:
                                            legs.append(
                                                {
                                                    "identifier": getattr(leg, "identifier", None),
                                                    "symbol": getattr(getattr(leg, "asset", None), "symbol", None),
                                                    "asset_type": getattr(getattr(leg, "asset", None), "asset_type", None),
                                                    "right": getattr(getattr(leg, "asset", None), "right", None),
                                                    "strike": getattr(getattr(leg, "asset", None), "strike", None),
                                                    "expiration": getattr(getattr(leg, "asset", None), "expiration", None),
                                                    "side": getattr(leg, "side", None),
                                                    "quantity": float(getattr(leg, "quantity", 0) or 0),
                                                    "fill_price": leg.get_fill_price(),
                                                }
                                            )
                                        self._audit_merge(
                                            order,
                                            {
                                                "fill.model": "smart_limit_parent",
                                                "fill.price": float(parent_price),
                                                "multileg.legs": legs,
                                                "multileg.note": "OHLC missing; legs filled at market open fallback",
                                            },
                                            overwrite=True,
                                        )

                                    self.stream.dispatch(
                                        self.FILLED_ORDER,
                                        wait_until_complete=True,
                                        order=order,
                                        price=parent_price,
                                        filled_quantity=parent_qty,
                                        multiplier=parent_multiplier,
                                    )
                                continue

                            filled = self._fill_multileg_smart_limit_children(order, strategy, float(smart_price))
                            if not filled:
                                filled = self._fill_multileg_children_at_market_open(order, strategy)

                            if filled:
                                parent_qty = sum(abs(o.quantity) for o in order.child_orders)
                                child_prices = [
                                    o.get_fill_price() if o.is_buy_order() else -o.get_fill_price()
                                    for o in order.child_orders
                                ]
                                parent_price = sum(child_prices)
                                parent_multiplier = getattr(getattr(order, "asset", None), "multiplier", 1) or 1

                                if audit_enabled:
                                    legs = []
                                    for leg in order.child_orders:
                                        legs.append(
                                            {
                                                "identifier": getattr(leg, "identifier", None),
                                                "symbol": getattr(getattr(leg, "asset", None), "symbol", None),
                                                "asset_type": getattr(getattr(leg, "asset", None), "asset_type", None),
                                                "right": getattr(getattr(leg, "asset", None), "right", None),
                                                "strike": getattr(getattr(leg, "asset", None), "strike", None),
                                                "expiration": getattr(getattr(leg, "asset", None), "expiration", None),
                                                "side": getattr(leg, "side", None),
                                                "quantity": float(getattr(leg, "quantity", 0) or 0),
                                                "fill_price": leg.get_fill_price(),
                                            }
                                        )
                                    self._audit_merge(
                                        order,
                                        {
                                            "fill.model": "smart_limit_parent",
                                            "fill.price": float(parent_price),
                                            "multileg.legs": legs,
                                        },
                                        overwrite=True,
                                    )

                                self.stream.dispatch(
                                    self.FILLED_ORDER,
                                    wait_until_complete=True,
                                    order=order,
                                    price=parent_price,
                                    filled_quantity=parent_qty,
                                    multiplier=parent_multiplier,
                                )
                            continue

                        if smart_price is not None and not self._is_invalid_price(smart_price):
                            self._execute_filled_order(
                                order=order,
                                price=smart_price,
                                filled_quantity=filled_quantity,
                                strategy=strategy,
                            )
                        # Keep the order open and retry on the next bar if no fill.
                        continue

                    # OHLC can be sparse/empty even when quotes exist. Attempt a quote-based fill
                    # before canceling so orders can still execute when bid/ask is actionable.
                    quote_fill_price = self._try_fill_with_quote(order, strategy, None, None, None)
                    if quote_fill_price is not None:
                        self._execute_filled_order(
                            order=order,
                            price=quote_fill_price,
                            filled_quantity=filled_quantity,
                            strategy=strategy,
                        )
                        continue

                    if strategy is not None:
                        display_symbol = getattr(order.asset, "symbol", order.asset)
                        order_identifier = getattr(order, "identifier", None)
                        if order_identifier is None:
                            order_identifier = getattr(order, "id", "<unknown>")
                        strategy.log_message(
                            f"[DIAG] No pandas bars for {display_symbol} at {self.datetime}; "
                            f"canceling {order.order_type} id={order_identifier}",
                            color="yellow",
                        )
                    self.cancel_order(order)
                    continue

                df_original = ohlc.df

                # Handle both pandas and polars DataFrames
                if hasattr(df_original, 'select'):  # Polars DataFrame
                    # Find datetime column
                    dt_col = None
                    for col in df_original.columns:
                        if df_original[col].dtype in [pl.Datetime, pl.Date]:
                            dt_col = col
                            break
                    if dt_col is None:
                        dt_col = 'datetime'  # fallback

                    df = df_original.filter(pl.col(dt_col) >= self.datetime)

                    # If the dataframe is empty, get the last row
                    if len(df) == 0:
                        df = df_original.tail(1)

                    # Get values
                    dt = df[dt_col][0]
                    open = df["open"][0]
                    high = df["high"][0]
                    low = df["low"][0]
                    close = df["close"][0]
                    volume = df["volume"][0]
                else:  # Pandas DataFrame
                    # Avoid same-bar lookahead for IBKR futures: bars are commonly timestamped at
                    # the bar start. When an order is submitted at `self.datetime` (end of bar),
                    # it should execute no earlier than the *next* bar. However, orders that were
                    # already working before `self.datetime` must still be evaluated on the
                    # current bar, even when the next bar is a large session gap (daily
                    # maintenance break / weekend reopen).
                    should_use_next_bar = (
                        data_source_name in {"INTERACTIVEBROKERSREST"}
                        and str(getattr(order.asset, "asset_type", "") or "").lower() in {"future", "cont_future"}
                        and str(getattr(self.data_source, "_timestep", "minute")) != "day"
                    )
                    if should_use_next_bar:
                        # IMPORTANT (no synthetic bars / no fills in gaps):
                        # IBKR historical bars can include real gaps (maintenance windows,
                        # holiday early closes, weekend). If the backtest clock lands on a
                        # timestamp with no bar, no fill is possible.
                        #
                        # We intentionally avoid the legacy "jump to next bar while keeping the
                        # older timestamp" behavior. Orders remain working and become eligible to
                        # fill once the clock reaches a bar timestamp.
                        #
                        # See: docs/BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md
                        now_ts = pd.Timestamp(self.datetime)
                        try:
                            if getattr(df_original.index, "tz", None) is not None:
                                if now_ts.tz is None:
                                    now_ts = now_ts.tz_localize(df_original.index.tz)
                                else:
                                    now_ts = now_ts.tz_convert(df_original.index.tz)
                        except Exception:
                            pass

                        if now_ts not in df_original.index:
                            continue
                        df = df_original[df_original.index >= now_ts]
                    else:
                        df = df_original[df_original.index >= self.datetime]

                    # If the dataframe is empty, then we should get the last row of the original dataframe
                    # because it is the best data we have
                    if len(df) == 0:
                        df = df_original.iloc[-1:]

                    dt = df.index[0]
                    open = df["open"].iloc[0]
                    high = df["high"].iloc[0]
                    low = df["low"].iloc[0]
                    close = df["close"].iloc[0]
                    volume = df["volume"].iloc[0]

            #############################
            # Determine transaction price.
            #############################
            simple_side = "buy" if order.is_buy_order() else "sell"
            if order.order_type == Order.OrderType.MARKET:
                price = open

            elif order.order_type == Order.OrderType.LIMIT:
                price = self.limit_order(order.limit_price, simple_side, open, high, low)

            elif order.order_type == Order.OrderType.STOP:
                price = self.stop_order(order.stop_price, simple_side, open, high, low)

            elif order.order_type == Order.OrderType.STOP_LIMIT:
                if not order.price_triggered:
                    price = self.stop_order(order.stop_price, simple_side, open, high, low)
                    if price is not None:
                        price = self.limit_order(order.limit_price, simple_side, price, high, low)
                        order.price_triggered = True
                elif order.price_triggered:
                    price = self.limit_order(order.limit_price, simple_side, open, high, low)

            elif str(order.order_type) == str(Order.OrderType.SMART_LIMIT):
                price, cancel_order = self._smart_limit_backtest_price(order, strategy, open, high, low)
                if cancel_order:
                    self.cancel_order(order)
                    continue

                # Package SMART_LIMIT orders (multileg parent) are *scheduling containers*.
                #
                # They must never fill on their own because that produces misleading trade logs
                # (parent filled, legs not filled) and prevents the strategy from seeing the
                # actual leg fills in `on_filled_order()`.
                if (
                    order.order_class is Order.OrderClass.MULTILEG
                    and order.child_orders
                    and getattr(order, "smart_limit", None) is not None
                ):
                    if price is None or self._is_invalid_price(price):
                        # Not executable yet; keep waiting.
                        continue

                    fill_method = "smart_limit_quotes"
                    filled = self._fill_multileg_smart_limit_children(order, strategy, float(price))
                    if not filled:
                        # Quotes missing (or invalid). SMART_LIMIT should downgrade to a market-style
                        # fill. For multileg orders that means filling each leg from its own OHLC open.
                        fill_method = "market_open_fallback"
                        filled = self._fill_multileg_children_at_market_open(order, strategy)

                    if filled:
                        parent_qty = sum(abs(o.quantity) for o in order.child_orders)
                        child_prices = [
                            o.get_fill_price() if o.is_buy_order() else -o.get_fill_price()
                            for o in order.child_orders
                        ]
                        parent_price = sum(child_prices)
                        parent_multiplier = getattr(getattr(order, "asset", None), "multiplier", 1) or 1

                        if audit_enabled:
                            legs = []
                            for leg in order.child_orders:
                                legs.append(
                                    {
                                        "identifier": getattr(leg, "identifier", None),
                                        "symbol": getattr(getattr(leg, "asset", None), "symbol", None),
                                        "asset_type": getattr(getattr(leg, "asset", None), "asset_type", None),
                                        "right": getattr(getattr(leg, "asset", None), "right", None),
                                        "strike": getattr(getattr(leg, "asset", None), "strike", None),
                                        "expiration": getattr(getattr(leg, "asset", None), "expiration", None),
                                        "side": getattr(leg, "side", None),
                                        "quantity": float(getattr(leg, "quantity", 0) or 0),
                                        "fill_price": leg.get_fill_price(),
                                    }
                                )
                            self._audit_merge(
                                order,
                                {
                                    "fill.model": "smart_limit_parent",
                                    "fill.price": float(parent_price),
                                    "multileg.fill_method": fill_method,
                                    "multileg.legs": legs,
                                },
                                overwrite=True,
                            )

                        self.stream.dispatch(
                            self.FILLED_ORDER,
                            wait_until_complete=True,
                            order=order,
                            price=parent_price,
                            filled_quantity=parent_qty,
                            multiplier=parent_multiplier,
                        )

                    # Always continue: either legs were filled (and we dispatched a parent fill for logging),
                    # or we could not fill and need to retry later. Never fill the parent directly.
                    continue

            elif order.order_type == Order.OrderType.TRAIL:
                current_trail_stop_price = order.get_current_trail_stop_price()
                if current_trail_stop_price:
                    # Check if we have hit the trail stop price for both sell/buy orders
                    price = self.stop_order(current_trail_stop_price, simple_side, open, high, low)

                # Update the stop price if the price has moved
                if order.is_sell_order():
                    if high is not None and not self._is_invalid_price(high):
                        order.update_trail_stop_price(high)
                elif order.is_buy_order():
                    if low is not None and not self._is_invalid_price(low):
                        order.update_trail_stop_price(low)

            else:
                raise ValueError(f"Order type {order.order_type} is not implemented for backtesting.")

            #############################
            # Fill the order.
            #############################

            if (price is None or self._is_invalid_price(price)) and self._should_attempt_quote_fallback(order, open, high, low):
                price = self._try_fill_with_quote(order, strategy, open, high, low)

            # If the price is set, then the order has been filled
            if price is not None:
                if audit_enabled:
                    bar_dt = None
                    try:
                        bar_dt = dt.isoformat()  # type: ignore[union-attr]
                    except Exception:
                        bar_dt = dt

                    self._audit_merge(
                        order,
                        {
                            "fill.model": "ohlc",
                            "fill.order_type": str(getattr(order, "order_type", None)),
                            "fill.price": float(price),
                            "bar.datetime": bar_dt,
                            "bar.open": self._coerce_price(open),
                            "bar.high": self._coerce_price(high),
                            "bar.low": self._coerce_price(low),
                            "bar.close": self._coerce_price(close),
                            "bar.volume": self._coerce_price(volume),
                            "order.limit_price": self._coerce_price(getattr(order, "limit_price", None)),
                            "order.stop_price": self._coerce_price(getattr(order, "stop_price", None)),
                            "data_source": getattr(getattr(self, "data_source", None), "SOURCE", None),
                            "timestep": getattr(getattr(self, "data_source", None), "_timestep", None),
                        },
                        overwrite=False,
                    )
                    self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)

                self._execute_filled_order(
                    order=order,
                    price=price,
                    filled_quantity=filled_quantity,
                    strategy=strategy,
                )
            else:
                if strategy is not None:
                    display_symbol = getattr(order.asset, "symbol", order.asset)
                    order_identifier = getattr(order, "identifier", None)
                    if order_identifier is None:
                        order_identifier = getattr(order, "id", "<unknown>")
                    detail = (
                        f"limit={order.limit_price}, high={high}, low={low}"
                        if order.order_type == Order.OrderType.LIMIT
                        else f"type={order.order_type}, high={high}, low={low}, stop={getattr(order, 'stop_price', None)}"
                    )
                    strategy.log_message(
                        f"[DIAG] Order remained open for {display_symbol} ({detail}) "
                        f"id={order_identifier} at {self.datetime}",
                        color="yellow",
                    )
                continue

        # Expired contracts settlement is only meaningful at (or after) the end of a session.
        #
        # Calling `process_expired_option_contracts()` on every bar is extremely expensive in long
        # intraday backtests (it scans positions + active orders) and does not change behavior
        # because the function intentionally skips settlement until after the close.
        #
        # - Intraday backtests: settlement is handled once per day by `StrategyExecutor._strategy_sleep()`
        #   when it advances the clock to the close.
        # - Daily backtests: `process_pending_orders()` runs once per day, so it is safe to settle here.
        timestep = getattr(getattr(self, "data_source", None), "_timestep", None)
        if timestep == "day":
            self.process_expired_option_contracts(strategy)

    def _coerce_price(self, value):
        """Convert numeric inputs to float when possible for safe comparisons."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return value

    def _is_invalid_price(self, value):
        """Determine whether a price is unusable (None, NaN, or non-positive)."""
        if value is None:
            return True

        # PERF: Fast paths for common numeric primitives. This function is called extremely
        # frequently during order fill evaluation; avoid `pd.isna()` and `float()` conversions
        # in the hot path.
        if isinstance(value, float):
            return math.isnan(value) or value <= 0
        if isinstance(value, int):
            return value <= 0
        if isinstance(value, np.floating):
            return bool(np.isnan(value)) or value <= 0
        if isinstance(value, np.integer):
            return int(value) <= 0

        if isinstance(value, Decimal):
            try:
                numeric_value = float(value)
            except (ValueError, TypeError):
                return True
            return math.isnan(numeric_value) or numeric_value <= 0

        try:
            if pd.isna(value):
                return True
        except Exception:
            pass

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return False
        return numeric_value <= 0

    def _bar_has_missing_prices(self, *values) -> bool:
        return any(self._is_invalid_price(val) for val in values)

    def _fast_get_bid_ask_for_fill(
        self,
        asset: Asset,
        quote: Optional[Asset],
        exchange: Optional[str] = None,
    ) -> tuple[Optional[float], Optional[float]]:
        """Fast-path for bid/ask retrieval in backtesting order fills.

        Why this exists:
        - `process_pending_orders()` can execute tens/hundreds of thousands of MARKET orders in a
          minute-cadence backtest.
        - The default `Broker.get_quote()` path constructs a `Quote` object and performs several
          layers of validation/normalization. That is correct, but expensive.
        - For warm-cache speed, order fills should be able to read bid/ask directly from the
          already-loaded `Data` object when available.

        Safety:
        - This is best-effort. On any failure, it returns (None, None) and callers must fall back
          to the canonical `get_quote()` path to preserve broker-specific semantics.
        - Currently scoped to IBKR REST backtesting only (explicit perf target).
        """
        data_source = getattr(self, "data_source", None)
        if data_source is None or data_source.__class__.__name__ != "InteractiveBrokersRESTBacktesting":
            return None, None

        now = getattr(self, "datetime", None)
        if now is None:
            return None, None

        # Match IBKR's `get_quote()` special-case for crypto at midnight: use the daily series.
        timestep = getattr(data_source, "_timestep", None) or "minute"
        if (
            str(getattr(asset, "asset_type", "") or "").lower() == "crypto"
            and now.hour == 0
            and now.minute == 0
            and now.second == 0
            and now.microsecond == 0
        ):
            timestep = "day"

        cache = getattr(self, "_fast_quote_data_cache", None)
        if cache is None:
            cache = {}
            setattr(self, "_fast_quote_data_cache", cache)

        # PERF: `Asset.__hash__`/`Asset.__eq__` are hot in high-churn backtests. Use `id()`-based
        # keys for this internal cache to avoid repeated rich-comparison and hashing work.
        effective_exchange = exchange if exchange is not None else getattr(data_source, "exchange", None)
        try:
            exchange_key = data_source._normalize_exchange_key(effective_exchange)  # type: ignore[attr-defined]
        except Exception:
            exchange_key = (str(effective_exchange or "").strip().upper() or "AUTO")

        cache_key = (id(asset), id(quote), str(timestep), exchange_key)
        if cache_key in cache:
            data_obj, bid_line, ask_line = cache[cache_key]
        else:
            data_obj = None
            bid_line = None
            ask_line = None

            quote_asset = quote if quote is not None else Asset("USD", asset_type=Asset.AssetType.FOREX)
            store = getattr(data_source, "_data_store", None)
            if isinstance(store, dict):
                try:
                    canonical_key, legacy_key = data_source._build_dataset_keys(  # type: ignore[attr-defined]
                        asset,
                        quote_asset,
                        str(timestep),
                        effective_exchange,
                    )
                except Exception:
                    canonical_key = (asset, quote_asset, str(timestep), exchange_key)
                    legacy_key = (asset, quote_asset, exchange_key)

                data_obj = store.get(canonical_key)
                if data_obj is None and str(timestep) == "minute":
                    data_obj = store.get(legacy_key)

            if data_obj is not None:
                try:
                    bid_line = getattr(data_obj, "datalines", {}).get("bid")
                    ask_line = getattr(data_obj, "datalines", {}).get("ask")
                except Exception:
                    bid_line = None
                    ask_line = None

            cache[cache_key] = (data_obj, bid_line, ask_line)

        if data_obj is None or bid_line is None or ask_line is None:
            return None, None

        try:
            i = data_obj.get_iter_count(now)
            bid = bid_line.dataline[i]
            ask = ask_line.dataline[i]
        except Exception:
            return None, None

        # Mirror `Data.get_quote()` rounding (bid/ask are rounded to 2 decimals).
        try:
            bid = round(bid, 2) if bid is not None else None
        except TypeError:
            pass
        try:
            ask = round(ask, 2) if ask is not None else None
        except TypeError:
            pass

        return bid, ask

    def _is_option_asset(self, asset) -> bool:
        if asset is None:
            return False
        return str(getattr(asset, "asset_type", "")).lower() == "option"

    def _should_attempt_quote_fallback(self, order, open_, high_, low_) -> bool:
        """Determine whether to attempt quote-based fills.

        Quotes can provide actionable fills even when OHLC bars are missing or trade prints
        are sparse. We rely on bid/ask availability rather than asset type or data source.
        """
        if self._bar_has_missing_prices(open_, high_, low_):
            return True

        timestep = getattr(self.data_source, "_timestep", None)
        return timestep == "day"

    def _is_thetadata_source(self) -> bool:
        if ThetaDataBacktestingPandas is None:
            return False
        return isinstance(self.data_source, ThetaDataBacktestingPandas)

    def _get_spread_limit(self, strategy, key: str) -> Optional[float]:
        if strategy is None or not key:
            return None
        if hasattr(strategy, key):
            try:
                return float(getattr(strategy, key))
            except (TypeError, ValueError):
                return None
        params = getattr(strategy, "parameters", None)
        if isinstance(params, dict) and key in params:
            try:
                return float(params[key])
            except (TypeError, ValueError):
                return None
        return None

    def _get_strategy_slippage_amount(self, strategy, order: Order) -> float:
        if strategy is None or order is None:
            return 0.0

        slippages = []
        if order.is_buy_order():
            slippages = getattr(strategy, "buy_trading_slippages", [])
        else:
            slippages = getattr(strategy, "sell_trading_slippages", [])

        total = 0.0
        for slippage in slippages or []:
            if hasattr(slippage, "amount"):
                total += float(slippage.amount)
            else:
                try:
                    total += float(slippage)
                except (TypeError, ValueError):
                    continue
        return total

    def _audit_enabled(self) -> bool:
        # Prefer a cached value (set in __init__) to avoid env lookups in hot loops.
        enabled = getattr(self, "_backtest_audit_enabled", None)
        if enabled is None:
            return self._truthy_env(os.environ.get("LUMIBOT_BACKTEST_AUDIT"))
        return bool(enabled)

    def _audit_merge(self, order: Order, payload: dict[str, Any], *, overwrite: bool = False) -> None:
        """Best-effort merge of audit fields onto an Order.

        Invariant: audit collection must never break backtests. This function must be safe to call
        in hot paths, and must swallow/ignore unexpected value types rather than raising.
        """
        if not self._audit_enabled() or not payload:
            return

        audit = getattr(order, "_audit", None)
        if not isinstance(audit, dict):
            audit = {}
            setattr(order, "_audit", audit)

        # Stamp a schema version once so downstream analysis can evolve safely.
        audit.setdefault("schema_version", 1)

        for key, value in payload.items():
            if key is None:
                continue
            key_str = str(key)
            if not overwrite and key_str in audit:
                continue

            try:
                if isinstance(value, datetime):
                    audit[key_str] = value.isoformat()
                elif isinstance(value, (dict, list, tuple)):
                    audit[key_str] = json.dumps(value, default=str, sort_keys=True)
                else:
                    audit[key_str] = value
            except Exception:
                audit[key_str] = str(value)

    def _audit_quote_fields(self, prefix: str, quote: object) -> dict[str, Any]:
        if quote is None:
            return {}

        def iso(value: Any) -> Any:
            try:
                return value.isoformat()  # type: ignore[no-any-return]
            except Exception:
                return value

        out: dict[str, Any] = {}
        for attr in (
            "price",
            "bid",
            "ask",
            "mid_price",
            "bid_size",
            "ask_size",
            "volume",
            "change",
            "percent_change",
        ):
            value = getattr(quote, attr, None)
            if value is None:
                continue
            coerced = self._coerce_price(value)
            if isinstance(coerced, float) and self._is_invalid_price(coerced):
                continue
            out[f"{prefix}.{attr}"] = coerced

        for attr in ("timestamp", "quote_time", "bid_time", "ask_time"):
            value = getattr(quote, attr, None)
            if value is None:
                continue
            out[f"{prefix}.{attr}"] = iso(value)

        raw = getattr(quote, "raw_data", None)
        if isinstance(raw, dict) and raw:
            try:
                raw_json = json.dumps(raw, default=str, sort_keys=True)
                out[f"{prefix}.raw_json"] = raw_json if len(raw_json) <= 2000 else f"<omitted len={len(raw_json)}>"
            except Exception:
                out[f"{prefix}.raw_json"] = "<unserializable>"

        return out

    def _audit_underlying_quote_fields(self, order: Order) -> dict[str, Any]:
        asset = getattr(order, "asset", None)
        if asset is None or str(getattr(asset, "asset_type", "")).lower() != "option":
            return {}

        underlying = getattr(asset, "underlying_asset", None)
        if underlying is None:
            return {"underlying.symbol": getattr(asset, "symbol", None), "underlying.missing": True}

        try:
            quote = self.get_quote(underlying, quote=getattr(order, "quote", None))
        except Exception as exc:
            return {"underlying.symbol": getattr(underlying, "symbol", None), "underlying.quote_error": str(exc)}

        fields = {"underlying.symbol": getattr(underlying, "symbol", None), "underlying.asset_type": getattr(underlying, "asset_type", None)}
        fields.update(self._audit_quote_fields("underlying_quote", quote))
        return fields

    def _audit_submit_fields(self, order: Order) -> dict[str, Any]:
        """Collect best-effort *order submission time* audit fields.

        WHY: Fill-time telemetry alone can hide issues where the strategy chose to submit an order
        when quotes were stale/wide/invalid. For accuracy investigations we record both submission-
        time and fill-time context so a human can validate every decision.
        """
        fields: dict[str, Any] = {}

        try:
            fields["submit.time"] = self.data_source.get_datetime()
        except Exception:
            pass

        try:
            fields["submit.order_class"] = str(getattr(order, "order_class", None))
            fields["submit.order_type"] = str(getattr(order, "order_type", None))
            fields["submit.side"] = str(getattr(order, "side", None))
            fields["submit.time_in_force"] = str(getattr(order, "time_in_force", None))
            fields["submit.quantity"] = str(getattr(order, "quantity", None))
            fields["submit.limit_price"] = self._coerce_price(getattr(order, "limit_price", None))
            fields["submit.stop_price"] = self._coerce_price(getattr(order, "stop_price", None))
        except Exception:
            pass

        asset = getattr(order, "asset", None)
        if asset is not None:
            try:
                fields["submit.asset.symbol"] = getattr(asset, "symbol", None)
                fields["submit.asset.asset_type"] = getattr(asset, "asset_type", None)
                fields["submit.asset.right"] = getattr(asset, "right", None)
                fields["submit.asset.strike"] = getattr(asset, "strike", None)
                fields["submit.asset.expiration"] = getattr(asset, "expiration", None)
                fields["submit.asset.multiplier"] = getattr(asset, "multiplier", None)
            except Exception:
                pass

        # Asset quote at submission time.
        try:
            quote = self.get_quote(asset, quote=getattr(order, "quote", None)) if asset is not None else None
            fields.update(self._audit_quote_fields("submit.asset_quote", quote))
            snap = getattr(quote, "snapshot", None)
            if snap is not None:
                fields.update(self._audit_quote_fields("submit.asset_quote.snapshot", snap))

            # For ThetaData backtests, the most useful execution-time NBBO is often only available
            # via the data source's `snapshot_only` fast-path. The generic Broker.get_quote() call
            # can return trade-derived prices without bid/ask, which isn't sufficient for audits.
            #
            # Best-effort: if the underlying data source supports `snapshot_only=True`, capture it.
            source = getattr(self, "data_source", None)
            if source is not None and asset is not None and hasattr(source, "get_quote"):
                try:
                    snap_quote = source.get_quote(asset, quote=getattr(order, "quote", None), exchange=None, snapshot_only=True)
                except TypeError:
                    snap_quote = None
                if snap_quote is not None:
                    fields.update(self._audit_quote_fields("submit.asset_quote.snapshot_only", snap_quote))
        except Exception as exc:
            fields["submit.asset_quote_error"] = str(exc)

        # Underlying quote at submission time (options only), kept distinct from fill-time fields.
        try:
            if asset is not None and str(getattr(asset, "asset_type", "")).lower() == "option":
                underlying = getattr(asset, "underlying_asset", None)
                fields["submit.underlying.symbol"] = getattr(underlying, "symbol", None) if underlying else None
                if underlying is not None:
                    uquote = self.get_quote(underlying, quote=getattr(order, "quote", None))
                    fields.update(self._audit_quote_fields("submit.underlying_quote", uquote))
                    usnap = getattr(uquote, "snapshot", None)
                    if usnap is not None:
                        fields.update(self._audit_quote_fields("submit.underlying_quote.snapshot", usnap))

                    source = getattr(self, "data_source", None)
                    if source is not None and hasattr(source, "get_quote"):
                        try:
                            usnap_quote = source.get_quote(underlying, quote=getattr(order, "quote", None), exchange=None, snapshot_only=True)
                        except TypeError:
                            usnap_quote = None
                        if usnap_quote is not None:
                            fields.update(self._audit_quote_fields("submit.underlying_quote.snapshot_only", usnap_quote))
        except Exception as exc:
            fields["submit.underlying_quote_error"] = str(exc)

        return fields

    def _try_fill_with_quote(self, order, strategy, open_=None, high_=None, low_=None) -> Optional[float]:
        """Attempt to fill an order using quotes when OHLC bars are missing."""
        timestep = getattr(self.data_source, "_timestep", None)
        if not (self._bar_has_missing_prices(open_, high_, low_) or timestep == "day"):
            return None
        if order.order_type not in (
            Order.OrderType.LIMIT,
            Order.OrderType.STOP,
            Order.OrderType.STOP_LIMIT,
            Order.OrderType.TRAIL,
            Order.OrderType.MARKET,
        ):
            return None
        if not (order.is_buy_order() or order.is_sell_order()):
            return None

        try:
            quote_kwargs = {}
            # ThetaData option NBBO is stored as intraday snapshot data. In daily-cadence backtests,
            # requesting full-day minute quotes per option can explode runtime. Use snapshot mode
            # (minimal window around `self.datetime`) so market/limit orders can still fill on
            # actionable quotes without downloading an entire session.
            if self._is_thetadata_source() and self._is_option_asset(order.asset) and getattr(self.data_source, "_timestep", None) == "day":
                # NOTE: `Broker.get_quote()` does not accept extra kwargs; call the data source
                # directly so we can pass `snapshot_only` and other backtesting-specific controls.
                quote_kwargs["snapshot_only"] = True
                quote = self.data_source.get_quote(order.asset, quote=order.quote, exchange=None, **quote_kwargs)
            else:
                # Default: preserve legacy broker behavior (and acceptance baselines) by using the
                # broker-level `get_quote()` path without backtesting-only kwargs.
                quote = self.get_quote(order.asset, quote=order.quote)
        except Exception as exc:  # pragma: no cover - defensive log for unexpected broker states
            self.logger.debug("Quote lookup failed for %s: %s", getattr(order.asset, "symbol", order.asset), exc)
            return None

        if quote is None:
            return None

        bid = self._coerce_price(getattr(quote, "bid", None))
        ask = self._coerce_price(getattr(quote, "ask", None))

        is_buy = order.is_buy_order()

        fill_price: Optional[float] = None
        crossed = False

        if order.order_type == Order.OrderType.MARKET:
            fill_price = ask if is_buy else bid
            crossed = True

        elif order.order_type == Order.OrderType.LIMIT:
            limit_price = self._coerce_price(order.limit_price)
            if is_buy:
                if ask is not None and limit_price is not None and limit_price >= ask:
                    fill_price = ask
                    crossed = True
                elif bid is not None and limit_price is not None and limit_price >= bid:
                    fill_price = limit_price
            else:
                if bid is not None and limit_price is not None and limit_price <= bid:
                    fill_price = bid
                    crossed = True
                elif ask is not None and limit_price is not None and limit_price <= ask:
                    fill_price = limit_price

        elif order.order_type == Order.OrderType.STOP:
            stop_price = self._coerce_price(getattr(order, "stop_price", None))
            if stop_price is None or self._is_invalid_price(stop_price):
                return None
            # Quote-based stop trigger:
            # - Buy stop triggers when the market trades/quotes at or above the stop. Use ask as the
            #   executable reference.
            # - Sell stop triggers when the market trades/quotes at or below the stop. Use bid as the
            #   executable reference.
            if is_buy:
                if ask is not None and not self._is_invalid_price(ask) and ask >= stop_price:
                    fill_price = ask
                    crossed = True
            else:
                if bid is not None and not self._is_invalid_price(bid) and bid <= stop_price:
                    fill_price = bid
                    crossed = True

        elif order.order_type == Order.OrderType.STOP_LIMIT:
            stop_price = self._coerce_price(getattr(order, "stop_price", None))
            limit_price = self._coerce_price(getattr(order, "limit_price", None))
            if stop_price is None or limit_price is None:
                return None
            # Stop prices must be strictly positive. Limit prices are allowed to be <=0 in
            # backtests as a shorthand for "marketable" (common pattern: sell limit @ 0).
            if self._is_invalid_price(stop_price):
                return None
            try:
                if isinstance(limit_price, float) and math.isnan(limit_price):
                    return None
                if pd.isna(limit_price):
                    return None
            except Exception:
                pass

            if not getattr(order, "price_triggered", False):
                triggered = False
                if is_buy:
                    if ask is not None and not self._is_invalid_price(ask) and ask >= stop_price:
                        triggered = True
                else:
                    if bid is not None and not self._is_invalid_price(bid) and bid <= stop_price:
                        triggered = True
                if not triggered:
                    return None
                order.price_triggered = True

            # Once triggered, behave like a normal limit order against the quote.
            if is_buy:
                if ask is not None and limit_price >= ask:
                    fill_price = ask
                    crossed = True
                elif bid is not None and limit_price >= bid:
                    fill_price = limit_price
            else:
                if bid is not None and limit_price <= bid:
                    fill_price = bid
                    crossed = True
                elif ask is not None and limit_price <= ask:
                    fill_price = limit_price

        elif order.order_type == Order.OrderType.TRAIL:
            # Quote-based trailing stop support for feeds that do not provide OHLC bars.
            #
            # WHY: IBKR crypto/futures history sources can be effectively quote-like (open/high/low
            # missing). Trailing stops still need to advance their stop level and be able to trigger
            # against bid/ask without relying on OHLC highs/lows.
            update_price = None
            if is_buy:
                update_price = bid
            else:
                update_price = ask
            if update_price is None or self._is_invalid_price(update_price):
                if bid is not None and ask is not None and not (self._is_invalid_price(bid) or self._is_invalid_price(ask)):
                    update_price = (bid + ask) / 2

            if update_price is None or self._is_invalid_price(update_price):
                return None

            try:
                order.update_trail_stop_price(float(update_price))
            except Exception:
                return None

            trail_stop = self._coerce_price(getattr(order, "_trail_stop_price", None))
            if trail_stop is None or self._is_invalid_price(trail_stop):
                return None

            if is_buy:
                if ask is not None and not self._is_invalid_price(ask) and ask >= trail_stop:
                    fill_price = ask
                    crossed = True
            else:
                if bid is not None and not self._is_invalid_price(bid) and bid <= trail_stop:
                    fill_price = bid
                    crossed = True

        if fill_price is None or self._is_invalid_price(fill_price):
            return None

        spread_key = "max_spread_buy_pct" if is_buy else "max_spread_sell_pct"
        spread_limit = self._get_spread_limit(strategy, spread_key)
        if spread_limit is None:
            spread_limit = self._get_spread_limit(strategy, "max_spread_pct")
        if spread_limit is not None and bid is not None and ask is not None and order.order_type != Order.OrderType.MARKET:
            # Only gate inside-spread fills. Marketable limits should fill regardless.
            limit_price = self._coerce_price(order.limit_price)
            if crossed or (limit_price is None) or (fill_price != limit_price):
                spread_limit = None

        if spread_limit is not None and bid is not None and ask is not None:
            mid = (ask + bid) / 2
            if mid > 0:
                spread_pct = (ask - bid) / mid
                if spread_pct > spread_limit:
                    if strategy is not None:
                        strategy.log_message(
                            f"Skipped quote fill for {order.identifier} "
                            f"(spread {spread_pct:.2%} exceeds {spread_limit:.2%}).",
                            color="yellow",
                        )
                    return None

        if strategy is not None:
            strategy.log_message(
                f"Filled {order.identifier} via quote @ {fill_price:.2f}.",
                color="yellow",
            )

        spread_pct = None
        if bid is not None and ask is not None:
            mid = (ask + bid) / 2
            if mid:
                try:
                    spread_pct = (ask - bid) / mid
                except Exception:
                    spread_pct = None

        self._audit_merge(
            order,
            {
                "fill.model": "quote_fallback",
                "fill.order_type": str(getattr(order, "order_type", None)),
                "fill.price": float(fill_price),
                "fill.crossed": bool(crossed),
                "order.limit_price": self._coerce_price(getattr(order, "limit_price", None)),
                "order.stop_price": self._coerce_price(getattr(order, "stop_price", None)),
                "spread.limit": spread_limit,
                "spread.pct": spread_pct,
            },
            overwrite=False,
        )
        self._audit_merge(order, self._audit_quote_fields("asset_quote", quote), overwrite=False)
        self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)

        setattr(order, "_price_source", "quote")
        return fill_price

    def _smart_limit_backtest_price(self, order, strategy, open_, high_, low_) -> tuple[Optional[float], bool]:
        smart_limit = getattr(order, "smart_limit", None)
        if smart_limit is None:
            return None, False
        audit_enabled = self._audit_enabled()
        legs_audit: Optional[list[dict[str, Any]]] = None

        state = getattr(order, "_smart_limit_state", None)
        if state is None:
            created_at = getattr(order, "_date_created", None) or self.datetime
            state = {"created_at": created_at, "missing_quote_warned": False}
            order._smart_limit_state = state

        created_at = state.get("created_at")
        elapsed_seconds = None
        if isinstance(created_at, datetime):
            elapsed_seconds = (self.datetime - created_at).total_seconds()
        elif isinstance(created_at, (int, float)):
            # Primarily used for live SMART_LIMIT orders (monotonic clock). Backtests should
            # typically use timezone-aware datetimes, but handle floats defensively.
            elapsed_seconds = time.monotonic() - float(created_at)

        if elapsed_seconds is not None:
            # Mirror the live SMART_LIMIT timeout model (Option Alpha style):
            # - steps-1 reprices at step_seconds cadence
            # - final step holds for final_hold_seconds, then cancels
            max_total_seconds = smart_limit.get_step_seconds() * (smart_limit.get_step_count() - 1)
            max_total_seconds += smart_limit.get_final_hold_seconds()
            if elapsed_seconds >= max_total_seconds:
                if strategy is not None and not state.get("timed_out_warned", False):
                    strategy.log_message(
                        f"[SMART_LIMIT] Timed out after {elapsed_seconds:.0f}s; canceling order {order.identifier}.",
                        color="yellow",
                    )
                    state["timed_out_warned"] = True
                return None, True

        bid = ask = None
        if order.order_class is Order.OrderClass.MULTILEG and order.child_orders:
            net_bid = 0.0
            net_ask = 0.0
            order_side = "buy" if order.is_buy_order() else "sell"
            if audit_enabled:
                legs_audit = []
            for leg in order.child_orders:
                try:
                    quote = self.get_quote(leg.asset, quote=leg.quote)
                except Exception:
                    quote = None
                leg_bid = self._coerce_price(getattr(quote, "bid", None)) if quote is not None else None
                leg_ask = self._coerce_price(getattr(quote, "ask", None)) if quote is not None else None
                if legs_audit is not None:
                    legs_audit.append(
                        {
                            "symbol": getattr(getattr(leg, "asset", None), "symbol", None),
                            "asset_type": getattr(getattr(leg, "asset", None), "asset_type", None),
                            "right": getattr(getattr(leg, "asset", None), "right", None),
                            "strike": getattr(getattr(leg, "asset", None), "strike", None),
                            "expiration": getattr(getattr(leg, "asset", None), "expiration", None),
                            "side": getattr(leg, "side", None),
                            "quantity": float(getattr(leg, "quantity", 0) or 0),
                            "bid": leg_bid,
                            "ask": leg_ask,
                        }
                    )
                if leg_bid is None or leg_ask is None:
                    net_bid = net_ask = None
                    break
                if order_side == "buy":
                    if leg.is_buy_order():
                        net_bid += leg_bid
                        net_ask += leg_ask
                    else:
                        net_bid -= leg_ask
                        net_ask -= leg_bid
                else:
                    if leg.is_buy_order():
                        net_bid -= leg_ask
                        net_ask -= leg_bid
                    else:
                        net_bid += leg_bid
                        net_ask += leg_ask
            bid = net_bid
            ask = net_ask
            if bid is not None and ask is not None:
                # Avoid float accumulation artifacts (e.g. 0.799999999999) which can break
                # tick-size inference for net multileg prices.
                bid = round(float(bid), 6)
                ask = round(float(ask), 6)
        else:
            try:
                quote = self.get_quote(order.asset, quote=order.quote)
            except Exception:
                quote = None

            bid = self._coerce_price(getattr(quote, "bid", None)) if quote is not None else None
            ask = self._coerce_price(getattr(quote, "ask", None)) if quote is not None else None

        if bid is None or ask is None or self._is_invalid_price(bid) or self._is_invalid_price(ask):
            if not state.get("missing_quote_warned", False):
                if strategy is not None:
                    missing_target = order.asset
                    if order.order_class is Order.OrderClass.MULTILEG and order.child_orders:
                        missing_target = "one or more multileg legs"
                    strategy.log_message(
                        f"[SMART_LIMIT] Missing bid/ask for {missing_target}; downgrading to market.",
                        color="yellow",
                    )
                state["missing_quote_warned"] = True
            order.trade_slippage = 0.0
            if audit_enabled:
                self._audit_merge(
                    order,
                    {
                        "fill.model": "smart_limit_missing_quote",
                        "fill.price": self._coerce_price(open_),
                        "smart_limit.bid": bid,
                        "smart_limit.ask": ask,
                        "smart_limit.legs": legs_audit,
                    },
                    overwrite=False,
                )
            return open_, False

        side = "buy" if order.is_buy_order() else "sell"
        tick = None
        if order.order_class is not Order.OrderClass.MULTILEG:
            try:
                asset_tick = getattr(order.asset, "min_tick", None)
                if asset_tick is not None and float(asset_tick) > 0:
                    tick = float(asset_tick)
            except Exception:
                tick = None
        if tick is None:
            tick = infer_tick_size(bid, ask)
        mid = compute_mid(bid, ask)
        final_price = compute_final_price(bid, ask, side, smart_limit.final_price_pct)
        slippage_amount = smart_limit.get_slippage_amount()
        if smart_limit.slippage is None:
            slippage_amount = self._get_strategy_slippage_amount(strategy, order)

        # Backtesting model: fill at mid +/- slippage (inside the spread) whenever bid/ask are available.
        #
        # We intentionally do not simulate the live cancel/replace timing ladder here because most
        # backtests run on minute bars (no sub-minute quote path). This keeps backtests fast, stable,
        # and aligned with the common "mid fill" assumption used by competitors.
        fill_price = expected_fill_price(mid, slippage_amount, side)
        if side == "buy":
            fill_price = min(fill_price, ask)
        else:
            fill_price = max(fill_price, bid)
        fill_price = round_to_tick(fill_price, tick, side=side)

        if side == "buy" and fill_price > final_price:
            return None, False
        if side == "sell" and fill_price < final_price:
            return None, False

        multiplier = (
            getattr(order.child_orders[0].asset, "multiplier", 1)
            if order.order_class is Order.OrderClass.MULTILEG and order.child_orders
            else getattr(order.asset, "multiplier", 1)
        ) or 1
        order.trade_slippage = abs(fill_price - mid) * float(order.quantity) * multiplier
        setattr(order, "_price_source", "smart_limit")
        if audit_enabled:
            payload: dict[str, Any] = {
                "fill.model": "smart_limit_net" if order.order_class is Order.OrderClass.MULTILEG else "smart_limit",
                "fill.price": float(fill_price),
                "smart_limit.bid": bid,
                "smart_limit.ask": ask,
                "smart_limit.mid": mid,
                "smart_limit.tick": tick,
                "smart_limit.final_price": final_price,
                "smart_limit.final_price_pct": getattr(smart_limit, "final_price_pct", None),
                "smart_limit.slippage_amount": slippage_amount,
            }
            if order.order_class is Order.OrderClass.MULTILEG and order.child_orders:
                payload["smart_limit.legs"] = legs_audit
            self._audit_merge(order, payload, overwrite=False)
            self._audit_merge(order, self._audit_underlying_quote_fields(order), overwrite=False)
        return fill_price, False

    def _fill_multileg_smart_limit_children(self, order: Order, strategy, net_fill_price: float) -> bool:
        """Fill SMART_LIMIT multileg child orders atomically using mid+slippage semantics.

        The parent order is a scheduling container. Once it becomes executable, we fill each
        leg at a price inside its own NBBO such that the net package price matches the parent
        SMART_LIMIT fill model (mid +/- slippage).
        """
        if not order.child_orders:
            return False
        audit_enabled = self._audit_enabled()
        shared_underlying_fields: dict[str, Any] = {}
        if audit_enabled:
            try:
                shared_underlying_fields = self._audit_underlying_quote_fields(order.child_orders[0])
            except Exception:
                shared_underlying_fields = {}

        side = "buy" if order.is_buy_order() else "sell"

        legs = []
        total_slack = 0.0
        net_mid = 0.0

        for leg in order.child_orders:
            try:
                quote = self.get_quote(leg.asset, quote=leg.quote)
            except Exception:
                quote = None
            bid = self._coerce_price(getattr(quote, "bid", None)) if quote is not None else None
            ask = self._coerce_price(getattr(quote, "ask", None)) if quote is not None else None
            if bid is None or ask is None or self._is_invalid_price(bid) or self._is_invalid_price(ask):
                return False

            leg_mid = round(compute_mid(bid, ask), 6)
            slack = max(0.0, (ask - bid) / 2.0)
            total_slack += slack

            sign = 1.0 if (leg.is_buy_order() if side == "buy" else leg.is_sell_order()) else -1.0
            net_mid += sign * leg_mid

            legs.append({"order": leg, "bid": bid, "ask": ask, "mid": leg_mid, "slack": slack})

        # Use configured slippage directly to avoid float cancellation issues when deriving
        # slippage from net_fill_price - net_mid.
        smart_limit = getattr(order, "smart_limit", None)
        slippage_amount = smart_limit.get_slippage_amount() if smart_limit is not None else 0.0
        if smart_limit is not None and smart_limit.slippage is None:
            slippage_amount = self._get_strategy_slippage_amount(strategy, order)

        delta = abs(float(slippage_amount))
        ratio = (delta / total_slack) if total_slack > 0 else 0.0

        for leg_info in legs:
            leg = leg_info["order"]
            leg_mid = leg_info["mid"]
            adj = leg_info["slack"] * ratio

            raw_fill = leg_mid + adj if leg.is_buy_order() else leg_mid - adj
            raw_fill = round(raw_fill, 6)
            tick = None
            try:
                asset_tick = getattr(leg.asset, "min_tick", None)
                if asset_tick is not None and float(asset_tick) > 0:
                    tick = float(asset_tick)
            except Exception:
                tick = None
            if tick is None:
                tick = infer_tick_size(leg_info["bid"], leg_info["ask"])
            fill_price = round_to_tick(raw_fill, tick, side="buy" if leg.is_buy_order() else "sell")

            multiplier = getattr(leg.asset, "multiplier", 1) or 1
            try:
                qty = float(abs(float(leg.quantity)))
            except Exception:
                qty = float(abs(leg.quantity))

            leg.trade_slippage = abs(fill_price - leg_mid) * qty * multiplier
            setattr(leg, "_price_source", "smart_limit")
            if audit_enabled:
                self._audit_merge(
                    leg,
                    {
                        "fill.model": "smart_limit_leg",
                        "fill.price": float(fill_price),
                        "parent.identifier": getattr(order, "identifier", None),
                        "parent.net_fill_price": float(net_fill_price),
                        "asset_quote.final_bid": leg_info.get("bid"),
                        "asset_quote.final_ask": leg_info.get("ask"),
                        "smart_limit.leg_mid": leg_mid,
                        "smart_limit.leg_slack": leg_info.get("slack"),
                        "smart_limit.leg_raw_fill": raw_fill,
                        "smart_limit.tick": tick,
                    },
                    overwrite=False,
                )
                self._audit_merge(leg, shared_underlying_fields, overwrite=False)

            self._execute_filled_order(
                order=leg,
                price=float(fill_price),
                filled_quantity=leg.quantity,
                strategy=strategy,
            )

        return True

    def _fill_multileg_children_at_market_open(self, order: Order, strategy) -> bool:
        """Fallback for multileg SMART_LIMIT when quotes are missing.

        Fill each child order at its own OHLC open (market-style) so the backtest
        doesn't stall or fill only the parent placeholder.
        """
        if not order.child_orders:
            return False
        audit_enabled = self._audit_enabled()
        shared_underlying_fields: dict[str, Any] = {}
        if audit_enabled:
            try:
                shared_underlying_fields = self._audit_underlying_quote_fields(order.child_orders[0])
            except Exception:
                shared_underlying_fields = {}

        for leg in order.child_orders:
            asset = leg.asset if getattr(leg.asset, "asset_type", None) != "crypto" else (leg.asset, leg.quote)
            try:
                timestep = getattr(self.data_source, "_timestep", "minute")
                ohlc = self.data_source.get_historical_prices(
                    asset=asset,
                    length=2,
                    quote=leg.quote,
                    timeshift=0 if str(timestep) == "day" else -1,
                    timestep=timestep,
                )
                if str(timestep) == "day" and ohlc is not None and not ohlc.empty:
                    try:
                        df_check = getattr(ohlc, "df", None)
                        last_dt = df_check.index.max() if df_check is not None and hasattr(df_check, "index") else None

                        def _ny_date(value):
                            ts = pd.Timestamp(value)
                            if ts.tz is None:
                                ts = ts.tz_localize("America/New_York")
                            else:
                                ts = ts.tz_convert("America/New_York")
                            return ts.date()

                        if last_dt is not None and _ny_date(last_dt) < _ny_date(self.datetime):
                            ohlc_next = self.data_source.get_historical_prices(
                                asset=asset,
                                length=2,
                                quote=leg.quote,
                                timeshift=-1,
                                timestep=timestep,
                            )
                            if ohlc_next is not None and not ohlc_next.empty:
                                ohlc = ohlc_next
                    except Exception:
                        pass
            except Exception:
                ohlc = None

            if ohlc is None or getattr(ohlc, "df", None) is None:
                return False

            open_price = None
            df = ohlc.df
            try:
                if hasattr(df, "index"):
                    open_price = df["open"].iloc[-1]
                else:
                    open_price = df["open"][-1]
            except Exception:
                open_price = None

            open_price = self._coerce_price(open_price)
            if open_price is None or self._is_invalid_price(open_price):
                return False

            leg.trade_slippage = 0.0
            setattr(leg, "_price_source", "market")
            if audit_enabled:
                self._audit_merge(
                    leg,
                    {
                        "fill.model": "market_open_fallback_leg",
                        "fill.price": float(open_price),
                        "parent.identifier": getattr(order, "identifier", None),
                        "bar.open": float(open_price),
                        "data_source": getattr(getattr(self, "data_source", None), "SOURCE", None),
                        "timestep": getattr(getattr(self, "data_source", None), "_timestep", None),
                    },
                    overwrite=False,
                )
                self._audit_merge(leg, shared_underlying_fields, overwrite=False)
            self._execute_filled_order(
                order=leg,
                price=float(open_price),
                filled_quantity=leg.quantity,
                strategy=strategy,
            )

        return True

    def limit_order(self, limit_price, side, open_, high, low):
        """Limit order logic."""
        open_val = self._coerce_price(open_)
        high_val = self._coerce_price(high)
        low_val = self._coerce_price(low)
        limit_val = self._coerce_price(limit_price)

        # For limit orders:
        # - bar prices must be strictly positive
        # - limit prices must be present (not None/NaN)
        #
        # BUT: allow `limit_price <= 0` as a shorthand for "marketable sell" (common pattern: sell limit @ 0).
        # This is used in tests and real strategies to express "fill immediately at best available price".
        if any(self._is_invalid_price(val) for val in (open_val, high_val, low_val)):
            return None
        if limit_val is None:
            return None
        try:
            if isinstance(limit_val, float) and math.isnan(limit_val):
                return None
            if pd.isna(limit_val):
                return None
        except Exception:
            pass

        # Gap Up case: Limit wasn't triggered by previous candle but current candle opens higher, fill it now
        if side == "sell" and (limit_val <= 0 or limit_val <= open_val):
            return open_val

        # Gap Down case: Limit wasn't triggered by previous candle but current candle opens lower, fill it now
        if side == "buy" and limit_val >= open_val:
            return open_val

        # Current candle triggered limit normally
        if low_val <= limit_val <= high_val:
            return limit_val

        # Limit has not been met
        return None

    def stop_order(self, stop_price, side, open_, high, low):
        """Stop order logic."""
        open_val = self._coerce_price(open_)
        high_val = self._coerce_price(high)
        low_val = self._coerce_price(low)
        stop_val = self._coerce_price(stop_price)

        if any(self._is_invalid_price(val) for val in (open_val, high_val, low_val, stop_val)):
            return None

        # Gap Down case: Stop wasn't triggered by previous candle but current candle opens lower, fill it now
        if side == "sell" and stop_val >= open_val:
            return open_val

        # Gap Up case: Stop wasn't triggered by previous candle but current candle opens higher, fill it now
        if side == "buy" and stop_val <= open_val:
            return open_val

        # Current candle triggered stop normally
        if low_val <= stop_val <= high_val:
            return stop_val

        # Stop has not been met
        return None

    # =========Market functions=======================
    def get_last_bar(self, asset):
        """Returns OHLCV dictionary for last bar of the asset."""
        return self.data_source.get_historical_prices(asset, 1)

    # ==========Processing streams data=======================

    def _get_stream_object(self):
        """get the broker stream connection"""
        stream = CustomStream()
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        broker = self

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_trade_event(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.NEW_ORDER,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.PLACEHOLDER_ORDER)
        def on_trade_event(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.PLACEHOLDER_ORDER,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event(order, price, filled_quantity, quantity=None, multiplier=1):
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=multiplier,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event(order, **payload):
            try:
                broker._process_trade_event(
                    order,
                    broker.CANCELED_ORDER,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.MODIFIED_ORDER)
        def on_trade_event(order, price):
            try:
                broker._process_trade_event(
                    order,
                    broker.MODIFIED_ORDER,
                    price=price,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CASH_SETTLED)
        def on_trade_event(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order,
                    broker.CASH_SETTLED,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
                return True
            except:
                logger.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        self.stream._run()

    def _pull_positions(self, strategy):
        """Get the account positions. return a list of
        position objects"""
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        return result

    def _pull_position(self, strategy, asset):
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
        response = self._pull_broker_position(asset)
        result = self._parse_broker_position(response, strategy)
        return result
