import math
import traceback
import threading
from collections import OrderedDict, defaultdict
from datetime import timedelta
from decimal import Decimal
from typing import Optional, Union

import pandas as pd
import polars as pl
import pytz

from lumibot.brokers import Broker
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Order, Position, TradingFee
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

    # =========Internal functions==================

    def _update_datetime(self, update_dt, cash=None, portfolio_value=None):
        """Works with either timedelta or datetime input
        and updates the datetime of the broker"""
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

        self.data_source._update_datetime(new_datetime, cash=cash, portfolio_value=portfolio_value)
        if self.option_source:
            self.option_source._update_datetime(new_datetime, cash=cash, portfolio_value=portfolio_value)

    # =========Clock functions=====================

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit datetime was reached"""

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
            logger.critical("Cannot predict future")
            return None

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self.datetime

        search = self._trading_days[now < self._trading_days.index]
        if search.empty:
            logger.info("Cannot predict future")
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
            logger.info("Backtesting reached end of available trading days data")
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
        # If the calculated time is non-positive, but the market was initially open (result > 0),
        # advance by a minimal amount to prevent potential infinite loops if called repeatedly near close.
        elif result > 0:  # Only if original result was strictly positive
            logger.debug("Calculated time to close is non-positive. Advancing time by 1 second.")
            self._update_datetime(1)
        # Otherwise (result <= 0 initially), do nothing, market is already closed.

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
        if order.order_class != Order.OrderClass.OCO:
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

            elif order.order_class == Order.OrderClass.OCO:
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
                if (order.order_class == Order.OrderClass.BRACKET or
                        (order.order_class == Order.OrderClass.OTO and order.secondary_stop_price)):
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

                if (order.order_class == Order.OrderClass.BRACKET or
                        (order.order_class == Order.OrderClass.OTO and order.secondary_limit_price)):
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

                if order.order_class == Order.OrderClass.BRACKET:
                    stop_loss_order.dependent_order = limit_order
                    limit_order.dependent_order = stop_loss_order

        return orders

    def _cancel_open_orders_for_asset(self, strategy_name: str, asset: Asset, exclude_identifiers: set | None = None):
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

        open_orders = self.get_tracked_orders(strategy=strategy_name)

        # Build a set of all child order identifiers to skip them in the main loop
        # (they will be handled by their parent orders)
        child_order_identifiers = set()
        for tracked_order in open_orders:
            if tracked_order.child_orders:
                for child in tracked_order.child_orders:
                    child_order_identifiers.add(child.identifier)

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
            if not tracked_order.is_active():
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

            if order not in self._filled_orders:
                self._filled_orders.append(order)

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
                self._cancel_open_orders_for_asset(order.strategy, order.asset, {order.identifier})
        else:
            self._filled_positions.append(position)  # New position, add it to the tracker

        # If this is a child order, update the parent order status if all children are filled or cancelled.
        if order.parent_identifier:
            parent_order = self.get_tracked_order(order.parent_identifier, use_placeholders=True)
            self._update_parent_order_status(parent_order)
        return position

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

            if order not in self._filled_orders:
                self._filled_orders.append(order)

    def _submit_order(self, order):
        """Submit an order for an asset"""

        # NOTE: This code is to address Tradier API requirements, they want is as "to_open" or "to_close" instead of just "buy" or "sell"
        # If the order has a "buy_to_open" or "buy_to_close" side, then we should change it to "buy"
        if order.is_buy_order():
            order.side = Order.OrderSide.BUY
        # If the order has a "sell_to_open" or "sell_to_close" side, then we should change it to "sell"
        if order.is_sell_order():
            order.side = Order.OrderSide.SELL

        # Submit regular and Bracket/OTO orders now.
        # OCO orders have no parent orders, so do not submit this "main" order. The children of an OCO will be
        # submitted below. Bracket/OTO orders will be submitted here, but their child orders will not be submitted
        # until the parent order is filled
        if order.order_class != Order.OrderClass.OCO:
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
                if child.is_buy_order():
                    child.side = Order.OrderSide.BUY
                elif child.is_sell_order():
                    child.side = Order.OrderSide.SELL

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
            parent_asset = Asset(symbol=symbol)
            parent_order = Order(
                asset=parent_asset,
                strategy=orders[0].strategy,
                order_class=Order.OrderClass.MULTILEG,
                side=orders[0].side,
                quantity=orders[0].quantity,
                order_type=orders[0].order_type,
                tag=orders[0].tag,
                status=Order.OrderStatus.SUBMITTED
            )

            for o in orders:
                o.parent_identifier = parent_order.identifier

            parent_order.child_orders = orders
            self._unprocessed_orders.append(parent_order)
            self.stream.dispatch(self.NEW_ORDER, order=parent_order)
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
        for child in order.child_orders:
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

        # First check if the option asset has an underlying asset
        if position.asset.underlying_asset is None:
            # Create a stock asset for the underlying asset
            underlying_asset = Asset(
                symbol=position.asset.symbol,
                asset_type="stock",
            )
        else:
            underlying_asset = position.asset.underlying_asset

        # Get the price of the underlying asset
        underlying_price = self.get_last_price(underlying_asset)

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

                # Skip if there are still active orders working this asset.
                active_orders = [
                    o for o in self.get_tracked_orders(strategy=strategy.name)
                    if o.asset == position.asset and o.is_active()
                ]
                if active_orders:
                    continue

                logger.info(f"Automatically selling expired contract for asset {position.asset}")

                # Cancel any outstanding orders tied to this asset before forcing settlement.
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
        if order.dependent_order:
            order.dependent_order.dependent_order_filled = True
            strategy.broker.cancel_order(order.dependent_order)

        if order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
            for child_order in order.child_orders:
                logger.info(
                    f"{child_order} was sent to broker {self.name} now that the parent Bracket/OTO order has been filled"
                )
                self._new_orders.append(child_order)

        is_multileg_parent = order.is_parent() and order.order_class == Order.OrderClass.MULTILEG

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

        self.stream.dispatch(
            self.FILLED_ORDER,
            wait_until_complete=True,
            order=order,
            price=price,
            filled_quantity=filled_quantity,
            quantity=filled_quantity,
            multiplier=multiplier,
        )

        # Only apply trade cost if it's not crypto with forex quote (already handled above)
        if (
            not is_multileg_parent
            and not (asset_type == Asset.AssetType.CRYPTO and quote_asset_type == Asset.AssetType.FOREX)
        ):
            self._apply_trade_cost(strategy, trade_cost)

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
        return (strategy_name, asset_symbol, asset_type)

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
        trade_cost = 0
        trading_fees = []
        side_value = str(order.side).lower() if order.side is not None else ""
        order_type_attr = getattr(order, "order_type", None)
        if hasattr(order_type_attr, "value"):
            order_type_value = str(order_type_attr.value).lower()
        else:
            order_type_value = str(order_type_attr).lower() if order_type_attr is not None else ""
        if side_value in ("buy", "buy_to_open", "buy_to_cover"):
            trading_fees = strategy.buy_trading_fees
        elif side_value in ("sell", "sell_to_close", "sell_short", "sell_to_open"):
            trading_fees = strategy.sell_trading_fees

        for trading_fee in trading_fees:
            if trading_fee.taker is True and order_type_value in {"market", "stop"}:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(str(price)) * Decimal(str(order.quantity)) * trading_fee.percent_fee
            elif trading_fee.maker is True and order_type_value in {"limit", "stop_limit"}:
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

        # OPTIMIZATION: Get orders only once per list to minimize lock acquisitions
        # This function is called 179k times
        strategy_name = strategy.name
        pending_orders = []

        # Get unprocessed orders once with single lock acquisition
        if hasattr(self, '_unprocessed_orders'):
            unprocessed = self._unprocessed_orders.get_list()  # One lock acquisition
            # Use list comprehension which is faster than extend with filter
            pending_orders.extend([o for o in unprocessed if o.strategy == strategy_name])

        # Get new orders once with single lock acquisition
        if hasattr(self, '_new_orders'):
            new_orders = self._new_orders.get_list()  # One lock acquisition
            pending_orders.extend([o for o in new_orders if o.strategy == strategy_name])

        if len(pending_orders) == 0:
            return

        # Prefetching: Track assets and schedule prefetch
        current_dt = self.datetime

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

            # OCO parent orders do not get filled
            if order.order_class == Order.OrderClass.OCO:
                continue

            # Multileg parent orders will wait for child orders to fill before processing
            if order.order_class == Order.OrderClass.MULTILEG:
                # If this is the final fill for a multileg order, mark the parent order as filled
                if all([o.is_filled() for o in order.child_orders]):
                    parent_qty = sum([abs(o.quantity) for o in order.child_orders])
                    child_prices = [o.get_fill_price() if o.is_buy_order() else -o.get_fill_price()
                                    for o in order.child_orders]
                    parent_price = sum(child_prices)
                    parent_multiplier = 1
                    if hasattr(order, "asset") and getattr(order.asset, "multiplier", None):
                        parent_multiplier = order.asset.multiplier

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

            #############################
            # Get OHLCV data for the asset
            #############################

            # Get the OHLCV data for the asset if we're using the YAHOO, CCXT data source
            data_source_name = self.data_source.SOURCE.upper()
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

            # Get the OHLCV data for the asset if we're using the PANDAS data source
            elif self.data_source.SOURCE == "PANDAS":
                # This is a hack to get around the fact that we need to get the previous day's data to prevent lookahead bias.
                ohlc = self.data_source.get_historical_prices(
                    asset=asset,
                    length=2,
                    quote=order.quote,
                    timeshift=-2,
                    timestep=self.data_source._timestep,
                )
                # Check if we got any ohlc data
                if ohlc is None or ohlc.empty:
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

                    # Filter for current time or future
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
                    # Make sure that we are only getting the prices for the current time exactly or in the future
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

            elif order.order_type == Order.OrderType.TRAIL:
                current_trail_stop_price = order.get_current_trail_stop_price()
                if current_trail_stop_price:
                    # Check if we have hit the trail stop price for both sell/buy orders
                    price = self.stop_order(current_trail_stop_price, simple_side, open, high, low)

                # Update the stop price if the price has moved
                if order.is_sell_order():
                    order.update_trail_stop_price(high)
                elif order.is_buy_order():
                    order.update_trail_stop_price(low)

            else:
                raise ValueError(f"Order type {order.order_type} is not implemented for backtesting.")

            #############################
            # Fill the order.
            #############################

            if price is None and self._should_attempt_quote_fallback(open, high, low):
                price = self._try_fill_with_quote(order, strategy, open, high, low)

            # If the price is set, then the order has been filled
            if price is not None:
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

        # After handling all pending orders, cash settle any residual expired contracts.
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
        """Determine whether a price is unusable (None or NaN)."""
        if value is None:
            return True
        if isinstance(value, Decimal):
            try:
                value = float(value)
            except (ValueError, TypeError):
                return True
        if isinstance(value, float) and math.isnan(value):
            return True
        try:
            if pd.isna(value):
                return True
        except Exception:
            pass
        return False

    def _bar_has_missing_prices(self, *values) -> bool:
        return any(self._is_invalid_price(val) for val in values)

    def _should_attempt_quote_fallback(self, open_, high_, low_) -> bool:
        return self._is_thetadata_source() and self._bar_has_missing_prices(open_, high_, low_)

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

    def _try_fill_with_quote(self, order, strategy, open_=None, high_=None, low_=None) -> Optional[float]:
        """Attempt to fill an order using ThetaData quotes when OHLC bars are missing."""
        if not self._is_thetadata_source():
            return None
        if not self._bar_has_missing_prices(open_, high_, low_):
            return None
        if order.order_type not in (Order.OrderType.LIMIT, Order.OrderType.STOP_LIMIT):
            return None
        if not (order.is_buy_order() or order.is_sell_order()):
            return None

        try:
            quote = self.get_quote(order.asset, quote=order.quote)
        except Exception as exc:  # pragma: no cover - defensive log for unexpected broker states
            self.logger.debug("ThetaData quote lookup failed for %s: %s", getattr(order.asset, "symbol", order.asset), exc)
            return None

        if quote is None:
            return None

        bid = self._coerce_price(getattr(quote, "bid", None))
        ask = self._coerce_price(getattr(quote, "ask", None))

        is_buy = order.is_buy_order()
        fill_price = ask if is_buy else bid
        if fill_price is None or self._is_invalid_price(fill_price):
            return None

        limit_price = self._coerce_price(order.limit_price)
        if limit_price is not None:
            if is_buy and fill_price > limit_price:
                return None
            if not is_buy and fill_price < limit_price:
                return None

        spread_key = "max_spread_buy_pct" if is_buy else "max_spread_sell_pct"
        spread_limit = self._get_spread_limit(strategy, spread_key)
        if spread_limit is not None and bid is not None and ask is not None:
            mid = (ask + bid) / 2
            if mid > 0:
                spread_pct = (ask - bid) / mid
                if spread_pct > spread_limit:
                    if strategy is not None:
                        strategy.log_message(
                            f"Skipped ThetaData quote fill for {order.identifier} "
                            f"(spread {spread_pct:.2%} exceeds {spread_limit:.2%}).",
                            color="yellow",
                        )
                    return None

        if strategy is not None:
            strategy.log_message(
                f"Filled {order.identifier} via ThetaData quote @ {fill_price:.2f}.",
                color="yellow",
            )

        setattr(order, "_price_source", "quote")
        return fill_price

    def limit_order(self, limit_price, side, open_, high, low):
        """Limit order logic."""
        open_val = self._coerce_price(open_)
        high_val = self._coerce_price(high)
        low_val = self._coerce_price(low)
        limit_val = self._coerce_price(limit_price)

        if any(self._is_invalid_price(val) for val in (open_val, high_val, low_val, limit_val)):
            return None

        # Gap Up case: Limit wasn't triggered by previous candle but current candle opens higher, fill it now
        if side == "sell" and limit_val <= open_val:
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
