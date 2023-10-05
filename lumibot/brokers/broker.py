import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import wraps
from queue import Queue
from threading import RLock, Thread

import pandas as pd
import pandas_market_calendars as mcal
from dateutil import tz
from termcolor import colored

from lumibot.data_sources import DataSource
from lumibot.entities import Order, Position
from lumibot.trading_builtins import SafeList


class Broker:

    # Metainfo
    IS_BACKTESTING_BROKER = False

    # Trading events flags
    NEW_ORDER = "new"
    CANCELED_ORDER = "canceled"
    FILLED_ORDER = "fill"
    PARTIALLY_FILLED_ORDER = "partial_fill"

    def __init__(self, name="", connect_stream=True):
        """Broker constructor"""
        # Shared Variables between threads
        self.name = name
        self._lock = RLock()
        self._unprocessed_orders = SafeList(self._lock)
        self._new_orders = SafeList(self._lock)
        self._canceled_orders = SafeList(self._lock)
        self._partially_filled_orders = SafeList(self._lock)
        self._filled_positions = SafeList(self._lock)
        self._subscribers = SafeList(self._lock)
        self._is_stream_subscribed = False
        self._trade_event_log_df = pd.DataFrame()
        self._hold_trade_events = False
        self._held_trades = []

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

    @property
    def _tracked_orders(self):
        return (
            self._unprocessed_orders + self._new_orders + self._partially_filled_orders
        )

    def _start_orders_thread(self):
        self._orders_thread = Thread(
            target=self._wait_for_orders, daemon=True, name=f"{self.name}_orders_thread"
        )
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
                        logging.info(
                            colored(
                                "%r was sent to broker %s" % (flat_order, self.name),
                                color="green",
                            )
                        )
                        self._unprocessed_orders.append(flat_order)

            self._orders_queue.task_done()

    def _submit_order(self, order):
        pass

    def _submit_orders(self, orders):
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

    # =========Internal functions==============

    def _set_initial_positions(self, strategy):
        """ Set initial positions """
        positions = self._pull_positions(strategy)
        for pos in positions:
            self._filled_positions.append(pos)

    def _process_new_order(self, order):
        # Check if this order already exists in self._new_orders based on the identifier
        if order in self._new_orders:
            return

        logging.info(colored(f"New {order} was submitted.", color="green"))
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        order.update_status(self.NEW_ORDER)
        order.set_new()
        self._new_orders.append(order)
        return order

    def _process_canceled_order(self, order):
        logging.info("%r was canceled." % order)
        self._new_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.update_status(self.CANCELED_ORDER)
        order.set_canceled()
        self._canceled_orders.append(order)
        return order

    def _process_partially_filled_order(self, order, price, quantity):
        logging.info(
            "Partial Fill Transaction: %s %d of %s at $%s per share"
            % (order.side, quantity, order.asset, price)
        )
        logging.info("%r was partially filled" % order)
        self._new_orders.remove(order.identifier, key="identifier")

        order.add_transaction(price, quantity)
        order.update_status(self.PARTIALLY_FILLED_ORDER)
        order.set_partially_filled()

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is None:
            # Create new position for this given strategy and asset
            position = order.to_position(quantity)
            self._filled_positions.append(position)
        else:
            # Add the order to the already existing position
            position.add_order(order, quantity)

        if order not in self._partially_filled_orders:
            self._partially_filled_orders.append(order)

        if order.asset.asset_type == "crypto":
            self._process_crypto_quote(order, quantity, price)

        return order, position

    def _process_filled_order(self, order, price, quantity):
        logging.info(
            colored(
                f"Filled Transaction: {order.side} {quantity} of {order.asset.symbol} at {price:,.8f} {'USD'} per share",
                color="green",
            )
        )
        logging.info(f"{order} was filled")
        self._new_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")

        order.add_transaction(price, quantity)
        order.update_status(self.FILLED_ORDER)
        order.set_filled()

        position = self.get_tracked_position(order.strategy, order.asset)
        if position is None:
            # Create new position for this given strategy and asset
            position = order.to_position(quantity)
            self._filled_positions.append(position)
        else:
            # Add the order to the already existing position
            position.add_order(order, quantity)
            if position.quantity == 0:
                logging.info("Position %r liquidated" % position)
                self._filled_positions.remove(position)

        if order.asset.asset_type == "crypto":
            self._process_crypto_quote(order, quantity, price)

        return position

    def _process_crypto_quote(self, order, quantity, price):
        """Used to process the quote side of a crypto trade. """
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
        date = date if date is not None else datetime.now()
        trading_hours = mkt_cal.schedule(
            start_date=date, end_date=date + timedelta(weeks=1)
        ).head(2)

        row = 0 if not next else 1
        th = trading_hours.iloc[row, :]
        market_open, market_close = th.iloc[0], th.iloc[1]

        if close:
            return market_close
        else:
            return market_open

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit timestamp was reached"""
        return True

    def market_close_time(self):
        return self.utc_to_local(self.market_hours(close=True))

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
        open_time = self.utc_to_local(self.market_hours(close=False))
        close_time = self.utc_to_local(self.market_hours(close=True))

        current_time = datetime.now().astimezone(tz=tz.tzlocal())
        if self.market == "24/7":
            return True
        return (current_time >= open_time) and (close_time >= current_time)

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        open_time_this_day = self.utc_to_local(
            self.market_hours(close=False, next=False)
        )
        open_time_next_day = self.utc_to_local(
            self.market_hours(close=False, next=True)
        )
        now = self.utc_to_local(datetime.now())
        open_time = (
            open_time_this_day if open_time_this_day > now else open_time_next_day
        )
        current_time = datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return 0
        else:
            result = open_time.timestamp() - current_time.timestamp()
            return result

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        close_time = self.utc_to_local(self.market_hours(close=True))
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
            logging.info("Sleeping until the market opens")
            self.sleep(sleeptime)

    def _await_market_to_close(self, timedelta=None, strategy=None):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            if timedelta is not None:
                time_to_close -= 60 * timedelta

            sleeptime = max(0, time_to_close)
            logging.info("Sleeping until the market closes")
            self.sleep(sleeptime)

    # =========Positions functions==================

    def _get_balances_at_broker(self, quote_asset):
        """Get the actual cash balance at the broker."""
        pass

    def get_tracked_position(self, strategy, asset):
        """get a tracked position given an asset and
        a strategy"""
        for position in self._filled_positions:
            if position.asset == asset and position.strategy == strategy:
                return position
        return None

    def get_tracked_positions(self, strategy):
        """get all tracked positions for a given strategy"""
        result = [
            position
            for position in self._filled_positions
            if position.strategy == strategy
        ]
        return result

    def get_historical_account_value(self):
        """Get the historical account value of the account."""
        pass

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        pass

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
        pass

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        pass

    def _pull_positions(self, strategy):
        """Get the account positions. return a list of
        position objects"""
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        return result

    def _pull_position(self, strategy, asset):
        """Get the account position for a given asset.
        return a position object"""
        response = self._pull_broker_position(asset)
        result = self._parse_broker_position(response, strategy)
        return result

    # =========Orders and assets functions=================

    def get_tracked_order(self, identifier):
        """get a tracked order given an identifier"""
        for order in self._tracked_orders:
            if order.identifier == identifier:
                return order
        return None

    def get_tracked_orders(self, strategy, asset=None):
        """get all tracked orders for a given strategy"""
        result = []
        for order in self._tracked_orders:
            if order.strategy == strategy and (asset is None or order.asset == asset):
                result.append(order)

        return result

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
        orders = self.get_tracked_orders(strategy, asset)
        for order in orders:
            quantity += order.get_increment()

        if type(quantity) == Decimal:
            if quantity.as_tuple().exponent > -4:
                quantity = float(quantity)  # has less than 5 decimal places, use float

        return quantity

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        pass

    def _parse_broker_orders(self, broker_orders, strategy_name, strategy_object=None):
        """parse a list of broker orders into a
        list of order objects"""
        result = []
        if broker_orders is not None:
            for broker_order in broker_orders:
                result.append(
                    self._parse_broker_order(
                        broker_order, strategy_name, strategy_object=strategy_object
                    )
                )
        else:
            logging.warning(
                "No orders found in broker._parse_broker_orders: the broker_orders object is None"
            )

        return result

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        pass

    def _pull_order(self, identifier, strategy_name):
        """pull and parse a broker order by id"""
        response = self._pull_broker_order(identifier)
        if response:
            order = self._parse_broker_order(response, strategy_name)
            return order
        return None

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        pass

    def _pull_open_orders(self, strategy_name, strategy_object):
        """Get a list of order objects representing the open
        orders"""
        response = self._pull_broker_open_orders()
        result = self._parse_broker_orders(
            response, strategy_name, strategy_object=strategy_object
        )
        return result

    def submit_order(self, order):
        """Submit an order for an asset"""
        self._orders_queue.put(order)

    def submit_orders(self, orders):
        """Submit orders"""
        self._orders_queue.put(orders)

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

    def cancel_order(self, order):
        """Cancel an order"""
        pass

    def cancel_orders(self, orders):
        """cancel orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                tasks.append(executor.submit(self.cancel_order, order))

    def cancel_open_orders(self, strategy):
        """cancel all open orders for a given strategy"""
        orders = self.get_tracked_orders(strategy)
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

    def sell_all(self, strategy_name, cancel_open_orders=True, strategy=None):
        """sell all positions"""
        logging.warning("Strategy %s: sell all" % strategy_name)
        if cancel_open_orders:
            self.cancel_open_orders(strategy_name)

        if not self.IS_BACKTESTING_BROKER:
            orders_result = self.wait_orders_clear(strategy_name)
            if not orders_result:
                logging.info(
                    "From sell_all, orders were still outstanding before the sell all event"
                )

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
        self.submit_orders(orders)

    # =========Market functions=======================

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs):
        """Takes an asset asset and returns the last known price"""
        pass

    def get_last_prices(self, assets, quote=None, exchange=None, **kwargs):
        """Takes a list of assets and returns the last known prices"""
        pass

    def _get_tick(self, order):
        raise NotImplementedError(f"Tick data is not available for {self.name}")

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
        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        subscriber.add_event(subscriber.NEW_ORDER, payload)

    def _on_canceled_order(self, order):
        """notify relevant subscriber/strategy about
        canceled order event"""
        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        subscriber.add_event(subscriber.CANCELED_ORDER, payload)

    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        """notify relevant subscriber/strategy about
        partially filled order event"""
        payload = dict(
            position=position,
            order=order,
            price=price,
            quantity=quantity,
            multiplier=multiplier,
        )
        subscriber = self._get_subscriber(order.strategy)
        subscriber.add_event(subscriber.PARTIALLY_FILLED_ORDER, payload)

    def _on_filled_order(self, position, order, price, quantity, multiplier):
        """notify relevant subscriber/strategy about
        filled order event"""
        payload = dict(
            position=position,
            order=order,
            price=price,
            quantity=quantity,
            multiplier=multiplier,
        )
        subscriber = self._get_subscriber(order.strategy)
        subscriber.add_event(subscriber.FILLED_ORDER, payload)

    # ==========Processing streams data=======================

    def _get_stream_object(self):
        """get the broker stream connection"""
        pass

    def _stream_established(self):
        self._is_stream_subscribed = True

    def process_held_trades(self):
        """Processes any held trade notifications."""
        while len(self._held_trades) > 0:
            th = self._held_trades.pop(0)
            self._process_trade_event(
                th[0],
                th[1],
                price=th[2],
                filled_quantity=th[3],
                multiplier=th[4],
            )

    def _process_trade_event(
        self, stored_order, type_event, price=None, filled_quantity=None, multiplier=1
    ):
        """process an occurred trading event and update the
        corresponding order"""
        if self._hold_trade_events and not self.IS_BACKTESTING_BROKER:
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
        if type_event in [self.FILLED_ORDER, self.PARTIALLY_FILLED_ORDER] and (
            price is None or filled_quantity is None
        ):
            raise ValueError(
                f"""For filled_order and partially_filled_order event,
                price and filled_quantity must be specified.
                Received respectively {price} and {filled_quantity}"""
            )

        if filled_quantity is not None:
            error = ValueError(
                f"filled_quantity must be a positive integer, received {filled_quantity} instead"
            )
            try:
                if not isinstance(filled_quantity, Decimal):
                    filled_quantity = Decimal(filled_quantity)
                if filled_quantity < 0:
                    raise error
            except ValueError:
                raise error

        if price is not None:
            error = ValueError(
                "price must be a positive float, received %r instead" % price
            )
            try:
                price = float(price)
                if price < 0:
                    raise error
            except ValueError:
                raise error

        if type_event == self.NEW_ORDER:
            stored_order = self._process_new_order(stored_order)
            self._on_new_order(stored_order)
        elif type_event == self.CANCELED_ORDER:
            # Do not cancel or re-cancel already completed orders
            if stored_order.is_active():
                stored_order = self._process_canceled_order(stored_order)
                self._on_canceled_order(stored_order)
        elif type_event == self.PARTIALLY_FILLED_ORDER:
            stored_order, position = self._process_partially_filled_order(
                stored_order, price, filled_quantity
            )
            self._on_partially_filled_order(
                position, stored_order, price, filled_quantity, multiplier
            )
        elif type_event == self.FILLED_ORDER:
            position = self._process_filled_order(stored_order, price, filled_quantity)
            self._on_filled_order(
                position, stored_order, price, filled_quantity, multiplier
            )
        else:
            logging.info(f"Unhandled type event {type_event} for {stored_order}")

        if (
            hasattr(self, "_data_source")
            and self._data_source is not None
            and hasattr(self._data_source, "get_datetime")
        ):
            current_dt = self._data_source.get_datetime()
            new_row = {
                "time": current_dt,
                "strategy": stored_order.strategy,
                "exchange": stored_order.exchange,
                "symbol": stored_order.symbol,
                "side": stored_order.side,
                "type": stored_order.type,
                "status": stored_order.status,
                "price": price,
                "filled_quantity": filled_quantity,
                "multiplier": multiplier,
                "trade_cost": stored_order.trade_cost,
                "time_in_force": stored_order.time_in_force,
                "asset.right": stored_order.asset.right,
                "asset.strike": stored_order.asset.strike,
                "asset.multiplier": stored_order.asset.multiplier,
                "asset.expiration": stored_order.asset.expiration,
                "asset.asset_type": stored_order.asset.asset_type,
            }
            # append row to the dataframe
            new_row_df = pd.DataFrame(new_row, index=[0])
            self._trade_event_log_df = pd.concat(
                [self._trade_event_log_df, new_row_df], axis=0
            )

        return

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        pass

    def _run_stream(self):
        pass

    def _launch_stream(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""
        self._register_stream_events()
        t = Thread(
            target=self._run_stream, daemon=True, name=f"broker_{self.name}_thread"
        )
        t.start()
        if not self.IS_BACKTESTING_BROKER:
            logging.info(
                """Waiting for the socket stream connection to be established, 
                method _stream_established must be called"""
            )
            while True:
                if self._is_stream_subscribed is True:
                    break
        return

    def export_trade_events_to_csv(self, filename):
        if len(self._trade_event_log_df) > 0:
            output_df = self._trade_event_log_df.set_index("time")
            output_df.to_csv(filename)

    def _get_greeks(
        self,
        asset,
        implied_volatility=False,
        delta=False,
        option_price=False,
        pv_dividend=False,
        gamma=False,
        vega=False,
        theta=False,
        underlying_price=False,
    ):
        return self.get_greeks(
            asset,
            implied_volatility=implied_volatility,
            delta=delta,
            option_price=option_price,
            pv_dividend=pv_dividend,
            gamma=gamma,
            vega=vega,
            theta=theta,
            underlying_price=underlying_price,
        )
