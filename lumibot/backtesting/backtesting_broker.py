import logging
import traceback
from datetime import timedelta
from decimal import Decimal
from functools import wraps
from typing import Union

import pytz

from lumibot.brokers import Broker
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Order, Position, TradingFee
from lumibot.trading_builtins import CustomStream

logger = logging.getLogger(__name__)


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

        if not isinstance(self.data_source, DataSourceBacktesting):
            raise ValueError("Must provide a backtesting data_source to run with a BacktestingBroker")

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

        if isinstance(update_dt, timedelta):
            new_datetime = self.datetime + update_dt
        elif isinstance(update_dt, int) or isinstance(update_dt, float):
            new_datetime = self.datetime + timedelta(seconds=update_dt)
        else:
            new_datetime = update_dt

        # This is needed to handle Daylight Savings Time changes
        new_datetime = tz.normalize(new_datetime) if is_pytz else new_datetime

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

        # For ANY market, check both today's and tomorrow's sessions since trading sessions 
        # can span multiple calendar days (futures: 6pm Thu -> 6pm Fri, forex: Sun 5pm -> Fri 5pm, 
        # crypto sessions, international markets, etc.)
        
        # First try to find today's session
        idx_today = self._trading_days.index.searchsorted(now, side='right')
        
        if idx_today < len(self._trading_days):
            market_close_today = self._trading_days.index[idx_today]
            market_open_today = self._trading_days.at[market_close_today, 'market_open']
            
            if market_open_today <= now < market_close_today:
                return True
        
        # If not in today's session, check tomorrow's session (might have started today)
        idx_tomorrow = idx_today + 1 if idx_today < len(self._trading_days) else len(self._trading_days)
        
        if idx_tomorrow < len(self._trading_days):
            market_close_tomorrow = self._trading_days.index[idx_tomorrow]
            market_open_tomorrow = self._trading_days.at[market_close_tomorrow, 'market_open']
            
            if market_open_tomorrow <= now < market_close_tomorrow:
                return True
        
        # Also check if we should look at yesterday's session that might extend to today
        if idx_today > 0:
            idx_yesterday = idx_today - 1
            market_close_yesterday = self._trading_days.index[idx_yesterday]
            market_open_yesterday = self._trading_days.at[market_close_yesterday, 'market_open']
            
            if market_open_yesterday <= now < market_close_yesterday:
                return True
        
        return False

    def _get_next_trading_day(self):
        now = self.datetime
        search = self._trading_days[now < self._trading_days.market_open]
        if search.empty:
            logging.info("Cannot predict future")

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self.datetime

        search = self._trading_days[now < self._trading_days.index]
        if search.empty:
            logging.info("Cannot predict future")
            return 0

        trading_day = search.iloc[0]
        open_time = trading_day.market_open

        # For Backtesting, sometimes the user can just pass in dates (i.e. 2023-08-01) and not datetimes
        # In this case the "now" variable is starting at midnight, so we need to adjust the open_time to be actual
        # market open time.  In the case where the user passes in a time inside a valid trading day, use that time
        # as the start of trading instead of market open.
        if self.IS_BACKTESTING_BROKER and now > open_time:
            open_time = self.data_source.datetime_start

        if now >= open_time:
            return 0

        delta = open_time - now
        return delta.total_seconds()

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        now = self.datetime

        # Use searchsorted for efficient searching and reduce unnecessary DataFrame access
        idx = self._trading_days.index.searchsorted(now, side='left')

        if idx >= len(self._trading_days):
            logging.info("Cannot predict future")
            return 0

        # Directly access the data needed using more efficient methods
        market_close_time = self._trading_days.index[idx]
        market_open = self._trading_days.at[market_close_time, 'market_open']
        market_close = market_close_time  # Assuming this is a scalar value directly from the index

        # If we're before the market opens for the found trading day,
        # count the whole time until that day's market close so the clock
        # can advance instead of stalling.
        if now < market_open:
            delta = market_close - now
            return delta.total_seconds()

        delta = market_close - now
        return delta.total_seconds()

    def _await_market_to_open(self, timedelta=None, strategy=None):
        # Process outstanding orders first before waiting for market to open
        # or else they don't get processed until the next day
        self.process_pending_orders(strategy=strategy)

        time_to_open = self.get_time_to_open()

        # Allow the caller to specify a buffer (in minutes) before the actual open
        if timedelta:
            time_to_open -= 60 * timedelta

        # Only advance time if there is something positive to advance;
        # prevents zero or negative time updates.
        if time_to_open <= 0:
            return

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
            logging.debug("Calculated time to close is non-positive. Advancing time by 1 second.")
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
                    stop_loss_order = Order(
                        order.strategy,
                        order.asset,
                        order.quantity,
                        order.side,
                        stop_price=order.stop_price,
                        quote=order.quote,
                    )
                    stop_loss_order = self._parse_broker_order(stop_loss_order, order.strategy)
                    orders.append(stop_loss_order)

            elif order.order_class == Order.OrderClass.OCO:
                stop_loss_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    order.side,
                    stop_price=order.stop_price,
                    quote=order.quote,
                )
                orders.append(stop_loss_order)

                limit_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    order.side,
                    limit_price=order.limit_price,
                    quote=order.quote,
                )
                orders.append(limit_order)

                stop_loss_order.dependent_order = limit_order
                limit_order.dependent_order = stop_loss_order

            elif order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
                side = Order.OrderSide.SELL if order.is_buy_order() else Order.OrderSide.BUY
                if (order.order_class == Order.OrderClass.BRACKET or
                        (order.order_class == Order.OrderClass.OTO and order.secondary_stop_price)):
                    stop_loss_order = Order(
                        order.strategy,
                        order.asset,
                        order.quantity,
                        side,
                        stop_price=order.secondary_stop_price,
                        stop_limit_price=order.secondary_stop_limit_price,
                        trail_price=order.secondary_trail_price,
                        trail_percent=order.secondary_trail_percent,
                        quote=order.quote,
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
                    )
                    orders.append(limit_order)

                if order.order_class == Order.OrderClass.BRACKET:
                    stop_loss_order.dependent_order = limit_order
                    limit_order.dependent_order = stop_loss_order

        return orders

    def _process_filled_order(self, order, price, quantity):
        """
        BackTesting needs to create/update positions when orders are filled becuase there is no broker to do it
        """
        # This is a parent order, typically for a Multileg strategy. The parent order itself is expected to be
        # filled after all child orders are filled.
        if order.is_parent() and order.order_class in [Order.OrderClass.MULTILEG, Order.OrderClass.OCO]:
            order.avg_fill_price = price
            order.quantity = quantity
            self._update_parent_order_status(order)
            return super()._process_filled_order(order, price, quantity)  # Do not store parent order positions

        existing_position = self.get_tracked_position(order.strategy, order.asset)

        # Currently perfect fill price in backtesting!
        order.avg_fill_price = price

        position = super()._process_filled_order(order, price, quantity)
        if existing_position:
            position.add_order(order, quantity)  # Add will update quantity, but not double count the order
            if position.quantity == 0:
                logger.info(f"Position {position} liquidated")
                self._filled_positions.remove(position)
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

    def _update_parent_order_status(self, order: Order):
        """
        Update the status of a parent order based on the status of its child orders
        """
        if (order.is_parent() and order.is_active() and
                order.order_class in [Order.OrderClass.OCO]):
            # No changes to parent order status if any of the child orders are still active
            if any([o.is_active() for o in order.child_orders]):
                return
            elif any([o.is_filled() for o in order.child_orders]):
                order.status = Order.OrderStatus.FILLED
                order.set_filled()
                self._new_orders.remove(order.identifier, key="identifier")
                self._unprocessed_orders.remove(order.identifier, key="identifier")
            elif all([o.is_cancelled() for o in order.child_orders]):
                self.cancel_order(order)

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
            logging.error("No orders to submit to broker when calling submit_orders")
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
            logging.error("Cannot cash settle options contract in live trading")
            return

        # Check that the position is an options contract
        if position.asset.asset_type != "option":
            logging.error(f"Cannot cash settle non-option contract {position.asset}")
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

                logger.info(f"Automatically selling expired contract for asset {position.asset}")

                # Cash settle the options contract
                self.cash_settle_options_contract(position, strategy)

    def calculate_trade_cost(self, order: Order, strategy, price: float):
        """Calculate the trade cost of an order for a given strategy"""
        trade_cost = 0
        trading_fees = []
        if order.side == "buy":
            trading_fees: list[TradingFee] = strategy.buy_trading_fees
        elif order.side == "sell":
            trading_fees: list[TradingFee] = strategy.sell_trading_fees

        for trading_fee in trading_fees:
            if trading_fee.taker == True and order.order_type in [
                "market",
                "stop",
            ]:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(price) * Decimal(order.quantity) * trading_fee.percent_fee
            elif trading_fee.maker == True and order.order_type in [
                "limit",
                "stop_limit",
            ]:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(price) * Decimal(order.quantity) * trading_fee.percent_fee

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

        # Process expired contracts.
        self.process_expired_option_contracts(strategy)

        pending_orders = [
            order for order in self.get_tracked_orders(strategy.name) if order.status in ["unprocessed", "new"]
        ]

        if len(pending_orders) == 0:
            return

        for order in pending_orders:
            if order.dependent_order_filled or order.status == self.CANCELED_ORDER:
                continue

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
                    self.stream.dispatch(
                        self.FILLED_ORDER,
                        wait_until_complete=True,
                        order=order,
                        price=parent_price,
                        filled_quantity=parent_qty,
                    )

                continue

            # Check validity if current date > valid date, cancel order. todo valid date
            # TODO: One day... I will purge all this crypto tuple stuff.
            asset = order.asset if order.asset.asset_type != "crypto" else (order.asset, order.quote)

            price = None
            filled_quantity = order.quantity

            #############################
            # Get OHLCV data for the asset
            #############################

            # Get the OHLCV data for the asset if we're using the YAHOO, CCXT data source
            data_source_name = self.data_source.SOURCE.upper()
            if data_source_name in ["CCXT", "YAHOO", "ALPACA"]:
                if data_source_name in ["CCXT", "ALPACA"]:
                    # If we're using the CCXT or Alpaca data source, we don't need to timeshift the data.
                    # We fill at the open price of the current bar.
                    timeshift = None
                else:
                    # Yahoo requires a negative timedelta so that we get today
                    # (normally would get yesterday's data to prevent lookahead bias)
                    timeshift = timedelta(
                        days=-1
                    )

                ohlc = self.data_source.get_historical_prices(
                    asset=asset,
                    length=1,
                    quote=order.quote,
                    timeshift=timeshift,
                )

                dt = ohlc.df.index[-1]
                open = ohlc.df['open'].iloc[-1]
                high = ohlc.df['high'].iloc[-1]
                low = ohlc.df['low'].iloc[-1]
                close = ohlc.df['close'].iloc[-1]
                volume = ohlc.df['volume'].iloc[-1]

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
                if ohlc is None or ohlc.df.empty:
                    self.cancel_order(order)
                    continue

                df_original = ohlc.df

                # # Make sure that we are only getting the prices for the current time exactly or in the future
                df = df_original[df_original.index >= self.datetime]

                # If the dataframe is empty, then we should get the last row of the original dataframe
                # because it is the best data we have
                if df.empty:
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

            # If the price is set, then the order has been filled
            if price is not None:
                if order.dependent_order:
                    order.dependent_order.dependent_order_filled = True
                    strategy.broker.cancel_order(order.dependent_order)
                    # self.cancel_order(order.dependent_order)

                # Child orders of Bracket and OTO are not submitted until the parent order is filled
                if order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
                    for child_order in order.child_orders:
                        logger.info(f"{child_order} was sent to broker {self.name} now that the parent Bracket/OTO "
                                    f"order has been filled")
                        self._new_orders.append(child_order)

                trade_cost = self.calculate_trade_cost(order, strategy, price)

                new_cash = strategy.cash - float(trade_cost)
                strategy._set_cash_position(new_cash)
                order.trade_cost = float(trade_cost)

                self.stream.dispatch(
                    self.FILLED_ORDER,
                    wait_until_complete=True,
                    order=order,
                    price=price,
                    filled_quantity=filled_quantity,
                )
            else:
                continue

    def limit_order(self, limit_price, side, open_, high, low):
        """Limit order logic."""
        # Gap Up case: Limit wasn't triggered by previous candle but current candle opens higher, fill it now
        if side == "sell" and limit_price <= open_:
            return open_

        # Gap Down case: Limit wasn't triggered by previous candle but current candle opens lower, fill it now
        if side == "buy" and limit_price >= open_:
            return open_

        # Current candle triggered limit normally
        if low <= limit_price <= high:
            return limit_price

        # Limit has not been met
        return None

    def stop_order(self, stop_price, side, open_, high, low):
        """Stop order logic."""
        # Gap Down case: Stop wasn't triggered by previous candle but current candle opens lower, fill it now
        if side == "sell" and stop_price >= open_:
            return open_

        # Gap Up case: Stop wasn't triggered by previous candle but current candle opens higher, fill it now
        if side == "buy" and stop_price <= open_:
            return open_

        # Current candle triggered stop normally
        if low <= stop_price <= high:
            return stop_price

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
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.PLACEHOLDER_ORDER)
        def on_trade_event(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.PLACEHOLDER_ORDER,
                )
                return True
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
                return True
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.CANCELED_ORDER,
                )
                return True
            except:
                logging.error(traceback.format_exc())

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
                logging.error(traceback.format_exc())

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
                logging.error(traceback.format_exc())

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
