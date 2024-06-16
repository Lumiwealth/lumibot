import logging
import traceback
from datetime import timedelta
from decimal import Decimal
from functools import wraps

import pandas as pd

from lumibot.brokers import Broker
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Order, Position, TradingFee
from lumibot.trading_builtins import CustomStream


class BacktestingBroker(Broker):
    # Metainfo
    IS_BACKTESTING_BROKER = True

    def __init__(self, data_source, connect_stream=True, max_workers=20, config=None, **kwargs):
        super().__init__(name="backtesting", data_source=data_source, connect_stream=connect_stream, **kwargs)
        # Calling init methods
        self.max_workers = max_workers
        self.market = "NASDAQ"

        # Legacy strategy.backtest code will always pass in a config even for Brokers that don't need it, so
        # catch it here and ignore it in this class. Child classes that need it should error check it themselves.
        # self._config = config

        if not isinstance(self.data_source, DataSourceBacktesting):
            raise ValueError("Must provide a backtesting data_source to run with a BacktestingBroker")

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)

        if name == "submit_order":
            broker = self

            @wraps(attr)
            def new_func(order, *args, **kwargs):
                result = attr(order, *args, **kwargs)
                if result.was_transmitted() and result.order_class and result.order_class == "oco":
                    orders = broker._flatten_order(result)
                    for order in orders:
                        logging.info(f"{order} was sent to broker {self.name}")
                        broker._new_orders.append(order)

                    # Remove the original order from the list of new orders because
                    # it's been replaced by the individual orders
                    broker._new_orders.remove(result)
                elif order not in broker._new_orders:
                    # David M: This seems weird and I don't understand why we're doing this.  It seems like
                    # we're adding the order to the new orders list twice, so checking first.
                    broker._new_orders.append(order)
                return result

            return new_func
        else:
            return attr

    @property
    def datetime(self):
        return self.data_source.get_datetime()

    def _submit_order(self, order):
        """TODO: Why is this not used for Backtesting, but it is used for real brokers?"""
        pass

    def _get_balances_at_broker(self, quote_asset):
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

        if isinstance(update_dt, timedelta):
            new_datetime = self.datetime + update_dt
        elif isinstance(update_dt, int) or isinstance(update_dt, float):
            new_datetime = self.datetime + timedelta(seconds=update_dt)
        else:
            new_datetime = update_dt

        self.data_source._update_datetime(new_datetime, cash=cash, portfolio_value=portfolio_value)
        logging.info(f"Current backtesting datetime {self.datetime}")

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

        # As the index is sorted, use searchsorted to find the relevant day
        idx = self._trading_days.index.searchsorted(now, side='right')

        # Check that the index is not out of bounds
        if idx >= len(self._trading_days):
            logging.error("Cannot predict future")
            return False

        # The index of the trading_day is used as the market close time
        market_close = self._trading_days.index[idx]

        # Retrieve market open time using .at since idx is a valid datetime index
        market_open = self._trading_days.at[market_close, 'market_open']

        # Check if 'now' is within the trading hours of the located day
        return market_open <= now < market_close

    def _get_next_trading_day(self):
        now = self.datetime
        search = self._trading_days[now < self._trading_days.market_open]
        if search.empty:
            logging.error("Cannot predict future")

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self.datetime

        search = self._trading_days[now < self._trading_days.index]
        if search.empty:
            logging.error("Cannot predict future")
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
            logging.error("Cannot predict future")
            return 0

        # Directly access the data needed using more efficient methods
        market_close_time = self._trading_days.index[idx]
        market_open = self._trading_days.at[market_close_time, 'market_open']
        market_close = market_close_time  # Assuming this is a scalar value directly from the index

        if now < market_open:
            return None

        delta = market_close - now
        return delta.total_seconds()

    def _await_market_to_open(self, timedelta=None, strategy=None):
        if self.data_source.SOURCE == "PANDAS" and self.data_source._timestep == "day":
            return

        # Process outstanding orders first before waiting for market to open
        # or else they don't get processed until the next day
        self.process_pending_orders(strategy=strategy)

        time_to_open = self.get_time_to_open()
        if timedelta:
            time_to_open -= 60 * timedelta
        self._update_datetime(time_to_open)

    def _await_market_to_close(self, timedelta=None, strategy=None):
        if self.data_source.SOURCE == "PANDAS" and self.data_source._timestep == "day":
            return

        # Process outstanding orders first before waiting for market to close
        # or else they don't get processed until the next day
        self.process_pending_orders(strategy=strategy)

        result = self.get_time_to_close()

        if result is None:
            time_to_close = 0
        else:
            time_to_close = result

        if timedelta is not None:
            time_to_close -= 60 * timedelta
        self._update_datetime(time_to_close)

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

        orders = []
        if order.order_class == "":
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

        elif order.order_class == "oco":
            stop_loss_order = Order(
                order.strategy,
                order.asset,
                order.quantity,
                order.side,
                stop_price=order.stop_loss_price,
                quote=order.quote,
            )
            orders.append(stop_loss_order)

            limit_order = Order(
                order.strategy,
                order.asset,
                order.quantity,
                order.side,
                limit_price=order.take_profit_price,
                quote=order.quote,
            )
            orders.append(limit_order)

            stop_loss_order.dependent_order = limit_order
            limit_order.dependent_order = stop_loss_order

        elif order.order_class in ["bracket", "oto"]:
            side = "sell" if order.side == "buy" else "buy"
            if order.order_class == "bracket" or (order.order_class == "oto" and order.stop_loss_price):
                stop_loss_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    side,
                    stop_price=order.stop_loss_price,
                    limit_price=order.stop_loss_limit_price,
                    quote=order.quote,
                )
                orders.append(stop_loss_order)

            if order.order_class == "bracket" or (order.order_class == "oto" and order.take_profit_price):
                limit_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    side,
                    limit_price=order.take_profit_price,
                    quote=order.quote,
                )
                orders.append(limit_order)

            if order.order_class == "bracket":
                stop_loss_order.dependent_order = limit_order
                limit_order.dependent_order = stop_loss_order

        return orders

    def _process_filled_order(self, order, price, quantity):
        """
        BackTesting needs to create/update positions when orders are filled becuase there is no broker to do it
        """
        existing_position = self.get_tracked_position(order.strategy, order.asset)

        # Currently perfect fill price in backtesting!
        order.avg_fill_price = price

        position = super()._process_filled_order(order, order.avg_fill_price, quantity)
        if existing_position:
            position.add_order(order, quantity)  # Add will update quantity, but not double count the order
            if position.quantity == 0:
                logging.info("Position %r liquidated" % position)
                self._filled_positions.remove(position)
        else:
            self._filled_positions.append(position)  # New position, add it to the tracker
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
                logging.info("Position %r liquidated" % existing_position)
                self._filled_positions.remove(existing_position)

    def submit_order(self, order):
        """Submit an order for an asset"""
        # NOTE: This code is to address Tradier API requirements, they want is as "to_open" or "to_close" instead of just "buy" or "sell"
        # If the order has a "buy_to_open" or "buy_to_close" side, then we should change it to "buy"
        if order.side in ["buy_to_open", "buy_to_close"]:
            order.side = "buy"
        # If the order has a "sell_to_open" or "sell_to_close" side, then we should change it to "sell"
        if order.side in ["sell_to_open", "sell_to_close"]:
            order.side = "sell"

        order.update_raw(order)
        self.stream.dispatch(
            self.NEW_ORDER,
            wait_until_complete=True,
            order=order,
        )
        return order

    def submit_orders(self, orders, **kwargs):
        results = []
        for order in orders:
            results.append(self.submit_order(order))
        return results

    def cancel_order(self, order):
        """Cancel an order"""
        self.stream.dispatch(
            self.CANCELED_ORDER,
            wait_until_complete=True,
            order=order,
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
        new_cash = strategy.get_cash() + profit_loss

        # Update the cash position
        strategy._set_cash_position(new_cash)

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

                logging.info(f"Automatically selling expired contract for asset {position.asset}")

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
            if trading_fee.taker == True and order.type in [
                "market",
                "stop",
            ]:
                trade_cost += trading_fee.flat_fee
                trade_cost += Decimal(price) * Decimal(order.quantity) * trading_fee.percent_fee
            elif trading_fee.maker == True and order.type in [
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

            # Check validity if current date > valid date, cancel order. todo valid date
            asset = order.asset if order.asset.asset_type != "crypto" else (order.asset, order.quote)

            price = None
            filled_quantity = order.quantity

            #############################
            # Get OHLCV data for the asset
            #############################

            # Get the OHLCV data for the asset if we're using the YAHOO, CCXT data source
            data_source_name = self.data_source.SOURCE.upper()
            if data_source_name in ["CCXT", "YAHOO"]:
                # If we're using the CCXT data source, we don't need to timeshift the data
                if data_source_name == "CCXT":
                    timeshift = None
                else:
                    timeshift = timedelta(
                        days=-1
                    )  # Is negative so that we get today (normally would get yesterday's data to prevent lookahead bias)

                ohlc = strategy.get_historical_prices(
                    asset,
                    1,
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
                ohlc = strategy.get_historical_prices(
                    asset,
                    2,
                    quote=order.quote,
                    timeshift=-2,
                    timestep=self.data_source._timestep,
                )
                # Check if we got any ohlc data
                if ohlc is None:
                    self.cancel_order(order)
                    continue

                df_original = ohlc.df

                # Make sure that we are only getting the prices for the current time exactly or in the future
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

            if order.type == "market":
                price = open

            elif order.type == "limit":
                price = self.limit_order(order.limit_price, order.side, open, high, low)

            elif order.type == "stop":
                price = self.stop_order(order.stop_price, order.side, open, high, low)

            elif order.type == "stop_limit":
                if not order.price_triggered:
                    price = self.stop_order(order.stop_price, order.side, open, high, low)
                    if price is not None:
                        price = self.limit_order(order.limit_price, order.side, price, high, low)
                        order.price_triggered = True
                elif order.price_triggered:
                    price = self.limit_order(order.limit_price, order.side, open, high, low)

            elif order.type == "trailing_stop":
                if order._trail_stop_price:
                    # Check if we have hit the trail stop price for both sell/buy orders
                    price = self.stop_order(order._trail_stop_price, order.side, open, high, low)

                # Update the stop price if the price has moved
                if order.side == "sell":
                    order.update_trail_stop_price(high)
                elif order.side == "buy":
                    order.update_trail_stop_price(low)

            else:
                raise ValueError(f"Order type {order.type} is not implemented for backtesting.")

            #############################
            # Fill the order.
            #############################

            # If the price is set, then the order has been filled
            if price is not None:
                if order.dependent_order:
                    order.dependent_order.dependent_order_filled = True
                    strategy.broker.cancel_order(order.dependent_order)

                    # self.cancel_order(order.dependent_order)

                if order.order_class in ["bracket", "oto"]:
                    orders = self._flatten_order(order)
                    for flat_order in orders:
                        logging.info(f"{order} was sent to broker {self.name}")
                        self._new_orders.append(flat_order)

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
