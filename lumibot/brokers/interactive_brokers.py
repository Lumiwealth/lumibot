import asyncio
import logging
import time
import traceback
from asyncio import CancelledError
from datetime import timezone
from dateutil import tz
import datetime

import pandas_market_calendars as mcal
import pandas as pd

from lumibot.data_sources import InteractiveBrokersData
from lumibot.entities import Order, Position
from .broker import Broker


class InteractiveBrokers(InteractiveBrokersData, Broker):
    """Inherit InteractiveBrokerData first and all the price market
    methods than inherits broker"""

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True):
        # Calling init methods
        InteractiveBrokersData.__init__(
            self,
            config,
            max_workers=max_workers,
            chunk_size=chunk_size,
        )
        Broker.__init__(self, name="interactive_brokers", connect_stream=connect_stream)

    # =========Clock functions=====================

    def utc_to_local(self, utc_dt):
        return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())

    def get_timestamp(self):
        """return current timestamp"""
        clock = self.ib.get_timestamp()
        return clock

    def market_hours(self, market="NASDAQ", close=True, next=False, date=None):
        mkt_cal = mcal.get_calendar(market)
        date = date if date is not None else datetime.datetime.now()
        trading_hours = mkt_cal.schedule(
            start_date=date, end_date=date + datetime.timedelta(weeks=1)
        ).head(2)

        row = 0 if not next else 1
        th = trading_hours.iloc[row, :]
        market_open, market_close = th[0], th[1]

        if close:
            return market_close
        else:
            return market_open

    def market_close_time(self):
        return self.utc_to_local(self.market_hours(close=True))

    def is_market_open(self):
        """return True if market is open else false"""
        open_time = self.utc_to_local(self.market_hours(close=False))
        close_time = self.utc_to_local(self.market_hours(close=True))

        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())

        return (current_time >= open_time) and (close_time >= current_time)

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        open_time_this_day = self.utc_to_local(
            self.market_hours(close=False, next=False)
        )
        open_time_next_day = self.utc_to_local(
            self.market_hours(close=False, next=True)
        )
        now = self.utc_to_local(datetime.datetime.now())
        open_time = (
            open_time_this_day if open_time_this_day > now else open_time_next_day
        )
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return None
        else:
            return open_time.timestamp() - current_time.timestamp()

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        close_time = self.utc_to_local(self.market_hours(close=True))
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return close_time.timestamp() - current_time.timestamp()
        else:
            return None

    # =========Positions functions==================

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        symbol = broker_position.Symbol
        quantity = int(broker_position.Quantity)
        position = Position(strategy, symbol, quantity, orders=orders)
        return position

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for account, broker_position in broker_positions.iterrows():
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_broker_position(self, symbol):
        """Given a symbol, get the broker representation
        of the corresponding symbol"""
        result = self._pull_broker_positions()
        result = result[result["Symbol"] == symbol].squeeze()
        return result

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        self.ib.reqPositions()  # associated callback: position

        # Gather the results.
        positions_exists = True
        positions = list()
        while positions_exists:
            try:
                position = self.ib.positionTracker.popleft()
                if len(position) > 0:
                    positions.append(position)
                else:
                    positions_exists = False
            except:
                time.sleep(0.02)
        current_positions = pd.DataFrame(
            data=positions,
            columns=["Account", "Symbol", "Quantity", "Average Cost", "Sec Type"],
        )
        current_positions = current_positions.set_index("Account", drop=True)
        current_positions["Quantity"] = current_positions["Quantity"].astype("int")
        current_positions = current_positions[current_positions["Quantity"] != 0]

        return current_positions

    # =======Orders and assets functions=========

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""

        order = Order(
            strategy,
            response.contract.localSymbol,
            response.totalQuantity,
            response.action.lower(),
            limit_price=response.lmtPrice,
            stop_price=response.adjustedStopPrice,
            time_in_force=response.tif,
        )
        order._transmitted = True
        order.set_identifier(response.orderId)
        order.update_status(response.orderState.status)
        order.update_raw(response)
        return order

    def _pull_broker_order(self, order_id):
        """Get a broker order representation by its id"""
        pull_order = [
            order for order in self.api.openOrders() if order.orderId == order_id
        ]
        response = pull_order[0] if len(pull_order) > 0 else None
        return response

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        orders = self.api.openOrders()
        return orders

    def _flatten_order(self, order):  # todo ask about this.
        """Some submitted orders may triggers other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        return [order]  # todo made iterable for now.

    def submit_order(self, order):
        """Submit an order for an asset"""
        try:
            contract_object = self.create_contract(order.symbol)
            order_object = self.create_order(order)
            nextID = self.ib.nextOrderId()
            self.ib.placeOrder(nextID, contract_object, order_object)
            while nextID not in self.ib.openOrderDict:
                time.sleep(0.02)
            while len(self.ib.openOrderDict[nextID]) == 0:
                time.sleep(0.02)
            response = self.ib.openOrderDict[nextID]

            ib_order = self._parse_broker_order(response[0], order.strategy)
            return ib_order
        except Exception as e:
            order.set_error(e)
            logging.info(
                "%r did not go through. The following error occured: %s" % (order, e)
            )

    def cancel_order(self, order_id):
        """Cancel an order"""
        order = self._pull_broker_order(order_id)
        print(order)
        if order:
            self.api.cancelOrder(order)

    def cancel_open_orders(self, strategy=None):
        """cancel all the strategy open orders"""
        self.ib.reqGlobalCancel()

    # =========Market functions=======================

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets from the market"""
        assets = self.api.list_assets()
        result = []
        for asset in assets:
            is_valid = asset.tradable
            if easy_to_borrow is not None and isinstance(easy_to_borrow, bool):
                is_valid = is_valid & (easy_to_borrow == asset.easy_to_borrow)
            if filter_func is not None:
                filter_test = filter_func(asset.symbol)
                is_valid = is_valid & filter_test

            if is_valid:
                result.append(asset.symbol)

        return result

    def _close_connection(self):
        self.ib.disconnect()

    #
    # # =======Stream functions=========
    #
    # def _get_stream_object(self):
    #     """get the broker stream connection"""
    #     # stream = tradeapi.StreamConn(self.api_key, self.api_secret, self.endpoint)
    #     stream = self.get_connection()
    #     return stream
    #
    # def _register_stream_events(self):
    #     """Register the function on_trade_event
    #     to be executed on each trade_update event"""
    #     broker = self
    #
    #     @self.stream.on(r"^trade_updates$")
    #     async def on_trade_event(conn, channel, data):
    #         try:
    #             logged_order = data.order
    #             type_event = data.event
    #             identifier = logged_order.get("id")
    #             stored_order = broker.get_tracked_order(identifier)
    #             if stored_order is None:
    #                 logging.info(
    #                     "Untracker order %s was logged by broker %s"
    #                     % (identifier, broker.name)
    #                 )
    #                 return False
    #
    #             price = data.price if hasattr(data, "price") else None
    #             filled_quantity = data.qty if hasattr(data, "qty") else None
    #             broker._process_trade_event(
    #                 stored_order,
    #                 type_event,
    #                 price=price,
    #                 filled_quantity=filled_quantity,
    #             )
    #
    #             return True
    #         except:
    #             logging.error(traceback.format_exc())
    #
    # def _run_stream(self):
    #     """Overloading default alpaca_trade_api.STreamCOnnect().run()
    #     Run forever and block until exception is raised.
    #     initial_channels is the channels to start with.
    #     """
    #     loop = self.stream.loop
    #     should_renew = True  # should renew connection if it disconnects
    #     while should_renew:
    #         try:
    #             if loop.is_closed():
    #                 self.stream.loop = asyncio.new_event_loop()
    #                 loop = self.stream.loop
    #             loop.run_until_complete(self.stream.subscribe(["trade_updates"]))
    #             self._stream_established()
    #             loop.run_until_complete(self.stream.consume())
    #         except KeyboardInterrupt:
    #             logging.info("Exiting on Interrupt")
    #             should_renew = False
    #         except Exception as e:
    #             m = "consume cancelled" if isinstance(e, CancelledError) else e
    #             logging.error(f"error while consuming ws messages: {m}")
    #             if self.stream._debug:
    #                 logging.error(traceback.format_exc())
    #             loop.run_until_complete(self.stream.close(should_renew))
    #             if loop.is_running():
    #                 loop.close()
