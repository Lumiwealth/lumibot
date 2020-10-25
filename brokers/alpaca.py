import asyncio
import logging
import time
import traceback
from asyncio import CancelledError
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Thread

import alpaca_trade_api as tradeapi

from data_sources import AlpacaData

from .broker import Broker


class Alpaca(Broker, AlpacaData):
    def __init__(
        self, config, connect_stream=True, max_workers=20, chunk_size=100, debug=False
    ):
        # Calling the Broker and AlpacaData init method
        Broker.__init__(self, debug=debug)
        AlpacaData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )

        # Connection to alpaca socket stream
        self.stream = tradeapi.StreamConn(
            self.api_key, self.api_secret, self.endpoint, debug=self.debug
        )
        if connect_stream:
            self.set_streams()

    # =======Orders and assets functions=========

    def get_positions(self):
        """Get the account positions"""
        positions = self.api.list_positions()
        return positions

    def get_open_orders(self):
        """Get the account open orders"""
        orders = self.api.list_orders(status="open")
        return orderslogging

    def cancel_open_orders(self):
        """Cancel all the buying orders with status still open"""
        orders = self.api.list_orders(status="open")
        for order in orders:
            self.api.cancel_order(order.id)

    def get_ongoing_assets(self):
        """Get the list of symbols for positions
        and open orders"""
        orders = self.get_open_orders()
        positions = self.get_positions()
        result = [o.symbol for o in orders] + [p.symbol for p in positions]
        return list(set(result))

    def get_account(self):
        """Get the account data from the API"""
        account = self.api.get_account()
        return account

    def submit_order(self, order):
        """Submit an order for an asset"""
        kwargs = {
            "type": order.type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            "limit_price": order.limit_price,
        }
        if order.stop_price:
            kwargs["stop_loss"] = {"stop_price": order.stop_price}

        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}
        try:
            raw = self.api.submit_order(
                order.symbol, order.quantity, order.side, **kwargs
            )
            order.update_raw(raw)
            order.set_identifier(raw.id)
        except Exception as e:
            order.set_error(e)
            message = str(e)
            if "stop price must not be greater than base price / 1.001" in message:
                logging.info(
                    "%r did not go through because the share base price became lesser than the stop loss price."
                    % order
                )
            else:
                logging.info(
                    "%r did not go through. The following error occured: %s"
                    % (order, e)
                )

        return order

    def submit_orders(self, orders):
        """submit orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                tasks.append(executor.submit(self.submit_order, order))

    def sell_all(self, cancel_open_orders=True):
        """sell all positions"""
        if cancel_open_orders:
            self.cancel_open_orders()

        orders = []
        positions = self.get_positions()
        for position in positions:
            order = {
                "symbol": position.symbol,
                "quantity": int(position.qty),
                "side": "sell",
            }
            orders.append(order)
        self.submit_orders(orders)

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        isOpen = self.is_market_open()
        while not isOpen:
            time_to_open = self.get_time_to_open()
            if time_to_open > 60 * 60:
                delta = timedelta(seconds=time_to_open)
                logging.info("Market will open in %s." % str(delta))
                time.sleep(60 * 60)
            elif time_to_open > 60:
                logging.info("%d minutes til market open." % int(time_to_open / 60))
                time.sleep(60)
            else:
                logging.info("%d seconds til market open." % time_to_open)
                time.sleep(time_to_open)

            isOpen = self.is_market_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            sleeptime = max(0, time_to_close)
            time.sleep(sleeptime)

    # =======Stream functions=========

    def run_stream(self):
        """Overloading default alpaca_trade_api.STreamCOnnect().run()
        Run forever and block until exception is raised.
        initial_channels is the channels to start with.
        """
        loop = self.stream.loop
        should_renew = True  # should renew connection if it disconnects
        while should_renew:
            try:
                if loop.is_closed():
                    self.stream.loop = asyncio.new_event_loop()
                    loop = self.stream.loop
                loop.run_until_complete(self.stream.subscribe(["trade_updates"]))
                self._is_stream_subscribed = True
                loop.run_until_complete(self.stream.consume())
            except KeyboardInterrupt:
                logging.info("Exiting on Interrupt")
                should_renew = False
            except Exception as e:
                m = "consume cancelled" if isinstance(e, CancelledError) else e
                logging.error(f"error while consuming ws messages: {m}")
                if self.stream._debug:
                    traceback.print_exc()
                loop.run_until_complete(self.stream.close(should_renew))
                if loop.is_running():
                    loop.close()

    def set_streams(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""
        @self.stream.on(r"^trade_updates$")
        async def default_on_trade_event(conn, channel, data):
            self.log_trade_event(data)

        t = Thread(target=self.run_stream, daemon=True)
        t.start()
        self._is_stream_subscribed = False
        while True:
            if self._is_stream_subscribed is True:
                break

    def log_trade_event(self, data):
        logged_order = data.order
        type_event = data.event
        identifier = logged_order.get("id")
        stored_order = self.get_order(identifier)
        if stored_order is None:
            logging.info(
                "Untracker order %s was logged by broker %s" % (identifier, self.name)
            )
            return None

        # if statement on event type
        if type_event == "new":
            self.move_order_to_new(stored_order)
        elif type_event == "canceled":
            self.move_order_to_canceled(stored_order)
        elif type_event == "fill":
            price = data.price
            filled_quantity = data.qty
            self.move_order_to_filled(stored_order, price, filled_quantity)
        elif type_event == "partial_fill":
            price = data.price
            filled_quantity = data.qty
            self.move_order_to_partially_filled(stored_order, price, filled_quantity)
        else:
            logging.debug("Unhandled type event %s for %r" % (type_event, stored_order))

        return None
