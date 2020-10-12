from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread
from functools import wraps

import time, logging

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from .broker import Broker
from data_sources import AlpacaData


class Alpaca(Broker):
    def __init__(self, config, connect_stream=True, max_workers=200, chunk_size=100):
        # Calling the Broker init method
        super().__init__()

        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading to 200
        # to go full speed if needed
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        api_key = config.API_KEY
        api_secret = config.API_SECRET
        if hasattr(config, "ENDPOINT"):
            endpoint = config.ENDPOINT
        else:
            endpoint = "https://paper-api.alpaca.markets"
        if hasattr(config, 'VERSION'):
            version = config.VERSION
        else:
            version = "v2"
        self.api = tradeapi.REST(api_key, api_secret, URL(endpoint), version)

        # Connection to alpaca socket stream
        self.stream = tradeapi.StreamConn(api_key, api_secret, URL(endpoint))
        if connect_stream:
            self.set_streams()

    def __getattr__(self, name):
        try:
            method = getattr(AlpacaData, name)
            api = self.api
            @wraps(method)
            def func(*arg, **kwargs):
                return method(api, *arg, **kwargs)

            return func
        except AttributeError:
            raise AttributeError(
                "type object %s has no attribute %s" % (self.__class__.__name__, name)
            )

    # =======API functions=========

    def get_positions(self):
        """Get the account positions"""
        positions = self.api.list_positions()
        return positions

    def get_open_orders(self):
        """Get the account open orders"""
        orders = self.api.list_orders(status="open")
        return orders

    def cancel_open_orders(self):
        """Cancel all the buying orders with status still open"""
        orders = self.api.list_orders(status="open")
        for order in orders:
            logging.info(
                "%s order of | %d %s %s | canceled."
                % (order.type, int(order.qty), order.symbol, order.side)
            )
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

    def submit_order(self, symbol, quantity, side, limit_price=None, stop_price=None):
        """Submit an order for an asset"""
        if quantity > 0:
            try:
                order_type = "limit" if limit_price else "market"
                order_class = "oto" if stop_price else None
                time_in_force = "day"
                kwargs = {
                    "type": order_type,
                    "order_class": order_class,
                    "time_in_force": time_in_force,
                    "limit_price": limit_price,
                }
                if stop_price:
                    kwargs["stop_loss"] = {"stop_price": stop_price}

                # Remove items with None values
                kwargs = {k: v for k, v in kwargs.items() if v}
                self.api.submit_order(symbol, quantity, side, **kwargs)
                return True
            except Exception as e:
                message = str(e)
                if "stop price must not be greater than base price / 1.001" in message:
                    logging.info(
                        "Order of | %d %s %s | did not go through because the share base price became lesser than the stop loss price."
                        % (quantity, symbol, side)
                    )
                    return False
                else:
                    logging.info(
                        "Order of | %d %s %s | did not go through. The following error occured: %s"
                        % (quantity, symbol, side, e)
                    )
                    return False
        else:
            logging.info(
                "Order of | %d %s %s | not completed" % (quantity, symbol, side)
            )
            return True

    def submit_orders(self, orders):
        """submit orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                symbol = order.get("symbol")
                quantity = order.get("quantity")
                side = order.get("side")

                func = lambda args, kwargs: self.submit_order(*args, **kwargs)
                args = (symbol, quantity, side)
                kwargs = {}
                if order.get("stop_price"):
                    kwargs["stop_price"] = order.get("stop_price")
                if order.get("limit_price"):
                    kwargs["limit_price"] = order.get("limit_price")

                tasks.append(executor.submit(func, args, kwargs))

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
                delta = dt.timedelta(seconds=time_to_open)
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

    def set_streams(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""

        @self.stream.on(r"^trade_updates$")
        async def default_on_trade_event(conn, channel, data):
            self.log_trade_event(data)

        t = Thread(target=self.stream.run, args=[["trade_updates"]], daemon=True)
        t.start()

    def log_trade_event(self, data):
        order = data.order
        type_event = data.event
        symbol = order.get("symbol")
        side = order.get("side")
        order_quantity = order.get("qty")
        order_type = order.get("order_type").capitalize()
        representation = {
            "type": order_type,
            "symbol": symbol,
            "side": side,
            "quantity": order_quantity,
        }

        # if statement on event type
        if type_event == "fill":
            price = data.price
            filled_quantity = data.qty
            logging.info(
                "%s order of | %s %s %s | filled. %s$ per share"
                % (order_type, filled_quantity, symbol, side, price)
            )
            if order_quantity != filled_quantity:
                representation["filled_quantity"] = filled_quantity
                logging.info(
                    "Initial %s order of | %s %s %s | completed."
                    % (order_type, order_quantity, symbol, side)
                )
            self.filled_orders.append(representation)

        elif type_event == "partial_fill":
            price = data.price
            filled_quantity = data.qty
            logging.info(
                "Executing Initial %s order of | %s %s %s |. Order partially filled"
                % (order_type, order_quantity, symbol, side)
            )
            logging.info(
                "%s order of | %s %s %s | completed. %s$ per share"
                % (order_type, filled_quantity, symbol, side, price)
            )
            representation["filled_quantity"] = filled_quantity
            self.partially_filled_orders.append(representation)

        elif type_event == "new":
            logging.info(
                "New %s order of | %s %s %s | submited."
                % (order_type, order_quantity, symbol, side)
            )
            self.new_orders.append(representation)

        elif type_event == "canceled":
            logging.info(
                "%s order of | %s %s %s | canceled."
                % (order_type, order_quantity, symbol, side)
            )
            self.canceled_orders.append(representation)

        else:
            logging.debug(
                "Unhandled type event %s for %s order of | %s %s %s |"
                % (type_event, order_type, order_quantity, symbol, side)
            )