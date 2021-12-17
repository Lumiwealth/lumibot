import asyncio
import logging
import traceback
# from asyncio import CancelledError
from datetime import timezone


from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order, Position

from .broker import Broker


class Ccxt(CcxtData, Broker):
    """Inherit CcxtData first and all the price market
    methods than inherits broker

    """

    ASSET_TYPE_MAP = dict(
        stock=["us_equity"],
        option=[],
        future=[],
        forex=[],
    )

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=False):
        # Calling init methods
        CcxtData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
        Broker.__init__(self, name="ccxt", connect_stream=connect_stream)

        self.market = "24/7"
    # =========Clock functions=====================

    def get_timestamp(self):
        """Returns the current UNIX timestamp representation from Alpaca

        Parameters
        ----------
        None

        Returns
        -------
        int
            Sample unix timestamp return value: 1612172730.000234

        """
        return self.api.microseconds() / 1000000

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
        return True

    def get_time_to_open(self):
        """How much time in seconds remains until the market next opens?

        Return the remaining time for the market to open in seconds

        Parameters
        ----------
        None

        Returns
        -------
        int
            Number of seconds until open.

        Examples
        --------
        If it is 0830 and the market next opens at 0930, then there are 3,600
        seconds until the next market open.

        >>> self.get_time_to_open()
        """

        return None

    def get_time_to_close(self):
        """How much time in seconds remains until the market closes?

        Return the remaining time for the market to closes in seconds

        Parameters
        ----------
        None

        Returns
        -------
        int
            Number of seconds until close.

        Examples
        --------
        If it is 1400 and the market closes at 1600, then there are 7,200
        seconds until the market closes.
        """
        clock = self.api.get_clock()
        closing_time = clock.next_close.timestamp()
        curr_time = clock.timestamp.timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    # =========Positions functions==================

    def _get_cash_balance_at_broker(self):
        """Get the current cash balance at Alpaca.

        Parameters
        ----------
        None

        Returns
        -------
        float
        """
        response = self.api.get_account()
        return float(response._raw["cash"])

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        position = broker_position._raw
        asset = Asset(
            symbol=position["symbol"],
        )
        quantity = position["qty"]
        position = Position(strategy, asset, quantity, orders=orders)
        return position

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
        response = self.api.get_position(asset)
        return response

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        response = self.api.list_positions()
        return response

    # =======Orders and assets functions=========
    def map_asset_type(self, type):
        for k, v in self.ASSET_TYPE_MAP.items():
            if type in v:
                return k
        raise ValueError(
            f"The type {type} is not in the ASSET_TYPE_MAP in the Alpaca Module."
        )

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""
        order = Order(
            strategy,
            Asset(
                symbol=response.symbol,
                asset_type="stock",
            ),
            response.qty,
            response.side,
            limit_price=response.limit_price,
            stop_price=response.stop_price,
            time_in_force=response.time_in_force,
        )
        order.set_identifier(response.id)
        order.update_status(response.status)
        order.update_raw(response)
        return order

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        response = self.api.get_order(id)
        return response

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        orders = self.api.list_orders(status="open")
        return orders

    def _flatten_order(self, order):
        """Some submitted orders may trigger other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]
        if order._raw.legs:
            strategy = order.strategy
            for json_sub_order in order._raw.legs:
                sub_order = self._parse_broker_order(json_sub_order, strategy)
                orders.append(sub_order)

        return orders

    def _submit_order(self, order):
        """Submit an order for an asset"""
        kwargs = {
            "type": order.type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            "limit_price": order.limit_price,
            "stop_price": order.stop_price,
            "trail_price": order.trail_price,
            "trail_percent": order.trail_percent,
        }
        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}

        if order.take_profit_price:
            kwargs["take_profit"] = {"limit_price": order.take_profit_price}

        if order.stop_loss_price:
            kwargs["stop_loss"] = {"stop_price": order.stop_loss_price}
            if order.stop_loss_limit_price:
                kwargs["stop_loss"]["limit_price"] = order.stop_loss_limit_price

        try:
            response = self.api.submit_order(
                order.asset.symbol, order.quantity, order.side, **kwargs
            )

            order.set_identifier(response.id)
            order.update_status(response.status)
            order.update_raw(response)

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

    def cancel_order(self, order):
        """Cancel an order"""
        self.api.cancel_order(order.identifier)

    # # =======Stream functions=========
    #
    # def _get_stream_object(self):
    #     """get the broker stream connection"""
    #     stream = tradeapi.StreamConn(self.api_key, self.api_secret, self.endpoint)
    #     return stream
    #
    # def _register_stream_events(self):
    #     """Register the function on_trade_event
    #     to be executed on each trade_update event"""
    #
    #     @self.stream.on(r"^trade_updates$")
    #     async def on_trade_event(conn, channel, data):
    #         self._orders_queue.join()
    #         try:
    #             logged_order = data.order
    #             type_event = data.event
    #             identifier = logged_order.get("id")
    #             stored_order = self.get_tracked_order(identifier)
    #             if stored_order is None:
    #                 logging.info(
    #                     "Untracked order %s was logged by broker %s"
    #                     % (identifier, self.name)
    #                 )
    #                 return False
    #
    #             price = data.price if hasattr(data, "price") else None
    #             filled_quantity = data.qty if hasattr(data, "qty") else None
    #             self._process_trade_event(
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
