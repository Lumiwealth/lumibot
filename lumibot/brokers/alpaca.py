import asyncio
import datetime
import logging
import traceback
from asyncio import CancelledError
from datetime import timezone
from decimal import Decimal

import alpaca_trade_api as tradeapi
from dateutil import tz
from numpy import str0
from termcolor import colored

from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset, Order, Position

from .broker import Broker


class Alpaca(AlpacaData, Broker):
    """A broker class that connects to Alpaca

    Attributes
    ----------
    api : tradeapi.REST
        Alpaca API object

    Methods
    -------
    get_timestamp()
        Returns the current UNIX timestamp representation from Alpaca

    is_market_open()
        Determines if the market is open.

    get_time_to_open()
        How much time in seconds remains until the market next opens?

    get_time_to_close()
        How much time in seconds remains until the market closes?

    Examples
    --------
    >>> # Connect to Alpaca
    >>> from lumibot.brokers import Alpaca
    >>> class AlpacaConfig:
    ...     API_KEY = 'your_api_key'
    ...     SECRET_KEY = 'your_secret_key'
    ...     ENDPOINT = 'https://paper-api.alpaca.markets'
    >>> alpaca = Alpaca(AlpacaConfig)
    >>> print(alpaca.get_time_to_open())
    >>> print(alpaca.get_time_to_close())
    >>> print(alpaca.is_market_open())

    >>> # Run a strategy on Alpaca
    >>> from lumibot.strategies import Strategy
    >>> from lumibot.brokers import Alpaca
    >>> from lumibot.traders import Trader
    >>>
    >>> class AlpacaConfig:
    ...     # Put your own Alpaca key here:
    ...     API_KEY = "YOUR_API_KEY"
    ...     # Put your own Alpaca secret here:
    ...     API_SECRET = "YOUR_API_SECRET"
    ...     # If you want to go live, you must change this
    ...     ENDPOINT = "https://paper-api.alpaca.markets"
    >>>
    >>> class AlpacaStrategy(Strategy):
    ...     def on_trading_interation(self):
    ...         if self.broker.is_market_open():
    ...             self.create_order(
    ...                 asset=Asset(symbol="AAPL"),
    ...                 quantity=1,
    ...                 order_type="market",
    ...                 side="buy",
    ...             )
    >>>
    >>> alpaca = Alpaca(AlpacaConfig)
    >>> strategy = AlpacaStrategy(broker=alpaca)
    >>> trader = Trader()
    >>> trader.add_strategy(strategy)
    >>> trader.run()

    """

    ASSET_TYPE_MAP = dict(
        stock=["us_equity"],
        option=[],
        future=[],
        forex=[],
    )

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True):
        # Calling init methods
        AlpacaData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
        Broker.__init__(self, name="alpaca", connect_stream=connect_stream)
        self.market = "NASDAQ"

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
        clock = self.api.get_clock()
        curr_time = clock.timestamp.replace(tzinfo=timezone.utc).timestamp()
        return curr_time

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
        if self.market is not None:
            open_time = self.utc_to_local(self.market_hours(close=False))
            close_time = self.utc_to_local(self.market_hours(close=True))

            current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
            if self.market == "24/7":
                return True
            return (current_time >= open_time) and (close_time >= current_time)
        else:
            return self.api.get_clock().is_open

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
        clock = self.api.get_clock()
        opening_time = clock.next_open.timestamp()
        curr_time = clock.timestamp.timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

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

    def _get_balances_at_broker(self, quote_asset):
        """Get's the current actual cash, positions value, and total
        liquidation value from Alpaca.

        This method will get the current actual values from Alpaca
        for the actual cash, positions value, and total liquidation.

        Returns
        -------
        tuple of float
            (cash, positions_value, total_liquidation_value)
        """

        response = self.api.get_account()
        total_cash_value = float(response._raw["cash"])
        gross_positions_value = float(response._raw["long_market_value"]) - float(
            response._raw["short_market_value"]
        )
        net_liquidation_value = float(response._raw["portfolio_value"])

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        position = broker_position._raw
        if position["asset_class"] == "crypto":
            asset = Asset(
                symbol=position["symbol"].replace("USD", ""),
                asset_type="crypto",
            )
        else:
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

    def _parse_broker_order(self, response, strategy_name, strategy_object):
        """parse a broker order representation
        to an order object"""
        order = Order(
            strategy_name,
            Asset(
                symbol=response.symbol,
                asset_type=response.asset_class,
            ),
            Decimal(response.qty),
            response.side,
            limit_price=response.limit_price,
            stop_price=response.stop_price,
            time_in_force=response.time_in_force,
            # TODO: remove hardcoding in case Alpaca allows crypto to crypto trading
            quote=Asset(symbol="USD", asset_type="forex"),
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
            strategy_name = order.strategy
            for json_sub_order in order._raw.legs:
                sub_order = self._parse_broker_order(json_sub_order, strategy_name)
                orders.append(sub_order)

        return orders

    def _submit_order(self, order):
        """Submit an order for an asset"""

        # For Alpaca, only "gtc" and "ioc" orders are supported for crypto
        # TODO: change this if Alpaca allows new order types for crypto
        if order.asset.asset_type == "crypto":
            if order.time_in_force != "gtc" or "ioc":
                order.time_in_force = "gtc"

        kwargs = {
            "type": order.type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            "limit_price": str(order.limit_price) if order.limit_price else None,
            "stop_price": str(order.stop_price) if order.stop_price else None,
            "trail_price": str(order.trail_price) if order.trail_price else None,
            "trail_percent": order.trail_percent,
        }
        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}

        if order.take_profit_price:
            kwargs["take_profit"] = {
                "limit_price": float(round(order.take_profit_price, 2))
                if isinstance(order.take_profit_price, Decimal)
                else order.take_profit_price,
            }

        if order.stop_loss_price:
            kwargs["stop_loss"] = {
                "stop_price": float(round(order.stop_loss_price, 2))
                if isinstance(order.stop_loss_price, Decimal)
                else order.stop_loss_price,
            }
            if order.stop_loss_limit_price:
                kwargs["stop_loss"]["limit_price"] = float(
                    round(order.stop_loss_limit_price, 2)
                    if isinstance(
                        order.stop_loss_limit_price,
                        Decimal,
                    )
                    else order.stop_loss_limit_price
                )

        if order.asset.asset_type == "crypto":
            trade_symbol = order.pair.replace("/", "")
        else:
            trade_symbol = order.asset.symbol

        try:
            qty = str(order.quantity)
            response = self.api.submit_order(trade_symbol, qty, order.side, **kwargs)

            order.set_identifier(response.id)
            order.update_status(response.status)
            order.update_raw(response)

        except Exception as e:
            order.set_error(e)
            message = str(e)
            if "stop price must not be greater than base price / 1.001" in message:
                logging.info(
                    colored(
                        "%r did not go through because the share base price became lesser than the stop loss price."
                        % order,
                        color="red",
                    )
                )
            else:
                logging.info(
                    colored(
                        "%r did not go through. The following error occured: %s"
                        % (order, e),
                        color="red",
                    )
                )

        return order

    def cancel_order(self, order):
        """Cancel an order

        Parameters
        ----------
        order : Order
            The order to cancel

        Returns
        -------
        Order
            The order that was cancelled
        """
        self.api.cancel_order(order.identifier)

    # =======Account functions=========

    def get_historical_account_value(self):
        """Get the historical account value of the account."""
        response_day = self.api.get_portfolio_history(period="12M", timeframe="1D")

        response_hour = self.api.get_portfolio_history(
            period="30D", timeframe="1H", extended_hours=True
        )

        response_minute = self.api.get_portfolio_history(
            period="1D", timeframe="1Min", extended_hours=True
        )

        return {
            "minute": response_minute.df,
            "hour": response_hour.df,
            "day": response_day.df,
        }

    # =======Stream functions=========

    def _get_stream_object(self):
        """get the broker stream connection"""
        stream = tradeapi.StreamConn(self.api_key, self.api_secret, self.endpoint)
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""

        @self.stream.on(r"^trade_updates$")
        async def on_trade_event(conn, channel, data):
            self._orders_queue.join()
            try:
                logged_order = data.order
                type_event = data.event
                identifier = logged_order.get("id")
                stored_order = self.get_tracked_order(identifier)
                if stored_order is None:
                    logging.info(
                        "Untracked order %s was logged by broker %s"
                        % (identifier, self.name)
                    )
                    return False

                price = data.price if hasattr(data, "price") else None
                filled_quantity = data.qty if hasattr(data, "qty") else None
                self._process_trade_event(
                    stored_order,
                    type_event,
                    price=price,
                    filled_quantity=filled_quantity,
                )

                return True
            except:
                logging.error(traceback.format_exc())

    def _run_stream(self):
        """Overloading default alpaca_trade_api.STreamCOnnect().run()
        Run forever and block until exception is raised.
        initial_channels is the channels to start with.
        """
        self.stream.loop = asyncio.new_event_loop()
        loop = self.stream.loop
        should_renew = True  # should renew connection if it disconnects
        while should_renew:
            try:
                if loop.is_closed():
                    self.stream.loop = asyncio.new_event_loop()
                    loop = self.stream.loop
                loop.run_until_complete(self.stream.subscribe(["trade_updates"]))
                self._stream_established()
                loop.run_until_complete(self.stream.consume())
            except KeyboardInterrupt:
                logging.info("Exiting on Interrupt")
                should_renew = False
            except Exception as e:
                m = "consume cancelled" if isinstance(e, CancelledError) else e
                logging.error(f"error while consuming ws messages: {m}")
                if self.stream._debug:
                    logging.error(traceback.format_exc())
                loop.run_until_complete(self.stream.close(should_renew))
                if loop.is_running():
                    loop.close()
