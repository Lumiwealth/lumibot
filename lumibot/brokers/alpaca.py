import asyncio
import datetime
import logging
import traceback
from asyncio import CancelledError
from datetime import timezone
from decimal import Decimal
from typing import Union

import pandas_market_calendars as mcal
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from dateutil import tz
from termcolor import colored

from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import has_more_than_n_decimal_places

from .broker import Broker

logger = logging.getLogger(__name__)


# Create our own OrderData class to pass to the API because this is easier to work with
# than the ones Alpaca provides, and because the new classes are missing bracket orders
class OrderData:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_request_fields(self):
        return self.__dict__


class Alpaca(Broker):
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
    >>> ALPACA_CONFIG = {
    ...     # Put your own Alpaca key here:
    ...     "API_KEY": "YOUR_API_KEY",
    ...     # Put your own Alpaca secret here:
    ...     "API_SECRET": "YOUR_API_SECRET",
    ...     # Set this to False to use a live account
    ...     "PAPER": True
    ... }
    >>> alpaca = Alpaca(ALPACA_CONFIG)
    >>> print(alpaca.get_time_to_open())
    >>> print(alpaca.get_time_to_close())
    >>> print(alpaca.is_market_open())

    >>> # Run a strategy on Alpaca
    >>> from lumibot.strategies import Strategy
    >>> from lumibot.brokers import Alpaca
    >>> from lumibot.traders import Trader
    >>>
    >>> ALPACA_CONFIG = {
    ...     # Put your own Alpaca key here:
    ...     "API_KEY": "YOUR_API_KEY",
    ...     # Put your own Alpaca secret here:
    ...     "API_SECRET": "YOUR_API_SECRET",
    ...     # Set this to False to use a live account
    ...     "PAPER": True
    ... }
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
    >>> alpaca = Alpaca(ALPACA_CONFIG)
    >>> strategy = AlpacaStrategy(broker=alpaca)
    >>> trader = Trader()
    >>> trader.add_strategy(strategy)
    >>> trader.run()

    """

    ASSET_TYPE_MAP = dict(
        stock=["us_equity"],
        option=["us_option"],
        future=[],
        forex=[],
    )

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True, data_source=None):
        # Calling init methods
        self.market = "NASDAQ"
        self.api_key = ""
        self.api_secret = ""
        self.is_paper = False

        # Set the config values
        self._update_attributes_from_config(config)

        if not data_source:
            data_source = AlpacaData(config, max_workers=max_workers, chunk_size=chunk_size)
        super().__init__(
            name="alpaca",
            connect_stream=connect_stream,
            data_source=data_source,
            config=config,
            max_workers=max_workers,
        )

        self.api = TradingClient(self.api_key, self.api_secret, paper=self.is_paper)

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
            if self.market == "24/7":
                return True

            open_time = self.utc_to_local(self.market_hours(close=False))
            close_time = self.utc_to_local(self.market_hours(close=True))
            current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())

            # Check if it is a holiday or weekend using pandas_market_calendars
            nyse = mcal.get_calendar("NYSE")
            schedule = nyse.schedule(start_date=open_time, end_date=close_time)
            if schedule.empty:
                return False

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

    def _get_balances_at_broker(self, quote_asset, strategy):
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
        total_cash_value = float(response.cash)
        gross_positions_value = float(response.long_market_value) - float(response.short_market_value)
        net_liquidation_value = float(response.portfolio_value)

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        position = broker_position
        if position.asset_class == "crypto":
            asset = Asset(
                symbol=position.symbol.replace("USD", ""),
                asset_type=Asset.AssetType.CRYPTO,
            )
        elif position.asset_class == "option":
            asset = Asset(
                symbol=position.symbol,
                asset_type=Asset.AssetType.OPTION,
            )
        else:
            asset = Asset(
                symbol=position.symbol,
            )

        quantity = position.qty
        position = Position(strategy, asset, quantity, orders=orders)
        return position

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
        response = self.api.get_position(asset)
        return response

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        response = self.api.get_all_positions()
        return response

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

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

    # =======Orders and assets functions=========
    def map_asset_type(self, alpaca_type):
        for k, v in self.ASSET_TYPE_MAP.items():
            if alpaca_type in v:
                return k
        raise ValueError(f"The type {alpaca_type} is not in the ASSET_TYPE_MAP in the Alpaca Module.")

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""

        # If the symbol includes a slash, then it is a crypto order and only the first part of
        # the symbol is the real symbol
        if "/" in response.symbol:
            symbol = response.symbol.split("/")[0]
        else:
            symbol = response.symbol

        # Alpaca Order type/class mostly matches LumiBot's Order type/class with exceptons of 'mleg' and 'trailing_stop'
        limit_price = response.limit_price if response.order_type != Order.OrderType.STOP_LIMIT else None
        stop_limit_price = response.limit_price if response.order_type == Order.OrderType.STOP_LIMIT else None
        order_class = response.order_class if response.order_class != "mleg" else Order.OrderClass.MULTILEG
        order_type = response.order_type if response.order_type != "trailing_stop" else Order.OrderType.TRAIL
        order = Order(
            strategy_name,
            Asset(
                symbol=symbol,
                asset_type=self.map_asset_type(response.asset_class),
            ),
            Decimal(response.qty),
            response.side,
            limit_price=limit_price,  # order.py always converts to 'float'. Crypto issue?
            stop_price=response.stop_price,
            stop_limit_price=stop_limit_price,
            trail_price=response.trail_price if response.trail_price else None,
            trail_percent=response.trail_percent if response.trail_percent else None,
            time_in_force=response.time_in_force,
            order_class=order_class,
            order_type=order_type,

            # TODO: remove hardcoding in case Alpaca allows crypto to crypto trading
            quote=Asset(symbol="USD", asset_type="forex"),
        )
        order.set_identifier(response.id)
        order.status = response.status
        order.update_raw(response)
        return order

    def _pull_broker_order(self, identifier):
        """Get a broker order representation by its id"""
        response = self.api.get_order(identifier)
        return response

    def _pull_broker_all_orders(self):
        """Get the broker orders"""
        return self.api.get_orders()

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
        if order.asset.asset_type == Asset.AssetType.CRYPTO:
            if order.time_in_force != "gtc" or "ioc":
                order.time_in_force = "gtc"
        # For Alpaca, only "day" is supported for option orders
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            order.time_in_force = "day"

        qty = str(order.quantity)

        if order.asset.asset_type == Asset.AssetType.CRYPTO:
            trade_symbol = f"{order.asset.symbol}/{order.quote.symbol}"
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            strike_formatted = f"{order.asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = order.asset.expiration.strftime("%y%m%d")
            trade_symbol = f"{order.asset.symbol}{date}{order.asset.right[0]}{strike_formatted}"
        else:
            trade_symbol = order.asset.symbol

        # If order class is OCO, set to type limit (Alpaca wants this for OCO), Bracket becomes 'market'
        alpaca_type = order.order_type
        if order.order_class == Order.OrderClass.OCO:
            alpaca_type = Order.OrderType.LIMIT
        elif order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
            alpaca_type = Order.OrderType.MARKET

        limit_price = order.limit_price if order.order_type != Order.OrderType.STOP_LIMIT else order.stop_limit_price
        kwargs = {
            "symbol": trade_symbol,
            "qty": qty,
            "side": order.side,
            "type": alpaca_type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            # Crypto can use 9 decimal places on Alpaca
            "limit_price": str(limit_price) if limit_price else None,
            "stop_price": str(order.stop_price) if order.stop_price else None,
            "trail_price": str(order.trail_price) if order.trail_price else None,
            "trail_percent": str(order.trail_percent) if order.trail_percent else None,
        }
        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}

        if order.order_class in [Order.OrderClass.OCO, Order.OrderClass.OTO, Order.OrderClass.BRACKET]:
            child_limit_orders = [child for child in order.child_orders if child.order_type == Order.OrderType.LIMIT]
            child_stop_orders = [child for child in order.child_orders if child.is_stop_order()]
            child_limit = child_limit_orders[0] if child_limit_orders else None
            child_stop = child_stop_orders[0] if child_stop_orders else None
            if child_limit:
                kwargs["take_profit"] = {
                    "limit_price": float(round(child_limit.limit_price, 2))
                    if isinstance(child_limit.limit_price, Decimal) else child_limit.limit_price,
                }

            if child_stop:
                kwargs["stop_loss"] = {
                    "stop_price": float(round(child_stop.stop_price, 2))
                    if isinstance(child_stop.stop_price, Decimal) else child_stop.stop_price,
                }
                if child_stop.stop_limit_price:
                    kwargs["stop_loss"]["limit_price"] = float(
                        round(child_stop.stop_limit_price, 2)
                        if isinstance(child_stop.stop_limit_price, Decimal) else child_stop.stop_limit_price
                    )

        try:
            order_data = OrderData(**kwargs)
            response = self.api.submit_order(order_data=order_data)

            order.set_identifier(response.id)
            order.status = response.status
            order.update_raw(response)
            self._unprocessed_orders.append(order)

        except Exception as e:
            order.set_error(e)
            message = str(e)
            if "stop price must not be greater than base price / 1.001" in message:
                logger.error(
                    colored(
                        f"{order} did not go through because the share base price became lesser than the stop loss price.",
                        color="red",
                    )
                )
            else:
                logger.error(
                    colored(
                        f"{order} did not go through. The following error occurred: {e}",
                        color="red",
                    )
                )

        return order

    def _conform_order(self, order):
        """Conform an order to Alpaca's requirements
        See: https://docs.alpaca.markets/docs/orders-at-alpaca
        """
        if order.asset.asset_type == Asset.AssetType.STOCK and order.order_type == Order.OrderType.LIMIT:
            """
            The minimum price variance exists for limit orders.
            Orders received in excess of the minimum price variance will be rejected.
            Limit price >=$1.00: Max Decimals = 2
            Limit price <$1.00: Max Decimals = 4
            """
            orig_price = order.limit_price
            conformed = False
            if order.limit_price >= 1.0 and has_more_than_n_decimal_places(order.limit_price, 2):
                    order.limit_price = round(order.limit_price, 2)
                    conformed = True
            elif order.limit_price < 1.0 and has_more_than_n_decimal_places(order.limit_price, 4):
                order.limit_price = round(order.limit_price, 4)
                conformed = True

            if conformed:
                logger.warning(
                    f"Order {order} was changed to conform to Alpaca's requirements. "
                    f"The limit price was changed from {orig_price} to {order.limit_price}."
                )

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
        self.api.cancel_order_by_id(order.identifier)

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        """
        raise NotImplementedError("AlpacaBroker modify order is not implemented.")

    # =======Account functions=========

    def get_historical_account_value(self):
        """Get the historical account value of the account."""
        response_day = self.api.get_portfolio_history(period="12M", timeframe="1D")

        response_hour = self.api.get_portfolio_history(period="30D", timeframe="1H", extended_hours=True)

        response_minute = self.api.get_portfolio_history(period="1D", timeframe="1Min", extended_hours=True)

        return {
            "minute": response_minute.df,
            "hour": response_hour.df,
            "day": response_day.df,
        }

    # =======Stream functions=========

    def _get_stream_object(self):
        """
        Get the broker stream connection
        """
        stream = TradingStream(self.api_key, self.api_secret, paper=self.is_paper)
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        pass

    def _run_stream(self):
        """Overloading default alpaca_trade_api.STreamCOnnect().run()
        Run forever and block until exception is raised.
        initial_channels is the channels to start with.
        """

        async def _trade_update(trade_update):
            try:
                logged_order = trade_update.order
                type_event = trade_update.event
                identifier = logged_order.id
                stored_order = self.get_tracked_order(identifier)
                if stored_order is None:
                    logger.debug(f"Untracked order {identifier} was logged by broker {self.name}")
                    return False

                price = trade_update.price
                filled_quantity = trade_update.qty
                self._process_trade_event(
                    stored_order,
                    type_event,
                    price=price,
                    filled_quantity=filled_quantity,
                )

                return True
            except ValueError:
                logger.error(traceback.format_exc())

        self.stream.loop = asyncio.new_event_loop()
        loop = self.stream.loop
        should_renew = True  # should renew connection if it disconnects
        while should_renew:
            try:
                if loop.is_closed():
                    self.stream.loop = asyncio.new_event_loop()
                    loop = self.stream.loop
                self.stream.subscribe_trade_updates(_trade_update)
                self._stream_established()
                loop.run_until_complete(self.stream.run())
            except KeyboardInterrupt:
                logger.info("Exiting on Interrupt")
                should_renew = False
            except Exception as e:
                m = "consume cancelled" if isinstance(e, CancelledError) else e
                logger.error(f"error while consuming ws messages: {m}")
                logger.error(traceback.format_exc())
                loop.run_until_complete(self.stream.close(should_renew))
                if loop.is_running():
                    loop.close()
