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
from alpaca.trading.requests import ReplaceOrderRequest
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest

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
        crypto=["crypto", "CRYPTO"],  # Added support for crypto asset class names
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
        # Make mapping case-insensitive for robustness
        alpaca_type_lower = alpaca_type.lower() if isinstance(alpaca_type, str) else alpaca_type
        for k, v in self.ASSET_TYPE_MAP.items():
            if alpaca_type_lower in [x.lower() for x in v]:
                return k
        raise ValueError(f"The type {alpaca_type} is not in the ASSET_TYPE_MAP in the Alpaca Module.")

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        # Handle missing symbol and fallback to raw if possible
        if isinstance(response, dict):
            resp_raw = response
        elif hasattr(response, '_raw') and isinstance(response._raw, dict):
            resp_raw = response._raw
        else:
            resp_raw = {}

        # Primary symbol from attribute or raw JSON
        resp_symbol = getattr(response, 'symbol', None) or resp_raw.get('symbol')

        # Fallback: for multi-leg parent orders, use the first leg's symbol
        if resp_symbol is None:
            first_leg_symbol = None
            legs = resp_raw.get('legs') if isinstance(resp_raw, dict) else None
            if legs is None and hasattr(response, 'legs'):
                legs = getattr(response, 'legs')
            if isinstance(legs, list) and legs:
                first_leg = legs[0]
                if isinstance(first_leg, dict):
                    first_leg_symbol = first_leg.get('symbol')
                else:
                    first_leg_symbol = getattr(first_leg, 'symbol', None)
            if first_leg_symbol:
                resp_symbol = first_leg_symbol
            else:
                raise ValueError(f"Order symbol is missing in response for order id {getattr(response, 'id', None)}")

        # Parse crypto symbol format
        if "/" in resp_symbol:
            symbol = resp_symbol.split("/")[0]
        else:
            symbol = resp_symbol

        # Retrieve order fields, falling back to raw JSON for multi-leg legs
        if isinstance(response, dict):
            resp_raw = response
        elif hasattr(response, '_raw') and isinstance(response._raw, dict):
            resp_raw = response._raw
        else:
            resp_raw = {}

        # Asset class for mapping
        asset_class_value = getattr(response, 'asset_class', None) or resp_raw.get('asset_class')
        # Fallback: try to get asset_class from first leg if missing
        if asset_class_value is None:
            legs = resp_raw.get('legs') if isinstance(resp_raw, dict) else None
            if legs is None and hasattr(response, 'legs'):
                legs = getattr(response, 'legs')
            if isinstance(legs, list) and legs:
                first_leg = legs[0]
                if isinstance(first_leg, dict):
                    asset_class_value = first_leg.get('asset_class')
                else:
                    asset_class_value = getattr(first_leg, 'asset_class', None)
        # Quantity and side
        qty_value = getattr(response, 'qty', None) or resp_raw.get('qty')
        side_value = getattr(response, 'side', None) or resp_raw.get('side')

        # Determine order and class types
        order_type_value = getattr(response, 'order_type', None) or resp_raw.get('type')
        order_class_raw = getattr(response, 'order_class', None) or resp_raw.get('order_class')
        order_class_value = order_class_raw if order_class_raw != "mleg" else Order.OrderClass.MULTILEG

        # Prices and limits
        limit_price_value = getattr(response, 'limit_price', None) or resp_raw.get('limit_price')
        stop_price_value = getattr(response, 'stop_price', None) or resp_raw.get('stop_price')
        trail_price_value = getattr(response, 'trail_price', None) or resp_raw.get('trail_price')
        trail_percent_value = getattr(response, 'trail_percent', None) or resp_raw.get('trail_percent')
        stop_limit_price = limit_price_value if order_type_value == Order.OrderType.STOP_LIMIT or order_type_value == "stop_limit" else None

        # Time in force and status
        time_in_force_value = getattr(response, 'time_in_force', None) or resp_raw.get('time_in_force')
        status_value = getattr(response, 'status', None) or resp_raw.get('status')

        # Identifier
        identifier_value = getattr(response, 'id', None) or resp_raw.get('id')

        # Construct Order object
        order = Order(
            strategy_name,
            Asset(
                symbol=symbol,
                asset_type=self.map_asset_type(asset_class_value),
            ),
            Decimal(qty_value),
            side_value,
            limit_price=limit_price_value if order_type_value != Order.OrderType.STOP_LIMIT else None,
            stop_price=stop_price_value,
            stop_limit_price=stop_limit_price,
            trail_price=trail_price_value if trail_price_value else None,
            trail_percent=trail_percent_value if trail_percent_value else None,
            time_in_force=time_in_force_value,
            order_class=order_class_value,
            order_type=order_type_value if order_type_value != "trailing_stop" else Order.OrderType.TRAIL,
            # TODO: remove hardcoding in case Alpaca allows crypto to crypto trading
            quote=Asset(symbol="USD", asset_type="forex"),
        )
        order.set_identifier(identifier_value)
        order.status = status_value
        order.update_raw(response)
        return order

    def _pull_broker_order(self, identifier):
        """Get a broker order representation by its id"""
        response = self.api.get_order_by_id(identifier)
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

    def _submit_orders(self, orders, is_multileg=False, order_type=None, duration="day", price=None):
        """
        Submit multiple orders to the broker. Supports multi-leg (MLeg) orders for options.
        """
        if not orders or len(orders) == 0:
            return

        if is_multileg:
            tag = orders[0].tag if hasattr(orders[0], "tag") and orders[0].tag else orders[0].strategy
            parent_order = self._submit_multileg_order(orders, order_type, duration, price, tag)
            return [parent_order]
        else:
            sub_orders = []
            for order in orders:
                sub_orders.append(self._submit_order(order))
            return sub_orders

    def _submit_multileg_order(self, orders, order_type="limit", duration="day", price=None, tag=None):
        """
        Submit a multi-leg (MLeg) options order to Alpaca.

        Note:
        - Tradier uses "credit" for net credit (receive premium) and "debit" for net debit (pay premium).
        - Alpaca only supports "market" and "limit" for multi-leg orders.
        - We convert "credit" and "debit" to "limit" for Alpaca, as both are limit orders in Alpaca's API.
        - The sign of the limit price (positive/negative) is not used by Alpaca to distinguish credit/debit.
        - Alpaca requires that the leg ratio quantities are relatively prime (GCD == 1).
        """
        # Convert Tradier-specific order types to Alpaca-supported types
        if order_type in ("credit", "debit"):
            order_type = "limit"
        # All legs must have the same underlying symbol
        symbol = orders[0].asset.symbol
        qty = str(orders[0].quantity)
        # Compose legs
        legs = []
        leg_quantities = []
        for order in orders:
            # Format option symbol
            if order.asset.asset_type == Asset.AssetType.OPTION:
                strike_formatted = f"{order.asset.strike:08.3f}".replace('.', '').rjust(8, '0')
                date = order.asset.expiration.strftime("%y%m%d")
                option_symbol = f"{order.asset.symbol}{date}{order.asset.right[0]}{strike_formatted}"
            else:
                option_symbol = order.asset.symbol
            # Determine position_intent (buy_to_open, sell_to_open, etc.)
            position_intent = getattr(order, "position_intent", None)
            if not position_intent:
                # Check if we have an open position in this option
                pos = self.get_tracked_position(order.strategy, order.asset)
                if pos is not None and pos.quantity != 0:
                    # Closing position
                    if order.side == "buy":
                        position_intent = "buy_to_close"
                    elif order.side == "sell":
                        position_intent = "sell_to_close"
                else:
                    # Opening position
                    if order.side == "buy":
                        position_intent = "buy_to_open"
                    elif order.side == "sell":
                        position_intent = "sell_to_open"
            # Collect leg quantities for GCD check
            leg_qty = int(abs(order.quantity))
            leg_quantities.append(leg_qty)
            legs.append({
                "symbol": option_symbol,
                "ratio_qty": str(order.quantity),
                "side": order.side,
                "position_intent": position_intent
            })
        # Ensure leg ratio quantities are relatively prime (GCD == 1)
        from math import gcd
        from functools import reduce
        if len(leg_quantities) > 1:
            leg_gcd = reduce(gcd, leg_quantities)
            if leg_gcd > 1:
                # Divide all ratio_qty by GCD to make them relatively prime
                for i, leg in enumerate(legs):
                    orig_qty = int(leg["ratio_qty"])
                    new_qty = int(orig_qty // leg_gcd)
                    leg["ratio_qty"] = str(new_qty)
                qty = str(int(qty) // leg_gcd)
        # Compose order payload
        kwargs = {
            "order_class": "mleg",
            "qty": qty,
            "type": order_type or "limit",
            "time_in_force": duration,
            "legs": legs,
        }
        # For limit/credit/debit orders, price is required
        if (order_type in ["limit", "credit", "debit", None]) and price is None:
            raise ValueError("limit price is required for limit orders (multi-leg) on Alpaca.")
        if price is not None:
            # Ensure limit price is at most 2 decimal places (Alpaca requirement)
            limit_price = round(float(price), 2)
            kwargs["limit_price"] = limit_price
        # Submit order
        try:
            response = self.api.submit_order(order_data=OrderData(**kwargs))
            parent_asset = Asset(symbol=symbol)
            parent_order = Order(
                identifier=response.id,
                asset=parent_asset,
                strategy=orders[0].strategy,
                order_class=Order.OrderClass.MULTILEG,
                side=orders[0].side,
                quantity=orders[0].quantity,
                order_type=orders[0].order_type,
                time_in_force=duration,
                limit_price=price,
                tag=tag,
                status=Order.OrderStatus.SUBMITTED
            )
            for o in orders:
                o.parent_identifier = parent_order.identifier
            parent_order.child_orders = orders
            parent_order.update_raw(response)
            self._unprocessed_orders.append(parent_order)
            return parent_order
        except Exception as e:
            for o in orders:
                o.set_error(e)
            raise

    def _submit_order(self, order):
        """Submit an order for an asset (single-leg, including options)"""

        # For Alpaca, only "gtc" and "ioc" orders are supported for crypto
        # TODO: change this if Alpaca allows new order types for crypto
        if order.asset.asset_type == Asset.AssetType.CRYPTO:
            if order.time_in_force != "gtc" or "ioc":
                order.time_in_force = "gtc"
        # For Alpaca, only "day" is supported for option orders
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            order.time_in_force = "day"

        qty = str(order.quantity)

        # Compose symbol for option
        if order.asset.asset_type == Asset.AssetType.OPTION:
            strike_formatted = f"{order.asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = order.asset.expiration.strftime("%y%m%d")
            trade_symbol = f"{order.asset.symbol}{date}{order.asset.right[0]}{strike_formatted}"
        elif order.asset.asset_type == Asset.AssetType.CRYPTO:
            trade_symbol = f"{order.asset.symbol}/{order.quote.symbol}"
        else:
            trade_symbol = order.asset.symbol

        # If order class is OCO, set to type limit (Alpaca wants this for OCO), Bracket becomes 'market'
        alpaca_type = order.order_type
        if order.order_class == Order.OrderClass.OCO:
            alpaca_type = Order.OrderType.LIMIT
        elif order.order_class in [Order.OrderClass.BRACKET, Order.OrderClass.OTO]:
            alpaca_type = Order.OrderType.MARKET

        # Validate stop-limit orders must have both stop and limit prices
        if order.order_type == Order.OrderType.STOP_LIMIT and (order.stop_price is None or order.stop_limit_price is None):
            raise ValueError("stop limit orders require both stop and limit price")

        # Determine raw prices
        raw_limit = order.limit_price if order.order_type != Order.OrderType.STOP_LIMIT else order.stop_limit_price
        raw_stop = order.stop_price
        raw_trail = order.trail_price
        raw_trail_pct = order.trail_percent

        # Helper to round according to asset type
        def _round_price(val: float) -> float:
            if order.asset.asset_type == Asset.AssetType.STOCK:
                return round(val, 2) if val >= 1.0 else round(val, 4)
            if order.asset.asset_type == Asset.AssetType.CRYPTO:
                return round(val, 9)
            # options and others: use 3 decimals by default
            return round(val, 3)

        # Apply rounding
        limit_price = _round_price(float(raw_limit)) if raw_limit is not None else None
        stop_price = _round_price(float(raw_stop)) if raw_stop is not None else None
        trail_price = _round_price(float(raw_trail)) if raw_trail is not None else None
        trail_percent = _round_price(float(raw_trail_pct)) if raw_trail_pct is not None else None

        # Map extended side values to simple buy/sell for Alpaca API
        side = order.side
        if side in ("buy_to_open", "buy_to_close"):
            side = "buy"
        elif side in ("sell_to_open", "sell_to_close"):
            side = "sell"

        # Build kwargs with rounded values and mapped side
        kwargs = {
            "symbol": trade_symbol,
            "qty": str(order.quantity),
            "side": side, # Use the mapped side value
            "type": alpaca_type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            # Crypto can use 9 decimal places on Alpaca
            "limit_price": str(limit_price) if limit_price is not None else None,
            "stop_price": str(stop_price) if stop_price is not None else None,
            "trail_price": str(trail_price) if trail_price is not None else None,
            "trail_percent": str(trail_percent) if trail_percent is not None else None,
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
            # Conform stop/stop_limit prices for stocks to Alpaca's minimum increment (0.01)
            if order.asset.asset_type == Asset.AssetType.STOCK:
                # Conform stop_price
                if order.is_stop_order() and order.stop_price is not None:
                    orig_stop = order.stop_price
                    conformed = False
                    if has_more_than_n_decimal_places(order.stop_price, 2):
                        order.stop_price = round(order.stop_price, 2)
                        conformed = True
                    if conformed:
                        logger.warning(
                            f"Order {order} was changed to conform to Alpaca's requirements. "
                            f"The stop price was changed from {orig_stop} to {order.stop_price}."
                        )
                    # Update kwargs for stop_price
                    if "stop_price" in kwargs:
                        kwargs["stop_price"] = str(order.stop_price)
                # Conform stop_limit_price
                if order.order_type == Order.OrderType.STOP_LIMIT and order.stop_limit_price is not None:
                    orig_stop_limit = order.stop_limit_price
                    conformed = False
                    if has_more_than_n_decimal_places(order.stop_limit_price, 2):
                        order.stop_limit_price = round(order.stop_limit_price, 2)
                        conformed = True
                    if conformed:
                        logger.warning(
                            f"Order {order} was changed to conform to Alpaca's requirements. "
                            f"The stop limit price was changed from {orig_stop_limit} to {order.stop_limit_price}."
                        )
                    # Update kwargs for stop_loss.limit_price if present
                    if "stop_loss" in kwargs and "limit_price" in kwargs["stop_loss"]:
                        kwargs["stop_loss"]["limit_price"] = float(order.stop_limit_price)

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
            elif "sub-penny increment does not fulfill minimum pricing criteria" in message:
                logger.error(
                    colored(
                        f"{order} did not go through because the stop price or stop limit price does not conform to Alpaca's minimum increment (0.01).",
                        color="red",
                    )
                )
            elif "stop limit orders require both stop and limit price" in message:
                logger.error(
                    colored(
                        f"{order} did not go through because both stop and limit price must be set for stop limit orders.",
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


    def _modify_order(
            self,
            order: Order,
            limit_price: float | None = None,
            stop_price: float | None = None,
        ):
            """
            Modify an existing Alpaca order via TradingClient.
            No‑ops if already filled or canceled. Only limit and/or stop price
            can be changed—quantity changes still require cancel + new submit.
            """
            # Must have been submitted
            if not order.identifier:
                raise ValueError(
                    "Order identifier is missing; cannot modify. Did you submit the order?"
                )

            # Fetch latest order status from Alpaca
            try:
                latest_order = self.api.get_order_by_id(order.identifier)
                latest_status = getattr(latest_order, "status", None)
            except Exception as e:
                logger.error(f"Could not fetch latest order status from Alpaca: {e}")
                return

            # Skip if done
            if str(latest_status).lower() in ("filled", "canceled", "canceled_by_user"):
                return

            # Gather only provided fields
            update_kwargs: dict[str, float] = {}
            if limit_price is not None:
                update_kwargs["limit_price"] = limit_price
            if stop_price is not None:
                update_kwargs["stop_price"] = stop_price

            # Nothing to do?
            if not update_kwargs:
                return

            # Build the replace request
            replace_req = ReplaceOrderRequest(**update_kwargs)

            # Try to replace the order on Alpaca, handle APIError for accepted status
            try:
                self.api.replace_order_by_id(
                    order_id=order.identifier,
                    order_data=replace_req,
                )
            except Exception as e:
                # If error is "cannot replace order in accepted status", just log and skip
                if hasattr(e, "args") and e.args and "cannot replace order in accepted status" in str(e.args[0]):
                    logger.info(f"Order {order.identifier} cannot be modified because it is still in 'accepted' status (Alpaca).")
                    return
                else:
                    raise

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

    def get_quote(self, asset: Asset, quote: Asset = None):
        """
        Get the latest quote for an asset (stock, option, or crypto).
        Returns a dictionary with bid, ask, last, and other fields if available.
        """
        return self.data_source.get_quote(asset, quote)