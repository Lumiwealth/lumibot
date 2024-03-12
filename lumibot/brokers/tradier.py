import logging
import traceback

import pandas as pd
from lumiwealth_tradier import Tradier as _Tradier
from lumiwealth_tradier.base import TradierApiError
from termcolor import colored

from lumibot.brokers import Broker
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import create_options_symbol
from lumibot.trading_builtins import PollingStream


class Tradier(Broker):
    """
    Broker that connects to Tradier API to place orders and retrieve data. Tradier API only supports Order streaming
    for live accounts, paper trading accounts must use a 'polling' method to retrieve order updates. This class will
    still use a CustomStream object to process order updates (which can be confusing!), but this will more seamlessly
    match what other LumiBrokers are doing without requiring changes to the stategy_executor. This
    polling method will also work for Live accounts, so it will be used by default. However, future updates will be
    made to natively support websocket streaming for Live accounts.
    """

    POLL_EVENT = PollingStream.POLL_EVENT

    def __init__(
            self,
            config=None,
            account_number=None,
            access_token=None,
            paper=None,
            connect_stream=True,
            data_source=None,
            polling_interval=5.0,

            # Need sequential order submission for Tradier becuase it is very strict that buy orders exist
            # before any stoploss/limit orders.
            max_workers=1,
    ):
        # Check if the user provided both config file and keys
        if (access_token is not None or account_number is not None or paper is not None) and config is not None:
            raise Exception(
                "Please provide either a config file or access_token, account_number, and paper for Tradier. "
                "You have provided both a config file and keys so we don't know which to use."
            )

        # Check if the user has provided a config file
        if config is not None:
            # Check if the user provided all the necessary keys
            if "ACCESS_TOKEN" not in config:
                raise Exception("'ACCESS_TOKEN' not found in Tradier config")
            if "ACCOUNT_NUMBER" not in config:
                raise Exception("'ACCOUNT_NUMBER' not found in Tradier config")
            if "PAPER" not in config:
                raise Exception("'PAPER' not found in Tradier config")

            # Set the values from the config file
            access_token = config["ACCESS_TOKEN"]
            account_number = config["ACCOUNT_NUMBER"]
            paper = config["PAPER"]

        # Check if the user has provided the necessary keys
        elif access_token is None or account_number is None or paper is None:
            raise Exception("Please provide a config file or access_token, account_number, and paper")

        # Set the values from the keys
        self._tradier_access_token = access_token
        self._tradier_account_number = account_number
        self._tradier_paper = paper
        self.polling_interval = polling_interval

        self.market = "NYSE"  # The default market is NYSE.

        # Create the Tradier object
        self.tradier = _Tradier(account_number, access_token, paper)

        # Check if the user has provided a data source, if not, create one
        if data_source is None:
            data_source = TradierData(
                account_number=account_number,
                access_token=access_token,
                paper=paper,
                max_workers=max_workers,
                delay=15 if paper else 0,
            )

        super().__init__(
            name="Tradier",
            data_source=data_source,
            config=config,
            max_workers=max_workers,
            connect_stream=connect_stream,
        )

    def cancel_order(self, order: Order):
        """Cancels an order at the broker. Nothing will be done for orders that are already cancelled or filled."""
        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            raise ValueError("Order identifier is not set, unable to cancel order. Did you remember to submit it?")

        # Cancel the order
        self.tradier.orders.cancel(order.identifier)

    def _submit_order(self, order: Order):
        """
        Do checking and input sanitization, then submit the order to the broker.
        Parameters
        ----------
        order: Order
            Order to submit to the broker

        Returns
        -------
            Updated order with broker identifier filled in
        """
        tag = order.tag if order.tag else order.strategy

        # Replace non-alphanumeric characters with '-', underscore "_" is not allowed by Tradier
        tag = "".join([c if c.isalnum() or c == "-" else "-" for c in tag])

        try:
            if order.asset.asset_type == "stock":
                # Place the order
                order_response = self.tradier.orders.order(
                    order.asset.symbol,
                    order.side,
                    order.quantity,
                    order_type=order.type,
                    duration=order.time_in_force,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    tag=tag,
                )
            elif order.asset.asset_type == "option":
                tradier_side = self._lumi_side2tradier(order)
                stock_symbol = order.asset.symbol
                option_symbol = create_options_symbol(
                    order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
                )

                if not tradier_side or not option_symbol:
                    logging.error(f"Unable to parse order {order} for Tradier.")
                    return None

                order_response = self.tradier.orders.order_option(
                    stock_symbol,
                    option_symbol,
                    tradier_side,
                    order.quantity,
                    order_type=order.type,
                    duration=order.time_in_force,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    tag=tag,
                )
            else:
                raise ValueError(f"Asset type {order.asset.asset_type} not supported by Tradier.")

            order.identifier = order_response["id"]
            order.status = "submitted"
            order.update_raw(order_response)  # This marks order as 'transmitted'
            self._unprocessed_orders.append(order)
            self.stream.dispatch(self.NEW_ORDER, order=order)

        except TradierApiError as e:
            msg = colored(f"Error submitting order {order}: {e}", color="red")
            self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=msg)

        return order

    def _get_balances_at_broker(self, quote_asset: Asset):
        df = self.tradier.account.get_account_balance()

        # Get the portfolio value (total_equity) column
        portfolio_value = float(df["total_equity"].iloc[0])

        # Get the cash (total_cash) column
        cash = float(df["total_cash"].iloc[0])

        # Calculate the gross positions value
        positions_value = portfolio_value - cash

        return cash, positions_value, portfolio_value

    def get_historical_account_value(self):
        pass

    def _pull_positions(self, strategy):
        positions_df = self.tradier.account.get_positions()
        positions_ret = []

        # Loop through each row in the dataframe
        for _, row in positions_df.iterrows():
            # Get the symbol/quantity and create the position asset
            symbol = row["symbol"]
            quantity = row["quantity"]
            asset = Asset.symbol2asset(symbol)  # Parse the symbol. Handles 'stock' and 'option' types

            # Create the position
            position = Position(
                strategy=strategy.name if strategy else "Unknown",
                asset=asset,
                quantity=quantity,
            )
            positions_ret.append(position)  # Add the position to the list

        return positions_ret

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
        all_positions = self._pull_positions(strategy)

        # Loop through each position and check if it matches the asset
        for position in all_positions:
            if position.asset == asset:
                # We found the position, return it
                return position

        return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None):
        """
        Parse a broker order representation to a Lumi order object. Once the Lumi order has been created, it will
        be dispatched to our "stream" queue for processing until a time when Live Streaming can be implemented.

        Tradier API Documentation:
        https://documentation.tradier.com/brokerage-api/reference/response/orders

        :param response: The output from TradierAPI call returned _by pull_broker_order()
        :param strategy_name: The name of the strategy that placed the order
        :param strategy_object: The strategy object that placed the order
        """
        strategy_name = (
            strategy_name if strategy_name else strategy_object.name if strategy_object else response.get("tag")
        )

        # Parse the symbol
        symbol = response["symbol"]
        option_symbol = response["option_symbol"] if "option_symbol" in response and response["option_symbol"] else None

        asset = (
            Asset.symbol2asset(option_symbol)
            if option_symbol and not pd.isna(option_symbol)
            else Asset.symbol2asset(symbol)
        )

        # Create the order object
        order = Order(
            identifier=response["id"],
            strategy=strategy_name,
            asset=asset,
            side=self._tradier_side2lumi(response["side"]),
            quantity=response["quantity"],
            type=response["type"],
            time_in_force=response["duration"],
            limit_price=response["price"] if "price" in response and response["price"] else None,
            stop_price=response["stop_price"] if "stop_price" in response and response["stop_price"] else None,
            tag=response["tag"] if "tag" in response and response["tag"] else None,
            date_created=response["create_date"],
        )
        order.status = response["status"]
        order.update_raw(response)  # This marks order as 'transmitted'
        return order

    def _pull_broker_order(self, identifier):
        """
        This function pulls a single order from the broker by its identifier. Order is converted to a dictionary,
        and then returned. It is expected that the caller will convert the dictionary to an Order object by
        calling parse_broker_order() on the dictionary. Parsing the order will also dispatch it to the stream for
        processing.
        """
        orders = self.tradier.orders.get_order(identifier).to_dict("records")
        return orders[0] if len(orders) > 0 else None

    def _pull_broker_all_orders(self):
        """
        This function pulls all orders from the broker. Orders are converted to a list of dictionaries,
        and then returned. It is expected that the caller will convert each dictionary to an Order object by
        calling parse_broker_order() on the dictionary.
        """
        df = self.tradier.orders.get_orders()

        # Check if the dataframe is empty or None
        if df is None or df.empty:
            return []

        return df.to_dict("records")

    def _lumi_side2tradier(self, order: Order) -> str:
        side = order.side
        if order.asset.asset_type == "stock":
            return side

        # Convert the side to the Tradier side for options orders if necessary
        if side == "buy" or side == "sell":
            # Check if we currently own the option
            position = self.get_tracked_position(order.strategy, order.asset)

            # Check if we own the option then we need to sell to close or buy to close
            if position is not None:
                if position.quantity > 0 and side == "sell":
                    side = "sell_to_close"
                elif position.quantity >= 0 and side == "buy":
                    side = "buy_to_open"
                elif position.quantity < 0 and side == "buy":
                    side = "buy_to_close"
                elif position.quantity <= 0 and side == "sell":
                    side = "sell_to_open"
                else:
                    logging.error(
                        f"Unable to determine the correct side for the order. " f"Position: {position}, Order: {order}"
                    )

            # Otherwise, we don't own the option so we need to buy to open or sell to open
            else:
                side = "buy_to_open" if side == "buy" else "sell_to_open"

        # Stoploss and limit orders are always used to close positions, even if they are submitted "before" the
        # position is technically open (i.e. buy and stoploss order are submitted simultaneously)
        if order.type in [Order.OrderType.STOP, Order.OrderType.LIMIT, Order.OrderType.TRAIL]:
            side = side.replace("to_open", "to_close")

        # Check if the side is a valid Tradier side
        if side not in ["buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"]:
            logging.error(f"Invalid option order side for Tradier: {order.side}")
            return ""

        return side

    @staticmethod
    def _tradier_side2lumi(side):
        """
        Converts a Tradier side to a Lumi side.
        Valid Stock Sides: buy, buy_to_cover, sell, sell_short
        Valid Option Sides: buy_to_open, buy_to_close, sell_to_open, sell_to_close
        """
        if "buy" in side:
            return "buy"
        elif "sell" in side:
            return "sell"
        else:
            raise ValueError(f"Invalid side {side} for Tradier.")

    # ==========Processing streams data=======================

    def do_polling(self):
        """
        This function is called every time the broker polls for new orders. It checks for new orders and
        dispatches them to the stream for processing.
        """
        # Pull the current Tradier positions and sync them with Lumibot's positions
        self.sync_positions(None)

        # Get current orders from Tradier and dispatch them to the stream for processing. Need to see all
        # lumi orders (not just active "tracked" ones) to catch any orders that might have changed final
        # status in Tradier.
        # df_orders = self.tradier.orders.get_orders()
        raw_orders = self._pull_broker_all_orders()
        stored_orders = {x.identifier: x for x in self.get_all_orders()}
        for order_row in raw_orders:
            order = self._parse_broker_order(order_row, strategy_name=order_row.get("tag"))

            # First time seeing this order, something weird has happened, dispatch it as a new order
            if order.identifier not in stored_orders:
                logging.info(
                    f"Poll Update: {self.name} has order {order}, but Lumibot doesn't know about it. "
                    f"Adding it as a new order."
                )
                # If the Tradier status is not "open", the next polling cycle will catch it and dispatch it as needed.
                self.stream.dispatch(self.NEW_ORDER, order=order)
            else:
                stored_order = stored_orders[order.identifier]
                stored_order.quantity = order.quantity  # Update the quantity in case it has changed

                # Status has changed since last time we saw it, dispatch the new status.
                #  - Polling methods are unable to track partial fills
                #     - Partial fills often happen quickly and it is highly likely that polling will miss some of them
                #     - Additionally, Lumi Order objects don't have a way to track quantity status changes and
                #        adjusting the average sell price can be tricky
                #     - Only dispatch filled orders if they are completely filled.
                if not order.equivalent_status(stored_order):
                    match order.status.lower():
                        case "submitted" | "open":
                            self.stream.dispatch(self.NEW_ORDER, order=stored_order)
                        case "partial_filled":
                            # Not handled for polling, only dispatch completely filled orders
                            pass
                        case "fill":
                            fill_price = order_row["avg_fill_price"]
                            fill_qty = order_row["exec_quantity"] if "exec_quantity" in order_row else order.quantity
                            self.stream.dispatch(
                                self.FILLED_ORDER, order=stored_order, price=fill_price, filled_quantity=fill_qty
                            )
                        case "canceled":
                            self.stream.dispatch(self.CANCELED_ORDER, order=stored_order)
                        case "error":
                            default_msg = f"{self.name} encountered an error with order {order.identifier} | {order}"
                            msg = order_row["reason_description"] if "reason_description" in order_row else default_msg
                            self.stream.dispatch(self.ERROR_ORDER, order=stored_order, error_msg=msg)
                        case "cash_settled":
                            # Don't know how to detect this case in Tradier.
                            # Reference: https://documentation.tradier.com/brokerage-api/reference/response/orders
                            # Theory:
                            #  - Tradier will auto settle and create a new fill order for cash settled orders. Needs
                            #    testing to confirm.
                            pass
                else:
                    # Status hasn't changed, but make sure we use the broker's status.
                    # I.e. 'submitted' becomes 'open'
                    stored_order.status = order.status

        # See if there are any tracked (aka active) orders that are no longer in the broker's list,
        # dispatch them as cancelled
        tracked_orders = {x.identifier: x for x in self.get_tracked_orders()}
        broker_ids = [o["id"] for o in raw_orders]
        for order_id, order in tracked_orders.items():
            if order_id not in broker_ids:
                logging.info(
                    f"Poll Update: {self.name} no longer has order {order}, but Lumibot does. " 
                    f"Dispatching as cancelled."
                )
                self.stream.dispatch(self.CANCELED_ORDER, order=order)

    def _get_stream_object(self):
        """get the broker stream connection"""
        stream = PollingStream(self.polling_interval)
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        broker = self

        @broker.stream.add_action(broker.POLL_EVENT)
        def on_trade_event_poll():
            self.do_polling()

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_trade_event_new(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.NEW_ORDER,
                )
                return True
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
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
        def on_trade_event_cancel(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.CANCELED_ORDER,
                )
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.CASH_SETTLED)
        def on_trade_event_cash(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order,
                    broker.CASH_SETTLED,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_trade_event_error(order, error_msg):
            try:
                if order.is_active():
                    broker._process_trade_event(
                        order,
                        broker.CANCELED_ORDER,
                    )
                logging.error(error_msg)
                order.set_error(error_msg)
            except:
                logging.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        self.stream._run()

    def _flatten_order(self, order):
        """Some submitted orders may trigger other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]

        # TODO: Need to implement this for Tradier

        return orders
