import logging

from lumiwealth_tradier import Tradier as _Tradier

from lumibot.brokers import Broker
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import create_options_symbol, parse_symbol


class Tradier(Broker):
    """
    Broker that connects to Tradier API to place orders and retrieve data
    """

    def __init__(
        self,
        config=None,
        account_number=None,
        access_token=None,
        paper=None,
        max_workers=20,
        connect_stream=True,
        data_source=None,
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
            )

        super().__init__(
            name="Tradier",
            data_source=data_source,
            config=config,
            max_workers=max_workers,
            connect_stream=connect_stream,
        )

    def validate_credentials(self):
        pass

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
            side = order.side

            # Convert the side to the Tradier side for options orders if necessary
            if side == "buy" or side == "sell":
                # Check if we currently own the option
                position = self._pull_position(None, order.asset)

                # Check if we own the option then we need to sell to close or buy to close
                if position is not None:
                    if position.quantity > 0 and side == "sell":
                        side = "sell_to_close"
                    elif position.quantity > 0 and side == "buy":
                        side = "buy_to_open"
                    elif position.quantity < 0 and side == "buy":
                        side = "buy_to_close"
                    elif position.quantity < 0 and side == "sell":
                        side = "sell_to_open"
                    else:
                        logging.error(
                            f"Unable to determine the correct side for the order. "
                            f"Position: {position}, Order: {order}"
                        )
                        return None

                # Otherwise, we don't own the option so we need to buy to open or sell to open
                else:
                    if side == "buy":
                        side = "buy_to_open"
                    elif side == "sell":
                        side = "sell_to_open"
                    else:
                        logging.error(
                            f"Unable to determine the correct side for the order. "
                            f"Position: {position}, Order: {order}"
                        )
                        return None

            # Check if the sie is a valid Tradier side
            if side not in ["buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"]:
                logging.error(f"Invalid order side for Tradier: {side}")
                return None

            option_symbol = create_options_symbol(
                order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
            )

            symbol_data = parse_symbol(option_symbol)
            stock_symbol = symbol_data["stock_symbol"]
            order_response = self.tradier.orders.order_option(
                stock_symbol,
                option_symbol,
                side,
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

    def _get_stream_object(self):
        pass

    def _register_stream_events(self):
        pass

    def _run_stream(self):
        pass

    def _pull_positions(self, strategy):
        positions_df = self.tradier.account.get_positions()

        positions_ret = []
        # Loop through each row in the dataframe
        for _, row in positions_df.iterrows():
            # Get the symbol
            symbol = row["symbol"]

            # Get the quantity
            quantity = row["quantity"]

            # Parse the symbol
            asset_dict = parse_symbol(symbol)

            # Check if the asset is an option
            if asset_dict["type"] == "option":
                # Get the stock symbol
                stock_symbol = asset_dict["stock_symbol"]

                # Get the strike
                strike = asset_dict["strike_price"]

                # Get the right
                right = asset_dict["option_type"]

                # Get the expiration
                expiration = asset_dict["expiration_date"]

                # Create the asset
                asset = Asset(
                    symbol=stock_symbol,
                    asset_type="option",
                    expiration=expiration,
                    right=right,
                    strike=strike,
                )

            # Otherwise, it's a stock
            else:
                # Get the stock symbol
                stock_symbol = symbol

                # Create the asset
                asset = Asset(
                    symbol=stock_symbol,
                    asset_type="stock",
                )

            # Create the position
            position = Position(
                strategy=strategy.name if strategy is not None else None,
                asset=asset,
                quantity=quantity,
            )

            # Add the position to the list
            positions_ret.append(position)

        return positions_ret

    def _pull_position(self, strategy, asset):
        all_positions = self._pull_positions(strategy)

        # Loop through each position and check if it matches the asset
        for position in all_positions:
            if position.asset == asset:
                # We found the position, return it
                return position

        return None

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        pass

    def _pull_broker_order(self, identifier):
        pass

    def _pull_broker_open_orders(self):
        pass
