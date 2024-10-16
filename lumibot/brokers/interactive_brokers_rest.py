import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import InteractiveBrokersRESTData
import datetime
from decimal import Decimal
from math import gcd

# Mapping of asset types to Interactive Brokers' type codes
TYPE_MAP = dict(
    stock="STK",
    option="OPT",
    future="FUT",
    forex="CASH",
    index="IND",
    multileg="BAG",
)

# Date format mappings for different asset types
DATE_MAP = dict(
    future="%Y%m%d",
    option="%Y%m%d",
)

# Mapping of order types to Interactive Brokers' order type codes
ORDERTYPE_MAPPING = dict(
    market="MKT",
    limit="LMT",
    stop="STP",
    stop_limit="STP LMT",
    trailing_stop="TRAIL",
)

# Mapping of currency symbols to their respective spread conids
SPREAD_CONID_MAP = {
    "AUD": 61227077,
    "CAD": 61227082,
    "CHF": 61227087,
    "CNH": 136000441,
    "GBP": 58666491,
    "HKD": 61227072,
    "INR": 136000444,
    "JPY": 61227069,
    "KRW": 136000424,
    "MXN": 136000449,
    "SEK": 136000429,
    "SGD": 426116555,
    "USD": 28812380,
}

# Mapping of Interactive Brokers' asset class codes to Asset.AssetType enums
ASSET_CLASS_MAPPING = {
    "STK": Asset.AssetType.STOCK,
    "OPT": Asset.AssetType.OPTION,
    "FUT": Asset.AssetType.FUTURE,
    "CASH": Asset.AssetType.FOREX,
}


class InteractiveBrokersREST(Broker):
    """
    Broker that connects to the Interactive Brokers REST API.

    This class provides methods to interact with Interactive Brokers' REST API,
    including submitting and canceling orders, retrieving account balances and positions,
    parsing broker-specific order and position data, and managing multileg orders.

    Attributes:
        NAME (str): The name identifier for the broker.
        market (str): The default market where trades are executed.
    """

    NAME = "InteractiveBrokersREST"

    def __init__(self, config, data_source=None):
        """
        Initializes the InteractiveBrokersREST broker instance.

        Args:
            config (dict): Configuration dictionary containing necessary credentials and settings.
            data_source (InteractiveBrokersRESTData, optional): An instance of the data source. Defaults to None.
        """
        if data_source is None:
            data_source = InteractiveBrokersRESTData(config)
        super().__init__(name=self.NAME, data_source=data_source, config=config)

        # Set the default market to NYSE
        self.market = "NYSE"

    # --------------------------------------------------------------
    # Broker Methods
    # --------------------------------------------------------------

    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Retrieves the account balances for the specified quote asset from the broker.

        Args:
            quote_asset (Asset): The quote asset for which to retrieve the account balances.
            strategy: The strategy instance requesting the balances.

        Returns:
            tuple of float: A tuple containing (cash, positions_value, total_liquidation_value).
                - cash: Cash balance in the account denominated in the quote asset.
                - positions_value: The total value of all positions in the account.
                - total_liquidation_value: The total equity value of the account (portfolio value).

        Raises:
            None
        """
        strategy_name = strategy._name
        # Retrieve account balances from Interactive Brokers Client Portal
        account_balances = self.data_source.get_account_balances()

        # Check if account balances were successfully retrieved
        if account_balances is None:
            logging.error(colored("Failed to retrieve account balances.", "red"))
            return 0.0, 0.0, 0.0

        # Extract the quote asset symbol
        quote_symbol = quote_asset.symbol

        cash = 0
        balances_for_quote_asset = None

        # Iterate through account balances to find the quote asset and process other currencies
        for currency, balances in account_balances.items():
            if currency == quote_symbol:
                # Extract cash balance for the quote asset
                balances_for_quote_asset = account_balances[quote_symbol]
                cash = balances_for_quote_asset["cashbalance"]
            elif currency != "BASE":
                # Create a Position object for each non-BASE currency asset with non-zero balance
                asset = Asset(symbol=currency, asset_type=Asset.AssetType.FOREX)
                quantity = balances["cashbalance"]

                if quantity != 0:
                    position = Position(
                        strategy=strategy_name,
                        asset=asset,
                        quantity=quantity,
                    )
                    self._filled_positions.append(position)

        # Example of account balances structure provided in comments for reference

        # Calculate total liquidation value for the quote asset
        total_liquidation_value = (
            balances_for_quote_asset["netliquidationvalue"]
            if balances_for_quote_asset is not None
            else 0
        )

        # Calculate the positions value as the difference between total liquidation value and cash
        positions_value = (
            (total_liquidation_value - cash) if total_liquidation_value != 0 else 0
        )

        # Check if there are any forex assets with non-zero quantity and recommend changing quote asset if necessary
        if not hasattr(self, "_quote_asset_checked"):
            forex_assets_with_quantity = [
                position
                for position in self._filled_positions
                if position.asset.asset_type == Asset.AssetType.FOREX
                and position.quantity > 0
            ]

            if cash == 0 and forex_assets_with_quantity:
                logging.warning(
                    colored(
                        f"The selected quote asset '{quote_asset.symbol}' has a quantity of 0. "
                        f"Consider using a different quote asset",
                        "yellow",
                    )
                )
                self._quote_asset_checked = True

        return cash, positions_value, total_liquidation_value

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """
        Parses a broker-specific order representation into an Order object.

        Args:
            response (dict): The raw response data from the broker API representing the order.
            strategy_name (str): The name of the strategy associated with the order.
            strategy_object (Strategy, optional): The strategy instance. Defaults to None.

        Returns:
            Order: An Order object populated with data from the broker response.
        """
        asset_type = [k for k, v in TYPE_MAP.items() if v == response["secType"]][0]
        totalQuantity = response["totalSize"]

        if asset_type == "multileg":
            # Create a multileg order
            order = Order(strategy_name)
            order.order_class = Order.OrderClass.MULTILEG
            order.child_orders = []

            # Parse the legs of the combo order using conidex
            legs = self.decode_conidex(response["conidex"])
            for leg, ratio in legs.items():
                # Parse each leg's order object
                child_order = self._parse_order_object(
                    strategy_name=strategy_name,
                    response=response,
                    quantity=float(ratio) * totalQuantity,
                    conId=leg,
                )
                order.child_orders.append(child_order)
        else:
            # Parse a single-leg order
            order = self._parse_order_object(
                strategy_name=strategy_name,
                response=response,
                quantity=float(totalQuantity),
                conId=response["conid"],
            )

        # Update order status and identifier
        order._transmitted = True
        order.set_identifier(response["orderId"])
        order.status = (response["status"],)
        order.update_raw(response)
        return order

    def _parse_order_object(self, strategy_name, response, quantity, conId):
        """
        Parses a single broker-specific order representation into an Order object.

        Args:
            strategy_name (str): The name of the strategy associated with the order.
            response (dict): The raw response data from the broker API representing the order.
            quantity (float): The quantity of the asset in the order.
            conId (int): The contract ID of the asset.

        Returns:
            Order: An Order object populated with data from the broker response.
        """
        if quantity < 0:
            side = "SELL"
            quantity = -quantity
        else:
            side = "BUY"

        symbol = response["ticker"]
        currency = response["cashCcy"]
        time_in_force = response["timeInForce"]
        limit_price = (
            response["price"]
            if "price" in response and response["price"] != ""
            else None
        )
        stop_price = (
            response["stop_price"]
            if "stop_price" in response and response["stop_price"] != ""
            else None
        )
        good_till_date = (
            response["goodTillDate"]
            if "goodTillDate" in response and response["goodTillDate"] != ""
            else None
        )

        # Retrieve contract details using conId
        contract_details = self.data_source.get_contract_details(conId)
        if contract_details is None:
            contract_details = {}

        secType = ASSET_CLASS_MAPPING.get(contract_details.get("instrument_type"), None)

        multiplier = 1
        right = None
        strike = None
        expiration = None

        if secType == Asset.AssetType.OPTION:
            right = contract_details.get("right")
            strike = float(contract_details.get("strike", 0.0))

        if secType in [Asset.AssetType.OPTION, Asset.AssetType.FUTURE]:
            multiplier = contract_details.get("multiplier", 1)
            maturity_date = contract_details.get("maturity_date")  # in YYYYMMDD

            if maturity_date:
                # Parse and format the expiration date
                expiration = datetime.datetime.strptime(maturity_date, DATE_MAP[secType])

        # Create the Asset object
        asset = Asset(symbol=symbol, asset_type=secType, multiplier=multiplier)

        if expiration is not None:
            asset.expiration = expiration
        if strike is not None:
            asset.strike = strike
        if right is not None:
            asset.right = right

        # Create the Order object with parsed details
        order = Order(
            strategy_name,
            asset,
            quantity=Decimal(quantity),
            side=side.lower(),
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            good_till_date=good_till_date,
            quote=Asset(symbol=currency, asset_type="forex"),
        )

        return order

    def _pull_broker_all_orders(self):
        """
        Retrieves all open orders from the broker.

        Returns:
            list or None: A list of open orders if available, None otherwise.
        """
        orders = self.data_source.get_open_orders()
        return orders

    def _pull_broker_order(self, identifier: str) -> Order:
        """
        Retrieves a specific broker order by its identifier.

        Args:
            identifier (str): The unique identifier of the order.

        Returns:
            Order: The corresponding Order object if found, otherwise a new Order with default values.
        """
        pull_order = [
            order
            for order in self.data_source.get_open_orders()
            if order.orderId == identifier
        ]
        response = pull_order[0] if len(pull_order) > 0 else None
        if response is None:
            logging.error(
                colored(f"Order with identifier {identifier} not found.", "red")
            )
            return Order(self._strategy_name)
        return response

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """
        Parses a broker-specific position representation into a Position object.

        Args:
            broker_position (dict): The raw position data from the broker.
            strategy (Strategy): The strategy associated with the position.
            orders (list, optional): List of orders associated with the position. Defaults to None.

        Returns:
            Position: A Position object populated with data from the broker position.
        """
        if broker_position["asset_type"] == "stock":
            asset = Asset(
                symbol=broker_position["symbol"],
            )
        elif broker_position["asset_type"] == "future":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="future",
                expiration=broker_position["expiration"],
                multiplier=broker_position["multiplier"],
            )
        elif broker_position["asset_type"] == "option":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="option",
                expiration=broker_position["expiration"],
                strike=broker_position["strike"],
                right=broker_position["right"],
                multiplier=broker_position["multiplier"],
            )
        elif broker_position["asset_type"] == "forex":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="forex",
            )
        else:
            # Raise an error for unsupported asset types
            raise ValueError(
                f"From Interactive Brokers, asset type can only be `stock`, "
                f"`future`, or `option`. A value of {broker_position['asset_type']} "
                f"was received."
            )

        quantity = broker_position["position"]
        position = Position(strategy, asset, quantity, orders=orders)
        return position

    def _parse_broker_positions(self, broker_positions, strategy):
        """
        Parses a list of broker-specific position representations into Position objects.

        Args:
            broker_positions (list): A list of raw position data from the broker.
            strategy (Strategy): The strategy associated with the positions.

        Returns:
            list of Position: A list of Position objects populated with data from the broker positions.
        """
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_position(self, strategy, asset: Asset) -> Position:
        """
        Retrieves a specific position for a given asset from the broker.

        Args:
            strategy (Strategy): The strategy requesting the position.
            asset (Asset): The asset for which to retrieve the position.

        Returns:
            Position: The corresponding Position object if found, otherwise a new Position with zero quantity.
        """
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        for pos in result:
            if pos.asset == asset:
                return pos
        return Position(strategy, asset, 0)

    def _pull_broker_positions(self, strategy=None):
        """
        Retrieves all positions from the broker.

        Args:
            strategy (Strategy, optional): The strategy associated with the positions. Defaults to None.

        Returns:
            list: A list of raw position data from the broker if available, otherwise an empty list.
        """
        positions = []
        ib_positions = self.data_source.get_positions()
        if ib_positions:
            for position in ib_positions:
                if position["position"] != 0:
                    positions.append(position)
        else:
            logging.debug("No positions found at Interactive Brokers.")

        return positions

    def _pull_positions(self, strategy) -> list[Position]:
        """
        Retrieves all positions from the broker for the given strategy.

        Args:
            strategy (Strategy): The strategy requesting the positions.

        Returns:
            list of Position: A list of Position objects representing the current positions in the account.
        """
        # Retrieve positions from Interactive Brokers Client Portal
        positions = self.data_source.get_positions()

        # Check if positions were successfully retrieved
        if positions is None:
            logging.error(colored("Failed to retrieve positions.", "red"))
            return []

        # Example of positions response structure provided in comments for reference

        # Initialize a list to store Position objects
        positions_list = []

        # Iterate through each position and create Position objects based on asset type
        for position in positions:
            symbol = position["contractDesc"]
            asset_class = ASSET_CLASS_MAPPING.get(position["assetClass"], None)

            # Create Asset object based on asset class
            if asset_class == Asset.AssetType.STOCK:
                asset = Asset(symbol=symbol, asset_type=asset_class)
            elif asset_class == Asset.AssetType.OPTION:
                expiry = position["expiry"]
                strike = position["strike"]
                right = position["putOrCall"]
                asset = Asset(
                    symbol=symbol,
                    asset_type=asset_class,
                    expiration=expiry,
                    strike=strike,
                    right=right,
                )
            elif asset_class == Asset.AssetType.FUTURE:
                expiry = position["expiry"]
                multiplier = position["multiplier"]
                asset = Asset(
                    symbol=symbol,
                    asset_type=asset_class,
                    expiration=expiry,
                    multiplier=multiplier,
                )
            else:
                # Log a warning for unsupported asset types and skip
                logging.warning(
                    colored(
                        f"Asset class '{asset_class}' not supported yet (we need to add code for this asset type): {asset_class} for position {position}",
                        "yellow",
                    )
                )
                continue

            # Create the Position object with parsed details
            position_obj = Position(
                strategy=strategy,
                asset=asset,
                quantity=position["position"],
                avg_fill_price=position["avgCost"],
            )

            # Append the Position object to the list
            positions_list.append(position_obj)

        return positions_list

    def _log_order_status(self, order, status, success=True):
        """
        Logs the status of an order based on its success or failure.

        Args:
            order (Order): The order whose status is being logged.
            status (str): The status message to log.
            success (bool, optional): Indicates if the order was successful. Defaults to True.

        Returns:
            None
        """
        if success:
            if order.order_class == Order.OrderClass.MULTILEG:
                logging.info(
                    colored(
                        "Order executed successfully: This is a multileg order.",
                        "green",
                    )
                )
                for child_order in order.child_orders:
                    logging.info(
                        colored(
                            f"Child Order: Ticker: {child_order.asset.symbol}, Quantity: {child_order.quantity}, "
                            f"Asset Type: {child_order.asset.asset_type}, Right: {child_order.asset.right}, Side: {child_order.side}",
                            "green",
                        )
                    )
            elif order.asset.asset_type in [
                Asset.AssetType.STOCK,
                Asset.AssetType.FOREX,
            ]:
                logging.info(
                    colored(
                        f"Order executed successfully: Ticker: {order.asset.symbol}, Quantity: {order.quantity}",
                        "green",
                    )
                )
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                logging.info(
                    colored(
                        f"Order executed successfully: Ticker: {order.asset.symbol}, Expiration Date: {order.asset.expiration}, "
                        f"Strike: {order.asset.strike}, Right: {order.asset.right}, Quantity: {order.quantity}, Side: {order.side}",
                        "green",
                    )
                )
            elif order.asset.asset_type == Asset.AssetType.FUTURE:
                logging.info(
                    colored(
                        f"Order executed successfully: Ticker: {order.asset.symbol}, Expiration Date: {order.asset.expiration}, "
                        f"Multiplier: {order.asset.multiplier}, Quantity: {order.quantity}",
                        "green",
                    )
                )
            else:
                logging.info(
                    colored(
                        f"Order executed successfully: Ticker: {order.asset.symbol}, Quantity: {order.quantity}, "
                        f"Asset Type: {order.asset.asset_type}",
                        "green",
                    )
                )
        else:
            if order.order_class == Order.OrderClass.MULTILEG:
                logging.debug(
                    colored("Order details for failed multileg order.", "blue")
                )
                for child_order in order.child_orders:
                    logging.debug(
                        colored(
                            f"Child Order: Ticker: {child_order.asset.symbol}, Quantity: {child_order.quantity}, "
                            f"Asset Type: {child_order.asset.asset_type}, Right: {child_order.asset.right}, Side: {child_order.side}",
                            "blue",
                        )
                    )
            elif order.asset.asset_type in [
                Asset.AssetType.STOCK,
                Asset.AssetType.FOREX,
            ]:
                logging.debug(
                    colored(
                        f"Order details for failed {order.asset.asset_type.lower()} order: "
                        f"Ticker: {order.asset.symbol}, Quantity: {order.quantity}",
                        "blue",
                    )
                )
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                logging.debug(
                    colored(
                        f"Order details for failed option order: Ticker: {order.asset.symbol}, "
                        f"Expiry Date: {order.asset.expiration}, Strike: {order.asset.strike}, "
                        f"Right: {order.asset.right}, Quantity: {order.quantity}, Side: {order.side}",
                        "blue",
                    )
                )
            elif order.asset.asset_type == Asset.AssetType.FUTURE:
                logging.debug(
                    colored(
                        f"Order details for failed future order: Ticker: {order.asset.symbol}, "
                        f"Expiry Date: {order.asset.expiration}, Multiplier: {order.asset.multiplier}, "
                        f"Quantity: {order.quantity}",
                        "blue",
                    )
                )
            else:
                logging.debug(
                    colored(
                        f"Order details for failed order: Ticker: {order.asset.symbol}, Quantity: {order.quantity}, "
                        f"Asset Type: {order.asset.asset_type}",
                        "blue",
                    )
                )

    def _submit_order(self, order: Order) -> Order:
        """
        Submits an order to the broker and logs its status.

        Args:
            order (Order): The Order object to be submitted.

        Returns:
            Order: The submitted Order object with updated identifier and status.
        """
        try:
            # Convert Order object to broker-specific order data format
            order_data = self.get_order_data_from_orders([order])
            response = self.data_source.execute_order(order_data)

            if response is None:
                # Log failure if no response received
                self._log_order_status(order, "failed", success=False)
                return order
            else:
                # Log successful execution
                self._log_order_status(order, "executed", success=True)

            # Update order identifier and status
            order.identifier = response[0]["order_id"]
            order.status = "submitted"
            self._unprocessed_orders.append(order)

            return order

        except Exception as e:
            # Log any exceptions that occur during order submission
            logging.error(
                colored(
                    f"An error occurred while submitting the order: {str(e)}", "red"
                )
            )
            logging.error(colored(f"Error details:", "red"), exc_info=True)
            return order

    def submit_orders(
        self,
        orders: list[Order],
        is_multileg: bool = False,
        order_type: str = "market",
        duration: str = "day",
        price=None,
    ):
        """
        Submits a list of orders to the broker, handling both single-leg and multileg orders.

        Args:
            orders (list of Order): The list of Order objects to be submitted.
            is_multileg (bool, optional): Indicates if the orders form a multileg (combo) order. Defaults to False.
            order_type (str, optional): The type of the order (e.g., 'market', 'limit'). Defaults to "market".
            duration (str, optional): The duration of the order (e.g., 'day', 'gtc'). Defaults to "day".
            price (float, optional): The price of the order, if applicable. Defaults to None.

        Returns:
            list of Order or None: A list of submitted Order objects if successful, otherwise None.

        Raises:
            None
        """
        try:
            if is_multileg:
                # Handle multileg (combo) orders
                if order_type == "credit":
                    if price is not None:
                        order_type = "limit"
                        if price < 0:
                            price = -price
                    else:
                        order_type = "market"

                elif order_type == "debit":
                    if price is not None:
                        order_type = "limit"
                    else:
                        order_type = "market"

                elif order_type == "even":
                    price = 0
                    order_type = "limit"

                # Convert multileg orders to broker-specific order data format
                order_data = self.get_order_data_multileg(
                    orders, order_type=order_type, duration=duration, price=price
                )
                response = self.data_source.execute_order(order_data)

                if response is None:
                    # Log failure for all orders if no response received
                    for order in orders:
                        self._log_order_status(order, "failed", success=False)
                    return None

                # Create a parent Order object for the multileg order
                order = Order(orders[0].strategy)
                order.order_class = Order.OrderClass.MULTILEG
                order.child_orders = orders
                order.status = "submitted"
                order.identifier = response[0]["order_id"]

                self._unprocessed_orders.append(order)
                self._log_order_status(order, "executed", success=True)
                return [order]

            else:
                # Handle single-leg orders
                order_data = self.get_order_data_from_orders([order])
                response = self.data_source.execute_order(order_data)

                if response is None:
                    # Log failure for all orders if no response received
                    for order in orders:
                        self._log_order_status(order, "failed", success=False)
                    return None

                # Update each order with its respective identifier and status
                order_id = 0
                for order in orders:
                    order.status = "submitted"
                    order.identifier = response[order_id]["order_id"]
                    self._unprocessed_orders.append(order)
                    self._log_order_status(order, "executed", success=True)
                    order_id += 1

                return orders

        except Exception as e:
            # Log any exceptions that occur during order submission
            logging.error(
                colored(
                    f"An error occurred while submitting the order: {str(e)}", "red"
                )
            )
            logging.error(colored(f"Error details:", "red"), exc_info=True)

    def cancel_order(self, order: Order) -> None:
        """
        Cancels (deletes) an existing order at the broker.

        Args:
            order (Order): The Order object to be canceled.

        Returns:
            None
        """
        self.data_source.delete_order(order)

    def decode_conidex(self, conidex: str) -> dict:
        """
        Decodes a conidex string into a dictionary mapping conids to their ratios.

        Args:
            conidex (str): The conidex string in the format "{spread_conid};;;{leg_conid1}/{ratio},{leg_conid2}/{ratio}".

        Returns:
            dict: A dictionary mapping leg conids to their respective ratios.
        """
        # Example format: "spread_conid;;;leg_conid1/ratio,leg_conid2/ratio"
        string = conidex
        _, ratios = string.split(";;;")
        legs = ratios.split(",")

        legs_dict = {}
        for leg in legs:
            leg_conid, ratio = leg.split("/")
            legs_dict[leg_conid] = ratio

        return legs_dict

    def get_order_data_from_order(self, order):
        """
        Converts a single Order object into a broker-specific order data dictionary.

        Args:
            order (Order): The Order object to be converted.

        Returns:
            dict or None: The broker-specific order data dictionary if successful, otherwise None.
        """
        try:
            conid = None
            side = None
            orderType = None

            # Determine the side of the order
            if order.is_buy_order():
                side = "BUY"
            elif order.is_sell_order():
                side = "SELL"
            else:
                logging.error(colored("Order Side Not Found", "red"))
                return None

            # Map the order type to broker-specific order type
            orderType = ORDERTYPE_MAPPING.get(order.type, None)

            # Retrieve the contract ID for the asset
            conid = self.data_source.get_conid_from_asset(order.asset)

            if conid is None:
                asset_type = order.asset.asset_type
                expiry_date = (
                    order.asset.expiration
                    if hasattr(order.asset, "expiration")
                    else "N/A"
                )
                logging.error(
                    colored(
                        f"Couldn't find an appropriate asset for {order.asset} (Type: {asset_type}, Expiry: {expiry_date}).",
                        "red",
                    )
                )
                return None

            # Construct the order data dictionary
            data = {
                "conid": conid,
                "quantity": round(order.quantity, 2),
                "orderType": orderType,
                "side": side,
                "tif": order.time_in_force.upper(),
                "price": round(order.limit_price, 2)
                if order.limit_price is not None
                else None,
                "auxPrice": round(order.stop_price, 2)
                if order.stop_price is not None
                else None,
                "listingExchange": order.exchange,
            }

            # Handle trailing stop parameters if present
            if order.trail_percent:
                data["trailingType"] = "%"
                data["trailingAmt"] = order.trail_percent

            if order.trail_price:
                data["trailingType"] = "amt"
                data["trailingAmt"] = order.trail_price

            # Remove items with value None from order_data
            data = {k: v for k, v in data.items() if v is not None}
            return data

        except Exception as e:
            # Log any exceptions that occur during order data processing
            logging.error(
                colored(
                    f"An error occurred while processing the order: {str(e)}", "red"
                )
            )
            logging.error(colored(f"Error details:", "red"), exc_info=True)
            return None

    def get_order_data_from_orders(self, orders: list[Order]):
        """
        Converts a list of Order objects into a broker-specific order data dictionary.

        Args:
            orders (list of Order): The list of Order objects to be converted.

        Returns:
            dict or None: A dictionary containing all broker-specific order data if successful, otherwise None.
        """
        order_data = {"orders": []}

        for order in orders:
            data = self.get_order_data_from_order(order)
            if data is not None:
                order_data["orders"].append(data)

        return order_data if order_data["orders"] else None

    def get_order_data_multileg(
        self, orders: list[Order], order_type=None, duration=None, price=None
    ):
        """
        Generates the order data dictionary for a multileg (combo) order.

        Args:
            orders (list of Order): List of Order objects representing the legs of the multileg order.
            order_type (str, optional): The type of the order (e.g., 'market', 'limit'). Defaults to None.
            duration (str, optional): The duration of the order (e.g., 'day', 'gtc'). Defaults to None.
            price (float, optional): The price of the order. Defaults to None.

        Returns:
            dict or None: A dictionary containing the broker-specific order data for the multileg order if successful, otherwise None.
        """
        # Initialize the order data dictionary
        order_data = {"orders": []}

        # Ensure the first order has a quote asset
        if not orders[0].quote:
            logging.error("Quote is None for the first order.")
            return None

        # Retrieve the spread conid for the quote asset
        spread_conid = SPREAD_CONID_MAP.get(orders[0].quote.symbol)
        if spread_conid is None:
            logging.error(colored("Spread conid Not Found", "red"))
            return None

        # Build the conidex string in the required format
        conidex = f"{spread_conid};;;"

        # List to store conid and quantity pairs
        ratios = []

        # Iterate through each order to extract conid and quantity
        for order in orders:
            side = None
            conid = None

            # Determine the side of the order
            if order.is_buy_order():
                side = "BUY"
            elif order.is_sell_order():
                side = "SELL"
            else:
                logging.error(colored("Order Side Not Found", "red"))
                return None

            # Retrieve the contract ID for the asset
            conid = self.data_source.get_conid_from_asset(order.asset)
            if conid is None:
                logging.error(colored("Order conid Not Found", "red"))
                return None

            quantity = order.quantity
            if quantity == 0 or quantity is None:
                return None

            # Adjust quantity for sell orders
            if side == "SELL":
                quantity = -quantity

            # Append the conid and quantity pair to the ratios list
            ratios.append((conid, quantity))

        # Calculate the greatest common divisor (GCD) of the quantities to simplify ratios
        quantities = [quant for _, quant in ratios]
        order_quantity = gcd(*quantities)

        # Build the conidex string with simplified quantities
        first_order = True
        for conid, quantity in ratios:
            if first_order:
                first_order = False
            else:
                conidex += ","
            conidex += f"{conid}/{quantity // order_quantity}"

        # Set the side to "BUY" for the multileg order
        side = "BUY"

        if not orders:
            raise ValueError("Orders list cannot be empty")

        order = orders[0]

        # Determine the order type, defaulting to "MKT" if not specified
        order_type_value = order_type if order_type is not None else order.type
        if order_type_value is None:
            order_type_value = "MKT"
            logging.info("Order type not specified. Defaulting to 'MKT'.")

        # Construct the order data dictionary
        data = {
            "conidex": conidex,
            "quantity": round(order_quantity, 2),
            "orderType": ORDERTYPE_MAPPING.get(order_type_value),
            "side": side,
            "tif": duration.upper() if duration is not None else order.time_in_force.upper(),
            "price": round(float(price), 2) if price is not None else None,
            "auxPrice": round(order.stop_price, 2) if order.stop_price is not None else None,
            "listingExchange": order.exchange,
        }

        # Remove items with value None from the order data
        data = {k: v for k, v in data.items() if v is not None}
        order_data["orders"].append(data)

        return order_data

    def get_historical_account_value(self) -> dict:
        """
        Retrieves historical account values from the broker.

        Note:
            This method is not yet implemented for Interactive Brokers.

        Returns:
            dict: A dictionary with keys 'hourly' and 'daily', both set to None.
        """
        logging.error(
            "The function get_historical_account_value is not implemented yet for Interactive Brokers."
        )
        return {"hourly": None, "daily": None}

    def _register_stream_events(self):
        """
        Registers stream events for real-time data handling.

        Note:
            This method is not yet implemented for Interactive Brokers.

        Returns:
            None
        """
        logging.error(
            colored("Method '_register_stream_events' is not yet implemented.", "red")
        )
        return None

    def _run_stream(self):
        """
        Initiates the streaming of real-time data from the broker.

        Note:
            This method is not yet implemented for Interactive Brokers.

        Returns:
            None
        """
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None

    def _get_stream_object(self):
        """
        Retrieves the stream object for handling real-time data.

        Note:
            This method is not yet implemented for Interactive Brokers.

        Returns:
            None
        """
        logging.error(
            colored("Method '_get_stream_object' is not yet implemented.", "red")
        )
        return None

    def _close_connection(self):
        """
        Closes the connection to the Interactive Brokers Client Portal.

        This method ensures that any running connections or containers are properly terminated.

        Returns:
            None
        """
        logging.info("Closing connection to the Client Portal...")
        self.data_source.stop()