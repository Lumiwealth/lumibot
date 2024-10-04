import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import InteractiveBrokersRESTData
import datetime
from decimal import Decimal
from math import gcd

TYPE_MAP = dict(
    stock="STK",
    option="OPT",
    future="FUT",
    forex="CASH",
    index="IND",
    multileg="BAG",
)

DATE_MAP = dict(
    future="%Y%m%d",
    option="%Y%m%d",
)

ORDERTYPE_MAPPING = dict(
    market="MKT",
    limit="LMT",
    stop="STP",
    stop_limit="STP LMT",
    trailing_stop="TRAIL",
)

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
    "USD": 28812380
}

ASSET_CLASS_MAPPING = {
    "STK": Asset.AssetType.STOCK,
    "OPT": Asset.AssetType.OPTION,
    "FUT": Asset.AssetType.FUTURE,
    "CASH": Asset.AssetType.FOREX,
}

class InteractiveBrokersREST(Broker):
    """
    Broker that connects to the Interactive Brokers REST API.
    """

    NAME = "InteractiveBrokersREST"

    def __init__(self, config, data_source=None):              
        if data_source is None:
            data_source = InteractiveBrokersRESTData(config)
        super().__init__(name=self.NAME, data_source=data_source, config=config)

        self.market = "NYSE"  # The default market is NYSE.

    # --------------------------------------------------------------
    # Broker methods
    # --------------------------------------------------------------

    # Existing method stubs with logging
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Get the account balances for the quote asset from the broker.
        
        Parameters
        ----------
        quote_asset : Asset
            The quote asset for which to retrieve the account balances.

        Returns
        -------
        tuple of float
            A tuple containing (cash, positions_value, total_liquidation_value).
            Cash = cash in the account (whatever the quote asset is).
            Positions value = the value of all the positions in the account.
            Portfolio value = the total equity value of the account (aka. portfolio value).
        """
        strategy_name = strategy._name
        # Get the account balances from the Interactive Brokers Client Portal
        account_balances = self.data_source.get_account_balances()

        # Check that the account balances were successfully retrieved
        if account_balances is None:
            logging.error(colored("Failed to retrieve account balances.", "red"))
            return 0.0, 0.0, 0.0

        # Get the quote asset symbol
        quote_symbol = quote_asset.symbol

        # account_balances = {'CHF': {'commoditymarketvalue': 0.0, 'futuremarketvalue': 0.0, 'settledcash': 188.59, 'exchangerate': 1.1847296, 'sessionid': 1, 'cashbalance': 188.59, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 188.59, 'interest': 0, 'unrealizedpnl': 0.0, 'stockmarketvalue': 0.0, 'moneyfunds': 0.0, 'currency': 'CHF', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', ...}, 'JPY': {'commoditymarketvalue': 0.0, 'futuremarketvalue': 0.0, 'settledcash': -3794999.0, 'exchangerate': 0.0069919, 'sessionid': 1, 'cashbalance': -3794999.0, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': -3794999.0, 'interest': 0, 'unrealizedpnl': 0.0, 'stockmarketvalue': 0.0, 'moneyfunds': 0.0, 'currency': 'JPY', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', ...}, 'EUR': {'commoditymarketvalue': 0.0, 'futuremarketvalue': 0.0, 'settledcash': 287480.9, 'exchangerate': 1.1157291, 'sessionid': 1, 'cashbalance': 287480.9, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 288112.94, 'interest': 632.03, 'unrealizedpnl': 0.0, 'stockmarketvalue': 0.0, 'moneyfunds': 0.0, 'currency': 'EUR', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', ...}, 'USD': {'commoditymarketvalue': 0.0, 'futuremarketvalue': -87.3, 'settledcash': 208917.02, 'exchangerate': 1, 'sessionid': 1, 'cashbalance': 208917.02, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 209711.64, 'interest': 518.04, 'unrealizedpnl': 19358.56, 'stockmarketvalue': 276.58, 'moneyfunds': 0.0, 'currency': 'USD', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', ...}, 'BASE': {'commoditymarketvalue': 0.0, 'futuremarketvalue': -87.3, 'settledcash': 503393.47, 'exchangerate': 1, 'sessionid': 1, 'cashbalance': 503393.47, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 504893.34, 'interest': 1223.307, 'unrealizedpnl': 19358.56, 'stockmarketvalue': 276.58, 'moneyfunds': 0.0, 'currency': 'BASE', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', ...}}

        # Loop through the account balances and find the quote asset. If not the quote asset, create a position object for the currency/forex asset.
        cash = None
        for currency, balances in account_balances.items():
            if currency == quote_symbol:
                # Get the account balances for the quote asset
                balances_for_quote_asset = account_balances[quote_symbol]

                # Get the cash balance for the quote asset
                cash = balances_for_quote_asset['cashbalance']
            elif currency != "BASE":
                # Create a position object for the currency/forex asset
                asset = Asset(symbol=currency, asset_type=Asset.AssetType.FOREX)
                quantity = balances['cashbalance']

                if quantity != 0:
                    position = Position(
                        strategy=strategy_name,
                        asset=asset,
                        quantity=quantity,
                    )
                    self._filled_positions.append(position)
                
                pos = self._filled_positions

        # Exmaple account balances response:
        # {'commoditymarketvalue': 0.0, 'futuremarketvalue': 677.49, 'settledcash': 202142.17, 'exchangerate': 1, 'sessionid': 1, 'cashbalance': 202142.17, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 202464.67, 'interest': 452.9, 'unrealizedpnl': 12841.38, 'stockmarketvalue': -130.4, 'moneyfunds': 0.0, 'currency': 'USD', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', 'timestamp': 1724382002, 'severity': 0, 'stockoptionmarketvalue': 0.0, 'futuresonlypnl': 677.49, 'tbondsmarketvalue': 0.0, 'futureoptionmarketvalue': 0.0, 'cashbalancefxsegment': 0.0, 'secondkey': 'USD', 'tbillsmarketvalue': 0.0, 'endofbundle': 1, 'dividends': 0.0, 'cryptocurrencyvalue': 0.0}

        # Get the net liquidation value for the quote asset
        total_liquidation_value = balances_for_quote_asset['netliquidationvalue']

        # Calculate the positions value
        positions_value = total_liquidation_value - cash

        return cash, positions_value, total_liquidation_value

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """Parse a broker order representation
        to an order object"""

        asset_type = [k for k, v in TYPE_MAP.items() if v == response['secType']][0]
        totalQuantity = response['totalSize']

        if asset_type == "multileg":
            # Create a multileg order.
            order = Order(strategy_name)
            order.order_class = Order.OrderClass.MULTILEG
            order.child_orders = []

            # Parse the legs of the combo order.
            legs = self.decode_conidex(response["conidex"])
            for leg, ratio in legs.items():
                # Create the object with just the conId
                # TODO check if all legs using the same response is an issue
                child_order = self._parse_order_object(strategy_name=strategy_name,
                                                       response=response,
                                                       quantity=float(ratio)*totalQuantity,
                                                       conId=leg
                                                       )
                order.child_orders.append(child_order)

        else:
            order = self._parse_order_object(strategy_name=strategy_name, 
                                             response=response,
                                             quantity=float(totalQuantity),
                                             conId=response['conid'],
                                             )
        
        order._transmitted = True
        order.set_identifier(response['orderId'])
        order.status = response['status'],
        order.update_raw(response)
        return order

    def _parse_order_object(self, strategy_name, response, quantity, conId):
        if quantity < 0:
            side = "SELL"
            quantity=-quantity
        else:
            side = "BUY"
        
        symbol=response['ticker']
        currency=response['cashCcy']
        time_in_force=response['timeInForce']
        limit_price = response['price'] if 'price' in response and response['price'] != '' else None
        stop_price = response['stop_price'] if 'stop_price' in response and response['stop_price'] != '' else None
        good_till_date = response['goodTillDate'] if 'goodTillDate' in response and response['goodTillDate'] != '' else None
        
        contract_details = self.data_source.get_contract_details(conId)
        if contract_details is None:
            contract_details = {}
        
        secType = ASSET_CLASS_MAPPING[contract_details["instrument_type"]]

        multiplier = 1
        right = None
        strike = None
        expiration = None

        if secType == "option":
            right = contract_details['right']
            strike = float(contract_details['strike'])

        if secType in ["option", "future"]:
            multiplier = contract_details['multiplier']
            maturity_date = contract_details["maturity_date"] # in YYYYMMDD

            # Format the datetime object as a string that matches the format in DATE_MAP[secType]
            expiration = datetime.datetime.strptime(
                maturity_date,
                DATE_MAP[secType]
            )

        order = Order(
            strategy_name,
            Asset(
                symbol=symbol,
                asset_type=secType,
                expiration=expiration,
                strike=strike,
                right=right,
                multiplier=multiplier
            ),
            quantity = Decimal(quantity),
            side = side.lower(),
            limit_price = limit_price,
            stop_price = stop_price,
            time_in_force = time_in_force,
            good_till_date = good_till_date,
            quote = Asset(symbol=currency, asset_type="forex"),
        )

        return order

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
        orders = self.data_source.get_open_orders()
        return orders
    
    def _pull_broker_order(self, identifier: str) -> Order:
        """Get a broker order representation by its id"""
        pull_order = [order for order in self.data_source.get_open_orders() if order.orderId == order_id]
        response = pull_order[0] if len(pull_order) > 0 else None
        return response
    
    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """Parse a broker position representation
        into a position object"""
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
        else:  # Unreachable code.
            raise ValueError(
                f"From Interactive Brokers, asset type can only be `stock`, "
                f"`future`, or `option`. A value of {broker_position['asset_type']} "
                f"was received."
            )

        quantity = broker_position["position"]
        position = Position(strategy, asset, quantity, orders=orders)
        return position
    
    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result
    
    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Position:
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        return result

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        positions = []
        ib_positions = self.data_source.get_positions()
        if ib_positions:
            for position in ib_positions:
                if position["position"] != 0:
                    positions.append(position)
        else:
            logging.debug("No positions found at interactive brokers.")

        return positions

    def _pull_positions(self, strategy: 'Strategy') -> list[Position]:
        """
        Get the positions from the broker for the given strategy.
        
        Parameters
        ----------
        strategy : Strategy
            The strategy for which to retrieve the positions.

        Returns
        -------
        list of Position
            A list of Position objects representing the positions in the account.
        """
        
        # Get the positions from the Interactive Brokers Client Portal
        positions = self.data_source.get_positions()

        # Check that the positions were successfully retrieved
        if positions is None:
            logging.error(colored("Failed to retrieve positions.", "red"))
            return []
        
        # Example positions response:
        # [{'acctId': 'DU4299039', 'conid': 265598, 'contractDesc': 'AAPL', 'position': -10.0, 'mktPrice': 225.0299988, 'mktValue': -2250.3, 'currency': 'USD', 'avgCost': 211.96394, 'avgPrice': 211.96394, 'realizedPnl': 0.0, 'unrealizedPnl': -130.66, 'exchs': None, 'expiry': None, 'putOrCall': None, 'multiplier': None, 'strike': 0.0, 'exerciseStyle': None, 'conExchMap': [], 'assetClass': 'STK', 'undConid': 0}]
        
        # Initialize a list to store the Position objects
        positions_list = []

        # Loop through the positions and create Position objects
        for position in positions:
            # Create the Asset object for the position
            symbol = position['contractDesc']
            asset_class = ASSET_CLASS_MAPPING[position['assetClass']]

            # If asset class is stock, create a stock asset
            if asset_class == Asset.AssetType.STOCK:
                asset = Asset(symbol=symbol, asset_type=asset_class)
            elif asset_class == Asset.AssetType.OPTION:
                expiry = position['expiry']
                strike = position['strike']
                right = position['putOrCall']
                # If asset class is option, create an option asset
                asset = Asset(
                    symbol=symbol,
                    asset_type=asset_class,
                    expiration=expiry,
                    strike=strike,
                    right=right,
                )
            elif asset_class == Asset.AssetType.FUTURE:
                expiry = position['expiry']
                multiplier = position['multiplier']
                asset = Asset(
                    symbol=symbol,
                    asset_type=asset_class,
                    expiration=expiry,
                    multiplier=multiplier,
                )
            else:
                logging.warning(colored(f"Asset class '{asset_class}' not supported yet (we need to add code for this asset type): {asset_class} for position {position}", "yellow"))
                continue
            
            # Create the Position object
            position_obj = Position(
                strategy=strategy,
                asset=asset,
                quantity=position['position'],
                avg_fill_price=position['avgCost'],
            )

            # Append the Position object to the list
            positions_list.append(position_obj)

        return positions_list
    
    def _log_order_status(self, order, status, success=True):
        if success:
            if order.order_class == Order.OrderClass.MULTILEG:
                logging.info(colored("Order executed successfully: This is a multileg order.", "green"))
                for child_order in order.child_orders:
                    logging.info(colored(f"Child Order: Ticker: {child_order.asset.symbol}, Quantity: {child_order.quantity}, Asset Type: {child_order.asset.asset_type}, Right: {child_order.asset.right}, Side: {child_order.side}", "green"))
            elif order.asset.asset_type in [Asset.AssetType.STOCK, Asset.AssetType.FOREX]:
                logging.info(colored(f"Order executed successfully: Ticker: {order.asset.symbol}, Quantity: {order.quantity}", "green"))
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                logging.info(colored(f"Order executed successfully: Ticker: {order.asset.symbol}, Expiration Date: {order.asset.expiration}, Strike: {order.asset.strike}, Right: {order.asset.right}, Quantity: {order.quantity}, Side: {order.side}", "green"))
            elif order.asset.asset_type == Asset.AssetType.FUTURE:
                logging.info(colored(f"Order executed successfully: Ticker: {order.asset.symbol}, Expiration Date: {order.asset.expiration}, Multiplier: {order.asset.multiplier}, Quantity: {order.quantity}", "green"))
            else:
                logging.info(colored(f"Order executed successfully: Ticker: {order.asset.symbol}, Quantity: {order.quantity}, Asset Type: {order.asset.asset_type}", "green"))
        else:
            if order.order_class == Order.OrderClass.MULTILEG:
                logging.debug(colored("Order details for failed multileg order.", "blue"))
                for child_order in order.child_orders:
                    logging.debug(colored(f"Child Order: Ticker: {child_order.asset.symbol}, Quantity: {child_order.quantity}, Asset Type: {child_order.asset.asset_type}, Right: {child_order.asset.right}, Side: {child_order.side}", "blue"))
            elif order.asset.asset_type in [Asset.AssetType.STOCK, Asset.AssetType.FOREX]:
                logging.debug(colored(f"Order details for failed {order.asset.asset_type.lower()} order: Ticker: {order.asset.symbol}, Quantity: {order.quantity}", "blue"))
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                logging.debug(colored(f"Order details for failed option order: Ticker: {order.asset.symbol}, Expiry Date: {order.asset.expiration}, Strike: {order.asset.strike}, Right: {order.asset.right}, Quantity: {order.quantity}, Side: {order.side}", "blue"))
            elif order.asset.asset_type == Asset.AssetType.FUTURE:
                logging.debug(colored(f"Order details for failed future order: Ticker: {order.asset.symbol}, Expiry Date: {order.asset.expiration}, Multiplier: {order.asset.multiplier}, Quantity: {order.quantity}", "blue"))
            else:
                logging.debug(colored(f"Order details for failed order: Ticker: {order.asset.symbol}, Quantity: {order.quantity}, Asset Type: {order.asset.asset_type}", "blue"))

    def _submit_order(self, order: Order) -> Order:
        try:
            order_data = self.get_order_data_from_orders([order])
            response = self.data_source.execute_order(order_data)
            if response is None:
                self._log_order_status(order, "failed", success=False)
                return order
            else:
                self._log_order_status(order, "executed", success=True)

            order.identifier = response[0]["order_id"]
            order.status = "submitted"
            self._unprocessed_orders.append(order)

            return order
        
        except Exception as e:
            logging.error(colored(f"An error occurred while submitting the order: {str(e)}", "red"))
            logging.error(colored(f"Error details:", "red"), exc_info=True)
            return order

    def submit_orders(self, orders: list[Order], is_multileg=False, order_type="market", duration="day", price=None):
        try:
            if is_multileg:
                order_data = self.get_order_data_multileg(orders, order_type=order_type, duration=duration, price=price)
                response = self.data_source.execute_order(order_data)
                if response is None:
                    for order in orders:
                        self._log_order_status(order, "failed", success=False)
                    return None

                order = Order(orders[0].strategy)
                order.order_class = Order.OrderClass.MULTILEG
                order.child_orders = orders
                order.status = "submitted"
                order.identifier = response[0]["order_id"]

                self._unprocessed_orders.append(order)
                self._log_order_status(order, "executed", success=True)
                return [order]
            
            else:
                order_data = self.get_order_data_from_orders([order])
                response = self.data_source.execute_order(order_data)
                if response is None:
                    for order in orders:
                        self._log_order_status(order, "failed", success=False)
                    return None
                
                # TODO Could be a problematic system
                order_id = 0
                for order in orders:
                    order.status = "submitted"
                    order.identifier = response[order_id]['order_id']
                    self._unprocessed_orders.append(order)
                    self._log_order_status(order, "executed", success=True)
                    order_id += 1

                return orders

        except Exception as e:
            logging.error(colored(f"An error occurred while submitting the order: {str(e)}", "red"))
            logging.error(colored(f"Error details:", "red"), exc_info=True)

    def cancel_order(self, order: Order) -> None:
        self.data_source.delete_order(order)

    def decode_conidex(self, conidex: str) -> dict:
        # Decode this format {spread_conid};;;{leg_conid1}/{ratio},{leg_conid2}/{ratio}
        string = conidex
        _, ratios = string.split(';;;')
        legs = ratios.split(',')

        legs_dict = {}
        for leg in legs:
            leg_conid, ratio = leg.split('/')
            legs_dict[leg_conid] = ratio
        
        return legs_dict
    
    def get_order_data_from_order(self, order):
        try:
            conid = None
            side = None
            orderType = None

            if order.is_buy_order():
                side = "BUY"
            elif order.is_sell_order():
                side = "SELL"
            else:
                logging.error(colored("Order Side Not Found", "red"))
                return None
            
            orderType = ORDERTYPE_MAPPING[order.type]

            conid = self.data_source.get_conid_from_asset(order.asset)

            if conid is None:
                asset_type = order.asset.asset_type
                expiry_date = order.asset.expiration if hasattr(order.asset, 'expiration') else 'N/A'
                logging.error(colored(f"Couldn't find an appropriate asset for {order.asset} (Type: {asset_type}, Expiry: {expiry_date}).", "red"))
                return None
            
            data = {
                "conid": conid,
                "quantity": order.quantity,
                "orderType": orderType,
                "side": side,
                "tif": order.time_in_force.upper(),
                "price": order.limit_price,
                "auxPrice": order.stop_price,
                "listingExchange": order.exchange
            }

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
            logging.error(colored(f"An error occurred while processing the order: {str(e)}", "red"))   
            logging.error(colored(f"Error details:", "red"), exc_info=True)
            return None      

    def get_order_data_from_orders(self, orders: list[Order]):
        order_data = {
                'orders': []
            }
        
        for order in orders:
            data = self.get_order_data_from_order(order)
            if data is not None:
                order_data['orders'].append(data)

        return order_data if order_data['orders'] else None
    
    def get_order_data_multileg(self, orders: list[Order], order_type=None, duration=None, price=None):
        # TODO buy_to_open and sell_to_open not respected
        order_data = {
                'orders': []
            }
        
        spread_conid = SPREAD_CONID_MAP[orders[0].quote.symbol]
        if spread_conid is None:
            logging.error(colored("Spread conid Not Found", "red"))
            return None
        
        # Build Conidex {spread_conid};;;{leg_conid1}/{ratio},{leg_conid2}/{ratio}
        conidex = f'{spread_conid};;;'

        # conid:quantity
        ratios = []

        for order in orders:
            side = None
            conid = None
            
            if order.is_buy_order():
                side = "BUY"
            elif order.is_sell_order():
                side = "SELL"
            else:
                logging.error(colored("Order Side Not Found", "red"))
                return None
                            
            conid = self.data_source.get_conid_from_asset(order.asset)
            if conid is None:
                logging.error(colored("Order conid Not Found", "red"))
                return None
            
            quantity = order.quantity
            if quantity == 0 or quantity is None:
                return None

            if side == "SELL":
                quantity = -quantity
            
            ratios.append((conid, quantity))

        # Fixing order_quantity
        quantities = []
        for _, quant in ratios:
            quantities.append(quant)

        # Make quantities as small as possible in the conidex. Not really necessary but it's prettier that way
        order_quantity = gcd(*quantities)

        first_order = True
        for conid, quantity in ratios:
            if first_order:
                first_order = False
            else:
                conidex += ","
            conidex += f'{conid}/{quantity // order_quantity}'
        
        # side = BUY, buys are indicated with a positive quantity in the conidex and sells with a negative
        side = "BUY"
        
        data = {
            "conidex": conidex,
            "quantity": order_quantity,
            "orderType": ORDERTYPE_MAPPING[order_type if order_type is not None else order.type],
            "side": side,
            "tif": duration.upper() if duration is not None else order.time_in_force.upper(),
            "price": float(price) if price is not None else None,
            "auxPrice": order.stop_price,
            "listingExchange": order.exchange
        }

        '''
        if order.trail_percent:
            data["trailingType"] = "%"
            data["trailingAmt"] = order.trail_percent

        if order.trail_price:
            data["trailingType"] = "amt"
            data["trailingAmt"] = order.trail_price
        ''' # TODO for later consideration

        # Remove items with value None from order_data
        data = {k: v for k, v in data.items() if v is not None}
        order_data['orders'].append(data)   

        return order_data
        
    def get_historical_account_value(self) -> dict:
        logging.error("The function get_historical_account_value is not implemented yet for Interactive Brokers.")
        return {"hourly": None, "daily": None}
    
    def _register_stream_events(self):
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None

    def _run_stream(self):
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None
    
    def _get_stream_object(self):
        logging.error(colored("Method '_get_stream_object' is not yet implemented.", "red"))
        return None

    def _close_connection(self):
        logging.info("Closing connection to the Client Portal...")
        self.data_source.stop()
