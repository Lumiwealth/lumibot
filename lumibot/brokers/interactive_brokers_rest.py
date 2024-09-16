import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import InteractiveBrokersRESTData
import datetime
from decimal import Decimal

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

    def __init__(self, config, data_source=None, api_url=None):              
        if data_source is None:
            data_source = InteractiveBrokersRESTData(config, api_url)
        super().__init__(name=self.NAME, data_source=data_source, config=config)

    # --------------------------------------------------------------
    # Broker methods
    # --------------------------------------------------------------

    # Existing method stubs with logging
    def _get_balances_at_broker(self, quote_asset: Asset) -> tuple:
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
        # Get the account balances from the Interactive Brokers Client Portal
        account_balances = self.data_source.get_account_balances()

        # Check that the account balances were successfully retrieved
        if account_balances is None:
            logging.error(colored("Failed to retrieve account balances.", "red"))
            return 0.0, 0.0, 0.0

        # Get the quote asset symbol
        quote_symbol = quote_asset.symbol

        # Get the account balances for the quote asset
        balances_for_quote_asset = account_balances[quote_symbol]

        # Exmaple account balances response:
        # {'commoditymarketvalue': 0.0, 'futuremarketvalue': 677.49, 'settledcash': 202142.17, 'exchangerate': 1, 'sessionid': 1, 'cashbalance': 202142.17, 'corporatebondsmarketvalue': 0.0, 'warrantsmarketvalue': 0.0, 'netliquidationvalue': 202464.67, 'interest': 452.9, 'unrealizedpnl': 12841.38, 'stockmarketvalue': -130.4, 'moneyfunds': 0.0, 'currency': 'USD', 'realizedpnl': 0.0, 'funds': 0.0, 'acctcode': 'DU4299039', 'issueroptionsmarketvalue': 0.0, 'key': 'LedgerList', 'timestamp': 1724382002, 'severity': 0, 'stockoptionmarketvalue': 0.0, 'futuresonlypnl': 677.49, 'tbondsmarketvalue': 0.0, 'futureoptionmarketvalue': 0.0, 'cashbalancefxsegment': 0.0, 'secondkey': 'USD', 'tbillsmarketvalue': 0.0, 'endofbundle': 1, 'dividends': 0.0, 'cryptocurrencyvalue': 0.0}

        # Get the cash balance for the quote asset
        cash = balances_for_quote_asset['settledcash']

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
                # Create the contract object with just the conId
                leg_secType = self.data_source.get_sectype_from_conid(leg)
                child_order = self._parse_order_object(strategy_name=strategy_name,
                                                       response=response,
                                                       quantity=float(ratio) * totalQuantity, ## maybe not how ratios work
                                                       conId=leg,
                                                       secType=leg_secType
                                                       )
                order.child_orders.append(child_order)

        else:
            order = self._parse_order_object(strategy_name=strategy_name, 
                                             response=response,
                                             quantity=totalQuantity,
                                             conId=response['conid'],
                                             secType=asset_type
                                             )
        
        order._transmitted = True
        order.set_identifier(response['orderId'])
        order.status = response['status'],
        order.update_raw(response)
        return order

    def _parse_order_object(self, strategy_name, response, quantity, secType, conId):
        side=response['side']
        symbol=response['ticker']
        currency=response['cashCcy']
        time_in_force=response['timeInForce']
        limit_price = response['price'] if 'price' in response and response['price'] != '' else None
        stop_price = response['stop_price'] if 'stop_price' in response and response['stop_price'] != '' else None
        good_till_date = response['goodTillDate'] if 'goodTillDate' in response and response['goodTillDate'] != '' else None
        #secType = ASSET_CLASS_MAPPING[secType]
        ## rethink the fields
        
        contract_details = self.data_source.get_contract_details(conId)
        if contract_details is None:
            contract_details = {}

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
                ## TODO: check if this is right, was generated by AI
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
            else:
                logging.error(colored(f"Asset class not supported yet (we need to add code for this asset type): {asset_class} for position {position}", "red"))
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
        
    def _submit_order(self, order: Order) -> Order:
        try:
            order_data = self.get_order_data_from_orders([order])
            order.identifier = self.data_source.execute_order(order_data)
            order.status = "submitted"
            self._unprocessed_orders.append(order)

            return order
        
        except Exception as e:
            logging.error(colored(f"An error occurred while submitting the order: {str(e)}", "red"))
            logging.error(colored(f"Error details:", "red"), exc_info=True)
            return order

    def submit_orders(self, orders: list[Order], is_multileg=False, order_type="market", duration="day", price=None): ## add multileg
        ## cant place order if there is open order
        try:
            if is_multileg:
                order_data = self.get_order_data_multileg(orders, order_type=order_type, duration=duration, price=price)
                response = self.data_source.execute_order(order_data)

                ## may need a orders.child_orders[] implementation?
                ## merge all orders into one?

                ## temp solution
                for order in orders:
                    order.status = "submitted"
                    order.identifier = response[0]['order_id']
                    self._unprocessed_orders.append(order)
            else:
                order_data = self.get_order_data_from_orders([order])
                response = self.data_source.execute_order(order_data)
            
                order_id = 0
                for order in orders:
                    order.status = "submitted"
                    order.identifier = response[order_id]['order_id']
                    self._unprocessed_orders.append(order)
                    order_id+=1

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
    
    def get_order_data_from_orders(self, orders: list[Order]):
        ## buy_to_open and sell_to_open not respected
        order_data = {
                'orders': []
            }
        
        for order in orders:
            try:
                conid = None
                side = None
                orderType = None

                if order.is_buy_order():
                    side = "BUY"
                elif order.is_sell_order():
                    side = "SELL"
                else:
                    raise Exception("Order Side Not Found")
                
                orderType = ORDERTYPE_MAPPING[order.type]

                conid = self.data_source.get_conid_from_asset(order.asset)

                if conid is None:
                    raise Exception("Order conid Not Found")

                data = {
                    "conid": conid, # required
                    "quantity": order.quantity, # required
                    "orderType": orderType, # required
                    "side": side, # required
                    "tif": order.time_in_force.upper(), # required
                    "price": order.limit_price,
                    "auxPrice": order.stop_price,
                    "listingExchange": order.exchange
                    ### Add other necessary fields based on the Order object
                }

                if order.trail_percent:
                    data["trailingType"] = "%"
                    data["trailingAmt"] = order.trail_percent

                if order.trail_price:
                    data["trailingType"] = "amt"
                    data["trailingAmt"] = order.trail_price

                # Remove items with value None from order_data
                data = {k: v for k, v in data.items() if v is not None}
                order_data['orders'].append(data)
        
            except Exception as e:
                logging.error(colored(f"An error occurred while processing the order: {str(e)}", "red"))            

        return order_data

    def get_order_data_multileg(self, orders: list[Order], order_type=None, duration=None, price=None):
        ## buy_to_open and sell_to_open not respected
        order_data = {
                'orders': []
            }

        conid = self.data_source.get_conid_from_asset(orders[0].asset)
        spread_conid = SPREAD_CONID_MAP[orders[0].quote.symbol]

        # Build Conidex {spread_conid};;;{leg_conid1}/{ratio},{leg_conid2}/{ratio}
        conidex = f'{spread_conid};;;'

        first_order = True
        for order in orders:
            side = None
            conid = None
            
            if order.is_buy_order():
                side = "BUY"
            elif order.is_sell_order():
                side = "SELL"
            else:
                raise Exception("Order Side Not Found")
                
            conid = self.data_source.get_conid_from_asset(order.asset)
            quantity = order.quantity
            if first_order:
                first_order = False
            else:
                conidex += ","

            if side == "SELL":
                quantity = -quantity

            conidex += f'{conid}/{quantity}'

            if conid is None:
                raise Exception("Order conid Not Found")
            
        side = "BUY"
                
        data = {
            "conidex": conidex, # required
            "quantity": 1, # required
            "orderType": ORDERTYPE_MAPPING[order_type if order_type is not None else order.type], # required
            "side": side, # required
            "tif": duration.upper() if duration is not None else order.time_in_force.upper(), # required
            "price": float(price),
            "auxPrice": order.stop_price,
            "listingExchange": order.exchange
            ### Add other necessary fields based on the Order object
        }

        '''
        if order.trail_percent:
            data["trailingType"] = "%"
            data["trailingAmt"] = order.trail_percent

        if order.trail_price:
            data["trailingType"] = "amt"
            data["trailingAmt"] = order.trail_price
        ''' ## for later consideration

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
