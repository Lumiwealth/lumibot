import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import InteractiveBrokersRESTData
from lumibot.tools import IBClientPortal
import datetime
from decimal import Decimal

from ibapi.contract import Contract

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

class InteractiveBrokersREST(Broker):
    """
    Broker that connects to the Interactive Brokers REST API.
    """

    ASSET_CLASS_MAPPING = {
        "STK": Asset.AssetType.STOCK,
        "OPT": Asset.AssetType.OPTION,
        "FUT": Asset.AssetType.FUTURE,
        "CASH": Asset.AssetType.FOREX,
    }

    NAME = "InteractiveBrokersREST"

    def __init__(self, config, data_source=None):
        if data_source is None:
            data_source = InteractiveBrokersRESTData()
        super().__init__(name=self.NAME, data_source=data_source, config=config)

        self.client_portal = IBClientPortal(config)
        self.accounts_info = self.client_portal.run()

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
        account_balances = self.client_portal.get_account_balances()

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
        cash = balances_for_quote_asset['cashbalance']

        # Get the net liquidation value for the quote asset
        total_liquidation_value = balances_for_quote_asset['netliquidationvalue']

        # Calculate the positions value
        positions_value = total_liquidation_value - cash

        return cash, positions_value, total_liquidation_value

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order: ## Prone to Bugs
        """Parse a broker order representation
        to an order object"""

        asset_type = [k for k, v in TYPE_MAP.items() if v == response.contract.secType][0]
        totalQuantity = response.totalQuantity
        limit_price = response.lmtPrice
        stop_price = response.auxPrice
        time_in_force = response.tif
        good_till_date = response.goodTillDate

        if asset_type == "multileg":
            # Create a multileg order.
            order = Order(strategy_name)
            order.order_class = Order.OrderClass.MULTILEG
            order.child_orders = []

            # Parse the legs of the combo order.
            for leg in response.contract.comboLegs:
                # Create the contract object with just the conId
                contract = Contract()
                contract.conId = leg.conId

                # Get the contract details for the leg.
                res = self.client_portal.get_contract_details_for_contract(contract)
                contract = res[0].contract

                action = leg.action
                child_order = self._parse_order_object(strategy_name, contract, leg.ratio * totalQuantity, action, limit_price, stop_price, time_in_force, good_till_date)
                order.child_orders.append(child_order)

        else:
            action = response.action
            order = self._parse_order_object(strategy_name, response.contract, totalQuantity, action, limit_price, stop_price, time_in_force, good_till_date)
        
        order._transmitted = True
        order.set_identifier(response.orderId)
        order.status = response.orderState.status
        order.update_raw(response)
        return order

    def _parse_order_object(self, strategy_name, contract, quantity, action, limit_price = None, stop_price = None, time_in_force = None, good_till_date = None):
        expiration = None
        multiplier = 1
        if contract.secType in ["OPT", "FUT"]:
            expiration = datetime.datetime.strptime(
                contract.lastTradeDateOrContractMonth,
                DATE_MAP[[d for d, v in TYPE_MAP.items() if v == contract.secType][0]],
            )
            multiplier = contract.multiplier

        right = None
        strike = None
        if contract.secType == "OPT":
            right = "CALL" if contract.right == "C" else "PUT"
            strike = contract.strike

        order = Order(
            strategy_name,
            Asset(
                symbol=contract.localSymbol,
                asset_type=[k for k, v in TYPE_MAP.items() if v == contract.secType][0],
                expiration=expiration,
                strike=strike,
                right=right,
                multiplier=multiplier,
            ),
            quantity = Decimal(quantity),
            side = action.lower(),
            limit_price = limit_price if limit_price != 0 else None,
            stop_price = stop_price if stop_price != 0 else None,
            time_in_force = time_in_force,
            good_till_date = good_till_date,
            quote = Asset(symbol=contract.currency, asset_type="forex"),
        )   

        return order

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
        orders = self.client_portal.get_open_orders()
        return orders
    
    def _pull_broker_order(self, identifier: str) -> Order:
        """Get a broker order representation by its id"""
        pull_order = [order for order in self.client_portal.get_open_orders() if order.orderId == order_id]
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
        ib_positions = self.client_portal.get_positions()
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
        positions = self.client_portal.get_positions()

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
            asset_class = self.ASSET_CLASS_MAPPING[position['assetClass']]

            # If asset class is stock, create a stock asset
            if asset_class == Asset.AssetType.STOCK:
                asset = Asset(symbol=symbol, asset_type=asset_class)
            elif asset_class == Asset.AssetType.OPTION:
                # TODO: check if this is right, was generated by AI
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
        logging.error(colored(f"Method '_submit_order' for order {order} is not yet implemented.", "red"))
        return None

    def cancel_order(self, order_id) -> None:
        logging.error(colored(f"Method 'cancel_order' for order_id {order_id} is not yet implemented.", "red"))
        return None

    def get_historical_account_value(self) -> dict:
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {}
    
    def _register_stream_events(self):
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None

    def _run_stream(self):
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None
    
    def _get_stream_object(self):
        logging.error(colored("Method '_get_stream_object' is not yet implemented.", "red"))
        return None