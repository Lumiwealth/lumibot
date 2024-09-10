import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import InteractiveBrokersRESTData
from lumibot.tools import IBClientPortal

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

    def __init__(self, config=None, data_source=None):
        if data_source is None:
            data_source = InteractiveBrokersRESTData()
        super().__init__(name=self.NAME, data_source=data_source, config=config)

        self.client_portal = IBClientPortal()
        self.accounts_info = self.client_portal.run()

        # Get account information
        # TODO: Add support for multiple accounts
        self.account_id = self.accounts_info['accounts'][0]['id']

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
        account_balances = self.client_portal.get_account_balances(self.account_id)

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

    def _get_stream_object(self):
        logging.error(colored("Method '_get_stream_object' is not yet implemented.", "red"))
        return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        logging.error(colored("Method '_parse_broker_order' is not yet implemented.", "red"))
        return None

    def _pull_broker_all_orders(self) -> list[Order]:
        logging.error(colored("Method '_pull_broker_all_orders' is not yet implemented.", "red"))
        return []

    def _pull_broker_order(self, identifier: str) -> Order:
        logging.error(colored(f"Method '_pull_broker_order' for order_id {identifier} is not yet implemented.", "red"))
        return None

    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Position:
        logging.error(colored(f"Method '_pull_position' for asset {asset} is not yet implemented.", "red"))
        return None

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
        positions = self.client_portal.get_positions(self.account_id)

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

    def _register_stream_events(self):
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None

    def _run_stream(self):
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None

    def _submit_order(self, order: Order) -> Order:
        logging.error(colored(f"Method '_submit_order' for order {order} is not yet implemented.", "red"))
        return None

    def cancel_order(self, order_id) -> None:
        logging.error(colored(f"Method 'cancel_order' for order_id {order_id} is not yet implemented.", "red"))
        return None

    def get_historical_account_value(self) -> dict:
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {}
