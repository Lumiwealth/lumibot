from lumibot.brokers import Broker
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import parse_symbol
from lumiwealth_tradier import Tradier as _Tradier


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
        pass

    def _submit_order(self, order: Order):
        pass

    def _get_balances_at_broker(self, quote_asset: Asset) -> float:
        pass

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
                strategy=strategy.name,
                asset=asset,
                quantity=quantity,
            )

            # Add the position to the list
            positions_ret.append(position)

        return positions_ret

    def _pull_position(self, strategy, asset):
        pass

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        pass

    def _pull_broker_order(self, identifier):
        pass

    def _pull_broker_open_orders(self):
        pass
