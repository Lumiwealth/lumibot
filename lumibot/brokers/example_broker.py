import logging
from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import ExampleBrokerData

class ExampleBroker(Broker):
    """
    Example broker that demonstrates how to connect to an API.
    """

    NAME = "ExampleBroker"

    def __init__(
            self,
            config=None,
            data_source=None,
    ):
        # Check if the user has provided a data source, if not, create one
        if data_source is None:
            data_source = ExampleBrokerData()

        super().__init__(
            name=self.NAME,
            data_source=data_source,
            config=config,
        )

    # Method stubs with logging for not yet implemented methods
    def _get_balances_at_broker(self, quote_asset: Asset) -> tuple:
        logging.error(colored("Method '_get_balances_at_broker' is not yet implemented.", "red"))

        cash = 0.0
        positions_value = 0.0
        portfolio_value = 0.0

        return cash, positions_value, portfolio_value

    def _get_stream_object(self):
        logging.error(colored("Method '_get_stream_object' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        logging.error(colored("Method '_parse_broker_order' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_broker_all_orders(self) -> list[Order]:
        logging.error(colored("Method '_pull_broker_all_orders' is not yet implemented.", "red"))
        return []  # Return an empty list as a placeholder

    def _pull_broker_order(self, identifier: str) -> Order:
        logging.error(colored(f"Method '_pull_broker_order' for order_id {identifier} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Position:
        logging.error(colored(f"Method '_pull_position' for asset {asset} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_positions(self, strategy: 'Strategy') -> list[Position]:
        logging.error(colored("Method '_pull_positions' is not yet implemented.", "red"))
        return []  # Return an empty list as a placeholder

    def _register_stream_events(self):
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _run_stream(self):
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _submit_order(self, order: Order) -> Order:
        logging.error(colored(f"Method '_submit_order' for order {order} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def cancel_order(self, order_id) -> None:
        logging.error(colored(f"Method 'cancel_order' for order_id {order_id} is not yet implemented.", "red"))
        return None  # Explicitly return None

    def get_historical_account_value(self) -> dict:
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {}  # Return an empty dictionary as a placeholder