import logging
from typing import Union

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
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Get the actual cash balance at the broker.
        
        Parameters
        ----------
        quote_asset : Asset
            The quote asset to get the balance of (e.g., USD, EUR).
        strategy : Strategy
            The strategy object that is requesting the balance.

        Returns
        -------
        tuple of float
            A tuple containing (cash, positions_value, total_liquidation_value).
            Cash = cash in the account (whatever the quote asset is).
            Positions value = the value of all the positions in the account.
            Portfolio value = the total equity value of the account (aka. portfolio value).
        """
        logging.error(colored("Method '_get_balances_at_broker' is not yet implemented.", "red"))

        cash = 0.0
        positions_value = 0.0
        portfolio_value = 0.0

        return cash, positions_value, portfolio_value

    def _get_stream_object(self):
        """
        Get the broker stream connection. This method should return an object that handles 
        the streaming connection to the broker's API.
        
        Returns
        -------
        object
            The stream object that will handle the streaming connection.
        """
        logging.info(colored("Method '_get_stream_object' is not yet implemented.", "yellow"))
        return None  # Return None as a placeholder

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        """
        Parse a broker order representation to an order object.

        Parameters
        ----------
        response : dict
            The broker order representation, typically from API response.
        strategy_name : str
            The name of the strategy that placed the order.
        strategy_object : Strategy, optional
            The strategy object that placed the order.

        Returns
        -------
        Order
            The order object created from the broker's response.
        """
        logging.error(colored("Method '_parse_broker_order' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_broker_all_orders(self) -> list:
        """
        Get the broker's open orders.

        Returns
        -------
        list
            A list of order responses from the broker query. These will be passed to 
            _parse_broker_order() to be converted to Order objects.
        """
        logging.error(colored("Method '_pull_broker_all_orders' is not yet implemented.", "red"))
        return []  # Return an empty list as a placeholder

    def _pull_broker_order(self, identifier: str) -> dict:
        """
        Get a broker order representation by its id.

        Parameters
        ----------
        identifier : str
            The identifier of the order to pull.

        Returns
        -------
        dict
            The order representation from the broker, or None if not found.
        """
        logging.error(colored(f"Method '_pull_broker_order' for order_id {identifier} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Position:
        """
        Pull a single position from the broker that matches the asset and strategy. If no position is found, None is
        returned.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull.
        asset: Asset
            The asset to pull the position for.

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None.
        """
        logging.error(colored(f"Method '_pull_position' for asset {asset} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _pull_positions(self, strategy: 'Strategy') -> list[Position]:
        """
        Get the account positions. Returns a list of position objects.

        Parameters
        ----------
        strategy : Strategy
            The strategy object to pull the positions for.

        Returns
        -------
        list[Position]
            A list of position objects.
        """
        logging.error(colored("Method '_pull_positions' is not yet implemented.", "red"))
        return []  # Return an empty list as a placeholder

    def _register_stream_events(self):
        """
        Register the function on_trade_event to be executed on each trade_update event.
        This method should set up callbacks for various order events from the broker's stream.
        """
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _run_stream(self):
        """
        Start and run the broker's data stream. This method is typically executed in a separate thread
        and manages the connection to the broker's streaming API.
        """
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def _submit_order(self, order: Order) -> Order:
        """
        Submit an order to the broker after necessary checks and input sanitization.
        
        Parameters
        ----------
        order : Order
            The order to submit to the broker.

        Returns
        -------
        Order
            Updated order with broker identifier filled in, or None if submission failed.
        """
        logging.error(colored(f"Method '_submit_order' for order {order} is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def cancel_order(self, order: Order) -> None:
        """
        Cancel an order at the broker. Nothing will be done for orders that are already cancelled or filled.
        
        Parameters
        ----------
        order : Order
            The order to cancel.

        Returns
        -------
        None
        """
        logging.error(colored(f"Method 'cancel_order' for order {order} is not yet implemented.", "red"))
        return None  # Explicitly return None

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        
        Parameters
        ----------
        order : Order
            The order to modify.
        limit_price : float, optional
            The new limit price for the order.
        stop_price : float, optional
            The new stop price for the order.

        Returns
        -------
        None
        """
        logging.error(colored(f"Method '_modify_order' for order {order} is not yet implemented.", "red"))
        return None

    def get_historical_account_value(self) -> dict:
        """
        Get the historical account value of the account.
        
        Returns
        -------
        dict
            A dictionary with keys 'hourly' and 'daily', each containing a DataFrame with
            historical account values. If not implemented, returns an empty dictionary.
        """
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {"hourly": None, "daily": None}  # Return a dictionary with empty values as a placeholder
