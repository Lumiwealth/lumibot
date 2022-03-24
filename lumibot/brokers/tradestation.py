import asyncio
import datetime
import logging
import traceback
from asyncio import CancelledError
from datetime import timezone
from decimal import Decimal

import TradeStation_trade_api as tradeapi
from dateutil import tz

from lumibot.data_sources import TradeStationData
from lumibot.entities import Asset, Order, Position

from .broker import Broker


class Tradestation(TradeStationData, Broker):
    """A broker class that connects to Tradestation

    Attributes
    ----------
    api : tradeapi.REST
        TradeStation API object

    Methods
    -------
    get_timestamp()
        Returns the current UNIX timestamp representation from TradeStation

    is_market_open()
        Determines if the market is open.

    get_time_to_open()
        How much time in seconds remains until the market next opens?

    get_time_to_close()
        How much time in seconds remains until the market closes?

    Examples
    --------
    >>> # Connect to TradeStation
    >>> from lumibot.brokers import TradeStation
    >>> class TradeStationConfig:
    ...     API_KEY = 'your_api_key'
    ...     SECRET_KEY = 'your_secret_key'
    ...     ENDPOINT = 'https://api.tradestation.com/v3'
    >>> tradestation = TradeStation(TradeStationConfig)
    >>> print(tradestation.get_time_to_open())
    >>> print(tradestation.get_time_to_close())
    >>> print(tradestation.is_market_open())

    >>> # Run a strategy on TradeStation
    >>> from lumibot.strategies import Strategy
    >>> from lumibot.brokers import TradeStation
    >>> from lumibot.traders import Trader
    >>>
    >>> class TradeStationConfig:
    ...     # Put your own TradeStation key here:
    ...     API_KEY = "YOUR_API_KEY"
    ...     # Put your own TradeStation secret here:
    ...     API_SECRET = "YOUR_API_SECRET"
    ...     ENDPOINT = "https://api.tradestation.com/v3"
    >>>
    >>> class TradeStationStrategy(Strategy):
    ...     def on_trading_interation(self):
    ...         if self.broker.is_market_open():
    ...             self.create_order(
    ...                 asset=Asset(symbol="AAPL"),
    ...                 quantity=1,
    ...                 order_type="market",
    ...                 side="buy",
    ...             )
    >>>
    >>> TradeStation = TradeStation(TradeStationConfig)
    >>> strategy = TradeStationStrategy(broker=TradeStation)
    >>> trader = Trader()
    >>> trader.add_strategy(strategy)
    >>> trader.run()

    """

    # ASSET_TYPE_MAP = dict(
    #     stock=["us_equity"],
    #     option=[],
    #     future=[],
    #     forex=[],
    # )

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True):
        # Calling init methods
        TradeStationData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
        Broker.__init__(self, name="tradestation", connect_stream=connect_stream)
        self.market = "NASDAQ"

    # =========Clock functions=====================

    def get_timestamp(self):
        """Returns the current UNIX timestamp representation from TradeStation

        Parameters
        ----------
        None

        Returns
        -------
        int
            Sample unix timestamp return value: 1612172730.000234

        """
        

    def is_market_open(self):
        """Determines if the market is open.

        Parameters
        ----------
        None

        Returns
        -------
        boolean
            True if market is open, false if the market is closed.

        Examples
        --------
        >>> self.is_market_open()
        True
        """
        

    def get_time_to_open(self):
        """How much time in seconds remains until the market next opens?

        Return the remaining time for the market to open in seconds

        Parameters
        ----------
        None

        Returns
        -------
        int
            Number of seconds until open.

        Examples
        --------
        If it is 0830 and the market next opens at 0930, then there are 3,600
        seconds until the next market open.

        >>> self.get_time_to_open()
        """
       

    def get_time_to_close(self):
        """How much time in seconds remains until the market closes?

        Return the remaining time for the market to closes in seconds

        Parameters
        ----------
        None

        Returns
        -------
        int
            Number of seconds until close.

        Examples
        --------
        If it is 1400 and the market closes at 1600, then there are 7,200
        seconds until the market closes.
        """

    # =========Positions functions==================

     def _get_balances_at_broker(self, account_keys: List[str]) -> dict:
        """Grabs all the balances for each account provided.
        Args:
        ----
        account_keys (List[str]): A list of account numbers. Can only be a max
            of 25 account numbers
        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.
        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # validate the token.
            self._token_validation()

            # argument validation.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys.")
            elif len(account_keys) > 0 and len(account_keys) <= 25:
                account_keys = ','.join(account_keys)
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys.")

            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='accounts/{account_numbers}/balances'.format(
                    account_numbers=account_keys)
            )

            # define the arguments
            params = {
                'access_token': self.state['access_token']
            }

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                args=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

        

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
       

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        

    # =======Orders and assets functions=========
    

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""
        

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        

   
        

    def _submit_order(self, order):
        """Submit an order for an asset"""
       

    def cancel_order(self, order):
        """Cancel an order

        Parameters
        ----------
        order : Order
            The order to cancel

        Returns
        -------
        Order
            The order that was cancelled
        """
        

    # =======Account functions=========

    def _parse_historical_account_value(self, df_account_values):
       

    def get_historical_account_value(self):
       

    # =======Stream functions=========

    def _get_stream_object(self):
       

    def _register_stream_events(self):
       

    def _run_stream(self):
        """Overloading default TradeStation_trade_api.STreamCOnnect().run()
        Run forever and block until exception is raised.
        initial_channels is the channels to start with.
        """
       
