import logging
from decimal import Decimal
from typing import Union

from termcolor import colored
from lumibot.entities import Asset, Bars
from lumibot.data_sources import DataSource

class TradeovateData(DataSource):
    """
    Data source that connects to the Tradovate Market Data API.
    Note: Tradovate market data is delivered via WebSocket.
    """
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradeovate"

    def __init__(self, config, trading_token=None, market_token=None):
        super().__init__()
        self.config = config
        # Use the market data WebSocket URL from config or default.
        self.ws_url = config.get("MD_WS_URL", "wss://md.tradovateapi.com/v1/websocket")
        # REST endpoint for market data.
        self.market_data_url = config.get("MD_URL", "https://md.tradovateapi.com/v1")
        # Store tokens directly
        self.trading_token = trading_token
        self.market_token = market_token
        # Trading API URL for contract lookup
        self.trading_api_url = config.get("TRADING_API_URL", "https://demo.tradovateapi.com/v1")

    def _get_headers(self, with_auth=True, with_content_type=False):
        """
        Create headers for API requests.
        
        Parameters
        ----------
        with_auth : bool
            Whether to include the Authorization header with the trading token
        with_content_type : bool
            Whether to include Content-Type header for JSON requests
            
        Returns
        -------
        dict
            Dictionary of headers for API requests
        """
        headers = {"Accept": "application/json"}
        if with_auth and self.trading_token:
            headers["Authorization"] = f"Bearer {self.trading_token}"
        if with_content_type:
            headers["Content-Type"] = "application/json"
        return headers

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        logging.error(colored("Method 'get_chains' does not work with Tradovate.", "red"))
        return {}

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ) -> Bars:
        """
        Retrieve historical chart data for the given asset via WebSocket using the md/getChart command.
        This method sends a WebSocket request to retrieve 'length' bars of historical data.
        
        Note: Tradovate provides historical chart data via WebSocket, not via a REST GET.
        """

        # Log that this method is not supported because Tradovate requires you to get a CME subscription which costs $440/month
        logging.error(colored("Method 'get_historical_prices' is not implemented for Tradovate because it requires a CME subscription which costs $440/month.", "red"))
        return None

    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        Retrieve the most recent price for the given asset via WebSocket.
        This method first retrieves the contract ID for the asset's symbol, then subscribes
        to market data using that contract ID.
        """
       
        # Log that this method is not supported because Tradovate requires you to get a CME subscription which costs $440/month
        logging.error(colored("Method 'get_last_price' is not implemented for Tradovate because it requires a CME subscription which costs $440/month.", "red"))
        return None