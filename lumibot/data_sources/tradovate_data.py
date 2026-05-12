from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from termcolor import colored

from lumibot.data_sources.data_source import DataSource
from lumibot.tools.lumibot_logger import get_logger

if TYPE_CHECKING:
    from lumibot.entities.bars import Bars

logger = get_logger(__name__)


def _config_string(config: Mapping[str, object], key: str, default: str) -> str:
    value = config.get(key, default)
    return value if isinstance(value, str) else default


class TradovateData(DataSource):
    """
    Data source that connects to the Tradovate Market Data API.
    Note: Tradovate market data is delivered via WebSocket.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "Tradovate"

    config: Mapping[str, object]
    ws_url: str
    market_data_url: str
    trading_token: str | None
    market_token: str | None
    trading_api_url: str

    def __init__(
        self,
        config: Mapping[str, object],
        trading_token: str | None = None,
        market_token: str | None = None,
    ) -> None:
        super().__init__()
        self.config = config
        # Use the market data WebSocket URL from config or default.
        self.ws_url = _config_string(config, "MD_WS_URL", "wss://md.tradovateapi.com/v1/websocket")
        # REST endpoint for market data.
        self.market_data_url = _config_string(config, "MD_URL", "https://md.tradovateapi.com/v1")
        # Store tokens directly
        self.trading_token = trading_token
        self.market_token = market_token
        # Trading API URL for contract lookup
        self.trading_api_url = _config_string(config, "TRADING_API_URL", "https://demo.tradovateapi.com/v1")

    def _get_headers(self, with_auth: bool = True, with_content_type: bool = False) -> dict[str, str]:
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

    def get_chains(self, asset: Any, quote: Any = None) -> dict[str, Any]:
        logger.error(colored("Method 'get_chains' does not work with Tradovate.", "red"))
        return {}

    def get_historical_prices(
        self,
        asset: Any,
        length: int,
        timestep: str = "",
        timeshift: timedelta | None = None,
        quote: Any = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        **kwargs: Any,
    ) -> Bars | None:
        """
        Retrieve historical chart data for the given asset via WebSocket using the md/getChart command.
        This method sends a WebSocket request to retrieve 'length' bars of historical data.

        Note: Tradovate provides historical chart data via WebSocket, not via a REST GET.
        """

        # Log that this method is not supported because Tradovate requires you to get a CME subscription which costs $440/month
        logger.error(
            colored(
                "Method 'get_historical_prices' is not implemented for Tradovate because it requires a CME subscription which costs $440/month.",
                "red",
            )
        )
        return None

    def get_last_price(self, asset: Any, quote: Any = None, exchange: str | None = None) -> float | Decimal | None:
        """
        Retrieve the most recent price for the given asset via WebSocket.
        This method first retrieves the contract ID for the asset's symbol, then subscribes
        to market data using that contract ID.
        """

        # Log that this method is not supported because Tradovate requires you to get a CME subscription which costs $440/month
        logger.error(
            colored(
                "Method 'get_last_price' is not implemented for Tradovate because it requires a CME subscription which costs $440/month.",
                "red",
            )
        )
        return None
