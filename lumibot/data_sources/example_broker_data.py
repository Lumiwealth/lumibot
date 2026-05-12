from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

from termcolor import colored

from lumibot.data_sources.data_source import DataSource
from lumibot.entities.asset import Asset
from lumibot.entities.bars import Bars
from lumibot.entities.quote import Quote
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class ExampleBrokerData(DataSource):
    """
    Data source that connects to the Example Broker API.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "ExampleBroker"

    def __init__(self, **kwargs: Any) -> None:
        del kwargs
        super().__init__()

    # Method stubs with logging for not yet implemented methods
    def get_chains(self, asset: Any, quote: Any = None) -> dict[str, Any]:
        logger.error(colored("Method 'get_chains' is not yet implemented.", "red"))
        return {}  # Return an empty dictionary as a placeholder

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
        logger.error(colored("Method 'get_historical_prices' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def get_last_price(self, asset: Any, quote: Any = None, exchange: str | None = None) -> float | Decimal | None:
        logger.error(colored("Method 'get_last_price' is not yet implemented.", "red"))
        return 0.0  # Return 0.0 as a placeholder

    def get_quote(self, asset: Asset, quote: Asset | None = None, exchange: str | None = None) -> Quote:
        """
        Get the latest quote for an asset.
        This is a placeholder implementation that returns an empty Quote object.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.

        Returns
        -------
        Quote
            A Quote object with the quote information.
        """
        logger.error(colored("Method 'get_quote' is not yet implemented.", "red"))
        return Quote(asset=asset)
