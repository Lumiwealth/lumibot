import logging
from termcolor import colored
from lumibot.entities import Asset, Bars

from .data_source import DataSource


class InteractiveBrokersRESTData(DataSource):
    """
    Data source that connects to the Interactive Brokers REST API.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "InteractiveBrokersREST"

    def __init__(self):
        super().__init__()

    # Method stubs with logging for not yet implemented methods
    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        logging.error(colored("Method 'get_chains' is not yet implemented.", "red"))
        return {}  # Return an empty dictionary as a placeholder

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ) -> Bars:
        logging.error(colored("Method 'get_historical_prices' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def get_last_price(self, asset, quote=None, exchange=None) -> float:
        logging.error(colored("Method 'get_last_price' is not yet implemented.", "red"))
        return 0.0  # Return 0.0 as a placeholder
    
    def get_quote(self, asset, quote=None, exchange=None):
        """
        This function returns the quote of an asset. The quote includes the bid and ask price.

        Parameters
        ----------
        asset: Asset
            The asset to get the quote for
        quote: Asset
            The quote asset to get the quote for (currently not used for Tradier)
        exchange: str
            The exchange to get the quote for (currently not used for Tradier)

        Returns
        -------
        dict
           Quote of the asset, including the bid, and ask price.
        """
        
        if exchange is None:
            exchange = "SMART"

        get_data_attempt = 0
        max_attempts = 2
        while get_data_attempt < max_attempts:
            try:
                result = self.client_portal.get_tick(asset, exchange=exchange, only_price=False)
                if result:
                    # If bid or ask are -1 then they are not available.
                    if result["bid"] == -1:
                        result["bid"] = None
                    if result["ask"] == -1:
                        result["ask"] = None

                    return result
                get_data_attempt += 1
            except:
                get_data_attempt += 1

        return None
    