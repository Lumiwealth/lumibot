from lumi_tradier import Tradier

from .data_source import DataSource


class TradierAPIError(Exception):
    pass


class TradierData(DataSource):
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradier"

    def __init__(self, account_id, api_key, paper=True, max_workers=20):
        super().__init__(api_key=api_key)
        self._account_id = account_id
        self._paper = paper
        self.max_workers = min(max_workers, 50)
        self.tradier = Tradier(account_id, api_key, paper)

    def _pull_source_symbol_bars(self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None,
                                 include_after_hours=True):
        pass

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None,
                          include_after_hours=True):
        pass

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        pass

    def get_last_price(self, asset, quote=None, exchange=None):
        """
        This function returns the last price of an asset.
        Parameters
        ----------
        asset
        quote
        exchange

        Returns
        -------
        float
           Price of the asset
        """
        price = self.tradier.market.get_last_price(asset.symbol)
        return price
