from .data_source import DataSource

TRADIER_LIVE_API_URL = "https://api.tradier.com/v1/"
TRADIER_PAPER_API_URL = "https://sandbox.tradier.com/v1/"
TRADIER_STREAM_API_URL = "https://stream.tradier.com/v1/"  # Only valid Live, no Paper support


class TradierAPIError(Exception):
    pass


class TradierData(DataSource):
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradier"

    def __init__(self, account_id, api_key, paper=True, max_workers=20):
        super().__init__(api_key=api_key)
        self._account_id = account_id
        self._paper = paper
        self._base_url = TRADIER_PAPER_API_URL if self._paper else TRADIER_LIVE_API_URL
        self.max_workers = min(max_workers, 50)

    def _pull_source_symbol_bars(self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None,
                                 include_after_hours=True):
        pass

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None,
                          include_after_hours=True):
        pass

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        pass

    def get_last_price(self, asset, quote=None, exchange=None):
        pass
