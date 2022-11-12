from .data_source import DataSource


class TradovateData(DataSource):
    """Common base class for data_sources/tradovate and brokers/tradovate """
    
    SOURCE = "Tradovate"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [
                "1 min",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "1 day",
            ],
        },
    ]
    
    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        pass
    
    def get_last_price(self, asset, quote=None, exchange=None, **kwargs):
        pass
    
    def get_barset_from_api(
        self, api, asset, freq, limit=None, end=None, start=None, quote=None
    ):
        pass
    
    
    # ---- should we remove these methods? ----
    
    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None
    ):
        pass
    
    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=MIN_TIMESTEP,
        timeshift=None,
        quote=None,
        exchange=None,
    ):
        pass
    
    def _parse_source_symbol_bars(self, response, asset, quote=None):
        pass
    