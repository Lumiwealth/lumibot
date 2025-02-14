import logging
from datetime import datetime

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import (
    CryptoBarsRequest,
    StockBarsRequest,
)
from alpaca.data.timeframe import TimeFrame

from lumibot.components.no_cache_system import NoCacheSystem
from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset
from lumibot.backtesting.base_pandas_backtesting import BasePandasBacktesting
from lumibot.components.cache_system import CacheSystem

logger = logging.getLogger(__name__)


class AlpacaBacktesting(BasePandasBacktesting):
    """
    Backtesting implementation for the Alpaca data source.
    Inherits common functionality from BasePandasBacktesting and implements
    custom logic for Alpaca-specific integration.
    """

    def __init__(
            self,
            datetime_start: datetime,
            datetime_end: datetime,
            max_memory: int = None,
            cache_system: CacheSystem = None,
            config: dict = None,
            **kwargs
    ):
        if cache_system is None:
            alpaca_data_source = AlpacaData(config)
            cache_system = NoCacheSystem(data_source=alpaca_data_source)

        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            max_memory=max_memory,
            cache_system=cache_system,
            **kwargs
        )