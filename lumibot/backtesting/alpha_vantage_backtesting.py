from __future__ import annotations

from typing import Any

from lumibot.data_sources.alpha_vantage_data import AlphaVantageData
from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting, DateTimeInput


class AlphaVantageBacktesting(DataSourceBacktesting, AlphaVantageData):
    def __init__(self, datetime_start: DateTimeInput, datetime_end: DateTimeInput, **kwargs: Any) -> None:
        raise Exception("AlphaVantageBacktesting is not currently operational")

        AlphaVantageData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
