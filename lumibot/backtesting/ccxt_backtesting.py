from __future__ import annotations

from typing import Any

from lumibot.data_sources.ccxt_backtesting_data import CcxtBacktestingData
from lumibot.data_sources.data_source_backtesting import DateTimeInput


class CcxtBacktesting(CcxtBacktestingData):
    def __init__(self, datetime_start: DateTimeInput, datetime_end: DateTimeInput, **kwargs: Any) -> None:
        CcxtBacktestingData.__init__(self, datetime_start, datetime_end, **kwargs)
