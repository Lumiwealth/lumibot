from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from lumibot.data_sources.data_source_backtesting import DateTimeInput
from lumibot.data_sources.yahoo_data import YahooData


class YahooDataBacktesting(YahooData):
    """
    YahooDataBacktesting is a DataSourceBacktesting that uses YahooData as a
    backtesting data source.
    """

    def __init__(self, datetime_start: DateTimeInput, datetime_end: DateTimeInput, **kwargs: Any) -> None:
        # Call super().__init__ to ensure the MRO is followed correctly
        # Fix: Don't pass self as an argument to super().__init__
        super().__init__(
            datetime_start=cast(datetime | None, datetime_start),
            datetime_end=cast(datetime | None, datetime_end),
            **kwargs,
        )
