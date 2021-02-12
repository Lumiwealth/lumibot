from datetime import datetime

from lumibot.data_sources import YahooData

from .data_source_backtesting import DataSourceBacktesting


class YahooDataBacktesting(YahooData, DataSourceBacktesting):
    MIN_TIMESTEP = YahooData.MIN_TIMESTEP

    def __init__(self, datetime_start, datetime_end, **kwargs):
        YahooData.__init__(self)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        backtesting_timeshift = datetime.now() - self._datetime
        if timeshift:
            backtesting_timeshift += timeshift

        result = YahooData._pull_source_symbol_bars(
            self, symbol, length, timestep=timestep, timeshift=backtesting_timeshift
        )
        filter_criteria = result.index <= self.localize_datetime(self._datetime)
        result = result[filter_criteria]
        return result
