from datetime import datetime

from lumibot.data_sources import YahooData


class YahooDataBacktesting(YahooData):
    IS_BACKTESTING_DATA_SOURCE = True
    MIN_TIMESTEP = YahooData.MIN_TIMESTEP

    def __init__(self, datetime_start, datetime_end, **kwargs):
        YahooData.__init__(self)
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start

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

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
