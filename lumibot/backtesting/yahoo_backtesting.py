from datetime import datetime

from lumibot.data_sources import YahooData


class YahooDataBacktesting(YahooData):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, datetime_start, datetime_end):
        YahooData.__init__(self)
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start

    def _pull_source_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        time_shift = datetime.now() - self._datetime
        if time_delta is None:
            time_delta = time_shift
        else:
            time_delta += time_shift
        result = YahooData._pull_source_symbol_bars(
            self, symbol, length, time_unit, time_delta=time_delta
        )
        filter_criteria = result.index <= self._datetime
        result = result[filter_criteria]
        return result

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
