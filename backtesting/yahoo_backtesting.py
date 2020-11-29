from datetime import datetime

from data_sources import YahooData


class YahooDataBacktesting(YahooData):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, timestamp_start, timestamp_end):
        YahooData.__init__(self)
        self.timestamp_start = timestamp_start
        self.timestamp_end = timestamp_end
        self._timestamp = timestamp_start

    def _pull_source_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        time_shift = datetime.now() - self._timestamp
        if time_delta is None:
            time_delta = time_shift
        else:
            time_delta += time_shift
        result = YahooData._pull_source_symbol_bars(
            self, symbol, length, time_unit, time_delta=time_delta
        )
        filter_criteria = result.index <= self._timestamp
        result = result[filter_criteria]
        return result

    def _update_timestamp(self, timestamp):
        self._timestamp = timestamp
