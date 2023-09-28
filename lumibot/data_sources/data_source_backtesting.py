from abc import ABC
from datetime import datetime, timedelta

from lumibot.data_sources import DataSource
from lumibot.tools import print_progress_bar


class DataSourceBacktesting(DataSource, ABC):
    """
    This class is the base class for all backtesting data sources.  It is also an abstract class and should not be
    instantiated directly.  Instead, instantiate one of the child classes like PandasData.
    """
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(
        self, datetime_start, datetime_end, backtesting_started=None
    ):
        super().__init__()

        if backtesting_started is None:
            _backtesting_started = datetime.now()
        else:
            _backtesting_started = backtesting_started

        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start
        self._iter_count = None
        self.backtesting_started = _backtesting_started

        # Subtract one minute from the datetime_end so that the strategy stops right before the datetime_end
        self.datetime_end -= timedelta(minutes=1)

    def get_datetime(self):
        return self._datetime

    def get_datetime_range(self, length, timestep="minute", timeshift=None):
        backtesting_timeshift = datetime.now() - self._datetime
        if timeshift:
            backtesting_timeshift += timeshift

        if timestep == "minute":
            period_length = length * timedelta(minutes=1)
            end_date = self.get_last_minute() - backtesting_timeshift
        else:
            period_length = length * timedelta(days=1)
            end_date = self.get_last_day() - backtesting_timeshift

        start_date = end_date - period_length
        return start_date, end_date

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
        print_progress_bar(
            new_datetime,
            self.datetime_start,
            self.datetime_end,
            self.backtesting_started,
        )
