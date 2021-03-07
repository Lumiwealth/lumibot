from datetime import datetime

from lumibot.data_sources import DataSource
from lumibot.tools import print_progress_bar


class DataSourceBacktesting:
    IS_BACKTESTING_DATA_SOURCE = True

    @classmethod
    def factory(cls, datasource_class):
        def __init__(self, datetime_start, datetime_end, **kwargs):
            datasource_class.__init__(self, **kwargs)
            cls.__init__(self, datetime_start, datetime_end)

        backtesting_class = type(
            datasource_class.__name__ + "Backtesting",
            (cls, datasource_class),
            {
                "MIN_TIMESTEP": datasource_class.MIN_TIMESTEP,
                "LIVE_DATA_SOURCE": datasource_class,
                "__init__": __init__,
            },
        )
        return backtesting_class

    def __init__(self, datetime_start, datetime_end):
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start

    def get_datetime(self):
        return self.DEFAULT_PYTZ.localize(self._datetime)

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
        return (start_date, end_date)

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
        print_progress_bar(new_datetime, self.datetime_start, self.datetime_end)

    def _pull_source_symbol_bars(self, symbol, length, timestep=None, timeshift=None):
        if timestep is None:
            timestep = self.MIN_TIMESTEP

        backtesting_timeshift = datetime.now() - self._datetime
        if timeshift:
            backtesting_timeshift += timeshift

        result = self.LIVE_DATA_SOURCE._pull_source_symbol_bars(
            self, symbol, length, timestep=timestep, timeshift=backtesting_timeshift
        )

        filter_criteria = result.index <= self.localize_datetime(self._datetime)
        result = result[filter_criteria]
        return result
