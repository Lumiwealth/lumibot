from datetime import datetime, timedelta

import pandas as pd

from lumibot.data_sources import DataSource
from lumibot.tools import print_progress_bar, to_datetime_aware


class DataSourceBacktesting(DataSource):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, datetime_start, datetime_end):
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start
        self._iter_count = None

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
        return (start_date, end_date)

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
        print_progress_bar(new_datetime, self.datetime_start, self.datetime_end)

    def _pull_source_symbol_bars(self, asset, length, timestep=None, timeshift=0):
        if timestep is None:
            timestep = self.get_timestep()
        if self.LIVE_DATA_SOURCE.SOURCE == "YAHOO":
            now = datetime.now()
            now_local = self.localize_datetime(now)
            backtesting_timeshift = now_local - self._datetime
            if timeshift:
                backtesting_timeshift += timeshift

            if timestep == "day":
                backtesting_timeshift += timedelta(days=1)
            elif timestep == "minute":
                backtesting_timeshift += timedelta(minutes=1)
        elif self.LIVE_DATA_SOURCE.SOURCE == "PANDAS":
            backtesting_timeshift = timeshift
        else:
            raise ValueError(
                f"An incorrect backtester values was received. Received"
                f" {self.LIVE_DATA_SOURCE.SOURCE}"
            )
        result = self.LIVE_DATA_SOURCE._pull_source_symbol_bars(
            self, asset, length, timestep=timestep, timeshift=backtesting_timeshift
        )

        if result is None:
            return result
        else:
            filter_criteria = result.index <= self._datetime
            result = result[filter_criteria]
            return result
