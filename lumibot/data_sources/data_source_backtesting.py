from abc import ABC
from datetime import datetime, timedelta

import pandas as pd

from lumibot.data_sources import DataSource
from lumibot.tools import print_progress_bar, to_datetime_aware


class DataSourceBacktesting(DataSource, ABC):
    """
    This class is the base class for all backtesting data sources.  It is also an abstract class and should not be
    instantiated directly because it does not define all necessary methods. Instead, instantiate one of the
    child classes like PandasData.
    """

    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(
        self, datetime_start, datetime_end, backtesting_started=None, config=None, api_key=None, pandas_data=None
    ):
        super().__init__(api_key=api_key)

        if backtesting_started is None:
            _backtesting_started = datetime.now()
        else:
            _backtesting_started = backtesting_started

        self.datetime_start = to_datetime_aware(datetime_start)
        self.datetime_end = to_datetime_aware(datetime_end)
        self._datetime = self.datetime_start
        self._iter_count = None
        self.backtesting_started = _backtesting_started

        # Subtract one minute from the datetime_end so that the strategy stops right before the datetime_end
        self.datetime_end -= timedelta(minutes=1)

        # Legacy strategy.backtest code will always pass in a config even for DataSources that don't need it, so
        # catch it here and ignore it in this class. Child classes that need it should error check it themselves.
        self._config = config

    def get_datetime(self, adjust_for_delay=False):
        """
        Get the current datetime of the backtest.

        Parameters
        ----------
        adjust_for_delay: bool
            Not used for backtesting data sources.  This parameter is only used for live data sources.

        Returns
        -------
        datetime
            The current datetime of the backtest.
        """
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

    def _update_datetime(self, new_datetime, cash=None, portfolio_value=None):
        self._datetime = new_datetime
        print_progress_bar(
            new_datetime,
            self.datetime_start,
            self.datetime_end,
            self.backtesting_started,
            cash=cash,
            portfolio_value=portfolio_value,
        )
