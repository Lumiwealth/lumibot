import csv
import datetime as dt
import os
from abc import ABC
from collections import deque
from datetime import datetime, timedelta

from lumibot.data_sources import DataSource
from lumibot.tools import print_progress_bar, to_datetime_aware
from lumibot.tools.helpers import get_timezone_from_datetime


class DataSourceBacktesting(DataSource, ABC):
    """
    This class is the base class for all backtesting data sources.  It is also an abstract class and should not be
    instantiated directly because it does not define all necessary methods. Instead, instantiate one of the
    child classes like PandasData.
    """

    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(
             self,
            datetime_start: datetime | None = None,
            datetime_end: datetime | None = None,
            backtesting_started: datetime | None = None,
            config: dict | None = None,
            api_key: str | None = None,
            show_progress_bar: bool = True,
            log_backtest_progress_to_file = False,
            delay: int | None = None,
            pandas_data: dict | list = None,
            **kwargs
    ):
        # Pass only api_key to parent class, not datetime_start and datetime_end
        # Remove any datetime_start or datetime_end from kwargs to avoid them being passed twice
        if 'datetime_start' in kwargs:
            # If datetime_start was also passed as a keyword arg, prioritize the keyword arg value
            datetime_start = kwargs.pop('datetime_start')
        if 'datetime_end' in kwargs:
            # If datetime_end was also passed as a keyword arg, prioritize the keyword arg value
            datetime_end = kwargs.pop('datetime_end')

        # Initialize parent class
        super().__init__(api_key=api_key, delay=delay, config=config, **kwargs)

        if backtesting_started is None:
            _backtesting_started = dt.datetime.now()
        else:
            _backtesting_started = backtesting_started

        self.datetime_start = to_datetime_aware(datetime_start)
        self.datetime_end = to_datetime_aware(datetime_end)
        self._datetime = self.datetime_start
        self._iter_count = None
        self.backtesting_started = _backtesting_started
        self.log_backtest_progress_to_file = log_backtest_progress_to_file
        self.tzinfo = get_timezone_from_datetime(self.datetime_start)
        self._eta_history = deque()
        self._eta_window_seconds = 30
        self._eta_calibration_seconds = 25
        self._eta_calibration_progress = 4  # percent

        # Subtract one minute from the datetime_end so that the strategy stops right before the datetime_end
        self.datetime_end -= timedelta(minutes=1)

        # Legacy strategy.backtest code will always pass in a config even for DataSources that don't need it, so
        # catch it here and ignore it in this class. Child classes that need it should error check it themselves.
        self._config = config

        # Progress bar should always show when enabled, regardless of quiet_logs
        # Quiet logs only affects INFO/WARNING/ERROR logging, not progress bar
        self._show_progress_bar = show_progress_bar

        self._progress_csv_path = "logs/progress.csv"
        # Add initialization for the logging timer attribute
        self._last_logging_time = None
        self._portfolio_value = None

    @staticmethod
    def estimate_requested_length(length=None, start_date=None, end_date=None, timestep="minute"):
        """
        Infer the number of rows required to satisfy a backtest data request.
        """
        if length is not None:
            try:
                return max(int(length), 1)
            except (TypeError, ValueError):
                pass

        if start_date is None or end_date is None:
            return 1

        try:
            td, unit = DataSource.convert_timestep_str_to_timedelta(timestep)
        except Exception:
            return 1

        if end_date < start_date:
            start_date, end_date = end_date, start_date

        if unit == "day":
            delta_days = (end_date.date() - start_date.date()).days
            return max(delta_days + 1, 1)

        interval_seconds = max(td.total_seconds(), 1)
        total_seconds = max((end_date - start_date).total_seconds(), 0)
        return max(int(total_seconds // interval_seconds) + 1, 1)

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

        total_seconds = max((self.datetime_end - self.datetime_start).total_seconds(), 1)
        current_seconds = max((new_datetime - self.datetime_start).total_seconds(), 0)
        percent = min((current_seconds / total_seconds) * 100, 100)
        now_wall = dt.datetime.now()
        eta_override = None

        if percent > 0:
            self._eta_history.append((now_wall, percent))
            while self._eta_history and (now_wall - self._eta_history[0][0]).total_seconds() > self._eta_window_seconds:
                self._eta_history.popleft()

            elapsed_seconds = (now_wall - self.backtesting_started).total_seconds()
            if (
                elapsed_seconds >= self._eta_calibration_seconds
                or percent >= self._eta_calibration_progress
            ) and len(self._eta_history) >= 2:
                earliest_time, earliest_percent = self._eta_history[0]
                delta_percent = percent - earliest_percent
                delta_time = (now_wall - earliest_time).total_seconds()
                if delta_percent > 0 and delta_time > 0:
                    speed = delta_percent / delta_time  # percent per second
                    remaining_percent = max(0.0, 100 - percent)
                    eta_seconds = remaining_percent / speed
                    if eta_seconds >= 0:
                        eta_override = timedelta(seconds=eta_seconds)

        if self._show_progress_bar:
            print_progress_bar(
                new_datetime,
                self.datetime_start,
                self.datetime_end,
                self.backtesting_started,
                cash=cash,
                portfolio_value=portfolio_value,
                eta_override=eta_override,
            )

        if self.log_backtest_progress_to_file:
            if portfolio_value is None:
                if hasattr(self, "_portfolio_value") and self._portfolio_value is not None:
                    portfolio_value = self._portfolio_value
            else:
                self._portfolio_value = portfolio_value

            if (self._last_logging_time is None) or ((now_wall - self._last_logging_time).total_seconds() >= 2):
                self._last_logging_time = now_wall
                elapsed = now_wall - self.backtesting_started
                log_eta = eta_override
                if portfolio_value is not None:
                    if isinstance(portfolio_value, (int, float)):
                        log_portfolio_value = f'{portfolio_value:,.2f}'
                    else:
                        try:
                            log_portfolio_value = f'{float(portfolio_value):,.2f}'
                        except (ValueError, TypeError):
                            log_portfolio_value = str(portfolio_value)
                else:
                    log_portfolio_value = ""
                self.log_backtest_progress_to_csv(percent, elapsed, log_eta, log_portfolio_value)

    def log_backtest_progress_to_csv(self, percent, elapsed, log_eta, portfolio_value):
        # If portfolio_value is None, use the last known value if available.
        if portfolio_value is None and hasattr(self, "_portfolio_value") and self._portfolio_value is not None:
            portfolio_value = self._portfolio_value

        elif portfolio_value is not None:
            self._portfolio_value = portfolio_value

        current_time = dt.datetime.now().isoformat()
        row = [
            current_time,
            f"{percent:.2f}",
            str(elapsed).split('.')[0],
            str(log_eta).split('.')[0] if log_eta else "",
            portfolio_value
        ]
        # Ensure the directory exists before opening the file.
        dir_path = os.path.dirname(self._progress_csv_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        with open(self._progress_csv_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["timestamp", "percent", "elapsed", "eta", "portfolio_value"])
            writer.writerow(row)
