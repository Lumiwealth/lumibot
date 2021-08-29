import datetime
import logging

from .dataline import Dataline
from lumibot import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
from lumibot.tools.helpers import to_datetime_aware
import pandas as pd


class Data:
    """Input and manage Pandas dataframes for backtesting.

    Parameters
    ----------

    Attributes
    ----------

    Methods
    -------

    """

    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(
        self,
        strategy,
        asset,
        df,
        date_start=None,
        date_end=None,
        time_start=datetime.time(9, 30),
        time_end=datetime.time(16, 0),
        timestep="minute",
        columns=None,
    ):
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol

        self.timestep = timestep
        self.df = self.columns(df)
        self.df = self.set_date_format(self.df)

        self.time_start, self.time_end = self.set_times(time_start, time_end)
        self.date_start, self.date_end = self.set_dates(date_start, date_end)

        self.df = self.trim_data(
            self.df, self.date_start, self.date_end, self.time_start, self.time_end
        )

    def set_times(self, time_start, time_end):
        if self.timestep == 'minute':
            ts = time_start
            te = time_end
        else:
            ts = datetime.time(0, 0)
            te = datetime.time(23, 59, 59, 999999)
        return ts, te

    def repair_times_and_fill(self, idx):
        # After all time series merged, adjust the local dataframe to reindex and fill nan's.
        # Create Datalines.
        df = self.df.reindex(idx)
        df.loc[df["volume"].isna(), "volume"] = 0
        df.loc[:, ~df.columns.isin(["open", "high", "low"])] = df.loc[:, ~df.columns.isin(["open",
                                                                      "high", "low"])].ffill()
        for col in ['open', 'high', 'low']:
            df.loc[df[col].isna(), col] = df.loc[df[col].isna(), "close"]

        self.df = df

        iter_index = pd.Series(df.index)
        self.iter_index = pd.Series(iter_index.index, index=iter_index)

        self.datalines = dict()
        self.to_datalines()

    def columns(self, df):
        # Select columns to use, change to lower case, rename `date` if necessary.
        df.columns = [
            col.lower()
            if col.lower() in ["open", "high", "low", "close", "volume"]
            else col
            for col in df.columns
        ]

        return df

    def set_date_format(self, df):
        df.index.name = "datetime"
        df.index = pd.to_datetime(df.index)
        if not df.index.tzinfo:
            df.index = df.index.tz_localize(DEFAULT_PYTZ)
        elif df.index.tzinfo != DEFAULT_PYTZ:
            df.index = df.index.tz_convert(DEFAULT_PYTZ)
        return df

    def set_dates(self, date_start, date_end):
        # Set the start and end dates of the data.
        for dt in [date_start, date_end]:
            if dt and not isinstance(dt, datetime.datetime):
                raise TypeError(
                    f"Start and End dates must be entries as full datetimes. {dt} "
                    f"was entered"
                )

        if not date_start:
            date_start = self.df.index[0]
        if not date_end:
            date_end = self.df.index[-1]

        date_start = to_datetime_aware(date_start)
        date_end = to_datetime_aware(date_end)

        if self.timestep == "day":
            date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        return (
            date_start,
            date_end,
        )

    def trim_data(self, df, date_start, date_end, time_start, time_end):
        # Trim the dataframe to match the desired backtesting dates.
        df = df.loc[date_start:date_end, :]
        if self.timestep == 'minute':
            df = df.between_time(time_start, time_end)
        return df

    def to_datalines(self):
        self.datalines.update(
            {
                "datetime": Dataline(
                    self.asset,
                    "datetime",
                    self.df.index.to_numpy(),
                    self.df.index.dtype,
                )
            }
        )
        setattr(self, "datetime", self.datalines["datetime"].dataline)

        for column in self.df.columns:
            self.datalines.update(
                {
                    column: Dataline(
                        self.asset,
                        column,
                        self.df[column].to_numpy(),
                        self.df[column].dtype,
                    )
                }
            )
            setattr(self, column, self.datalines[column].dataline)

    def get_iter_count(self, dt):
        # Return the index location for a given datetime.
        return self.iter_index[dt]

    def get_last_price(self, dt):
        # Get the last close price.
        return self.datalines["close"].dataline[self.get_iter_count(dt)]

    def _get_bars_dict(self, dt, length, timestep=None, timeshift=0):
        # Get bars.
        end_row = self.get_iter_count(dt) + 1 - timeshift
        start_row = end_row - length
        if start_row < 0:
            start_row = 0

        df_dict = {}
        for dl_name, dl in self.datalines.items():
            df_dict[dl_name] = dl.dataline[start_row:end_row]

        return df_dict

    def get_bars(self, dt, length, timestep=MIN_TIMESTEP, timeshift=0):
        df_dict = self._get_bars_dict(dt, length, timestep=self.timestep, timeshift=timeshift)
        df = pd.DataFrame(df_dict).set_index("datetime")
        return df
