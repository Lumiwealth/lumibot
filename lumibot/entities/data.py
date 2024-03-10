import datetime
import logging
import re

import pandas as pd
from lumibot import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
from lumibot.tools.helpers import parse_timestep_qty_and_unit, to_datetime_aware

from .asset import Asset
from .dataline import Dataline


class Data:
    """Input and manage Pandas dataframes for backtesting.

    Parameters
    ----------
    asset : Asset Object
        Asset to which this data is attached.
    df : dataframe
        Pandas dataframe containing OHLCV etc. trade data. Loaded by user
        from csv.
        Index is date and must be pandas datetime64.
        Columns are strictly ["open", "high", "low", "close", "volume"]
    quote : Asset Object
        The quote asset for this data. If not provided, then the quote asset will default to USD.
    date_start : Datetime or None
        Starting date for this data, if not provided then first date in
        the dataframe.
    date_end : Datetime or None
        Ending date for this data, if not provided then last date in
        the dataframe.
    trading_hours_start : datetime.time or None
        If not supplied, then default is 0001 hrs.
    trading_hours_end : datetime.time or None
        If not supplied, then default is 2359 hrs.
    timestep : str
        Either "minute" (default) or "day"
    localize_timezone : str or None
        If not None, then localize the timezone of the dataframe to the
        given timezone as a string. The values can be any supported by tz_localize,
        e.g. "US/Eastern", "UTC", etc.

    Attributes
    ----------
    asset : Asset Object
        Asset object to which this data is attached.
    sybmol : str
        The underlying or stock symbol as a string.
    df : dataframe
        Pandas dataframe containing OHLCV etc trade data. Loaded by user
        from csv.
        Index is date and must be pandas datetime64.
        Columns are strictly ["open", "high", "low", "close", "volume"]
    date_start : Datetime or None
        Starting date for this data, if not provided then first date in
        the dataframe.
    date_end : Datetime or None
        Ending date for this data, if not provided then last date in
        the dataframe.
    trading_hours_start : datetime.time or None
        If not supplied, then default is 0001 hrs.
    trading_hours_end : datetime.time or None
        If not supplied, then default is 2359 hrs.
    timestep : str
        Either "minute" (default) or "day"
    datalines : dict
        Keys are column names like `datetime` or `close`, values are
        numpy arrays.
    iter_index : Pandas Series
        Datetime in the index, range count in values. Used to retrieve
        the current df iteration for this data and datetime.

    Methods
    -------
    set_times
        Sets the start and end time for the data.
    repair_times_and_fill
        After all time series merged, adjust the local dataframe to reindex and fill nan's.
    columns
        Adjust date and column names to lower case.
    set_date_format
        Ensure datetime in local datetime64 format.
    set_dates
        Set start and end dates.
    trim_data
        Trim the dataframe to match the desired backtesting dates.
    to_datalines
        Create numpy datalines from existing date index and columns.
    get_iter_count
        Returns the current index number (len) given a date.
    check_data (wrapper)
        Validates if the provided date, length, timeshift, and timestep
        will return data. Runs function if data, returns None if no data.
    get_last_price
        Gets the last price from the current date.
    _get_bars_dict
        Returns bars in the form of a dict.
    get_bars
        Returns bars in the form of a dataframe.
    """

    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(
        self,
        asset,
        df,
        date_start=None,
        date_end=None,
        trading_hours_start=datetime.time(0, 0),
        trading_hours_end=datetime.time(23, 59),
        timestep="minute",
        quote=None,
        timezone=None,
    ):
        self.asset = asset
        self.symbol = self.asset.symbol

        if self.asset.asset_type == "crypto" and quote is None:
            raise ValueError(
                f"A crypto asset {self.symbol} was added to data without a corresponding"
                f"`quote` asset. Please add the quote asset. For example, if trying to add "
                f"`BTCUSD` to data, you would need to add `USD` as the quote asset."
                f"Quote must be provided for crypto assets."
            )
        else:
            self.quote = quote

        # Throw an error if the quote is not an asset object
        if self.quote is not None and not isinstance(self.quote, Asset):
            raise ValueError(
                f"The quote asset for Data must be an Asset object. You provided a {type(self.quote)} object."
            )

        if timestep not in ["minute", "day"]:
            raise ValueError(
                f"Timestep must be either 'minute' or 'day', the value you enetered ({timestep}) is not currently supported."
            )

        self.timestep = timestep

        self.df = self.columns(df)

        # Check if the index is datetime (it has to be), and if it's not then try to find it in the columns
        if str(self.df.index.dtype).startswith("datetime") is False:
            date_cols = [
                "Date",
                "date",
                "Time",
                "time",
                "Datetime",
                "datetime",
                "timestamp",
                "Timestamp",
            ]
            for date_col in date_cols:
                if date_col in self.df.columns:
                    self.df[date_col] = pd.to_datetime(self.df[date_col])
                    self.df = self.df.set_index(date_col)
                    break

        if timezone is not None:
            self.df.index = self.df.index.tz_localize(timezone)

        self.df = self.set_date_format(self.df)
        self.df = self.df.sort_index()

        self.trading_hours_start, self.trading_hours_end = self.set_times(trading_hours_start, trading_hours_end)
        self.date_start, self.date_end = self.set_dates(date_start, date_end)

        self.df = self.trim_data(
            self.df,
            self.date_start,
            self.date_end,
            self.trading_hours_start,
            self.trading_hours_end,
        )
        self.datetime_start = self.df.index[0]
        self.datetime_end = self.df.index[-1]

    def set_times(self, trading_hours_start, trading_hours_end):
        """Set the start and end times for the data. The default is 0001 hrs to 2359 hrs.

        Parameters
        ----------
        trading_hours_start : datetime.time
            The start time of the trading hours.

        trading_hours_end : datetime.time
            The end time of the trading hours.

        Returns
        -------
        trading_hours_start : datetime.time
            The start time of the trading hours.

        trading_hours_end : datetime.time
            The end time of the trading hours.
        """
        # Set the trading hours start and end times.
        if self.timestep == "minute":
            ts = trading_hours_start
            te = trading_hours_end
        else:
            ts = datetime.time(0, 0)
            te = datetime.time(23, 59, 59, 999999)
        return ts, te

    def columns(self, df):
        # Select columns to use, change to lower case, rename `date` if necessary.
        df.columns = [
            col.lower() if col.lower() in ["open", "high", "low", "close", "volume"] else col for col in df.columns
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
                raise TypeError(f"Start and End dates must be entries as full datetimes. {dt} " f"was entered")

        if not date_start:
            date_start = self.df.index.min()
        if not date_end:
            date_end = self.df.index.max()

        date_start = to_datetime_aware(date_start)
        date_end = to_datetime_aware(date_end)

        date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        return (
            date_start,
            date_end,
        )

    def trim_data(self, df, date_start, date_end, trading_hours_start, trading_hours_end):
        # Trim the dataframe to match the desired backtesting dates.

        df = df.loc[(df.index >= date_start) & (df.index <= date_end), :]
        if self.timestep == "minute":
            df = df.between_time(trading_hours_start, trading_hours_end)
        if df.empty:
            raise ValueError(
                f"When attempting to load a dataframe for {self.asset}, "
                f"an empty dataframe was returned. This is likely due "
                f"to your backtesting start and end dates not being "
                f"within the start and end dates of the data provided. "
                f"\nPlease check that your at least one of your start "
                f"or end dates for backtesting is within the range of "
                f"your start and end dates for your data. "
            )
        return df

    def repair_times_and_fill(self, idx):
        # Trim the global index so that it is within the local data.
        idx = idx[(idx >= self.datetime_start) & (idx <= self.datetime_end)]

        # After all time series merged, adjust the local dataframe to reindex and fill nan's.
        df = self.df.reindex(idx, method="ffill")
        df.loc[df["volume"].isna(), "volume"] = 0
        df.loc[:, ~df.columns.isin(["open", "high", "low"])] = df.loc[
            :, ~df.columns.isin(["open", "high", "low"])
        ].ffill()
        for col in ["open", "high", "low"]:
            df.loc[df[col].isna(), col] = df.loc[df[col].isna(), "close"]

        self.df = df

        iter_index = pd.Series(df.index)
        self.iter_index = pd.Series(iter_index.index, index=iter_index)
        self.iter_index_dict = self.iter_index.to_dict()

        self.datalines = dict()
        self.to_datalines()

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

        # Check if the date is in the dataframe, if not then get the last
        # known data (this speeds up the process)
        i = None

        # Check if we have the iter_index_dict, if not then repair the times and fill (which will create the iter_index_dict)
        if getattr(self, "iter_index_dict", None) is None:
            self.repair_times_and_fill(self.df.index)

        # Search for dt in self.iter_index_dict
        if dt in self.iter_index_dict:
            i = self.iter_index_dict[dt]
        else:
            # If not found, get the last known data
            i = self.iter_index.asof(dt)

        return i

    def check_data(func):
        # Validates if the provided date, length, timeshift, and timestep
        # will return data. Runs function if data, returns None if no data.
        def checker(self, *args, **kwargs):
            if type(kwargs.get("length", 1)) not in [int, float]:
                raise TypeError(f"Length must be an integer. {type(kwargs.get('length', 1))} was provided.")

            dt = args[0]

            # Check if the iter date is outside of this data's date range.
            if dt < self.datetime_start:
                raise ValueError(
                    f"The date you are looking for ({dt}) for ({self.asset}) is outside of the data's date range ({self.datetime_start} to {self.datetime_end}). This could be because the data for this asset does not exist for the date you are looking for, or something else."
                )

            # Search for dt in self.iter_index_dict
            if getattr(self, "iter_index_dict", None) is None:
                self.repair_times_and_fill(self.df.index)

            if dt in self.iter_index_dict:
                i = self.iter_index_dict[dt]
            else:
                # If not found, get the last known data
                i = self.iter_index.asof(dt)

            length = kwargs.get("length", 1)
            timeshift = kwargs.get("timeshift", 0)
            data_index = i + 1 - length - timeshift
            is_data = data_index >= 0
            if not is_data:
                # Log a warning
                logging.warning(
                    f"The date you are looking for ({dt}) is outside of the data's date range ({self.datetime_start} to {self.datetime_end}) after accounting for a length of {kwargs.get('length', 1)} and a timeshift of {kwargs.get('timeshift', 0)}. Keep in mind that the length you are requesting must also be available in your data, in this case we are {data_index} rows away from the data you need."
                )

            res = func(self, *args, **kwargs)
            # print(f"Results last price: {res}")
            return res

        return checker

    @check_data
    def get_last_price(self, dt, length=1, timeshift=0):
        """Returns the last known price of the data.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the last price.
        length : int
            The number of periods to get the last price.
        timestep : str
            The frequency of the data to get the last price.
        timeshift : int
            The number of periods to shift the data.

        Returns
        -------
        float
        """
        iter_count = self.get_iter_count(dt)
        return self.datalines["open"].dataline[iter_count]

    @check_data
    def _get_bars_dict(self, dt, length=1, timestep=None, timeshift=0):
        """Returns a dictionary of the data.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the data.
        length : int
            The number of periods to get the data.
        timestep : str
            The frequency of the data to get the data.
        timeshift : int
            The number of periods to shift the data.

        Returns
        -------
        dict

        """

        # Get bars.
        end_row = self.get_iter_count(dt) - timeshift
        start_row = end_row - length

        if start_row < 0:
            start_row = 0

        # Cast both start_row and end_row to int
        start_row = int(start_row)
        end_row = int(end_row)

        dict = {}
        for dl_name, dl in self.datalines.items():
            dict[dl_name] = dl.dataline[start_row:end_row]

        return dict

    def _get_bars_between_dates_dict(self, timestep=None, start_date=None, end_date=None):
        """Returns a dictionary of all the data available between the start and end dates.

        Parameters
        ----------
        timestep : str
            The frequency of the data to get the data.
        start_date : datetime.datetime
            The start date to get the data for.
        end_date : datetime.datetime
            The end date to get the data for.

        Returns
        -------
        dict
        """

        end_row = self.get_iter_count(end_date)
        start_row = self.get_iter_count(start_date)

        if start_row < 0:
            start_row = 0

        # Cast both start_row and end_row to int
        start_row = int(start_row)
        end_row = int(end_row)

        dict = {}
        for dl_name, dl in self.datalines.items():
            dict[dl_name] = dl.dataline[start_row:end_row]

        return dict

    def get_bars(self, dt, length=1, timestep=MIN_TIMESTEP, timeshift=0):
        """Returns a dataframe of the data.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the data.
        length : int
            The number of periods to get the data.
        timestep : str
            The frequency of the data to get the data. Only minute and day are supported.
        timeshift : int
            The number of periods to shift the data.

        Returns
        -------
        pandas.DataFrame

        """
        # Parse the timestep
        quantity, timestep = parse_timestep_qty_and_unit(timestep)

        if timestep == "minute" and self.timestep == "day":
            raise ValueError("You are requesting minute data from a daily data source. This is not supported.")

        if timestep != "minute" and timestep != "day":
            raise ValueError(f"Only minute and day are supported for timestep. You provided: {timestep}")

        agg_column_map = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        if timestep == "day" and self.timestep == "minute":
            # If the data is minute data and we are requesting daily data then multiply the length by 1440
            length = length * 1440
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        else:
            unit = "min"  # Guaranteed to be minute timestep at this point
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        if data is None:
            return None

        df = pd.DataFrame(data).assign(datetime=lambda df: pd.to_datetime(df['datetime'])).set_index('datetime')
        df_result = df.resample(f"{quantity}{unit}").agg(agg_column_map)

        # Drop any rows that have NaN values (this can happen if the data is not complete, eg. weekends)
        df_result = df_result.dropna()

        return df_result

    def get_bars_between_dates(self, timestep=MIN_TIMESTEP, exchange=None, start_date=None, end_date=None):
        """Returns a dataframe of all the data available between the start and end dates.

        Parameters
        ----------
        timestep : str
            The frequency of the data to get the data. Only minute and day are supported.
        exchange : str
            The exchange to get the data for.
        start_date : datetime.datetime
            The start date to get the data for.
        end_date : datetime.datetime
            The end date to get the data for.

        Returns
        -------
        pandas.DataFrame
        """

        if timestep == "minute" and self.timestep == "day":
            raise ValueError("You are requesting minute data from a daily data source. This is not supported.")

        if timestep != "minute" and timestep != "day":
            raise ValueError(f"Only minute and day are supported for timestep. You provided: {timestep}")

        if timestep == "day" and self.timestep == "minute":
            dict = self._get_bars_between_dates_dict(timestep=timestep, start_date=start_date, end_date=end_date)

            if dict is None:
                return None

            df = pd.DataFrame(dict).set_index("datetime")

            df_result = df.resample("D").agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )

            return df_result

        else:
            dict = self._get_bars_between_dates_dict(timestep=timestep, start_date=start_date, end_date=end_date)

            if dict is None:
                return None

            df = pd.DataFrame(dict).set_index("datetime")
            return df
