import datetime
from decimal import Decimal
from typing import Optional, Union

import pandas as pd
import polars as pl

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
import pytz
from lumibot.tools.helpers import parse_timestep_qty_and_unit, to_datetime_aware
from lumibot.tools.lumibot_logger import get_logger

from .asset import Asset
from .dataline import Dataline

logger = get_logger(__name__)

# Set the option to raise an error if downcasting is not possible (if available in this pandas version)
try:
    pd.set_option('future.no_silent_downcasting', True)
except (pd._config.config.OptionError, AttributeError):
    # Option not available in this pandas version, skip it
    pass


class DataPolars:
    """Input and manage Polars dataframes for backtesting.

    This is a polars-optimized version of the Data class that stores data as polars
    DataFrames internally and only converts to pandas when explicitly requested.

    Parameters
    ----------
    asset : Asset Object
        Asset to which this data is attached.
    df : polars.DataFrame
        Polars DataFrame containing OHLCV etc. trade data.
        Must have a 'datetime' column with datetime type.
        Other columns are strictly ["open", "high", "low", "close", "volume"]
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
    symbol : str
        The underlying or stock symbol as a string.
    polars_df : polars.DataFrame
        Polars DataFrame containing OHLCV etc trade data.
        Has a 'datetime' column with datetime type.
        Other columns are strictly ["open", "high", "low", "close", "volume"]
    df : pandas.DataFrame (property)
        Pandas DataFrame conversion for compatibility.
        This is computed on-demand and cached.
    date_start : Datetime or None
        Starting date for this data.
    date_end : Datetime or None
        Ending date for this data.
    trading_hours_start : datetime.time or None
        Trading hours start time.
    trading_hours_end : datetime.time or None
        Trading hours end time.
    timestep : str
        Either "minute" (default) or "day"
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
                f"The quote asset for DataPolars must be an Asset object. You provided a {type(self.quote)} object."
            )

        if timestep not in ["minute", "day"]:
            raise ValueError(
                f"Timestep must be either 'minute' or 'day', the value you entered ({timestep}) is not currently supported."
            )

        self.timestep = timestep

        # Store the polars DataFrame
        self.polars_df = self._columns(df)

        # Ensure datetime column exists and is properly typed
        if "datetime" not in self.polars_df.columns:
            raise ValueError("Polars DataFrame must have a 'datetime' column")

        # Convert datetime column to proper type if needed
        # CRITICAL: Preserve timezone if it already exists (e.g., UTC from DataBento)
        dtype = self.polars_df.schema["datetime"]
        if isinstance(dtype, pl.datatypes.Datetime) and dtype.time_zone:
            # Column already has timezone, preserve it during cast
            desired = pl.datatypes.Datetime(time_unit=dtype.time_unit, time_zone=dtype.time_zone)
            self.polars_df = self.polars_df.with_columns(pl.col("datetime").cast(desired))
        elif self.polars_df["datetime"].dtype != pl.Datetime:
            # No timezone, cast to naive datetime
            self.polars_df = self.polars_df.with_columns(
                pl.col("datetime").cast(pl.Datetime(time_unit="ns"))
            )

        # Apply timezone if specified
        if timezone is not None:
            # For polars, we'll handle timezone in the pandas conversion
            self._timezone = timezone
        else:
            self._timezone = None

        # Set dates and times
        self.polars_df = self.polars_df.sort("datetime")

        self.trading_hours_start, self.trading_hours_end = self.set_times(trading_hours_start, trading_hours_end)
        self.date_start, self.date_end = self.set_dates(date_start, date_end)

        self.polars_df = self.trim_data(
            self.polars_df,
            self.date_start,
            self.date_end,
            self.trading_hours_start,
            self.trading_hours_end,
        )

        # Set datetime start and end from polars DataFrame
        self.datetime_start = self.polars_df["datetime"][0]
        self.datetime_end = self.polars_df["datetime"][-1]

        # Convert polars datetime to pandas datetime for compatibility
        if hasattr(self.datetime_start, 'to_pydatetime'):
            self.datetime_start = self.datetime_start.to_pydatetime()
        if hasattr(self.datetime_end, 'to_pydatetime'):
            self.datetime_end = self.datetime_end.to_pydatetime()

        # Cached pandas DataFrame (lazy conversion)
        self._pandas_df = None

    def _localize_or_convert_index(self, index, tz):
        """Ensure index is tz-aware using the provided timezone."""
        if isinstance(tz, str):
            tz = pytz.timezone(tz)

        if getattr(index, "tz", None) is None:
            return index.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")

        if str(index.tz) == str(tz):
            return index

        return index.tz_convert(tz)

    @property
    def df(self):
        """Return pandas DataFrame for compatibility. Converts from polars on-demand."""
        if self._pandas_df is None:
            logger.debug(f"[CONVERSION] DataPolars.df | polars → pandas | {self.symbol}")

            # Check if polars datetime has timezone
            polars_tz = None
            if "datetime" in self.polars_df.columns:
                polars_tz = self.polars_df["datetime"].dtype.time_zone

            # Convert polars to pandas and set datetime as index
            self._pandas_df = self.polars_df.to_pandas()

            if "datetime" in self._pandas_df.columns:
                self._pandas_df.set_index("datetime", inplace=True)

            # Apply timezone conversion: UTC → America/New_York
            if self._timezone is not None:
                self._pandas_df.index = self._localize_or_convert_index(self._pandas_df.index, self._timezone)
            else:
                if polars_tz is not None:
                    self._pandas_df.index = self._localize_or_convert_index(self._pandas_df.index, polars_tz)

                if not getattr(self._pandas_df.index, "tz", None):
                    self._pandas_df.index = self._localize_or_convert_index(self._pandas_df.index, DEFAULT_PYTZ)
                elif str(self._pandas_df.index.tz) != str(DEFAULT_PYTZ):
                    self._pandas_df.index = self._pandas_df.index.tz_convert(DEFAULT_PYTZ)

        return self._pandas_df

    def set_times(self, trading_hours_start, trading_hours_end):
        """Set the start and end times for the data. The default is 0001 hrs to 2359 hrs."""
        if self.timestep == "minute":
            ts = trading_hours_start
            te = trading_hours_end
        else:
            ts = datetime.time(0, 0)
            te = datetime.time(23, 59, 59, 999999)
        return ts, te

    def _columns(self, df):
        """Adjust column names to lower case."""
        # Rename columns to lowercase if they match OHLCV
        rename_map = {}
        for col in df.columns:
            if col.lower() in ["open", "high", "low", "close", "volume"]:
                rename_map[col] = col.lower()

        if rename_map:
            df = df.rename(rename_map)

        return df

    def set_dates(self, date_start, date_end):
        """Set the start and end dates of the data."""
        for dt in [date_start, date_end]:
            if dt and not isinstance(dt, datetime.datetime):
                raise TypeError(f"Start and End dates must be entered as full datetimes. {dt} was entered")

        if not date_start:
            date_start = self.polars_df["datetime"].min()
            if hasattr(date_start, 'to_pydatetime'):
                date_start = date_start.to_pydatetime()
        if not date_end:
            date_end = self.polars_df["datetime"].max()
            if hasattr(date_end, 'to_pydatetime'):
                date_end = date_end.to_pydatetime()

        date_start = to_datetime_aware(date_start)
        date_end = to_datetime_aware(date_end)

        date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        return date_start, date_end

    def trim_data(self, df, date_start, date_end, trading_hours_start, trading_hours_end):
        """Trim the polars dataframe to match the desired backtesting dates."""
        # Align date comparisons to polars datetime column timezone (matching pandas approach)
        datetime_tz = df["datetime"].dtype.time_zone if "datetime" in df.columns else None

        # Convert comparison timestamps to match column timezone
        if datetime_tz is not None:
            # Column has timezone, align dates to it
            date_start_aligned = pd.Timestamp(date_start).tz_convert(datetime_tz) if hasattr(pd.Timestamp(date_start), 'tz_convert') else pd.Timestamp(date_start).tz_localize(datetime_tz)
            date_end_aligned = pd.Timestamp(date_end).tz_convert(datetime_tz) if hasattr(pd.Timestamp(date_end), 'tz_convert') else pd.Timestamp(date_end).tz_localize(datetime_tz)
        else:
            # Column is naive, make dates naive too
            date_start_aligned = pd.Timestamp(date_start).tz_localize(None) if hasattr(pd.Timestamp(date_start), 'tz') and pd.Timestamp(date_start).tz else pd.Timestamp(date_start)
            date_end_aligned = pd.Timestamp(date_end).tz_localize(None) if hasattr(pd.Timestamp(date_end), 'tz') and pd.Timestamp(date_end).tz else pd.Timestamp(date_end)

        # Filter by date range
        df = df.filter(
            (pl.col("datetime") >= date_start_aligned) & (pl.col("datetime") <= date_end_aligned)
        )

        # Filter by trading hours if minute data
        if self.timestep == "minute":
            df = df.filter(
                (pl.col("datetime").dt.time() >= trading_hours_start) &
                (pl.col("datetime").dt.time() <= trading_hours_end)
            )

        if df.height == 0:
            raise ValueError(
                f"When attempting to load a dataframe for {self.asset}, "
                f"an empty dataframe was returned. This is likely due "
                f"to your backtesting start and end dates not being "
                f"within the start and end dates of the data provided. "
                f"\nPlease check that at least one of your start "
                f"or end dates for backtesting is within the range of "
                f"your start and end dates for your data. "
            )
        return df

    def repair_times_and_fill(self, idx):
        """Create datalines and fill missing values.

        This converts to pandas for compatibility with the existing dataline system.
        """
        # Get pandas DataFrame
        df = self.df

        # OPTIMIZATION: Use searchsorted instead of expensive boolean indexing
        start_pos = idx.searchsorted(self.datetime_start, side='left')
        end_pos = idx.searchsorted(self.datetime_end, side='right')
        idx = idx[start_pos:end_pos]

        # OPTIMIZATION: More efficient duplicate removal
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep='first')]

        # Reindex the DataFrame with the new index and forward-fill missing values.
        df = df.reindex(idx, method="ffill")

        # Check if we have a volume column, if not then add it and fill with 0 or NaN.
        if "volume" in df.columns:
            df.loc[df["volume"].isna(), "volume"] = 0
        else:
            df["volume"] = None

        # OPTIMIZATION: More efficient column selection and forward fill
        ohlc_cols = ["open", "high", "low"]
        non_ohlc_cols = [col for col in df.columns if col not in ohlc_cols]
        if non_ohlc_cols:
            df[non_ohlc_cols] = df[non_ohlc_cols].ffill()

        # If any of close, open, high, low columns are missing, add them with NaN.
        for col in ["close", "open", "high", "low"]:
            if col not in df.columns:
                df[col] = None

        # OPTIMIZATION: Vectorized NaN filling for OHLC columns
        if "close" in df.columns:
            for col in ["open", "high", "low"]:
                if col in df.columns:
                    try:
                        # More efficient: compute mask once, use where
                        mask = df[col].isna()
                        if mask.any():
                            df[col] = df[col].where(~mask, df["close"])
                    except Exception as e:
                        logger.error(f"Error filling {col} column: {e}")

        # Update the cached pandas DataFrame
        self._pandas_df = df

        # Set up iter_index and iter_index_dict for later use.
        iter_index = pd.Series(df.index)
        self.iter_index = pd.Series(iter_index.index, index=iter_index)
        self.iter_index_dict = self.iter_index.to_dict()

        # Populate the datalines dictionary.
        self.datalines = dict()
        self.to_datalines()

    def to_datalines(self):
        """Create datalines from the pandas DataFrame."""
        df = self.df

        self.datalines.update(
            {
                "datetime": Dataline(
                    self.asset,
                    "datetime",
                    df.index.to_numpy(),
                    df.index.dtype,
                )
            }
        )
        self.datetime = self.datalines["datetime"].dataline

        for column in df.columns:
            self.datalines.update(
                {
                    column: Dataline(
                        self.asset,
                        column,
                        df[column].to_numpy(),
                        df[column].dtype,
                    )
                }
            )
            setattr(self, column, self.datalines[column].dataline)

    def get_iter_count(self, dt):
        """Return the index location for a given datetime."""
        i = None

        # Check if we have the iter_index_dict, if not then repair the times and fill
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
        """Validates if the provided date, length, timeshift, and timestep will return data."""
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
            # Convert timeshift to integer if it's a timedelta
            if isinstance(timeshift, datetime.timedelta):
                timestep = kwargs.get("timestep", self.timestep)
                if timestep == "day":
                    timeshift = timeshift.days
                else:  # minute
                    timeshift = int(timeshift.total_seconds() / 60)
            data_index = i + 1 - length - timeshift
            is_data = data_index >= 0
            if not is_data:
                logger.warning(
                    f"The date you are looking for ({dt}) is outside of the data's date range ({self.datetime_start} to {self.datetime_end}) after accounting for a length of {kwargs.get('length', 1)} and a timeshift of {kwargs.get('timeshift', 0)}. Keep in mind that the length you are requesting must also be available in your data, in this case we are {data_index} rows away from the data you need."
                )

            res = func(self, *args, **kwargs)
            return res

        return checker

    @check_data
    def get_last_price(self, dt, length=1, timeshift=0) -> Union[float, Decimal, None]:
        """Returns the last known price of the data."""
        iter_count = self.get_iter_count(dt)
        open_price = self.datalines["open"].dataline[iter_count]
        close_price = self.datalines["close"].dataline[iter_count]
        price = close_price if dt > self.datalines["datetime"].dataline[iter_count] else open_price
        return price

    @check_data
    def get_quote(self, dt, length=1, timeshift=0):
        """Returns the last known quote data."""
        required_price_cols = ["open", "high", "low", "close", "volume"]
        missing_price_cols = [col for col in required_price_cols if col not in self.datalines]
        if missing_price_cols:
            logger.warning(
                "DataPolars object %s is missing price columns %s required for quote retrieval.",
                self.asset,
                missing_price_cols,
            )
            return {}

        quote_fields = {
            "open": ("open", 2),
            "high": ("high", 2),
            "low": ("low", 2),
            "close": ("close", 2),
            "volume": ("volume", 0),
            "bid": ("bid", 2),
            "ask": ("ask", 2),
            "bid_size": ("bid_size", 0),
            "bid_condition": ("bid_condition", 0),
            "bid_exchange": ("bid_exchange", 0),
            "ask_size": ("ask_size", 0),
            "ask_condition": ("ask_condition", 0),
            "ask_exchange": ("ask_exchange", 0),
        }

        missing_quote_cols = [
            col for col in ["bid", "ask", "bid_size", "ask_size", "bid_condition", "ask_condition",
                            "bid_exchange", "ask_exchange"]
            if col not in self.datalines
        ]
        if missing_quote_cols:
            logger.warning(
                "DataPolars object %s is missing quote columns %s; returning None for those values.",
                self.asset,
                missing_quote_cols,
            )

        iter_count = self.get_iter_count(dt)

        def _get_value(column: str, round_digits: Optional[int]):
            if column not in self.datalines:
                return None
            value = self.datalines[column].dataline[iter_count]
            try:
                if round_digits is None:
                    return value
                return round(value, round_digits)
            except TypeError:
                return value

        quote_dict = {
            name: _get_value(column, digits) for name, (column, digits) in quote_fields.items()
        }

        return quote_dict

    @check_data
    def _get_bars_dict(self, dt, length=1, timestep=None, timeshift=0):
        """Returns a dictionary of the data."""
        # Convert timeshift to integer if it's a timedelta
        if isinstance(timeshift, datetime.timedelta):
            logger.debug(f"[TIMESHIFT_CONVERT] asset={self.symbol} input_timeshift={timeshift} type={type(timeshift)} repr={repr(timeshift)}")
            ts = timestep if timestep is not None else self.timestep
            if ts == "day":
                timeshift_converted = int(timeshift.total_seconds() / (24 * 3600))
                logger.debug(f"[TIMESHIFT_CONVERT] asset={self.symbol} timestep=day total_seconds={timeshift.total_seconds()} converted={timeshift_converted}")
                timeshift = timeshift_converted
            else:  # minute
                timeshift_converted = int(timeshift.total_seconds() / 60)
                logger.debug(f"[TIMESHIFT_CONVERT] asset={self.symbol} timestep=minute total_seconds={timeshift.total_seconds()} converted={timeshift_converted}")
                timeshift = timeshift_converted

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
        """Returns a dictionary of all the data available between the start and end dates."""
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
        """Returns a dataframe of the data."""
        # Parse the timestep
        quantity, timestep = parse_timestep_qty_and_unit(timestep)
        num_periods = length

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
            length = length * 1440
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        elif timestep == 'day' and self.timestep == 'day':
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        else:
            unit = "min"
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        if data is None:
            return None

        df = pd.DataFrame(data).assign(datetime=lambda df: pd.to_datetime(df['datetime'])).set_index('datetime')
        if "dividend" in df.columns:
            agg_column_map["dividend"] = "sum"
        df_result = df.resample(f"{quantity}{unit}").agg(agg_column_map)

        # Drop any rows that have NaN values
        df_result = df_result.dropna()

        # Remove partial day data from the current day
        if timestep == "day" and self.timestep == "minute":
            df_result = df_result[df_result.index < dt.replace(hour=0, minute=0, second=0, microsecond=0)]

        # Only return the last n rows
        df_result = df_result.tail(n=int(num_periods))

        return df_result

    def get_bars_between_dates(self, timestep=MIN_TIMESTEP, exchange=None, start_date=None, end_date=None):
        """Returns a dataframe of all the data available between the start and end dates."""
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
