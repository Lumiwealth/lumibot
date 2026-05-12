from __future__ import annotations

import datetime
from collections.abc import Callable
from decimal import Decimal
from importlib import import_module
from types import ModuleType
from typing import Any, ClassVar, cast

import pytz

from lumibot.tools.lumibot_logger import get_logger

from .asset import Asset
from .dataline import Dataline

logger = get_logger(__name__)
_DEFAULT_PYTZ = None
_PARSE_TIMESTEP_QTY_AND_UNIT = None
_TO_DATETIME_AWARE = None

_DATA_REQUIRED_PRICE_COLS = ("open", "high", "low", "close", "volume")
_DATA_QUOTE_COLS = (
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "bid_condition",
    "ask_condition",
    "bid_exchange",
    "ask_exchange",
)
_DATA_QUOTE_FIELDS: dict[str, tuple[str, int | None]] = {
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

_default_pytz_cache: Any | None = None
_parse_timestep_qty_and_unit_cache: Callable[..., Any] | None = None
_to_datetime_aware_cache: Callable[..., Any] | None = None


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        self._module_name = module_name
        self._module = None

    def _load(self) -> ModuleType:
        module = self._module
        if module is None:
            module_name = self._module_name
            module = import_module(module_name)
            if module_name == "pandas":
                try:
                    module.set_option("future.no_silent_downcasting", True)
                except (module._config.config.OptionError, AttributeError):
                    pass
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
pl = _LazyModule("polars")


def _default_pytz() -> Any:
    global _default_pytz_cache
    if _default_pytz_cache is None:
        from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

        _default_pytz_cache = LUMIBOT_DEFAULT_PYTZ
    return _default_pytz_cache


def parse_timestep_qty_and_unit(*args: Any, **kwargs: Any) -> Any:
    global _parse_timestep_qty_and_unit_cache
    if _parse_timestep_qty_and_unit_cache is None:
        helpers = import_module("lumibot.tools.helpers")
        _parse_timestep_qty_and_unit_cache = cast(Callable[..., Any], cast(Any, helpers).parse_timestep_qty_and_unit)
    return _parse_timestep_qty_and_unit_cache(*args, **kwargs)


def to_datetime_aware(*args: Any, **kwargs: Any) -> datetime.datetime:
    global _to_datetime_aware_cache
    if _to_datetime_aware_cache is None:
        helpers = import_module("lumibot.tools.helpers")
        _to_datetime_aware_cache = cast(Callable[..., Any], cast(Any, helpers).to_datetime_aware)
    return cast(datetime.datetime, _to_datetime_aware_cache(*args, **kwargs))


def _to_pydatetime_if_available(value: Any) -> Any:
    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        return to_pydatetime()
    return value


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

    MIN_TIMESTEP: ClassVar[str] = "minute"
    TIMESTEP_MAPPING: ClassVar[list[dict[str, list[str] | str]]] = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "hour", "representations": ["1H", "hour"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    asset: Asset
    symbol: str | None
    quote: Asset | None
    timestep: str
    polars_df: Any
    trading_hours_start: datetime.time
    trading_hours_end: datetime.time
    date_start: datetime.datetime
    date_end: datetime.datetime
    datetime_start: Any
    datetime_end: Any
    datalines: dict[str, Dataline]
    iter_index: Any | None
    iter_index_dict: dict[Any, Any] | None
    _pandas_df: Any | None
    _timezone: Any | None
    _quote_required_cols_present: bool
    _quote_missing_cols: list[str]
    _quote_presence_logged: bool
    open: Any | None
    close: Any | None

    def __init__(
        self,
        asset: Asset,
        df: Any,
        date_start: datetime.datetime | None = None,
        date_end: datetime.datetime | None = None,
        trading_hours_start: datetime.time = datetime.time(0, 0),
        trading_hours_end: datetime.time = datetime.time(23, 59),
        timestep: str = "minute",
        quote: Any | None = None,
        timezone: Any | None = None,
    ) -> None:
        self.asset = asset
        self.symbol = self.asset.symbol
        self.datalines = {}
        self.iter_index = None
        self.iter_index_dict = None
        self._quote_required_cols_present = True
        self._quote_missing_cols = []
        self._quote_presence_logged = False
        self.open = None
        self.close = None

        if "crypto" == self.asset.asset_type and quote is None:
            raise ValueError(
                f"A crypto asset {self.symbol} was added to data without a corresponding"
                f"`quote` asset. Please add the quote asset. For example, if trying to add "
                f"`BTCUSD` to data, you would need to add `USD` as the quote asset."
                f"Quote must be provided for crypto assets."
            )

        # Throw an error if the quote is not an asset object
        if quote is not None and not isinstance(quote, Asset):
            raise ValueError(
                f"The quote asset for DataPolars must be an Asset object. You provided a {type(quote)} object."
            )
        self.quote = quote

        if timestep not in ["minute", "hour", "day"]:
            raise ValueError(f"Timestep must be one of 'minute', 'hour', or 'day'. You entered: {timestep}")

        self.timestep = timestep

        # Store the polars DataFrame
        self.polars_df = self._columns(df)

        # Ensure datetime column exists and is properly typed
        if "datetime" not in self.polars_df.columns:
            raise ValueError("Polars DataFrame must have a 'datetime' column")

        # Convert datetime column to proper type if needed
        # CRITICAL: Preserve timezone if it already exists (e.g., UTC from DataBento)
        dtype: Any = self.polars_df.schema["datetime"]
        polars_datetime_type = cast(type[Any], pl.datatypes.Datetime)
        if isinstance(dtype, polars_datetime_type) and getattr(dtype, "time_zone", None):
            # Column already has timezone, preserve it during cast
            desired = pl.datatypes.Datetime(time_unit=dtype.time_unit, time_zone=dtype.time_zone)
            self.polars_df = self.polars_df.with_columns(pl.col("datetime").cast(desired))
        elif self.polars_df["datetime"].dtype != pl.Datetime:
            # No timezone, cast to naive datetime
            self.polars_df = self.polars_df.with_columns(pl.col("datetime").cast(pl.Datetime(time_unit="ns")))

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
        self.datetime_start = _to_pydatetime_if_available(self.polars_df["datetime"][0])
        self.datetime_end = _to_pydatetime_if_available(self.polars_df["datetime"][-1])

        # Cached pandas DataFrame (lazy conversion)
        self._pandas_df = None

    def _localize_or_convert_index(self, index: Any, tz: Any) -> Any:
        """Ensure index is tz-aware using the provided timezone."""
        if isinstance(tz, str):
            tz = pytz.timezone(tz)

        if getattr(index, "tz", None) is None:
            return index.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")

        if str(index.tz) == str(tz):
            return index

        return index.tz_convert(tz)

    @property
    def df(self) -> Any:
        """Return pandas DataFrame for compatibility. Converts from polars on-demand."""
        cached_df = self._pandas_df
        if cached_df is None:
            logger.debug(f"[CONVERSION] DataPolars.df | polars → pandas | {self.symbol}")

            # Check if polars datetime has timezone
            polars_tz: Any | None = None
            if "datetime" in self.polars_df.columns:
                polars_tz = getattr(self.polars_df["datetime"].dtype, "time_zone", None)

            # Convert polars to pandas and set datetime as index
            pandas_df: Any = self.polars_df.to_pandas()

            if "datetime" in pandas_df.columns:
                pandas_df.set_index("datetime", inplace=True)

            # Apply timezone conversion: UTC → America/New_York
            if self._timezone is not None:
                pandas_df.index = self._localize_or_convert_index(pandas_df.index, self._timezone)
            else:
                if polars_tz is not None:
                    pandas_df.index = self._localize_or_convert_index(pandas_df.index, polars_tz)

                if not getattr(pandas_df.index, "tz", None):
                    pandas_df.index = self._localize_or_convert_index(pandas_df.index, _default_pytz())
                elif str(pandas_df.index.tz) != str(_default_pytz()):
                    pandas_df.index = pandas_df.index.tz_convert(_default_pytz())

            self._pandas_df = pandas_df

            return pandas_df

        return cached_df

    def set_times(
        self,
        trading_hours_start: datetime.time,
        trading_hours_end: datetime.time,
    ) -> tuple[datetime.time, datetime.time]:
        """Set the start and end times for the data. The default is 0001 hrs to 2359 hrs."""
        if self.timestep == "minute":
            ts = trading_hours_start
            te = trading_hours_end
        else:
            ts = datetime.time(0, 0)
            te = datetime.time(23, 59, 59, 999999)
        return ts, te

    def _columns(self, df: Any) -> Any:
        """Adjust column names to lower case."""
        # Rename columns to lowercase if they match OHLCV
        rename_map: dict[str, str] = {}
        for col in df.columns:
            col_name = cast(str, col)
            col_lower = col_name.lower()
            if col_lower in ["open", "high", "low", "close", "volume"]:
                rename_map[col_name] = col_lower

        if rename_map:
            df = df.rename(rename_map)

        return df

    def set_dates(
        self,
        date_start: Any | None,
        date_end: Any | None,
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """Set the start and end dates of the data."""
        for dt in [date_start, date_end]:
            if dt and not isinstance(dt, datetime.datetime):
                raise TypeError(f"Start and End dates must be entered as full datetimes. {dt} was entered")

        date_start_value: Any = date_start
        date_end_value: Any = date_end

        if date_start_value is None:
            date_start_value = self.polars_df["datetime"].min()
        if date_end_value is None:
            date_end_value = self.polars_df["datetime"].max()

        date_start_aware = to_datetime_aware(_to_pydatetime_if_available(date_start_value))
        date_end_aware = to_datetime_aware(_to_pydatetime_if_available(date_end_value))

        date_start_aware = date_start_aware.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end_aware = date_end_aware.replace(hour=23, minute=59, second=59, microsecond=999999)

        return date_start_aware, date_end_aware

    def trim_data(
        self,
        df: Any,
        date_start: datetime.datetime,
        date_end: datetime.datetime,
        trading_hours_start: datetime.time,
        trading_hours_end: datetime.time,
    ) -> Any:
        """Trim the polars dataframe to match the desired backtesting dates."""
        # Align date comparisons to polars datetime column timezone (matching pandas approach)
        datetime_tz: Any | None = getattr(df["datetime"].dtype, "time_zone", None) if "datetime" in df.columns else None
        date_start_ts: Any = pd.Timestamp(date_start)
        date_end_ts: Any = pd.Timestamp(date_end)

        # Convert comparison timestamps to match column timezone
        if datetime_tz is not None:
            # Column has timezone, align dates to it
            date_start_aligned = (
                date_start_ts.tz_convert(datetime_tz)
                if getattr(date_start_ts, "tz", None) is not None
                else date_start_ts.tz_localize(datetime_tz)
            )
            date_end_aligned = (
                date_end_ts.tz_convert(datetime_tz)
                if getattr(date_end_ts, "tz", None) is not None
                else date_end_ts.tz_localize(datetime_tz)
            )
        else:
            # Column is naive, make dates naive too
            date_start_aligned = (
                date_start_ts.tz_localize(None) if getattr(date_start_ts, "tz", None) is not None else date_start_ts
            )
            date_end_aligned = (
                date_end_ts.tz_localize(None) if getattr(date_end_ts, "tz", None) is not None else date_end_ts
            )

        # Filter by date range
        df = df.filter((pl.col("datetime") >= date_start_aligned) & (pl.col("datetime") <= date_end_aligned))

        # Filter by trading hours if intraday data
        if self.timestep in {"minute", "hour"}:
            df = df.filter(
                (pl.col("datetime").dt.time() >= trading_hours_start)
                & (pl.col("datetime").dt.time() <= trading_hours_end)
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

    def repair_times_and_fill(self, idx: Any) -> None:
        """Create datalines and fill missing values.

        This converts to pandas for compatibility with the existing dataline system.
        """
        # Get pandas DataFrame
        df: Any = self.df

        # OPTIMIZATION: Use searchsorted instead of expensive boolean indexing
        start_pos = int(idx.searchsorted(self.datetime_start, side="left"))
        end_pos = int(idx.searchsorted(self.datetime_end, side="right"))
        idx = idx[start_pos:end_pos]

        # OPTIMIZATION: More efficient duplicate removal
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep="first")]

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
        iter_index_lookup: Any = pd.Series(iter_index.index, index=iter_index)
        self.iter_index = iter_index_lookup
        self.iter_index_dict = cast(dict[Any, Any], iter_index_lookup.to_dict())

        # Populate the datalines dictionary.
        self.datalines = dict()
        self.to_datalines()

    def to_datalines(self) -> None:
        """Create datalines from the pandas DataFrame."""
        df: Any = self.df
        datalines = self.datalines

        datetime_line = Dataline(
            self.asset,
            "datetime",
            df.index.to_numpy(),
            df.index.dtype,
        )
        datalines["datetime"] = datetime_line
        self.datetime = datetime_line.dataline

        for column in df.columns:
            column_name = cast(str, column)
            series = df[column_name]
            dataline = Dataline(
                self.asset,
                column_name,
                series.to_numpy(),
                series.dtype,
            )
            datalines[column_name] = dataline
            setattr(self, column_name, dataline.dataline)

        self._quote_required_cols_present = all(col in datalines for col in _DATA_REQUIRED_PRICE_COLS)
        self._quote_missing_cols = [col for col in _DATA_QUOTE_COLS if col not in datalines]
        self._quote_presence_logged = False

    def get_iter_count(self, dt: Any) -> int:
        """Return the index location for a given datetime."""
        # Check if we have the iter_index_dict, if not then repair the times and fill
        if self.iter_index_dict is None:
            self.repair_times_and_fill(self.df.index)
        if self.iter_index_dict is None or self.iter_index is None:
            raise RuntimeError("iter_index_dict was not initialized")

        # Search for dt in self.iter_index_dict
        if dt in self.iter_index_dict:
            i = self.iter_index_dict[dt]
        else:
            # If not found, get the last known data
            i = self.iter_index.asof(dt)

        return int(i)

    def check_data(func: Callable[..., Any]) -> Callable[..., Any]:  # pyright: ignore[reportGeneralTypeIssues, reportSelfClsParameterName]
        """Validates if the provided date, length, timeshift, and timestep will return data."""

        def checker(self: DataPolars, *args: Any, **kwargs: Any) -> Any:
            if type(kwargs.get("length", 1)) not in [int, float]:
                raise TypeError(f"Length must be an integer. {type(kwargs.get('length', 1))} was provided.")

            dt: Any = args[0]
            length = cast(int | float, kwargs.get("length", 1))
            timeshift_raw = kwargs.get("timeshift", 0)
            if timeshift_raw is None:
                timeshift_raw = 0
                kwargs["timeshift"] = 0

            # Check if the iter date is outside of this data's date range.
            if dt < self.datetime_start:
                raise ValueError(
                    f"The date you are looking for ({dt}) for ({self.asset}) is outside of the data's date range "
                    f"({self.datetime_start} to {self.datetime_end}). This could be because the data for this asset "
                    "does not exist for the date you are looking for, or something else."
                )

            # Search for dt in self.iter_index_dict
            if getattr(self, "iter_index_dict", None) is None:
                self.repair_times_and_fill(self.df.index)
            if self.iter_index_dict is None or self.iter_index is None:
                raise RuntimeError("iter_index_dict was not initialized")
            iter_index_dict = self.iter_index_dict
            iter_index = self.iter_index

            if dt in iter_index_dict:
                i = iter_index_dict[dt]
            else:
                # If not found, get the last known data
                i = iter_index.asof(dt)
            i = int(i)

            # Convert timeshift to integer if it's a timedelta
            if isinstance(timeshift_raw, datetime.timedelta):
                timestep = kwargs.get("timestep", self.timestep)
                if timestep == "day":
                    timeshift = timeshift_raw.days
                elif timestep == "hour":
                    timeshift = int(timeshift_raw.total_seconds() / 3600)
                else:  # minute
                    timeshift = int(timeshift_raw.total_seconds() / 60)
                kwargs["timeshift"] = timeshift
            else:
                timeshift = cast(int | float, timeshift_raw)
            data_index: int | float = i + 1 - length - timeshift
            is_data: bool = data_index >= 0
            if not is_data:
                logger.warning(
                    f"The date you are looking for ({dt}) is outside of the data's date range "
                    f"({self.datetime_start} to {self.datetime_end}) after accounting for a length of "
                    f"{kwargs.get('length', 1)} and a timeshift of {kwargs.get('timeshift', 0)}. Keep in mind that "
                    f"the requested length must also be available in your data; we are {data_index} rows away."
                )

            res = func(self, *args, **kwargs)
            return res

        return checker

    @check_data
    def get_last_price(
        self,
        dt: Any,
        length: int = 1,
        timeshift: int | datetime.timedelta | None = 0,
    ) -> float | Decimal | None:
        """Returns the last known price of the data."""
        iter_count = self.get_iter_count(dt)
        open_price = self.datalines["open"].dataline[iter_count]
        close_price = self.datalines["close"].dataline[iter_count]
        if self.timestep == "day":
            price = close_price
        else:
            price = close_price if dt > self.datalines["datetime"].dataline[iter_count] else open_price
        if price is None:
            return None
        try:
            if pd.isna(price):
                return None
        except (TypeError, ValueError):
            pass
        return cast(float | Decimal, price)

    @check_data
    def get_quote(
        self,
        dt: Any,
        length: int = 1,
        timeshift: int | datetime.timedelta | None = 0,
    ) -> dict[str, Any]:
        """Returns the last known quote data."""
        if not self._quote_required_cols_present and not self._quote_presence_logged:
            missing_price_cols = [col for col in _DATA_REQUIRED_PRICE_COLS if col not in self.datalines]
            logger.warning(
                "DataPolars object %s is missing price columns %s required for quote retrieval.",
                self.asset,
                missing_price_cols,
            )
            self._quote_presence_logged = True

        if self._quote_missing_cols and not self._quote_presence_logged:
            logger.warning(
                "DataPolars object %s is missing quote columns %s; returning None for those values.",
                self.asset,
                self._quote_missing_cols,
            )
            self._quote_presence_logged = True

        iter_count = self.get_iter_count(dt)

        def _get_value(column: str, round_digits: int | None) -> Any:
            if column not in self.datalines:
                return None
            value = self.datalines[column].dataline[iter_count]
            try:
                if round_digits is None:
                    return value
                return round(value, round_digits)
            except TypeError:
                return value

        quote_dict: dict[str, Any] = {
            name: _get_value(column, digits) for name, (column, digits) in _DATA_QUOTE_FIELDS.items()
        }

        return quote_dict

    @check_data
    def _get_bars_dict(
        self,
        dt: Any,
        length: int = 1,
        timestep: str | None = None,
        timeshift: int | datetime.timedelta | None = 0,
    ) -> dict[str, Any]:
        """Returns a dictionary of the data."""
        if timeshift is None:
            timeshift = 0

        # Convert timeshift to integer if it's a timedelta
        if isinstance(timeshift, datetime.timedelta):
            logger.debug(
                f"[TIMESHIFT_CONVERT] asset={self.symbol} input_timeshift={timeshift} type={type(timeshift)} repr={repr(timeshift)}"
            )
            ts = timestep if timestep is not None else self.timestep
            if ts == "day":
                timeshift_converted = int(timeshift.total_seconds() / (24 * 3600))
                logger.debug(
                    f"[TIMESHIFT_CONVERT] asset={self.symbol} timestep=day total_seconds={timeshift.total_seconds()} converted={timeshift_converted}"
                )
                timeshift = timeshift_converted
            elif ts == "hour":
                timeshift_converted = int(timeshift.total_seconds() / 3600)
                logger.debug(
                    f"[TIMESHIFT_CONVERT] asset={self.symbol} timestep=hour total_seconds={timeshift.total_seconds()} converted={timeshift_converted}"
                )
                timeshift = timeshift_converted
            else:  # minute
                timeshift_converted = int(timeshift.total_seconds() / 60)
                logger.debug(
                    f"[TIMESHIFT_CONVERT] asset={self.symbol} timestep=minute total_seconds={timeshift.total_seconds()} converted={timeshift_converted}"
                )
                timeshift = timeshift_converted

        # Get bars.
        end_row = self.get_iter_count(dt) - timeshift
        start_row = end_row - length

        if start_row < 0:
            start_row = 0

        # Cast both start_row and end_row to int
        start_row = int(start_row)
        end_row = int(end_row)

        bars_data: dict[str, Any] = {}
        for dl_name, dl in self.datalines.items():
            bars_data[dl_name] = dl.dataline[start_row:end_row]

        return bars_data

    def _get_bars_between_dates_dict(
        self,
        timestep: str | None = None,
        start_date: Any | None = None,
        end_date: Any | None = None,
    ) -> dict[str, Any]:
        """Returns a dictionary of all the data available between the start and end dates."""
        end_row = self.get_iter_count(end_date)
        start_row = self.get_iter_count(start_date)

        if start_row < 0:
            start_row = 0

        # Cast both start_row and end_row to int
        start_row = int(start_row)
        end_row = int(end_row)

        bars_data: dict[str, Any] = {}
        for dl_name, dl in self.datalines.items():
            bars_data[dl_name] = dl.dataline[start_row:end_row]

        return bars_data

    def get_bars(
        self,
        dt: Any,
        length: int = 1,
        timestep: str = MIN_TIMESTEP,
        timeshift: int | datetime.timedelta | None = 0,
    ) -> Any:
        """Returns a dataframe of the data."""
        if timeshift is None:
            timeshift = 0

        # Parse the timestep
        quantity_raw, timestep_raw = parse_timestep_qty_and_unit(timestep)
        quantity = int(quantity_raw)
        timestep = str(timestep_raw)
        num_periods = length
        data: dict[str, Any]

        if timestep == "minute" and self.timestep in {"day", "hour"}:
            raise ValueError(
                "You are requesting minute data from a higher-timeframe data source. This is not supported."
            )

        if timestep == "hour" and self.timestep == "day":
            raise ValueError("You are requesting hour data from a daily data source. This is not supported.")

        if timestep not in {"minute", "hour", "day"}:
            raise ValueError(f"Only minute, hour, and day are supported for timestep. You provided: {timestep}")

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

        elif timestep == "day" and self.timestep == "hour":
            length = length * 24
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="hour", timeshift=timeshift)

        elif timestep == "day" and self.timestep == "day":
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        elif timestep == "hour" and self.timestep == "minute":
            length = length * 60 * quantity
            unit = "h"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        elif timestep == "hour" and self.timestep == "hour":
            unit = "h"
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep="hour", timeshift=timeshift)

        else:
            unit = "min"
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        df: Any = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        if "dividend" in df.columns:
            agg_column_map["dividend"] = "sum"
        df_result = df.resample(f"{quantity}{unit}").agg(agg_column_map)

        # Drop any rows that have NaN values
        df_result = df_result.dropna()

        # Remove partial day data from the current day
        if timestep == "day" and self.timestep in {"minute", "hour"}:
            df_result = df_result[df_result.index < dt.replace(hour=0, minute=0, second=0, microsecond=0)]

        # Only return the last n rows
        df_result = df_result.tail(n=int(num_periods))

        return df_result

    def get_bars_between_dates(
        self,
        timestep: str = MIN_TIMESTEP,
        exchange: Any | None = None,
        start_date: Any | None = None,
        end_date: Any | None = None,
    ) -> Any:
        """Returns a dataframe of all the data available between the start and end dates."""
        quantity_raw, timestep_raw = parse_timestep_qty_and_unit(timestep)
        quantity = int(quantity_raw)
        timestep = str(timestep_raw)

        if timestep == "minute" and self.timestep in {"day", "hour"}:
            raise ValueError(
                "You are requesting minute data from a higher-timeframe data source. This is not supported."
            )

        if timestep == "hour" and self.timestep == "day":
            raise ValueError("You are requesting hour data from a daily data source. This is not supported.")

        if timestep not in {"minute", "hour", "day"}:
            raise ValueError(f"Only minute, hour, and day are supported for timestep. You provided: {timestep}")

        data = self._get_bars_between_dates_dict(timestep=timestep, start_date=start_date, end_date=end_date)

        df: Any = pd.DataFrame(data).set_index("datetime")
        if df is None or df.empty:
            return df

        if timestep == "minute" and int(quantity) == 1:
            return df

        agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        if "dividend" in df.columns:
            agg["dividend"] = "sum"

        unit_code = "min" if timestep == "minute" else "h" if timestep == "hour" else "D"
        return df.resample(f"{int(quantity)}{unit_code}").agg(agg).dropna()
