import datetime
from decimal import Decimal
from typing import Optional, Union

import numpy as np

import pandas as pd

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
from lumibot.tools.helpers import parse_timestep_qty_and_unit, to_datetime_aware
from lumibot.tools.lumibot_logger import get_logger

from .asset import Asset
from .dataline import Dataline

logger = get_logger(__name__)

# PERF: Constants used in tight quote loops (avoid per-call allocations).
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
_DATA_QUOTE_FIELDS = {
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

# PERF: module-level sentinel used to avoid eager-evaluating fallbacks in `getattr()` hot paths.
_MISSING = object()

# Set the option to raise an error if downcasting is not possible (if available in this pandas version)
try:
    pd.set_option('future.no_silent_downcasting', True)
except (pd._config.config.OptionError, AttributeError):
    # Option not available in this pandas version, skip it
    pass


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
        {"timestep": "hour", "representations": ["1H", "hour"]},
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

        if "crypto" == self.asset.asset_type and quote is None:
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

        if timestep not in ["minute", "hour", "day"]:
            raise ValueError(
                f"Timestep must be one of 'minute', 'hour', or 'day'. You entered: {timestep}"
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
        # PERF: many hot paths (quotes/bars) assume the underlying index is unique. Cache this once.
        # When the index is unique and we're not resampling, we can return bars without expensive resample/agg work.
        self._index_is_unique = bool(getattr(self.df.index, "is_unique", False))

        self.trading_hours_start, self.trading_hours_end = self.set_times(trading_hours_start, trading_hours_end)
        self.date_start, self.date_end = self.set_dates(date_start, date_end)

        self.df = self.trim_data(
            self.df,
            self.date_start,
            self.date_end,
            self.trading_hours_start,
            self.trading_hours_end,
        )
        # PERF: `get_bars()` is called extremely frequently in minute-level backtests. Cache the
        # current dataset length once so hot paths don't repeatedly call `len(DatetimeIndex)`.
        try:
            self._data_len = int(len(self.df.index))
        except Exception:
            self._data_len = None
        # PERF: `check_data` compares python datetimes against these bounds in tight loops.
        # Storing them as python datetimes avoids pandas scalar validation/conversion overhead.
        start_ts = self.df.index[0]
        end_ts = self.df.index[-1]
        self.datetime_start = start_ts.to_pydatetime() if isinstance(start_ts, pd.Timestamp) else start_ts
        self.datetime_end = end_ts.to_pydatetime() if isinstance(end_ts, pd.Timestamp) else end_ts

        # PERF: `get_bars()` is called extremely frequently in minute-level backtests.
        # Avoid doing expensive pandas operations (dropna/fillna) on every slice when we can
        # prove the underlying dataset is already complete.
        #
        # This keeps correctness: if the dataset contains NaNs in any OHLC column, we keep
        # the legacy dropna path for every slice.
        self._ohlc_has_nan = False
        self._volume_has_nan = False
        self._dividend_has_nan = False
        try:
            required = [c for c in ("open", "high", "low", "close") if c in self.df.columns]
            if required:
                try:
                    values = self.df[required].to_numpy(copy=False)
                except Exception:
                    values = self.df[required].to_numpy()
                self._ohlc_has_nan = bool(pd.isna(values).any())
            if "volume" in self.df.columns:
                self._volume_has_nan = bool(pd.isna(self.df["volume"].to_numpy(copy=False)).any())
            if "dividend" in self.df.columns:
                self._dividend_has_nan = bool(pd.isna(self.df["dividend"].to_numpy(copy=False)).any())
        except Exception:
            self._ohlc_has_nan = True
            self._volume_has_nan = True
            self._dividend_has_nan = True

        # PERF: `get_bars()` slices and then selects OHLCV columns on every call. Cache a stable
        # OHLCV view once (initialized lazily after `repair_times_and_fill()` so it reflects any
        # NaN filling performed there).
        bars_cols = ["open", "high", "low", "close", "volume"]
        if "dividend" in self.df.columns:
            bars_cols.append("dividend")
        self._bars_cols = [c for c in bars_cols if c in self.df.columns]
        self._bars_df = None
        # PERF: `get_bars()` performs repeated `col in df.columns` membership checks which go
        # through `Index.__contains__` (hot in minute backtests). Cache the schema facts once.
        self._bars_has_volume = "volume" in self._bars_cols
        self._bars_has_dividend = "dividend" in self._bars_cols
        self._bars_required_cols = [c for c in ("open", "high", "low", "close") if c in self._bars_cols]

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
            df.index = pd.to_datetime(df.index).tz_localize(DEFAULT_PYTZ)
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
        if self.timestep in {"minute", "hour"}:
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

    # ./lumibot/build/__editable__.lumibot-3.1.14-py3-none-any/lumibot/entities/data.py:280:
    # FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version.
    # Call result.infer_objects(copy=False) instead.
    # To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`

    def repair_times_and_fill(self, idx):
        # OPTIMIZATION: Use searchsorted instead of expensive boolean indexing
        # Replace: idx[(idx >= self.datetime_start) & (idx <= self.datetime_end)]
        start_pos = idx.searchsorted(self.datetime_start, side='left')
        end_pos = idx.searchsorted(self.datetime_end, side='right')
        idx = idx[start_pos:end_pos]

        # OPTIMIZATION: More efficient duplicate removal
        if self.df.index.has_duplicates:
            self.df = self.df[~self.df.index.duplicated(keep='first')]

        # Reindex the DataFrame with the new index and forward-fill missing values.
        df = self.df.reindex(idx, method="ffill")

        # Check if we have a volume column, if not then add it and fill with 0 or NaN.
        if "volume" in df.columns:
            df.loc[df["volume"].isna(), "volume"] = 0
        else:
            df["volume"] = None

        # CRITICAL FIX: Time-gap aware forward-fill for bid/ask columns
        # Prevent stale weekend/after-hours quote data from being forward-filled
        # into the first bar of a new trading session.
        # The reindex above already did ffill, but we need to UNDO it for bid/ask
        # where there's a large time gap (> 2 hours).
        quote_cols = ["bid", "ask", "bid_size", "ask_size"]
        quote_cols_present = [col for col in quote_cols if col in df.columns]
        apply_quote_session_boundaries = self.timestep in {"minute", "hour"}

        # NOTE: Only apply session-boundary quote clearing for minute data.
        # Daily datasets (e.g., option EOD NBBO) are intentionally sparse and must be forward-filled
        # across sessions so mark-to-market pricing remains stable between observations.
        if apply_quote_session_boundaries and quote_cols_present and isinstance(df.index, pd.DatetimeIndex):
            # Calculate time gaps between consecutive rows
            time_diff = df.index.to_series().diff()
            max_gap_minutes = 120  # 2 hours - allows filling within a session
            gap_threshold = pd.Timedelta(minutes=max_gap_minutes)
            session_boundaries = time_diff > gap_threshold

            if session_boundaries.sum() > 0:
                # At session boundaries, revert bid/ask to NaN (undo the reindex ffill)
                # We need to get the ORIGINAL values from self.df to check if they were NaN
                for col in quote_cols_present:
                    if col in self.df.columns:
                        # Create a series aligned to df's index. Values will be NaN for newly
                        # introduced rows during the reindex/ffill step.
                        original_values = self.df[col].reindex(df.index)

                        # Only clear values that were forward-filled across a large time gap.
                        # Do NOT wipe real quotes that exist on the boundary bar (common for daily EOD NBBO).
                        clear_mask = session_boundaries & original_values.isna()
                        if clear_mask.any():
                            df.loc[clear_mask, col] = float("nan")

        # OPTIMIZATION: More efficient column selection and forward fill
        ohlc_cols = ["open", "high", "low"]
        # MODIFIED: Exclude bid/ask from standard ffill - handle them separately
        quote_cols_set = set(quote_cols)
        non_ohlc_cols = [col for col in df.columns if col not in ohlc_cols and col not in quote_cols_set]
        if non_ohlc_cols:
            df[non_ohlc_cols] = df[non_ohlc_cols].ffill()

        # For quote columns, do segment-wise ffill (don't fill across session boundaries)
        if apply_quote_session_boundaries and quote_cols_present and isinstance(df.index, pd.DatetimeIndex):
            time_diff = df.index.to_series().diff()
            max_gap_minutes = 120
            gap_threshold = pd.Timedelta(minutes=max_gap_minutes)
            session_boundaries = time_diff > gap_threshold
            segment_ids = session_boundaries.cumsum()

            for col in quote_cols_present:
                # Group by segment and forward-fill within each group only
                df[col] = df.groupby(segment_ids)[col].ffill()

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

        # PERF: `Bars.__init__` historically computed derived columns (notably `return`) on every
        # slice returned by `get_historical_prices()`. In minute-level backtests this is a dominant
        # cost because strategies often request historical windows every iteration. Precompute these
        # derived columns once per underlying dataset so per-call work is mostly slicing.
        #
        # NOTE: This intentionally favors speed. When slices include `return`, the first row's
        # `return` value reflects the prior row from the full series (not NaN as if computed on the
        # slice). This is generally acceptable and avoids repeated DataFrame column insertions.
        try:
            if "dividend" in df.columns:
                missing = any(c not in df.columns for c in ("price_change", "dividend_yield", "return"))
                if missing and "close" in df.columns:
                    close_series = df["close"]
                    try:
                        close = close_series.to_numpy(dtype="float64", copy=False)
                    except Exception:
                        close = pd.to_numeric(close_series, errors="coerce").to_numpy(dtype="float64", copy=False)

                    price_change = np.empty(len(close), dtype="float64")
                    price_change[:] = np.nan
                    if len(close) > 1:
                        prev = close[:-1]
                        curr = close[1:]
                        with np.errstate(divide="ignore", invalid="ignore"):
                            price_change[1:] = (curr - prev) / prev

                    div_series = df["dividend"]
                    try:
                        div = div_series.to_numpy(dtype="float64", copy=False)
                    except Exception:
                        div = pd.to_numeric(div_series, errors="coerce").to_numpy(dtype="float64", copy=False)
                    with np.errstate(divide="ignore", invalid="ignore"):
                        dividend_yield = div / close

                    df["price_change"] = price_change
                    df["dividend_yield"] = dividend_yield
                    df["return"] = dividend_yield + price_change
            else:
                if "return" not in df.columns and "close" in df.columns:
                    close_series = df["close"]
                    try:
                        close = close_series.to_numpy(dtype="float64", copy=False)
                    except Exception:
                        close = pd.to_numeric(close_series, errors="coerce").to_numpy(dtype="float64", copy=False)

                    returns = np.empty(len(close), dtype="float64")
                    returns[:] = np.nan
                    if len(close) > 1:
                        prev = close[:-1]
                        curr = close[1:]
                        with np.errstate(divide="ignore", invalid="ignore"):
                            returns[1:] = (curr - prev) / prev
                    df["return"] = returns
        except Exception:
            logger.debug("[DATA][REPAIR] failed to precompute derived columns", exc_info=True)

        self.df = df
        try:
            self._data_len = int(len(self.df.index))
        except Exception:
            self._data_len = None

        # Set up iter_index and iter_index_dict for later use.
        iter_index = pd.Series(df.index)
        self.iter_index = pd.Series(iter_index.index, index=iter_index)
        # PERF: `to_dict()` produces keys as `pd.Timestamp`, which do not hash-equal to
        # `datetime.datetime` objects. Many hot paths pass python datetimes, causing dictionary
        # misses and forcing an expensive `Series.asof()` fallback.
        #
        # Store a second mapping keyed by python datetimes so `dt in iter_index_dict` is fast.
        self.iter_index_dict = {ts.to_pydatetime(): int(pos) for ts, pos in self.iter_index.items()}

        # PERF: Precompute an integer nanoseconds view of the datetime index so `get_iter_count()`
        # can use NumPy search/forward cursors without triggering pandas datetime scalar validation.
        #
        # NOTE: For tz-aware indexes, `.asi8` is UTC nanoseconds since epoch, which matches
        # `datetime.timestamp()` semantics for tz-aware python datetimes.
        try:
            self._index_values_ns = self.df.index.asi8
        except Exception:
            self._index_values_ns = None

        # Reset the per-series cursor used by `get_iter_count()` (safe; backtests are single-threaded).
        self._iter_count_cursor_ns = None
        self._iter_count_cursor_i = 0
        self._iter_count_last_dt_key = None

        # Populate the datalines dictionary (assuming to_datalines is defined elsewhere).
        self.datalines = dict()
        self.to_datalines()

        # Initialize the cached OHLCV view after any in-place NaN filling above so `get_bars()`
        # does not retain a stale pre-fill view (important for stubbed test fixtures that start
        # with NaNs in open/high/low but expect them to be filled from close).
        try:
            # Update cached column list if we added derived columns above.
            bars_cols = ["open", "high", "low", "close", "volume"]
            if "dividend" in self.df.columns:
                bars_cols.append("dividend")
                for col in ("price_change", "dividend_yield", "return"):
                    if col in self.df.columns:
                        bars_cols.append(col)
            else:
                if "return" in self.df.columns:
                    bars_cols.append("return")

            self._bars_cols = [c for c in bars_cols if c in self.df.columns]
            if self._bars_cols:
                # `df[cols]` can produce a view with `_is_copy` metadata; downstream `Bars` may
                # legitimately add derived columns (e.g., `return`), which would otherwise emit
                # SettingWithCopyWarning in tight backtest loops.
                self._bars_df = self.df[self._bars_cols].copy(deep=False)
        except Exception:
            self._bars_df = None

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
        self.datetime = self.datalines["datetime"].dataline

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

        # Cache column presence flags for `get_quote()` which is called extremely frequently.
        self._quote_required_cols_present = all(col in self.datalines for col in _DATA_REQUIRED_PRICE_COLS)
        self._quote_missing_cols = [col for col in _DATA_QUOTE_COLS if col not in self.datalines]
        self._quote_presence_logged = False

    def get_iter_count(self, dt):
        # Return the index location for a given datetime.

        # Check if the date is in the dataframe, if not then get the last
        # known data (this speeds up the process)
        i = None

        # Check if we have the iter_index_dict, if not then repair the times and fill (which will create the iter_index_dict)
        if getattr(self, "iter_index_dict", None) is None:
            self.repair_times_and_fill(self.df.index)

        # Normalize dt to a python datetime for fast dict lookups.
        # Callers can pass `pd.Timestamp` (common in pandas-heavy code paths); mixing Timestamp and
        # datetime keys leads to misses and forces an expensive `asof()` fallback.
        dt_key = dt.to_pydatetime() if isinstance(dt, pd.Timestamp) else dt

        # PERF: repeated calls in the same iteration commonly ask for the same `(data, dt)`.
        # Short-circuit before any dict membership/search work.
        last_dt_key = getattr(self, "_iter_count_last_dt_key", None)
        if last_dt_key == dt_key:
            cursor_i = getattr(self, "_iter_count_cursor_i", None)
            if cursor_i is not None:
                return int(cursor_i)

        # Fast-path: exact bar timestamp lookup.
        i = self.iter_index_dict.get(dt_key)
        if i is not None:
            index_ns = getattr(self, "_index_values_ns", None)
            if index_ns is not None:
                try:
                    # Cursor uses the index's own value to avoid timestamp() float math.
                    self._iter_count_cursor_ns = int(index_ns[int(i)])
                except Exception:
                    self._iter_count_cursor_ns = None
            self._iter_count_cursor_i = int(i)
            self._iter_count_last_dt_key = dt_key
            return i

        # Fast-path: monotonic cursor (common in backtests where dt advances by 1 bar).
        index_ns = getattr(self, "_index_values_ns", None)
        if index_ns is not None:
            try:
                dt_ns = int(dt_key.timestamp() * 1_000_000_000)
            except Exception:
                dt_ns = None

            if dt_ns is not None:
                cursor_ns = getattr(self, "_iter_count_cursor_ns", None)
                cursor_i = getattr(self, "_iter_count_cursor_i", None)
                if cursor_ns is not None and cursor_i is not None and dt_ns >= int(cursor_ns):
                    i = int(cursor_i)
                    n = len(index_ns)
                    while (i + 1) < n and int(index_ns[i + 1]) <= dt_ns:
                        i += 1
                    self._iter_count_cursor_ns = dt_ns
                    self._iter_count_cursor_i = i
                    self._iter_count_last_dt_key = dt_key
                    return i

                # Fallback: binary search on the integer index.
                i = int(np.searchsorted(index_ns, dt_ns, side="right")) - 1
                self._iter_count_cursor_ns = dt_ns
                self._iter_count_cursor_i = i
                self._iter_count_last_dt_key = dt_key
                return i

        # Fallback: pandas searchsorted (kept for safety when the fast-path index is unavailable).
        i = int(self.df.index.searchsorted(dt_key, side="right")) - 1
        self._iter_count_cursor_ns = None
        self._iter_count_cursor_i = int(i)
        self._iter_count_last_dt_key = dt_key
        return i

    def check_data(func):
        # Validates if the provided date, length, timeshift, and timestep
        # will return data. Runs function if data, returns None if no data.
        def checker(self, *args, **kwargs):
            if type(kwargs.get("length", 1)) not in [int, float]:
                raise TypeError(f"Length must be an integer. {type(kwargs.get('length', 1))} was provided.")

            dt = args[0]
            dt_key = dt.to_pydatetime() if isinstance(dt, pd.Timestamp) else dt
            length = kwargs.get("length", 1)
            timeshift = kwargs.get("timeshift", 0)
            if timeshift is None:
                # Strategy.get_historical_prices defaults `timeshift=None` (no shift). Treat None
                # equivalently to 0 so backtests don't crash when callers omit timeshift.
                timeshift = 0
                kwargs["timeshift"] = 0

            if isinstance(timeshift, datetime.timedelta):
                if self.timestep == "day":
                    timeshift = int(timeshift.total_seconds() / (24 * 3600))
                elif self.timestep == "hour":
                    timeshift = int(timeshift.total_seconds() / 3600)
                else:
                    timeshift = int(timeshift.total_seconds() / 60)
                kwargs["timeshift"] = timeshift

            # Check if the iter date is outside of this data's date range.
            if dt_key < self.datetime_start:
                raise ValueError(
                    f"The date you are looking for ({dt_key}) for ({self.asset}) is outside of the data's date range ({self.datetime_start} to {self.datetime_end}). This could be because the data for this asset does not exist for the date you are looking for, or something else."
                )

            # For daily data, compare dates (not timestamps) to handle timezone issues.
            # ThetaData daily bars are timestamped at 00:00 UTC, which when converted to EST
            # appears as the previous day's evening. A bar for Nov 3 00:00 UTC represents
            # trading on Nov 3 and should cover the entire Nov 3 trading day.
            dt_exceeds_end = False
            if self.timestep == "day":
                # Convert datetime_end to UTC to get the actual date the bar represents
                import pytz
                utc = pytz.UTC
                if hasattr(self.datetime_end, 'astimezone'):
                    datetime_end_utc = self.datetime_end.astimezone(utc)
                else:
                    datetime_end_utc = self.datetime_end
                datetime_end_date = datetime_end_utc.date()
                dt_date = dt_key.date()
                dt_exceeds_end = dt_date > datetime_end_date
            else:
                dt_exceeds_end = dt_key > self.datetime_end

            if dt_exceeds_end:
                strict_end_check = getattr(self, "strict_end_check", False)
                if strict_end_check:
                    raise ValueError(
                        f"The date you are looking for ({dt_key}) for ({self.asset}) is after the available data's end ({self.datetime_end}) with length={length} and timeshift={timeshift}; data refresh required instead of using stale bars."
                    )
                gap = dt_key - self.datetime_end
                max_gap = datetime.timedelta(days=3)
                if gap > max_gap:
                    raise ValueError(
                        f"The date you are looking for ({dt_key}) for ({self.asset}) is after the available data's end ({self.datetime_end}) with length={length} and timeshift={timeshift}; data refresh required instead of using stale bars."
                    )
                message = (
                    f"The date you are looking for ({dt_key}) is after the available data's end ({self.datetime_end}) by {gap}. "
                    f"Using the last available bar (within tolerance of {max_gap})."
                )
                if self.timestep == "day":
                    logger.debug(message)
                else:
                    logger.warning(message)

            # Search for dt in self.iter_index_dict
            if getattr(self, "iter_index_dict", None) is None:
                self.repair_times_and_fill(self.df.index)

            # Use the optimized iter-count implementation (dict hit, cursor, or searchsorted fallback).
            i = self.get_iter_count(dt_key)

            data_index = i + 1 - length - timeshift
            is_data = data_index >= 0
            if not is_data:
                # Log a warning
                logger.warning(
                    f"The date you are looking for ({dt_key}) is outside of the data's date range ({self.datetime_start} to {self.datetime_end}) after accounting for a length of {kwargs.get('length', 1)} and a timeshift of {kwargs.get('timeshift', 0)}. Keep in mind that the length you are requesting must also be available in your data, in this case we are {data_index} rows away from the data you need."
                )
                try:
                    idx_vals = self.df.index
                    idx_min = idx_vals.min()
                    idx_max = idx_vals.max()
                    logger.info(
                        "[DATA][CHECK] asset=%s timestep=%s dt=%s length=%s timeshift=%s iter_index=%s idx_min=%s idx_max=%s rows=%s",
                        getattr(self.asset, "symbol", self.asset),
                        getattr(self, "timestep", None),
                        dt_key,
                        length,
                        timeshift,
                        i,
                        idx_min,
                        idx_max,
                        len(idx_vals),
                    )
                except Exception:
                    logger.debug("[DATA][CHECK] failed to log index diagnostics", exc_info=True)

            res = func(self, *args, **kwargs)
            # print(f"Results last price: {res}")
            return res

        return checker

    @check_data
    def get_last_price(self, dt, length=1, timeshift=0) -> Union[float, Decimal, None]:
        """Returns the last known price of the data.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the last price.
        length : int
            The number of periods to get the last price.
        timestep : str
            The frequency of the data to get the last price.
        timeshift : int | datetime.timedelta
            The number of periods to shift the data, or a timedelta that will be converted to periods.

        Returns
        -------
        float or Decimal or None
            Returns the close price (or open price for intraday before bar completion).
            
            IMPORTANT: This method is trade/bar based only. It never falls back to bid/ask
            quotes. Use `get_quote()` / `get_price_snapshot()` for quote/mark pricing.
        """
        iter_count = self.get_iter_count(dt)
        open_price = self.datalines["open"].dataline[iter_count]
        close_price = self.datalines["close"].dataline[iter_count]
        # For daily bars, use the completed session's close; using the open can miss drawdowns.
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

        return price

    @check_data
    def get_price_snapshot(self, dt, length=1, timeshift=0):
        """Return OHLC, bid/ask, and timestamp metadata for the provided datetime."""
        iter_count = self.get_iter_count(dt)

        def _get_value(column: str):
            if column not in self.datalines:
                return None
            return self.datalines[column].dataline[iter_count]

        def _get_timestamp(column: str) -> Optional[datetime.datetime]:
            if column not in self.datalines:
                return None
            raw_value = self.datalines[column].dataline[iter_count]
            if raw_value is None:
                return None
            if isinstance(raw_value, float) and np.isnan(raw_value):
                return None
            if pd.isna(raw_value):
                return None
            if isinstance(raw_value, pd.Timestamp):
                return raw_value.to_pydatetime()
            if isinstance(raw_value, datetime.datetime):
                return raw_value
            try:
                ts = pd.Timestamp(raw_value)
            except Exception:
                return None
            return ts.to_pydatetime()

        snapshot = {
            "open": _get_value("open"),
            "high": _get_value("high"),
            "low": _get_value("low"),
            "close": _get_value("close"),
            "bid": _get_value("bid"),
            "ask": _get_value("ask"),
            "last_trade_time": _get_timestamp("last_trade_time"),
            "last_bid_time": _get_timestamp("last_bid_time"),
            "last_ask_time": _get_timestamp("last_ask_time"),
        }
        return snapshot

    @check_data
    def get_quote(self, dt, length=1, timeshift=0):
        """Returns the last known price of the data.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the last price.
        length : int
            The number of periods to get the last price.
        timestep : str
            The frequency of the data to get the last price.
        timeshift : int | datetime.timedelta
            The number of periods to shift the data, or a timedelta that will be converted to periods.

        Returns
        -------
        dict
        """
        if not getattr(self, "_quote_required_cols_present", True):
            # Log once per Data instance; avoid per-call warning spam in tight loops.
            #
            # IMPORTANT: Quote history datasets (e.g., ThetaData option NBBO) may not contain OHLCV
            # columns, but we still want to surface bid/ask. Missing price columns simply return
            # None for those fields.
            if not getattr(self, "_quote_presence_logged", False):
                missing_price_cols = [col for col in _DATA_REQUIRED_PRICE_COLS if col not in self.datalines]
                logger.warning(
                    "Data object %s is missing price columns %s required for quote retrieval.",
                    self.asset,
                    missing_price_cols,
                )
                self._quote_presence_logged = True

        missing_quote_cols = getattr(self, "_quote_missing_cols", None)
        if missing_quote_cols and not getattr(self, "_quote_presence_logged", False):
            logger.warning(
                "Data object %s is missing quote columns %s; returning None for those values.",
                self.asset,
                missing_quote_cols,
            )
            self._quote_presence_logged = True

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

        quote_dict = {name: _get_value(column, digits) for name, (column, digits) in _DATA_QUOTE_FIELDS.items()}

        return quote_dict

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
        if timeshift is None:
            timeshift = 0

        if isinstance(timeshift, datetime.timedelta):
            if self.timestep == "day":
                timeshift = int(timeshift.total_seconds() / (24 * 3600))
            elif self.timestep == "hour":
                timeshift = int(timeshift.total_seconds() / 3600)
            else:
                timeshift = int(timeshift.total_seconds() / 60)

        iter_count = self.get_iter_count(dt)
        try:
            if pd.isna(iter_count):
                iter_count = 0
        except Exception:
            pass

        # IMPORTANT:
        # - This method slices with `end_row` as an *exclusive* bound.
        # - For daily data, `get_iter_count()` returns the last bar <= dt, and daily bars are
        #   already "complete" for any intraday dt on that date. We therefore add +1 so the
        #   last available bar is included by default.
        # - For intraday data, the legacy behaviour is preserved to avoid lookahead.
        if self.timestep == "day":
            end_row = iter_count + 1 - timeshift
        else:
            end_row = iter_count - timeshift

        data_len = len(next(iter(self.datalines.values())).dataline) if self.datalines else 0
        if end_row > data_len:
            end_row = data_len
        if end_row < 0:
            end_row = 0

        start_row = end_row - length
        if start_row < 0:
            start_row = 0
        if start_row > end_row:
            start_row = end_row
        if start_row == end_row and end_row > 0:
            start_row = max(0, end_row - 1)

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
        if timeshift is None:
            timeshift = 0
        # Parse the timestep
        quantity, timestep = parse_timestep_qty_and_unit(timestep)
        num_periods = length

        if timestep == "minute" and self.timestep in {"day", "hour"}:
            raise ValueError(
                "You are requesting minute data from a higher-timeframe data source. This is not supported."
            )

        if timestep == "hour" and self.timestep == "day":
            raise ValueError("You are requesting hour data from a daily data source. This is not supported.")

        if timestep not in {"minute", "hour", "day"}:
            raise ValueError(f"Only minute, hour, and day are supported for timestep. You provided: {timestep}")

        # Fast-path: when the underlying dataset is already in the requested multi-minute cadence
        # (e.g., IBKR-native "15min" bars loaded into a minute-based Data object), avoid building
        # a minute-level window and resampling on every call. Slice the native series directly.
        #
        # This is a key component of "prefetch once → slice forever" speed: a strategy that requests
        # 15-minute history every iteration should not pay a resample cost every time.
        native_qty = getattr(self, "_native_timestep_quantity", 1)
        native_unit = str(getattr(self, "_native_timestep_unit", "") or "").strip().lower()
        if (
            timestep == "minute"
            and self.timestep == "minute"
            and int(quantity) > 1
            and self._index_is_unique
            and int(native_qty) == int(quantity)
            and native_unit == "minute"
        ):
            try:
                iter_count = self.get_iter_count(dt)
                if pd.isna(iter_count):
                    iter_count = 0
            except Exception:
                iter_count = self.get_iter_count(dt)

            df_source = getattr(self, "_bars_df", None)
            if df_source is None:
                try:
                    bars_cols = getattr(self, "_bars_cols", None)
                    df_source = self.df[bars_cols].copy(deep=False) if bars_cols else self.df
                    if bars_cols:
                        self._bars_df = df_source
                except Exception:
                    df_source = self.df

            if isinstance(timeshift, datetime.timedelta):
                timeshift = int(timeshift.total_seconds() / 60)

            end_row = int(iter_count) - int(timeshift or 0)
            data_len = getattr(self, "_data_len", None)
            if data_len is None:
                data_len = int(len(df_source.index))
                self._data_len = data_len
            end_row = max(0, min(end_row, data_len))
            start_row = max(0, end_row - int(num_periods))
            if start_row > end_row:
                start_row = end_row
            if start_row == end_row and end_row > 0:
                start_row = max(0, end_row - 1)

            # PERF: Many strategies request multi-minute history every minute (e.g., 15m SMA while
            # running on a 1m cadence). When the "current" native bar has not advanced, the
            # resulting slice is identical. Cache the last slice to avoid repeated DataFrame
            # construction and allow downstream `Bars` to reuse precomputed derived columns.
            cache_key = (
                "native_multi_minute",
                int(quantity),
                int(num_periods),
                int(timeshift or 0),
                int(start_row),
                int(end_row),
            )
            cached_key = getattr(self, "_get_bars_slice_cache_key", None)
            if cached_key == cache_key:
                cached_df = getattr(self, "_get_bars_slice_cache_df", None)
                if cached_df is not None and cached_df.shape[0] != 0:
                    return cached_df

            # PERF: `.iloc[start:end]` goes through the indexer stack (`_iLocIndexer`) which
            # performs validation on every call. In backtesting we already operate on integer
            # row bounds; `_slice()` is the internal fast-path that avoids the indexer overhead.
            df = df_source._slice(slice(start_row, end_row))
            if df is None or df.shape[0] == 0:
                return None

            # PERF: avoid `col in df.columns` membership checks (`Index.__contains__`) on every call.
            has_volume = getattr(self, "_bars_has_volume", _MISSING)
            if has_volume is _MISSING:
                has_volume = "volume" in df.columns
                self._bars_has_volume = has_volume
            has_dividend = getattr(self, "_bars_has_dividend", _MISSING)
            if has_dividend is _MISSING:
                has_dividend = "dividend" in df.columns
                self._bars_has_dividend = has_dividend

            # PERF: avoid fillna on every slice unless the dataset actually contains NaNs.
            needs_copy = False
            if has_volume and getattr(self, "_volume_has_nan", False):
                needs_copy = True
            if has_dividend and getattr(self, "_dividend_has_nan", False):
                needs_copy = True
            if needs_copy:
                df = df.copy()
                if has_volume and getattr(self, "_volume_has_nan", False):
                    df["volume"] = df["volume"].fillna(0)
                if has_dividend and getattr(self, "_dividend_has_nan", False):
                    df["dividend"] = df["dividend"].fillna(0)

            required = getattr(self, "_bars_required_cols", None)
            if required is None:
                required = [c for c in ("open", "high", "low", "close") if c in df.columns]
                self._bars_required_cols = required
            if required and getattr(self, "_ohlc_has_nan", True):
                df = df.dropna(subset=required)

            self._get_bars_slice_cache_key = cache_key
            self._get_bars_slice_cache_df = df
            return df

        agg_column_map = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        # Fast-path: requesting native bars (1 minute or 1 day) from a Data object that is already in
        # that native timestep can avoid building dataline dicts and a resample/agg per call.
        #
        # IMPORTANT: this block must run *before* `_get_bars_dict()` to avoid paying for an unused
        # dataline slice when we can slice `self.df` directly.
        if (
            quantity == 1
            and self._index_is_unique
            and (
                (timestep == "minute" and self.timestep == "minute")
                or (timestep == "day" and self.timestep == "day")
            )
        ):
            # PERF: avoid reconstructing a DataFrame from datalines on every call.
            # The underlying `self.df` is already indexed by datetime, so we can slice by
            # row bounds in O(1) and return a stable OHLCV schema.
            try:
                iter_count = self.get_iter_count(dt)
                if pd.isna(iter_count):
                    iter_count = 0
            except Exception:
                iter_count = self.get_iter_count(dt)

            df_source = getattr(self, "_bars_df", None)
            if df_source is None:
                try:
                    bars_cols = getattr(self, "_bars_cols", None)
                    df_source = self.df[bars_cols].copy(deep=False) if bars_cols else self.df
                    if bars_cols:
                        self._bars_df = df_source
                except Exception:
                    df_source = self.df

            if isinstance(timeshift, datetime.timedelta):
                if self.timestep == "day":
                    timeshift = int(timeshift.total_seconds() / (24 * 3600))
                elif self.timestep == "hour":
                    timeshift = int(timeshift.total_seconds() / 3600)
                else:
                    timeshift = int(timeshift.total_seconds() / 60)

            if self.timestep == "day":
                end_row = int(iter_count) + 1 - int(timeshift or 0)
            else:
                end_row = int(iter_count) - int(timeshift or 0)

            data_len = getattr(self, "_data_len", None)
            if data_len is None:
                data_len = int(len(df_source.index))
                self._data_len = data_len
            end_row = max(0, min(end_row, data_len))
            start_row = max(0, end_row - int(length))
            if start_row > end_row:
                start_row = end_row
            if start_row == end_row and end_row > 0:
                start_row = max(0, end_row - 1)

            # PERF: Cache the last native slice. This is particularly effective for `timestep="day"`
            # requests when strategies run on an intraday cadence: the daily window only changes
            # at most once per day, so most calls can reuse the same slice.
            cache_key = (
                "native_1",
                str(timestep),
                int(length),
                int(timeshift or 0),
                int(start_row),
                int(end_row),
            )
            cached_key = getattr(self, "_get_bars_slice_cache_key", None)
            if cached_key == cache_key:
                cached_df = getattr(self, "_get_bars_slice_cache_df", None)
                if cached_df is not None and cached_df.shape[0] != 0:
                    return cached_df

            # PERF: `.iloc[start:end]` goes through the indexer stack (`_iLocIndexer`) which
            # performs validation on every call. In backtesting we already operate on integer
            # row bounds; `_slice()` is the internal fast-path that avoids the indexer overhead.
            df = df_source._slice(slice(start_row, end_row))
            if df is None or df.shape[0] == 0:
                return None

            # PERF: avoid `col in df.columns` membership checks (`Index.__contains__`) on every call.
            has_volume = getattr(self, "_bars_has_volume", _MISSING)
            if has_volume is _MISSING:
                has_volume = "volume" in df.columns
                self._bars_has_volume = has_volume
            has_dividend = getattr(self, "_bars_has_dividend", _MISSING)
            if has_dividend is _MISSING:
                has_dividend = "dividend" in df.columns
                self._bars_has_dividend = has_dividend

            # PERF: avoid fillna on every slice unless the dataset actually contains NaNs.
            needs_copy = False
            if has_volume and getattr(self, "_volume_has_nan", False):
                needs_copy = True
            if has_dividend and getattr(self, "_dividend_has_nan", False):
                needs_copy = True
            if needs_copy:
                df = df.copy()
                if has_volume and getattr(self, "_volume_has_nan", False):
                    df["volume"] = df["volume"].fillna(0)
                if has_dividend and getattr(self, "_dividend_has_nan", False):
                    df["dividend"] = df["dividend"].fillna(0)

            required = getattr(self, "_bars_required_cols", None)
            if required is None:
                required = [c for c in ("open", "high", "low", "close") if c in df.columns]
                self._bars_required_cols = required
            if required and getattr(self, "_ohlc_has_nan", True):
                df = df.dropna(subset=required)

            self._get_bars_slice_cache_key = cache_key
            self._get_bars_slice_cache_df = df
            return df

        if timestep == "day" and self.timestep == "minute":
            # If the data is minute data and we are requesting daily data then multiply the length by 1440
            length = length * 1440
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        elif timestep == "day" and self.timestep == "hour":
            # If the data is hourly data and we are requesting daily data then multiply the length by 24
            length = length * 24
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="hour", timeshift=timeshift)

        elif timestep == 'day' and self.timestep == 'day':
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        elif timestep == "hour" and self.timestep == "minute":
            # Convert requested hours to minutes to pull enough base data for resample.
            length = length * 60 * quantity
            unit = "h"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        elif timestep == "hour" and self.timestep == "hour":
            unit = "h"
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep="hour", timeshift=timeshift)

        else:
            unit = "min"  # Guaranteed to be minute timestep at this point
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep=timestep, timeshift=timeshift)

        if data is None:
            return None

        df = pd.DataFrame(data).assign(datetime=lambda df: pd.to_datetime(df['datetime'])).set_index('datetime')
        if "dividend" in df.columns:
            agg_column_map["dividend"] = "sum"
        df_result = df.resample(f"{quantity}{unit}").agg(agg_column_map)

        # Drop any rows that have NaN values (this can happen if the data is not complete, eg. weekends)
        df_result = df_result.dropna()

        # Remove partial day data from the current day, which can happen if the data is in minute timestep.
        if timestep == "day" and self.timestep in {"minute", "hour"}:
            df_result = df_result[df_result.index < dt.replace(hour=0, minute=0, second=0, microsecond=0)]

        # The original df_result may include more rows when timestep is day and self.timestep is minute.
        # In this case, we only want to return the last n rows.
        df_result = df_result.tail(n=int(num_periods))

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

        quantity, timestep = parse_timestep_qty_and_unit(timestep)

        if timestep == "minute" and self.timestep in {"day", "hour"}:
            raise ValueError(
                "You are requesting minute data from a higher-timeframe data source. This is not supported."
            )

        if timestep == "hour" and self.timestep == "day":
            raise ValueError("You are requesting hour data from a daily data source. This is not supported.")

        if timestep not in {"minute", "hour", "day"}:
            raise ValueError(f"Only minute, hour, and day are supported for timestep. You provided: {timestep}")

        data = self._get_bars_between_dates_dict(timestep=timestep, start_date=start_date, end_date=end_date)
        if data is None:
            return None

        df = pd.DataFrame(data).set_index("datetime")
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
        df_result = df.resample(f"{int(quantity)}{unit_code}").agg(agg).dropna()
        return df_result
