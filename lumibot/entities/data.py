import datetime
import logging
import re
from typing import Union, Optional, Dict, Any, List

import pandas as pd
from lumibot import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
from lumibot.tools.helpers import parse_timestep_qty_and_unit, to_datetime_aware

from .asset import Asset
from .dataline import Dataline


class Data:
    """
    A container for a single asset's time-series data (OHLCV, etc.) used in LumiBot backtesting.

    This class wraps a Pandas DataFrame and ensures consistent formatting, indexing,
    time-zone alignment, plus iteration and slicing used by LumiBot's backtest engine.

    Parameters
    ----------
    asset : Asset
        The asset (symbol + type) that this data represents.
    df : pd.DataFrame
        A DataFrame of OHLCV or related columns. Must have a DatetimeIndex
        or a recognized date/time column that can be set as index.
        Required columns: ["open", "high", "low", "close", "volume"] (case-insensitive).
    date_start : datetime, optional
        The earliest datetime we want to keep in df. If None, uses the min index in df.
    date_end : datetime, optional
        The latest datetime we want to keep in df. If None, uses the max index in df.
    trading_hours_start : datetime.time, optional
        The earliest time in a day we will keep in minute data. Default 00:00 for "minute" data.
        For "day" data, this is overridden to 00:00 internally.
    trading_hours_end : datetime.time, optional
        The latest time in a day we will keep in minute data. Default 23:59 for "minute" data.
        For "day" data, this is overridden to 23:59:59.999999 internally.
    timestep : str
        Either "minute" or "day".
    quote : Asset, optional
        If the asset is crypto or forex, specify the quote asset. E.g. for BTC/USD, quote=USD.
    timezone : str, optional
        E.g. "US/Eastern". If not None, we localize or convert to that timezone as needed.

    Attributes
    ----------
    asset : Asset
        The asset this data belongs to.
    symbol : str
        The same as asset.symbol.
    df : pd.DataFrame
        The underlying time-series data with columns: open, high, low, close, volume
        and a DatetimeIndex with tz=UTC.
    date_start : datetime
    date_end : datetime
    trading_hours_start : datetime.time
    trading_hours_end : datetime.time
    timestep : str
        "minute" or "day".
    datalines : Dict[str, Dataline]
        A dictionary of columns -> Dataline objects for faster iteration.
    iter_index : pd.Series
        A mapping from the df's index to a consecutive range, used for fast lookups.

    Methods
    -------
    repair_times_and_fill(idx: pd.DatetimeIndex) -> None
        Reindex the df to a given index, forward-fill, etc., then update datalines/iter_index.
    get_last_price(dt: datetime, length=1, timeshift=0) -> float
        Return the last known price at dt. If dt is between open/close of bar, returns open vs close.
    get_bars(dt: datetime, length=1, timestep="minute", timeshift=0) -> pd.DataFrame
        Return the last 'length' bars up to dt, optionally aggregated to day if needed.
    get_bars_between_dates(timestep="minute", start_date=None, end_date=None) -> pd.DataFrame
        Return bars for a date range.
    """

    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING: List[Dict[str, Any]] = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(
        self,
        asset: Asset,
        df: pd.DataFrame,
        date_start: Optional[datetime.datetime] = None,
        date_end: Optional[datetime.datetime] = None,
        trading_hours_start: datetime.time = datetime.time(0, 0),
        trading_hours_end: datetime.time = datetime.time(23, 59),
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        timezone: Optional[str] = None,
    ):
        self.asset = asset
        self.symbol = self.asset.symbol

        # Crypto must have a quote asset
        if self.asset.asset_type == "crypto" and quote is None:
            raise ValueError(
                f"Missing quote asset for crypto {self.symbol}. For BTC/USD, quote=Asset('USD','forex')."
            )
        else:
            self.quote = quote

        if self.quote is not None and not isinstance(self.quote, Asset):
            raise ValueError(f"quote must be an Asset object, got {type(self.quote)}")

        if timestep not in ["minute", "day"]:
            raise ValueError(f"timestep must be 'minute' or 'day', got {timestep}")

        self.timestep = timestep

        self.df = self.columns(df)

        # If index isn't datetime, try a known column
        if not str(self.df.index.dtype).startswith("datetime"):
            date_cols = [
                "Date", "date", "Time", "time", "Datetime", "datetime",
                "timestamp", "Timestamp",
            ]
            for date_col in date_cols:
                if date_col in self.df.columns:
                    self.df[date_col] = pd.to_datetime(self.df[date_col])
                    self.df = self.df.set_index(date_col)
                    break

        if timezone:
            self.df.index = self.df.index.tz_localize(timezone)

        self.df = self.set_date_format(self.df)
        self.df = self.df.sort_index()

        # Force times if day-based data
        self.trading_hours_start, self.trading_hours_end = self.set_times(
            trading_hours_start, trading_hours_end
        )
        self.date_start, self.date_end = self.set_dates(date_start, date_end)

        self.df = self.trim_data(
            self.df,
            self.date_start,
            self.date_end,
            self.trading_hours_start,
            self.trading_hours_end
        )
        self.datetime_start = self.df.index[0]
        self.datetime_end = self.df.index[-1]

    def set_times(
        self,
        trading_hours_start: datetime.time,
        trading_hours_end: datetime.time
    ) -> (datetime.time, datetime.time):
        """
        Adjust the trading hours for day-based data. If day, set them to full day range.
        If minute, allow user-supplied hours.

        Returns
        -------
        (trading_hours_start, trading_hours_end)
        """
        if self.timestep == "minute":
            return trading_hours_start, trading_hours_end
        else:
            # day timeframe
            return datetime.time(0, 0), datetime.time(23, 59, 59, 999999)

    def columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert recognized columns (open, high, low, close, volume) to lowercase,
        leaving other columns alone.

        Returns
        -------
        pd.DataFrame
        """
        df.columns = [
            col.lower() if col.lower() in ["open", "high", "low", "close", "volume"] else col
            for col in df.columns
        ]
        return df

    def set_date_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure the index is named 'datetime', is typed as a DatetimeIndex, and is localized or converted to UTC.

        Returns
        -------
        pd.DataFrame
        """
        df.index.name = "datetime"
        df.index = pd.to_datetime(df.index)
        if not df.index.tzinfo:
            df.index = df.index.tz_localize(DEFAULT_PYTZ)
        elif df.index.tzinfo != DEFAULT_PYTZ:
            df.index = df.index.tz_convert(DEFAULT_PYTZ)
        return df

    def set_dates(
        self,
        date_start: Optional[datetime.datetime],
        date_end: Optional[datetime.datetime]
    ) -> (datetime.datetime, datetime.datetime):
        """
        Resolve the date_start, date_end range. If None, use df.index min/max.

        Returns
        -------
        (date_start, date_end)
        """
        for dt in [date_start, date_end]:
            if dt and not isinstance(dt, datetime.datetime):
                raise TypeError(f"date_start/date_end must be datetime. Got {dt}.")

        if not date_start:
            date_start = self.df.index.min()
        if not date_end:
            date_end = self.df.index.max()

        date_start = to_datetime_aware(date_start)
        date_end = to_datetime_aware(date_end)

        # For day-based data, set to 0:00 and 23:59:59
        date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        return date_start, date_end

    def trim_data(
        self,
        df: pd.DataFrame,
        date_start: datetime.datetime,
        date_end: datetime.datetime,
        trading_hours_start: datetime.time,
        trading_hours_end: datetime.time
    ) -> pd.DataFrame:
        """
        Clip df to [date_start, date_end], and if minute-based, also clip to the trading_hours.

        Raises
        ------
        ValueError
            If the resulting df is empty.

        Returns
        -------
        pd.DataFrame
        """
        df = df.loc[(df.index >= date_start) & (df.index <= date_end), :]
        if self.timestep == "minute":
            df = df.between_time(trading_hours_start, trading_hours_end)
        if df.empty:
            raise ValueError(
                f"No data remains for {self.asset} after trimming to date range "
                f"{date_start} - {date_end} and hours {trading_hours_start}-{trading_hours_end}."
            )
        return df

    def repair_times_and_fill(self, idx: pd.DatetimeIndex) -> None:
        """
        Reindex df to match idx, forward-fill, set volume=0 where missing, etc.
        Then re-create datalines for iteration.

        Parameters
        ----------
        idx : pd.DatetimeIndex
            A global index that might include more timestamps than we originally had.
        """
        idx = idx[(idx >= self.datetime_start) & (idx <= self.datetime_end)]
        df = self.df.reindex(idx, method="ffill")

        # Fill volume=0 if missing
        df.loc[df["volume"].isna(), "volume"] = 0

        # forward fill close, then set open/high/low if missing to the close
        df.loc[:, ~df.columns.isin(["open", "high", "low"])] = (
            df.loc[:, ~df.columns.isin(["open", "high", "low"])].ffill()
        )
        for col in ["open", "high", "low"]:
            df.loc[df[col].isna(), col] = df.loc[df[col].isna(), "close"]

        self.df = df

        iter_index = pd.Series(df.index)
        self.iter_index = pd.Series(iter_index.index, index=iter_index)
        self.iter_index_dict = self.iter_index.to_dict()

        self.datalines = {}
        self.to_datalines()

    def to_datalines(self) -> None:
        """
        Convert each df column into a Dataline object for performance in backtesting loops.
        """
        self.datalines.update({
            "datetime": Dataline(
                self.asset, "datetime", self.df.index.to_numpy(), self.df.index.dtype
            )
        })
        setattr(self, "datetime", self.datalines["datetime"].dataline)

        for column in self.df.columns:
            self.datalines[column] = Dataline(
                self.asset,
                column,
                self.df[column].to_numpy(),
                self.df[column].dtype
            )
            setattr(self, column, self.datalines[column].dataline)

    def get_iter_count(self, dt: datetime.datetime) -> int:
        """
        Return the integer index location for dt, or the last known date if dt not exact.

        Parameters
        ----------
        dt : datetime.datetime

        Returns
        -------
        int
            The integer location of dt in self.iter_index_dict.
        """
        if not hasattr(self, "iter_index_dict") or self.iter_index_dict is None:
            self.repair_times_and_fill(self.df.index)

        if dt in self.iter_index_dict:
            return self.iter_index_dict[dt]
        else:
            return self.iter_index.asof(dt)

    def check_data(func):
        """
        Decorator for data-checking around get_last_price, get_bars, etc.
        Ensures dt is within range and enough data is available for length/timeshift.
        """

        def checker(self: "Data", *args, **kwargs):
            dt = args[0]
            if dt < self.datetime_start:
                raise ValueError(
                    f"Requested dt {dt} is before data start {self.datetime_start} for {self.asset}"
                )

            if not hasattr(self, "iter_index_dict") or self.iter_index_dict is None:
                self.repair_times_and_fill(self.df.index)

            if dt in self.iter_index_dict:
                i = self.iter_index_dict[dt]
            else:
                i = self.iter_index.asof(dt)

            length = kwargs.get("length", 1)
            timeshift = kwargs.get("timeshift", 0)
            if not isinstance(length, (int, float)):
                raise TypeError(f"length must be int, got {type(length)}")

            data_index = i + 1 - length - timeshift
            if data_index < 0:
                logging.warning(
                    f"Requested dt {dt} for {self.asset} is out of range after length={length}, timeshift={timeshift}."
                )

            return func(self, *args, **kwargs)

        return checker

    @check_data
    def get_last_price(self, dt: datetime.datetime, length: int = 1, timeshift: int = 0) -> float:
        """
        Return the last known price at dt. If dt is after the bar's own index,
        we consider the close. If dt matches the bar's index exactly, consider open.

        Parameters
        ----------
        dt : datetime.datetime
        length : int
            How many bars back we want (mostly for the check_data process).
        timeshift : int
            Shifts the index lookup.

        Returns
        -------
        float
        """
        iter_count = self.get_iter_count(dt)
        open_price = self.datalines["open"].dataline[iter_count]
        close_price = self.datalines["close"].dataline[iter_count]
        # If dt > the bar's index, we consider it "after the bar closed"
        price = close_price if dt > self.datalines["datetime"].dataline[iter_count] else open_price
        return float(price)

    @check_data
    def get_quote(
        self, dt: datetime.datetime, length: int = 1, timeshift: int = 0
    ) -> dict:
        """
        Return a dict with open, high, low, close, volume, bid/ask info, etc.

        Parameters
        ----------
        dt : datetime.datetime
        length : int
        timeshift : int

        Returns
        -------
        dict
        """
        i = self.get_iter_count(dt)
        def r(col: str, decimals=2):
            return round(self.datalines[col].dataline[i], decimals) if col in self.datalines else None

        return {
            "open": r("open", 2),
            "high": r("high", 2),
            "low": r("low", 2),
            "close": r("close", 2),
            "volume": r("volume", 0),
            "bid": r("bid", 2),
            "ask": r("ask", 2),
            "bid_size": r("bid_size", 0),
            "bid_condition": r("bid_condition", 0),
            "bid_exchange": r("bid_exchange", 0),
            "ask_size": r("ask_size", 0),
            "ask_condition": r("ask_condition", 0),
            "ask_exchange": r("ask_exchange", 0),
        }

    @check_data
    def _get_bars_dict(
        self,
        dt: datetime.datetime,
        length: int = 1,
        timestep: Optional[str] = None,
        timeshift: int = 0
    ) -> dict:
        """
        Return a dict of numpy arrays for each column from [start_row:end_row].

        Parameters
        ----------
        dt : datetime.datetime
        length : int
        timestep : str, unused here
        timeshift : int

        Returns
        -------
        dict
            e.g. {"datetime": [...], "open": [...], ...}
        """
        end_row = self.get_iter_count(dt) - timeshift
        start_row = end_row - length
        if start_row < 0:
            start_row = 0

        start_row = int(start_row)
        end_row = int(end_row)

        bars_dict = {}
        for dl_name, dl in self.datalines.items():
            bars_dict[dl_name] = dl.dataline[start_row:end_row]
        return bars_dict

    def _get_bars_between_dates_dict(
        self,
        timestep: Optional[str] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None
    ) -> dict:
        """
        Return a dict of arrays for all bars between [start_date, end_date].

        Parameters
        ----------
        timestep : str, unused here
        start_date : datetime.datetime
        end_date : datetime.datetime

        Returns
        -------
        dict
        """
        end_row = self.get_iter_count(end_date)
        start_row = self.get_iter_count(start_date)
        if start_row < 0:
            start_row = 0

        start_row = int(start_row)
        end_row = int(end_row)

        d = {}
        for dl_name, dl in self.datalines.items():
            d[dl_name] = dl.dataline[start_row:end_row]
        return d

    @check_data
    def get_bars(
        self,
        dt: datetime.datetime,
        length: int = 1,
        timestep: str = MIN_TIMESTEP,
        timeshift: int = 0
    ) -> Union[pd.DataFrame, None]:
        """
        Return a pd.DataFrame of the last 'length' bars up to dt, aggregated if needed.

        Parameters
        ----------
        dt : datetime.datetime
        length : int
        timestep : str
            Either "minute" or "day". If local data is minute-based but we want "day", we resample.
        timeshift : int

        Returns
        -------
        pd.DataFrame or None
        """
        quantity, parsed_timestep = parse_timestep_qty_and_unit(timestep)
        num_periods = length

        if parsed_timestep == "minute" and self.timestep == "day":
            raise ValueError("Cannot request minute data from a day-only dataset.")
        if parsed_timestep not in ["minute", "day"]:
            raise ValueError(f"Only 'minute' or 'day' supported, got {parsed_timestep}.")

        agg_map = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        if parsed_timestep == "day" and self.timestep == "minute":
            # We have minute-level data but want daily bars
            length = length * 1440  # approximate: 1440 minutes in a day
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)
        elif parsed_timestep == "day" and self.timestep == "day":
            unit = "D"
            data = self._get_bars_dict(dt, length=length, timestep="day", timeshift=timeshift)
        else:
            # both are "minute"
            unit = "min"
            length = length * quantity
            data = self._get_bars_dict(dt, length=length, timestep="minute", timeshift=timeshift)

        if data is None:
            return None

        df = pd.DataFrame(data).assign(
            datetime=lambda df_: pd.to_datetime(df_["datetime"])
        ).set_index("datetime")

        if "dividend" in df.columns:
            agg_map["dividend"] = "sum"

        df_result = df.resample(f"{quantity}{unit}").agg(agg_map)
        df_result.dropna(inplace=True)

        # If minute-based source, remove partial day data for the last day
        if parsed_timestep == "day" and self.timestep == "minute":
            df_result = df_result[df_result.index < dt.replace(hour=0, minute=0, second=0, microsecond=0)]

        # Return only the last 'num_periods' rows
        df_result = df_result.tail(int(num_periods))
        return df_result

    def get_bars_between_dates(
        self,
        timestep: str = MIN_TIMESTEP,
        exchange: Optional[str] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None
    ) -> Union[pd.DataFrame, None]:
        """
        Return all bars in [start_date, end_date], resampled if needed.

        Parameters
        ----------
        timestep : str
            "minute" or "day"
        exchange : str, optional
            Not used here, but part of LumiBot's function signature.
        start_date : datetime
        end_date : datetime

        Returns
        -------
        pd.DataFrame or None
        """
        if timestep == "minute" and self.timestep == "day":
            raise ValueError("Cannot request minute bars from day-only dataset.")
        if timestep not in ["minute", "day"]:
            raise ValueError(f"Only 'minute' or 'day' supported, got {timestep}.")

        if timestep == "day" and self.timestep == "minute":
            d = self._get_bars_between_dates_dict(
                timestep=timestep, start_date=start_date, end_date=end_date
            )
            if d is None:
                return None
            df = pd.DataFrame(d).set_index("datetime")
            # Resample up to daily
            df_result = df.resample("D").agg(
                {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
            )
            return df_result

        else:
            d = self._get_bars_between_dates_dict(
                timestep=timestep, start_date=start_date, end_date=end_date
            )
            if d is None:
                return None
            df = pd.DataFrame(d).set_index("datetime")
            return df
