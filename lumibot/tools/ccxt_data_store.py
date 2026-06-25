import time
import duckdb
import os
import uuid
import ccxt
from datetime import datetime, timezone, timedelta
from tabulate import tabulate
import pandas as pd
from pandas import DataFrame
from ..constants import LUMIBOT_CACHE_FOLDER
from lumibot.tools.lumibot_logger import get_logger
import math
from typing import Union

logger = get_logger(__name__)

_TIMEFRAME_SECONDS = {
    "1m": 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
}
_OHLCV_CACHE_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "missing"]


class CcxtCacheDB:
    """A ccxt data cache class using duckdb.
    The data being cached is OHLCV data and is stored in UTC.
    After importing the data, you'll need to change the timezone if necessary.
    Create an exchange_id folder in the cache folder, and create a symbol_timeframe.duckdb file under it.
    ex) Create a BTC_USDT_1m.duckdb file in the binance folder.
    If there is an existing cache file, it will use it to fetch the data, otherwise it will use ccxt to fetch the data.
    If a cache file exists, but the requested data range is not in the cache file, the data will be fetched using ccxt.
    For example, if the cache file contains data from 2023-01-01 to 2023-01-10, and you request data from 2023-01-05 to 2023-01-15,
    the data from 2023-01-05 to 2023-01-10 will be fetched from the cache file, and the data from 2023-01-11 to 2023-01-15 will be fetched using ccxt.
    The newly fetched data is stored in the cache file and the range of data stored in the cache file is updated to 2023-01-05 ~ 2023-01-15.
    The cache file uses two tables to store the data using duckdb.
    The candles table, which stores the OHLCV data, has the columns open, high, low, close, volume, and missing.
    The cache_dt_ranges table, which stores the ranges of the cached data, has the following columns: id, start_dt, end_dt.
    We use the missing column to fill in missing data in the time series data.
    The missing column is 1 for missing data and 0 for non-missing data.

    If max_download_limit is not set, both 1m and 1d will be set to 50000.
    Raise an error if 'end_datetime - start_datetime' is greater than max_download_limit.
    max_download_limit can be set in __init__.
    """

    def __init__(self, exchange_id:str,max_download_limit:int=None):
        """Initialize the CcxtCacheDB class.

        Args:
            exchange_id (str): "binance","coinbase","kraken" etc.
            max_download_limit (int, optional): Maximum number of data to be downloaded at once using CCXT. Defaults to None.

        """
        self.logger = get_logger(self.__class__.__name__)
        try:
            exchange_class = getattr(ccxt, exchange_id)
        except:
            raise Exception(
                "Could not find exchange named '{}'. Are you sure you are spelling the exchange_id correctly?".format(
                    exchange_id
                )
            )

        self.exchange_id = exchange_id
        self.api = exchange_class()
        last_error = None
        for attempt in range(3):
            try:
                self.api.load_markets()
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(1 + attempt)
        if last_error is not None:
            raise last_error
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True
        self.max_download_limit = 50000 if max_download_limit is None else max_download_limit

    @staticmethod
    def _to_utc_naive(value: datetime) -> datetime:
        """Normalize cache boundaries to UTC-naive datetimes.

        DuckDB stores CCXT candle timestamps as UTC-naive values. A timezone-aware
        strategy clock must be converted to UTC before dropping tzinfo; otherwise
        midnight in New York is incorrectly queried as midnight UTC.
        """
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            return value.replace(tzinfo=None)
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def _current_utc_naive(self) -> datetime:
        return datetime.utcnow().replace(tzinfo=None)

    def _normalize_download_window(self, start: datetime, end: datetime | None) -> tuple[datetime, datetime]:
        """Expand cache windows by UTC date without requesting future provider bars."""
        now_dt = self._current_utc_naive()
        if end is None:
            end = now_dt

        start_dt = self._to_utc_naive(start)
        end_dt = self._to_utc_naive(end)

        # Cache coverage is tracked by UTC date buckets, but same-day requests
        # must stop at the provider's current time. Coinbase rejects OHLCV calls
        # whose paginated ``since`` value drifts into the future.
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        if end_dt > now_dt:
            end_dt = now_dt

        return start_dt, end_dt


    def get_cache_file_name(self, symbol:str, timeframe:str)->str:
        """Returns the cache file name. If the cache folder does not exist, it is created.
        cache folder is created under LUMIBOT_CACHE_FOLDER with exchange_id and symbol_timeframe.duckdb file.
        e.g. BTC_USDT_1m.duckdb file is created under binance folder.

        Args:
            symbol (str): BTC/USDT, ETH/USDT etc.
            timeframe (str): 1m, 1d etc.

        Raises:
            Exception: OSError if the cache folder cannot be created.

        Returns:
            str: cache full file name
        """
        cache_folder = os.path.join(LUMIBOT_CACHE_FOLDER,self.exchange_id)
        try:
            if not os.path.exists(cache_folder):
                os.makedirs(cache_folder)
        except OSError:
            raise Exception("Could not create cache folder at {}".format(cache_folder))

        cache_file = os.path.join(cache_folder,
                                  f"{symbol.replace('/', '_')}_{timeframe}.duckdb")
        return cache_file


    def get_data_from_cache(self, symbol:str, timeframe:str,
                            start:datetime, end:datetime)->DataFrame:
        """Fetch data from cache. Raise an exception if the cache file does not exist.
        Fetch data in the range start and end from the cache file.

        Args:
            symbol (str): BTC/USDT, ETH/USDT etc.
            timeframe (str): 1m, 1d etc.
            start (datetime): datetime object, ex) datetime(2023, 3, 2), datetime(2023, 3, 2, 12, 1, 0, 0)
            end (datetime): datetime object, ex) datetime(2023, 3, 4), datetime(2023, 3, 4, 10, 14, 0, 0)

        Raises:
            Exception: cache file이 없으면 예외를 발생시킨다.

        Returns:
            DataFrame: Data fetched from cache.
                       Use datetime as the index.
                       datetime, open, high, low, close, volume, missing columns.
        """

        cache_file = self.get_cache_file_name(symbol, timeframe)
        if not os.path.exists(cache_file):
            raise Exception(f"Cache file {cache_file} does not exist")

        start = self._to_utc_naive(start)
        end = self._to_utc_naive(end)

        with duckdb.connect(database = cache_file) as con:
            df = con.execute("""select datetime, open, high, low, close, volume, missing
                             from candles
                             where datetime  between  ? and ?
                             order by datetime asc
                             """,(start,end)).fetch_df()

        df = self._filter_executable_rows(df)
        df.drop_duplicates(inplace=True,subset=['datetime'])
        df.set_index("datetime", inplace=True)
        return df


    # timeframes: 1m, 1h, 1d
    def download_ohlcv(self, symbol:str,timeframe:str,
                       start:datetime, end:datetime,  limit:int=None)->DataFrame:
        """Download data according to the given symbol, timeframe, start, end, and limit.
        Store the downloaded data in a cache.
        Data that is not in the cache is downloaded using CCXT.
        If a cache file exists, but the requested data range is not in the cache file, the data will be fetched using ccxt.
        For example, if the cache file contains data from 2023-01-01 to 2023-01-10, and you request data from 2023-01-05 to 2023-01-15,
        the data from 2023-01-05 to 2023-01-10 will be fetched from the cache file, and the data from 2023-01-11 to 2023-01-15 will be fetched using ccxt.
        The newly fetched data is stored in the cache file and the range of data stored in the cache file is updated to 2023-01-05 ~ 2023-01-15.

        Args:
            symbol (str):  BTC/USDT, ETH/USDT etc.
            timeframe (str): 1m, 1d etc.
            start (datetime): datetime object, ex) datetime(2023, 3, 2), datetime(2023, 3, 2, 12, 1, 0, 0)
            end (datetime): datetime object, ex) datetime(2023, 3, 4), datetime(2023, 3, 4, 10, 14, 0, 0)
            limit (int, optional): max download limit. Defaults to None.

        Raises:
            Exception: Raise an exception if the max download limit is exceeded.

        Returns:
            DataFrame: Data fetched from cache.
                       Use datetime as the index.
                       datetime, open, high, low, close, volume, missing columns.
        """
        if limit is None:
            limit = self.max_download_limit

        if timeframe not in _TIMEFRAME_SECONDS:
            raise ValueError(f"Unsupported CCXT timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAME_SECONDS)}")

        start_dt, end_dt = self._normalize_download_window(start, end)
        if start_dt > end_dt:
            self.logger.warning(
                "Requested CCXT range starts after the provider's current time; "
                "no historical bars are available yet for %s %s.",
                symbol,
                timeframe,
            )
            empty = self._empty_ohlcv_frame()
            empty.set_index("datetime", inplace=True)
            return empty

        download_ranges,overap_range_ids,cache_range = self._calc_download_ranges(symbol, timeframe,start_dt, end_dt)

        self.logger.info(f"download ranges :\n{self._table_str(download_ranges,headers=['from','to'])}")

        downloaded_coverage_ranges = []

        for download_start,download_end in download_ranges:
            range_cnt = self._count_timeframe_units(download_start, download_end, timeframe)

            if range_cnt > limit:
                raise Exception(f"Request download range {range_cnt} is greater than download limit {limit}")

            df = self._get_barset_from_api(symbol, timeframe,
                                           range_cnt, download_start, download_end)
            df = self._fill_missing_data(df, timeframe)
            self._cache_ohlcv(symbol, df, timeframe)
            coverage_range = self._coverage_range_from_rows(df, download_start, download_end, timeframe)
            if coverage_range is not None:
                downloaded_coverage_ranges.append(coverage_range)

        cache_file = self.get_cache_file_name(symbol, timeframe)

        if downloaded_coverage_ranges:
            with duckdb.connect(cache_file) as con:
                coverage_ranges = downloaded_coverage_ranges
                if len(overap_range_ids) > 0:
                    old_ranges = []
                    for range_id in overap_range_ids:
                        old_ranges.extend(
                            con.execute(
                                """SELECT start_dt, end_dt FROM cache_dt_ranges WHERE id = ?""",
                                (range_id,),
                            ).fetchall()
                        )
                    coverage_ranges = old_ranges + coverage_ranges
                    params = [(id,) for id in overap_range_ids]
                    con.executemany("""DELETE FROM cache_dt_ranges WHERE id = ?""", params)

                for start_dt, end_dt in self._merge_coverage_ranges(coverage_ranges):
                    con.execute(
                        """INSERT INTO cache_dt_ranges VALUES (?, ?, ?)""",
                        (str(uuid.uuid4().hex), start_dt, end_dt),
                    )

        with duckdb.connect(cache_file) as con:
            df = con.execute("""select * from cache_dt_ranges""").fetch_df()
        self.logger.info(f"cache ranges:\n{self._table_str(df[['start_dt', 'end_dt']],headers=['from','to'])}")

        df_cache = self.get_data_from_cache(symbol, timeframe, start, end)
        return df_cache


    def _cache_ohlcv(self, symbol:str, df:DataFrame, timeframe:str)->None:
        """ccxt에서 가져온 데이터를 cache에 저장한다.

        Args:
            symbol (str): BCH/USDT, ETH/USDT etc.
            df (DataFrame): DataFrame to store in cache(datetime, open, high, low, close, volume, missing columns)
            timeframe (str): 1m, 1d etc.
        """
        cache_file = self.get_cache_file_name(symbol, timeframe)

        with duckdb.connect(cache_file) as con:
            con.execute("""CREATE TABLE IF NOT EXISTS candles (
                            datetime DATETIME,
                            open DECIMAL, high DECIMAL, low DECIMAL, close DECIMAL, volume DECIMAL, missing INTEGER)""")

            # cache ranges table
            con.execute("""CREATE TABLE  IF NOT EXISTS cache_dt_ranges (
                            id STRING ,
                            start_dt DATETIME ,
                            end_dt DATETIME)""")
            # insert df to cache db
            if df is not None and not df.empty:
                con.execute("""INSERT INTO candles SELECT *  from df""")

    def _coverage_range_from_rows(
        self,
        df: DataFrame,
        requested_start: datetime,
        requested_end: datetime,
        timeframe: str,
    ) -> tuple[datetime, datetime] | None:
        """Return the actual cache coverage represented by provider bars.

        Cache metadata must describe real executable rows, not the requested
        window. Coinbase can return a partial page without raising; marking the
        entire requested window as cached would make later reads trust missing
        bars and fail with stale data.
        """
        df = self._filter_executable_rows(df)
        if df.empty:
            return None

        first_bar = pd.Timestamp(df["datetime"].min()).to_pydatetime().replace(tzinfo=None)
        last_bar = pd.Timestamp(df["datetime"].max()).to_pydatetime().replace(tzinfo=None)
        bar_end = last_bar + timedelta(seconds=_TIMEFRAME_SECONDS[timeframe]) - timedelta(microseconds=1)

        coverage_start = max(first_bar, requested_start)
        coverage_end = min(bar_end, requested_end)
        if coverage_end < coverage_start:
            return None

        return coverage_start, coverage_end

    def _merge_coverage_ranges(
        self,
        ranges: list[tuple[datetime, datetime]],
    ) -> list[tuple[datetime, datetime]]:
        normalized = sorted((start, end) for start, end in ranges if start <= end)
        if not normalized:
            return []

        merged = [normalized[0]]
        for start, end in normalized[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end + timedelta(microseconds=1):
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged


    def _calc_download_ranges(self,symbol:str,timeframe:str,
                             start:datetime,
                             end:datetime)->tuple[list[tuple[datetime, datetime]],list[str]]:
        """Checks for duplicates between the data stored in the cache and the requested data,
        and returns a range of non-duplicate download data ranges, overap ranges ids, new cache range.
        For example, suppose you have the following data ranges stored in cache
        ----------------------------------
        | id |   start_dt   |   end_dt   |
        ----------------------------------
        | id1 | 2023-01-01 | 2023-01-10  |
        | id2 | 2023-02-03 | 2023-03-11  |
        | id3 | 2023-05-01 | 2023-06-07  |
        ----------------------------------

        If the requested data is 2023-01-05 to 2023-03-07, return 2023-01-11 to 2023-02-02 as the new download range,
        overlapping range ids returns [id1,id2].
        And the new cache_range returns (2023-01-01,2023-03-11).

        Args:
            symbol (str): BTC/USDT, ETH/USDT etc.
            timeframe (str): 1m, 1d etc.
            start (datetime): datetime object,
                  ex) datetime(2023, 1, 5), datetime(2023, 1, 5, 12, 1, 0, 0)
            end (datetime): datetime object,
                  ex) datetime(2023, 3, 7), datetime(2023, 3, 7, 10, 14, 0, 0)

        Returns:
            tuple[list[tuple[datetime, datetime]],list[str]]: (new download ranges,overap range ids,new cache range)
        """
        cache_file = self.get_cache_file_name(symbol, timeframe)
        if not os.path.exists(cache_file):
            return [(start, end)],[],(start, end)
        if os.path.exists(cache_file):
            # get cache data ranges (id, start_dt, end_dt)
            with duckdb.connect(cache_file) as con:
                df = con.execute("""select * from cache_dt_ranges""").fetch_df()
        if len(df) > 0:
            return self._find_non_overlapping_range(df,start, end)
        else:
            return [(start, end)],[],(start, end)


    def _find_non_overlapping_range(self,df:DataFrame,
                                    new_start:datetime, new_end:datetime)->list[list,list,tuple]:
        """Checks for duplicates between the data stored in the cache and the requested data,
        and returns a range of non-duplicate download data ranges, overap ranges ids, new cache range.

        Args:
            df (DataFrame): data ranges stored in cache (id, start_dt, end_dt)
            new_start (datetime): Data request start date time,ex) datetime(2023, 1, 5), datetime(2023, 1, 5, 12, 1, 0, 0)
            new_end (datetime): Data request end date time, ex) datetime(2023, 3, 7), datetime(2023, 3, 7, 10, 14, 0, 0)

        Returns:
            list[list,list,tuple]: (new download ranges,overap range ids,new cache range)
        """

        # find overlapping ranges
        def find_overlapping_range(row):
            id=row['id']
            e=row['end_dt']
            s=row['start_dt']
            if s <= new_end and new_start <= e:
                return  id, s, e
            else:
                return pd.NaT,pd.NaT,pd.NaT

        overap = df.apply(find_overlapping_range, axis=1)
        df['id'], df['start_dt'],df['end_dt'] = zip(*overap)
        df.dropna(inplace=True)

        if len(df) == 0:
            return [(new_start,new_end)],[], (new_start,new_end)

        df.sort_values(by='start_dt', inplace=True)
        df.reset_index(drop=True, inplace=True)
        dt_start,dt_end = df.start_dt.min(),df.end_dt.max()

        ranges = []

        start = new_start
        end = new_end

        if new_start < dt_start:
            ranges.append((new_start,df.start_dt.iloc[0]))
        else:
            start = dt_start

        if len(df) > 1:
            for i in  range(0,len(df)-1):
                ranges.append((df.end_dt.iloc[i], df.start_dt.iloc[i+1]))

        if new_end > dt_end:
            ranges.append((df.end_dt.iloc[-1],new_end))
        else:
            end = dt_end

        return ranges, df.id.tolist(), (start,end)


    def _get_barset_from_api(self, symbol:str, timeframe:str,
                             limit:int=None, start:datetime=None, end:datetime=None)->Union[DataFrame ,None]:
        """Use CCXT to get historical candle data for a given cryptocurrency symbol and time parameters.
        Outputs a dataframe open, high, low, close columns and a native timezone index.

        Args:
            symbol (str): BTC/USDT, ETH/USDT etc.
            timeframe (str): 1m, 1d etc.
            limit (int, optional): max download limit. Defaults to None.
            start (datetime, optional): When to start downloading data. Defaults to None.
            end (datetime, optional): When the data download ends. Defaults to None.

        Raises:
            Exception: Raise an exception if there is no start or end date.
        Returns:
            DataFrame: candle data (datetime, open, high, low, close, volume)
        """

        if not self.api.has["fetchOHLCV"]:
            logger.error("Exchange does not support fetching OHLCV data")

        market = self.api.markets.get(symbol, None)
        if market is None:
            logger.error(
                f"A request for market data for {symbol} was submitted. " f"The market for that pair does not exist"
            )
            return self._empty_ohlcv_frame()

        if end is None or start is None:
            raise Exception("Start and end must be specified")

        if limit is None:
            limit = self.max_download_limit

        if timeframe not in _TIMEFRAME_SECONDS:
            raise ValueError(f"Unsupported CCXT timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAME_SECONDS)}")

        endunix = self.api.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))
        timeframe_ms = _TIMEFRAME_SECONDS[timeframe] * 1000

        df_ret = None
        curr_start = self.api.parse8601(start.strftime("%Y-%m-%d %H:%M:%S"))
        curr_start = curr_start if curr_start > 0 else 0

        cnt = 0

        loop_limit = 300
        rate_limit = 10  # Requests per second in burst.
        page_span_ms = loop_limit * timeframe_ms
        max_pages = max(1, math.ceil(max(endunix - curr_start, 0) / page_span_ms) + 5)

        while curr_start <= endunix:
            cnt += 1
            candles = self.api.fetch_ohlcv(symbol, timeframe,
                                           since=curr_start, limit=loop_limit, params={})

            df = pd.DataFrame(candles, columns=["datetime",
                                                "open", "high", "low", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
            df = df.set_index("datetime")

            next_start = curr_start + page_span_ms

            if len(df) > 0:
                last_curr_end = self.api.parse8601(df.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
                next_start = max(last_curr_end + timeframe_ms, curr_start + timeframe_ms)
                if df_ret is None:
                    df_ret = df
                else:
                    df_ret = pd.concat([df_ret, df])

                df_ret = df_ret.sort_index()
            else:
                last_curr_end = None

            if df_ret is not None and len(df_ret) >= limit and last_curr_end is not None and last_curr_end >= endunix:
                break
            if last_curr_end is not None and last_curr_end >= endunix:
                break
            if next_start <= curr_start:
                break
            curr_start = next_start

            # Sleep for half a second every rate_limit requests to prevent rate limiting issues
            if cnt % rate_limit == 0:
                time.sleep(1)

            # Catch if endless loop.
            if cnt > max_pages:
                raise RuntimeError(
                    f"CCXT OHLCV pagination did not reach requested end for {symbol} {timeframe}: "
                    f"last_since={pd.to_datetime(curr_start, unit='ms')} end={end}"
                )


        if df_ret is None:
            return self._empty_ohlcv_frame()

        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        df_ret = df_ret[(df_ret.index >= start_ts) & (df_ret.index <= end_ts)]
        df_ret.drop_duplicates(inplace=True)
        df_ret.reset_index(inplace=True)

        return df_ret


    def _fill_missing_data(self, df:DataFrame, freq:str)->DataFrame:
        """Normalize provider candles without fabricating missing executable bars.

        Args:
            df (DataFrame): candle data (datetime, open, high, low, close, volume)
            freq (str): 1m, 1d etc.

        Returns:
            DataFrame: candle data (datetime, open, high, low, close, volume, missing)
        """
        return self._filter_executable_rows(df)

    def _empty_ohlcv_frame(self) -> DataFrame:
        return pd.DataFrame(
            {
                "datetime": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "missing": pd.Series(dtype="int64"),
            }
        )

    def _count_timeframe_units(self, start: datetime, end: datetime, timeframe: str) -> int:
        seconds = _TIMEFRAME_SECONDS.get(timeframe)
        if seconds is None:
            raise ValueError(f"Unsupported CCXT timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAME_SECONDS)}")
        return max(0, math.ceil((end - start).total_seconds() / seconds))

    def _filter_executable_rows(self, df:Union[DataFrame, None])->DataFrame:
        """Return only real provider bars that are safe to expose to strategies."""
        if df is None or df.empty:
            return self._empty_ohlcv_frame()

        df = df.copy()
        if "datetime" not in df.columns:
            df.reset_index(inplace=True)
            if "datetime" not in df.columns and "index" in df.columns:
                df.rename(columns={"index": "datetime"}, inplace=True)

        if "datetime" not in df.columns:
            return self._empty_ohlcv_frame()

        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df.dropna(subset=["datetime"], inplace=True)

        if "missing" in df.columns:
            try:
                missing_values = df["missing"].fillna(0).astype(int)
                df = df[missing_values == 0]
            except Exception:
                df = df[df["missing"].isna() | df["missing"].isin([0, False, "0", "False", "false"])]

        for col in ("open", "high", "low", "close"):
            if col not in df.columns:
                return self._empty_ohlcv_frame()
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if "volume" not in df.columns:
            df["volume"] = 0
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        df.dropna(subset=["open", "high", "low", "close"], inplace=True)
        if df.empty:
            return self._empty_ohlcv_frame()

        df["missing"] = 0
        df = df[_OHLCV_CACHE_COLUMNS]
        df.drop_duplicates(inplace=True, subset=["datetime"])
        df.sort_values("datetime", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


    def _table_str(self, df, headers="keys"):
        return tabulate(df, headers=headers, tablefmt='psql')

if __name__ == "__main__":
    exchange_id = "binance"
    symbol = "SOL/USDT"
    timeframe = "1m"

    cache = CcxtCacheDB(exchange_id)

    cache_file_path = cache.get_cache_file_name(symbol,timeframe)
     # Remove cache file if exists.
    if os.path.exists(cache_file_path):
        os.remove(cache_file_path)

    # no overap new download range
    start = datetime(2023, 3, 1)
    end = datetime(2023, 3, 11)
    df = cache.download_ohlcv(symbol,timeframe,start,end)
    print(f"data length: {len(df)}")

    # no overap new download range
    start = datetime(2023, 3, 13)
    end = datetime(2023, 3, 15)
    df = cache.download_ohlcv(symbol,timeframe,start,end)
    print(f"data length: {len(df)}")

    # fully overap download range, no download
    start = datetime(2023, 3, 13)
    end = datetime(2023, 3, 14)
    df = cache.download_ohlcv(symbol,timeframe,start,end)
    print(f"data length: {len(df)}")

    # no overap new download range
    start = datetime(2023, 4, 5)
    end = datetime(2023, 4, 7)
    df = cache.download_ohlcv(symbol,timeframe,start,end)
    print(f"data length: {len(df)}")

    # Partially nested download ranges
    # cache ranges updated
    start = datetime(2023, 3, 9)
    end = datetime(2023, 3, 17)
    df = cache.download_ohlcv(symbol,timeframe,start,end)
    print(f"data length: {len(df)}")

    df = cache.get_data_from_cache("SOL/USDT","1m",datetime(2000,1,1),datetime(2025,1,1))
    print(f"total data length: {len(df)}")
