import logging
import time
import duckdb
import os
import uuid
import ccxt
from datetime import datetime
from tabulate import tabulate
import pandas as pd
from pandas import DataFrame
from lumibot import LUMIBOT_CACHE_FOLDER
import math
import numpy as np
from typing import Union


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

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
        self.logger = logging.getLogger(self.__class__.__name__)
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
        self.api.load_markets()
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True
        self.max_download_limit = 50000 if max_download_limit is None else max_download_limit


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

        start = start.replace(tzinfo=None)
        end = end.replace(tzinfo=None)

        with duckdb.connect(database = cache_file) as con:
            df = con.execute("""select datetime, open, high, low, close, volume, missing
                             from candles
                             where datetime  between  ? and ?
                             order by datetime asc
                             """,(start,end)).fetch_df()
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
        if end is None:
            end = datetime.utcnow()

        if limit is None:
            limit = self.max_download_limit

        start_dt = start.replace(tzinfo=None)
        end_dt = end.replace(tzinfo=None)

        # set start_dt to 00:00:00 and end_dt to 23:59:59
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

        download_ranges,overap_range_ids,cache_range = self._calc_download_ranges(symbol, timeframe,start_dt, end_dt)

        self.logger.info(f"download ranges :\n{self._table_str(download_ranges,headers=['from','to'])}")

        for download_start,download_end in download_ranges:
            range_cnt = download_end - download_start
            if timeframe == "1m":
                range_cnt = math.ceil(range_cnt.total_seconds() / 60)
            elif timeframe == "1d":
                range_cnt = range_cnt.days

            if range_cnt > limit:
                raise Exception(f"Request download range {range_cnt} is greater than download limit {limit}")

            df = self._get_barset_from_api(symbol, timeframe,
                                           range_cnt, download_start, download_end)
            df = self._fill_missing_data(df, timeframe)
            self._cache_ohlcv(symbol, df, timeframe)

        cache_file = self.get_cache_file_name(symbol, timeframe)

        if download_ranges:
            if len(overap_range_ids) > 0:
                start_dt = cache_range[0]
                end_dt = cache_range[1]
            else:
                start_dt = df.datetime.min()
                end_dt = df.datetime.max()

            with duckdb.connect(cache_file) as con:
                # insert new cache data range
                con.execute("""INSERT INTO cache_dt_ranges VALUES (?, ?, ?)""",
                            (str(uuid.uuid4().hex),start_dt,end_dt))
                # delete overlapping ranges
                if len(overap_range_ids) > 0:
                    params = [(id,) for id in overap_range_ids]
                    con.executemany("""DELETE FROM  cache_dt_ranges WHERE id = ?""", params)

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
                            open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INTEGER, missing INTEGER)""")

            # cache ranges table
            con.execute("""CREATE TABLE  IF NOT EXISTS cache_dt_ranges (
                            id STRING ,
                            start_dt DATETIME ,
                            end_dt DATETIME)""")
            # insert df to cache db
            con.execute("""INSERT INTO candles SELECT *  from df""")


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
            if (s < new_start and new_start < e) or (new_start < s and e < new_end) or (s < new_end and new_end < e):
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
            logging.error("Exchange does not support fetching OHLCV data")

        market = self.api.markets.get(symbol, None)
        if market is None:
            logging.error(
                f"A request for market data for {symbol} was submitted. " f"The market for that pair does not exist"
            )
            return None

        if end is None or start is None:
            raise Exception("Start and end must be specified")

        if limit is None:
            limit = self.max_download_limit

        endunix = self.api.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))

        df_ret = None
        curr_start = self.api.parse8601(start.strftime("%Y-%m-%d %H:%M:%S"))
        curr_start = curr_start if curr_start > 0 else 0

        cnt = 0
        last_curr_end = None

        loop_limit = 300
        rate_limit = 10  # Requests per second in burst.

        while True:
            cnt += 1
            candles = self.api.fetch_ohlcv(symbol, timeframe,
                                           since=curr_start, limit=loop_limit, params={})

            df = pd.DataFrame(candles, columns=["datetime",
                                                "open", "high", "low", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
            df = df.set_index("datetime")
            if df_ret is None:
                df_ret = df
            else:
                df_ret = pd.concat([df_ret, df])

            df_ret = df_ret.sort_index()

            if len(df) > 0:
                last_curr_end = self.api.parse8601(df.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            else:
                last_curr_end = None

            if len(df_ret) >= limit:
                break
            elif last_curr_end is None:
                break
            elif last_curr_end > endunix:
                break

            if curr_start == last_curr_end:
                break
            else:
                curr_start = last_curr_end

            # Sleep for half a second every rate_limit requests to prevent rate limiting issues
            if cnt % rate_limit == 0:
                time.sleep(1)

            # Catch if endless loop.
            if cnt > 500:
                break


        df_ret.drop_duplicates(inplace=True)
        df_ret.reset_index(inplace=True)

        return df_ret


    def _fill_missing_data(self, df:DataFrame, freq:str)->DataFrame:
        """If datetime is missing from the candle data, fill in the missing data.
        Missing data is marked with a 1 in the missing column and 0 for the rest.

        Args:
            df (DataFrame): candle data (datetime, open, high, low, close, volume)
            freq (str): 1m, 1d etc.

        Returns:
            DataFrame: candle data (datetime, open, high, low, close, volume, missing)
        """
        df.set_index("datetime", inplace=True)
        if freq == "1d":
            dt_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="D")
        else:
            dt_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="T")

        df_complete = df.reindex(dt_range).fillna(method='ffill')
        df_complete['missing'] = np.where(df_complete.index.isin(df.index), 0, 1)
        #  Change "datetime" to come to the front
        #  When inserting DB, make sure that the datetime comes to the first colmun.
        df_complete.insert(0,"datetime", df_complete.index)
        df_complete.reset_index(drop=True, inplace=True)
        return df_complete


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