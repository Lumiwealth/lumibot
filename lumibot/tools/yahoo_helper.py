import logging
import os
import pickle
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ

from .helpers import get_lumibot_datetime

INFO_DATA = "info"


class _YahooData:
    def __init__(self, symbol, type, data):
        self.symbol = symbol
        self.type = type.lower()
        self.data = data
        self.file_name = f"{symbol}_{type.lower()}.pickle"

    def is_up_to_date(self, last_needed_datetime=None):
        if last_needed_datetime is None:
            last_needed_datetime = get_lumibot_datetime()

        if self.type == '1d':
            last_needed_date = last_needed_datetime.date()
            last_day = self.data.index[-1].to_pydatetime().date()

            # ip_up_to_date will always return False on holidays even though
            # the data is up-to-date because the market is still closed
            return last_day >= last_needed_date

        if self.type == INFO_DATA:
            if self.data.get("error"):
                return False

            last_needed_date = last_needed_datetime.date()
            last_day = self.data.get("last_update").date()

            return last_day >= last_needed_date

        return False


class YahooHelper:
    # =========Internal initialization parameters and methods============

    CACHING_ENABLED = False
    LUMIBOT_YAHOO_CACHE_FOLDER = os.path.join(LUMIBOT_CACHE_FOLDER, "yahoo")

    if not os.path.exists(LUMIBOT_YAHOO_CACHE_FOLDER):
        try:
            os.makedirs(LUMIBOT_YAHOO_CACHE_FOLDER)
            CACHING_ENABLED = True
        except Exception as e:
            pass
    else:
        CACHING_ENABLED = True

    # ====================Caching methods=================================

    @staticmethod
    def check_pickle_file(symbol, type):
        if YahooHelper.CACHING_ENABLED:
            file_name = f"{symbol}_{type.lower()}.pickle"
            pickle_file_path = os.path.join(YahooHelper.LUMIBOT_YAHOO_CACHE_FOLDER, file_name)
            if os.path.exists(pickle_file_path):
                try:
                    with open(pickle_file_path, "rb") as f:
                        return pickle.load(f)
                except Exception as e:
                    logging.error("Error while loading pickle file %s: %s" % (pickle_file_path, e))
                    # Remove the file because it is corrupted.  This will enable re-download.
                    os.remove(pickle_file_path)
                    return None

        return None

    @staticmethod
    def dump_pickle_file(symbol, type, data):
        if YahooHelper.CACHING_ENABLED:
            yahoo_data = _YahooData(symbol, type, data)
            file_name = "%s_%s.pickle" % (symbol, type.lower())
            pickle_file_path = os.path.join(YahooHelper.LUMIBOT_YAHOO_CACHE_FOLDER, file_name)
            with open(pickle_file_path, "wb") as f:
                pickle.dump(yahoo_data, f)

    # ====================Formatters methods===============================

    @staticmethod
    def format_df(df, auto_adjust):
        if auto_adjust:
            del df["Adj Ratio"]
            del df["Close"]
            del df["Open"]
            del df["High"]
            del df["Low"]
            df.rename(
                columns={
                    "Adj Close": "Close",
                    "Adj Open": "Open",
                    "Adj High": "High",
                    "Adj Low": "Low",
                },
                inplace=True,
            )
        else:
            for col in ["Adj Ratio", "Adj Open", "Adj High", "Adj Low"]:
                if col in df.columns:
                    del df[col]

        return df

    @staticmethod
    def process_df(df, asset_info=None):
        df = df.dropna().copy()

        # If the df is empty, return it
        if df.empty:
            return df

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(LUMIBOT_DEFAULT_PYTZ)
        else:
            df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)

        return df

    # ===================Data download method=============================

    @staticmethod
    def download_symbol_info(symbol):
        ticker = yf.Ticker(symbol)

        try:
            info = ticker.info
        except Exception as e:
            logging.debug(f"Error while downloading symbol info for {symbol}, setting info to None for now.")
            logging.debug(e)
            return {
                "ticker": symbol,
                "last_update": get_lumibot_datetime(),
                "error": True,
                "info": None,
            }

        return {
            "ticker": ticker.ticker,
            "last_update": get_lumibot_datetime(),
            "error": False,
            "info": info,
        }

    @staticmethod
    def get_symbol_info(symbol):
        ticker = yf.Ticker(symbol)
        return ticker.info

    @staticmethod
    def get_symbol_last_price(symbol):
        ticker = yf.Ticker(symbol)

        # Get the last price from the history
        df = ticker.history(period="7d", auto_adjust=False)
        if df.empty:
            return None

        return df["Close"].iloc[-1]

    @staticmethod
    def download_symbol_data(symbol, interval="1d"):
        ticker = yf.Ticker(symbol)
        try:
            if interval == "1m":
                # Yahoo only supports 1 minute interval for past 7 days
                df = ticker.history(interval=interval, start=get_lumibot_datetime() - timedelta(days=7), auto_adjust=False)
            elif interval == "15m":
                # Yahoo only supports 15 minute interval for past 60 days
                df = ticker.history(interval=interval, start=get_lumibot_datetime() - timedelta(days=60), auto_adjust=False)
            else:
                df = ticker.history(interval=interval, period="max", auto_adjust=False)
        except Exception as e:
            logging.debug(f"Error while downloading symbol day data for {symbol}, returning empty dataframe for now.")
            logging.debug(e)
            return None

        # Adjust the time when we are getting daily stock data to the beginning of the day
        # This way the times line up when backtesting daily data
        info = YahooHelper.get_symbol_info(symbol)
        if info.get("info") and info.get("info").get("market") == "us_market":
            # Check if the timezone is already set, if not set it to the default timezone
            if df.index.tzinfo is None:
                df.index = df.index.tz_localize(info.get("info").get("exchangeTimezoneName"))
            else:
                df.index = df.index.tz_convert(info.get("info").get("exchangeTimezoneName"))
            df.index = df.index.map(lambda t: t.replace(hour=16, minute=0))
        elif info.get("info") and info.get("info").get("market") == "ccc_market":
            # Check if the timezone is already set, if not set it to the default timezone
            if df.index.tzinfo is None:
                df.index = df.index.tz_localize(info.get("info").get("exchangeTimezoneName"))
            else:
                df.index = df.index.tz_convert(info.get("info").get("exchangeTimezoneName"))
            df.index = df.index.map(lambda t: t.replace(hour=23, minute=59))

        df = YahooHelper.process_df(df, asset_info=info)
        return df

    @staticmethod
    def download_symbols_data(symbols, interval="1d"):
        if len(symbols) == 1:
            item = YahooHelper.download_symbol_data(symbols[0], interval)
            return {symbols[0]: item}

        result = {}
        tickers = yf.Tickers(" ".join(symbols))
        df_yf = tickers.history(
            period="max",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
        )

        for i in df_yf.columns.levels[0]:
            result[i] = YahooHelper.process_df(df_yf[i])

        return result

    # ===================Cache retrieval and dumping=====================

    @staticmethod
    def fetch_symbol_info(symbol, caching=True, last_needed_datetime=None):
        if caching:
            cached_data = YahooHelper.check_pickle_file(symbol, INFO_DATA)
            if cached_data:
                if cached_data.is_up_to_date(last_needed_datetime=last_needed_datetime):
                    return cached_data.data

        # Caching is disabled or no previous data found
        # or data found not up to date
        data = YahooHelper.download_symbol_info(symbol)
        YahooHelper.dump_pickle_file(symbol, INFO_DATA, data)
        return data

    @staticmethod
    def fetch_symbol_data(symbol, caching=True, last_needed_datetime=None, interval="1d"):
        if caching:
            cached_data = YahooHelper.check_pickle_file(symbol, interval)
            if cached_data:
                if cached_data.is_up_to_date(last_needed_datetime=last_needed_datetime):
                    return cached_data.data

        # Caching is disabled or no previous data found
        # or data found not up to date
        data = YahooHelper.download_symbol_data(symbol, interval)

        # Check if the data is empty
        if data is None or data.empty:
            return data

        YahooHelper.dump_pickle_file(symbol, interval, data)
        return data

    @staticmethod
    def fetch_symbols_data(symbols, interval, caching=True):
        result = {}
        missing_symbols = symbols.copy()

        if caching:
            for symbol in symbols:
                cached_data = YahooHelper.check_pickle_file(symbol, interval)
                if cached_data:
                    if cached_data.is_up_to_date():
                        result[symbol] = cached_data.data
                        missing_symbols.remove(symbol)

        if missing_symbols:
            missing_data = YahooHelper.download_symbols_data(missing_symbols, interval)
            for symbol, data in missing_data.items():
                result[symbol] = data
                YahooHelper.dump_pickle_file(symbol, interval, data)

        return result

    # ======Shortcut methods==================================

    @staticmethod
    def get_symbol_info(symbol, caching=True):
        return YahooHelper.fetch_symbol_info(symbol, caching=caching)

    @staticmethod
    def get_symbol_data(
        symbol,
        interval="1d",
        caching=True,
        auto_adjust=False,
        last_needed_datetime=None,
    ):
        if interval in ["1m", "15m", "1d"]:
            df = YahooHelper.fetch_symbol_data(
                symbol,
                interval=interval,
                caching=caching,
                last_needed_datetime=last_needed_datetime,
            )
            return YahooHelper.format_df(df, False)
        else:
            raise ValueError("Unknown interval %s" % interval)

    @staticmethod
    def get_symbols_data(symbols, interval="1d", auto_adjust=True, caching=True):
        result = YahooHelper.fetch_symbols_data(symbols, interval=interval, caching=caching)
        for key, df in result.items():
            result[key] = YahooHelper.format_df(df, auto_adjust)
        return result

    @staticmethod
    def get_symbols_data(symbols, interval="1d", auto_adjust=True, caching=True):
        if interval in ["1m", "15m", "1d"]:
            return YahooHelper.get_symbols_data(symbols, interval=interval, auto_adjust=auto_adjust, caching=caching)
        else:
            raise ValueError("Unknown interval %s" % interval)

    @staticmethod
    def get_symbol_dividends(symbol, caching=True):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol_data(symbol, caching=caching)
        dividends = history["Dividends"]
        return dividends[dividends != 0].dropna()

    @staticmethod
    def get_symbols_dividends(symbols, caching=True):
        result = {}
        data = YahooHelper.get_symbols_data(symbols, caching=caching)
        for symbol, df in data.items():
            dividends = df["Dividends"]
            result[symbol] = dividends[dividends != 0].dropna()

        return result

    @staticmethod
    def get_symbol_splits(symbol, caching=True):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol_data(symbol, caching=caching)
        splits = history["Stock Splits"]
        return splits[splits != 0].dropna()

    @staticmethod
    def get_symbols_splits(symbols, caching=True):
        result = {}
        data = YahooHelper.get_symbols_data(symbols, caching=caching)
        for symbol, df in data.items():
            splits = df["Stock Splits"]
            result[symbol] = splits[splits != 0].dropna()

        return result

    @staticmethod
    def get_symbol_actions(symbol, caching=True):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol__data(symbol, caching=caching)
        actions = history[["Dividends", "Stock Splits"]]
        return actions[actions != 0].dropna(how="all").fillna(0)

    @staticmethod
    def get_symbols_actions(symbols, caching=True):
        result = {}
        data = YahooHelper.get_symbols__data(symbols, caching=caching)
        for symbol, df in data.items():
            actions = df[["Dividends", "Stock Splits"]]
            result[symbol] = actions[actions != 0].dropna(how="all").fillna(0)

        return result

    @staticmethod
    def get_risk_free_rate(with_logging: bool = False, caching: bool = True, dt: datetime = None):
        # 13 Week Treasury Rate (^IRX)
        if dt is None:
            # If we don't have a datetime, we will get the latest value
            irx_price = YahooHelper.get_symbol_last_price("^IRX")
        else:
            # If we do have a datetime, we will get the value at that datetime
            irx_df = YahooHelper.get_symbol_data("^IRX", last_needed_datetime=dt)

            if irx_df is None or irx_df.empty:
                return None

            # Ensure the DataFrame index is in datetime format and sort it
            irx_df.index = pd.to_datetime(irx_df.index)
            irx_df.sort_index(inplace=True)

            # Calculate the absolute difference between the given datetime and all dates in the index
            delta = abs(irx_df.index - dt)

            # Find the index of the minimum difference
            closest_date_index = delta.argmin()

            # Extract the row for the closest date
            irx_row = irx_df.iloc[closest_date_index]

            # Get the close price
            irx_price = irx_row["Close"]

        risk_free_rate = irx_price / 100
        if with_logging:
            logging.info(f"Risk Free Rate {risk_free_rate * 100:0.2f}%")

        return risk_free_rate

    # ==========Appending Data====================================

    @staticmethod
    def append_actions_data(symbol, df, caching=True):
        if df.empty:
            return df

        df = df.copy()
        df["dividend"] = 0
        df["stock_splits"] = 0

        dividends_actions = YahooHelper.get_symbol_actions(symbol, caching=caching)
        start = df.index[0]
        end = df.index[-1]
        filtered_actions = dividends_actions[(dividends_actions.index >= start) & (dividends_actions.index <= end)]

        for index, row in filtered_actions.iterrows():
            dividends = row["Dividends"]
            stock_splits = row["Stock Splits"]
            search = df[df.index >= index]
            if not search.empty:
                target_day = search.index[0]
                df.loc[target_day, "dividend"] = dividends
                df.loc[target_day, "stock_splits"] = stock_splits

        return df
