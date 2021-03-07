from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from alpha_vantage.timeseries import TimeSeries

from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars

from .data_source import DataSource


class AlphaVantageData(DataSource):
    SOURCE = "ALPHAVANTAGE"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "represntations": ["1min"]},
        {"timestep": "day", "represntations": ["1D"]},
    ]

    def __init__(self, config, max_workers=24):
        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading with a maximum
        # of 200
        self.name = "alphavantage"
        self.max_workers = min(max_workers, 24)

        # Stores
        self._data_store_minutes = {}
        self._data_store_days = {}

        # Connection to alphavantage REST API
        self.config = config
        self.api_key = config.API_KEY
        self.api = TimeSeries(key=self.api_key, output_format="csv")

    def _get_store(self, timestep):
        if timestep == "minute":
            return self._data_store_minutes
        return self._data_store_days

    def _csv_to_list(self, csv_reader):
        columns = next(csv_reader)
        data = []
        for row in csv_reader:
            data.append(dict(zip(columns, row)))
        return data

    def _request_minutes_data(self, symbol, slice):
        csv_reader, metadata = self.api.get_intraday_extended(
            symbol, interval="1min", slice=slice
        )
        data = self._csv_to_list(csv_reader)
        return data

    def _request_daily_data(self, symbol):
        csv_reader, metadata = self.api.get_daily_adjusted(symbol, outputsize="full")
        data = self._csv_to_list(csv_reader)
        return data

    def _request_data(self, symbol, timestep):
        if timestep == "minute":
            slices = [f"year{i}month{j}" for i in range(1, 3) for j in range(1, 13)]
            with ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix=f"{self.name}_requesting_data",
            ) as executor:
                tasks = []
                for slice in slices:
                    tasks.append(
                        executor.submit(self._request_minutes_data, symbol, slice)
                    )

                data = []
                for task in as_completed(tasks):
                    data.extend(task.result())

        else:
            data = self._request_daily_data(symbol)

        return data

    def _append_data(self, symbol, data, timestep):
        store = self._get_store(timestep)
        df = pd.DataFrame(data)
        if "time" in df.columns:
            index_column = "time"
        else:
            index_column = "timestamp"

        df.set_index(index_column, inplace=True)
        df.sort_index(inplace=True)
        df.index = df.index.map(lambda d: datetime.strptime(d, "%Y-%m-%d")).tz_localize(
            self.DEFAULT_TIMEZONE
        )
        store[symbol] = df
        return df

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        store = self._get_store(timestep)
        if symbol in store:
            data = store[symbol]
        else:
            data = self._request_data(symbol, timestep)
            if not data:
                raise NoDataFound(self.SOURCE, symbol)

            data = self._append_data(symbol, data, timestep)

        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            data = data[data.index <= end]

        result = data.tail(length)
        return result

    def _pull_source_bars(self, symbols, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list symbols"""
        result = {}
        self._parse_source_timestep(timestep, reverse=True)
        for symbol in symbols:
            result[symbol] = self._pull_source_symbol_bars(
                symbol, length, timestep=timestep, timeshift=timeshift
            )
        return result

    def _parse_source_symbol_bars(self, response, symbol):
        df = response.copy()
        if "adjusted_close" in df.columns:
            del df["adjusted_close"]

        if "dividend_amount" in df.columns:
            df.rename(columns={"dividend_amount": "dividend"}, inplace=True)
        else:
            df["dividend"] = 0

        if "split_coefficient" in df.columns:
            df.rename(columns={"split_coefficient": "stock_splits"}, inplace=True)
        else:
            df["stock_splits"] = 0

        df = df.astype(
            {
                "open": "float64",
                "high": "float64",
                "low": "float64",
                "close": "float64",
                "volume": "int64",
                "dividend": "float64",
                "stock_splits": "float64",
            }
        )

        df["price_change"] = df["close"].pct_change()
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars
