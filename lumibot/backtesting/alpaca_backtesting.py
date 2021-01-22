import logging
import math
from datetime import datetime, timedelta

import pandas as pd
from data_sources import AlpacaData
from entities import Bars
from lumibot.tools import deduplicate_sequence


class AlpacaDataBacktesting(AlpacaData):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, config, datetime_start, datetime_end):
        AlpacaData.__init__(self, config)
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start
        self._data_store = {}

    def _get_start_end_dates(self, length, time_unit, time_delta=None):
        time_shift = datetime.now() - self._datetime
        if time_delta is None:
            time_delta = time_shift
        else:
            time_delta += time_shift
        end_date = datetime.now() - time_delta
        period_length = length * time_unit
        start_date = end_date - period_length
        return (start_date, end_date)

    def _deduplicate_store_row(self, symbol):
        self._data_store[symbol] = deduplicate_sequence(self._data_store[symbol])

    def _update_store(self, symbol, start_date, end_date):
        start_date = self.NY_PYTZ.localize(start_date)
        end_date = self.NY_PYTZ.localize(end_date)
        query_ranges = []
        if symbol in self._data_store:
            data = self._data_store[symbol]
            first_date = data[0].t.to_pydatetime()
            last_date = data[-1].t.to_pydatetime()
            if first_date > start_date:
                period = first_date - start_date
                n_years = math.ceil(period / timedelta(days=366))
                query_ranges.append(
                    (first_date - n_years * timedelta(days=366), first_date)
                )
            if last_date < end_date:
                period = end_date - last_date
                n_years = math.ceil(period / timedelta(days=366))
                query_ranges.append(
                    (last_date, last_date + n_years * timedelta(days=366))
                )
        else:
            self._data_store[symbol] = []
            period = end_date - start_date
            n_years = math.ceil(period / timedelta(days=366))
            query_ranges.append(
                (
                    start_date - timedelta(days=366),
                    start_date + n_years * timedelta(days=366),
                )
            )

        if query_ranges:
            logging.info("Fetching new Data for %r" % symbol)
            for start_query_date, end_query_date in query_ranges:
                period = end_query_date - start_query_date
                n_years = math.ceil(period / timedelta(days=366))
                for i in range(n_years):
                    start = self.format_datetime(
                        start_query_date + i * timedelta(days=366)
                    )
                    end = self.format_datetime(
                        start_query_date + (i + 1) * timedelta(days=366)
                    )
                    response = self.api.get_barset(symbol, "1Min", start=start, end=end)
                    self._data_store[symbol].extend(response[symbol])

            self._data_store[symbol].sort(key=lambda x: x.t)
            self._deduplicate_store_row(symbol)

    def _extract_data(self, symbol, length, end, interval=None):
        if interval is None:
            interval = timedelta(minutes=1)
        data = self._data_store[symbol]
        filtered = [row for row in data if row.t.timestamp() <= end.timestamp()]
        result = []
        for item in filtered:
            if result:
                last_timestamp = result[-1].t
                if item.t - last_timestamp >= interval:
                    result.append(item)
            else:
                result.append(item)
        result = result[-length:]
        return result

    def _pull_source_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        start_date, end_date = self._get_start_end_dates(
            length, time_unit, time_delta=time_delta
        )
        self._update_store(symbol, start_date, end_date)
        data = self._extract_data(symbol, length, end_date, interval=time_unit)
        return data

    def _parse_source_symbol_bars(self, response):
        if not response:
            return

        df = pd.DataFrame()
        for row in response:
            item = {
                "time": row.t,
                "open": row.o,
                "high": row.h,
                "low": row.l,
                "close": row.c,
                "volume": row.v,
                "dividend": 0,
                "stock_splits": 0,
            }
            df = df.append(item, ignore_index=True)

        df = df.set_index("time")
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, raw=response)
        return bars

    def _pull_source_bars(self, symbols, length, time_unit, time_delta=None):
        result = {}
        for symbol in symbols:
            data = self._pull_source_symbol_bars(
                symbol, length, time_unit, time_delta=time_delta
            )
            result[symbol] = data
        return result

    def _parse_source_bars(self, response):
        result = {}
        for symbol, data in response.items():
            result[symbol] = self._parse_source_symbol_bars(data)
        return result
