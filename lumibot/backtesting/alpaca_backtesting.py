import bisect
import logging
import math
from datetime import datetime, timedelta

import pandas as pd
from alpaca_trade_api.entity import Bar

from lumibot.data_sources import AlpacaData
from lumibot.entities import Bars
from lumibot.tools import deduplicate_sequence


class AlpacaDataBacktesting(AlpacaData):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, datetime_start, datetime_end, auth=None):
        AlpacaData.__init__(self, auth)
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start
        self._data_store = {}

    def _get_start_end_dates(self, length, timestep="minute", timeshift=None):
        backtesting_timeshift = datetime.now() - self._datetime
        if timeshift:
            backtesting_timeshift += timeshift

        end_date = datetime.now() - backtesting_timeshift
        if timestep == "minute":
            period_length = length * timedelta(minutes=1)
        else:
            period_length = length * timedelta(days=1)
        start_date = end_date - period_length
        return (start_date, end_date)

    def _deduplicate_store_row(self, symbol):
        self._data_store[symbol] = deduplicate_sequence(self._data_store[symbol])

    def _get_missing_range(self, symbol, start_date, end_date):
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
                for i in range(-n_years, 0, -1):
                    query_ranges.append((
                        first_date - i * timedelta(days=366),
                        first_date - (i + 1) * timedelta(days=366)
                    ))
            if last_date < end_date:
                period = end_date - last_date
                n_years = math.ceil(period / timedelta(days=366))
                for i in range(n_years):
                    query_ranges.append((
                        last_date + i * timedelta(days=366),
                        last_date + (i + 1) * timedelta(days=366),
                    ))
        else:
            self._data_store[symbol] = []
            period = end_date - start_date
            n_years = math.ceil(period / timedelta(days=366))
            for i in range(-1, n_years):
                query_ranges.append((
                    start_date + i * timedelta(days=366),
                    start_date + (i + 1) * timedelta(days=366),
                ))

        return query_ranges

    def _update_store(self, symbol, start_date, end_date):
        query_ranges = self._get_missing_range(symbol, start_date, end_date)
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
                    logging.info(f"Fetching data from {start} to {end}")
                    print(f"Fetching data from {start} to {end}")
                    response = self.api.get_barset(symbol, "1Min", start=start, end=end)
                    self._data_store[symbol].extend(response[symbol])

            self._data_store[symbol].sort(key=lambda x: x.t)
            self._deduplicate_store_row(symbol)

    def _extract_data(self, symbol, length, end, timestep="minute"):
        result = []
        data = self._data_store[symbol]
        dummy_bar = Bar(None)

        if timestep == "minute":
            dummy_bar.t = self.NY_PYTZ.localize(end)
            end_position = bisect.bisect_right(data, dummy_bar) - 1
        else:
            next_day_date = end.date() + timedelta(days=1)
            next_day_datetime = datetime.combine(next_day_date, datetime.min.time())
            dummy_bar.t = self.NY_PYTZ.localize(next_day_datetime)
            end_position = bisect.bisect_left(data, dummy_bar) - 1

        for index in range(end_position, -1, -1):
            item = Bar(data[index]._raw)
            if result:
                last_timestamp = result[0].t
                interval = timedelta(minutes=1)
                if timestep == "minute" and last_timestamp - item.t >= interval:
                    result.insert(0, item)
                elif timestep == "day" and last_timestamp.date() != item.t.date():
                    new_date = datetime.combine(item.t.date(), datetime.min.time())
                    item.t = self.NY_PYTZ.localize(new_date)
                    result.insert(0, item)
            else:
                if timestep == "minute":
                    result.append(item)
                elif timestep == "day":
                    new_date = datetime.combine(item.t.date(), datetime.min.time())
                    item.t = self.NY_PYTZ.localize(new_date)
                    result.append(item)

            if len(result) >= length:
                return result

        return result

    def _pull_source_symbol_bars(
        self, symbol, length, timestep="minute", timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        start_date, end_date = self._get_start_end_dates(
            length, timestep=timestep, timeshift=timeshift
        )
        self._update_store(symbol, start_date, end_date)
        data = self._extract_data(symbol, length, end_date, timestep=timestep)
        return data

    def _parse_source_symbol_bars(self, response):
        if not response:
            return

        raw = []
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
            raw.append(item)

        df = pd.DataFrame(raw)
        df = df.set_index("time")
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, raw=response)
        return bars

    def _pull_source_bars(self, symbols, length, timestep="minute", timeshift=None):
        self._parse_source_timestep(timestep, reverse=True)
        result = {}
        for symbol in symbols:
            data = self._pull_source_symbol_bars(
                symbol, length, timestep=timestep, timeshift=timeshift
            )
            result[symbol] = data
        return result

    def _parse_source_bars(self, response):
        result = {}
        for symbol, data in response.items():
            result[symbol] = self._parse_source_symbol_bars(data)
        return result

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
