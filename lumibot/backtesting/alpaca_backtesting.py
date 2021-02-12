import bisect
import logging
import math
from datetime import datetime, timedelta

import pandas as pd
from alpaca_trade_api.entity import Bar

from lumibot.data_sources import AlpacaData
from lumibot.entities import Bars
from lumibot.tools import deduplicate_sequence
from lumibot.trading_builtins import get_redis_db

from .data_source_backtesting import DataSourceBacktesting


class AlpacaDataBacktesting(AlpacaData, DataSourceBacktesting):
    IS_BACKTESTING_DATA_SOURCE = True

    def __init__(self, datetime_start, datetime_end, auth=None):
        AlpacaData.__init__(self, auth)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
        self._redis_cache = get_redis_db()
        self._data_store = {}

    def _redis_mapping_parser(self, data):
        item = {
            "c": data.get("close"),
            "h": data.get("high"),
            "l": data.get("low"),
            "o": data.get("open"),
            "t": int(data.get("timestamp")),
            "v": data.get("volume"),
        }
        return Bar(item)

    def _get_datetime_range(self, length, timestep="minute", timeshift=None):
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
        """Return a list of tuple. Each tuple corresponds to a date range,
        first value is a datetime that indicates the start of the daterange,
        second is the end of the daterange. Both datetime objects are
        tz-aware America/New_York Timezones"""
        start_date = self.localize_datetime(start_date)
        end_date = self.localize_datetime(end_date)
        query_ranges = []
        if symbol in self._data_store and self._data_store[symbol]:
            data = self._data_store[symbol]
            first_date = data[0].t.to_pydatetime()
            last_date = data[-1].t.to_pydatetime()
            if first_date > start_date:
                period = first_date - start_date
                n_years = math.ceil(period / timedelta(days=366))
                for i in range(-n_years, 0):
                    query_ranges.append(
                        (
                            first_date + i * timedelta(days=366),
                            first_date + (i + 1) * timedelta(days=366),
                        )
                    )

            if last_date < end_date:
                period = end_date - last_date
                n_years = math.ceil(period / timedelta(days=366))
                for i in range(n_years):
                    query_ranges.append(
                        (
                            last_date + i * timedelta(days=366),
                            last_date + (i + 1) * timedelta(days=366),
                        )
                    )
        else:
            self._data_store[symbol] = []
            period = end_date - start_date
            n_years = math.ceil(period / timedelta(days=366))
            for i in range(-1, n_years):
                query_ranges.append(
                    (
                        start_date + i * timedelta(days=366),
                        start_date + (i + 1) * timedelta(days=366),
                    )
                )

        return query_ranges

    def _update_store(self, symbol, start_date, end_date):
        if self._redis_cache and symbol not in self._data_store:
            logging.info(f"Checking {symbol} data in redis database")
            redis_store = self._redis_cache.retrieve_store(
                self.SOURCE, [symbol], parser=self._redis_mapping_parser
            )
            redis_store[symbol].sort(key=lambda b: b.t)
            self._data_store[symbol] = redis_store[symbol]

        query_ranges = self._get_missing_range(symbol, start_date, end_date)
        if query_ranges:
            logging.info("Fetching new Data for %r" % symbol)
            for start, end in query_ranges:
                logging.info(f"Fetching data from {start} to {end}")
                start = self._format_datetime(start)
                end = self._format_datetime(end)
                response = self.api.get_barset(symbol, "1Min", start=start, end=end)
                if response and self._redis_cache:
                    bars = self._parse_source_symbol_bars(response[symbol], symbol)
                    self._redis_cache.store_bars(bars)
                    self._redis_cache.bgsave()
                self._data_store[symbol].extend(response[symbol])

            self._data_store[symbol].sort(key=lambda x: x.t)
            self._deduplicate_store_row(symbol)

    def _extract_data(self, symbol, length, end, timestep="minute"):
        result = []
        data = self._data_store[symbol]
        dummy_bar = Bar(None)

        if timestep == "minute":
            dummy_bar.t = self.localize_datetime(end)
            end_position = bisect.bisect_right(data, dummy_bar) - 1
        else:
            next_day_date = end.date() + timedelta(days=1)
            next_day_datetime = datetime.combine(next_day_date, datetime.min.time())
            dummy_bar.t = self.localize_datetime(next_day_datetime)
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
                    item.t = self.localize_datetime(new_date)
                    result.insert(0, item)
            else:
                if timestep == "minute":
                    result.append(item)
                elif timestep == "day":
                    new_date = datetime.combine(item.t.date(), datetime.min.time())
                    item.t = self.localize_datetime(new_date)
                    result.append(item)

            if len(result) >= length:
                return result

        return result

    def _pull_source_symbol_bars(
        self, symbol, length, timestep="minute", timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        start_date, end_date = self._get_datetime_range(
            length, timestep=timestep, timeshift=timeshift
        )
        self._update_store(symbol, start_date, end_date)
        data = self._extract_data(symbol, length, end_date, timestep=timestep)
        return data

    def _parse_source_symbol_bars(self, response, symbol):
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
        bars = Bars(df, self.SOURCE, symbol, raw=response)
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
            result[symbol] = self._parse_source_symbol_bars(data, symbol)
        return result
