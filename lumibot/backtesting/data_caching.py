import bisect
import logging
import math
from datetime import datetime, timedelta

from lumibot.entities import Bar, Bars
from lumibot.tools import deduplicate_sequence
from lumibot.trading_builtins import get_redis_db

from .data_source_backtesting import DataSourceBacktesting


class DataCaching(DataSourceBacktesting):
    def __init__(self, datetime_start, datetime_end):
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
        self._redis_cache = get_redis_db()
        self._data_store = {}

    def _redis_mapping_parser(self, data):
        """Parse a redis mapping and return a Bar entity"""
        item = {
            "timestamp": int(data.get("timestamp")),
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "volume": data.get("volume"),
            "dividend": data.get("dividend", 0),
            "stock_splits": data.get("stock_splits", 0),
        }
        return Bar(item)

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

            # Needs to be changed
            first_date = data[0].datetime
            last_date = data[-1].datetime

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

            redis_store[symbol].sort(key=lambda b: b.timestamp)
            self._data_store[symbol] = redis_store[symbol]

        query_ranges = self._get_missing_range(symbol, start_date, end_date)
        if query_ranges:
            logging.info("Fetching new Data for %r" % symbol)
            for start, end in query_ranges:
                logging.info(f"Fetching data from {start} to {end}")
                start = self._format_datetime(start)
                end = self._format_datetime(end)
                length = 1 + (end - start) // timedelta(minutes=1)
                bars = self.get_symbol_bars(symbol, length, "minute")
                bar_list = bars.split()
                if response and self._redis_cache:
                    self._redis_cache.store_bars(bar_list)
                    self._redis_cache.bgsave()

                self._data_store[symbol].extend(bar_list)

            self._data_store[symbol].sort(key=lambda x: x.t)
            self._deduplicate_store_row(symbol)

    def _extract_data(self, symbol, length, end, timestep="minute"):
        result = []
        data = self._data_store[symbol]

        dummy_bar = Bar.get_empty_bar()
        if timestep == "minute":
            dummy_bar.timestamp = self.localize_datetime(end)
            end_position = bisect.bisect_right(data, dummy_bar) - 1
        else:
            next_day_date = end.date() + timedelta(days=1)
            next_day_datetime = datetime.combine(next_day_date, datetime.min.time())
            dummy_bar.timestamp = self.localize_datetime(next_day_datetime)
            end_position = bisect.bisect_left(data, dummy_bar) - 1

        for index in range(end_position, -1, -1):
            item = data[index]
            if result:
                last_datetime = result[0].datetime
                interval = timedelta(minutes=1)
                if timestep == "minute" and last_datetime - item.datetime >= interval:
                    result.insert(0, item)
                elif timestep == "day" and last_datetime.date() != item.datetime.date():
                    new_date = datetime.combine(
                        item.datetime.date(), datetime.min.time()
                    )
                    item.datetime = self.localize_datetime(new_date)
                    result.insert(0, item)
            else:
                if timestep == "minute":
                    result.append(item)
                elif timestep == "day":
                    new_date = datetime.combine(
                        item.datetime.date(), datetime.min.time()
                    )
                    item.datetime = self.localize_datetime(new_date)
                    result.append(item)

            if len(result) >= length:
                return result

        return result

    def _pull_source_symbol_bars(
        self, symbol, length, timestep="minute", timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        start_date, end_date = self.get_datetime_range(
            length, timestep=timestep, timeshift=timeshift
        )
        self._update_store(symbol, start_date, end_date)
        data = self._extract_data(symbol, length, end_date, timestep=timestep)
        return data

    def _pull_source_bars(self, symbols, length, timestep="minute", timeshift=None):
        self._parse_source_timestep(timestep, reverse=True)
        result = {}
        for symbol in symbols:
            data = self._pull_source_symbol_bars(
                symbol, length, timestep=timestep, timeshift=timeshift
            )
            result[symbol] = data
        return result

    def _parse_source_symbol_bars(self, response, symbol):
        if not response:
            return

        bars = Bars.parse_bar_list(response, self.SOURCE, symbol)
        return bars
