from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import pytz

from lumibot.tools import get_chunks


class DataSource:
    SOURCE = ""
    IS_BACKTESTING_DATA_SOURCE = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = []
    DEFAULT_TIMEZONE = "America/New_York"
    DEFAULT_PYTZ = pytz.timezone(DEFAULT_TIMEZONE)

    @classmethod
    def localize_datetime(cls, dt):
        return cls.DEFAULT_PYTZ.localize(dt, is_dst=None)

    @classmethod
    def to_default_timezone(cls, dt):
        return dt.astimezone(cls.DEFAULT_PYTZ)

    def _parse_source_timestep(self, timestep, reverse=False):
        """transform the data source timestep variable
        into lumibot representation. set reverse to True
        for opposite direction"""
        for item in self.TIMESTEP_MAPPING:
            if reverse:
                if timestep == item["timestep"]:
                    return item["represntations"][0]
            else:
                if timestep in item["represntations"]:
                    return item["timestep"]

        raise ValueError("timestep %r did not match" % timestep)

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull source bars for a given symbol"""
        pass

    def _pull_source_bars(self, symbols, length, timestep=MIN_TIMESTEP, timeshift=None):
        pass

    def _parse_source_symbol_bars(self, response, symbol):
        pass

    def _parse_source_bars(self, response):
        pass

    def get_symbol_bars(self, symbol, length, timestep="", timeshift=None):
        """Get bars for a given symbol"""
        if not timestep:
            timestep = self.MIN_TIMESTEP

        if not isinstance(symbol, str):
            raise ValueError("symbol parameter must be a string, received %r" % symbol)
        response = self._pull_source_symbol_bars(
            symbol, length, timestep=timestep, timeshift=timeshift
        )
        bars = self._parse_source_symbol_bars(response, symbol)
        return bars

    def get_bars(
        self,
        symbols,
        length,
        timestep="",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of symbols"""
        if not timestep:
            timestep = self.MIN_TIMESTEP

        chunks = get_chunks(symbols, chunk_size)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = []
            func = lambda args, kwargs: self._pull_source_bars(*args, **kwargs)
            kwargs = dict(timestep=timestep, timeshift=timeshift)
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for chunk in chunks:
                tasks.append(executor.submit(func, (chunk, length), kwargs))

            result = {}
            for task in as_completed(tasks):
                response = task.result()
                parsed = self._parse_source_bars(response)
                result = {**result, **parsed}

        return result

    def get_last_price(self, symbol):
        """Takes an asset symbol and returns the last known price"""
        bars = self.get_symbol_bars(symbol, 1, timestep=self.MIN_TIMESTEP)
        return bars.df.iloc[0].close

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        result = {}
        symbols_bars = self.get_bars(symbols, 1, timestep=self.MIN_TIMESTEP)
        for symbol, bars in symbols_bars.items():
            if bars is not None:
                last_value = bars.df.iloc[0].close
                result[symbol] = last_value

        return result

    def get_yesterday_dividend(self, symbol):
        """Return dividend per share for a given
        symbol for the day before"""
        bars = self.get_symbol_bars(
            symbol, 1, timestep="day", timeshift=timedelta(days=1)
        )
        return bars.get_last_dividend()

    def get_yesterday_dividends(self, symbols):
        """Return dividend per share for a list of
        symbols for the day before"""
        result = {}
        symbols_bars = self.get_bars(
            symbols, 1, timestep="day", timeshift=timedelta(days=1)
        )
        for symbol, bars in symbols_bars.items():
            if bars is not None:
                result[symbol] = bars.get_last_dividend()

        return result
