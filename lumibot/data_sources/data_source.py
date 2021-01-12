from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

from lumibot.tools import get_chunks


class DataSource:
    IS_BACKTESTING_DATA_SOURCE = False
    MIN_TIME_STEP = timedelta(minutes=1)

    def _parse_source_time_unit(self, time_unit, reverse=False):
        """parse the data source time_unit variable
        into a datetime.timedelta. set reverse to True to parse
        timedelta to data_source time_unit representation"""
        pass

    def _pull_source_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        """pull source bars for a given symbol"""
        pass

    def _pull_source_bars(self, symbols, length, time_unit, time_delta=None):
        pass

    def _parse_source_symbol_bars(self, response):
        pass

    def _parse_source_bars(self, response):
        pass

    def get_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        """Get bars for a given symbol"""
        if not isinstance(symbol, str):
            raise ValueError("symbol parameter must be a string, received %r" % symbol)
        response = self._pull_source_symbol_bars(
            symbol, length, time_unit, time_delta=time_delta
        )
        bars = self._parse_source_symbol_bars(response)
        return bars

    def get_bars(
        self,
        symbols,
        length,
        time_unit,
        time_delta=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of symbols"""
        chunks = get_chunks(symbols, chunk_size)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = []
            func = lambda args, kwargs: self._pull_source_bars(*args, **kwargs)
            kwargs = dict(time_delta=time_delta)
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for chunk in chunks:
                tasks.append(executor.submit(func, (chunk, length, time_unit), kwargs))

            result = {}
            for task in as_completed(tasks):
                response = task.result()
                parsed = self._parse_source_bars(response)
                result = {**result, **parsed}

        return result

    def get_last_price(self, symbol):
        """Takes an asset symbol and returns the last known price"""
        bars = self.get_symbol_bars(symbol, 1, self.MIN_TIME_STEP)
        return bars.df.iloc[0].close

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        result = {}
        symbols_bars = self.get_bars(symbols, 1, self.MIN_TIME_STEP)
        for symbol, bars in symbols_bars.items():
            if bars is not None:
                last_value = bars.df.iloc[0].close
                result[symbol] = last_value

        return result

    def get_yesterday_dividend(self, symbol):
        """Return dividend per share for a given
        symbol for the day before"""
        bars = self.get_symbol_bars(symbol, 1, timedelta(days=1), timedelta(days=1))
        return bars.get_last_dividend()

    def get_yesterday_dividends(self, symbols):
        """Return dividend per share for a list of
        symbols for the day before"""
        result = {}
        symbols_bars = self.get_bars(symbols, 1, timedelta(days=1), timedelta(days=1))
        for symbol, bars in symbols_bars.items():
            if bars is not None:
                result[symbol] = bars.get_last_dividend()

        return result
