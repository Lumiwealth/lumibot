from datetime import timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from alpaca_trade_api.entity import BarSet
import datetime as dt
import pandas as pd

from tools import get_chunks


class AlpacaData:
    @staticmethod
    def is_market_open(api):
        """return True if market is open else false"""
        return api.get_clock().is_open

    @staticmethod
    def get_time_to_open(api):
        """Return the remaining time for the market to open in seconds"""
        clock = api.get_clock()
        opening_time = clock.next_open.replace(tzinfo=timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=timezone.utc).timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

    @staticmethod
    def get_time_to_close(api):
        """Return the remaining time for the market to close in seconds"""
        clock = api.get_clock()
        closing_time = clock.next_close.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    @staticmethod
    def get_tradable_assets(api, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets from the market"""
        assets = api.list_assets()
        result = []
        for asset in assets:
            is_valid = asset.tradable
            if easy_to_borrow is not None and isinstance(easy_to_borrow, bool):
                is_valid = is_valid & (easy_to_borrow == asset.easy_to_borrow)
            if filter_func is not None:
                filter_test = filter_func(asset.symbol)
                is_valid = is_valid & filter_test

            if is_valid:
                result.append(asset.symbol)

        return result

    @staticmethod
    def get_symbol_bars(api, symbol, time_unit, length=None, start=None, end=None):
        """Get bars for a give symbol"""
        kwargs = dict(limit=length, start=start, end=end)
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        df = api.get_barset(symbol, time_unit, **kwargs).df[symbol]
        return df

    @staticmethod
    def get_symbols_bars(
        api,
        symbols,
        time_unit,
        length=None,
        start=None,
        end=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of symbols"""
        raw = {}
        chunks = get_chunks(symbols, chunk_size)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = []
            func = lambda args, kwargs: api.get_barset(*args, **kwargs)
            kwargs = dict(limit=length, start=start, end=end)
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for chunk in chunks:
                tasks.append(executor.submit(func, (chunk, time_unit), kwargs))

            for task in as_completed(tasks):
                assets_bars = task.result()
                for k, v in assets_bars.items():
                    raw[k] = v._raw

        return BarSet(raw)

    @staticmethod
    def get_last_price(api, symbol):
        """Takes an asset symbol and returns the last known price"""
        df = api.get_barset([symbol], "minute", 1).df[symbol]
        return df.iloc[0].close

    @staticmethod
    def get_last_prices(api, symbols):
        """Takes a list of symbols and returns the last known prices"""
        result = {}
        bars = AlpacaData.get_symbol_bars(api, symbols, "minute", 1)
        for symbol, symbol_bars in bars.items():
            if symbol_bars:
                last_value = symbol_bars.df["close"][-1]
                result[symbol] = last_value

        return result

    @staticmethod
    def get_asset_momentum(
        api,
        symbol,
        time_unit="minute",
        momentum_length=1,
        length=None,
        start=None,
        end=None,
    ):
        """Calculates an asset momentum over a period and returns a dataframe"""
        df = AlpacaData.get_symbol_bars(
            api, symbol, time_unit, length=length, start=start, end=end
        )
        n_rows = len(df.index)
        if n_rows <= momentum_length:
            raise Exception(
                'Number of timestamps must be superior to the momentum_length.'
                'received %d timestamps with a momentum_length set to %d.' %
                (n_rows, momentum_length)
            )

        df["price_change"] = df["close"].pct_change()
        df["momentum"] = df["close"].pct_change(periods=momentum_length)
        return df[df['momentum'].notna()]

    @staticmethod
    def get_assets_momentum(
        api,
        symbols,
        time_unit="minute",
        momentum_length=1,
        length=None,
        start=None,
        end=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Calculates a list of asset momentums
        over a period and returns a dataframe"""
        result = {}
        barsets = AlpacaData.get_symbols_bars(
            api,
            symbols,
            time_unit,
            length=length,
            start=start,
            end=end,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )
        if not barsets:
            return result

        test = list(barsets.values())[0]
        n_rows = len(test)
        if n_rows <= momentum_length:
            raise Exception(
                'Number of timestamps must be superior to the momentum_length.'
                'received %d timestamps with a momentum_length set to %d.' %
                (n_rows, momentum_length)
            )

        for k, v in barsets.items():
            df = v.df
            df["price_change"] = df["close"].pct_change()
            df["momentum"] = df["close"].pct_change(periods=momentum_length)
            result[k] = df[df['momentum'].notna()]

        return result

