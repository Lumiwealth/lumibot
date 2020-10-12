from datetime import timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from alpaca_trade_api.entity import BarSet
import datetime as dt
import pandas as pd

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

from tools import get_chunks


class AlpacaData:
    def __init__(self, config, connect_stream=True, max_workers=200, chunk_size=100):
        # Calling the Broker init method
        super().__init__()

        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading to 200
        # to go full speed if needed
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        api_key = config.API_KEY
        api_secret = config.API_SECRET
        if hasattr(config, "ENDPOINT"):
            endpoint = config.ENDPOINT
        else:
            endpoint = "https://paper-api.alpaca.markets"
        if hasattr(config, 'VERSION'):
            version = config.VERSION
        else:
            version = "v2"
        self.api = tradeapi.REST(api_key, api_secret, URL(endpoint), version)

    def is_market_open(self):
        """return True if market is open else false"""
        return self.api.get_clock().is_open

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        clock = self.api.get_clock()
        opening_time = clock.next_open.replace(tzinfo=timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=timezone.utc).timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        clock = self.api.get_clock()
        closing_time = clock.next_close.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets from the market"""
        assets = self.api.list_assets()
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

    def get_symbol_bars(self, symbol, time_unit, length=None, start=None, end=None):
        """Get bars for a given symbol"""
        kwargs = dict(limit=length, start=start, end=end)
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        df = self.api.get_barset(symbol, time_unit, **kwargs).df[symbol]
        return df

    def get_symbols_bars(
        self,
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
            func = lambda args, kwargs: self.api.get_barset(*args, **kwargs)
            kwargs = dict(limit=length, start=start, end=end)
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for chunk in chunks:
                tasks.append(executor.submit(func, (chunk, time_unit), kwargs))

            for task in as_completed(tasks):
                assets_bars = task.result()
                for k, v in assets_bars.items():
                    raw[k] = v._raw

        return BarSet(raw)

    def get_last_price(self, symbol):
        """Takes an asset symbol and returns the last known price"""
        df = self.api.get_barset([symbol], "minute", 1).df[symbol]
        return df.iloc[0].close

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        result = {}
        bars = self.get_symbol_bars(self.api, symbols, "minute", 1)
        for symbol, symbol_bars in bars.items():
            if symbol_bars:
                last_value = symbol_bars.df["close"][-1]
                result[symbol] = last_value

        return result

    def get_asset_momentum(
        self,
        symbol,
        time_unit="minute",
        momentum_length=1,
        length=None,
        start=None,
        end=None,
    ):
        """Calculates an asset momentum over a period and returns a dataframe"""
        df = self.get_symbol_bars(
            symbol, time_unit, length=length, start=start, end=end
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

    def get_assets_momentum(
        self,
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
            self.api,
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

