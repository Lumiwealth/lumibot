from datetime import datetime

import pandas as pd

from .bar import Bar


class Bars:
    def __init__(self, df, source, symbol, raw=None):
        """
        df columns: open, high, low, close, dividend, volume
        df index: pd.Timestamp
        """
        self.source = source.upper()
        self.symbol = symbol.upper()
        self.df = df
        self._raw = raw

    def __repr__(self):
        return repr(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()

    @classmethod
    def parse_bar_list(cls, bar_list, source, symbol):
        raw = []
        for bar in bar_list:
            raw.append(bar)

        df = pd.DataFrame(raw)
        df = df.set_index("timestamp")
        df["price_change"] = df["close"].pct_change()
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = cls(df, source, symbol, raw=bar_list)
        return bars

    def split(self):
        result = []
        for index, row in self.df.iterrows():
            item = {
                "timestamp": int(index.timestamp()),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "dividend": row.get("dividend", 0),
                "stock_splits": row.get("stock_splits", 0),
            }
            bar = Bar(item)
            result.append(bar)

        return result

    def get_last_price(self):
        return self.df["close"][-1]

    def get_last_dividend(self):
        return self.df["dividend"][-1]

    def get_momentum_df(self, momentum_length):
        df = self.df.copy()
        df["momentum"] = df["close"].pct_change(periods=momentum_length)
        return df[df["momentum"].notna()]

    def get_momentum(self, start=None, end=None):
        df_copy = self.df.copy()
        if isinstance(start, datetime):
            df_copy = df_copy[df_copy.index >= start]
        if isinstance(end, datetime):
            df_copy = df_copy[df_copy.index <= end]

        n_rows = df_copy.shape[0]
        if n_rows <= 1:
            return 0

        momentum = df_copy["close"].pct_change(n_rows - 1)[-1]
        return momentum

    def get_total_volume(self, period=None):
        if period is None:
            volume = self.df["volume"].sum()
        else:
            volume = self.df[-period:]["volume"].sum()

        return volume
