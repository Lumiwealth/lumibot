import logging
from datetime import datetime
from decimal import Decimal
from typing import Union

import pandas as pd

from .bar import Bar


class Bars:
    """Pricing and financial data for given Symbol.

    The OHLCV, and if available, dividends, stock splits for a given
    financial instrument. Price change, dividend yield and return
    are calculated if appropriate.

    Parameters
    ----------
    df : Pandas Dataframe
        Dataframe with:
            datetime.datetime index time zone aware.
            columns = ['open', 'high', 'low', 'close', 'volume']
            optional columns ['dividend', 'stock_splits']

    source :
            The source of the data e.g. (yahoo, alpaca, …)

    asset : Asset
        The asset for which the bars are holding data.

            source: the source of the data e.g. (yahoo, alpaca, …)
    symbol : str
        The ticker symbol. eg: "AAPL". If cryptocurrency, the symbol
        will be the pair of coins. eg: "ETH/BTC"

    quote: Asset
        For cryptocurrency only. This is the other asset for trading
        getting ohlcv quotes.

    Methods
    -------
    get_last_price
        Returns the closing price of the last dataframe row

    get_last_dividend
        Returns the dividend per share value of the last dataframe row

    get_momentum(num_periods=1)
        Calculates the global price momentum of the dataframe.

    aggregate_bars(frequency)
        Will convert a set of bars to a different timeframe (eg. 1 min
        to 15 min) frequency (string): The new timeframe that the bars
        should be in, eg. “15Min”, “1H”, or “1D”. Returns a new Bars
        object.

    get_total_volume(start=None, end=None)
        Returns the total volume of the dataframe.

    get_total_dividends(start=None, end=None)
        Returns the total dividend amount of the dataframe.

    get_total_stock_splits(start=None, end=None)
        Returns the total stock split amount of the dataframe.

    get_total_return(start=None, end=None)
        Returns the total return of the dataframe.

    get_total_return_pct(start=None, end=None)
        Returns the total return percentage of the dataframe.

    get_total_return_pct_change(start=None, end=None)
        Returns the total return percentage change of the dataframe.

    Examples
    --------
    >>> # Get the most recent bars for AAPL
    >>> bars = bars.get_bars("AAPL")
    >>> # Get the most recent bars for AAPL between 2018-01-01 and 2018-01-02
    >>> bars = bars.get_bars("AAPL", start=datetime(2018, 1, 1), end=datetime(2018, 1, 10))
    >>> df = bars.df
    >>> self.log_message(df["close"][-1])

    >>> # Get the most recent bars for ES futures contract
    >>> asset = Asset(symbol="ES", asset_type="future", multiplier=100)
    >>> bars = bars.get_bars(asset)
    >>> df = bars.df
    >>> self.log_message(df["close"][-1])

    >>> # Get the most recent bars for Ethereum into Bitcoin.
    >>> asset = Asset(symbol="ETH", asset_type="crypto")
    >>> quote = Asset(symbol="BTC", asset_type="crypto")
    >>> bars = bars.get_bars(asset)
    >>> df = bars.df
    >>> self.log_message(df["close"][-1])
    """

    def __init__(self, df, source, asset, quote=None, raw=None):
        """
        df columns: open, high, low, close, volume, dividend, stock_splits
        df index: pd.Timestamp localized at the timezone America/New_York
        """
        if df.shape[0] == 0:
            logging.warning(f"Unable to get bar data for {asset} {source}")

        self.source = source.upper()
        self.asset = asset
        if isinstance(asset, tuple):
            self.symbol = f"{asset[0].symbol}/{asset[1].symbol}".upper()
        else:
            self.symbol = asset.symbol.upper()
        self.quote = quote
        self._raw = raw

        if "dividend" in df.columns:
            df.loc[:, "price_change"] = df["close"].pct_change(fill_method=None)
            df.loc[:, "dividend_yield"] = df["dividend"] / df["close"]
            df.loc[:, "return"] = df["dividend_yield"] + df["price_change"]
        else:
            df = df.assign(return_=df["close"].pct_change(fill_method=None))
            df.rename(columns={"return_": "return"}, inplace=True)

        self.df = df

    def __repr__(self):
        return repr(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()
    
    def __len__(self):
        """Return the number of bars (rows) in the DataFrame"""
        return len(self.df)

    @classmethod
    def parse_bar_list(cls, bar_list, source, asset):
        raw = []
        for bar in bar_list:
            raw.append(bar)

        df = pd.DataFrame(raw)
        df = df.set_index("timestamp")
        df["price_change"] = df["close"].pct_change()
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = cls(df, source, asset, raw=bar_list)
        return bars

    def split(self):
        """Return a list of Bars objects, each with a single bar

        Parameters
        ----------
        None

        Returns
        -------
        list of Bars objects
        """
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

    def get_last_price(self) -> Union[float, Decimal, None]:
        """Return the last price of the last bar

        Parameters
        ----------
        None

        Returns
        -------
        float, Decimal or None

        """
        return self.df["close"].iloc[-1]

    def get_last_dividend(self):
        """Return the last dividend of the last bar

        Parameters
        ----------
        None

        Returns
        -------
        float
        """
        if "dividend" in self.df.columns:
            return self.df["dividend"].iloc[-1]
        else:
            logging.debug("Unable to find 'dividend' column in bars")
            return 0

    def filter(self, start=None, end=None):
        """Return a Bars object with only the bars between start and end

        Parameters
        ----------
        start : datetime.datetime
            The start of the range to filter on

        end : datetime.datetime
            The end of the range to filter on

        Returns
        -------
        Bars object
        """
        df_copy = self.df
        if isinstance(start, datetime):
            df_copy = df_copy[df_copy.index >= start]
        if isinstance(end, datetime):
            df_copy = df_copy[df_copy.index <= end]

        return df_copy

    def get_momentum(self, num_periods: int = 1):
        """
        Calculate the momentum of the asset over the last num_periods rows. If dividends are provided by the data source,
        and included in the return calculation, the momentum will be adjusted for dividends.
        """
        df_copy = self.df.copy()
        if "return" in df_copy.columns:
            period_adj_returns = df_copy['return'].iloc[-num_periods:]
            momentum = (1 + period_adj_returns).cumprod().iloc[-1] - 1
        else:
            momentum = df_copy['close'].pct_change(num_periods).iloc[-1]
        return momentum

    def get_total_volume(self, start=None, end=None):
        """Return the total volume of the bars between start and end

        Parameters
        ----------
        start : datetime.datetime
            The start of the range to filter on (inclusive) (default: None)

        end : datetime.datetime
            The end of the range to filter on (inclusive) (default: None)

        Returns
        -------
        float
        """
        df_copy = self.filter(start=start, end=end)
        n_rows = df_copy.shape[0]
        if n_rows == 0:
            return 0

        volume = df_copy["volume"].sum()
        return volume

    def aggregate_bars(self, frequency, **grouper_kwargs):
        """
        Will convert a set of bars to a different timeframe (eg. 1 min to 15 min)
        frequency (string): The new timeframe that the bars should be in, eg. "15Min", "1H", or "1D"
        Returns a new bars object.

        Parameters
        ----------
        frequency : str
            The new timeframe that the bars should be in, eg. "15Min", "1H", or "1D"

        Returns
        -------
        Bars object

        Examples
        --------
        >>> # Get the 15 minute bars for the last hour
        >>> bars = self.get_historical_prices("AAPL", 60, "minute")
        >>> bars_agg = bars.aggregate_bars("15Min")
        """
        new_df = self.df.groupby(pd.Grouper(freq=frequency, **grouper_kwargs)).agg(
            {
                "open": "first",
                "close": "last",
                "low": "min",
                "high": "max",
                "volume": "sum",
            }
        )
        new_df.columns = ["open", "close", "low", "high", "volume"]
        new_df = new_df.dropna()

        new_bars = Bars(new_df, self.source, self.asset)

        return new_bars


class NoBarDataFound(Exception):
    def __init__(self, source, asset):
        message = (
            f"{source} did not return data for symbol {asset}. "
            f"Make sure there is no symbol typo or use another data source"
        )
        super(NoBarDataFound, self).__init__(message)

    def aggregate_bars(self, frequency):
        """
        Will convert a set of bars to a different timeframe (eg. 1 min to 15 min)
        frequency (string): The new timeframe that the bars should be in, eg. "15Min", "1H", or "1D"
        Returns a new bars object.
        """
        new_df = self.df.groupby(pd.Grouper(freq=frequency)).agg(
            {
                "open": "first",
                "close": "last",
                "low": "min",
                "high": "max",
                "volume": "sum",
            }
        )
        new_df.columns = ["open", "close", "low", "high", "volume"]
        new_df = new_df.dropna()

        new_bars = Bars(new_df, self.source, self.asset)

        return new_bars


class NoBarDataFound(Exception):
    def __init__(self, source, asset):
        message = (
            f"{source} did not return data for symbol {asset}. "
            f"Make sure there is no symbol typo or use another data source"
        )
        super(NoBarDataFound, self).__init__(message)
