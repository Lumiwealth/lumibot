from datetime import datetime
from decimal import Decimal
from typing import Union

import numpy as np
import polars as pl

from lumibot.tools.lumibot_logger import get_logger

from .bar import Bar
from .polars_dataframe_wrapper import PolarsDataFrameWrapper

logger = get_logger(__name__)


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
        datetime column for polars DataFrames
        """
        # Convert pandas to polars if needed
        if not isinstance(df, pl.DataFrame):
            # This is a pandas DataFrame, convert it
            if hasattr(df, 'index') and hasattr(df.index, 'name'):
                df = pl.from_pandas(df.reset_index())
            else:
                df = pl.from_pandas(df)

        if df.shape[0] == 0:
            logger.warning(f"Unable to get bar data for {asset} {source}")

        self.source = source.upper()
        self.asset = asset
        if isinstance(asset, tuple):
            self.symbol = f"{asset[0].symbol}/{asset[1].symbol}".upper()
        else:
            self.symbol = asset.symbol.upper()
        self.quote = quote
        self._raw = raw

        # Polars implementation - use native operations
        columns = df.columns

        # Use polars native operations for maximum efficiency
        if "dividend" in columns:
            # Calculate all derived columns in one operation
            df = df.with_columns([
                pl.col("close").pct_change().alias("price_change"),
                (pl.col("dividend") / pl.col("close")).alias("dividend_yield"),
                ((pl.col("dividend") / pl.col("close")) + pl.col("close").pct_change()).alias("return")
            ])
        else:
            # Just price returns
            df = df.with_columns([
                pl.col("close").pct_change().alias("return")
            ])

        # Store the polars DataFrame internally
        self._polars_df = df
        # Create wrapper for compatibility
        self._df_wrapper = PolarsDataFrameWrapper(df)
        # Convert to pandas for full compatibility with strategies
        self._pandas_df = None  # Lazy conversion

    @property
    def df(self):
        """Return the DataFrame - convert to pandas for compatibility"""
        if self._pandas_df is None:
            # Lazy conversion to pandas
            self._pandas_df = self._polars_df.to_pandas()
            # Debug logging
            logger.debug(f"[Bars.df] Converted Polars to Pandas. Columns: {self._pandas_df.columns.tolist()}")
            # Set datetime-like column as index if it exists
            # Check for common datetime column names
            for col_name in ['datetime', 'timestamp', 'date', 'time']:
                if col_name in self._pandas_df.columns:
                    logger.debug(f"[Bars.df] Setting {col_name} as index")
                    self._pandas_df = self._pandas_df.set_index(col_name)
                    break
            else:
                logger.debug("[Bars.df] No datetime-like column found to set as index!")
        return self._pandas_df

    @df.setter
    def df(self, value):
        """Allow setting the DataFrame"""
        if isinstance(value, pl.DataFrame):
            self._polars_df = value
            self._pandas_df = None  # Reset pandas cache
        else:
            self._pandas_df = value
            self._polars_df = None  # Reset polars cache

    def __repr__(self):
        return repr(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()

    def __len__(self):
        """Return the number of bars (rows) in the DataFrame"""
        return len(self.df)

    @property
    def empty(self):
        """Check if the DataFrame is empty (compatible with both pandas and polars)"""
        if self._polars_df is not None:
            return self._polars_df.is_empty()
        elif self._pandas_df is not None:
            return self._pandas_df.empty
        return True

    @classmethod
    def parse_bar_list(cls, bar_list, source, asset):
        raw = []
        for bar in bar_list:
            raw.append(bar)

        # Create polars DataFrame directly
        df = pl.DataFrame(raw)

        # Calculate derived columns using polars
        df = df.with_columns([
            pl.col("close").pct_change().alias("price_change"),
            (pl.col("dividend") / pl.col("close")).alias("dividend_yield"),
        ])

        # Calculate return
        df = df.with_columns([
            (pl.col("dividend_yield") + pl.col("price_change")).alias("return")
        ])

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
        # Use polars DataFrame for iteration
        underlying_df = self._polars_df if self._polars_df is not None else pl.from_pandas(self.df)
        # Polars implementation
        for row in underlying_df.iter_rows(named=True):
            # Find datetime column
            dt_val = None
            for col in underlying_df.columns:
                if underlying_df[col].dtype in [pl.Datetime, pl.Date]:
                    dt_val = row[col]
                    break

            if dt_val is None:
                # Try to find a column with date/time in the name
                for col in ['datetime', 'date', 'timestamp']:
                    if col in row:
                        dt_val = row[col]
                        break

            timestamp = int(dt_val.timestamp()) if hasattr(dt_val, 'timestamp') else int(dt_val.timestamp())

            item = {
                "timestamp": timestamp,
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
        return self.df["close"][-1]

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
            return self.df["dividend"][-1]
        else:
            logger.debug("Unable to find 'dividend' column in bars")
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
        # Get polars DataFrame for operations
        df_copy = self._polars_df if self._polars_df is not None else pl.from_pandas(self.df)
        # Find datetime column
        dt_col = None
        for col in df_copy.columns:
            if df_copy[col].dtype in [pl.Datetime, pl.Date]:
                dt_col = col
                break

        if dt_col is None:
            # Try common datetime column names
            for col in ['datetime', 'date', 'timestamp']:
                if col in df_copy.columns:
                    dt_col = col
                    break

        if dt_col:
            if isinstance(start, datetime):
                df_copy = df_copy.filter(pl.col(dt_col) >= start)
            if isinstance(end, datetime):
                df_copy = df_copy.filter(pl.col(dt_col) <= end)

        return df_copy

    def get_momentum(self, num_periods: int = 1):
        """
        Calculate the momentum of the asset over the last num_periods rows. If dividends are provided by the data source,
        and included in the return calculation, the momentum will be adjusted for dividends.
        """
        if "return" in self.df.columns:
            # Use existing return column
            underlying_df = self._polars_df if self._polars_df is not None else pl.from_pandas(self.df)
            period_adj_returns = underlying_df['return'].tail(num_periods)
            momentum = float((1 + period_adj_returns).product() - 1)
        else:
            # Calculate momentum directly
            underlying_df = self._polars_df if self._polars_df is not None else pl.from_pandas(self.df)
            close_values = underlying_df['close'].to_numpy()
            if len(close_values) > num_periods:
                momentum = (close_values[-1] / close_values[-num_periods-1]) - 1
            else:
                momentum = np.nan
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
        # Get polars DataFrame
        underlying_df = self._polars_df if self._polars_df is not None else pl.from_pandas(self.df)
        # Find datetime column
        dt_col = None
        for col in underlying_df.columns:
            if underlying_df[col].dtype in [pl.Datetime, pl.Date]:
                dt_col = col
                break

        if dt_col is None:
            # Try common datetime column names
            for col in ['datetime', 'date', 'timestamp']:
                if col in underlying_df.columns:
                    dt_col = col
                    break

        if dt_col is None:
            raise ValueError("No datetime column found in DataFrame")

        # Convert frequency string to polars duration
        freq_map = {
            "15Min": "15m", "15m": "15m", "15T": "15m",
            "1H": "1h", "1h": "1h", "H": "1h",
            "1D": "1d", "1d": "1d", "D": "1d",
            "5Min": "5m", "5m": "5m", "5T": "5m",
            "30Min": "30m", "30m": "30m", "30T": "30m",
        }

        polars_freq = freq_map.get(frequency, frequency)

        # Group by time intervals and aggregate
        new_df = (
            underlying_df
            .sort(dt_col)
            .group_by_dynamic(
                dt_col,
                every=polars_freq,
                closed="left",
                **grouper_kwargs
            )
            .agg([
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
            ])
        )

        # Drop null rows
        new_df = new_df.drop_nulls()

        new_bars = Bars(new_df, self.source, self.asset)

        return new_bars


class NoBarDataFound(Exception):
    def __init__(self, source, asset):
        message = (
            f"{source} did not return data for symbol {asset}. "
            f"Make sure there is no symbol typo or use another data source"
        )
        super(NoBarDataFound, self).__init__(message)
