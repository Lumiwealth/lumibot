from datetime import datetime
from decimal import Decimal
from typing import Union, Set
import warnings
import atexit

import numpy as np
import pandas as pd
import polars as pl

from lumibot.tools.lumibot_logger import get_logger

from .bar import Bar

logger = get_logger(__name__)


class PolarsConversionTracker:
    """Track Polars to Pandas conversions and report them efficiently."""
    _instance = None
    _warned_assets: Set[str] = set()
    _total_conversions: int = 0
    _first_warning_shown: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Register cleanup function to show summary
            atexit.register(cls._instance.show_summary)
        return cls._instance
    
    def track_conversion(self, asset_symbol: str):
        """Track a conversion and show warning if needed."""
        self._total_conversions += 1
        
        # Show warning on first encounter
        if not self._first_warning_shown:
            logger.warning(
                "\n" + "="*70 + "\n"
                "PERFORMANCE TIP: DataFrame Conversion Detected\n" + 
                "="*70 + "\n"
                "Polars DataFrames are being converted to Pandas, which adds overhead.\n"
                "\n"
                "For ~2-5x faster backtesting, modify your strategy:\n"
                "\n"
                "  Change: bars = self.get_historical_prices(asset, length, timestep)\n"
                "      To: bars = self.get_historical_prices(asset, length, timestep, return_polars=True)\n"
                "\n"
                "Note: When using return_polars=True, use Polars DataFrame methods instead of Pandas.\n" +
                "="*70
            )
            self._first_warning_shown = True
        
        # Track which assets have been converted
        if asset_symbol not in self._warned_assets:
            self._warned_assets.add(asset_symbol)
    
    def show_summary(self):
        """Show summary at the end if there were conversions."""
        if self._total_conversions > 0:
            unique_assets = len(self._warned_assets)
            assets_list = list(self._warned_assets)[:5]  # Show first 5 assets
            assets_str = ", ".join(assets_list)
            if unique_assets > 5:
                assets_str += f", ... ({unique_assets - 5} more)"
            
            logger.warning(
                f"\n" + "="*70 + "\n"
                f"BACKTEST PERFORMANCE SUMMARY\n" + 
                "="*70 + "\n"
                f"Total DataFrame conversions: {self._total_conversions}\n"
                f"Unique assets affected: {unique_assets} [{assets_str}]\n"
                f"\n"
                f"Estimated performance impact: ~{self._total_conversions * 10}-{self._total_conversions * 50}ms added overhead\n"
                f"\n"
                f"To speed up your backtest, add return_polars=True to get_historical_prices()\n" +
                "="*70
            )
    
    @classmethod
    def reset(cls):
        """Reset the tracker (useful for testing)."""
        if cls._instance:
            cls._instance._warned_assets.clear()
            cls._instance._total_conversions = 0
            cls._instance._first_warning_shown = False


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

    def __init__(self, df, source, asset, quote=None, raw=None, return_polars=False):
        """
        df columns: open, high, low, close, volume, dividend, stock_splits
        datetime column for polars DataFrames
        return_polars: if True, keep as polars DataFrame, otherwise convert to pandas
        """
        self.source = source.upper()
        self.asset = asset
        if isinstance(asset, tuple):
            self.symbol = f"{asset[0].symbol}/{asset[1].symbol}".upper()
        else:
            self.symbol = asset.symbol.upper()
        self.quote = quote
        self._raw = raw
        self._return_polars = return_polars
        # Cache for on-demand conversions to avoid repeated expensive copies
        self._polars_cache = None
        
        # Check if empty
        if (isinstance(df, pl.DataFrame) and df.shape[0] == 0) or \
           (isinstance(df, pd.DataFrame) and df.shape[0] == 0):
            logger.warning(f"Unable to get bar data for {asset} {source}")
        
        if isinstance(df, pl.DataFrame):
            # Already polars, process it
            columns = df.columns
            
            # Calculate derived columns using polars
            if "dividend" in columns:
                df = df.with_columns([
                    pl.col("close").pct_change().alias("price_change"),
                    (pl.col("dividend") / pl.col("close")).alias("dividend_yield"),
                    ((pl.col("dividend") / pl.col("close")) + pl.col("close").pct_change()).alias("return")
                ])
            else:
                df = df.with_columns([
                    pl.col("close").pct_change().alias("return")
                ])
            
            if return_polars:
                # Keep as polars
                self._df = df
            else:
                # Convert to pandas and track the conversion
                tracker = PolarsConversionTracker()
                tracker.track_conversion(asset.symbol if hasattr(asset, 'symbol') else str(asset))
                
                self._df = df.to_pandas()
                # Set datetime index if exists
                for col_name in ['datetime', 'timestamp', 'date', 'time']:
                    if col_name in self._df.columns:
                        self._df = self._df.set_index(col_name)
                        break
        else:
            # Already pandas, keep it as is
            self._df = df
            # Calculate derived columns if needed
            if "dividend" in df.columns:
                self._df["price_change"] = df["close"].pct_change()
                self._df["dividend_yield"] = df["dividend"] / df["close"]
                self._df["return"] = self._df["dividend_yield"] + self._df["price_change"]
            else:
                self._df["return"] = df["close"].pct_change()

    @property
    def df(self):
        """Return the DataFrame"""
        return self._df

    @df.setter
    def df(self, value):
        """Allow setting the DataFrame"""
        self._df = value
        # Invalidate cached converted forms when df changes
        self._polars_cache = None
    
    @property
    def polars_df(self):
        """Return as Polars DataFrame if needed"""
        if isinstance(self._df, pl.DataFrame):
            return self._df
        else:
            # Convert pandas to polars once and cache
            if self._polars_cache is not None:
                return self._polars_cache
            if hasattr(self._df, 'index') and getattr(self._df.index, 'name', None):
                self._polars_cache = pl.from_pandas(self._df.reset_index())
            else:
                self._polars_cache = pl.from_pandas(self._df)
            return self._polars_cache

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
        if isinstance(self._df, pl.DataFrame):
            return self._df.is_empty()
        else:
            return self._df.empty

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
        # Use appropriate DataFrame for iteration
        if isinstance(self._df, pl.DataFrame):
            underlying_df = self._df
        else:
            underlying_df = pl.from_pandas(self._df.reset_index() if hasattr(self._df, 'index') else self._df)
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
        df_copy = self.polars_df
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
            underlying_df = self.polars_df
            period_adj_returns = underlying_df['return'].tail(num_periods)
            momentum = float((1 + period_adj_returns).product() - 1)
        else:
            # Calculate momentum directly
            underlying_df = self.polars_df
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
        underlying_df = self.polars_df
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
