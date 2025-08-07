from datetime import time, timedelta
from decimal import Decimal
from typing import Union, Optional

import polars as pl
import numpy as np
import pandas as pd


def day_deduplicate(df_: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicate dates, keeping only the first occurrence."""
    # Get datetime column (first column or one named datetime/date)
    dt_col = _get_datetime_column(df_)
    return df_.unique(subset=[dt_col], keep="first")


def is_daily_data(df_: pl.DataFrame) -> bool:
    """Check if the DataFrame contains daily data (all times are 00:00)."""
    dt_col = _get_datetime_column(df_)
    
    # Extract time component and check if all are midnight
    times = df_.select(pl.col(dt_col).dt.time()).unique()
    
    if len(times) == 1:
        # Get the single time value
        time_val = times.item(0, 0)
        if time_val == time(0, 0):
            return True
    return False


def fill_void(df_: pl.DataFrame, interval: timedelta, end) -> pl.DataFrame:
    """Fill missing time intervals in the DataFrame using native polars operations."""
    dt_col = _get_datetime_column(df_)
    
    # Get the datetime column as a series
    dt_series = df_[dt_col].sort()
    
    # Find gaps in the time series
    gaps = []
    dt_values = dt_series.to_list()
    
    for i in range(len(dt_values) - 1):
        current = dt_values[i]
        next_val = dt_values[i + 1]
        expected_next = current + interval
        
        # If there's a gap
        if next_val > expected_next:
            # Calculate number of missing intervals
            n_missing = int((next_val - current) / interval) - 1
            
            # Get the row data for forward filling
            row_data = df_.filter(pl.col(dt_col) == current)
            
            # Create missing timestamps
            for j in range(1, n_missing + 1):
                new_timestamp = current + (interval * j)
                # Clone the row with new timestamp
                new_row = row_data.with_columns(pl.lit(new_timestamp).alias(dt_col))
                gaps.append(new_row)
    
    # Handle gap at the end if needed
    last_dt = dt_values[-1]
    if last_dt < end:
        n_missing = int((end - last_dt) / interval)
        row_data = df_.filter(pl.col(dt_col) == last_dt)
        
        for j in range(1, n_missing + 1):
            new_timestamp = last_dt + (interval * j)
            new_row = row_data.with_columns(pl.lit(new_timestamp).alias(dt_col))
            gaps.append(new_row)
    
    # Combine original data with gaps
    if gaps:
        all_dfs = [df_] + gaps
        result = pl.concat(all_dfs).sort(dt_col)
        return result
    
    return df_


def print_full_polars_dataframes():
    """Show the whole dataframe when printing polars dataframes."""
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_width_chars(1000)
    pl.Config.set_fmt_str_lengths(1000)


def set_polars_float_display_precision(precision: int = 5):
    """Set the float display precision for polars dataframes."""
    pl.Config.set_float_precision(precision)


def prettify_dataframe_with_decimals(df: pl.DataFrame, decimal_places: int = 5) -> str:
    """Format DataFrame with specified decimal places."""
    # Use polars native formatting
    with pl.Config(float_precision=decimal_places):
        return str(df)


def pandas_to_polars(df_pandas: pd.DataFrame) -> pl.DataFrame:
    """Convert pandas DataFrame to polars DataFrame, handling datetime index."""
    if isinstance(df_pandas.index, pd.DatetimeIndex):
        # Reset index to make it a column
        df_reset = df_pandas.reset_index()
        # Ensure the index column has a name
        if df_reset.columns[0] == 'index':
            df_reset = df_reset.rename(columns={'index': 'datetime'})
        return pl.from_pandas(df_reset)
    else:
        return pl.from_pandas(df_pandas)


def polars_to_pandas(df_polars: pl.DataFrame, index_col: Optional[str] = None) -> pd.DataFrame:
    """Convert polars DataFrame to pandas DataFrame, optionally setting index."""
    df_pandas = df_polars.to_pandas()
    
    # If no index column specified, try to find datetime column
    if index_col is None:
        dt_col = _find_datetime_column(df_polars)
        if dt_col:
            index_col = dt_col
    
    if index_col and index_col in df_pandas.columns:
        df_pandas = df_pandas.set_index(index_col)
        df_pandas.index.name = index_col
    
    return df_pandas


def _get_datetime_column(df: pl.DataFrame) -> str:
    """Get the name of the datetime column in a polars DataFrame."""
    # First check for columns with datetime dtype
    for col in df.columns:
        if df[col].dtype in [pl.Datetime, pl.Date]:
            return col
    
    # Then check for common datetime column names
    datetime_names = ['datetime', 'date', 'time', 'timestamp', 'Datetime', 'Date', 'Time', 'Timestamp']
    for col in df.columns:
        if col in datetime_names:
            return col
    
    # Default to first column
    return df.columns[0]


def _find_datetime_column(df: pl.DataFrame) -> Optional[str]:
    """Find a datetime column in the DataFrame, return None if not found."""
    # Check for datetime dtype columns
    for col in df.columns:
        if df[col].dtype in [pl.Datetime, pl.Date]:
            return col
    
    # Check for common names
    datetime_names = ['datetime', 'date', 'time', 'timestamp']
    for col in df.columns:
        if col.lower() in datetime_names:
            return col
    
    return None


# Unified interface functions that work with both pandas and polars
def is_dataframe(obj) -> bool:
    """Check if object is either a pandas or polars DataFrame."""
    return isinstance(obj, (pd.DataFrame, pl.DataFrame))


def get_dataframe_backend(df) -> str:
    """Get the backend type of a dataframe ('pandas' or 'polars')."""
    if isinstance(df, pd.DataFrame):
        return 'pandas'
    elif isinstance(df, pl.DataFrame):
        return 'polars'
    else:
        raise TypeError(f"Unknown dataframe type: {type(df)}")


def ensure_polars(df) -> pl.DataFrame:
    """Ensure the dataframe is a polars DataFrame, converting if necessary."""
    if isinstance(df, pl.DataFrame):
        return df
    elif isinstance(df, pd.DataFrame):
        return pandas_to_polars(df)
    else:
        raise TypeError(f"Expected DataFrame, got {type(df)}")


def ensure_pandas(df) -> pd.DataFrame:
    """Ensure the dataframe is a pandas DataFrame, converting if necessary."""
    if isinstance(df, pd.DataFrame):
        return df
    elif isinstance(df, pl.DataFrame):
        return polars_to_pandas(df)
    else:
        raise TypeError(f"Expected DataFrame, got {type(df)}")