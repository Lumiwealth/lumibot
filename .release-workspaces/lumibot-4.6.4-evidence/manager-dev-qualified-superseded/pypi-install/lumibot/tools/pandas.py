from datetime import time
from decimal import Decimal

import pandas as pd
import numpy as np


def day_deduplicate(df_):
    df_copy = df_.copy()
    df_copy = df_copy.groupby(level=0).head(1)

    return df_copy


def is_daily_data(df_):
    times = pd.Series(df_.index).apply(lambda x: x.time()).unique()
    if len(times) == 1 and times[0] == time(0, 0):
        return True
    return False


def fill_void(df_, interval, end):
    n_rows = len(df_.index)
    missing_lines = pd.DataFrame()
    for index, row in df_.iterrows():
        position = df_.index.get_loc(index)
        if position + 1 == n_rows:
            if index < end:
                n_missing = (end - index) // interval
                missing_days = [index + (i + 1) * interval for i in range(n_missing)]
                missing_lines = pd.concat(
                    [missing_lines, pd.DataFrame(row.to_dict(), index=missing_days)]
                )
            break

        step = (df_.index[position + 1] - index).to_pytimedelta()
        if step != interval:
            n_missing = (step // interval) - 1
            missing_days = [index + (i + 1) * interval for i in range(n_missing)]
            missing_lines = pd.concat(
                [missing_lines, pd.DataFrame(row.to_dict(), index=missing_days)]
            )

    df_ = pd.concat([df_, missing_lines])
    df_ = df_.sort_index()
    return df_


def print_full_pandas_dataframes():
    """
    Show the whole dataframe when printing pandas dataframes
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 1000)


def set_pandas_float_display_precision(precision: int = 5):
    format_str = '{:.' + str(precision) + 'f}'
    pd.set_option('display.float_format', format_str.format)


def prettify_dataframe_with_decimals(df: pd.DataFrame, decimal_places: int = 5) -> str:
    def decimal_formatter(x):
        if isinstance(x, (Decimal, float, int, np.float64, np.int64)):
            return f"{x:.{decimal_places}f}"
        return x

    return df.to_string(formatters={col: decimal_formatter for col in df.columns})
