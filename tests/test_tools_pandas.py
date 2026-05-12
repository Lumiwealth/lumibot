from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd

from lumibot.tools.pandas import day_deduplicate, fill_void, is_daily_data, prettify_dataframe_with_decimals


def test_day_deduplicate_keeps_first_row_per_index() -> None:
    index = pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03"])
    df = pd.DataFrame({"value": [1, 2, 3]}, index=index)

    result = day_deduplicate(df)

    assert result["value"].tolist() == [1, 3]


def test_is_daily_data_detects_midnight_only_index() -> None:
    daily = pd.DataFrame({"value": [1, 2]}, index=pd.to_datetime(["2024-01-02", "2024-01-03"]))
    intraday = pd.DataFrame({"value": [1, 2]}, index=pd.to_datetime(["2024-01-02 09:30", "2024-01-03 09:30"]))

    assert is_daily_data(daily) is True
    assert is_daily_data(intraday) is False


def test_fill_void_forward_fills_missing_intervals() -> None:
    index = pd.to_datetime(["2024-01-02", "2024-01-04"])
    df = pd.DataFrame({"value": [10, 40]}, index=index)

    result = fill_void(df, timedelta(days=1), pd.Timestamp("2024-01-05"))

    assert list(result.index) == list(pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]))
    assert result["value"].tolist() == [10, 10, 40, 40]


def test_prettify_dataframe_with_decimals_formats_numeric_values() -> None:
    df = pd.DataFrame({"decimal": [Decimal("1.23456")], "float": [2.34567], "label": ["x"]})

    rendered = prettify_dataframe_with_decimals(df, decimal_places=2)

    assert "1.23" in rendered
    assert "2.35" in rendered
    assert "x" in rendered
