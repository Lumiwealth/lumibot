from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.tools.indicators import cagr, stats_summary, volatility


def _make_returns_df(index: pd.Index, returns: list[float] | None = None) -> pd.DataFrame:
    if returns is None:
        returns = [0.0, 1.0]
    return pd.DataFrame({"return": returns}, index=index)


def _assert_stats_summary_shape(result: dict) -> None:
    def _is_number(value: object) -> bool:
        return isinstance(value, (int, float))

    assert set(result) == {"cagr", "volatility", "sharpe", "max_drawdown", "romad", "total_return"}
    assert _is_number(result["cagr"])
    assert _is_number(result["volatility"])
    assert _is_number(result["sharpe"])
    assert _is_number(result["romad"])
    assert _is_number(result["total_return"])
    assert isinstance(result["max_drawdown"], dict)
    assert set(result["max_drawdown"]) == {"drawdown", "date"}


def _expected_cagr(start: pd.Timestamp, end: pd.Timestamp, total_return: float) -> float:
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    if end.tzinfo is None:
        end = end.tz_localize("UTC")
    period_years = (end - start).days / 365.25
    return (1 + total_return) ** (1 / period_years) - 1


def test_cagr_supports_datetime64_us_index():
    """Regression: numpy datetime64[us] -> .astype('O') yields datetime.datetime (no / support)."""
    arr_us = np.array(
        ["2026-01-01T00:00:00.000000", "2027-01-01T00:00:00.000000"],
        dtype="datetime64[us]",
    )
    df = _make_returns_df(pd.Index(arr_us))

    value = cagr(df)
    assert isinstance(value, float)
    assert value != 0
    assert abs(value - _expected_cagr(pd.Timestamp("2026-01-01"), pd.Timestamp("2027-01-01"), total_return=1.0)) < 1e-12


def test_cagr_supports_datetime64_s_index():
    """Regression: datetime64[s] should not crash CAGR/volatility computations."""
    arr_s = np.array(["2026-01-01T00:00:00", "2027-01-01T00:00:00"], dtype="datetime64[s]")
    df = _make_returns_df(pd.Index(arr_s))

    value = cagr(df)
    assert isinstance(value, float)
    assert value != 0
    assert abs(value - _expected_cagr(pd.Timestamp("2026-01-01"), pd.Timestamp("2027-01-01"), total_return=1.0)) < 1e-12


def test_volatility_supports_datetime64_us_index():
    arr_us = np.array(
        ["2026-01-01T00:00:00.000000", "2027-01-01T00:00:00.000000"],
        dtype="datetime64[us]",
    )
    df = _make_returns_df(pd.Index(arr_us))

    value = volatility(df)
    assert isinstance(value, float)
    assert value != 0


def test_stats_summary_supports_datetime64_us_index():
    arr_us = np.array(
        ["2026-01-01T00:00:00.000000", "2027-01-01T00:00:00.000000"],
        dtype="datetime64[us]",
    )
    df = _make_returns_df(pd.Index(arr_us))

    result = stats_summary(df, risk_free_rate=0.0)
    _assert_stats_summary_shape(result)
    assert result["cagr"] != 0


@pytest.mark.parametrize(
    "index",
    [
        pd.date_range("2026-01-01", periods=4, freq="1D"),
        pd.date_range("2026-01-01", periods=4, freq="1D", tz="UTC"),
        pd.Index(
            [
                datetime(2026, 1, 1),
                datetime(2026, 1, 2),
                datetime(2026, 1, 3),
                datetime(2026, 1, 4),
            ],
            dtype="object",
        ),
        pd.Index(
            [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 2, tzinfo=UTC),
                datetime(2026, 1, 3, tzinfo=UTC),
                datetime(2026, 1, 4, tzinfo=UTC),
            ],
            dtype="object",
        ),
        pd.Index(np.array(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"], dtype="datetime64[ns]")),
    ],
    ids=[
        "DatetimeIndex_naive",
        "DatetimeIndex_utc",
        "ObjectIndex_py_datetime_naive",
        "ObjectIndex_py_datetime_utc",
        "Index_datetime64_ns",
    ],
)
def test_stats_summary_and_max_drawdown_handle_common_datetime_indexes(index: pd.Index):
    """Broad regression: end-of-backtest stats should not depend on a single datetime index dtype."""
    returns = [0.0, 0.5, -0.5, 0.25]
    df = _make_returns_df(index, returns=returns)

    result = stats_summary(df, risk_free_rate=0.0)
    _assert_stats_summary_shape(result)

    assert result["cagr"] != 0
    assert result["volatility"] != 0

    drawdown = result["max_drawdown"]["drawdown"]
    assert isinstance(drawdown, float)
    assert 0 <= drawdown <= 1
    assert pd.Timestamp(result["max_drawdown"]["date"]) == pd.Timestamp(index[2])
