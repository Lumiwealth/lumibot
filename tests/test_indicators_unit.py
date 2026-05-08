"""Unit tests for lumibot.indicators.Indicators.

These tests run the indicator subsystem in isolation against hand-crafted
DataFrames so we don't need a real broker/data source. The goal is to
pin down: compute-once semantics, argument-keyed memo, custom-indicator
dispatch, IndicatorRow access, and edge cases (empty input, missing
indicator name, different arg combos).
"""
from __future__ import annotations

import types
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.indicators import Indicators, IndicatorRow
import lumibot.indicators.indicators as indicators_module


def _make_strategy(df: pd.DataFrame, now):
    """Build a minimal stand-in for a Strategy that satisfies the Indicators
    contract: ``broker.data_source._data_store`` holds the asset's full-history
    DataFrame; ``get_datetime()`` returns the current bar time."""

    class _Data:
        def __init__(self, df):
            self.df = df

    class _Asset:
        def __init__(self, symbol):
            self.symbol = symbol
            self.asset_type = "stock"

        def __eq__(self, other):
            return isinstance(other, _Asset) and self.symbol == other.symbol

        def __hash__(self):
            return hash((self.symbol, self.asset_type))

    class _DataSource:
        def __init__(self, asset, data):
            self._data_store = {(asset, None): data}

    class _Broker:
        def __init__(self, data_source):
            self.data_source = data_source

    asset = _Asset("TQQQ")
    data = _Data(df)
    data_source = _DataSource(asset, data)
    broker = _Broker(data_source)

    strategy = types.SimpleNamespace()
    strategy.broker = broker

    state = {"now": now}

    def get_datetime():
        return state["now"]

    strategy.get_datetime = get_datetime
    strategy.set_now = lambda t: state.__setitem__("now", t)
    strategy.asset = asset
    return strategy


@pytest.fixture
def strategy_with_daily_bars():
    dates = pd.date_range("2024-01-02", periods=300, freq="B", tz="UTC")
    rng = np.random.default_rng(42)
    prices = 100 + np.cumsum(rng.normal(0, 1, size=len(dates)))
    df = pd.DataFrame(
        {
            "open": prices,
            "high": prices + 0.5,
            "low": prices - 0.5,
            "close": prices,
            "volume": rng.integers(1_000, 10_000, size=len(dates)),
        },
        index=dates,
    )
    return _make_strategy(df, dates[-1])


def test_indicator_backend_modules_are_lazy_until_used():
    assert type(indicators_module.np).__name__ == "_LazyModule"
    assert type(indicators_module.pd).__name__ == "_LazyModule"

    lazy_np = indicators_module._LazyModule("numpy")
    lazy_pd = indicators_module._LazyModule("pandas")
    assert object.__getattribute__(lazy_np, "_module") is None
    assert object.__getattribute__(lazy_pd, "_module") is None

    assert lazy_np.nan is np.nan
    assert lazy_pd.DataFrame is pd.DataFrame


def test_sma_returns_scalar_at_current_bar(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    val = ind.sma(strategy_with_daily_bars.asset, length=20)
    assert isinstance(val, (float, np.floating))
    assert not np.isnan(val)


def test_sma_matches_manual_calculation(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    val = ind.sma(strategy_with_daily_bars.asset, length=20)
    df = strategy_with_daily_bars.broker.data_source._data_store[
        (strategy_with_daily_bars.asset, None)
    ].df
    expected = df["close"].rolling(20).mean().iloc[-1]
    assert np.isclose(val, expected), f"indicators.sma={val}, rolling.mean={expected}"


def test_cache_hit_does_not_recompute(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    ind.sma(strategy_with_daily_bars.asset, length=20)
    assert ind.cache_size == 1
    ind.sma(strategy_with_daily_bars.asset, length=20)
    assert ind.cache_size == 1
    ind.sma(strategy_with_daily_bars.asset, length=50)
    assert ind.cache_size == 2


def test_current_bar_changes_as_strategy_time_advances(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    df = strategy_with_daily_bars.broker.data_source._data_store[
        (strategy_with_daily_bars.asset, None)
    ].df
    strategy_with_daily_bars.set_now(df.index[100])
    v100 = ind.sma(strategy_with_daily_bars.asset, length=20)
    strategy_with_daily_bars.set_now(df.index[200])
    v200 = ind.sma(strategy_with_daily_bars.asset, length=20)
    assert ind.cache_size == 1, "Same args should reuse the memo across time"
    expected_100 = df["close"].rolling(20).mean().iloc[100]
    expected_200 = df["close"].rolling(20).mean().iloc[200]
    assert np.isclose(v100, expected_100)
    assert np.isclose(v200, expected_200)


def test_multi_column_result_returns_indicator_row(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    row = ind.bbands(strategy_with_daily_bars.asset, length=20, std=2)
    assert isinstance(row, IndicatorRow)
    as_dict = row.as_dict()
    assert any("BBL" in col for col in as_dict)
    assert any("BBU" in col for col in as_dict)


def test_unknown_indicator_raises(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    with pytest.raises(AttributeError):
        ind.this_is_not_an_indicator(strategy_with_daily_bars.asset, length=10)


def test_custom_indicator_runs_once_and_caches(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    call_count = {"n": 0}

    def double_close(df, bump=0):
        call_count["n"] += 1
        return df["close"] * 2 + bump

    v1 = ind.custom("double_close", double_close, strategy_with_daily_bars.asset, bump=1.0)
    v2 = ind.custom("double_close", double_close, strategy_with_daily_bars.asset, bump=1.0)
    assert call_count["n"] == 1, "memo should short-circuit the second call"
    assert v1 == v2

    ind.custom("double_close", double_close, strategy_with_daily_bars.asset, bump=5.0)
    assert call_count["n"] == 2, "different kwargs -> distinct cache entry"


def test_custom_indicator_rejects_non_callable(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    with pytest.raises(TypeError):
        ind.custom("not_a_fn", 42, strategy_with_daily_bars.asset)


def test_custom_indicator_dataframe_returns_row(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)

    def multi_col(df):
        return pd.DataFrame(
            {"above_100": df["close"] > 100, "below_100": df["close"] <= 100},
            index=df.index,
        )

    row = ind.custom("multi_col", multi_col, strategy_with_daily_bars.asset)
    assert isinstance(row, IndicatorRow)
    assert "above_100" in row
    assert "below_100" in row


def test_invalidate_clears_single_asset(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    ind.sma(strategy_with_daily_bars.asset, length=20)
    ind.sma(strategy_with_daily_bars.asset, length=50)
    assert ind.cache_size == 2
    ind.invalidate(strategy_with_daily_bars.asset)
    assert ind.cache_size == 0


def test_invalidate_all(strategy_with_daily_bars):
    ind = Indicators(strategy_with_daily_bars)
    ind.sma(strategy_with_daily_bars.asset, length=20)
    assert ind.cache_size == 1
    ind.invalidate()
    assert ind.cache_size == 0


def test_empty_dataframe_returns_none():
    empty = pd.DataFrame({"open": [], "high": [], "low": [], "close": [], "volume": []})
    empty.index = pd.DatetimeIndex([], tz="UTC")
    strategy = _make_strategy(empty, datetime(2024, 1, 1))
    ind = Indicators(strategy)
    assert ind.sma(strategy.asset, length=20) is None


def test_time_before_first_bar_returns_nan():
    dates = pd.date_range("2024-06-01", periods=50, freq="B", tz="UTC")
    df = pd.DataFrame(
        {
            "open": np.linspace(100, 110, 50),
            "high": np.linspace(100, 110, 50) + 1,
            "low": np.linspace(100, 110, 50) - 1,
            "close": np.linspace(100, 110, 50),
            "volume": [1000] * 50,
        },
        index=dates,
    )
    strategy = _make_strategy(df, pd.Timestamp("2024-01-01", tz="UTC"))
    ind = Indicators(strategy)
    val = ind.sma(strategy.asset, length=20)
    assert val is None or (isinstance(val, float) and np.isnan(val))
