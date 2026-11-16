"""fin-primitives backend for ``self.indicators`` (lumibot/indicators/fast_backend.py).

``sma``, ``ema``, ``rsi``, ``macd``, ``bbands`` and ``atr`` in their plain form
run on ``fin_primitives.fast`` instead of pandas-ta-classic. These tests pin the
contract: the result has the same type, index, name and column names as the
pandas-ta-classic call, the same NaN warm-up rows, and the same values (to 1e-12;
on the benchmark data sma/ema/rsi/macd/atr are bit for bit equal and bbands
differs by at most 6.4e-16 relative). Every other form keeps the pandas-ta path.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lumibot.indicators import fast_backend
from lumibot.indicators.indicators import Indicators, _get_ta_module

pytest.importorskip("fin_primitives.fast")


def _bars(n=2_000, *, seed=11, flat=False):
    """Random-walk OHLCV rounded to cents, like real equity bars."""
    rng = np.random.default_rng(seed)
    close = np.full(n, 50.0) if flat else 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    spread = 0 if flat else 1
    high = close * (1 + spread * np.abs(rng.normal(0, 0.003, n)))
    low = close * (1 - spread * np.abs(rng.normal(0, 0.003, n)))
    index = pd.date_range("2024-01-02", periods=n, freq="min", tz="UTC")
    return pd.DataFrame(
        {"open": close, "high": high, "low": low, "close": close, "volume": 1_000.0}, index=index
    ).round(2)


def _pandas_ta(df, name, kwargs):
    args = {col: df[col] for col in ("open", "high", "low", "close", "volume")}
    args.update(kwargs)
    return getattr(_get_ta_module(), name)(**args)


CASES = [
    ("sma", {}),
    ("sma", {"length": 1}),
    ("sma", {"length": 200}),
    ("ema", {}),
    ("ema", {"length": 1}),
    ("ema", {"length": 55}),
    ("rsi", {}),
    ("rsi", {"length": 2}),
    ("rsi", {"length": 20}),
    ("atr", {}),
    ("atr", {"length": 1}),
    ("atr", {"length": 30}),
    ("macd", {}),
    ("macd", {"fast": 26, "slow": 12}),  # pandas-ta swaps fast/slow, names included
    ("macd", {"fast": 5, "slow": 35, "signal": 5}),
    ("bbands", {}),
    ("bbands", {"length": 20, "std": 2}),  # int std is named "2.0" like pandas-ta
    ("bbands", {"length": 2, "std": 1.5}),
    ("sma", {"length": 20, "offset": 0}),
]


def _assert_same(new, old):
    assert type(new) is type(old)
    if isinstance(old, pd.Series):
        assert new.name == old.name
        pd.testing.assert_series_equal(new, old, check_exact=False, rtol=1e-12, atol=1e-12)
    else:
        assert list(new.columns) == list(old.columns)
        assert new.name == old.name
        pd.testing.assert_frame_equal(new, old, check_exact=False, rtol=1e-12, atol=1e-12)
    assert new.category == old.category


@pytest.mark.parametrize("flat", [False, True], ids=["random_walk", "flat"])
@pytest.mark.parametrize("name,kwargs", CASES)
def test_fast_backend_matches_pandas_ta_classic(name, kwargs, flat):
    df = _bars(flat=flat)
    new = fast_backend.compute(df, name, kwargs)
    assert new is not None, "plain calls must take the fin-primitives path"
    _assert_same(new, _pandas_ta(df, name, kwargs))


@pytest.mark.parametrize("n", [14, 15, 26, 33, 34, 35])
@pytest.mark.parametrize("name", ["rsi", "atr", "macd", "bbands", "ema", "sma"])
def test_fast_backend_matches_pandas_ta_at_warmup_boundaries(name, n):
    df = _bars(n, seed=n)
    new = fast_backend.compute(df, name, {})
    if new is None:  # shorter than the period: pandas-ta handles it as before
        return
    _assert_same(new, _pandas_ta(df, name, {}))


@pytest.mark.parametrize(
    "name,kwargs",
    [
        ("sma", {"length": 20, "talib": False}),
        ("sma", {"length": 20, "offset": 1}),
        ("sma", {"length": 20, "min_periods": 5}),
        ("ema", {"length": 20, "adjust": True}),
        ("rsi", {"length": 14, "scalar": 50}),
        ("rsi", {"length": 14, "signal_indicators": True}),
        ("atr", {"length": 14, "mamode": "sma"}),
        ("atr", {"length": 14, "percent": True}),
        ("bbands", {"length": 20, "ddof": 1}),
        ("macd", {"asmode": True}),
        ("sma", {"length": 20.0}),
        ("sma", {"length": 5_000}),  # longer than the data
        ("stoch", {}),  # not handled by the backend at all
    ],
)
def test_other_forms_fall_back_to_pandas_ta(name, kwargs):
    assert fast_backend.compute(_bars(500), name, kwargs) is None


def test_missing_values_fall_back_to_pandas_ta():
    df = _bars(300)
    df.iloc[100, df.columns.get_loc("close")] = np.nan
    assert fast_backend.compute(df, "rsi", {"length": 14}) is None


def test_missing_fin_primitives_falls_back_to_pandas_ta(monkeypatch):
    monkeypatch.setattr(fast_backend, "_fast", None)
    assert fast_backend.compute(_bars(300), "rsi", {"length": 14}) is None


class _Strategy:
    def __init__(self, now):
        self.now = now

    def get_datetime(self):
        return self.now


def _accessor(df):
    strategy = _Strategy(df.index[-1])
    accessor = Indicators(strategy)
    accessor._full_history = lambda asset, timestep, quote=None, exchange=None, required_length=None: df
    accessor._record_consumed_input = lambda *a, **k: None
    return strategy, accessor


@pytest.mark.parametrize("name,kwargs", [(n, k) for n, k in CASES if n in ("sma", "ema", "rsi", "atr")])
def test_strategy_scalar_values_match_pandas_ta(name, kwargs):
    df = _bars(1_500)
    strategy, accessor = _accessor(df)
    expected = _pandas_ta(df, name, kwargs)
    for stamp in (df.index[300], df.index[-1]):
        strategy.now = stamp
        value = getattr(accessor, name)("X", **kwargs)
        assert value == pytest.approx(expected.loc[stamp], rel=1e-12, abs=1e-12)


@pytest.mark.parametrize("name,kwargs", [("macd", {}), ("bbands", {"length": 20, "std": 2})])
def test_strategy_row_columns_match_pandas_ta(name, kwargs):
    df = _bars(1_500)
    strategy, accessor = _accessor(df)
    row = getattr(accessor, name)("X", **kwargs)
    expected = _pandas_ta(df, name, kwargs).iloc[-1]
    assert dict(row.as_dict()) == pytest.approx(dict(expected), rel=1e-12, abs=1e-12)
    assert list(row.as_dict()) == list(expected.index)
