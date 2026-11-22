"""pandas >= 3.1 adds ``DataFrame.select``; it must not be mistaken for a polars frame."""

import pandas as pd
import pytest

from tests.test_alpaca_benchmark_window_returns import report


@pytest.fixture
def pandas_with_select(monkeypatch):
    """Mimic pandas 3.1, where ``hasattr(pd.DataFrame(), "select")`` is True."""
    monkeypatch.setattr(pd.DataFrame, "select", lambda self, *a, **k: self, raising=False)
    assert hasattr(pd.DataFrame(), "select")


def test_benchmark_stats_treat_pandas_frame_as_pandas(monkeypatch, pandas_with_select):
    bars = pd.DataFrame(
        {"open": [90., 100., 104., 120.], "close": [95., 105., 110., 125.]},
        index=pd.date_range("2025-01-03", periods=4, freq="B", tz="America/New_York"),
    )
    result = report(monkeypatch, bars, timestep="minute")
    assert isinstance(result, pd.DataFrame)
    # Window 2025-01-06..08 closes 105, 110, 125: cumulative return is 125 / 105.
    assert result.symbol_cumprod.iloc[-1] == pytest.approx(125.0 / 105.0)
