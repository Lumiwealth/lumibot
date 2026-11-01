"""The Alpaca reporting path must measure the complete daily investment window."""

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting
from lumibot.entities import Asset
from lumibot.strategies._strategy import _Strategy


def report(monkeypatch, bars, *, timestep="day", start="2025-01-06", end="2025-01-08"):
    source = object.__new__(AlpacaBacktesting)
    source._timestep = timestep
    source.market = "NYSE"
    monkeypatch.setattr(source, "get_historical_prices_between_dates", lambda **kwargs: bars.copy())
    strategy = SimpleNamespace(
        is_backtesting=True,
        _benchmark_asset=Asset("BENCH"),
        _backtesting_start=pd.Timestamp(start, tz="America/New_York"),
        _backtesting_end=pd.Timestamp(end, tz="America/New_York"),
        _benchmark_returns_df=None,
        broker=SimpleNamespace(data_source=source, market="NYSE"),
        logger=Mock(),
    )
    _Strategy._dump_benchmark_stats(strategy)
    return strategy._benchmark_returns_df


@pytest.mark.parametrize("start", ["2025-01-06", "2025-01-06 09:30"])
def test_daily_report_includes_first_open_and_last_close(monkeypatch, start):
    bars = pd.DataFrame(
        {"open": [90., 100., 104., 120.], "close": [95., 105., 110., 125.]},
        index=pd.date_range("2025-01-03", periods=4, freq="B", tz="America/New_York"),
    )
    result = report(monkeypatch, bars, start=start)
    assert result.close.dropna().tolist() == [105., 110.]
    assert result.symbol_cumprod.iloc[0] == 1
    assert result.symbol_cumprod.iloc[-1] == pytest.approx(1.10)
    assert result.attrs["entry_basis"] == "first_available_open"


@pytest.mark.parametrize("missing", [0, 1, 2])
def test_daily_report_exposes_missing_first_interior_or_last_session(monkeypatch, missing):
    bars = pd.DataFrame(
        {"open": [100., 101., 102.], "close": [101., 102., 103.]},
        index=pd.date_range("2025-01-06", periods=3, tz="America/New_York"),
    ).drop(pd.Timestamp("2025-01-06", tz="America/New_York") + pd.Timedelta(days=missing))
    assert report(monkeypatch, bars, end="2025-01-09") is None


def test_daily_report_retains_dividends_after_entry(monkeypatch):
    bars = pd.DataFrame(
        {"open": [100., 104.], "close": [105., 110.], "dividend": [2., 1.]},
        index=pd.date_range("2025-01-06", periods=2, tz="America/New_York"),
    )
    assert report(monkeypatch, bars).symbol_cumprod.iloc[-1] == pytest.approx(1.11)


def test_intraday_report_keeps_its_existing_observation_basis(monkeypatch):
    bars = pd.DataFrame(
        {"open": [100., 104.], "close": [105., 110.]},
        index=pd.date_range("2025-01-06 09:30", periods=2, freq="min", tz="America/New_York"),
    )
    result = report(monkeypatch, bars, timestep="minute")
    assert result.index.equals(bars.index)
    assert result.symbol_cumprod.iloc[-1] == pytest.approx(110 / 105)
