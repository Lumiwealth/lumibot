"""A passive benchmark owns the same complete window, not just day two."""
import pandas as pd
import pytest

from lumibot.tools import indicators


def test_benchmark_begins_at_first_available_open_and_includes_final_close():
    frame = pd.DataFrame({"open": [100., 104.], "close": [105., 110.], "dividend": [2., 1.]},
        index=pd.date_range("2025-01-06", periods=2, tz="America/New_York"))
    result = indicators.benchmark_returns_from_bars(frame)
    assert result.symbol_cumprod.iloc[0] == 1
    assert result.symbol_cumprod.iloc[1] == pytest.approx(1.05)
    assert result.symbol_cumprod.iloc[-1] == pytest.approx(1.11)
    assert pd.isna(result.close.iloc[0]), "A valuation anchor is not a fabricated market quote"
    assert result.attrs["entry_basis"] == "first_available_open"


def test_incomplete_benchmark_does_not_invent_flat_returns_after_last_observation():
    index = pd.date_range("2025-01-06", periods=3, tz="America/New_York")
    strategy = pd.DataFrame({"portfolio_value": [100., 101., 102.]}, index=index)
    benchmark = pd.DataFrame({"symbol_cumprod": [1., 1.05]}, index=index[:2])
    result = indicators._prepare_tearsheet_returns(strategy, benchmark)
    assert result is None, "An incomplete comparison must stay unavailable, not become a zero-return day"


def test_first_session_return_survives_utc_tearsheet_alignment():
    index = pd.date_range("2025-01-06", periods=2, tz="America/New_York")
    bars = pd.DataFrame({"open": [100., 104.], "close": [105., 110.]}, index=index)
    benchmark = indicators.benchmark_returns_from_bars(bars)
    strategy = pd.DataFrame({"portfolio_value": [100., 102.]}, index=index + pd.Timedelta(hours=16))
    prepared = indicators._prepare_tearsheet_returns(strategy, benchmark)
    assert prepared is not None
    assert (1 + prepared["benchmark"]).prod() == pytest.approx(1.10)
