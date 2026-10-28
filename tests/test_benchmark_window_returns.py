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


@pytest.mark.parametrize("bad_close", [None, float("nan"), 0., float("inf")])
def test_invalid_observed_benchmark_close_is_not_a_flat_session(bad_close):
    bars = pd.DataFrame({"open": [100., 101., 102.], "close": [101., bad_close, 103.]},
        index=pd.date_range("2025-01-06", periods=3, tz="America/New_York"))
    with pytest.raises(ValueError, match="Benchmark"):
        indicators.benchmark_returns_from_bars(bars)


def test_missing_open_session_cannot_be_filled_as_a_flat_benchmark_day():
    bars = pd.DataFrame({"open": [100., 102.], "close": [101., 103.]},
        index=pd.to_datetime(["2025-01-06", "2025-01-08"]).tz_localize("America/New_York"))
    with pytest.raises(ValueError, match="session"):
        indicators.benchmark_returns_from_bars(bars, market="NYSE")


@pytest.mark.parametrize("dates,market", [
    (["2025-01-17", "2025-01-21"], "NYSE"),  # weekend and exchange holiday
    (["2025-01-04", "2025-01-05"], "24/7"),
    (["2025-04-17", "2025-04-22"], "LSE"),  # Good Friday and Easter Monday
])
def test_benchmark_coverage_uses_the_configured_calendar(dates, market):
    bars = pd.DataFrame({"open": [100., 102.], "close": [101., 103.]},
        index=pd.to_datetime(dates).tz_localize("UTC"))
    result = indicators.benchmark_returns_from_bars(bars, market=market)
    assert result.symbol_cumprod.iloc[-1] == pytest.approx(1.03)
