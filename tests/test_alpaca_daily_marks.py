"""Daily marks must follow exchange sessions without revealing future prices."""

import pandas as pd
import pytest

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import Asset, Bars


def source(monkeypatch, *, date="2025-01-07", market="NYSE"):
    start = pd.Timestamp(date, tz="America/New_York") - pd.Timedelta(days=1)
    data = AlpacaBacktesting(
        start.to_pydatetime(), (start + pd.Timedelta(days=4)).to_pydatetime(),
        config={"API_KEY": "fixture", "API_SECRET": "fixture", "PAPER": True},
        timestep="day", full_window=True, market=market,
    )
    bars = pd.DataFrame(
        {"open": [90., 100.], "high": [96., 111.], "low": [89., 99.],
         "close": [95., 110.], "volume": [1000., 1000.]},
        index=pd.DatetimeIndex([start, start + pd.Timedelta(days=1)]),
    )
    monkeypatch.setattr(data, "get_historical_prices_between_dates", lambda **kwargs: bars.copy())
    return data


@pytest.mark.parametrize("clock,expected", [
    ("08:30", 95.), ("09:29", 95.), ("09:30", 100.),
    ("15:59", 100.), ("16:00", 110.), ("19:00", 110.),
])
def test_daily_stock_mark_uses_only_prices_available_at_session_time(monkeypatch, clock, expected):
    data = source(monkeypatch)
    data._datetime = pd.Timestamp(f"2025-01-07 {clock}", tz="America/New_York")
    assert data.get_last_price(Asset("TEST")) == expected


@pytest.mark.parametrize("clock,expected", [("12:59", 100.), ("13:00", 110.), ("16:00", 110.)])
def test_early_close_marks_at_actual_exchange_close(monkeypatch, clock, expected):
    data = source(monkeypatch, date="2025-11-28")
    data._datetime = pd.Timestamp(f"2025-11-28 {clock}", tz="America/New_York")
    assert data.get_last_price(Asset("TEST")) == expected


def test_first_session_preopen_does_not_leak_the_upcoming_open(monkeypatch):
    data = source(monkeypatch)
    data._datetime = pd.Timestamp("2025-01-06 08:30", tz="America/New_York")
    assert data.get_last_price(Asset("TEST")) is None


def test_crypto_daily_bar_does_not_close_with_the_equity_session(monkeypatch):
    data = source(monkeypatch, market="24/7")
    data._datetime = pd.Timestamp("2025-01-07 16:00", tz="America/New_York")
    assert data.get_last_price(Asset("COIN", asset_type="crypto")) == 100.


@pytest.mark.parametrize("clock,expected", [("09:30", None), ("11:00", 100.), ("16:00", 110.)])
def test_daily_option_mark_preserves_first_print_and_session_close(monkeypatch, clock, expected):
    data = source(monkeypatch)
    asset = Asset("TEST", asset_type="option", expiration=pd.Timestamp("2025-01-17").date(),
                  strike=100, right="CALL")
    frame = pd.DataFrame({"open": [100.], "high": [111.], "low": [99.], "close": [110.],
                          "volume": [10.]}, index=pd.DatetimeIndex([
                              pd.Timestamp("2025-01-07 11:00", tz="America/New_York")]))
    monkeypatch.setattr(data, "_get_option_historical_prices",
                        lambda *args, **kwargs: Bars(frame, data.SOURCE, asset))
    data._datetime = pd.Timestamp(f"2025-01-07 {clock}", tz="America/New_York")
    assert data.get_last_price(asset) == expected


@pytest.mark.parametrize("zone,clock,expected", [
    ("America/Chicago", "08:00", 95.), ("America/Chicago", "08:30", 100.),
    ("America/Chicago", "15:00", 110.), ("UTC", "14:30", 100.), ("UTC", "21:00", 110.),
])
def test_daily_marks_preserve_provider_session_across_timezones(monkeypatch, zone, clock, expected):
    data = source(monkeypatch)
    provider = data.get_historical_prices_between_dates()
    provider = pd.concat([provider, provider.iloc[[-1]].set_axis(
        pd.DatetimeIndex([pd.Timestamp("2025-01-08", tz="America/New_York")]))])
    provider.index = provider.index.tz_convert(zone)
    monkeypatch.setattr(data, "get_historical_prices_between_dates", lambda **kwargs: provider.copy())
    data._datetime = pd.Timestamp(f"2025-01-07 {clock}", tz=zone)
    assert data.get_last_price(Asset("TEST")) == expected
