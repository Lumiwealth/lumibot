"""YahooData must keep each bar interval separate.

Before the fix the in-memory store was keyed by asset only, so once daily bars
were loaded a later minute request returned those daily bars labeled minute.
"""

from datetime import datetime

import pandas as pd
import pytz

from lumibot.backtesting import YahooDataBacktesting
from lumibot.entities import Asset
from lumibot.tools.yahoo_helper import YahooHelper


def _frame(index):
    closes = [100.0 + i for i in range(len(index))]
    return pd.DataFrame(
        {
            "Open": closes,
            "High": closes,
            "Low": closes,
            "Close": closes,
            "Volume": [1_000] * len(closes),
            "Dividends": [0.0] * len(closes),
            "Stock Splits": [0.0] * len(closes),
        },
        index=index,
    )


def _data_source():
    data_source = YahooDataBacktesting(
        datetime_start=datetime(2026, 1, 1),
        datetime_end=datetime(2026, 1, 10),
        show_progress_bar=False,
    )
    eastern = pytz.timezone("America/New_York")
    data_source._datetime = eastern.localize(datetime(2026, 1, 6, 10, 0))
    return data_source


def test_minute_request_after_daily_load_returns_minute_bars(monkeypatch):
    eastern = "America/New_York"
    daily_index = pd.date_range("2025-12-29", periods=6, freq="B", tz=eastern)
    minute_index = pd.date_range("2026-01-06 09:30", periods=20, freq="min", tz=eastern)
    requested = []

    def fake_get_symbol_data(symbol, interval="1d", **kwargs):
        requested.append(interval)
        return _frame(daily_index if interval == "1d" else minute_index)

    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(fake_get_symbol_data))
    data_source = _data_source()
    asset = Asset("SPY")

    daily = data_source.get_historical_prices(asset, 3, timestep="day")
    minute = data_source.get_historical_prices(asset, 5, timestep="minute")

    assert requested == ["1d", "1m"]
    assert len(daily.df) == 3
    minute_times = minute.df.index
    assert len(minute_times) == 5
    assert (minute_times[1:] - minute_times[:-1]).max() == pd.Timedelta(minutes=1)
    assert minute_times[-1] < pd.Timestamp("2026-01-06 10:00", tz=eastern)

    # The daily series is still served for day requests.
    again = data_source.get_historical_prices(asset, 3, timestep="day")
    assert list(again.df["close"]) == list(daily.df["close"])
    assert requested == ["1d", "1m"]


def test_minute_request_without_minute_history_returns_absence(monkeypatch):
    eastern = "America/New_York"
    daily_index = pd.date_range("2025-12-29", periods=6, freq="B", tz=eastern)

    def fake_get_symbol_data(symbol, interval="1d", **kwargs):
        if interval == "1d":
            return _frame(daily_index)
        return _frame(pd.DatetimeIndex([], tz=eastern))

    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(fake_get_symbol_data))
    data_source = _data_source()
    asset = Asset("SPY")

    assert data_source.get_historical_prices(asset, 3, timestep="day") is not None
    assert data_source.get_historical_prices(asset, 5, timestep="minute") is None


def test_unsupported_interval_raises_instead_of_serving_daily_bars(monkeypatch):
    import pytest

    from lumibot.data_sources.exceptions import UnavailabeTimestep

    eastern = "America/New_York"
    daily_index = pd.date_range("2025-12-29", periods=6, freq="B", tz=eastern)
    requested = []

    def fake_get_symbol_data(symbol, interval="1d", **kwargs):
        requested.append(interval)
        return _frame(daily_index)

    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(fake_get_symbol_data))
    data_source = _data_source()
    asset = Asset("SPY")

    assert data_source.get_historical_prices(asset, 3, timestep="day") is not None
    with pytest.raises(UnavailabeTimestep):
        data_source.get_historical_prices(asset, 5, timestep="5minute")
    assert requested == ["1d"]
