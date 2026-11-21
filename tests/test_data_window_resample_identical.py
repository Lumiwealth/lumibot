"""N-minute bars from minute data read full bins from one resample of the series, with identical results.

Why: a strategy asking for 5-minute bars every 5 minutes moves its window on every call, so
``Data.get_bars`` resampled the window from scratch each time. In a profiled 1-month BTC
perpetual 5-minute backtest that was 68 of 193 profiled seconds. Bins wholly inside the window
are the same as the same bins of the whole series, so they are now read from one resample.

Invariant under test: for many request times, lengths and bar sizes, on time-zone-aware and
naive series with gaps, NaN prices and DST changes, ``get_bars`` returns exactly the same frame
(values, dtypes, index) as the per-window resample.
"""

import datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.entities import Asset, Data

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)


def _data(tz, seed, start="2026-03-06 09:00"):
    rng = np.random.default_rng(seed)
    # Built on the real New York clock (it crosses the March DST change), then handed to Data as
    # local wall times, the form Data expects. ``tz=None`` starts after the change instead.
    if tz is None:
        start = "2026-03-10 09:00"
    index = pd.date_range(start, periods=6000, freq="min", tz="America/New_York").tz_localize(None)
    keep = rng.random(len(index)) > 0.08  # gaps
    index = index[keep]
    close = 100 + rng.normal(0, 0.2, len(index)).cumsum()
    frame = pd.DataFrame(
        {
            "open": close + rng.normal(0, 0.05, len(index)),
            "high": close + 0.3,
            "low": close - 0.3,
            "close": close,
            "volume": rng.random(len(index)) * 10,
        },
        index=index,
    )
    frame.loc[rng.random(len(index)) < 0.01, "close"] = np.nan
    return Data(asset=Asset("XYZ"), df=frame, quote=QUOTE, timestep="minute", timezone="America/New_York")


def _times(data):
    index = data.df.index
    rng = np.random.default_rng(7)
    picks = sorted(rng.choice(len(index), size=150, replace=False))
    return [index[i].to_pydatetime() for i in picks if i > 200]


@pytest.mark.parametrize("tz", ["America/New_York", None])
@pytest.mark.parametrize("quantity", [2, 5, 15, 30])
def test_window_resample_matches_per_window_resample(monkeypatch, tz, quantity):
    data = _data(tz, seed=quantity)
    fast_calls = {"n": 0}
    original = Data._resample_minute_window

    def counting(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        if result is not None:
            fast_calls["n"] += 1
        return result

    for when in _times(data):
        for length in (1, 7, 55):
            monkeypatch.setattr(Data, "_resample_minute_window", lambda self, *a, **k: None)
            expected = data.get_bars(when, length=length, timestep=f"{quantity} minutes")
            monkeypatch.setattr(Data, "_resample_minute_window", counting)
            actual = data.get_bars(when, length=length, timestep=f"{quantity} minutes")
            if expected is None:
                assert actual is None
                continue
            pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert fast_calls["n"] > 0


def test_bar_sizes_that_do_not_divide_half_an_hour_use_the_window_resample(monkeypatch):
    data = _data("America/New_York", seed=1)
    called = {"fast": 0}
    original = Data._resample_minute_window

    def spy(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        called["fast"] += result is not None
        return result

    monkeypatch.setattr(Data, "_resample_minute_window", spy)
    when = data.df.index[3000].to_pydatetime()
    assert data.get_bars(when, length=10, timestep="7 minutes") is not None
    assert called["fast"] == 0


def test_naive_series():
    rng = np.random.default_rng(3)
    index = pd.date_range("2026-01-05 00:00", periods=3000, freq="min")
    close = 50 + rng.normal(0, 0.1, len(index)).cumsum()
    frame = pd.DataFrame({"open": close, "high": close + 1, "low": close - 1, "close": close,
                          "volume": rng.random(len(index))}, index=index)
    data = Data(asset=Asset("ABC"), df=frame, quote=QUOTE, timestep="minute", timezone="America/New_York")
    for offset in (301, 777, 1503, 2999):
        when = data.df.index[offset].to_pydatetime()
        fast = data.get_bars(when, length=20, timestep="5 minutes")
        data._window_resample_cache = None
        slow_index = data.df.index
        assert fast is not None and len(fast) > 0
        assert fast.index.max() < slow_index[offset] + datetime.timedelta(minutes=1)
