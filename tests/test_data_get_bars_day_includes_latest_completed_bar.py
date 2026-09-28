from __future__ import annotations

import datetime

import pandas as pd
import pytz

from lumibot.entities import Asset
from lumibot.entities.data import Data


def test_data_get_bars_day_includes_latest_completed_bar() -> None:
    """
    Regression test: day-cadence `get_bars()` should include the most recent completed bar.

    Bug (2026-01-04):
    - When requesting day bars at market open, Data.get_iter_count() returned the *asof* bar's index
      position, but slicing uses an exclusive end bound. This caused an extra off-by-one and made
      strategies lag by one full trading day (signals/trades shifted).
    """
    ny = pytz.timezone("America/New_York")
    idx = pd.DatetimeIndex(
        [
            ny.localize(datetime.datetime(2015, 8, 19, 16, 0)),
            ny.localize(datetime.datetime(2015, 8, 20, 16, 0)),
            ny.localize(datetime.datetime(2015, 8, 21, 16, 0)),
        ],
        name="datetime",
    )
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [10.0, 20.0, 30.0],
            "volume": [0, 0, 0],
        },
        index=idx,
    )

    data = Data(asset=Asset("TQQQ", asset_type="stock"), df=df, timestep="day")

    # At the open on 2015-08-21, the latest *completed* day bar is 2015-08-20 16:00.
    bars = data.get_bars(ny.localize(datetime.datetime(2015, 8, 21, 9, 30)), length=2, timestep="day")
    assert bars.index[-1].date() == datetime.date(2015, 8, 20)
    assert float(bars["close"].iloc[-1]) == 20.0



# ---------------------------------------------------------------------------
# Intraday: a bar that has closed is visible even when no later bar exists yet
# (release gate 2026-09-25: at 03:00 the newest visible bar was 19:58, not 19:59; the
# 19:59 bar closed at 20:00 but stayed hidden until the 04:00 bar existed).
# ---------------------------------------------------------------------------

import pytest

_NY = pytz.timezone("America/New_York")


def _intraday_frame(stamps: list[str]) -> pd.DataFrame:
    idx = pd.DatetimeIndex([_NY.localize(datetime.datetime.fromisoformat(s)) for s in stamps], name="datetime")
    px = [100.0 + i for i in range(len(idx))]
    return pd.DataFrame({"open": px, "high": px, "low": px, "close": px, "volume": [1] * len(idx)}, index=idx)


def _data(kind: str, frame: pd.DataFrame, timestep: str):
    asset = Asset("SPY", asset_type="stock")
    if kind == "pandas":
        return Data(asset=asset, df=frame, timestep=timestep)
    import polars as pl

    from lumibot.entities.data_polars import DataPolars

    flat = frame.reset_index().rename(columns={frame.index.name or "index": "datetime"})
    return DataPolars(asset=asset, df=pl.from_pandas(flat), timestep=timestep, quote=asset)


def _last_visible(data, at: str, length: int = 3) -> str:
    bars = data.get_bars(_NY.localize(datetime.datetime.fromisoformat(at)), length=length, timestep=data.timestep)
    return bars.index[-1].tz_convert(_NY).strftime("%m-%d %H:%M")


@pytest.mark.parametrize("kind", ["pandas", "polars"])
@pytest.mark.parametrize(
    "at, expected",
    [
        ("2026-09-15 19:59", "09-15 19:58"),  # 19:59 bar still forming
        ("2026-09-15 20:00", "09-15 19:59"),  # closed at the session end
        ("2026-09-16 03:00", "09-15 19:59"),  # overnight
        ("2026-09-16 04:00", "09-15 19:59"),  # 04:00 bar forming
        ("2026-09-16 04:01", "09-16 04:00"),
    ],
)
def test_minute_bar_is_visible_once_closed_even_without_a_later_bar(kind, at, expected):
    frame = _intraday_frame(["2026-09-15 19:57", "2026-09-15 19:58", "2026-09-15 19:59",
                             "2026-09-16 04:00", "2026-09-16 04:01", "2026-09-16 04:02"])
    assert _last_visible(_data(kind, frame, "minute"), at) == expected


@pytest.mark.parametrize("kind", ["pandas", "polars"])
@pytest.mark.parametrize(
    "at, expected",
    [
        ("2026-09-15 10:02", "09-15 09:55"),  # 10:00 five-minute bar runs to 10:05: never early
        ("2026-09-15 10:05", "09-15 10:00"),  # closed, and no 10:05 bar exists (gap)
    ],
)
def test_multi_minute_bars_stored_as_minute_are_never_visible_before_they_close(kind, at, expected):
    frame = _intraday_frame(["2026-09-15 09:45", "2026-09-15 09:50", "2026-09-15 09:55",
                             "2026-09-15 10:00", "2026-09-15 10:15"])
    assert _last_visible(_data(kind, frame, "minute"), at) == expected


@pytest.mark.parametrize("kind", ["pandas", "polars"])
@pytest.mark.parametrize(
    "at, expected",
    [
        ("2026-09-15 15:30", "09-15 14:00"),  # the 15:00 hourly bar runs to 16:00
        ("2026-09-15 16:00", "09-15 15:00"),
        ("2026-09-16 08:00", "09-15 15:00"),
    ],
)
def test_hourly_bar_after_an_irregular_first_bar_is_not_visible_early(kind, at, expected):
    frame = _intraday_frame(["2026-09-15 09:30", "2026-09-15 10:00", "2026-09-15 11:00", "2026-09-15 12:00",
                             "2026-09-15 13:00", "2026-09-15 14:00", "2026-09-15 15:00", "2026-09-16 09:30"])
    assert _last_visible(_data(kind, frame, "hour"), at) == expected


@pytest.mark.parametrize("kind", ["pandas", "polars"])
@pytest.mark.parametrize(
    "at, expected_price",
    [
        ("2026-09-15 10:00", 102.0),   # 10:00 five-minute bar starts: its open
        ("2026-09-15 10:02", 102.0),   # still forming (runs to 10:05): never its close
        ("2026-09-15 10:05", 102.5),   # closed, no 10:05 bar yet: its close
    ],
)
def test_last_price_and_quote_never_use_the_close_of_a_forming_bar(kind, at, expected_price):
    """2026-09-25: a bar stamped at its start is still forming until start + length. Its close
    is the future; the last price and quote price at dt must come from its open."""
    frame = _intraday_frame(["2026-09-15 09:50", "2026-09-15 09:55", "2026-09-15 10:00", "2026-09-15 10:15"])
    frame["close"] = frame["open"] + 0.5
    frame["bid"] = frame["close"]  # synthesized from the close, as IBKR/Polygon history does
    frame["ask"] = frame["close"]
    data = _data(kind, frame, "minute")
    dt = _NY.localize(datetime.datetime.fromisoformat(at))
    assert float(data.get_last_price(dt)) == expected_price
    quote = data.get_quote(dt)
    assert float(quote["close"]) == expected_price
    assert (float(quote["bid"]), float(quote["ask"])) == (expected_price, expected_price)
