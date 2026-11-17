"""A futures minute cache with a hole between two earlier downloads must be repaired.

A held December MNQ position failed a six month backtest because the cache had no bars
for 22 hours between two earlier downloads. Stock and index minute caches already repair
such holes; dated futures did not. These tests use made-up bars and a fake IBKR feed.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper
from tests.test_ibkr_daily_gap_self_healing import (
    _minute_feed,
    _minute_setup,
    _new_minute_process,
)

_NY = "America/New_York"
_MNQ = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 12, 18))
_USD = Asset("USD", asset_type=Asset.AssetType.FOREX)


def _globex_minutes(first_open: str, last_close: str) -> pd.DataFrame:
    """Made-up one-minute bars with the real weekly shape: closed 17:00-18:00 and Fri 17:00 to Sun 18:00."""
    index = pd.date_range(pd.Timestamp(first_open, tz=_NY), pd.Timestamp(last_close, tz=_NY), freq="1min")
    keep = []
    for ts in index:
        weekday, minutes = ts.weekday(), ts.hour * 60 + ts.minute
        if weekday == 5 or (weekday == 4 and minutes >= 17 * 60) or (weekday == 6 and minutes < 18 * 60):
            continue
        if 17 * 60 <= minutes < 18 * 60:
            continue
        keep.append(ts)
    index = pd.DatetimeIndex(keep)
    px = 28000.0 + pd.Series(range(len(index)), index=index, dtype="float64") * 0.25
    return pd.DataFrame({"open": px, "high": px + 1, "low": px - 1, "close": px + 0.5, "volume": 50.0}, index=index)


def _setup(monkeypatch, tmp_path) -> None:
    _minute_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_now_utc", lambda: datetime(2026, 11, 17, tzinfo=timezone.utc))
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_FUTURES_GAPS_TRIED", {}, raising=False)


_KW = dict(asset=_MNQ, quote=_USD, timestep="minute", include_after_hours=True)
_THU_17 = pd.Timestamp("2026-11-05 17:00", tz=_NY)
_FRI_16 = pd.Timestamp("2026-11-06 16:00", tz=_NY)


def _two_earlier_backtests(monkeypatch, vendor):
    feed, _ = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=pd.Timestamp("2026-11-03 09:00", tz=_NY).to_pydatetime(),
                               end_dt=_THU_17.to_pydatetime(), **_KW)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=_FRI_16.to_pydatetime(),
                               end_dt=pd.Timestamp("2026-11-09 12:00", tz=_NY).to_pydatetime(), **_KW)


def test_futures_minute_hole_between_two_earlier_backtests_is_fetched(monkeypatch, tmp_path) -> None:
    _setup(monkeypatch, tmp_path)
    vendor = _globex_minutes("2026-11-02 18:00", "2026-11-09 12:00")
    _two_earlier_backtests(monkeypatch, vendor)

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=pd.Timestamp("2026-11-03 09:00", tz=_NY).to_pydatetime(),
                                    end_dt=pd.Timestamp("2026-11-09 12:00", tz=_NY).to_pydatetime(), **_KW)

    hole = vendor.loc[(vendor.index > _THU_17) & (vendor.index < _FRI_16)]
    assert len(hole) > 1000
    assert set(hole.index) <= set(pd.DatetimeIndex(df.index).tz_convert(_NY))
    assert served["pages"] > 0

    # Repaired once: the next process reads the whole window from the cache.
    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    again = ibkr_helper.get_price_data(start_dt=pd.Timestamp("2026-11-03 09:00", tz=_NY).to_pydatetime(),
                                       end_dt=pd.Timestamp("2026-11-09 12:00", tz=_NY).to_pydatetime(), **_KW)
    assert set(hole.index) <= set(pd.DatetimeIndex(again.index).tz_convert(_NY))
    # At most the ordinary window-edge page; the 22 hour hole is not fetched again.
    assert served["pages"] <= 1


def test_maintenance_break_weekend_and_quiet_minutes_are_not_futures_holes() -> None:
    vendor = _globex_minutes("2026-11-02 18:00", "2026-11-09 12:00")
    # A quiet hour with no prints inside the session is shorter than the hole threshold.
    quiet = vendor.loc[~((vendor.index >= pd.Timestamp("2026-11-04 10:00", tz=_NY))
                         & (vendor.index < pd.Timestamp("2026-11-04 11:30", tz=_NY)))]
    gaps = ibkr_helper._futures_minute_interior_gaps(
        quiet, symbol="MNQ", start_dt=pd.Timestamp("2026-11-02 18:00", tz=_NY).to_pydatetime(),
        end_dt=pd.Timestamp("2026-11-09 12:00", tz=_NY).to_pydatetime())
    assert gaps == []


def test_a_long_gap_while_the_exchange_was_open_is_a_futures_hole() -> None:
    vendor = _globex_minutes("2026-11-02 18:00", "2026-11-09 12:00")
    holed = vendor.loc[(vendor.index <= _THU_17) | (vendor.index >= _FRI_16)]
    gaps = ibkr_helper._futures_minute_interior_gaps(
        holed, symbol="MNQ", start_dt=pd.Timestamp("2026-11-02 18:00", tz=_NY).to_pydatetime(),
        end_dt=pd.Timestamp("2026-11-09 12:00", tz=_NY).to_pydatetime())
    assert len(gaps) == 1
    assert gaps[0][0] <= pd.Timestamp("2026-11-05 17:01", tz=_NY) and gaps[0][1] == _FRI_16
