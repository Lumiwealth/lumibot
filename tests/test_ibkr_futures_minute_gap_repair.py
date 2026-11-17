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


# ---------------------------------------------------------------------------------------
# Multi-minute series (5, 15, 30 minute bars) and answers that came back empty.
#
# WHY (2026-11-17): the repair measured gaps with a one minute step on every "...minute"
# series. On 5 minute bars the last bar before a close starts at 16:55, so 16:56 to 17:00
# looked open and every weekend and holiday close was asked from the data provider again,
# in every backtest: 23 empty requests before the first simulated bar of a three month run.
# ---------------------------------------------------------------------------------------

_LABOR_DAY_HALT = (pd.Timestamp("2026-09-07 13:00", tz=_NY), pd.Timestamp("2026-09-07 18:00", tz=_NY))


def _globex_bars(first_open: str, last_close: str, minutes: int, closed=()) -> pd.DataFrame:
    """Made-up N-minute bars (labelled by bar start) with the weekly shape plus extra closures."""
    one = _globex_minutes(first_open, last_close)
    for closed_from, closed_to in closed:
        one = one.loc[~((one.index >= closed_from) & (one.index < closed_to))]
    if minutes == 1:
        return one
    grouped = one.groupby(one.index.floor(f"{minutes}min"))
    return pd.DataFrame({"open": grouped["open"].first(), "high": grouped["high"].max(),
                         "low": grouped["low"].min(), "close": grouped["close"].last(),
                         "volume": grouped["volume"].sum()})


def _history_feed(vendor: pd.DataFrame, **kwargs):
    """The fake feed, counting only price history requests (not contract lookups)."""
    inner, _ = _minute_feed(vendor, **kwargs)
    served = {"pages": 0}

    def _request(url, querystring=None, **rest):
        if "startTime" not in (querystring or {}):
            raise KeyError("startTime")
        served["pages"] += 1
        return inner(url, querystring=querystring, **rest)

    return _request, served


def _process(monkeypatch, feed) -> None:
    """A fresh backtest process: nothing remembered in memory, only the cache files."""
    _new_minute_process(monkeypatch, feed)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_FUTURES_GAPS_TRIED", {}, raising=False)


def _kw(timestep: str) -> dict:
    return dict(asset=_MNQ, quote=_USD, timestep=timestep, include_after_hours=True)


def _ask(start: str, end: str, timestep: str) -> pd.DataFrame:
    return ibkr_helper.get_price_data(start_dt=pd.Timestamp(start, tz=_NY).to_pydatetime(),
                                      end_dt=pd.Timestamp(end, tz=_NY).to_pydatetime(), **_kw(timestep))


def _real_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    if "missing" in df.columns:
        df = df.loc[~df["missing"].fillna(False).astype(bool)]
    return pd.DatetimeIndex(df.index).tz_convert(_NY)


def test_five_minute_weekend_and_holiday_closes_are_not_asked_again(monkeypatch, tmp_path) -> None:
    _setup(monkeypatch, tmp_path)
    # Three weekends and the Labor Day 13:00 ET halt.
    vendor = _globex_bars("2026-08-23 18:00", "2026-09-11 12:00", 5, closed=[_LABOR_DAY_HALT])
    window = ("2026-08-24 09:00", "2026-09-11 12:00")
    feed, _ = _history_feed(vendor)
    _process(monkeypatch, feed)
    _ask(*window, "5 minutes")

    # The next backtest finds the whole window cached: no request at all.
    feed, served = _history_feed(vendor)
    _process(monkeypatch, feed)
    df = _ask(*window, "5 minutes")

    assert served["pages"] == 0
    inside = vendor.loc[(vendor.index >= pd.Timestamp(window[0], tz=_NY)) & (vendor.index < pd.Timestamp(window[1], tz=_NY))]
    assert set(inside.index) <= set(_real_index(df))


def test_closures_are_not_holes_on_any_bar_size() -> None:
    for minutes in (1, 5, 15, 30):
        bars = _globex_bars("2026-08-23 18:00", "2026-09-11 12:00", minutes, closed=[_LABOR_DAY_HALT])
        gaps = ibkr_helper._futures_minute_interior_gaps(
            bars, symbol="MNQ", start_dt=pd.Timestamp("2026-08-23 18:00", tz=_NY).to_pydatetime(),
            end_dt=pd.Timestamp("2026-09-11 12:00", tz=_NY).to_pydatetime(),
            bar_step=timedelta(minutes=minutes))
        assert gaps == [], minutes


def test_five_minute_hole_inside_an_open_session_is_still_fetched_once(monkeypatch, tmp_path) -> None:
    _setup(monkeypatch, tmp_path)
    vendor = _globex_bars("2026-11-02 18:00", "2026-11-09 12:00", 5)
    feed, _ = _history_feed(vendor)
    _process(monkeypatch, feed)
    _ask("2026-11-03 09:00", "2026-11-05 17:00", "5 minutes")
    _process(monkeypatch, feed)
    _ask("2026-11-06 16:00", "2026-11-09 12:00", "5 minutes")

    feed, served = _history_feed(vendor)
    _process(monkeypatch, feed)
    df = _ask("2026-11-03 09:00", "2026-11-09 12:00", "5 minutes")
    hole = vendor.loc[(vendor.index > _THU_17) & (vendor.index < _FRI_16)]
    assert len(hole) > 200
    assert set(hole.index) <= set(_real_index(df))
    assert served["pages"] > 0

    feed, served = _history_feed(vendor)
    _process(monkeypatch, feed)
    again = _ask("2026-11-03 09:00", "2026-11-09 12:00", "5 minutes")
    assert set(hole.index) <= set(_real_index(again))
    assert served["pages"] <= 1


_QUIET = (pd.Timestamp("2026-11-04 10:00", tz=_NY), pd.Timestamp("2026-11-04 13:30", tz=_NY))


def test_a_gap_the_provider_answered_empty_is_remembered_until_it_expires(monkeypatch, tmp_path) -> None:
    for timestep, minutes in (("minute", 1), ("5 minutes", 5)):
        _setup(monkeypatch, tmp_path / timestep.replace(" ", ""))
        # Nothing traded for three and a half hours inside an open session.
        vendor = _globex_bars("2026-11-02 18:00", "2026-11-06 12:00", minutes, closed=[_QUIET])
        window = ("2026-11-03 09:00", "2026-11-06 12:00")
        feed, _ = _history_feed(vendor)
        _process(monkeypatch, feed)
        _ask(*window, timestep)

        # Asked once and answered empty: the next backtest does not ask again.
        feed, served = _history_feed(vendor)
        _process(monkeypatch, feed)
        df = _ask(*window, timestep)
        assert served["pages"] == 0, timestep
        # The remembered answer is never a price.
        real = _real_index(df)
        assert not ((real >= _QUIET[0]) & (real < _QUIET[1])).any()

        # The note expires; bars that exist by then are fetched and replace it.
        monkeypatch.setattr(ibkr_helper, "_ibkr_history_now_utc", lambda: datetime(2026, 11, 19, tzinfo=timezone.utc))
        full = _globex_bars("2026-11-02 18:00", "2026-11-06 12:00", minutes)
        feed, served = _history_feed(full)
        _process(monkeypatch, feed)
        df = _ask(*window, timestep)
        assert served["pages"] > 0, timestep
        quiet = full.loc[(full.index >= _QUIET[0]) & (full.index < _QUIET[1])]
        assert set(quiet.index) <= set(_real_index(df)), timestep


def test_a_failed_gap_request_is_not_remembered_as_empty(monkeypatch, tmp_path) -> None:
    _setup(monkeypatch, tmp_path)
    vendor = _globex_bars("2026-11-02 18:00", "2026-11-09 12:00", 5)
    feed, _ = _history_feed(vendor)
    _process(monkeypatch, feed)
    _ask("2026-11-03 09:00", "2026-11-05 17:00", "5 minutes")
    _process(monkeypatch, feed)
    _ask("2026-11-06 16:00", "2026-11-09 12:00", "5 minutes")

    # The provider fails on every request: nothing is written about the hole.
    failing, _ = _history_feed(vendor, fail_after=0, exc=RuntimeError)
    _process(monkeypatch, failing)
    _ask("2026-11-03 09:00", "2026-11-09 12:00", "5 minutes")

    feed, served = _history_feed(vendor)
    _process(monkeypatch, feed)
    df = _ask("2026-11-03 09:00", "2026-11-09 12:00", "5 minutes")
    hole = vendor.loc[(vendor.index > _THU_17) & (vendor.index < _FRI_16)]
    assert served["pages"] > 0
    assert set(hole.index) <= set(_real_index(df))
