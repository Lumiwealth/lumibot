from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from lumibot.entities import Asset


def test_futures_daily_reads_only_requested_completed_sessions(monkeypatch):
    import pandas_market_calendars as mcal
    import lumibot.tools.ibkr_helper as helper

    schedule = mcal.get_calendar("us_futures").schedule(start_date="2026-10-02", end_date="2026-10-08")
    first, last = schedule.iloc[0], schedule.iloc[-1]
    start = first.market_open + pd.Timedelta(hours=2)
    end = last.market_close + pd.Timedelta(hours=1)
    calls = []
    index = pd.date_range(first.market_open, last.market_close, freq="1h")
    hourly = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100.5, "volume": 1}, index=index)

    def cached(**kwargs):
        calls.append(kwargs)
        assert kwargs["start_dt"] == first.market_open.to_pydatetime()
        assert kwargs["end_dt"] == last.market_close.to_pydatetime()
        return hourly

    monkeypatch.setattr(helper, "_get_cached_bars_for_source", cached)
    monkeypatch.setattr(helper, "_ibkr_history_now_utc", lambda: end.to_pydatetime())
    result = helper._get_futures_daily_bars(
        asset=Asset("MES", asset_type="future", expiration=date(2026, 12, 18)),
        quote=None, start_dt=start.to_pydatetime(), end_dt=end.to_pydatetime(),
        exchange="CME", include_after_hours=True, source="Trades",
    )
    assert len(calls) == 1
    assert result.index.tz_convert("UTC").equals(pd.DatetimeIndex(schedule.market_close))


def test_futures_daily_does_not_fetch_when_no_session_has_completed(monkeypatch):
    import lumibot.tools.ibkr_helper as helper

    monkeypatch.setattr(helper, "_get_cached_bars_for_source", lambda **kwargs: pytest.fail("no completed session requested"))
    result = helper._get_futures_daily_bars(
        asset=Asset("MES", asset_type="future", expiration=date(2026, 12, 18)),
        quote=None, start_dt=datetime(2026, 10, 8, 12, tzinfo=timezone.utc),
        end_dt=datetime(2026, 10, 8, 16, tzinfo=timezone.utc), exchange="CME",
        include_after_hours=True, source="Trades",
    )
    assert result.empty


@pytest.mark.parametrize("provider_empty", [False, True])
def test_futures_daily_health_exposes_missing_completed_sessions(monkeypatch, provider_empty):
    import pandas_market_calendars as mcal
    import lumibot.tools.ibkr_helper as helper

    asset = Asset("MGC", asset_type="future", expiration=date(2026, 10, 28))
    schedule = mcal.get_calendar("us_futures").schedule(start_date="2026-10-01", end_date="2026-10-02")
    first, missing = schedule.iloc[0], schedule.iloc[1]
    idx = pd.date_range(first.market_open, first.market_close, freq="1h")
    hourly = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100.5, "volume": 1}, index=idx)
    # Leave the following completed session empty, as both cached and provider
    # reads did for the October 2026 contract during the live investigation.
    hourly = hourly.loc[hourly.index < first.market_close]
    health = []
    calls = []

    def cached(**kwargs):
        calls.append(kwargs["timestep"])
        return hourly.copy() if kwargs["timestep"] == "hour" and not provider_empty else pd.DataFrame()

    monkeypatch.setattr(helper, "_get_cached_bars_for_source", cached)
    monkeypatch.setattr(helper, "_maybe_augment_futures_bid_ask", lambda **kwargs: (kwargs["df_cache"], False))
    monkeypatch.setattr(helper, "record_history_health", lambda **event: health.append(event))
    result = helper._get_futures_daily_bars(
        asset=asset, quote=None, start_dt=(first.market_open + pd.Timedelta(minutes=1)).to_pydatetime(),
        end_dt=missing.market_close.to_pydatetime(), exchange="COMEX", include_after_hours=True, source="Trades",
    )

    assert len(result) == (0 if provider_empty else 1)
    assert "minute" in calls
    assert health, "missing futures sessions must not silently disappear from history health"
    event = health[-1]
    assert event["outcome"] is helper.HistoryOutcome.PARTIAL
    assert event["expected_sessions"] == 2
    assert event["returned_sessions"] == len(result)
    assert str(missing.market_close.date()) in event["missing_sessions"]
    assert event["reason"] == "missing_futures_daily_sessions"
    assert event["symbol"] == "MGC"
    if not result.empty:
        assert result.index[0] == first.market_close
        assert result.close.iloc[0] == 100.5


def test_ibkr_futures_daily_bars_are_session_aligned_not_midnight(monkeypatch):
    import pandas_market_calendars as mcal
    import lumibot.tools.ibkr_helper as ibkr_helper

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19))

    cal = mcal.get_calendar("us_futures")
    schedule = cal.schedule(start_date=pd.Timestamp("2025-12-08"), end_date=pd.Timestamp("2025-12-09"))
    assert not schedule.empty

    sess = schedule.iloc[0]
    open_local = pd.Timestamp(sess["market_open"]).tz_convert("America/New_York")
    close_local = pd.Timestamp(sess["market_close"]).tz_convert("America/New_York")
    assert open_local < close_local

    idx = pd.date_range(open_local, close_local, freq="1h", tz="America/New_York")
    intraday = pd.DataFrame(
        {
            "open": [100.0 for _ in idx],
            "high": [101.0 for _ in idx],
            "low": [99.0 for _ in idx],
            "close": [100.5 for _ in idx],
            "volume": [1 for _ in idx],
        },
        index=idx,
    )

    calls: list[str] = []

    def fake_get_cached_bars_for_source(*, asset, quote, timestep, start_dt, end_dt, exchange, include_after_hours, source):
        calls.append(str(timestep))
        if str(timestep) == "hour":
            return intraday
        raise AssertionError(f"Unexpected fallback to timestep={timestep!r} for futures daily derivation")

    monkeypatch.setattr(ibkr_helper, "_get_cached_bars_for_source", fake_get_cached_bars_for_source)
    monkeypatch.setattr(ibkr_helper, "_maybe_augment_futures_bid_ask", lambda **kwargs: (kwargs["df_cache"], False))

    out = ibkr_helper._get_futures_daily_bars(
        asset=fut,
        quote=None,
        start_dt=datetime(2025, 12, 8, tzinfo=timezone.utc),
        end_dt=datetime(2025, 12, 9, tzinfo=timezone.utc),
        exchange="CME",
        include_after_hours=True,
        source="Trades",
    )

    assert calls and calls[0] == "hour"
    assert not out.empty
    # Futures "day" bar should be timestamped at the session close, not midnight.
    assert out.index[0].tz is not None
    assert out.index[0].hour != 0
