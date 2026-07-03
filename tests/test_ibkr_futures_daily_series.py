from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from lumibot.entities import Asset


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

    def fake_get_cached_bars_for_source(
        *,
        asset,
        quote,
        timestep,
        start_dt,
        end_dt,
        exchange,
        include_after_hours,
        source,
    ):
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
