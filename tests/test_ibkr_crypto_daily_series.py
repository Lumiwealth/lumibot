from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from lumibot.entities import Asset
import lumibot.tools.ibkr_helper as ibkr_helper


def test_ibkr_crypto_daily_bounds_exclusive_end_midnight():
    tz = ibkr_helper.LUMIBOT_DEFAULT_PYTZ
    start = tz.localize(datetime(2025, 1, 1, 0, 0, 0))
    end = tz.localize(datetime(2025, 1, 3, 0, 0, 0))

    start_day, end_day = ibkr_helper._crypto_day_bounds(start, end)
    assert start_day == tz.localize(datetime(2025, 1, 1, 0, 0, 0))
    # end is exclusive at midnight, so last day is Jan 2
    assert end_day == tz.localize(datetime(2025, 1, 2, 0, 0, 0))


def test_ibkr_crypto_derive_daily_from_intraday_produces_continuous_days():
    tz = ibkr_helper.LUMIBOT_DEFAULT_PYTZ
    start_day = tz.localize(datetime(2025, 1, 1, 0, 0, 0))
    end_day = tz.localize(datetime(2025, 1, 2, 0, 0, 0))

    # Hourly bars covering both days, with a small gap (should not create a missing day).
    idx = pd.date_range(start=start_day, end=end_day + timedelta(hours=23), freq="1h", tz=tz)
    df = pd.DataFrame(
        {
            "open": range(len(idx)),
            "high": [x + 0.5 for x in range(len(idx))],
            "low": [x - 0.5 for x in range(len(idx))],
            "close": range(len(idx)),
            "volume": [1] * len(idx),
        },
        index=idx,
    )
    df = df.drop(df.index[5])  # gap inside the day

    daily = ibkr_helper._derive_daily_from_intraday(df, start_day=start_day, end_day=end_day)
    assert list(daily.index) == list(pd.date_range(start=start_day, end=end_day, freq="D", tz=tz))
    assert daily["missing"].tolist() == [False, False]
    assert float(daily["close"].iloc[0]) == float(df.loc[df.index.normalize() == start_day]["close"].iloc[-1])


def test_ibkr_fetch_history_between_dates_does_not_break_early_on_short_chunks(monkeypatch):
    calls = {"history": 0}
    tz = ibkr_helper.LUMIBOT_DEFAULT_PYTZ

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    start = tz.localize(datetime(2025, 1, 1, 0, 0, 0))
    end = tz.localize(datetime(2025, 1, 1, 6, 0, 0))

    # Stub conid resolution.
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **kwargs: 12345)

    # Return 2-hour chunks each call, so len(df) < 1000 but we still need multiple calls to reach start.
    def _fake_history_request(**kwargs):
        calls["history"] += 1
        cursor_end = kwargs["start_time"].astimezone(timezone.utc)
        cursor_start = cursor_end - timedelta(hours=2)
        if cursor_start < start.astimezone(timezone.utc):
            cursor_start = start.astimezone(timezone.utc)
        idx = pd.date_range(
            start=cursor_start,
            end=cursor_end,
            freq="1h",
            tz="UTC",
        )
        # Convert to IBKR payload format: ms timestamps with o/h/l/c/v.
        data = []
        for i, ts in enumerate(idx):
            ms = int(ts.timestamp() * 1000)
            data.append({"t": ms, "o": 100 + i, "h": 101 + i, "l": 99 + i, "c": 100 + i, "v": 1})
        return {"data": data}

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", _fake_history_request)

    df = ibkr_helper._fetch_history_between_dates(
        asset=base,
        quote=quote,
        timestep="hour",
        start_dt=start,
        end_dt=end,
        exchange="ZEROHASH",
        include_after_hours=True,
        source="Trades",
        source_was_explicit=True,
    )
    assert calls["history"] >= 2, "Expected multiple short-chunk requests to reach start_dt"
    assert not df.empty
    assert df.index.min() <= start


def test_ibkr_fetch_history_between_dates_keeps_prior_chunks_when_later_page_is_empty(monkeypatch):
    calls = {"history": 0, "missing_window": 0}
    tz = ibkr_helper.LUMIBOT_DEFAULT_PYTZ

    base = Asset("VIX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    start = tz.localize(datetime(2026, 1, 5, 9, 30, 0))
    end = tz.localize(datetime(2026, 1, 5, 16, 0, 0))

    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **kwargs: 13455763)

    def _fake_missing_window(**kwargs):
        calls["missing_window"] += 1

    monkeypatch.setattr(ibkr_helper, "_record_missing_window", _fake_missing_window)

    def _fake_history_request(**kwargs):
        calls["history"] += 1
        if calls["history"] == 1:
            # Return data covering the full requested window.  With full
            # coverage in a single page the function no longer paginates, so
            # only one history call is expected.
            idx = pd.date_range(
                start=start.astimezone(timezone.utc),
                end=end.astimezone(timezone.utc),
                freq="1h",
                tz="UTC",
            )
            data = []
            for i, ts in enumerate(idx):
                ms = int(ts.timestamp() * 1000)
                data.append({"t": ms, "o": 20 + i, "h": 21 + i, "l": 19 + i, "c": 20 + i, "v": 1})
            return {"data": data}
        # Simulate an empty page on a later pagination request.
        return {"data": []}

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", _fake_history_request)

    df = ibkr_helper._fetch_history_between_dates(
        asset=base,
        quote=quote,
        timestep="hour",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
        source="Midpoint",
        source_was_explicit=True,
    )
    assert calls["history"] >= 1
    assert calls["missing_window"] == 0
    assert not df.empty
    assert df.index.max() <= end


def test_ibkr_crypto_get_price_data_derives_daily_for_1d_like_timesteps(monkeypatch):
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    called = {"count": 0}

    def _fake_daily(**kwargs):
        called["count"] += 1
        return pd.DataFrame(
            {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [0]},
            index=pd.DatetimeIndex([pd.Timestamp("2025-01-01", tz=ibkr_helper.LUMIBOT_DEFAULT_PYTZ)]),
        )

    monkeypatch.setattr(ibkr_helper, "_get_crypto_daily_bars", _fake_daily)

    for ts in ("day", "1d", "1day", "2day"):
        df = ibkr_helper.get_price_data(
            asset=base,
            quote=quote,
            timestep=ts,
            start_dt=start,
            end_dt=end,
            exchange="ZEROHASH",
            include_after_hours=True,
            source="Trades",
        )
        assert df is not None
        assert not df.empty

    assert called["count"] == 4
