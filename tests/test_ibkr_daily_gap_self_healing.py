from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper


def _daily_frame(dates: list[str], *, missing: bool = False) -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [pd.Timestamp(f"{day} 16:00", tz="America/New_York") for day in dates]
    )
    values = [pd.NA] * len(index) if missing else [100.0 + i for i in range(len(index))]
    return pd.DataFrame(
        {
            "open": values,
            "high": values,
            "low": values,
            "close": values,
            "volume": values,
            "missing": [missing] * len(index),
        },
        index=index,
    )


def test_merge_frames_never_allows_placeholder_to_replace_real_bar() -> None:
    real = _daily_frame(["2026-07-27"])
    placeholder = _daily_frame(["2026-07-27"], missing=True)

    real_then_placeholder = ibkr_helper._merge_frames(real, placeholder)
    placeholder_then_real = ibkr_helper._merge_frames(placeholder, real)

    assert bool(real_then_placeholder.iloc[0]["missing"]) is False
    assert real_then_placeholder.iloc[0]["close"] == 100.0
    assert bool(placeholder_then_real.iloc[0]["missing"]) is False
    assert placeholder_then_real.iloc[0]["close"] == 100.0


def test_retryable_daily_gaps_include_unmarked_and_legacy_markers_but_not_fresh_markers() -> None:
    now = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    frame = _daily_frame(["2026-07-27"])
    markers = _daily_frame(["2026-07-28", "2026-07-29"], missing=True)
    markers.loc[pd.Timestamp("2026-07-29 16:00", tz="America/New_York"), "missing_retry_after"] = (
        now + timedelta(hours=12)
    ).isoformat()
    frame = pd.concat([frame, markers]).sort_index()

    gaps = ibkr_helper._retryable_us_daily_sessions(
        frame,
        start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 31, tzinfo=timezone.utc),
        now=now,
    )

    assert [ts.date().isoformat() for ts in gaps] == ["2026-07-28", "2026-07-30"]


def test_retryable_daily_gap_scan_is_fast_for_three_year_warm_cache() -> None:
    schedule = ibkr_helper._expected_us_daily_sessions(
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
    )
    frame = _daily_frame([ts.date().isoformat() for ts in schedule])

    started = pd.Timestamp.now()
    gaps = ibkr_helper._retryable_us_daily_sessions(
        frame,
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
        now=datetime(2026, 7, 30, 12, tzinfo=timezone.utc),
    )
    elapsed = (pd.Timestamp.now() - started).total_seconds()

    assert gaps == []
    assert elapsed < 1.0


def test_daily_gap_repair_fetches_only_missing_month_with_bounded_wait(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED", 0.0)
    frame = _daily_frame(["2026-07-27", "2026-07-29"])
    calls = []

    def _fake_fetch(**kwargs):
        calls.append(kwargs)
        return _daily_frame(["2026-07-28"])

    writes = []
    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _fake_fetch)
    monkeypatch.setattr(
        ibkr_helper,
        "_write_cache_frame",
        lambda path, updated: writes.append((path, updated.copy())),
    )

    asset = Asset("QQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    result = ibkr_helper._repair_us_stock_index_daily_gaps(
        frame,
        cache_file=tmp_path / "QQQ.parquet",
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert len(calls) == 1
    assert calls[0]["_period_override"] == "1m"
    assert calls[0]["_record_missing_on_empty"] is False
    assert calls[0]["_max_timeout_attempts"] == 1
    assert 0 < calls[0]["_queue_timeout"] <= 15
    assert [ts.date().isoformat() for ts in result.index] == [
        "2026-07-27",
        "2026-07-28",
        "2026-07-29",
    ]
    assert len(writes) == 1


def test_daily_gap_repair_failure_returns_available_bars_without_failing(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED", 0.0)
    frame = _daily_frame(["2026-07-27", "2026-07-29"])

    def _timeout(**_kwargs):
        raise TimeoutError("repair budget expired")

    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _timeout)
    monkeypatch.setattr(
        ibkr_helper,
        "_write_cache_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an unsuccessful repair must not rewrite the cache")
        ),
    )

    result = ibkr_helper._repair_us_stock_index_daily_gaps(
        frame,
        cache_file=tmp_path / "QQQ.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="day",
        start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert result.equals(frame)


def test_daily_gap_repair_records_expiring_marker_when_small_request_is_empty(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED", 0.0)
    frame = _daily_frame(["2026-07-27", "2026-07-29"])
    writes = []
    monkeypatch.setattr(
        ibkr_helper,
        "_fetch_history_between_dates",
        lambda **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        ibkr_helper,
        "_write_cache_frame",
        lambda path, updated: writes.append((path, updated.copy())),
    )

    result = ibkr_helper._repair_us_stock_index_daily_gaps(
        frame,
        cache_file=tmp_path / "QQQ.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="day",
        start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    marker = result.loc[pd.Timestamp("2026-07-28 16:00", tz="America/New_York")]
    assert bool(marker["missing"]) is True
    assert pd.Timestamp(marker["missing_retry_after"]).tzinfo is not None
    assert len(writes) == 1
