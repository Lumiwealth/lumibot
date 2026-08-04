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


def _hourly_frame(start: str, end: str, *, missing: bool = False) -> pd.DataFrame:
    index = pd.date_range(start=start, end=end, freq="h", tz="America/New_York")
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


def test_daily_gap_repair_fetches_missing_range_with_bounded_wait(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
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
        end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert len(calls) == 1
    # The repaired July 28 session produces one padded, end-exclusive window.
    # Derive the provider period from that window instead of freezing its floor.
    repair_days = (calls[0]["end_dt"] - calls[0]["start_dt"]).days + 1
    expected_period_days = max(5, min(30, repair_days))
    assert calls[0]["_period_override"] == f"{expected_period_days}d"
    assert calls[0]["_record_missing_on_empty"] is False
    assert calls[0]["_max_timeout_attempts"] == 1
    assert calls[0]["_deadline_monotonic"] > 0
    assert (
        0
        < calls[0]["_queue_timeout"]
        <= ibkr_helper.IBKR_DAILY_GAP_REPAIR_TIMEOUT_SECONDS
    )
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


def test_daily_gap_repair_does_not_persist_ambiguous_empty_response(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
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

    # An ambiguous empty repair remains absent and process-retryable. It must not
    # write a durable missing marker that would suppress a later healthy run.
    assert pd.Timestamp("2026-07-28 16:00", tz="America/New_York") not in result.index
    assert writes == []


def test_all_placeholder_daily_cache_repairs_without_a_real_anchor(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    frame = _daily_frame(["2026-07-27", "2026-07-28", "2026-07-29"], missing=True)
    calls = []

    def _fake_fetch(**kwargs):
        calls.append(kwargs)
        return _daily_frame(["2026-07-27", "2026-07-28", "2026-07-29"])

    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _fake_fetch)
    monkeypatch.setattr(ibkr_helper, "_write_cache_frame", lambda *_args, **_kwargs: None)

    result = ibkr_helper._repair_us_stock_index_daily_gaps(
        frame,
        cache_file=tmp_path / "GLD.parquet",
        asset=Asset("GLD", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="day",
        start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert len(calls) == 1
    assert calls[0]["_period_override"] == "6d"
    assert result["missing"].fillna(False).astype(bool).sum() == 0


def test_hourly_gap_repair_fetches_large_internal_gap_once_with_bounded_deadline(
    monkeypatch,
    tmp_path,
) -> None:
    """Reproduce the production QQQ shape: real bars at both ends and a multi-year hole."""
    ibkr_helper._RUNTIME_HOURLY_GAP_CHECKED_SERIES.clear()
    left = _hourly_frame("2023-07-31 09:00", "2023-08-01 16:00")
    right = _hourly_frame("2026-07-29 09:00", "2026-07-30 16:00")
    frame = pd.concat([left, right]).sort_index()
    calls = []

    def _fake_fetch(**kwargs):
        calls.append(kwargs)
        return _hourly_frame("2023-08-01 17:00", "2026-07-29 08:00")

    writes = []
    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _fake_fetch)
    monkeypatch.setattr(
        ibkr_helper,
        "_write_cache_frame",
        lambda path, updated: writes.append((path, updated.copy())),
    )

    result = ibkr_helper._repair_us_stock_index_hourly_gaps(
        frame,
        cache_file=tmp_path / "QQQ-hour.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="hour",
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert len(calls) == 1
    assert (
        calls[0]["_period_override"]
        == ibkr_helper.IBKR_STOCK_INDEX_HOURLY_REPAIR_PERIOD
    )
    assert calls[0]["_record_missing_on_empty"] is False
    assert calls[0]["_max_timeout_attempts"] == 1
    assert calls[0]["_deadline_monotonic"] > 0
    real = result.loc[~result["missing"].fillna(False)]
    assert real.index.to_series().diff().dropna().max() <= pd.Timedelta(days=7)
    assert len(writes) == 1


def test_hourly_gap_repair_warm_complete_cache_makes_zero_downloader_calls(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_HOURLY_GAP_CHECKED_SERIES.clear()
    frame = _hourly_frame("2026-07-20 09:00", "2026-07-30 16:00")
    gap_scan_calls = []
    original_gap_scan = ibkr_helper._hourly_internal_gaps
    monkeypatch.setattr(
        ibkr_helper,
        "_hourly_internal_gaps",
        lambda *args, **kwargs: gap_scan_calls.append((args, kwargs))
        or original_gap_scan(*args, **kwargs),
    )

    monkeypatch.setattr(
        ibkr_helper,
        "_fetch_history_between_dates",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("complete hourly cache must not hit the downloader")
        ),
    )

    result = ibkr_helper._repair_us_stock_index_hourly_gaps(
        frame,
        cache_file=tmp_path / "QQQ-hour.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="hour",
        start_dt=datetime(2026, 7, 20, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 31, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert result.equals(frame)
    second = ibkr_helper._repair_us_stock_index_hourly_gaps(
        frame,
        cache_file=tmp_path / "QQQ-hour.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="hour",
        start_dt=datetime(2026, 7, 20, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 31, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )
    assert second.equals(frame)
    assert len(gap_scan_calls) == 1


def test_hourly_retry_marker_lookup_accepts_tz_naive_cache_index() -> None:
    now = datetime(2026, 7, 30, 12, tzinfo=timezone.utc)
    retry_after = datetime(2026, 7, 31, 12, tzinfo=timezone.utc).isoformat()
    frame = pd.DataFrame(
        {
            "missing": [True, True],
            "missing_retry_after": [retry_after, retry_after],
            "missing_reason": [
                "hourly_internal_gap_empty",
                "hourly_internal_gap_empty",
            ],
        },
        index=pd.DatetimeIndex(
            [
                "2026-07-20 12:00:00",
                "2026-07-25 12:00:00",
            ]
        ),
    )

    assert ibkr_helper._gap_has_fresh_retry_marker(
        frame,
        gap_start=pd.Timestamp("2026-07-19 12:00", tz="America/New_York"),
        gap_end=pd.Timestamp("2026-07-26 12:00", tz="America/New_York"),
        now=now,
    )


def test_hourly_clean_scan_is_repeated_when_request_window_widens(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_HOURLY_GAP_CHECKED_SERIES.clear()
    left = _hourly_frame("2023-07-31 09:00", "2023-08-01 16:00")
    right = _hourly_frame("2026-07-29 09:00", "2026-07-30 16:00")
    frame = pd.concat([left, right]).sort_index()
    calls = []
    monkeypatch.setattr(
        ibkr_helper,
        "_fetch_history_between_dates",
        lambda **kwargs: calls.append(kwargs)
        or _hourly_frame("2023-08-01 17:00", "2026-07-29 08:00"),
    )
    monkeypatch.setattr(ibkr_helper, "_write_cache_frame", lambda *_args: None)
    common = {
        "df_cache": frame,
        "cache_file": tmp_path / "QQQ-hour.parquet",
        "asset": Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        "quote": Asset("USD", asset_type=Asset.AssetType.FOREX),
        "timestep": "hour",
        "exchange": None,
        "include_after_hours": True,
        "source": "Trades",
        "source_was_explicit": False,
    }

    ibkr_helper._repair_us_stock_index_hourly_gaps(
        **common,
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2023, 8, 2, tzinfo=timezone.utc),
    )
    assert calls == []

    ibkr_helper._repair_us_stock_index_hourly_gaps(
        **common,
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
    )
    assert len(calls) == 1


def test_partial_hourly_repair_does_not_negative_cache_the_remaining_gap(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_HOURLY_GAP_CHECKED_SERIES.clear()
    left = _hourly_frame("2023-07-31 09:00", "2023-08-01 16:00")
    right = _hourly_frame("2026-07-29 09:00", "2026-07-30 16:00")
    frame = pd.concat([left, right]).sort_index()
    monkeypatch.setattr(
        ibkr_helper,
        "_fetch_history_between_dates",
        lambda **_kwargs: _hourly_frame(
            "2025-07-29 09:00",
            "2026-07-29 08:00",
        ),
    )
    writes = []
    monkeypatch.setattr(
        ibkr_helper,
        "_write_cache_frame",
        lambda path, updated: writes.append((path, updated.copy())),
    )

    result = ibkr_helper._repair_us_stock_index_hourly_gaps(
        frame,
        cache_file=tmp_path / "QQQ-hour.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="hour",
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert ibkr_helper._hourly_internal_gaps(
        result,
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
    )
    assert not result["missing"].fillna(False).any()
    assert len(writes) == 1


def test_generic_fresh_missing_markers_do_not_block_hourly_self_healing(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_HOURLY_GAP_CHECKED_SERIES.clear()
    left = _hourly_frame("2023-07-31 09:00", "2023-08-01 16:00")
    right = _hourly_frame("2026-07-29 09:00", "2026-07-30 16:00")
    retry_after = datetime(2026, 7, 31, tzinfo=timezone.utc).isoformat()
    generic_markers = pd.DataFrame(
        {
            "open": [pd.NA, pd.NA],
            "high": [pd.NA, pd.NA],
            "low": [pd.NA, pd.NA],
            "close": [pd.NA, pd.NA],
            "volume": [pd.NA, pd.NA],
            "missing": [True, True],
            "missing_retry_after": [retry_after, retry_after],
        },
        index=pd.to_datetime(
            [
                "2023-08-01 17:00:00-04:00",
                "2026-07-29 08:00:00-04:00",
            ],
            utc=True,
        ).tz_convert("America/New_York"),
    )
    frame = pd.concat([left, generic_markers, right]).sort_index()
    calls = []
    monkeypatch.setattr(
        ibkr_helper,
        "_fetch_history_between_dates",
        lambda **kwargs: calls.append(kwargs)
        or _hourly_frame("2023-08-01 17:00", "2026-07-29 08:00"),
    )
    monkeypatch.setattr(ibkr_helper, "_write_cache_frame", lambda *_args: None)

    ibkr_helper._repair_us_stock_index_hourly_gaps(
        frame,
        cache_file=tmp_path / "QQQ-hour.parquet",
        asset=Asset("QQQ", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="hour",
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert len(calls) == 1


def test_daily_gap_repair_budget_is_per_series_not_global(
    monkeypatch,
    tmp_path,
) -> None:
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    calls = []

    def _fake_fetch(**kwargs):
        calls.append(kwargs)
        return _daily_frame(["2026-07-28"])

    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _fake_fetch)
    monkeypatch.setattr(ibkr_helper, "_write_cache_frame", lambda *_args: None)

    for symbol in ("QQQ", "SQQQ"):
        result = ibkr_helper._repair_us_stock_index_daily_gaps(
            _daily_frame(["2026-07-27", "2026-07-29"]),
            cache_file=tmp_path / f"{symbol}-day.parquet",
            asset=Asset(symbol, asset_type=Asset.AssetType.STOCK),
            quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
            timestep="day",
            start_dt=datetime(2026, 7, 27, tzinfo=timezone.utc),
            end_dt=datetime(2026, 7, 30, 23, tzinfo=timezone.utc),
            exchange=None,
            include_after_hours=True,
            source="Trades",
            source_was_explicit=False,
        )
        assert pd.Timestamp("2026-07-28 16:00", tz="America/New_York") in result.index

    assert len(calls) == 2
