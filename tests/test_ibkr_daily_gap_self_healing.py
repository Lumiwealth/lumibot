from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

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


# ---------------------------------------------------------------------------
# 1-minute stock/index cache holes (2026-09-25 release gate for 4.6.1)
#
# The minute path only compared the edges of the requested window with the cached
# frame. A cache holding June and September (two earlier backtests) therefore served a
# June-to-September backtest with July and August silently missing: zero requests, no
# error, and the strategy saw one stale bar for weeks. The same hole appears after an
# interrupted download (page checkpoints) or a failed older page (kept newer pages), and
# 4.6.0 left one at every weekend in the shared S3 cache. These tests drive
# `get_price_data` against a fake IBKR feed.
# ---------------------------------------------------------------------------

_NY = "America/New_York"


def _minute_days(first: str, last: str, *, closed=("2026-06-19", "2026-07-03", "2026-09-07")) -> list[str]:
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(first, last) if d.strftime("%Y-%m-%d") not in set(closed)]


def _minute_vendor(days: list[str], *, first="04:00", last="19:59") -> pd.DataFrame:
    frames = []
    base = 650.0
    for day in days:
        idx = pd.date_range(pd.Timestamp(f"{day} {first}", tz=_NY), pd.Timestamp(f"{day} {last}", tz=_NY), freq="1min")
        px = base + pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.001
        frames.append(pd.DataFrame({"open": px, "high": px + 0.05, "low": px - 0.05, "close": px + 0.01, "volume": 1000.0}, index=idx))
        base += 1.0
    return pd.concat(frames).sort_index()


class _StopRun(BaseException):
    """Stands in for a force-stopped or timed-out backtest process."""


def _minute_feed(vendor: pd.DataFrame, *, fail_after=None, exc=_StopRun):
    served = {"pages": 0}

    def _fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        served["pages"] += 1
        if fail_after is not None and served["pages"] > fail_after:
            raise exc("simulated stop or failure")
        end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        period = str(querystring["period"])
        if period.endswith("min"):
            start = end - pd.Timedelta(minutes=int(period.removesuffix("min")))
        else:
            start = end - pd.Timedelta(days=int(period.removesuffix("d")))
        rows = vendor.loc[(vendor.index > start) & (vendor.index <= end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    return _fake_queue_request, served


def _new_minute_process(monkeypatch, feed) -> None:
    """Reset per-process memory, as a fresh backtest process would start."""
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_MINUTE_GAP_CHECKED_SERIES", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_MINUTE_GAP_FAILED_SESSIONS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_MINUTE_GAP_REPAIR_STOPPED", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_MINUTE_GAP_LAST_WARNING", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "queue_request", feed)


def _minute_setup(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "x")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_now_utc", lambda: datetime(2026, 9, 25, tzinfo=timezone.utc))


def _sessions(frame: pd.DataFrame) -> set[str]:
    if frame is None or frame.empty:
        return set()
    return set(pd.DatetimeIndex(frame.index).tz_convert(_NY).strftime("%Y-%m-%d"))


_SPY = Asset("SPY", asset_type=Asset.AssetType.STOCK)
_USD = Asset("USD", asset_type=Asset.AssetType.FOREX)
_MINUTE_KW = dict(asset=_SPY, quote=_USD, timestep="minute", include_after_hours=True)


def test_minute_cache_hole_between_two_earlier_backtests_is_fetched(monkeypatch, tmp_path) -> None:
    _minute_setup(monkeypatch, tmp_path)
    vendor = _minute_vendor(_minute_days("2026-06-01", "2026-09-18"))
    feed, _ = _minute_feed(vendor)

    # Two earlier backtests on the same symbol: June, then September.
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 6, 1, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 6, 30, 23, tzinfo=timezone.utc), **_MINUTE_KW)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 9, 1, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)

    # A June-to-September backtest must see July and August, not skip them.
    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 6, 1, 8, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)

    want = set(_minute_days("2026-06-01", "2026-09-18"))
    assert sorted(want - _sessions(df)) == []
    july_august = set(_minute_days("2026-07-01", "2026-08-31"))
    # One page per missing session, no more.
    assert served["pages"] == len(july_august)

    # The repaired cache now serves the whole window with zero requests.
    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 6, 1, 8, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)
    assert sorted(want - _sessions(df)) == []
    assert served["pages"] == 0


@pytest.mark.parametrize("failure", ["stopped", "older_page_error"])
def test_minute_cache_hole_left_by_interrupted_download_is_fetched_next_run(monkeypatch, tmp_path, failure) -> None:
    _minute_setup(monkeypatch, tmp_path)
    vendor = _minute_vendor(_minute_days("2026-07-01", "2026-09-18"))

    feed, _ = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 1, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 7, 31, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)

    # Extending to September stops after 15 pages. Page checkpoints (every 10 pages) or the
    # kept newer pages leave September in the cache with August missing.
    feed, _ = _minute_feed(vendor, fail_after=15, exc=_StopRun if failure == "stopped" else RuntimeError)
    _new_minute_process(monkeypatch, feed)
    try:
        ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 1, 8, tzinfo=timezone.utc),
                                   end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)
    except _StopRun:
        pass

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 1, 8, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)

    assert sorted(set(_minute_days("2026-07-01", "2026-09-18")) - _sessions(df)) == []
    assert served["pages"] > 0


def test_minute_session_without_trades_is_asked_once_then_marked(monkeypatch, tmp_path) -> None:
    """A thin symbol with no prints on one day must not be requested again by every backtest."""
    _minute_setup(monkeypatch, tmp_path)
    days = _minute_days("2026-08-03", "2026-08-14")
    quiet_day = "2026-08-07"
    vendor = _minute_vendor([d for d in days if d != quiet_day])
    feed, _ = _minute_feed(vendor)

    # The cache holds both weeks except the quiet day (for example two earlier backtests).
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 6, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 10, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    assert served["pages"] == 1
    assert quiet_day not in _sessions(df)
    assert bool(df.get("missing", pd.Series(False, index=df.index)).fillna(False).astype(bool).any()) is False

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    assert served["pages"] == 0


def test_minute_hole_in_index_regular_session_cache_is_fetched(monkeypatch, tmp_path) -> None:
    _minute_setup(monkeypatch, tmp_path)
    spx = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    kw = dict(asset=spx, quote=_USD, timestep="minute", include_after_hours=False)
    # Windows match the index session (09:30 to 16:00 ET) so only the hole needs requests.
    vendor = _minute_vendor(_minute_days("2026-07-06", "2026-07-31"), first="09:30", last="15:59")
    feed, _ = _minute_feed(vendor)

    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 7, 10, 20, tzinfo=timezone.utc), **kw)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 27, 13, 30, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 7, 31, 20, tzinfo=timezone.utc), **kw)

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 7, 31, 20, tzinfo=timezone.utc), **kw)
    assert sorted(set(_minute_days("2026-07-06", "2026-07-31")) - _sessions(df)) == []
    assert served["pages"] == len(_minute_days("2026-07-13", "2026-07-24"))


def test_minute_hole_scan_runs_once_per_series_and_window(monkeypatch, tmp_path) -> None:
    """A warm, complete cache pays for the session scan once, not on every bar."""
    _minute_setup(monkeypatch, tmp_path)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    feed, _ = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)

    scans = []
    original = ibkr_helper._missing_us_minute_sessions
    monkeypatch.setattr(ibkr_helper, "_missing_us_minute_sessions",
                        lambda *a, **k: scans.append(1) or original(*a, **k))
    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    for _ in range(5):
        ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                   end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    assert served["pages"] == 0
    assert len(scans) == 1


def _failing_minute_feed(vendor: pd.DataFrame, failing_days: set[str] | None = None):
    """IBKR feed whose requests for the given sessions (or all, when None) raise."""
    ok_feed, _ = _minute_feed(vendor)
    served = {"pages": 0}

    def _feed(url, querystring=None, headers=None, timeout=None, **kw):
        served["pages"] += 1
        end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        day = end.tz_convert(_NY).strftime("%Y-%m-%d")
        if failing_days is None or day in failing_days:
            raise RuntimeError("history payload remained invalid after rebuild")
        return ok_feed(url, querystring=querystring, headers=headers, timeout=timeout, **kw)

    return _feed, served


def _cache_two_weeks_with_a_hole(monkeypatch, vendor):
    feed, _ = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 5, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 12, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)


def test_minute_hole_session_that_failed_is_not_requested_again_within_the_cooldown(monkeypatch, tmp_path) -> None:
    """CodeRabbit on PR #1180 worried the minute repair can stall a backtest. Its cost is one request
    per missing session, the same as downloading that window cold, but a session whose request
    FAILED was asked again by every later call with a different window in the same process (a
    sliding-window strategy asks with a new window on every bar)."""
    _minute_setup(monkeypatch, tmp_path)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    _cache_two_weeks_with_a_hole(monkeypatch, vendor)

    feed, served = _failing_minute_feed(vendor, failing_days={"2026-08-07"})
    _new_minute_process(monkeypatch, feed)
    for minute in range(10):  # ten calls, each with a slightly wider window
        ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                   end_dt=datetime(2026, 8, 14, 23, minute, tzinfo=timezone.utc), **_MINUTE_KW)
    # Aug 6, 10 and 11 are fetched once; Aug 7 is asked once and not again within the cooldown.
    assert served["pages"] == 4


def test_minute_hole_repair_pauses_while_the_downloader_keeps_failing(monkeypatch, tmp_path) -> None:
    _minute_setup(monkeypatch, tmp_path)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    _cache_two_weeks_with_a_hole(monkeypatch, vendor)

    feed, served = _failing_minute_feed(vendor, failing_days=None)
    _new_minute_process(monkeypatch, feed)
    for minute in range(10):
        ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                   end_dt=datetime(2026, 8, 14, 23, minute, tzinfo=timezone.utc), **_MINUTE_KW)
    assert served["pages"] == ibkr_helper.IBKR_MINUTE_GAP_REPAIR_MAX_CONSECUTIVE_FAILURES


def test_minute_hole_repair_never_costs_more_than_a_cold_download_of_the_window(monkeypatch, tmp_path) -> None:
    """The bound that replaces a timer: at most one request per session in the window, once per
    process. A timer would leave July missing on a busy downloader and not on a quiet one."""
    _minute_setup(monkeypatch, tmp_path)
    days = _minute_days("2026-06-01", "2026-09-18")
    vendor = _minute_vendor(days)
    feed, _ = _minute_feed(vendor)
    # Only the first and last sessions are cached: every session between them is a hole.
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 6, 1, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 6, 1, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(start_dt=datetime(2026, 9, 18, 8, tzinfo=timezone.utc),
                               end_dt=datetime(2026, 9, 18, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)

    feed, served = _minute_feed(vendor)
    _new_minute_process(monkeypatch, feed)
    df = ibkr_helper.get_price_data(start_dt=datetime(2026, 6, 1, 8, tzinfo=timezone.utc),
                                    end_dt=datetime(2026, 9, 19, tzinfo=timezone.utc), **_MINUTE_KW)
    assert sorted(set(days) - _sessions(df)) == []
    assert served["pages"] <= len(days) - 2


def test_minute_hole_scan_finds_no_false_holes_across_daylight_saving_switches() -> None:
    for first, last, end in (("2026-03-02", "2026-03-13", datetime(2026, 3, 14, tzinfo=timezone.utc)),
                             ("2026-10-26", "2026-11-06", datetime(2026, 11, 7, tzinfo=timezone.utc))):
        frame = _minute_vendor(_minute_days(first, last))
        start = pd.Timestamp(f"{first} 04:00", tz=_NY).to_pydatetime()
        assert ibkr_helper._missing_us_minute_sessions(frame, start_dt=start, end_dt=end) == []
        holey = frame.loc[~frame.index.tz_convert(_NY).strftime("%Y-%m-%d").isin([_minute_days(first, last)[5]])]
        found = ibkr_helper._missing_us_minute_sessions(holey, start_dt=start, end_dt=end)
        assert [open_.date().isoformat() for open_, _ in found] == [_minute_days(first, last)[5]]


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_minute_hole_repair_retries_in_the_same_process_after_the_downloader_recovers(monkeypatch, tmp_path) -> None:
    """Release gate probe, 2026-09-25: after 3 failed repair requests the series was given up for the
    life of the process. A second backtest in the same long-lived process (a notebook, a local
    script, a multi-backtest service) ran after the downloader recovered and silently got 34 of 77
    sessions, with no request and no warning. Failed marks now expire after a cooldown."""
    _minute_setup(monkeypatch, tmp_path)
    clock = _Clock()
    monkeypatch.setattr(ibkr_helper, "_ibkr_monotonic", clock, raising=False)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    _cache_two_weeks_with_a_hole(monkeypatch, vendor)
    window = dict(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                  end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc))

    feed, _ = _failing_minute_feed(vendor, failing_days=None)
    _new_minute_process(monkeypatch, feed)
    ibkr_helper.get_price_data(**window, **_MINUTE_KW)  # backtest A: downloader down

    feed, served = _minute_feed(vendor)
    monkeypatch.setattr(ibkr_helper, "queue_request", feed)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    clock.now += ibkr_helper.IBKR_MINUTE_GAP_RETRY_COOLDOWN_SECONDS + 1
    df = ibkr_helper.get_price_data(**window, **_MINUTE_KW)  # backtest B: downloader back
    assert sorted(set(_minute_days("2026-08-03", "2026-08-14")) - _sessions(df)) == []
    assert served["pages"] == 4


def test_minute_series_served_with_unrepaired_sessions_logs_a_warning_each_time(monkeypatch, tmp_path, caplog) -> None:
    _minute_setup(monkeypatch, tmp_path)
    clock = _Clock()
    monkeypatch.setattr(ibkr_helper, "_ibkr_monotonic", clock, raising=False)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    _cache_two_weeks_with_a_hole(monkeypatch, vendor)
    feed, _ = _failing_minute_feed(vendor, failing_days=None)
    _new_minute_process(monkeypatch, feed)

    for backtest in range(3):
        caplog.clear()
        with caplog.at_level("WARNING", logger=ibkr_helper.logger.name):
            ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                       end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
        served_with_holes = [r.getMessage() for r in caplog.records if "missing 4 session" in r.getMessage()]
        assert served_with_holes and "SPY" in served_with_holes[0] and "minute" in served_with_holes[0], (
            backtest, [r.getMessage() for r in caplog.records])
        clock.now += ibkr_helper.IBKR_MINUTE_GAP_WARNING_INTERVAL_SECONDS + 1


def test_each_new_backtest_in_the_process_warns_about_unrepaired_sessions(monkeypatch, tmp_path, caplog) -> None:
    """Release gate probe: backtest B started seconds after backtest A in the same process and was
    served the same holes without a word, because the once-a-minute limit belonged to A. Every
    backtest data source sets its own downloader queue client id, so the limit is per backtest."""
    from types import SimpleNamespace

    import lumibot.tools.data_downloader_queue_client as queue_client

    _minute_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(ibkr_helper, "_ibkr_monotonic", _Clock(), raising=False)
    vendor = _minute_vendor(_minute_days("2026-08-03", "2026-08-14"))
    _cache_two_weeks_with_a_hole(monkeypatch, vendor)
    feed, _ = _failing_minute_feed(vendor, failing_days=None)
    _new_minute_process(monkeypatch, feed)

    for backtest_id in ("Backtest_aaaa1111", "Backtest_bbbb2222"):
        monkeypatch.setattr(queue_client, "_queue_client", SimpleNamespace(client_id=backtest_id))
        caplog.clear()
        with caplog.at_level("WARNING", logger=ibkr_helper.logger.name):
            ibkr_helper.get_price_data(start_dt=datetime(2026, 8, 3, 8, tzinfo=timezone.utc),
                                       end_dt=datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc), **_MINUTE_KW)
        assert any("missing 4 session" in r.getMessage() for r in caplog.records), backtest_id
