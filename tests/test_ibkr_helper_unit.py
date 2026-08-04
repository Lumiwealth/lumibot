from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from lumibot.entities import Asset


@pytest.fixture(autouse=True)
def _reset_ibkr_history_health_state():
    from lumibot.tools.ibkr_history_health import reset_ibkr_history_health_for_testing

    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def test_ibkr_helper_caches_history_and_reuses_cache(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    ibkr_helper._RUNTIME_CONID_CACHE.clear()

    calls = {"secdef": 0, "history": 0}

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        if url.endswith("/ibkr/iserver/secdef/search"):
            calls["secdef"] += 1
            return [
                {
                    "conid": 123,
                    "sections": [{"secType": "CRYPTO", "exchange": "PAXOS"}],
                }
            ]
        if url.endswith("/ibkr/iserver/marketdata/history"):
            calls["history"] += 1
            # two 1-min bars ending at startTime (ms timestamps)
            return {
                "data": [
                    {"t": 1700000000000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10},
                    {"t": 1700000060000, "o": 2, "h": 3, "l": 2, "c": 3, "v": 11},
                ]
            }
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")
    start = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    end = datetime.fromtimestamp(1700000060, tz=timezone.utc)

    df1 = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
    )
    assert not df1.empty
    assert "open" in df1.columns
    assert "bid" in df1.columns
    assert "ask" in df1.columns
    assert "missing" not in df1.columns
    assert isinstance(df1.index, pd.DatetimeIndex)

    # NOTE: IBKR crypto backtesting requires actionable bid/ask for quote-aware fills.
    # `ibkr_helper` may fetch additional history sources (e.g. Bid_Ask + Midpoint) to
    # derive bid/ask when Trades bars don't contain separate quote fields.
    history_calls_after_first = calls["history"]

    # Second call should reuse cached data without hitting the history endpoints again.
    df2 = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
    )
    assert not df2.empty
    assert calls["history"] == history_calls_after_first
    assert calls["secdef"] == 1


def test_ibkr_helper_persists_fetched_bars_even_when_requested_window_has_no_overlap(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    ibkr_helper._RUNTIME_CONID_CACHE.clear()

    calls = {"secdef": 0, "history": 0}

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        if url.endswith("/ibkr/iserver/secdef/search"):
            calls["secdef"] += 1
            return [
                {
                    "conid": 123,
                    "sections": [{"secType": "CRYPTO", "exchange": "PAXOS"}],
                }
            ]
        if url.endswith("/ibkr/iserver/marketdata/history"):
            calls["history"] += 1
            # two 1-min bars at an earlier time range (ms timestamps)
            return {
                "data": [
                    {"t": 1700000000000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10},
                    {"t": 1700000060000, "o": 2, "h": 3, "l": 2, "c": 3, "v": 11},
                ]
            }
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")

    # Request a time window that is strictly AFTER the returned bars.
    start = datetime.fromtimestamp(1700000000 + 3600, tz=timezone.utc)
    end = datetime.fromtimestamp(1700000060 + 7200, tz=timezone.utc)

    df = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
    )
    assert df.empty

    # Multiple parquet files may be produced when `ibkr_helper` fetches/derives bid/ask.
    # We require that the Trades series was persisted even if it doesn't overlap the request.
    trades_files = list(tmp_path.rglob("*_TRADES_AHR.parquet"))
    assert len(trades_files) == 1
    cached = pd.read_parquet(trades_files[0])
    assert len(cached) == 2
    assert calls["history"] >= 1
    assert calls["secdef"] == 1


def test_ibkr_fetch_history_between_dates_keeps_chunks_on_later_empty_page(monkeypatch):
    """Mid-walk empty page in IBKR backward pagination must NOT discard earlier chunks.

    History: CME futures have weekend + nightly-maintenance closes. IBKR's history
    endpoint correctly returns `{"data": []}` when a `period`-sized backward window
    lands entirely inside a no-trading range. Previously this test asserted that
    the helper RAISED a `RuntimeError` in that case — which was the bug that
    caused every CME futures backtest to fail when the backward walk hit a
    weekend (commit `0e89a50a` "fix: short-circuit empty IBKR prefetch" restores
    the correct behaviour: break out of the loop and return whatever earlier
    chunks we already have). Locking in the correct contract here so a future
    edit can't silently reintroduce the raise.
    """
    import lumibot.tools.ibkr_helper as ibkr_helper

    asset = Asset(symbol="TSLA", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 2, tzinfo=timezone.utc)

    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **kwargs: 76792991)
    page_one = {
        "data": [
            {
                "t": int(ts.value // 1_000_000),
                "o": 400.0 + i,
                "h": 401.0 + i,
                "l": 399.0 + i,
                "c": 400.5 + i,
                "v": 100 + i,
            }
            for i, ts in enumerate(pd.date_range(end=end, periods=7, freq="B", tz=timezone.utc))
        ]
    }
    calls = {"count": 0}

    def _fake_history_request(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return page_one
        return {"data": []}

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", _fake_history_request)

    result = ibkr_helper._fetch_history_between_dates(
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=False,
        source="Trades",
        source_was_explicit=True,
    )

    # Helper must preserve the page-1 chunks and stop paging on the empty page —
    # MUST NOT raise. See commit 0e89a50a for the behavioural invariant.
    assert isinstance(result, pd.DataFrame)
    assert not result.empty, "page-1 chunks must survive the mid-walk empty page"
    assert len(result) == 7, f"expected 7 rows from page 1, got {len(result)}"
    assert calls["count"] == 2


def test_ibkr_bounded_repair_preserves_pages_before_a_later_error(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    asset = Asset(symbol="QQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_kwargs: 123)
    calls = {"count": 0}

    def _history_request(**_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "data": [
                    {
                        "t": 1760000000000 + offset * 3_600_000,
                        "o": 100 + offset,
                        "h": 101 + offset,
                        "l": 99 + offset,
                        "c": 100.5 + offset,
                        "v": 1000,
                    }
                    for offset in range(7)
                ]
            }
        raise RuntimeError("provider failed after the first repair page")

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", _history_request)
    result = ibkr_helper._fetch_history_between_dates(
        asset=asset,
        quote=quote,
        timestep="hour",
        start_dt=datetime(2025, 9, 1, tzinfo=timezone.utc),
        end_dt=datetime(2025, 10, 31, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
        _deadline_monotonic=ibkr_helper.time.perf_counter() + 60,
    )

    assert len(result) == 7
    assert calls["count"] == 2


def test_ibkr_frame_covers_requested_window_rejects_underfilled_daily_series_and_allows_flat_series():
    import lumibot.tools.ibkr_helper as ibkr_helper

    asset = Asset(symbol="TSLA", asset_type=Asset.AssetType.STOCK)
    start = datetime(2026, 2, 20, tzinfo=timezone.utc)
    end = datetime(2026, 3, 13, tzinfo=timezone.utc)

    underfilled_end = pd.Timestamp(end).tz_convert("America/New_York") - pd.Timedelta(days=10)
    underfilled_idx = pd.date_range(end=underfilled_end, periods=5, freq="B")
    underfilled = pd.DataFrame(
        {
            "open": [400, 401, 402, 403, 404],
            "high": [401, 402, 403, 404, 405],
            "low": [399, 400, 401, 402, 403],
            "close": [400.5, 401.5, 402.5, 403.5, 404.5],
            "volume": [100, 101, 102, 103, 104],
        },
        index=underfilled_idx,
    )

    flat_start = pd.Timestamp(start).tz_convert("America/New_York")
    flat_idx = pd.date_range(start=flat_start, periods=7, freq="B")
    flat = pd.DataFrame(
        {
            "open": [405.18] * 7,
            "high": [406.50] * 7,
            "low": [394.65] * 7,
            "close": [395.01] * 7,
            "volume": [0] * 7,
        },
        index=flat_idx,
    )

    assert (
        ibkr_helper.frame_covers_requested_window(
            underfilled,
            asset=asset,
            timestep="day",
            start_dt=start,
            end_dt=end,
        )
        is False
    )
    assert (
        ibkr_helper.frame_covers_requested_window(
            flat,
            asset=asset,
            timestep="day",
            start_dt=start,
            end_dt=flat_idx[-1].to_pydatetime(),
        )
        is True
    )


def test_ibkr_hourly_frame_treats_weekend_start_as_covered():
    import lumibot.tools.ibkr_helper as ibkr_helper

    asset = Asset(symbol="QQQ", asset_type=Asset.AssetType.STOCK)
    frame = pd.DataFrame(
        {"close": [100.0, 101.0]},
        index=pd.DatetimeIndex(
            [
                "2023-07-31 04:00:00-04:00",
                "2023-08-01 16:00:00-04:00",
            ]
        ),
    )

    assert ibkr_helper.frame_covers_requested_window(
        frame,
        asset=asset,
        timestep="hour",
        start_dt=datetime(2023, 7, 30, tzinfo=timezone.utc),
        end_dt=datetime(2023, 8, 1, 20, tzinfo=timezone.utc),
    )


def test_ibkr_hourly_frame_does_not_hide_missing_final_trading_day():
    import lumibot.tools.ibkr_helper as ibkr_helper

    asset = Asset(symbol="QQQ", asset_type=Asset.AssetType.STOCK)
    frame = pd.DataFrame(
        {"close": [100.0, 101.0]},
        index=pd.DatetimeIndex(
            [
                "2026-07-28 09:30:00-04:00",
                "2026-07-29 16:00:00-04:00",
            ]
        ),
    )

    assert not ibkr_helper.frame_covers_requested_window(
        frame,
        asset=asset,
        timestep="hour",
        start_dt=datetime(2026, 7, 28, 13, 30, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, 16, tzinfo=timezone.utc),
    )


def test_ibkr_downloader_payload_contract_accepts_complete_and_explicit_no_data():
    import lumibot.tools.ibkr_helper as ibkr_helper

    ibkr_helper._ensure_cacheable_downloader_history_payload(
        {
            "_botspot_meta": {
                "provider": "ibkr",
                "classification": "complete",
                "cache_write_policy": "allow",
            }
        }
    )
    ibkr_helper._ensure_cacheable_downloader_history_payload(
        {
            "_botspot_meta": {
                "provider": "ibkr",
                "classification": "explicit_no_data",
                "cache_write_policy": "negative_only",
            }
        }
    )


def test_ibkr_downloader_payload_contract_rejects_partial_or_uncacheable_history():
    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.tools.ibkr_history_health import HistoryOutcome, classify_history_failure

    with pytest.raises(
        RuntimeError,
        match="partial_history:non_cacheable_downloader_payload",
    ) as exc_info:
        ibkr_helper._ensure_cacheable_downloader_history_payload(
            {
                "_botspot_meta": {
                    "provider": "ibkr",
                    "classification": "partial",
                    "cache_write_policy": "deny",
                    "error": "partial_history:unverified_seam",
                }
            }
        )
    assert classify_history_failure(exc_info.value).outcome == HistoryOutcome.PARTIAL
    assert "unverified_seam" not in str(exc_info.value)


def test_cache_placeholder_metadata_never_reaches_strategy_frames():
    import lumibot.tools.ibkr_helper as ibkr_helper

    frame = pd.DataFrame(
        {
            "close": [100.0],
            "missing": [False],
            "missing_retry_after": ["2026-08-04T00:00:00+00:00"],
            "missing_reason": ["confirmed_no_data"],
            "missing_outcome": ["confirmed_no_data"],
        }
    )

    returned = ibkr_helper._strip_missing_cache_metadata(frame)

    assert returned.columns.tolist() == ["close"]


def test_ibkr_get_price_data_returns_real_cached_bars_when_window_stays_underfilled_after_refresh_error(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    asset = Asset(symbol="VIX", asset_type=Asset.AssetType.INDEX)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 20, tzinfo=timezone.utc)

    cache_file = ibkr_helper._cache_file_for(
        asset=asset,
        quote=quote,
        timestep="day",
        exchange=None,
        source=ibkr_helper.IBKR_DEFAULT_INDEX_HISTORY_SOURCE,
        include_after_hours=True,
    )
    stale_idx = pd.date_range(
        end=pd.Timestamp(end).tz_convert("America/New_York") - pd.Timedelta(days=10),
        periods=5,
        freq="B",
    )
    stale = pd.DataFrame(
        {
            "open": [20.0, 21.0, 22.0, 23.0, 24.0],
            "high": [21.0, 22.0, 23.0, 24.0, 25.0],
            "low": [19.0, 20.0, 21.0, 22.0, 23.0],
            "close": [20.5, 21.5, 22.5, 23.5, 24.5],
            "volume": [0, 0, 0, 0, 0],
            "missing": [False, False, False, False, False],
        },
        index=stale_idx,
    )
    ibkr_helper._write_cache_frame(cache_file, stale)

    def _raise_fetch_error(**kwargs):
        raise RuntimeError("submit timed out")

    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _raise_fetch_error)

    df = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
    )

    assert not df.empty
    assert len(df) == len(stale)
    assert df["close"].tolist() == stale["close"].tolist()


def test_stock_index_daily_period_cap_is_5y():
    """Regression guard for the daily stock/index history period cap.

    IBKR Client Portal REST caps daily responses at ~1000 data points per
    call (``IBKR_HISTORY_MAX_POINTS``). A ``5y`` request already hits that
    ceiling (verified against production on 2026-04-17: 1000 bars covering
    ~4 trading years), so it is the efficient ceiling for pagination.

    Prior value ``180d`` returned ~125 bars per call, which in practice
    caused the backward-walking loop in ``_fetch_history_between_dates``
    to complete in a single iteration (the initial fetch near real-now
    populated the cache, and the coverage check then skipped the true
    historical window for every subsequent simulation date — producing a
    flat "latest price" for the entire backtest range).

    If you need to tighten this again, write a regression test that
    reproduces the long-backtest flat-price failure first.
    """
    import lumibot.tools.ibkr_helper as ibkr_helper

    assert ibkr_helper.IBKR_STOCK_INDEX_DAILY_MAX_PERIOD == "5y"


def test_history_period_for_request_daily_stock_index_uses_cap():
    """Daily stock/index requests must use the class-wide max period so
    pagination reaches back to the requested window efficiently.

    Only tests canonical IBKR bar strings (``1d``) because that's what
    ``_timestep_to_ibkr_bar`` produces — ``day`` is a lumibot *timestep*
    label that is always converted to ``1d`` before this helper is called.
    """
    import lumibot.tools.ibkr_helper as ibkr_helper

    for asset_type in ("stock", "index"):
        for bar in ("1d", "1D"):
            period = ibkr_helper._history_period_for_request(
                asset_type=asset_type, bar=bar, source="Trades"
            )
            assert period == ibkr_helper.IBKR_STOCK_INDEX_DAILY_MAX_PERIOD, (
                f"asset_type={asset_type} bar={bar} period={period}"
            )


def test_unresolvable_stock_conid_is_terminal_no_data():
    import lumibot.tools.ibkr_helper as ibkr_helper

    assert ibkr_helper._is_terminal_no_data_error(RuntimeError("Unable to resolve IBKR conid for ARCH-DEFUNCT-2838"))
    assert ibkr_helper._is_terminal_no_data_error(RuntimeError("IBKR conid lookup is negatively cached for ARCH-DEFUNCT-2838"))


def test_ibkr_malformed_rebuild_failure_is_partial_not_terminal_no_data():
    import lumibot.tools.ibkr_helper as ibkr_helper

    assert not ibkr_helper._is_terminal_no_data_error(
        RuntimeError(
            "Request abc permanently failed: IBKR history remained invalid after rebuild "
            "(conid=72539702 period=1000min bar=1min reason=mid:malformed_history_payload)"
        )
    )
    assert not ibkr_helper._is_terminal_no_data_error(
        RuntimeError(
            "IBKR history remained invalid after rebuild "
            "(conid=72539702 period=1000min bar=1min reason=tail:malformed_history_payload)"
        )
    )
    assert not ibkr_helper._is_terminal_no_data_error(
        RuntimeError(
            "IBKR history remained invalid after rebuild "
            "(conid=72539702 period=1000min bar=1min reason=mid:overlap_bar_mismatch:1775834580000)"
        )
    )
    assert not ibkr_helper._is_terminal_no_data_error(RuntimeError("Timed out waiting for downloader queue"))


def test_ibkr_malformed_rebuild_failure_uses_process_cooldown_without_persisting_marker(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    ibkr_helper._RUNTIME_HISTORY_NO_DATA_WINDOWS.clear()

    asset = Asset(symbol="TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    start = datetime(2015, 12, 30, 3, 43, tzinfo=timezone.utc)
    end = datetime(2015, 12, 30, 20, 23, tzinfo=timezone.utc)
    calls = {"fetch": 0}

    def _raise_permanent_malformed_history(**kwargs):
        calls["fetch"] += 1
        raise RuntimeError(
            "Request 01baaf8d permanently failed: IBKR history remained invalid after rebuild "
            "(conid=72539702 period=1000min bar=1min reason=mid:malformed_history_payload)"
        )

    monkeypatch.setattr(ibkr_helper, "_fetch_history_between_dates", _raise_permanent_malformed_history)

    first = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
        source="Trades",
    )

    assert first.empty
    assert calls["fetch"] == 1

    cache_file = ibkr_helper._cache_file_for(
        asset=asset,
        quote=quote,
        timestep="minute",
        exchange=None,
        source="Trades",
        include_after_hours=True,
    )
    # Malformed history is retryable process state, not durable no-data evidence,
    # so no cache placeholder file should be created.
    assert not cache_file.exists()

    second = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=start + timedelta(minutes=5),
        end_dt=end - timedelta(minutes=5),
        exchange=None,
        include_after_hours=True,
        source="Trades",
    )

    assert second.empty
    assert calls["fetch"] == 1


def test_stock_history_identity_failure_refreshes_conid_once(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.tools.ibkr_history_health import ibkr_history_health_snapshot

    asset = Asset(symbol="GLD", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    resolve_calls = []
    history_conids = []

    def fake_resolve_conid(*, asset, quote, exchange, force_refresh=False):
        resolve_calls.append(force_refresh)
        return 51529211 if force_refresh else 54927692

    def fake_history_request(**kwargs):
        history_conids.append(kwargs["conid"])
        if kwargs["conid"] == 54927692:
            raise RuntimeError("Chart data unavailable")
        return {
            "data": [
                {
                    "t": int(datetime(2026, 7, 29, tzinfo=timezone.utc).timestamp() * 1000),
                    "o": 300.0,
                    "h": 301.0,
                    "l": 299.0,
                    "c": 300.5,
                    "v": 1000,
                }
            ]
        }

    monkeypatch.setattr(ibkr_helper, "_resolve_conid", fake_resolve_conid)
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", fake_history_request)

    result = ibkr_helper._fetch_history_between_dates(
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=datetime(2026, 7, 29, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
        source="Trades",
        source_was_explicit=False,
    )

    assert not result.empty
    assert resolve_calls == [False, True]
    assert history_conids == [54927692, 51529211]
    health = ibkr_history_health_snapshot()["series"][0]
    assert health["outcome"] == "complete"
    assert health["conid_refreshes"] == 1


def test_failed_conid_refresh_preserves_transient_history_failure(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.tools.ibkr_history_health import ibkr_history_health_snapshot

    asset = Asset(symbol="GLD", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    def fake_resolve_conid(*, force_refresh=False, **_kwargs):
        if force_refresh:
            raise RuntimeError("Unable to resolve IBKR conid during gateway outage")
        return 54927692

    monkeypatch.setattr(ibkr_helper, "_resolve_conid", fake_resolve_conid)
    def fail_history_request(**_kwargs):
        raise RuntimeError("Chart data unavailable")

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", fail_history_request)

    with pytest.raises(RuntimeError, match="Chart data unavailable"):
        ibkr_helper._fetch_history_between_dates(
            asset=asset,
            quote=quote,
            timestep="day",
            start_dt=datetime(2026, 7, 29, tzinfo=timezone.utc),
            end_dt=datetime(2026, 7, 30, tzinfo=timezone.utc),
            exchange=None,
            include_after_hours=True,
            source="Trades",
            source_was_explicit=False,
        )

    health = ibkr_history_health_snapshot()["series"][0]
    assert health["outcome"] == "transient_failure"
    assert health["conid_refreshes"] == 1


def test_unresolvable_stock_conid_uses_negative_cache(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    ibkr_helper._RUNTIME_CONID_CACHE.clear()
    ibkr_helper._NEGATIVE_CONID_CACHE.clear()
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE_LOADED", False)

    calls = {"secdef": 0}

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        if url.endswith("/ibkr/iserver/secdef/search"):
            calls["secdef"] += 1
            return []
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset(symbol="ARCH-DEFUNCT-2838", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    with pytest.raises(RuntimeError, match="Unable to resolve IBKR conid"):
        ibkr_helper._resolve_conid(asset=asset, quote=quote, exchange=None)

    assert calls["secdef"] == 2
    assert (tmp_path / "ibkr" / "conids_negative.json").exists()
    assert "stock|ARCH-DEFUNCT-2838|USD||" in ibkr_helper._NEGATIVE_CONID_CACHE

    with pytest.raises(RuntimeError, match="Unable to resolve IBKR conid"):
        ibkr_helper._resolve_conid(asset=asset, quote=quote, exchange=None)

    assert calls["secdef"] == 2


def test_stock_conid_resolution_prefers_stock_contract_for_ambiguous_symbol(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    ibkr_helper._RUNTIME_CONID_CACHE.clear()
    ibkr_helper._NEGATIVE_CONID_CACHE.clear()
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE_LOADED", False)

    seen_queries = []

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        if url.endswith("/ibkr/iserver/secdef/search"):
            seen_queries.append(dict(querystring))
            if querystring.get("secType") == "STK":
                return [
                    {
                        "conid": 265598,
                        "symbol": "MHO",
                        "sections": [{"secType": "STK", "exchange": "NYSE"}],
                    }
                ]
            return [
                {
                    "conid": 569790685,
                    "symbol": "MHO",
                    "sections": [{"secType": "FUT", "exchange": "NYMEX"}],
                },
                {
                    "conid": 265598,
                    "symbol": "MHO",
                    "sections": [{"secType": "STK", "exchange": "NYSE"}],
                },
            ]
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset(symbol="MHO", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    conid = ibkr_helper._resolve_conid(asset=asset, quote=quote, exchange=None)

    assert conid == 265598
    assert seen_queries == [{"symbol": "MHO", "secType": "STK"}]
    assert ibkr_helper._RUNTIME_CONID_CACHE["stock|MHO|USD||"] == 265598
