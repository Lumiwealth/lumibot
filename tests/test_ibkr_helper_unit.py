from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset


def test_derive_crypto_daily_preserves_vendor_gaps():
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.DatetimeIndex(
        [
            LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 1, 12, 0)),
            LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 3, 12, 0)),
        ]
    )
    intraday = pd.DataFrame(
        {
            "open": [100.0, 103.0],
            "high": [101.0, 104.0],
            "low": [99.0, 102.0],
            "close": [100.5, 103.5],
            "volume": [10.0, 12.0],
        },
        index=idx,
    )

    daily = ibkr_helper._derive_daily_from_intraday(
        intraday,
        start_day=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 1)),
        end_day=LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 3)),
    )

    missing_day = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 2))
    assert bool(daily.loc[missing_day, "missing"]) is True
    assert pd.isna(daily.loc[missing_day, "close"])


def test_crypto_daily_merge_keeps_real_cached_rows_over_missing_markers():
    import lumibot.tools.ibkr_helper as ibkr_helper

    real_day = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 1))
    new_day = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 1, 2))
    df_cache = pd.DataFrame(
        {
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
            "missing": [False],
        },
        index=pd.DatetimeIndex([real_day]),
    )
    daily = pd.DataFrame(
        {
            "open": [pd.NA, 200.0],
            "high": [pd.NA, 201.0],
            "low": [pd.NA, 199.0],
            "close": [pd.NA, 200.5],
            "volume": [pd.NA, 12.0],
            "missing": [True, False],
        },
        index=pd.DatetimeIndex([real_day, new_day]),
    )

    filtered = ibkr_helper._preserve_real_daily_cache_rows(df_cache, daily)

    assert real_day not in filtered.index
    assert new_day in filtered.index


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

    with pytest.raises(RuntimeError, match="non-cacheable history payload"):
        ibkr_helper._ensure_cacheable_downloader_history_payload(
            {
                "_botspot_meta": {
                    "provider": "ibkr",
                    "classification": "partial",
                    "cache_write_policy": "deny",
                }
            }
        )


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
