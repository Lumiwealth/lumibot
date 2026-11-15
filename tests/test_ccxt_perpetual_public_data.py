"""Public provider contracts; real local/shared cache writes, never a network call."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import Mock

import ccxt
import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ccxt_data_store as store


DATA = json.loads((Path(__file__).parent / "fixtures/ccxt_public_perpetual_data.json").read_text())
NOW = datetime(2026, 11, 15, 12)
START = pd.Timestamp("2026-11-13", tz="UTC")
END = START + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
INVERSE = Asset("BTC/USD:BTC", "crypto_future")


@pytest.fixture
def public_cache(tmp_path, monkeypatch):
    from lumibot import constants
    from lumibot.tools import backtest_cache, parquet_series_cache, shared_bar_cache
    monkeypatch.setattr(store, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(backtest_cache, "get_backtest_cache", lambda: None)
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: None)
    monkeypatch.setattr(store.CcxtCacheDB, "_current_utc_naive", lambda self: NOW)
    monkeypatch.delenv("LUMIBOT_SHARED_BARS", raising=False)
    monkeypatch.delenv("LUMIBOT_SHARED_BARS_SCOPE", raising=False)
    shared_bar_cache._reset_process_memory()
    original = ccxt.phemex
    apis = []

    def factory(exchange="phemex"):
        api = original()
        api.id = exchange
        markets = []
        for native, raw in DATA["products"].items():
            inverse = native == "BTCUSD"
            quote, settle = raw["quoteCurrency"], raw["settleCurrency"]
            markets.append({"id": native, "symbol": f"BTC/{quote}:{settle}", "base": "BTC",
                "baseId": "BTC", "quote": quote, "quoteId": quote, "settle": settle,
                "settleId": settle, "type": "swap", "spot": False, "swap": True,
                "future": False, "option": False, "contract": True, "inverse": inverse,
                "linear": not inverse, "active": True, "contractSize": raw.get("contractSize"),
                "priceScale": raw.get("priceScale"), "valueScale": 8,
                "precision": {"price": raw.get("tickSize", .1), "amount": raw.get("lotSize", .001)},
                "limits": {}, "info": deepcopy(raw)})
        def load():
            api.set_markets(deepcopy(markets))
            return api.markets
        api.load_markets = Mock(side_effect=load)
        api.has["fetchFundingRateHistory"] = True
        api.v2GetApiDataPublicDataFundingRateHistory = Mock(return_value=deepcopy(DATA["funding"]))
        api.publicGetMdV2KlineList = Mock(return_value=deepcopy(DATA["trade_candles"]))
        apis.append(api)
        return api
    monkeypatch.setattr(store.ccxt, "phemex", factory)
    monkeypatch.setattr(store.ccxt, "other", lambda: factory("other"), raising=False)
    return tmp_path, apis


def test_public_market_snapshot_reuses_across_instances_and_expires(public_cache, monkeypatch):
    root, apis = public_cache
    first = store.CcxtCacheDB("phemex")
    spec = first.get_contract_spec(INVERSE)
    assert (spec.base, spec.quote, spec.settle, spec.inverse, spec.contract_size) == ("BTC", "USD", "BTC", True, 1)
    assert first.get_contract_spec((INVERSE, Asset("USD", "crypto"))) == spec
    assert first.get_market(Asset("BTCUSD", "crypto_future"))["symbol"] == "BTC/USD:BTC"
    for settle in ("USDT", "USDC"):
        linear = first.get_contract_spec(Asset(f"BTC/{settle}:{settle}", "crypto_future"))
        assert (linear.base, linear.quote, linear.settle, linear.inverse,
                linear.contract_size, linear.quantity_unit) == ("BTC", settle, settle, False, 1, "contracts")
    second = store.CcxtCacheDB("phemex")
    assert second.get_contract_spec(INVERSE) == spec
    assert second.get_contract_spec(Asset("BTC/USDC:USDC", "crypto_future")) == linear
    assert [api.load_markets.call_count for api in apis] == [1, 0]
    snapshot = json.loads((root / "ccxt/phemex/markets.json").read_text())
    assert snapshot["schema"] == 1 and snapshot["fetchedAt"] and snapshot["currencies"]
    assert snapshot["markets"]["BTC/USD:BTC"]["info"]["markSymbol"] == ".MBTC"
    monkeypatch.setattr(store.CcxtCacheDB, "_current_utc_naive", lambda self: NOW + timedelta(hours=2))
    store.CcxtCacheDB("phemex")
    assert apis[-1].load_markets.call_count == 1


def test_sparse_funding_month_is_shared_and_keeps_recorded_rates(public_cache):
    root, apis = public_cache
    first = store.CcxtCacheDB("phemex")
    observed_end = START + pd.Timedelta(hours=16)
    rows = first.get_funding_events(INVERSE, START, observed_end)
    assert [r["fundingRate"] for r in rows] == ["0.0001", "0.00009158", "0.0001"]
    assert [r["timestamp"] for r in rows] == list(pd.date_range(START, periods=3, freq="8h"))
    second = store.CcxtCacheDB("phemex")
    assert second.get_funding_events(INVERSE, START, observed_end) == rows
    assert [a.v2GetApiDataPublicDataFundingRateHistory.call_count for a in apis] == [1, 0]
    paths = list((root / "ccxt/phemex/funding").rglob("2026-11.parquet"))
    assert len(paths) == 1
    cached = pd.read_parquet(paths[0])
    assert len(cached) == 3 and {"fundingRate", "coverageStart", "coverageEnd"} <= set(cached)
    assert first.api.v2GetApiDataPublicDataFundingRateHistory.call_args.args[0]["symbol"] == ".BTCFR8H"


@pytest.mark.parametrize("failure", ["unsupported", "empty", "error", "nonadvancing"])
def test_unavailable_funding_is_visible_and_not_persisted(public_cache, failure):
    root, _ = public_cache
    cache = store.CcxtCacheDB("other")
    cache.api.has["fetchFundingRateHistory"] = failure != "unsupported"
    if failure == "error":
        cache.api.fetch_funding_rate_history = Mock(side_effect=ConnectionError("recorded service failure"))
    elif failure == "nonadvancing":
        cache.api.fetch_funding_rate_history = Mock(return_value=[{"timestamp": int(START.timestamp()*1000)-1,
            "fundingRate": ".001"}] * 100)
    else:
        cache.api.fetch_funding_rate_history = Mock(return_value=[])
    if failure in {"error", "nonadvancing"}:
        with pytest.raises((ConnectionError, ValueError)):
            cache.get_funding_events(INVERSE, START, END)
    else:
        assert cache.get_funding_events(INVERSE, START, END) is None
    assert cache.get_funding_events(INVERSE, START, END) is None
    assert cache.api.fetch_funding_rate_history.call_count <= 1
    assert not list((root / "ccxt/other/funding").rglob("*.parquet"))


def test_funding_request_clips_future_and_preserves_sparse_gaps(public_cache):
    _, _apis = public_cache
    cache = store.CcxtCacheDB("phemex")
    cache.get_funding_events(INVERSE, START, pd.Timestamp("2026-12-01", tz="UTC"))
    calls = cache.api.v2GetApiDataPublicDataFundingRateHistory.call_args_list
    assert calls and all(c.args[0]["end"] <= int(NOW.replace(tzinfo=timezone.utc).timestamp()*1000) for c in calls)


def _funding_answer_through(params, *, days):
    answer = deepcopy(DATA["funding"])
    answer["data"]["rows"] = [dict(row, fundingTime=row["fundingTime"] + day * 86400000)
        for day in range(days) for row in DATA["funding"]["data"]["rows"]
        if params["start"] <= row["fundingTime"] + day * 86400000 <= params["end"]]
    return answer


def test_partial_funding_response_keeps_tail_open_for_later_publication(public_cache):
    root, _ = public_cache
    through = END + pd.Timedelta(days=1)
    first = store.CcxtCacheDB("phemex")
    first.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=1)
    assert len(first.get_funding_events(INVERSE, START, through)) == 3
    cached = pd.read_parquet(next((root / "ccxt/phemex/funding").rglob("*.parquet")))
    assert pd.to_datetime(cached["coverageEnd"], utc=True).max() <= cached.index.max()

    later = store.CcxtCacheDB("phemex")
    later.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=2)
    rows = later.get_funding_events(INVERSE, START, through)
    assert len(rows) == 6
    assert [row["timestamp"] for row in rows] == list(pd.date_range(START, periods=6, freq="8h"))
    assert later.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1
    assert later.get_funding_coverage(INVERSE) == {"source": "ccxt:phemex", "first": START,
        "last": START + pd.Timedelta(hours=40), "interval": pd.Timedelta(hours=8)}


def test_unknown_funding_tail_keeps_known_settlements_and_memoizes_query(public_cache, monkeypatch):
    from lumibot.tools.parquet_series_cache import ParquetSeriesCache

    first = store.CcxtCacheDB("phemex")
    expected = first.get_funding_events(INVERSE, START, START + pd.Timedelta(hours=16))
    later = store.CcxtCacheDB("phemex")
    later.api.v2GetApiDataPublicDataFundingRateHistory.return_value = {"code": 0, "data": {"rows": []}}
    assert later.get_funding_events(INVERSE, START, END) == expected
    assert later.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1
    monkeypatch.setattr(ParquetSeriesCache, "read", Mock(side_effect=AssertionError("repeated parquet read")))
    assert later.get_funding_events(INVERSE, START, END) == expected
    assert later.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1


def test_empty_funding_interval_does_not_poison_later_settlement(public_cache):
    cache = store.CcxtCacheDB("phemex")
    cache.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=1)
    assert cache.get_funding_events(INVERSE, START + pd.Timedelta(hours=1), START + pd.Timedelta(hours=2)) is None
    assert cache.get_funding_coverage(INVERSE) == {"source": "ccxt:phemex", "first": None,
        "last": None, "interval": pd.Timedelta(hours=8)}
    rows = cache.get_funding_events(INVERSE, START + pd.Timedelta(hours=7), START + pd.Timedelta(hours=8))
    assert len(rows) == 1 and rows[0]["timestamp"] == START + pd.Timedelta(hours=8)
    assert cache.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 2


def test_existing_overstated_funding_coverage_is_clamped_to_observed_rows(public_cache):
    root, _ = public_cache
    first = store.CcxtCacheDB("phemex")
    first.get_funding_events(INVERSE, START, START + pd.Timedelta(hours=16))
    path = next((root / "ccxt/phemex/funding").rglob("*.parquet"))
    cached = pd.read_parquet(path)
    cached["coverageEnd"] = (END + pd.Timedelta(days=1)).isoformat()
    cached.to_parquet(path)
    later = store.CcxtCacheDB("phemex")
    later.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=2)
    assert len(later.get_funding_events(INVERSE, START, END + pd.Timedelta(days=1))) == 6
    assert later.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1


@pytest.mark.parametrize("independent_managers", [False, True], ids=["shared-manager", "separate-managers"])
def test_stale_funding_writer_retains_other_instance_settlements(public_cache, monkeypatch, independent_managers):
    import io
    from lumibot.tools import backtest_cache, parquet_series_cache
    from tests.test_backtest_cache_manager import ConditionalS3Client, _build_settings, _parquet_bytes

    root, _ = public_cache
    # Existing S3 response/CAS contract fake, also recording the old blind-upload
    # path so the red failure reproduces an actual destructive publication.
    class RecordingS3(ConditionalS3Client):
        def __init__(self, body):
            super().__init__(body)
            self.blind_uploads = 0

        def upload_file(self, source, bucket, key):
            self.blind_uploads += 1
            self.body = Path(source).read_bytes()
            self.etag = f'"blind-{self.blind_uploads}"'

    seed = pd.DataFrame({"fundingRate": ["0.0001"], "coverageStart": [START.isoformat()],
        "coverageEnd": [START.isoformat()]}, index=pd.DatetimeIndex([START]))
    remote = RecordingS3(_parquet_bytes(seed))
    first, second = store.CcxtCacheDB("phemex"), store.CcxtCacheDB("phemex")
    managers = [backtest_cache.BacktestCacheManager(_build_settings(), client_factory=lambda _: remote)
                for _ in range(2 if independent_managers else 1)]
    active = [managers[0]]
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: active[0])
    for cache in (first, second):
        cache.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=1)

    first.get_funding_events(INVERSE, START, START)  # Stale in-memory month has 00:00 only.
    active[0] = managers[-1]
    second.get_funding_events(INVERSE, START + pd.Timedelta(hours=8), START + pd.Timedelta(hours=8))
    active[0] = managers[0]
    first.get_funding_events(INVERSE, START + pd.Timedelta(hours=16), START + pd.Timedelta(hours=16))

    expected = pd.date_range(START, periods=3, freq="8h")
    remote_rows = pd.read_parquet(io.BytesIO(remote.body))
    local_rows = pd.read_parquet(next((root / "ccxt/phemex/funding").rglob("*.parquet")))
    pd.testing.assert_index_equal(remote_rows.index, expected, check_names=False)
    pd.testing.assert_frame_equal(local_rows, remote_rows)
    assert remote_rows["fundingRate"].tolist() == ["0.0001", "0.00009158", "0.0001"]
    assert remote.blind_uploads == 0
    assert remote.put_calls == (3 if independent_managers else 2)  # Separate writer retries its stale ETag.
    assert first._funding_frames[(INVERSE.symbol, "2026-11")].index.equals(expected)
    for _bounds, rows in remote_rows.groupby(["coverageStart", "coverageEnd"]):
        assert pd.to_datetime(rows["coverageEnd"], utc=True).max() <= rows.index.max()
    calls = first.api.v2GetApiDataPublicDataFundingRateHistory.call_count
    monkeypatch.setattr(parquet_series_cache.ParquetSeriesCache, "read", Mock(side_effect=AssertionError("per-bar disk read")))
    first.get_funding_events(INVERSE, START + pd.Timedelta(hours=16), START + pd.Timedelta(hours=16))
    assert first.api.v2GetApiDataPublicDataFundingRateHistory.call_count == calls


def test_simultaneous_funding_writers_preserve_local_and_remote_union(public_cache, monkeypatch):
    import io
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from lumibot.tools import backtest_cache, parquet_series_cache
    from tests.test_backtest_cache_manager import ConditionalS3Client, _build_settings, _parquet_bytes

    root, _ = public_cache
    class RecordingS3(ConditionalS3Client):
        lock = threading.Lock()

        def put_object(self, **kwargs):
            with self.lock:
                return super().put_object(**kwargs)

    seed = pd.DataFrame({"fundingRate": ["0.0001"], "coverageStart": [START.isoformat()],
        "coverageEnd": [START.isoformat()]}, index=pd.DatetimeIndex([START]))
    remote = RecordingS3(_parquet_bytes(seed))
    caches = [store.CcxtCacheDB("phemex"), store.CcxtCacheDB("phemex")]
    manager = backtest_cache.BacktestCacheManager(_build_settings(), client_factory=lambda _: remote)
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: manager)
    for cache in caches:
        cache.api.v2GetApiDataPublicDataFundingRateHistory.side_effect = lambda params: _funding_answer_through(params, days=1)
        cache.get_funding_events(INVERSE, START, START)
    path = next((root / "ccxt/phemex/funding").rglob("*.parquet"))
    ready = threading.Barrier(2, timeout=3)
    first_replaced, second_replaced = threading.Event(), threading.Event()
    first_writer, role_guard = [], threading.Lock()
    original_write = parquet_series_cache.ParquetSeriesCache.write
    original_replace = Path.replace

    def prepared_old_month(cache, frame, **kwargs):
        if cache.path == path:
            assert len(frame) == 2  # Each caller has seed + its own new settlement.
            ready.wait()  # Both old caller snapshots are ready before either write.
        return original_write(cache, frame, **kwargs)

    def overlap_replace(source, target):
        if Path(target) != path:
            return original_replace(source, target)
        name = threading.current_thread().name
        with role_guard:
            if not first_writer:
                first_writer.append(name)
            is_first = first_writer[0] == name
        if not is_first:
            assert first_replaced.wait(3)
        value = original_replace(source, target)
        if is_first:
            first_replaced.set()
            # With correct path serialization the second replacement waits;
            # without it this forces B to replace A before A publishes.
            second_replaced.wait(.2)
        else:
            second_replaced.set()
        return value

    monkeypatch.setattr(parquet_series_cache.ParquetSeriesCache, "write", prepared_old_month)
    monkeypatch.setattr(Path, "replace", overlap_replace)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="funding-overlap") as pool:
        pending = [pool.submit(cache.get_funding_events, INVERSE, START + pd.Timedelta(hours=hours),
                              START + pd.Timedelta(hours=hours)) for cache, hours in zip(caches, (8, 16))]
        results = [future.result(timeout=5) for future in pending]
    expected = pd.date_range(START, periods=3, freq="8h")
    remote_rows = pd.read_parquet(io.BytesIO(remote.body))
    pd.testing.assert_index_equal(remote_rows.index, expected, check_names=False)
    pd.testing.assert_frame_equal(pd.read_parquet(path), remote_rows)
    assert remote_rows["fundingRate"].tolist() == ["0.0001", "0.00009158", "0.0001"]
    assert all(rows is not None and len(rows) == 1 for rows in results)


def test_funding_coverage_fetches_only_uncovered_requested_ranges(public_cache):
    cache = store.CcxtCacheDB("phemex")
    cache.get_funding_events(INVERSE, START, END)
    assert cache.api.v2GetApiDataPublicDataFundingRateHistory.call_args.args[0]["start"] == int(START.timestamp()*1000)
    later = store.CcxtCacheDB("phemex")
    shifted = deepcopy(DATA["funding"])
    for row in shifted["data"]["rows"]:
        row["fundingTime"] += 86400000
    later.api.v2GetApiDataPublicDataFundingRateHistory.return_value = shifted
    rows = later.get_funding_events(INVERSE, START, END + pd.Timedelta(days=1))
    assert len(rows) == 6
    assert later.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1
    query = later.api.v2GetApiDataPublicDataFundingRateHistory.call_args.args[0]
    # The first request did not prove its unobserved tail complete.
    assert query["start"] == int((START + pd.Timedelta(hours=16, milliseconds=1)).timestamp()*1000)


def test_nonfinite_funding_never_creates_persisted_coverage(public_cache):
    root, _ = public_cache
    cache = store.CcxtCacheDB("phemex")
    response = deepcopy(DATA["funding"])
    response["data"]["rows"][0]["fundingRate"] = "NaN"
    cache.api.v2GetApiDataPublicDataFundingRateHistory.return_value = response
    with pytest.raises(ValueError, match="finite"):
        cache.get_funding_events(INVERSE, START, END)
    assert cache.get_funding_events(INVERSE, START, END) is None
    assert not list((root / "ccxt/phemex/funding").rglob("*.parquet"))


def test_successful_funding_history_is_not_reread_for_every_bar(public_cache, monkeypatch):
    from lumibot.tools.parquet_series_cache import ParquetSeriesCache
    cache = store.CcxtCacheDB("phemex")
    expected = cache.get_funding_events(INVERSE, START, END)
    monkeypatch.setattr(ParquetSeriesCache, "read", Mock(side_effect=AssertionError("repeated parquet read")))
    assert cache.get_funding_events(INVERSE, START, END) == expected
    assert cache.api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1


def test_mark_history_uses_recorded_native_prices_and_distinct_shared_series(public_cache):
    _root, _apis = public_cache
    cache = store.CcxtCacheDB("phemex")
    def answer(params):
        return deepcopy(DATA["mark_candles"] if params["symbol"] == ".MBTC" else DATA["trade_candles"])
    cache.api.publicGetMdV2KlineList = Mock(side_effect=answer)
    finish = START + pd.Timedelta(minutes=9)
    trade = cache.download_ohlcv("BTC/USD:BTC", "1m", START.to_pydatetime(), finish.to_pydatetime())
    mark = cache.download_mark_ohlcv(INVERSE, "1m", START.to_pydatetime(), finish.to_pydatetime())
    assert trade.iloc[1]["close"] == 71346.3
    assert mark.iloc[1]["close"] == 71346.9345
    before = cache.api.publicGetMdV2KlineList.call_count
    again = cache.download_mark_ohlcv(INVERSE, "1m", START.to_pydatetime(), finish.to_pydatetime())
    pd.testing.assert_frame_equal(mark, again)
    assert cache.api.publicGetMdV2KlineList.call_count == before
    mark_series = cache.shared_series("BTC/USD:BTC", "1m", price="mark")
    assert mark_series.digest != cache.shared_series("BTC/USD:BTC", "1m").digest
    other = store.CcxtCacheDB("other")
    assert mark_series.digest != other.shared_series("BTC/USD:BTC", "1m", price="mark").digest


def test_unsupported_mark_history_does_not_substitute_trade_bars(public_cache):
    cache = store.CcxtCacheDB("other")
    cache.api.has["fetchMarkOHLCV"] = False
    cache.api.fetch_ohlcv = Mock(side_effect=AssertionError("trade substitution"))
    assert cache.download_mark_ohlcv(INVERSE, "1m", START, END) is None
    cache.api.fetch_ohlcv.assert_not_called()


def test_settlement_mark_uses_exact_minute_open_never_future_close(public_cache):
    cache = store.CcxtCacheDB("phemex")
    frame = pd.DataFrame({"open": [71366.7], "close": [90000.]}, index=[START])
    cache.download_mark_ohlcv = Mock(return_value=frame)
    assert cache.get_perpetual_mark(INVERSE, START) == (71366.7, True)
    assert cache.get_perpetual_mark(INVERSE, START + pd.Timedelta(seconds=30)) == (71366.7, False)


def test_missing_settlement_minute_never_returns_stale_mark_as_exact(public_cache):
    cache = store.CcxtCacheDB("phemex")
    cache.download_mark_ohlcv = Mock(return_value=pd.DataFrame({"open": [71366.7]},
        index=[START - pd.Timedelta(minutes=1)]))
    assert cache.get_perpetual_mark(INVERSE, START) == (None, False)
