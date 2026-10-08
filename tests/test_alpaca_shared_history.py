"""Chart/backtest history must share provider bars, never filled simulation rows."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset
from lumibot.tools.alpaca_history import fetch_alpaca_bars


def provider_frame(start="2025-01-01", end="2025-04-01", symbols=("TSLA",)):
    index = pd.date_range(start, end, inclusive="left", freq="D", tz="UTC")
    frames = []
    for symbol in symbols:
        frames.append(
            pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000.0}, index=index)
        )
    return pd.concat(frames, keys=symbols, names=["symbol", "timestamp"])


def test_real_alpaca_data_repeated_closed_history_uses_shared_provider_cache(tmp_path, monkeypatch):
    from lumibot import constants

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    source = AlpacaData(
        {"API_KEY": "synthetic-key", "API_SECRET": "synthetic-secret"},
        auto_adjust=True,
        remove_incomplete_current_bar=False,
    )
    client = Mock()
    client._api_key = "synthetic-key"
    client._oauth_token = None
    client.get_stock_bars.return_value = SimpleNamespace(df=provider_frame())
    monkeypatch.setattr(source, "_get_stock_client", lambda: client)
    monkeypatch.setattr(
        "lumibot.data_sources.alpaca_data._date_n_trading_days_from_date", lambda **kwargs: datetime(2025, 1, 1).date()
    )
    shift = datetime.now(timezone.utc) - datetime(2025, 3, 31, tzinfo=timezone.utc)
    first = source._get_dataframe_from_api(Asset("TSLA"), 60, "day", timeshift=shift)
    second = source._get_dataframe_from_api(Asset("TSLA"), 60, "day", timeshift=shift)
    pd.testing.assert_frame_equal(first, second)
    assert client.get_stock_bars.call_count == 1
    assert list(tmp_path.rglob("*.parquet")), "Cache must use backtest Parquet format"


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("kind", ["stock", "option", "crypto"])
def test_real_alpaca_reader_does_not_hide_invalid_provider_bars(tmp_path, monkeypatch, bulk, kind):
    from lumibot import constants

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    source = AlpacaData(
        {"API_KEY": "synthetic-key", "API_SECRET": "synthetic-secret"},
        auto_adjust=True,
        remove_incomplete_current_bar=False,
    )
    asset = Asset("TSLA")
    symbol = "TSLA"
    if kind == "option":
        asset = Asset("TSLA", asset_type="option", expiration=datetime(2025, 4, 18).date(), strike=100, right="CALL")
        symbol = "TSLA250418C00100000"
    elif kind == "crypto":
        asset = Asset("BTC", asset_type="crypto")
        symbol = "BTC/USD"
    frame = provider_frame(symbols=[symbol])
    frame.iloc[0, frame.columns.get_loc("low")] = 500.0
    client = Mock()
    client._api_key = "synthetic-key"
    client._oauth_token = None
    client.get_stock_bars.return_value = SimpleNamespace(df=frame)
    client.get_option_bars.return_value = SimpleNamespace(df=frame)
    client.get_crypto_bars.return_value = SimpleNamespace(df=frame)
    monkeypatch.setattr(source, "_get_stock_client", lambda: client)
    monkeypatch.setattr(source, "_get_option_client", lambda: client)
    monkeypatch.setattr(source, "_get_crypto_client", lambda: client)
    monkeypatch.setattr(
        "lumibot.data_sources.alpaca_data._date_n_trading_days_from_date", lambda **kwargs: datetime(2025, 1, 1).date()
    )
    shift = datetime.now(timezone.utc) - datetime(2025, 3, 31, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="invalid OHLCV"):
        if bulk:
            source.get_bars([asset], 60, "day", timeshift=shift)
        else:
            source.get_historical_prices(asset, 60, "day", timeshift=shift)
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.fixture
def history(tmp_path, monkeypatch):
    from lumibot import constants

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    calls = []
    client = SimpleNamespace(_api_key="synthetic-key", _oauth_token=None)

    def read(request):
        calls.append(request)
        frame = provider_frame("2025-01-01", "2025-06-01", request.symbol_or_symbols)
        times = frame.index.get_level_values(1)
        return SimpleNamespace(
            df=frame[
                (times >= pd.to_datetime(request.start, utc=True)) & (times <= pd.to_datetime(request.end, utc=True))
            ]
        )

    def fetch(start="2025-01-15", end="2025-03-20", symbols=("TSLA",), **kwargs):
        request = StockBarsRequest(
            symbol_or_symbols=list(symbols),
            start=start,
            end=end,
            timeframe=kwargs.pop("timeframe", TimeFrame.Day),
            adjustment=kwargs.pop("adjustment", Adjustment.ALL),
            feed=kwargs.pop("feed", None),
            limit=kwargs.pop("limit", None),
            currency=kwargs.pop("currency", None),
            asof=kwargs.pop("asof", None),
            sort=kwargs.pop("sort", None),
        )
        return fetch_alpaca_bars(client, request, read, now=kwargs.pop("now", "2025-06-10"), **kwargs).df

    return fetch, calls, client, tmp_path


def test_overlap_downloads_only_missing_month(history):
    fetch, calls, _, _ = history
    first = fetch()
    overlap = fetch("2025-02-10", "2025-04-25")
    assert len(calls) == 2
    assert calls[1].start == datetime(2025, 4, 1, tzinfo=timezone.utc)
    assert calls[1].end == datetime(2025, 5, 1, tzinfo=timezone.utc)
    pd.testing.assert_frame_equal(
        first.loc[(slice(None), slice("2025-02-10", "2025-03-20")), :],
        overlap.loc[(slice(None), slice("2025-02-10", "2025-03-20")), :],
    )
    assert overlap.index.get_level_values(1).min() == pd.Timestamp("2025-02-10", tz="UTC")
    assert overlap.index.get_level_values(1).max() == pd.Timestamp("2025-04-25", tz="UTC")


def test_partial_month_first_read_does_not_clip_later_read(history):
    fetch, calls, _, _ = history
    assert len(fetch("2025-02-15", "2025-02-16")) == 2
    assert len(fetch("2025-02-01", "2025-02-28")) == 28
    assert len(calls) == 1
    assert calls[0].start.day == 1


def test_add_symbol_downloads_only_new_symbol_and_preserves_existing(history):
    fetch, calls, _, _ = history
    original = fetch()
    combined = fetch(symbols=("TSLA", "QQQ"))
    assert calls[1].symbol_or_symbols == ["QQQ"]
    pd.testing.assert_frame_equal(original.xs("TSLA"), combined.xs("TSLA"))
    assert set(combined.index.get_level_values(0)) == {"TSLA", "QQQ"}
    assert len(calls) == 2


@pytest.mark.parametrize(
    "change",
    [
        {"timeframe": TimeFrame.Minute},
        {"adjustment": Adjustment.RAW},
        {"feed": DataFeed.IEX},
        {"feed": DataFeed.SIP},
        {"currency": "USD"},
        {"asof": "2025-01-01"},
    ],
)
def test_data_contracts_cannot_share_bars(history, change):
    fetch, calls, _, _ = history
    fetch()
    fetch(**change)
    assert len(calls) == 2


def test_credential_scope_cannot_cross_entitlements(history):
    fetch, calls, client, _ = history
    fetch()
    client._api_key = "different-synthetic-key"
    fetch()
    assert len(calls) == 2


@pytest.mark.parametrize("kwargs", [{"refresh": True}, {"now": "2025-06-12"}, {"limit": 5}])
def test_explicit_refresh_expiry_and_limits_fetch_provider(history, kwargs):
    fetch, calls, _, _ = history
    fetch()
    fetch(**kwargs)
    assert len(calls) == 2


def test_current_month_tail_is_read_again(history):
    fetch, calls, _, _ = history
    fetch("2025-05-01", "2025-05-18", now="2025-05-20")
    fetch("2025-05-01", "2025-05-18", now="2025-05-20")
    assert len(calls) == 2
    assert calls[0].end == pd.Timestamp("2025-05-18", tz="UTC")


def test_minute_overlap_preserves_utc_sessions_through_daylight_saving(tmp_path, monkeypatch):
    from lumibot import constants

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    # These are actual session-shaped observations. The UTC open changes when
    # New York enters DST; gaps overnight/weekends must stay gaps.
    sessions = pd.bdate_range("2025-01-02", "2025-04-30")
    timestamps = (
        pd.DatetimeIndex(
            [
                stamp
                for day in sessions
                for stamp in pd.date_range(day + pd.Timedelta(hours=9, minutes=30), periods=390, freq="min")
            ]
        )
        .tz_localize("America/New_York")
        .tz_convert("UTC")
    )
    values = pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 500.0}, index=timestamps
    )
    bars = pd.concat([values], keys=["TSLA"], names=["symbol", "timestamp"])
    client = SimpleNamespace(_api_key="synthetic-minute-key", _oauth_token=None)
    calls = []

    def provider(request):
        calls.append(request)
        times = bars.index.get_level_values("timestamp")
        return SimpleNamespace(
            df=bars[
                (times >= pd.to_datetime(request.start, utc=True)) & (times <= pd.to_datetime(request.end, utc=True))
            ]
        )

    def fetch(start, end):
        request = StockBarsRequest(symbol_or_symbols=["TSLA"], start=start, end=end, timeframe=TimeFrame.Minute)
        return fetch_alpaca_bars(client, request, provider, now="2025-06-10").df

    original = fetch("2025-01-01", "2025-03-31T23:59:59Z")
    overlapping = fetch("2025-02-01", "2025-04-30T23:59:59Z")
    repeated = fetch("2025-02-01", "2025-04-30T23:59:59Z")
    assert len(calls) == 2
    assert calls[1].start == datetime(2025, 4, 1, tzinfo=timezone.utc)
    pd.testing.assert_frame_equal(overlapping, repeated)
    common = original.index.intersection(overlapping.index)
    pd.testing.assert_frame_equal(original.loc[common], overlapping.loc[common])
    assert len(common) > 15000
    assert pd.Timestamp("2025-03-07T14:30:00Z") in overlapping.index.get_level_values("timestamp")
    assert pd.Timestamp("2025-03-10T13:30:00Z") in overlapping.index.get_level_values("timestamp")
    assert not overlapping.index.has_duplicates
    assert len(overlapping) == len(values.loc["2025-02-01":"2025-04-30"])


def test_corrupt_partition_is_fetched_again(history):
    fetch, calls, _, root = history
    original = fetch()
    next(root.rglob("*.parquet")).write_bytes(b"broken")
    actual = fetch()
    pd.testing.assert_frame_equal(original, actual)
    assert len(calls) == 2


def test_wrong_partition_metadata_is_rejected(history):
    fetch, calls, _, root = history
    fetch()
    path = next(root.rglob("*.parquet"))
    rows = pd.read_parquet(path)
    rows.attrs["alpacaPartition"]["symbol"] = "QQQ"
    rows.to_parquet(path)
    fetch()
    assert len(calls) == 2


def test_provider_errors_are_visible_and_not_cached(history):
    _, _, client, root = history
    request = StockBarsRequest(symbol_or_symbols="TSLA", start="2025-01-01", end="2025-01-31", timeframe=TimeFrame.Day)

    def fail(request):
        raise RuntimeError("provider unavailable")

    with pytest.raises(RuntimeError, match="provider unavailable"):
        fetch_alpaca_bars(client, request, fail, now="2025-06-01")
    assert not list(root.rglob("*.parquet"))


def test_cache_contains_only_real_sparse_observations(history):
    _, _, client, root = history
    rows = provider_frame("2025-01-02", "2025-01-03")
    request = StockBarsRequest(symbol_or_symbols="TSLA", start="2025-01-01", end="2025-01-31", timeframe=TimeFrame.Day)
    actual = fetch_alpaca_bars(client, request, lambda _: SimpleNamespace(df=rows), now="2025-06-01").df
    pd.testing.assert_frame_equal(rows, actual)
    cached = pd.read_parquet(next(root.rglob("*.parquet")))
    assert len(cached) == 1
    assert "missing" not in cached


def test_invalid_cached_prices_are_not_used(history):
    fetch, calls, _, root = history
    original = fetch()
    path = next(root.rglob("*.parquet"))
    rows = pd.read_parquet(path)
    rows.iloc[0, rows.columns.get_loc("close")] = float("nan")
    rows.to_parquet(path)
    pd.testing.assert_frame_equal(original, fetch())
    assert len(calls) == 2


@pytest.mark.parametrize("first_reader", ["chart", "backtest"])
def test_chart_and_backtest_share_actual_s3_parquet_in_both_directions(tmp_path, monkeypatch, first_reader):
    import io
    from pathlib import Path

    import pytz

    from lumibot import constants
    from lumibot.backtesting import AlpacaBacktesting
    from lumibot.tools import backtest_cache, parquet_series_cache
    from lumibot.tools.backtest_cache import BacktestCacheManager, BacktestCacheSettings, CacheMode

    objects = {}

    class Remote:
        def download_file(self, bucket, key, path):
            if key not in objects:
                raise FileNotFoundError(key)
            Path(path).write_bytes(objects[key])

        def upload_file(self, path, bucket, key):
            objects[key] = Path(path).read_bytes()

    client = Mock(_api_key="synthetic-key", _oauth_token=None)
    client.get_stock_bars.return_value = SimpleNamespace(df=provider_frame())
    monkeypatch.setattr(
        "lumibot.data_sources.alpaca_data._date_n_trading_days_from_date", lambda **kwargs: datetime(2025, 1, 1).date()
    )

    def run(reader, root):
        monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(root))
        monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(root))
        monkeypatch.setattr("lumibot.backtesting.alpaca_backtesting.LUMIBOT_CACHE_FOLDER", str(root))
        manager = BacktestCacheManager(
            BacktestCacheSettings(
                backend="s3",
                mode=CacheMode.S3_READWRITE,
                bucket="synthetic-cache",
                prefix="sandbox/cache",
                region="us-east-1",
                version="v44",
            ),
            client_factory=lambda _: Remote(),
        )
        monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: manager)
        if reader == "chart":
            source = AlpacaData(
                {"API_KEY": "synthetic-key", "API_SECRET": "synthetic-secret"},
                auto_adjust=True,
                remove_incomplete_current_bar=False,
            )
            monkeypatch.setattr(source, "_get_stock_client", lambda: client)
            shift = datetime.now(timezone.utc) - datetime(2025, 3, 31, tzinfo=timezone.utc)
            return source._get_dataframe_from_api(Asset("TSLA"), 90, "day", timeshift=shift).tz_convert("UTC")
        source = AlpacaBacktesting.__new__(AlpacaBacktesting)
        source._auto_adjust = True
        source._refresh_cache = False
        source._data_store = {}
        source._refreshed_keys = {}
        source._stock_client = client
        source.tzinfo = pytz.UTC
        source.market = "NYSE"
        monkeypatch.setattr(source, "_load_ohlcv_into_data_store", lambda _: False)
        monkeypatch.setattr(source, "_alpaca_request", lambda method, query, **_: method(query))
        return source._history_segment(
            asset=Asset("TSLA"),
            quote=Asset("USD", asset_type="forex"),
            source_timestep="day",
            segment_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            segment_end=datetime(2025, 4, 1, tzinfo=timezone.utc),
        )

    first = run(first_reader, tmp_path / "first-process")
    calls_before = client.get_stock_bars.call_count
    second = run("backtest" if first_reader == "chart" else "chart", tmp_path / "second-process")
    assert client.get_stock_bars.call_count == calls_before
    assert objects and all(key.startswith("sandbox/cache/v44/alpaca/bars/") for key in objects)
    for body in objects.values():
        shared = pd.read_parquet(io.BytesIO(body))
        assert shared.index.tz is not None
        assert set(shared.columns) == {"open", "high", "low", "close", "volume"}
        assert not any(key in body for key in (b"synthetic-key", b"synthetic-secret"))
    common = first.index.intersection(second.index)
    assert len(common) > 50
    pd.testing.assert_frame_equal(
        first.loc[common, ["open", "high", "low", "close", "volume"]],
        second.loc[common, ["open", "high", "low", "close", "volume"]],
        check_freq=False,
    )


def test_cache_measurements_distinguish_fetch_time_from_lookup_time(history):
    fetch, calls, _, _ = history
    cold = fetch().attrs["historyCache"]
    warm = fetch().attrs["historyCache"]
    assert cold["providerReads"] == 1 and warm["providerReads"] == 0
    assert cold["providerMs"] >= 0 and warm["providerMs"] == 0
    assert cold["lookupMs"] >= 0 and warm["lookupMs"] >= 0
    assert cold["writeMs"] >= 0 and warm["writeMs"] == 0
    assert cold["oldestFetchedAt"] == warm["oldestFetchedAt"]


def test_interrupted_partition_write_does_not_destroy_valid_cache(tmp_path, monkeypatch):
    from lumibot.tools.parquet_series_cache import ParquetSeriesCache

    path = tmp_path / "partition.parquet"
    cache = ParquetSeriesCache(path, tz="UTC")
    original = provider_frame().xs("TSLA")
    cache.write(original)
    previous = path.read_bytes()

    def fail(frame, target, *args, **kwargs):
        from pathlib import Path

        Path(target).write_bytes(b"partial-write")
        raise OSError("interrupted")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail)
    with pytest.raises(OSError, match="interrupted"):
        cache.write(original.iloc[:1])
    assert path.read_bytes() == previous
    assert len(list(tmp_path.iterdir())) == 1


def test_concurrent_overlap_writers_publish_complete_months(history):
    from concurrent.futures import ThreadPoolExecutor

    fetch, _, _, root = history
    windows = [("2025-02-01", "2025-02-09"), ("2025-02-20", "2025-02-28"), ("2025-02-10", "2025-02-15")]
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda bounds: fetch(*bounds), windows))
    assert [len(frame) for frame in results] == [9, 9, 6]
    assert len(fetch("2025-02-01", "2025-02-28")) == 28
    assert all(len(pd.read_parquet(path)) == 28 for path in root.rglob("*.parquet"))


@pytest.mark.parametrize("field,value", [("close", float("nan")), ("high", 50.0), ("volume", -1.0)])
def test_invalid_provider_bars_fail_visibly_without_writing_cache(history, field, value):
    _, _, client, root = history
    rows = provider_frame("2025-02-01", "2025-02-28")
    rows.iloc[0, rows.columns.get_loc(field)] = value
    request = StockBarsRequest(symbol_or_symbols="TSLA", start="2025-02-01", end="2025-02-28", timeframe=TimeFrame.Day)
    with pytest.raises(ValueError, match="invalid OHLCV bars"):
        fetch_alpaca_bars(client, request, lambda _: SimpleNamespace(df=rows), now="2025-06-01")
    assert not list(root.rglob("*.parquet"))


def test_empty_month_is_not_falsely_recorded_as_successful_coverage(history):
    _, _, client, root = history
    read = Mock(return_value=SimpleNamespace(df=provider_frame("2025-02-01", "2025-03-01").iloc[:0]))
    request = StockBarsRequest(symbol_or_symbols="TSLA", start="2025-02-01", end="2025-02-28", timeframe=TimeFrame.Day)
    for _ in range(2):
        assert fetch_alpaca_bars(client, request, read, now="2025-06-01").df.empty
    assert read.call_count == 2
    assert not list(root.rglob("*.parquet"))


@pytest.mark.parametrize(
    "request_class,symbol", [("OptionBarsRequest", "TSLA260116C00200000"), ("CryptoBarsRequest", "BTC/USD")]
)
def test_option_contract_and_crypto_quote_have_distinct_cache_paths(history, request_class, symbol):
    from alpaca.data import requests

    _, _, client, root = history
    cls = getattr(requests, request_class)
    request = cls(symbol_or_symbols=symbol, start="2025-02-01", end="2025-02-28", timeframe=TimeFrame.Day)
    read = Mock(return_value=SimpleNamespace(df=provider_frame("2025-02-01", "2025-03-01", (symbol,))))
    expected = fetch_alpaca_bars(client, request, read, now="2025-06-01").df
    actual = fetch_alpaca_bars(client, request, read, now="2025-06-01").df
    pd.testing.assert_frame_equal(expected, actual)
    assert read.call_count == 1
    assert len(list(root.rglob("*.parquet"))) == 1
