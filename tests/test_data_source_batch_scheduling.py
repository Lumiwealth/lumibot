"""History scheduling uses the provider pool without discarding call limits."""
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as real_wait

from lumibot.entities import Asset
from tests.test_data_source_sleep_time_defaults import _DummyDataSource


def test_large_chunk_uses_available_workers(monkeypatch):
    source = _DummyDataSource(backtesting=True)
    pool = ThreadPoolExecutor(max_workers=4)
    chunks = []

    class RecordingPool:
        def submit(self, fn, chunk):
            chunks.append(chunk)
            return pool.submit(fn, chunk)

    monkeypatch.setattr(source, "_get_or_create_thread_pool", lambda: RecordingPool())
    monkeypatch.setattr(source, "get_historical_prices", lambda *, asset, **kwargs: asset.symbol)
    try:
        result = source.get_bars([Asset(f"S{i}") for i in range(8)], 100,
            chunk_size=100, max_workers=200)
        assert list(sorted(result.values())) == [f"S{i}" for i in range(8)]
        assert not result.errors
        assert [len(chunk) for chunk in chunks] == [2, 2, 2, 2]
    finally:
        pool.shutdown()


def test_batch_preserves_quote_and_history_arguments(monkeypatch):
    source = _DummyDataSource(backtesting=True)
    calls = []
    base, quote = Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex")

    def history(**kwargs):
        calls.append(kwargs)
        return {"bars": [1, 2]}

    monkeypatch.setattr(source, "get_historical_prices", history)
    try:
        result = source.get_bars([(base, quote)], length=100, timestep="5min", timeshift=1,
            exchange="TEST", include_after_hours=False, max_workers=1)
        assert calls == [dict(asset=base, quote=quote, length=100, timestep="5min",
            timeshift=1, exchange="TEST", include_after_hours=False)]
        assert result[(base, quote)] == {"bars": [1, 2]}
    finally:
        source.shutdown()


def test_per_call_limit_bounds_submitted_work(monkeypatch):
    source = _DummyDataSource(backtesting=True)
    pool = ThreadPoolExecutor(max_workers=4)
    submitted = []
    waits = []

    class RecordingPool:
        def submit(self, *args):
            future = pool.submit(*args)
            submitted.append(future)
            return future

    def checked_wait(futures, **kwargs):
        waits.append(len(futures))
        assert len(futures) <= 1
        return real_wait(futures, **kwargs)

    monkeypatch.setattr(source, "_get_or_create_thread_pool", lambda: RecordingPool())
    monkeypatch.setattr("concurrent.futures.wait", checked_wait)
    try:
        result = source.get_bars([Asset(f"S{i}") for i in range(6)], 10,
            chunk_size=1, max_workers=1)
        assert len(result) == 6
        assert len(submitted) == 6
        assert all(future.done() for future in submitted)
        assert waits == [1] * 6
    finally:
        pool.shutdown()
