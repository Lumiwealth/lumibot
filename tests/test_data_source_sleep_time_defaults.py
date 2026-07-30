import pytest

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset


class _DummyDataSource(DataSource):
    SOURCE = "DUMMY"
    TIMESTEP_MAPPING = []

    def __init__(self, *, backtesting: bool):
        # Do not call DataSource.__init__ (it expects real provider setup).
        self.IS_BACKTESTING_DATA_SOURCE = backtesting
        self._thread_pool = None
        self._thread_pool_max_workers = 4

    def _get_or_create_thread_pool(self):
        # Use a real thread pool from the base implementation.
        return super()._get_or_create_thread_pool()

    def get_historical_prices(self, *args, **kwargs):
        # Return a non-None sentinel; DataSource.get_bars only cares about exceptions.
        return {"ok": True}

    def get_last_price(self, asset, quote=None, exchange=None):
        return 0.0

    def get_chains(self, asset):
        return {}


def test_get_bars_default_sleep_time_is_zero_in_backtesting(monkeypatch):
    ds = _DummyDataSource(backtesting=True)

    calls = {"n": 0}

    def _sleep(_):
        calls["n"] += 1

    monkeypatch.setattr("lumibot.data_sources.data_source.time.sleep", _sleep)

    ds.get_bars([Asset("SPY")], length=1, timestep="minute", chunk_size=1, max_workers=1)
    assert calls["n"] == 0


def test_get_bars_default_sleep_time_applies_in_live(monkeypatch):
    ds = _DummyDataSource(backtesting=False)

    calls = {"n": 0}

    def _sleep(_):
        calls["n"] += 1

    monkeypatch.setattr("lumibot.data_sources.data_source.time.sleep", _sleep)

    ds.get_bars([Asset("SPY"), Asset("AAPL")], length=1, timestep="minute", chunk_size=1, max_workers=1)
    # One sleep per asset by default.
    assert calls["n"] == 2


class _HttpError(RuntimeError):
    def __init__(self, status_code):
        super().__init__("sensitive provider response must not cross the boundary")
        self.status_code = status_code


def test_get_bars_preserves_partial_results_with_sanitized_error_metadata(monkeypatch):
    ds = _DummyDataSource(backtesting=True)

    def _history(*, asset, **_kwargs):
        if asset.symbol == "RATE":
            raise _HttpError(429)
        if asset.symbol == "AUTH":
            raise _HttpError(401)
        if asset.symbol == "UNSUPPORTED":
            raise NotImplementedError("provider detail")
        return {"symbol": asset.symbol}

    monkeypatch.setattr(ds, "get_historical_prices", _history)
    assets = [
        Asset("OK"),
        Asset("RATE"),
        Asset("AUTH"),
        Asset("UNSUPPORTED"),
    ]
    result = ds.get_bars(
        assets,
        length=10,
        timestep="day",
        chunk_size=2,
        sleep_time=0,
    )

    assert result[assets[0]] == {"symbol": "OK"}
    assert result[assets[1]] is None
    assert result.errors[assets[1]] == {
        "category": "unavailable",
        "errorType": "data_unavailable",
        "retryable": True,
    }
    assert result.errors[assets[2]]["category"] == "unavailable"
    assert result.errors[assets[2]]["retryable"] is True
    assert result.errors[assets[3]]["category"] == "unsupported"
    assert result.errors[assets[3]]["errorType"] == "unsupported_operation"
    assert result.errors[assets[3]]["retryable"] is False
    assert "sensitive provider response" not in str(result.errors)


def test_get_bars_preserves_tuple_string_failures_as_partial_results(
    monkeypatch, caplog
):
    ds = _DummyDataSource(backtesting=True)

    def _history(*, asset, **_kwargs):
        if asset == "BTC":
            raise RuntimeError("provider detail")
        return {"symbol": asset.symbol}

    monkeypatch.setattr(ds, "get_historical_prices", _history)
    pair = ("BTC", "USD")
    result = ds.get_bars(
        [pair, Asset("SPY")],
        length=10,
        timestep="day",
        chunk_size=2,
        sleep_time=0,
    )

    assert result[pair] is None
    assert result.errors[pair] == {
        "category": "unavailable",
        "errorType": "data_unavailable",
        "retryable": True,
    }
    assert "BTC" not in caplog.text
    assert "provider detail" not in caplog.text
    assert "Error retrieving data for str" in caplog.text


def test_get_bars_rejects_duplicate_assets_before_provider_work(monkeypatch):
    ds = _DummyDataSource(backtesting=True)
    provider_called = False

    def _history(**_kwargs):
        nonlocal provider_called
        provider_called = True
        return {"ok": True}

    monkeypatch.setattr(ds, "get_historical_prices", _history)

    with pytest.raises(ValueError, match="duplicate entries"):
        ds.get_bars(
            ["SPY", "SPY"],
            length=10,
            timestep="day",
            chunk_size=1,
            sleep_time=0,
        )

    assert provider_called is False
