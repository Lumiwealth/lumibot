from __future__ import annotations

from datetime import datetime, timezone

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset


class _FakeDayData:
    def __init__(self, price: float):
        self._price = price

    def get_last_price(self, now):
        return self._price

    def get_quote(self, now):
        return {
            "close": self._price,
            "bid": self._price - 0.1,
            "ask": self._price + 0.1,
            "volume": 1000,
            "bid_size": 10,
            "ask_size": 12,
        }


class _FailingDayData:
    def get_last_price(self, now):
        raise ValueError("requested timestamp is outside of this daily slice")

    def get_quote(self, now):
        raise ValueError("requested timestamp is outside of this daily slice")


def _make_data_source() -> InteractiveBrokersRESTBacktesting:
    start = datetime(2026, 4, 9, 20, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc)
    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=start,
        datetime_end=end,
        market="NYSE",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    data_source._update_datetime(end)
    return data_source


def test_ibkr_index_get_last_price_prefers_loaded_day_series(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("VIX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FakeDayData(21.5)

    def _unexpected_update(*args, **kwargs):
        raise AssertionError("minute fetch should not run when day series is already loaded")

    monkeypatch.setattr(data_source, "_update_pandas_data", _unexpected_update)

    assert data_source.get_last_price(asset) == 21.5


def test_ibkr_stock_get_last_price_refreshes_loaded_day_series_without_minute_fetch(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FailingDayData()
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(92.75)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    assert data_source.get_last_price(asset) == 92.75
    assert len(refresh_calls) == 1


def test_ibkr_stock_get_last_price_loads_day_series_without_minute_fallback(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("PARR", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(61.25)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    assert data_source.get_last_price(asset) == 61.25
    assert len(refresh_calls) == 1


def test_ibkr_string_stock_get_last_price_uses_loaded_day_series_without_minute_fetch(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FailingDayData()
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["asset"] == asset
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(92.75)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    assert data_source.get_last_price("MU") == 92.75
    assert len(refresh_calls) == 1


def test_ibkr_stock_get_last_price_does_not_fall_back_to_minute_after_day_refresh_failure(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("CSTM", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FailingDayData()

    def _refresh_window_around_datetime(**kwargs):
        assert kwargs["dataset_key"] == "day"
        raise ValueError("daily refresh failed")

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    assert data_source.get_last_price(asset) is None


def test_ibkr_index_get_quote_prefers_loaded_day_series(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FakeDayData(5100.25)

    def _unexpected_update(*args, **kwargs):
        raise AssertionError("minute fetch should not run when day series is already loaded")

    monkeypatch.setattr(data_source, "_update_pandas_data", _unexpected_update)

    snapshot = data_source.get_quote(asset)
    assert snapshot is not None
    assert snapshot.price == 5100.25
    assert snapshot.bid == 5100.15
    assert snapshot.ask == 5100.35


def test_ibkr_stock_get_quote_refreshes_loaded_day_series_without_minute_fetch(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FailingDayData()
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(93.25)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    snapshot = data_source.get_quote(asset)
    assert snapshot is not None
    assert snapshot.price == 93.25
    assert snapshot.bid == 93.15
    assert snapshot.ask == 93.35
    assert len(refresh_calls) == 1


def test_ibkr_stock_get_quote_loads_day_series_without_minute_fallback(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("PARR", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(61.25)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    snapshot = data_source.get_quote(asset)
    assert snapshot is not None
    assert snapshot.price == 61.25
    assert snapshot.bid == 61.15
    assert snapshot.ask == 61.35
    assert len(refresh_calls) == 1


def test_ibkr_string_stock_get_quote_uses_loaded_day_series_without_minute_fetch(monkeypatch):
    data_source = _make_data_source()
    asset = Asset("MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FailingDayData()
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["asset"] == asset
        assert kwargs["dataset_key"] == "day"
        assert kwargs["include_after_hours"] is False
        data_source._data_store[day_key] = _FakeDayData(93.25)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)

    snapshot = data_source.get_quote("MU")
    assert snapshot is not None
    assert snapshot.price == 93.25
    assert snapshot.bid == 93.15
    assert snapshot.ask == 93.35
    assert len(refresh_calls) == 1
