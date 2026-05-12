from __future__ import annotations

from datetime import UTC, datetime

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


def _make_data_source() -> InteractiveBrokersRESTBacktesting:
    start = datetime(2026, 4, 9, 20, 0, tzinfo=UTC)
    end = datetime(2026, 4, 10, 20, 0, tzinfo=UTC)
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
