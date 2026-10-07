from __future__ import annotations

from datetime import datetime, timezone

import pytest

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


def test_ibkr_option_get_last_price_uses_day_bars_in_daily_backtests(monkeypatch):
    from datetime import date

    data_source = _make_data_source()
    # Daily strategies (sleeptime "1D") prime the data source to day cadence.
    data_source._timestep = "day"
    asset = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2027, 1, 15),
        strike=100,
        right="CALL",
    )
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "day"
        data_source._data_store[day_key] = _FakeDayData(42.5)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)
    assert data_source.get_last_price(asset) == 42.5
    assert refresh_calls and refresh_calls[0]["dataset_key"] == "day"


def _intraday_option_asset():
    from datetime import date

    return Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2027, 1, 15),
        strike=100,
        right="CALL",
    )


def test_ibkr_option_get_last_price_uses_minute_bars_in_intraday_backtests(monkeypatch):
    """Intraday option marks must come from the minute bars fills use, not a stale daily close."""
    data_source = _make_data_source()
    assert data_source._timestep == "minute"
    asset = _intraday_option_asset()
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    minute_key = (asset, quote, "minute", "AUTO")
    data_source._data_store[day_key] = _FakeDayData(42.5)
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "minute"
        data_source._data_store[minute_key] = _FakeDayData(44.75)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)
    assert data_source.get_last_price(asset) == 44.75
    assert [call["dataset_key"] for call in refresh_calls] == ["minute"]


def test_ibkr_option_get_quote_uses_minute_bars_in_intraday_backtests(monkeypatch):
    data_source = _make_data_source()
    asset = _intraday_option_asset()
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    minute_key = (asset, quote, "minute", "AUTO")
    data_source._data_store[day_key] = _FakeDayData(42.5)
    refresh_calls = []

    def _refresh_window_around_datetime(**kwargs):
        refresh_calls.append(kwargs)
        assert kwargs["dataset_key"] == "minute"
        data_source._data_store[minute_key] = _FakeDayData(44.75)

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _refresh_window_around_datetime)
    snapshot = data_source.get_quote(asset)
    assert snapshot.price == 44.75
    assert snapshot.bid == 44.65
    assert [call["dataset_key"] for call in refresh_calls] == ["minute"]


def test_ibkr_option_get_quote_uses_day_bars_in_daily_backtests(monkeypatch):
    data_source = _make_data_source()
    data_source._timestep = "day"
    asset = _intraday_option_asset()
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_key = (asset, quote, "day", "AUTO")
    data_source._data_store[day_key] = _FakeDayData(42.5)

    def _unexpected_refresh(**kwargs):
        raise AssertionError("daily option quotes must not fetch minute history")

    monkeypatch.setattr(data_source, "_refresh_window_around_datetime", _unexpected_refresh)
    assert data_source.get_quote(asset).price == 42.5


# Intraday (30-minute) IBKR stock backtest, 2026-10 investigation. During the
# session stats valued two holdings on the daily series (one price per session,
# flat from the open to the close) while the strategy's own intraday bars, and its
# fills, priced them about $250 lower at 10:00 ET. Intraday runs must mark stocks
# on the intraday bars already loaded; daily runs keep the day series.
def _real_series(asset, quote, rows, timestep, native=(1, None)):
    import pandas as pd

    from lumibot.entities import Data

    index = pd.DatetimeIndex([row[0] for row in rows]).tz_convert("America/New_York")
    df = pd.DataFrame(
        {
            "open": [row[1] for row in rows],
            "high": [max(row[1], row[2]) for row in rows],
            "low": [min(row[1], row[2]) for row in rows],
            "close": [row[2] for row in rows],
            "volume": [1000 for _ in rows],
        },
        index=index,
    )
    # Mirror InteractiveBrokersRESTBacktesting._update_pandas_data construction.
    data = Data(asset, df, timestep=timestep, quote=quote)
    data.strict_end_check = timestep != "day"
    data._native_timestep_quantity = int(native[0])
    data._native_timestep_unit = native[1] or timestep
    return data


def _intraday_session_data_source(*, daily_cadence: bool):
    import pandas as pd

    start = datetime(2026, 9, 21, 13, 30, tzinfo=timezone.utc)
    end = datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)
    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=start,
        datetime_end=end,
        market="NYSE",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    if daily_cadence:
        data_source._timestep = "day"
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    # (symbol, quantity, 9/22 day open/close, 9/23 day open/close, intraday price around 10:00 ET 9/23)
    holdings = {
        "AAA": (15, (305.00, 309.00), (310.83, 299.00), 301.10),
        "BBB": (44, (122.00, 122.50), (123.71, 120.00), 121.32),
    }
    for symbol, (_qty, day22, day23, intraday) in holdings.items():
        asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        day_rows = [
            (pd.Timestamp("2026-09-22 00:00", tz="America/New_York"), *day22),
            (pd.Timestamp("2026-09-23 00:00", tz="America/New_York"), *day23),
        ]
        data_source._data_store[(asset, quote, "day", "AUTO")] = _real_series(asset, quote, day_rows, "day")
        minutes = pd.date_range("2026-09-23 09:30", "2026-09-23 10:30", freq="min", tz="America/New_York")
        minute_rows = [(ts, intraday, intraday) for ts in minutes]
        data_source._data_store[(asset, quote, "minute", "AUTO")] = _real_series(asset, quote, minute_rows, "minute")
    data_source._update_datetime(datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc))
    return data_source, holdings


def test_ibkr_intraday_backtest_marks_stocks_on_loaded_intraday_bars_not_the_day_open(monkeypatch):
    data_source, holdings = _intraday_session_data_source(daily_cadence=False)

    def _no_fetch(*args, **kwargs):
        raise AssertionError("valuation must use the already loaded series, not fetch history")

    monkeypatch.setattr(data_source, "_update_pandas_data", _no_fetch)

    holdings_value = 0.0
    for symbol, (qty, _day22, _day23, intraday) in holdings.items():
        price = data_source.get_last_price(Asset(symbol, asset_type=Asset.AssetType.STOCK))
        assert price == pytest.approx(intraday), symbol
        holdings_value += qty * price

    assert holdings_value == pytest.approx(9854.58)


def test_ibkr_daily_backtest_still_marks_stocks_on_the_day_series(monkeypatch):
    data_source, holdings = _intraday_session_data_source(daily_cadence=True)
    monkeypatch.setattr(
        data_source,
        "_update_pandas_data",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no fetch expected")),
    )
    day_series_price = {}
    for symbol in holdings:
        asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        day_series_price[symbol] = data_source._data_store[(asset, quote, "day", "AUTO")].get_last_price(
            data_source.get_datetime()
        )
        assert data_source.get_last_price(asset) == pytest.approx(day_series_price[symbol])


def test_ibkr_intraday_backtest_marks_stocks_on_loaded_hourly_bars(monkeypatch):
    import pandas as pd

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2026, 9, 21, 13, 30, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc),
        market="NYSE",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    asset = Asset("AAA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_rows = [
        (pd.Timestamp("2026-09-22 00:00", tz="America/New_York"), 305.0, 309.0),
        (pd.Timestamp("2026-09-23 00:00", tz="America/New_York"), 310.83, 299.0),
    ]
    data_source._data_store[(asset, quote, "day", "AUTO")] = _real_series(asset, quote, day_rows, "day")
    hours = pd.date_range("2026-09-23 09:00", "2026-09-23 12:00", freq="h", tz="America/New_York")
    hourly = _real_series(asset, quote, [(ts, 301.1, 301.1) for ts in hours], "minute", native=(1, "hour"))
    data_source._data_store[(asset, quote, "hour", "AUTO")] = hourly
    # 10:00 ET is the start of an hourly bar, so the loaded hourly series is fresh and the
    # position is marked on it (301.1). Before hourly keys were accepted, valuation used the
    # day series (299.0) all session. Between hourly bars Data reports the bar as stale and
    # valuation falls back to the day series.
    data_source._update_datetime(datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(
        data_source,
        "_update_pandas_data",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no fetch expected")),
    )

    assert data_source.get_last_price(asset) == pytest.approx(301.1)


def test_ibkr_intraday_valuation_prefers_the_most_recent_bar_over_a_finer_interval(monkeypatch):
    import pandas as pd

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2026, 9, 21, 13, 30, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc),
        market="NYSE",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    asset = Asset("AAA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    # The minute series has a gap after 09:59 (bars resume at 10:30); the 30-minute
    # series has a 10:00 bar. At 10:01 the newer 10:00 bar must win over 09:59.
    minutes = pd.date_range("2026-09-23 09:30", "2026-09-23 09:59", freq="min", tz="America/New_York").append(
        pd.date_range("2026-09-23 10:30", "2026-09-23 11:00", freq="min", tz="America/New_York")
    )
    data_source._data_store[(asset, quote, "minute", "AUTO")] = _real_series(
        asset, quote, [(ts, 300.0, 300.0) for ts in minutes], "minute"
    )
    halves = pd.date_range("2026-09-23 09:30", "2026-09-23 11:00", freq="30min", tz="America/New_York")
    data_source._data_store[(asset, quote, "30minute", "AUTO")] = _real_series(
        asset, quote, [(ts, 302.0, 302.5) for ts in halves], "minute", native=(30, "minute")
    )
    data_source._update_datetime(datetime(2026, 9, 23, 14, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(
        data_source,
        "_update_pandas_data",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no fetch expected")),
    )

    assert data_source.get_last_price(asset) == pytest.approx(302.0)
