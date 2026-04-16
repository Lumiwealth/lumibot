from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset


def test_ibkr_rest_backtesting_plumbs_history_source(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    calls = {"count": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls["count"] += 1
        assert source == "Bid_Ask"
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        history_source="Bid_Ask",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(
        asset=asset,
        quote=quote,
        timestep="minute",
        start_dt=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_dt=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
        exchange=None,
        include_after_hours=True,
    )

    assert calls["count"] == 1


def test_ibkr_rest_stock_daily_uses_rth(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    captured = {"include_after_hours": None, "count": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        captured["count"] += 1
        captured["include_after_hours"] = bool(include_after_hours)
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 3, tzinfo=timezone.utc),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    ds._pull_source_symbol_bars(
        asset=Asset(symbol="TQQQ", asset_type=Asset.AssetType.STOCK),
        length=2,
        timestep="day",
        quote=Asset(symbol="USD", asset_type=Asset.AssetType.FOREX),
        include_after_hours=True,
    )

    assert captured["count"] == 1
    assert captured["include_after_hours"] is False


def test_ibkr_rest_stock_daily_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 3, tzinfo=timezone.utc),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="TSLA", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    ds._pull_source_symbol_bars(
        asset=asset,
        length=200,
        timestep="day",
        quote=quote,
        include_after_hours=True,
    )

    assert (asset, quote, "day", "AUTO") not in ds._fully_loaded_series


def test_ibkr_rest_crypto_minute_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        idx = pd.date_range(start=start_dt, periods=5, freq="1min")
        return pd.DataFrame(
            {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 1.0},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    ds._pull_source_symbol_bars(
        asset=asset,
        length=30,
        timestep="minute",
        quote=quote,
        include_after_hours=True,
    )

    assert (asset, quote, "minute", "AUTO") not in ds._fully_loaded_series


def test_ibkr_rest_futures_minute_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        idx = pd.date_range(start=start_dt, periods=5, freq="1min")
        return pd.DataFrame(
            {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 1.0},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    ds._pull_source_symbol_bars(
        asset=asset,
        length=30,
        timestep="minute",
        quote=quote,
        include_after_hours=True,
    )

    assert (asset, quote, "minute", "AUTO") not in ds._fully_loaded_series
