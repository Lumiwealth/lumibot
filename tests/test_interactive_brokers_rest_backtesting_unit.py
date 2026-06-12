from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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


def test_ibkr_rest_crypto_last_price_rejects_stale_underfilled_intraday_frame(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    ny = ZoneInfo("America/New_York")
    stale_dt = datetime(2026, 3, 24, 0, 0, tzinfo=ny)
    sim_dt = datetime(2026, 4, 17, 10, 0, tzinfo=ny)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        idx = pd.DatetimeIndex([stale_dt])
        return pd.DataFrame(
            {
                "open": [70511.75],
                "high": [70511.75],
                "low": [70511.75],
                "close": [70511.75],
                "bid": [70510.75],
                "ask": [70512.75],
                "volume": [1.0],
            },
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2026, 3, 9, tzinfo=ny),
        datetime_end=datetime(2026, 6, 5, tzinfo=ny),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._update_datetime(sim_dt)

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    assert ds.get_last_price(asset, quote=quote) is None

    key = (asset, quote, "minute", "AUTO")
    data = ds._data_store[key]
    assert data.strict_end_check is True
    assert float(data.df["close"].iloc[-1]) == 70511.75


def test_ibkr_rest_crypto_last_price_rejects_future_underfilled_intraday_frame(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    ny = ZoneInfo("America/New_York")
    future_dt = datetime(2026, 3, 24, 0, 0, tzinfo=ny)
    sim_dt = datetime(2026, 3, 12, 7, 0, tzinfo=ny)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        idx = pd.DatetimeIndex([future_dt])
        return pd.DataFrame(
            {
                "open": [70511.75],
                "high": [70511.75],
                "low": [70511.75],
                "close": [70511.75],
                "bid": [70510.75],
                "ask": [70512.75],
                "volume": [1.0],
            },
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2026, 3, 9, tzinfo=ny),
        datetime_end=datetime(2026, 6, 5, tzinfo=ny),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._update_datetime(sim_dt)

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    assert ds.get_last_price(asset, quote=quote) is None

    key = (asset, quote, "minute", "AUTO")
    data = ds._data_store[key]
    assert data.strict_end_check is True
    assert float(data.df["close"].iloc[0]) == 70511.75


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


def test_ibkr_rest_get_last_price_fetches_current_slice_without_full_window_prefetch(monkeypatch):
    """Do not answer an Apr sim-time lookup from a May-only minute cache.

    Regression for the Alpha Picks option backtests: a full-window minute prefetch can return only
    the tail of the requested window. Spot lookups should fetch the local simulation slice directly
    instead of downloading the whole backtest window at minute granularity.
    """

    import lumibot.tools.ibkr_helper as ibkr_helper

    ny = ZoneInfo("America/New_York")
    calls = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt))
        if pd.Timestamp(end_dt).date() >= datetime(2026, 5, 1).date():
            idx = pd.DatetimeIndex(
                [
                    datetime(2026, 5, 4, 9, 30, tzinfo=ny),
                    datetime(2026, 5, 7, 16, 0, tzinfo=ny),
                ]
            )
            closes = [100.0, 101.0]
        else:
            idx = pd.DatetimeIndex(
                [
                    datetime(2026, 4, 9, 9, 30, tzinfo=ny),
                    datetime(2026, 4, 9, 9, 31, tzinfo=ny),
                ]
            )
            closes = [42.0, 43.0]
        return pd.DataFrame(
            {"open": closes, "high": closes, "low": closes, "close": closes, "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = InteractiveBrokersRESTBacktesting(
        datetime_start=datetime(2026, 4, 9, tzinfo=ny),
        datetime_end=datetime(2026, 5, 8, 23, 59, tzinfo=ny),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._update_datetime(datetime(2026, 4, 9, 9, 30, tzinfo=ny))

    asset = Asset(symbol="MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    assert ds.get_last_price(asset) == 42.0
    assert len(calls) == 1
    assert calls[0][0].date().isoformat() == "2026-04-09"
    assert calls[0][1].date().isoformat() == "2026-04-10"
    assert (asset, quote, "minute", "AUTO") not in ds._fully_loaded_series
