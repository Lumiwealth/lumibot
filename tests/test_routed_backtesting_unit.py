from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas, _ProviderRegistry
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset


def test_router_routes_crypto_to_ibkr(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = {"ibkr": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True):
        calls["ibkr"] += 1
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=UTC),
        datetime_end=datetime(2025, 1, 2, tzinfo=UTC),
        config={"backtesting_data_routing": {"crypto": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(
        asset, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=UTC)
    )

    assert calls["ibkr"] == 1
    assert ds._data_store


def test_router_accepts_futures_key_alias(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = {"ibkr": 0}

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        calls["ibkr"] += 1
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=UTC),
        datetime_end=datetime(2025, 1, 2, tzinfo=UTC),
        config={"backtesting_data_routing": {"futures": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    fut = Asset(symbol="MES", asset_type="future")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(fut, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=UTC))

    assert calls["ibkr"] == 1


def test_router_uses_rth_for_ibkr_stock_daily(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    captured = {"include_after_hours": None, "calls": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True):
        captured["calls"] += 1
        captured["include_after_hours"] = bool(include_after_hours)
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 2, 0, 0, tzinfo=UTC),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                "volume": [1000, 1100],
            },
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=UTC),
        datetime_end=datetime(2025, 1, 5, tzinfo=UTC),
        config={"backtesting_data_routing": {"stock": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    stock = Asset(symbol="TQQQ", asset_type="stock")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(stock, quote, length=2, timestep="day", start_dt=datetime(2025, 1, 2, tzinfo=UTC))

    assert captured["calls"] == 1
    assert captured["include_after_hours"] is False


def test_router_get_quote_aligns_non_theta_daily_mode(monkeypatch):
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}  # type: ignore[attr-defined]
    routed._registry = _ProviderRegistry(routed)  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._effective_day_mode = True  # type: ignore[attr-defined]
    routed._update_cadence_from_dt = lambda _dt: None  # type: ignore[attr-defined]
    routed.get_datetime = lambda: datetime(2025, 1, 2, tzinfo=UTC)  # type: ignore[attr-defined]

    captured = {}

    def fake_super_get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        captured["timestep"] = timestep
        captured["snapshot_only"] = bool(kwargs.get("snapshot_only", False))
        return object()

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_quote", fake_super_get_quote, raising=True)

    routed.get_quote(Asset("TQQQ", asset_type=Asset.AssetType.STOCK))

    assert captured["timestep"] == "day"
    assert captured["snapshot_only"] is False


def test_router_get_quote_aligns_snapshot_only_for_non_theta_daily_mode(monkeypatch):
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}  # type: ignore[attr-defined]
    routed._registry = _ProviderRegistry(routed)  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._effective_day_mode = True  # type: ignore[attr-defined]
    routed._update_cadence_from_dt = lambda _dt: None  # type: ignore[attr-defined]
    routed.get_datetime = lambda: datetime(2025, 1, 2, tzinfo=UTC)  # type: ignore[attr-defined]

    captured = {}

    def fake_super_get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        captured["timestep"] = timestep
        captured["snapshot_only"] = bool(kwargs.get("snapshot_only", False))
        return object()

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_quote", fake_super_get_quote, raising=True)

    routed.get_quote(Asset("TQQQ", asset_type=Asset.AssetType.STOCK), snapshot_only=True)

    assert captured["timestep"] == "day"
    assert captured["snapshot_only"] is True


def test_update_cadence_daily_mode_does_not_mark_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "day"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._cadence_last_dt = datetime(2025, 1, 2, 8, 30, tzinfo=UTC)  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(datetime(2025, 1, 2, 9, 30, tzinfo=UTC))

    assert routed._observed_intraday_cadence is False  # type: ignore[attr-defined]
    assert routed._effective_day_mode is True  # type: ignore[attr-defined]


def test_update_cadence_intraday_mode_marks_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "minute"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    start_dt = datetime(2025, 1, 2, 14, 0, tzinfo=UTC)
    routed._cadence_last_dt = start_dt  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(start_dt + timedelta(minutes=1))

    assert routed._observed_intraday_cadence is True  # type: ignore[attr-defined]


def test_update_cadence_daily_lifecycle_pattern_does_not_mark_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "minute"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._cadence_last_dt = datetime(2025, 1, 2, 8, 30, tzinfo=UTC)  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(datetime(2025, 1, 2, 9, 30, tzinfo=UTC))

    assert routed._observed_intraday_cadence is False  # type: ignore[attr-defined]
    assert routed._effective_day_mode is True  # type: ignore[attr-defined]
