from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas, _ProviderRegistry
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset, Data


def test_router_routes_crypto_to_ibkr(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = {"ibkr": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, history_as_of=None):
        calls["ibkr"] += 1
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

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(asset, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=timezone.utc))

    assert calls["ibkr"] == 1
    assert ds._data_store


def test_router_routes_crypto_future_to_exact_quote_spot_proxy(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.backtesting.routed_backtesting as routed_backtesting

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    captured = {}
    log_messages = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, history_as_of=None):
        captured["asset"] = asset
        captured["quote"] = quote
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
    monkeypatch.setattr(routed_backtesting.logger, "info", log_messages.append)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    perp = Asset(symbol="BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
    quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
    ds._update_pandas_data(perp, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=timezone.utc))

    assert captured["asset"] == Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    assert captured["quote"] == Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    assert (perp, quote, "minute") in ds._data_store
    stored = ds._data_store[(perp, quote, "minute")]
    assert stored.asset == perp
    assert stored.quote == quote
    assert any("Using BTC/USDT spot proxy" in message for message in log_messages)
    assert not any("mapped to USD" in message for message in log_messages)


def test_router_accepts_futures_key_alias(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = {"ibkr": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None, history_as_of=None):
        calls["ibkr"] += 1
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

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"futures": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    fut = Asset(symbol="MES", asset_type="future")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(fut, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=timezone.utc))

    assert calls["ibkr"] == 1


def test_router_uses_rth_for_ibkr_stock_daily(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    captured = {"include_after_hours": None, "calls": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, history_as_of=None):
        captured["calls"] += 1
        captured["include_after_hours"] = bool(include_after_hours)
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc),
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
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 5, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"stock": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )

    stock = Asset(symbol="TQQQ", asset_type="stock")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(stock, quote, length=2, timestep="day", start_dt=datetime(2025, 1, 2, tzinfo=timezone.utc))

    assert captured["calls"] == 1
    assert captured["include_after_hours"] is False


def test_router_get_quote_aligns_non_theta_daily_mode(monkeypatch):
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}  # type: ignore[attr-defined]
    routed._registry = _ProviderRegistry(routed)  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._effective_day_mode = True  # type: ignore[attr-defined]
    routed._update_cadence_from_dt = lambda _dt: None  # type: ignore[attr-defined]
    routed.get_datetime = lambda: datetime(2025, 1, 2, tzinfo=timezone.utc)  # type: ignore[attr-defined]

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
    routed.get_datetime = lambda: datetime(2025, 1, 2, tzinfo=timezone.utc)  # type: ignore[attr-defined]

    captured = {}

    def fake_super_get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        captured["timestep"] = timestep
        captured["snapshot_only"] = bool(kwargs.get("snapshot_only", False))
        return object()

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_quote", fake_super_get_quote, raising=True)

    routed.get_quote(Asset("TQQQ", asset_type=Asset.AssetType.STOCK), snapshot_only=True)

    assert captured["timestep"] == "day"
    assert captured["snapshot_only"] is True


def test_routed_daily_stock_last_price_uses_day_frame_over_stale_minute(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 4, 9, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 5, 9, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"stock": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = datetime(2026, 4, 9, 9, 30, tzinfo=timezone.utc)
    ds._effective_day_mode = True
    ds._observed_intraday_cadence = False

    asset = Asset("MU", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_df = pd.DataFrame(
        {"open": [10.0, 19.0], "high": [11.0, 21.0], "low": [9.0, 18.0], "close": [10.5, 20.0], "volume": [1, 2]},
        index=pd.DatetimeIndex(
            [datetime(2026, 4, 8, tzinfo=timezone.utc), datetime(2026, 4, 9, tzinfo=timezone.utc)]
        ),
    )
    stale_minute_df = pd.DataFrame(
        {"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.5], "volume": [1]},
        index=pd.DatetimeIndex([datetime(2026, 5, 4, 14, 0, tzinfo=timezone.utc)]),
    )
    ds._data_store[(asset, quote, "day")] = Data(asset, day_df, quote=quote, timestep="day")
    ds._data_store[(asset, quote, "minute")] = Data(asset, stale_minute_df, quote=quote, timestep="minute")

    requested_timesteps = []

    def fake_update(_asset, _quote, _length, timestep, *_args, **_kwargs):
        requested_timesteps.append(timestep)

    monkeypatch.setattr(ds, "_update_pandas_data", fake_update)

    assert ds.get_last_price(asset) == 20.0
    assert requested_timesteps == ["day"]


def test_routed_daily_stock_last_price_does_not_fall_back_to_stale_minute(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 4, 9, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 5, 9, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"stock": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = datetime(2026, 4, 9, 9, 30, tzinfo=timezone.utc)
    ds._effective_day_mode = True
    ds._observed_intraday_cadence = False

    asset = Asset("CSTM", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    stale_minute_df = pd.DataFrame(
        {"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.5], "volume": [1]},
        index=pd.DatetimeIndex([datetime(2026, 5, 4, 14, 0, tzinfo=timezone.utc)]),
    )
    ds._data_store[(asset, quote, "minute")] = Data(asset, stale_minute_df, quote=quote, timestep="minute")

    requested_timesteps = []

    def fake_update(_asset, _quote, _length, timestep, *_args, **_kwargs):
        requested_timesteps.append(timestep)

    monkeypatch.setattr(ds, "_update_pandas_data", fake_update)

    assert ds.get_last_price(asset) is None
    assert requested_timesteps == ["day"]


def test_update_cadence_daily_mode_does_not_mark_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "day"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._cadence_last_dt = datetime(2025, 1, 2, 8, 30, tzinfo=timezone.utc)  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(datetime(2025, 1, 2, 9, 30, tzinfo=timezone.utc))

    assert routed._observed_intraday_cadence is False  # type: ignore[attr-defined]
    assert routed._effective_day_mode is True  # type: ignore[attr-defined]


def test_update_cadence_intraday_mode_marks_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "minute"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    start_dt = datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc)
    routed._cadence_last_dt = start_dt  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(start_dt + timedelta(minutes=1))

    assert routed._observed_intraday_cadence is True  # type: ignore[attr-defined]


def test_update_cadence_daily_lifecycle_pattern_does_not_mark_intraday():
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._timestep = "minute"  # type: ignore[attr-defined]
    routed._observed_intraday_cadence = False  # type: ignore[attr-defined]
    routed._cadence_last_dt = datetime(2025, 1, 2, 8, 30, tzinfo=timezone.utc)  # type: ignore[attr-defined]

    routed._update_cadence_from_dt(datetime(2025, 1, 2, 9, 30, tzinfo=timezone.utc))

    assert routed._observed_intraday_cadence is False  # type: ignore[attr-defined]
    assert routed._effective_day_mode is True  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 5, 10, 15 and 30-minute stock bars built from the loaded 1-minute series.
# Bars are made up (seeded) in the shape the house source returns: all-hours minute rows with
# missing/bid/ask columns, and a provider series whose extended-hours bars differ from a rebuild.
# ---------------------------------------------------------------------------
import numpy as np
import pytest
import pytz

_NY = pytz.timezone("America/New_York")
_OHLCV_ORDER = {"open": "first", "close": "last", "high": "max", "low": "min", "volume": "sum",
                "missing": "last", "bid": "last", "ask": "last"}


_HOUSE_CALLS = {"active": []}


def _made_up_house_minutes(quiet_minutes=()):
    rng = np.random.default_rng(20251126)
    frames = []
    price = 100.0
    for day in ((2025, 11, 25), (2025, 11, 26)):
        index = pd.date_range(_NY.localize(datetime(*day, 4, 0)), _NY.localize(datetime(*day, 20, 0)),
                              freq="1min", inclusive="left")
        count = len(index)
        close = np.round(price + np.cumsum(rng.normal(0, 0.05, count)), 2)
        open_ = np.round(np.concatenate([[price], close[:-1]]) + rng.normal(0, 0.01, count), 2)
        frames.append(pd.DataFrame({
            "open": open_, "close": close,
            "high": np.round(np.maximum(open_, close) + np.abs(rng.normal(0, 0.03, count)), 2),
            "low": np.round(np.minimum(open_, close) - np.abs(rng.normal(0, 0.03, count)), 2),
            "volume": rng.integers(100, 50_000, count).astype(float), "missing": False,
            "bid": close, "ask": close}, index=index))
        price = float(close[-1])
    frame = pd.concat(frames)
    for stamp in quiet_minutes:  # a minute without trades: a filled placeholder row
        frame.loc[stamp, ["volume", "missing"]] = [0.0, True]
    frame["missing"] = frame["missing"].astype(bool)
    frame.index.name = "timestamp"
    return frame


def _made_up_house_provider(minute_df, minutes):
    frame = minute_df.resample(f"{minutes}min", label="left", closed="left").agg(_OHLCV_ORDER).dropna(subset=["open"])
    clock = frame.index.time
    extended = (clock < datetime(2000, 1, 1, 9, 30).time()) | (clock >= datetime(2000, 1, 1, 16, 0).time())
    # Outside the regular session the provider's own bars do not equal a rebuild from minutes.
    frame.loc[extended, "high"] = frame.loc[extended, "high"] + 0.37
    frame["missing"] = False
    return frame


def _house_source(monkeypatch, minute_df, calls):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *a, **k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *a, **k: None)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None,
                            include_after_hours=True, source=None, history_as_of=None, **kwargs):
        _HOUSE_CALLS["active"].append(str(timestep))
        digits = "".join(ch for ch in str(timestep) if ch.isdigit())
        return (minute_df if not digits else _made_up_house_provider(minute_df, int(digits))).copy()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    _HOUSE_CALLS["active"] = calls
    source = RoutedBacktestingPandas(
        datetime_start=_NY.localize(datetime(2025, 11, 26)),
        datetime_end=_NY.localize(datetime(2025, 11, 27)),
        config={"backtesting_data_routing": {"default": "ibkr"}},
        username="dev", password="dev", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    source._test_calls = calls
    return source


def _history(source, when, length, timestep, asset=None):
    _HOUSE_CALLS["active"] = source._test_calls  # provider requests are recorded per source
    source._datetime = when
    bars = source.get_historical_prices(asset or Asset("SPY"), length, timestep, quote=Asset("USD", "forex"))
    return None if bars is None else bars.df


@pytest.mark.parametrize("minutes", [5, 10, 15, 30])
def test_house_source_intraday_bars_from_loaded_minutes_equal_provider_bars(monkeypatch, minutes):
    minute_df = _made_up_house_minutes()
    timestep = f"{minutes} minutes"
    rebuilt_calls, provider_calls = [], []
    monkeypatch.delenv("BACKTESTING_INTRADAY_BARS_FROM_MINUTES", raising=False)
    rebuilt = _house_source(monkeypatch, minute_df, rebuilt_calls)
    reference = _house_source(monkeypatch, minute_df, provider_calls)
    reference._intraday_bars_from_minutes = False  # always the provider series

    open_at = _NY.localize(datetime(2025, 11, 26, 9, 30))
    assert _history(rebuilt, open_at, 1, "minute") is not None  # the 1-minute series is loaded
    compared = 0
    now = open_at
    while now <= _NY.localize(datetime(2025, 11, 26, 16, 0)):
        finished = int((now - open_at).total_seconds() // (minutes * 60))
        # The bar before the oldest requested one must be a session bar too (its close feeds
        # the oldest bar's return), so the session's first bar is never the oldest one here.
        for length in sorted({1, 2, finished - 1} - {0, -1}):
            if length > finished - 1:
                continue
            got = _history(rebuilt, now, length, timestep)
            expected = _history(reference, now, length, timestep)
            pd.testing.assert_frame_equal(got, expected)
            compared += len(got)
        now += timedelta(minutes=1)

    assert compared > 150
    assert set(rebuilt_calls) == {"minute"}  # the provider's multi-minute series was never requested
    assert f"{minutes}minute" in provider_calls

    # A request that reaches before the open, or comes after the close, is the provider's answer.
    first_bar_done = open_at + timedelta(minutes=minutes)  # the 09:30 bar's return needs a pre-market close
    for when, length in ((_NY.localize(datetime(2025, 11, 26, 10, 0)), 40), (first_bar_done, 1),
                         (_NY.localize(datetime(2025, 11, 26, 16, 0)) + timedelta(minutes=2 * minutes), 1)):
        fresh_calls = []
        fresh = _house_source(monkeypatch, minute_df, fresh_calls)
        assert _history(fresh, open_at, 1, "minute") is not None
        got = _history(fresh, when, length, timestep)
        pd.testing.assert_frame_equal(got, _history(reference, when, length, timestep))
        assert f"{minutes}minute" in fresh_calls


@pytest.mark.parametrize("change", ["env_off", "minutes_not_loaded", "hourly", "future", "quiet_minute"])
def test_house_source_intraday_bars_from_minutes_falls_back_to_the_provider_series(monkeypatch, change):
    when = _NY.localize(datetime(2025, 11, 26, 10, 30))
    quiet = [_NY.localize(datetime(2025, 11, 26, 10, 22))] if change == "quiet_minute" else []
    minute_df = _made_up_house_minutes(quiet_minutes=quiet)
    monkeypatch.delenv("BACKTESTING_INTRADAY_BARS_FROM_MINUTES", raising=False)
    if change == "env_off":
        monkeypatch.setenv("BACKTESTING_INTRADAY_BARS_FROM_MINUTES", "false")
    calls = []
    source = _house_source(monkeypatch, minute_df, calls)
    asset = Asset("MES", asset_type="cont_future") if change == "future" else Asset("SPY")
    timestep = "1 hour" if change == "hourly" else "5 minutes"
    if change != "minutes_not_loaded":
        _history(source, when, 1, "minute", asset=asset)
        calls.clear()

    _history(source, when, 3, timestep, asset=asset)
    assert any(call != "minute" for call in calls), calls
