"""Per-iteration costs of a minute backtest: same answers, fixed operation counts.

A 12-month minute iron condor runs about 97,500 iterations, so anything a backtest does on
every bar is multiplied by that. These tests pin the fast paths to the original answers and
count operations (never elapsed time), so a change that brings the per-bar work back fails.
"""

import copy
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from alpaca.data.models import BarSet

from lumibot.entities import Asset
from lumibot.tools import shared_bar_cache as sbc
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source

# --------------------------------------------------------------------------- held-leg marks


def _prints(symbol, day):
    """Sparse trade prints across a session: a print every 7 minutes, two gaps."""
    start = pd.Timestamp(f"{day} 13:30", tz="UTC")
    rows = []
    for i in range(0, 390, 7):
        if 120 <= i < 180:
            continue  # an hour with no prints
        t = start + pd.Timedelta(minutes=i)
        price = 1.0 + (i % 13) / 100.0
        rows.append(dict(t=t.isoformat(), o=price, h=price + 0.05, l=price - 0.05, c=price + 0.01, v=1, n=1,
                         vw=price))
    return rows


class _Client:
    def __init__(self):
        self.requests = 0

    def get_option_bars(self, request):
        self.requests += 1
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        return BarSet({s: _prints(s, "2026-08-04") + _prints(s, "2026-08-05") for s in symbols})


def _leg(strike):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=strike, right="CALL")


def _source(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path, start=datetime(2026, 8, 3), end=datetime(2026, 8, 7))
    source._option_client = _Client()
    return source


def _marks(source, legs, clocks):
    out = []
    for clock in clocks:
        source._datetime = _NY_TZ.localize(clock)
        out.append(tuple(source.get_last_price(leg) for leg in legs))
    return out


CLOCKS = [datetime(2026, 8, 4, 9, 30) + timedelta(minutes=m) for m in range(0, 390, 3)] + \
    [datetime(2026, 8, 5, 9, 30) + timedelta(minutes=m) for m in range(0, 390, 11)]


def test_held_leg_marks_from_arrays_equal_the_pandas_path(monkeypatch, tmp_path):
    legs = [_leg(640), _leg(645)]
    fast_source = _source(monkeypatch, tmp_path / "fast")
    fast = _marks(fast_source, legs, CLOCKS)
    assert fast_source.__dict__.get("_option_mark_cache")  # the array path ran
    from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting

    monkeypatch.setattr(AlpacaBacktesting, "_option_mark_arrays", lambda self, frame, timestep: None)
    slow = _marks(_source(monkeypatch, tmp_path / "slow"), legs, CLOCKS)
    assert fast == slow
    assert len({m for m in fast}) > 10  # opens at prints, closes between prints, across two days


def test_held_leg_marks_read_no_pandas_columns(monkeypatch, tmp_path):
    source = _source(monkeypatch, tmp_path)
    legs = [_leg(640), _leg(645), _leg(650), _leg(655)]
    _marks(source, legs, CLOCKS[:5])  # loads and builds the arrays once
    reads = []
    original = pd.DataFrame.__getitem__

    def counting(self, key):
        reads.append(key)
        return original(self, key)

    monkeypatch.setattr(pd.DataFrame, "__getitem__", counting)
    _marks(source, legs, CLOCKS[5:105])  # 100 bars x 4 legs
    assert reads == []


def test_held_leg_marks_follow_a_replaced_frame(monkeypatch, tmp_path):
    source = _source(monkeypatch, tmp_path)
    leg = _leg(640)
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 10, 0))
    first = source.get_last_price(leg)
    key = next(k for k in source._data_store if "640" in k)
    frame = source._data_store[key].copy()
    frame["open"] = frame["open"] + 1.0
    frame["close"] = frame["close"] + 1.0
    source._data_store[key] = frame  # an extension or backfill stores a new frame object
    assert source.get_last_price(leg) == pytest.approx(float(first) + 1.0)


def test_asset_key_memo_returns_the_built_key(monkeypatch, tmp_path):
    source = _source(monkeypatch, tmp_path)
    quote = Asset("USD", Asset.AssetType.FOREX)
    for leg in (_leg(640), _leg(645), Asset("SPY")):
        built = source._build_asset_key(base_asset=leg, quote_asset=quote, timestep="minute")
        assert source._get_asset_key(base_asset=leg, quote_asset=quote, timestep="minute") == built
        assert source._get_asset_key(base_asset=leg, quote_asset=quote, timestep="minute") == built
    for asset, asset_quote in ((Asset("SPY", Asset.AssetType.STOCK), quote),
                               (Asset("BTC", Asset.AssetType.CRYPTO), quote),
                               (Asset("ETH", Asset.AssetType.CRYPTO), Asset("BTC", Asset.AssetType.CRYPTO))):
        for timestep in ("minute", "day"):
            built = source._build_asset_key(base_asset=asset, quote_asset=asset_quote, timestep=timestep)
            assert source._get_asset_key(base_asset=asset, quote_asset=asset_quote, timestep=timestep) == built
    other = source._get_asset_key(base_asset=_leg(640), quote_asset=quote, timestep="minute",
                                  data_datetime_start=datetime(2026, 7, 1))
    assert other == source._build_asset_key(base_asset=_leg(640), quote_asset=quote, timestep="minute",
                                            data_datetime_start=datetime(2026, 7, 1))


# --------------------------------------------------------------------------- cache validation


def _validate_rows_pandas(rows):
    """The previous implementation, kept as the reference."""
    if rows.empty:
        return True
    if not set(sbc.PRICE_FIELDS) <= set(rows.columns):
        return False
    prices = rows[sbc.PRICE_FIELDS].to_numpy(dtype=float)
    ok = (
        isinstance(rows.index, pd.DatetimeIndex)
        and not rows.index.hasnans
        and not rows.index.has_duplicates
        and bool(np.isfinite(prices).all())
        and bool((prices > 0).all())
        and bool((rows["low"] <= rows[["open", "close", "high"]].min(axis=1)).all())
        and bool((rows["high"] >= rows[["open", "close", "low"]].max(axis=1)).all())
    )
    if ok and "volume" in rows.columns:
        volume = rows["volume"].to_numpy(dtype=float)
        ok = bool(np.isfinite(volume).all()) and bool((volume >= 0).all())
    return ok


def test_validate_rows_matches_the_pandas_reference():
    rng = np.random.default_rng(7)
    index = pd.date_range("2026-01-02 14:30", periods=50, freq="1min", tz="UTC")
    for trial in range(400):
        base = rng.uniform(1, 100, size=50)
        frame = pd.DataFrame({"open": base, "high": base * 1.01, "low": base * 0.99, "close": base,
                              "volume": rng.integers(0, 100, size=50).astype(float)}, index=index)
        i = int(rng.integers(0, 50))
        kind = trial % 8
        if kind == 1:
            frame.iloc[i, 1] = frame.iloc[i, 0] * 0.98  # high below open
        elif kind == 2:
            frame.iloc[i, 2] = frame.iloc[i, 3] * 1.02  # low above close
        elif kind == 3:
            frame.iloc[i, 0] = np.nan
        elif kind == 4:
            frame.iloc[i, 3] = -1.0
        elif kind == 5:
            frame.iloc[i, 4] = -5.0
        elif kind == 6:
            frame.iloc[i, 1] = np.inf
        elif kind == 7:
            frame.iloc[i, 2] = frame.iloc[i, 1]  # low equals high: still consistent only if open/close equal
        assert sbc.validate_rows(frame) == _validate_rows_pandas(frame), (trial, kind)


# --------------------------------------------------------------------------- idle iterations


def test_idle_minute_iterations_make_no_deepcopy(monkeypatch, tmp_path):
    """An iteration that does nothing must not copy anything (pandas attrs copies included)."""
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Data
    from lumibot.strategies import Strategy

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    index = pd.date_range("2026-08-03 09:30", "2026-08-07 16:00", freq="1min", tz="America/New_York")
    frame = pd.DataFrame({"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.2, "volume": 10.0}, index=index)
    asset, quote = Asset("SPY"), Asset("USD", Asset.AssetType.FOREX)
    pandas_data = {(asset, quote): Data(asset, frame, timestep="minute", quote=quote)}
    calls = {"n": 0}
    original = copy.deepcopy

    def counting(obj, memo=None):
        calls["n"] += 1
        return original(obj, memo) if memo is not None else original(obj)

    marks = []

    class Idle(Strategy):
        def initialize(self):
            self.sleeptime = "1M"

        def on_trading_iteration(self):
            marks.append(calls["n"])

    monkeypatch.setattr(copy, "deepcopy", counting)
    Idle.run_backtest(PandasDataBacktesting, datetime(2026, 8, 3), datetime(2026, 8, 7), pandas_data=pandas_data,
                      benchmark_asset=None, show_plot=False, show_tearsheet=False, save_tearsheet=False,
                      show_indicators=False, quiet_logs=True, show_progress_bar=False, budget=10000)
    assert len(marks) > 1000
    per_iteration = (marks[-1] - marks[10]) / (len(marks) - 11)
    assert per_iteration == 0, per_iteration
