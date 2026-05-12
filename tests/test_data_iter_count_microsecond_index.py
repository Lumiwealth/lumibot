"""Regression for the 2026-04-20 Alpha Picks prod leak bug.

Bug: `Data.get_iter_count(dt)` returned `len(df) - 1` (the last row of the
frame) for any sim_time that didn't exactly match a bar timestamp, when the
underlying index used `datetime64[us, tz]` precision (pandas 2.x default for
tz-aware indexes).

Root cause: `_index_values_ns` was populated via `self.df.index.asi8`, which
returns integers in the index's NATIVE unit — microseconds for
`datetime64[us]`, nanoseconds for `datetime64[ns]`. Downstream comparisons
assumed nanoseconds uniformly. With microsecond storage, every `dt_ns`
computed as `int(dt.timestamp() * 1e9)` was 1000× larger than every
`idx_ns[i]`, so `np.searchsorted(idx, dt_ns, side="right")` always returned
`len(idx)` and iter_count landed on the last row.

Observed symptom: Alpha Picks prod backtest (window 2022-07-01 → 2022-08-01,
IBKR data source, lumibot 4.5.1) sized COP orders using `$97.43` — the close
of 2022-07-29, the LAST bar in the fetched frame — instead of the real
2022-07-01 close of `$90.98`. Under-invested every position day-1, capped
returns at ~8.18% vs ground-truth +10%.

IBKR downloader parquets on S3 were written by newer pandas and deserialize
as `datetime64[us, America/New_York]`. Older caches written by older pandas
(the local dev cache) deserialize as `datetime64[ns]` and accidentally
worked, hiding the bug.
"""

from __future__ import annotations

import pandas as pd
import pytest
import pytz

from lumibot.entities import Asset, Data


def _make_frame(unit: str) -> pd.DataFrame:
    """Build a daily OHLCV frame with an explicit datetime precision unit."""
    ny = pytz.timezone("America/New_York")
    idx = pd.DatetimeIndex(
        [
            ny.localize(pd.Timestamp("2022-06-28 16:00")),
            ny.localize(pd.Timestamp("2022-06-29 16:00")),
            ny.localize(pd.Timestamp("2022-06-30 16:00")),
            ny.localize(pd.Timestamp("2022-07-01 16:00")),
            ny.localize(pd.Timestamp("2022-07-05 16:00")),
            ny.localize(pd.Timestamp("2022-07-29 16:00")),
        ]
    ).as_unit(unit)
    return pd.DataFrame(
        {
            "open": [95.00, 96.00, 88.00, 90.00, 89.00, 96.00],
            "high": [96.00, 97.00, 91.00, 91.00, 90.00, 97.92],
            "low": [94.00, 95.00, 87.00, 88.00, 88.00, 95.40],
            "close": [95.22, 91.46, 89.81, 90.98, 88.65, 97.43],
            "volume": [1.0] * 6,
        },
        index=idx,
    )


@pytest.mark.parametrize("unit", ["ns", "us"])
def test_get_iter_count_handles_any_index_precision(unit: str):
    """`get_iter_count` must return the correct iter_count regardless of
    whether the underlying DatetimeIndex stores values in ns or us.
    """
    ny = pytz.timezone("America/New_York")
    asset = Asset("COP", "stock")
    df = _make_frame(unit)
    data = Data(asset, df, timestep="day")
    data.repair_times_and_fill(df.index)

    # Mid-day sim_time on 2022-07-01: last completed bar is 2022-06-30 (iter_count=2).
    sim_time = ny.localize(pd.Timestamp("2022-07-01 10:00"))
    ic = data.get_iter_count(sim_time)

    assert ic == 2, (
        f"unit={unit}: iter_count should be 2 (2022-06-30) but got {ic}. "
        f"If this lands at 5 (last row = 2022-07-29, close $97.43), the "
        f"unit-mismatch bug from the 2026-04-20 Alpha Picks prod leak has "
        f"regressed: `asi8` on datetime64[{unit}] stores in {unit} but "
        f"downstream comparisons assume ns."
    )


@pytest.mark.parametrize("unit", ["ns", "us"])
def test_get_bars_length1_returns_sim_time_bar_any_precision(unit: str):
    """End-to-end: `Data.get_bars(dt, length=1, timestep='day', timeshift=0)`
    must return the bar at or before sim_time, not the last row of the frame.

    This is the exact call shape used by `Strategy.get_last_price`'s daily
    shortcut — the 2026-04-20 prod leak symptom was this returning the last
    row's close ($97.43) instead of the actual 2022-06-30 bar close ($89.81).
    """
    ny = pytz.timezone("America/New_York")
    asset = Asset("COP", "stock")
    df = _make_frame(unit)
    data = Data(asset, df, timestep="day")
    data.repair_times_and_fill(df.index)

    sim_time = ny.localize(pd.Timestamp("2022-07-01 10:00"))
    out = data.get_bars(sim_time, length=1, timestep="day", timeshift=0)

    assert out is not None and len(out) == 1, f"unit={unit}: expected 1 bar, got {out}"
    # Accept either 2022-06-30 close (89.81) or 2022-07-01 close (90.98)
    # — both are "at or before sim_time" and either is sim-time safe.
    # What MUST NOT happen is returning 97.43 (the 2022-07-29 bar = last row).
    close = float(out["close"].iloc[-1])
    assert close in (pytest.approx(89.81), pytest.approx(90.98)), (
        f"unit={unit}: get_bars(length=1, ts=0) returned close={close} for "
        f"sim_time=2022-07-01 10:00 ET. Expected 89.81 (2022-06-30 close) or "
        f"90.98 (2022-07-01 close). 97.43 is the 2022-07-29 bar — the "
        f"microsecond-index iter_count bug has regressed."
    )


def test_asi8_microsecond_index_has_smaller_magnitude_than_nanoseconds():
    """Whitebox: document WHY the bug exists. `asi8` on a datetime64[us]
    index returns microseconds (1000× smaller magnitude than nanoseconds),
    and naive comparison against `dt.timestamp() * 1e9` yields garbage.

    Pinning this so anyone tempted to "optimize" `_index_values_ns` knows
    they must normalize to a single unit.
    """
    ny = pytz.timezone("America/New_York")
    idx_ns = pd.DatetimeIndex([ny.localize(pd.Timestamp("2022-07-01 16:00"))]).as_unit("ns")
    idx_us = pd.DatetimeIndex([ny.localize(pd.Timestamp("2022-07-01 16:00"))]).as_unit("us")

    v_ns = int(idx_ns.asi8[0])
    v_us = int(idx_us.asi8[0])

    # Same wall-clock moment, 1000x different magnitudes — this is the trap.
    assert v_ns == v_us * 1000, (
        f"Expected asi8 unit to differ by 1000x between ns ({v_ns}) and us "
        f"({v_us}). If this assertion fails, pandas changed behavior and the "
        f"unit-mismatch fix in Data.__init__ needs to be reviewed."
    )


def test_large_iter_index_uses_lazy_exact_lookup(monkeypatch):
    """Large data sets should not materialize a python-datetime dict per row."""
    import lumibot.entities.data as data_module

    monkeypatch.setattr(data_module, "_ITER_INDEX_DICT_MAX_ROWS", 2)
    ny = pytz.timezone("America/New_York")
    idx = pd.date_range(ny.localize(pd.Timestamp("2024-01-02 09:30")), periods=3, freq="1min")
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [1.0, 2.0, 3.0],
            "volume": [1.0, 1.0, 1.0],
        },
        index=idx,
    )
    data = Data(Asset("SPY"), df, timestep="minute")
    data.repair_times_and_fill(df.index)

    assert type(data.iter_index_dict).__name__ == "_DatetimeIndexLookup"
    assert data.iter_index_dict.get(idx[1].to_pydatetime()) == 1
    assert idx[2].to_pydatetime() in data.iter_index_dict
    assert data.get_previous_bar_close_fast(idx[2].to_pydatetime()) == 2.0


def test_large_daily_iter_index_uses_lower_lazy_threshold(monkeypatch):
    """Large daily series can use lazy exact lookup without slowing hot minute scans."""
    import lumibot.entities.data as data_module

    monkeypatch.setattr(data_module, "_ITER_INDEX_DICT_MAX_ROWS", 100)
    monkeypatch.setattr(data_module, "_ITER_INDEX_DAY_DICT_MAX_ROWS", 2)
    ny = pytz.timezone("America/New_York")
    minute_idx = pd.date_range(ny.localize(pd.Timestamp("2024-01-02 09:30")), periods=3, freq="1min")
    day_idx = pd.date_range(ny.localize(pd.Timestamp("2024-01-02 16:00")), periods=3, freq="1D")

    def frame(idx):
        return pd.DataFrame(
            {
                "open": [1.0, 2.0, 3.0],
                "high": [1.0, 2.0, 3.0],
                "low": [1.0, 2.0, 3.0],
                "close": [1.0, 2.0, 3.0],
                "volume": [1.0, 1.0, 1.0],
            },
            index=idx,
        )

    minute_data = Data(Asset("SPY"), frame(minute_idx), timestep="minute")
    minute_data.repair_times_and_fill(minute_idx)
    day_data = Data(Asset("SPY"), frame(day_idx), timestep="day")
    day_data.repair_times_and_fill(day_idx)

    assert type(minute_data.iter_index_dict) is dict
    assert type(day_data.iter_index_dict).__name__ == "_DatetimeIndexLookup"
    assert day_data.iter_index_dict.get(day_idx[2].to_pydatetime()) == 2


def test_large_crypto_minute_iter_index_uses_lower_lazy_threshold(monkeypatch):
    """Crypto minute data can avoid datetime dicts without changing non-crypto minute data."""
    import lumibot.entities.data as data_module

    monkeypatch.setattr(data_module, "_ITER_INDEX_DICT_MAX_ROWS", 100)
    monkeypatch.setattr(data_module, "_ITER_INDEX_CRYPTO_MINUTE_DICT_MAX_ROWS", 2)
    ny = pytz.timezone("America/New_York")
    idx = pd.date_range(ny.localize(pd.Timestamp("2024-01-02 09:30")), periods=3, freq="1min")
    frame = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [1.0, 2.0, 3.0],
            "volume": [1.0, 1.0, 1.0],
        },
        index=idx,
    )

    stock_data = Data(Asset("SPY"), frame, timestep="minute")
    stock_data.repair_times_and_fill(idx)
    crypto_data = Data(
        Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        frame,
        timestep="minute",
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
    )
    crypto_data.repair_times_and_fill(idx)

    assert type(stock_data.iter_index_dict) is dict
    assert type(crypto_data.iter_index_dict).__name__ == "_DatetimeIndexLookup"
    assert crypto_data.iter_index_dict.get(idx[2].to_pydatetime()) == 2


def test_lazy_iter_index_handles_large_forward_jumps(monkeypatch):
    """Lazy exact lookup should not depend on a small monotonic cursor step."""
    import lumibot.entities.data as data_module

    monkeypatch.setattr(data_module, "_ITER_INDEX_DICT_MAX_ROWS", 2)
    ny = pytz.timezone("America/New_York")
    idx = pd.date_range(ny.localize(pd.Timestamp("2024-01-02 09:30")), periods=200, freq="1min")
    df = pd.DataFrame(
        {
            "open": range(200),
            "high": range(200),
            "low": range(200),
            "close": range(200),
            "volume": [1.0] * 200,
        },
        index=idx,
    )
    data = Data(Asset("SPY"), df, timestep="minute")
    data.repair_times_and_fill(df.index)

    assert data.iter_index_dict.get(idx[1].to_pydatetime()) == 1
    assert data.iter_index_dict.get(idx[180].to_pydatetime()) == 180
    assert data.iter_index_dict.get(idx[40].to_pydatetime()) == 40
