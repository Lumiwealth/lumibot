"""Shared candle builder: batch resampling, streaming forming candles, native timeframes.

The parity tests pin the shared builder to the exact pandas logic each former
copy used, so switching a caller can never change a backtest result.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from lumibot.tools.candles import (
    Candle,
    CandleBuilder,
    OHLCV_AGGREGATION,
    build_candles,
    choose_candle_source,
    parse_timeframe,
    resample_ohlcv,
    timeframe_label,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _minute_frame(n=600, tz="America/New_York", start="2024-03-08 09:30", seed=3, gaps=True, extra=False):
    rng = np.random.default_rng(seed)
    index = pd.date_range(start, periods=n, freq="1min", tz=tz)
    if gaps:
        keep = np.ones(n, dtype=bool)
        keep[rng.choice(n, size=n // 10, replace=False)] = False
        index = index[keep]
    m = len(index)
    close = 100 + np.cumsum(rng.normal(0, 0.2, m))
    open_ = close + rng.normal(0, 0.05, m)
    frame = pd.DataFrame(
        {
            "open": open_,
            "high": np.maximum(open_, close) + rng.random(m) * 0.1,
            "low": np.minimum(open_, close) - rng.random(m) * 0.1,
            "close": close,
            "volume": rng.integers(1, 1000, m).astype(float),
        },
        index=index,
    )
    if extra:
        frame["dividend"] = 0.0
        frame.iloc[5, frame.columns.get_loc("dividend")] = 0.25
        frame["stock_splits"] = np.nan
        frame.iloc[50:60, frame.columns.get_loc("stock_splits")] = 0.0
    return frame


FRAMES = {
    "ny_gaps": lambda: _minute_frame(),
    "utc_no_gaps": lambda: _minute_frame(tz="UTC", gaps=False),
    "with_nan_rows": lambda: _minute_frame().assign(
        close=lambda d: d["close"].where(d.index.minute % 17 != 0)
    ),
    "unsorted": lambda: _minute_frame().sample(frac=1.0, random_state=1),
    "dividends_splits": lambda: _minute_frame(extra=True),
    "empty": lambda: _minute_frame().iloc[0:0],
}


# The exact code each former copy ran. Kept here, verbatim in behavior, as the
# reference the shared builder must match.


def legacy_alpaca(df, rule):
    if df.empty:
        return df
    aggregation = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    available = {c: m for c, m in aggregation.items() if c in df.columns}
    out = df.sort_index().resample(rule, label="left", closed="left").agg(available)
    required = [c for c in ("open", "high", "low", "close") if c in out.columns]
    if required:
        out = out.dropna(subset=required, how="any")
    return out


def legacy_strategy(df, rule):
    return df.resample(rule, label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()


def legacy_data(df, rule, with_splits=True):
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    if "dividend" in df.columns:
        agg["dividend"] = "sum"
    if with_splits and "stock_splits" in df.columns:
        agg["stock_splits"] = "max"
    return df.resample(rule).agg(agg).dropna()


RULES = ["1min", "3min", "5min", "15min", "1h", "4h", "1D"]


# ---------------------------------------------------------------------------
# Parity with the former copies
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("frame_name", list(FRAMES))
@pytest.mark.parametrize("rule", RULES)
def test_parity_alpaca_resample(frame_name, rule):
    df = FRAMES[frame_name]()
    expected = legacy_alpaca(df, rule)
    actual = resample_ohlcv(df, rule) if not df.empty else df
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize("frame_name", [n for n in FRAMES if n not in {"empty", "dividends_splits"}])
@pytest.mark.parametrize("rule", RULES)
def test_parity_strategy_fallback(frame_name, rule):
    df = FRAMES[frame_name]()
    expected = legacy_strategy(df, rule)
    actual = resample_ohlcv(df, rule, aggregation=dict(OHLCV_AGGREGATION), dropna="any", sort=False)
    pd.testing.assert_frame_equal(actual, expected)


def test_parity_strategy_fallback_missing_volume_still_raises():
    df = _minute_frame().drop(columns=["volume"])
    with pytest.raises(KeyError):
        legacy_strategy(df, "5min")
    with pytest.raises(KeyError):
        resample_ohlcv(df, "5min", aggregation=dict(OHLCV_AGGREGATION), dropna="any", sort=False)


@pytest.mark.parametrize("frame_name", [n for n in FRAMES if n != "empty"])
@pytest.mark.parametrize("rule", RULES)
@pytest.mark.parametrize("with_splits", [True, False])
def test_parity_data_entities(frame_name, rule, with_splits):
    df = FRAMES[frame_name]()
    expected = legacy_data(df, rule, with_splits=with_splits)
    aggregation = dict(OHLCV_AGGREGATION)
    if "dividend" in df.columns:
        aggregation["dividend"] = "sum"
    if with_splits and "stock_splits" in df.columns:
        aggregation["stock_splits"] = "max"
    actual = resample_ohlcv(df, rule, aggregation=aggregation, dropna="any", sort=False)
    pd.testing.assert_frame_equal(actual, expected)


def test_callers_use_the_shared_builder():
    """The switched callers route through resample_ohlcv (guards against a new copy)."""
    import inspect

    from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting
    from lumibot.entities import data as data_module
    from lumibot.entities import data_polars as data_polars_module
    from lumibot.strategies import strategy as strategy_module

    assert "resample_ohlcv" in inspect.getsource(AlpacaBacktesting._resample_ohlcv_dataframe)
    for module in (data_module, data_polars_module, strategy_module):
        source = inspect.getsource(module)
        assert "resample_ohlcv(" in source
        assert ".resample(f\"{quantity}{unit}\")" not in source
        assert "'open': 'first'" not in source


# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,minutes",
    [
        ("1m", 1),
        ("5m", 5),
        ("15m", 15),
        ("1h", 60),
        ("4h", 240),
        ("1d", 1440),
        ("5min", 5),
        ("5 minutes", 5),
        ("minute", 1),
        ("hour", 60),
        ("day", 1440),
        ("2H", 120),
        ("30s", 0.5),
    ],
)
def test_parse_timeframe(text, minutes):
    assert parse_timeframe(text) == pd.Timedelta(minutes=minutes)


@pytest.mark.parametrize("bad", ["", "0m", "-5m", "1M", "1w", "fortnight", None, 0])
def test_parse_timeframe_rejects(bad):
    with pytest.raises(ValueError):
        parse_timeframe(bad)


def test_timeframe_label_round_trips():
    for text in ["1m", "5m", "15m", "1h", "4h", "1d", "30s"]:
        assert timeframe_label(parse_timeframe(text)) == text


def test_choose_candle_source_prefers_native():
    native = ["1m", "5m", "15m", "1h", "1d"]
    assert choose_candle_source("15m", native) == ("15m", False)
    assert choose_candle_source("15min", native) == ("15m", False)
    # 10m is not native: build from 1m
    assert choose_candle_source("10m", native) == ("1m", True)
    # 4h not native: still built from 1m (the fallback) when 1m exists
    assert choose_candle_source("4h", native) == ("1m", True)


def test_choose_candle_source_without_one_minute_uses_largest_divisor():
    assert choose_candle_source("4h", ["5m", "1h", "1d"]) == ("1h", True)
    with pytest.raises(ValueError):
        choose_candle_source("7m", ["5m", "1h"])


def test_choose_candle_source_accepts_ccxt_style_mapping():
    native = {"1m": "1", "5m": "5", "1h": "60"}
    assert choose_candle_source("5m", native) == ("5m", False)


# ---------------------------------------------------------------------------
# Batch builder (UTC boundaries)
# ---------------------------------------------------------------------------


def test_build_candles_cuts_on_utc_boundaries():
    # Minute bars indexed in New York time; 4h candles must start at 00/04/08/12/16/20 UTC.
    df = _minute_frame(n=2000, gaps=False)
    candles = build_candles(df, "4h")
    assert all(ts.tz_convert("UTC").hour % 4 == 0 and ts.minute == 0 for ts in candles.index)
    assert str(candles.index.tz) == "America/New_York"  # original zone preserved


def test_build_candles_daily_starts_at_utc_midnight():
    df = _minute_frame(n=3000, gaps=False)
    candles = build_candles(df, "1d")
    assert all(ts.tz_convert("UTC") == ts.tz_convert("UTC").normalize() for ts in candles.index)


def test_build_candles_values_match_manual_aggregation():
    df = _minute_frame(n=300, tz="UTC", gaps=False)
    candles = build_candles(df, "15m")
    first = df.iloc[: 15 - (df.index[0].minute % 15)]
    row = candles.iloc[0]
    assert row["open"] == first["open"].iloc[0]
    assert row["high"] == first["high"].max()
    assert row["low"] == first["low"].min()
    assert row["close"] == first["close"].iloc[-1]
    assert row["volume"] == first["volume"].sum()


def test_build_candles_drop_forming_last_candle():
    df = _minute_frame(n=37, tz="UTC", start="2024-01-01 00:00", gaps=False)
    with_forming = build_candles(df, "15m")
    closed_only = build_candles(df, "15m", include_forming=False)
    assert len(with_forming) == 3  # 00:00, 00:15, 00:30 (forming, 7 of 15 minutes)
    assert len(closed_only) == 2


def test_build_candles_naive_index_is_utc():
    df = _minute_frame(n=120, tz="UTC", gaps=False)
    naive = df.tz_localize(None)
    a = build_candles(df, "1h")
    b = build_candles(naive, "1h")
    assert list(a.index.tz_localize(None)) == list(b.index)


# ---------------------------------------------------------------------------
# Streaming builder
# ---------------------------------------------------------------------------


def _bars_ms(start_ms, n, step_ms=60_000):
    rows = []
    price = 100.0
    for i in range(n):
        o = price
        c = price + (1 if i % 3 else -0.5)
        rows.append({"t": start_ms + i * step_ms, "o": o, "h": max(o, c) + 0.25, "l": min(o, c) - 0.25, "c": c, "v": 10 + i})
        price = c
    return rows


def test_streaming_matches_batch_builder():
    start = int(pd.Timestamp("2024-05-01 00:00", tz="UTC").value // 1_000_000)
    rows = _bars_ms(start, 95)
    builder = CandleBuilder("15m")
    closed = []
    for row in rows:
        closed.extend(c for c in builder.update(row) if c.closed)
    df = pd.DataFrame(rows)
    df.index = pd.to_datetime(df["t"], unit="ms", utc=True)
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})[
        ["open", "high", "low", "close", "volume"]
    ]
    batch = build_candles(df, "15m", include_forming=False)
    assert len(closed) == len(batch)
    for candle, (ts, row) in zip(closed, batch.iterrows()):
        assert candle.time == ts
        assert (candle.open, candle.high, candle.low, candle.close, candle.volume) == (
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
        )
    forming = builder.forming
    assert forming is not None and not forming.closed
    assert forming.time == pd.Timestamp("2024-05-01 01:30", tz="UTC")


def test_forming_candle_updates_live_including_wicks_then_locks():
    builder = CandleBuilder("5m")
    t0 = pd.Timestamp("2024-05-01 00:00", tz="UTC")
    out = builder.update({"t": t0, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 1}, closed=False)
    assert out[-1].closed is False and out[-1].high == 11
    # same source minute updates again (exchange re-sends the forming 1m bar): replaced, not added
    out = builder.update({"t": t0, "o": 10, "h": 12, "l": 8, "c": 11, "v": 2}, closed=False)
    candle = out[-1]
    assert (candle.high, candle.low, candle.close, candle.volume) == (12, 8, 11, 2)
    # next minute
    out = builder.update({"t": t0 + pd.Timedelta(minutes=1), "o": 11, "h": 11.5, "l": 10, "c": 11.2, "v": 3})
    candle = out[-1]
    assert (candle.open, candle.high, candle.low, candle.close, candle.volume) == (10, 12, 8, 11.2, 5)
    # last minute of the bucket arrives closed: candle locks immediately
    out = builder.update({"t": t0 + pd.Timedelta(minutes=4), "o": 11.2, "h": 13, "l": 11, "c": 12, "v": 1}, closed=True)
    assert out[-1].closed is True and out[-1].high == 13
    assert builder.forming is None
    # a late tick for the locked candle is refused
    with pytest.raises(ValueError):
        builder.update({"t": t0 + pd.Timedelta(minutes=2), "o": 1, "h": 1, "l": 1, "c": 1, "v": 1})


def test_new_bucket_closes_previous_candle():
    builder = CandleBuilder("5m")
    t0 = pd.Timestamp("2024-05-01 00:00", tz="UTC")
    builder.update({"t": t0, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 1})
    out = builder.update({"t": t0 + pd.Timedelta(minutes=7), "o": 2, "h": 3, "l": 1.5, "c": 2.5, "v": 1})
    assert [c.closed for c in out] == [True, False]
    assert out[0].time == t0 and out[0].close == 1.5
    assert out[1].time == t0 + pd.Timedelta(minutes=5)


def test_close_through_locks_on_clock_without_a_new_bar():
    builder = CandleBuilder("5m")
    t0 = pd.Timestamp("2024-05-01 00:00", tz="UTC")
    builder.update({"t": t0, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 1})
    assert builder.close_through(t0 + pd.Timedelta(minutes=4, seconds=59)) == []
    closed = builder.close_through(t0 + pd.Timedelta(minutes=5))
    assert len(closed) == 1 and closed[0].closed and builder.forming is None


def test_native_source_passes_candles_through():
    builder = CandleBuilder("15m", source_timeframe="15m")
    t0 = pd.Timestamp("2024-05-01 00:00", tz="UTC")
    out = builder.update({"t": t0, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 7}, closed=False)
    assert out[-1].time == t0 and not out[-1].closed
    out = builder.update({"t": t0, "o": 1, "h": 2.5, "l": 0.5, "c": 2, "v": 9}, closed=True)
    assert out[-1].closed and out[-1].high == 2.5 and out[-1].volume == 9


def test_source_must_divide_target():
    with pytest.raises(ValueError):
        CandleBuilder("10m", source_timeframe="3m")
    with pytest.raises(ValueError):
        CandleBuilder("5m", source_timeframe="15m")


def test_missing_volume_stays_missing():
    builder = CandleBuilder("5m")
    t0 = pd.Timestamp("2024-05-01 00:00", tz="UTC")
    out = builder.update({"t": t0, "o": 1, "h": 2, "l": 0.5, "c": 1.5})
    assert out[-1].volume is None


def test_candle_feeds_an_indicator():
    from lumibot.indicators import registry

    builder = CandleBuilder("5m")
    sma = registry.create("sma", length=2)
    t0 = int(pd.Timestamp("2024-05-01 00:00", tz="UTC").value // 1_000_000)
    values = []
    for row in _bars_ms(t0, 20):
        for candle in builder.update(row):
            result = sma.update(candle, closed=candle.closed)
            if candle.closed:
                values.append(result["sma"])
    assert len(values) == 3 and values[0] is None and values[1] is not None
    assert isinstance(builder.forming, Candle) or builder.forming is None
    assert not any(isinstance(v, float) and math.isnan(v) for v in values)
