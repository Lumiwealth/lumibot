"""Every example in the Indicators documentation runs.

Offline tests cover every example. Network examples replace only the HTTP or
exchange boundary with a response in the provider's real shape (recorded from
the live public endpoint on 2026-10-25). The ``apitest``/``public_http``
tests call the real public endpoints and are run by hand:

    pytest tests/test_example_indicators.py -m apitest
"""

from __future__ import annotations

import math
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.example_indicators import donchian_midline, funding_rate, ml_direction, up_down_ratio, volume_weighted_trend
from lumibot.example_indicators.live_crypto_stream import stream
from lumibot.indicators import registry


def _frame(n=120, seed=7, freq="1D", start="2024-01-01"):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    index = pd.date_range(start, periods=n, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "open": close + rng.normal(0, 0.2, n),
            "high": close + 1 + rng.random(n),
            "low": close - 1 - rng.random(n),
            "close": close,
            "volume": rng.integers(100, 1000, n).astype(float),
        },
        index=index,
    )


EXAMPLES = [
    (donchian_midline.DonchianMidline, {"length": 10}),
    (up_down_ratio.UpDownRatio, {"length": 7}),
    (volume_weighted_trend.VolumeWeightedTrend, {"length": 9}),
    (ml_direction.MLDirection, {"lookback": 3, "train_window": 40, "retrain_every": 5}),
]


@pytest.mark.parametrize("cls,inputs", EXAMPLES)
def test_example_is_registered_and_describes_itself(cls, inputs):
    assert registry.get(cls.key, cls.version) is cls
    meta = cls.describe(inputs)
    assert meta["key"] == cls.key and meta["outputs"]


@pytest.mark.parametrize("cls,inputs", EXAMPLES)
def test_example_streaming_equals_compute_and_forming_is_rollback_safe(cls, inputs):
    df = _frame()
    full = cls(**inputs).compute(df)
    live = cls(**inputs)
    bars = list(df.itertuples())
    for i, row in enumerate(bars):
        bar = {"t": row.Index, "o": row.open, "h": row.high, "l": row.low, "c": row.close, "v": row.volume}
        forming = live.update({**bar, "c": row.close * 1.01}, closed=False)  # a tick before the close
        assert set(forming) == {o.id for o in cls.outputs}
        committed = live.update(bar, closed=True)
        for output, series in full.items():
            assert committed[output] == pytest.approx(series[i], nan_ok=True) or (committed[output] is None and series[i] is None)


@pytest.mark.parametrize("cls,inputs", EXAMPLES)
def test_example_never_uses_future_bars(cls, inputs):
    df = _frame()
    full = cls(**inputs).compute(df)
    for k in (30, 61, 99):
        prefix = cls(**inputs).compute(df.iloc[:k])
        for output in full:
            assert prefix[output][-1] == full[output][k - 1]


def test_donchian_midline_values():
    df = _frame(30)
    out = donchian_midline.DonchianMidline(length=5).compute(df)
    i = 10
    hi, lo = df["high"].iloc[i - 4 : i + 1].max(), df["low"].iloc[i - 4 : i + 1].min()
    assert out["upper"][i] == hi and out["lower"][i] == lo and out["mid"][i] == (hi + lo) / 2
    assert out["mid"][3] is None
    assert donchian_midline.DonchianMidline.overlay is True


def test_up_down_ratio_values():
    closes = [10, 11, 10, 12, 13, 12]
    df = pd.DataFrame({"open": closes, "high": closes, "low": closes, "close": closes}, index=pd.date_range("2024", periods=6, tz="UTC"))
    out = up_down_ratio.UpDownRatio(length=3).compute(df)["ratio"]
    # moves: +1 -1 +2 +1 -1 ; last 3 = +2 +1 -1 -> 3 / 4
    assert out[:3] == [None, None, None]
    assert out[5] == pytest.approx(75.0)
    assert up_down_ratio.UpDownRatio.overlay is False
    assert [lv["value"] for lv in up_down_ratio.UpDownRatio().current_levels()] == [70, 30]


def test_volume_weighted_trend_values_and_missing_volume():
    df = _frame(40)
    out = volume_weighted_trend.VolumeWeightedTrend(length=5).compute(df)["trend"]
    w = df.iloc[10 - 4 : 11]
    sma = w["close"].mean()
    vwma = (w["close"] * w["volume"]).sum() / w["volume"].sum()
    assert out[10] == pytest.approx(100 * (vwma - sma) / sma)
    no_volume = df.drop(columns=["volume"])
    assert set(volume_weighted_trend.VolumeWeightedTrend(length=5).compute(no_volume)["trend"]) == {None}


def test_ml_direction_learns_a_simple_pattern():
    # Returns alternate down/up: after a down bar the next bar goes up.
    closes = [100.0]
    for i in range(300):
        closes.append(closes[-1] * (1.01 if i % 2 else 0.99))
    df = pd.DataFrame({"open": closes, "high": closes, "low": closes, "close": closes}, index=pd.date_range("2024", periods=len(closes), tz="UTC"))
    out = ml_direction.MLDirection(lookback=2, train_window=100, retrain_every=10).compute(df)["prob_up"]
    last_return_down = closes[-1] < closes[-2]
    assert out[-1] is not None
    assert (out[-1] > 80) if last_return_down else (out[-1] < 20)
    assert all(v is None or 0 <= v <= 100 for v in out)


# ---- funding rate (outside data) ----------------------------------------------

# Shape recorded from GET https://www.okx.com/api/v5/public/funding-rate-history on 2026-10-25.
def _okx_page(times_ms, rates):
    return {
        "code": "0",
        "data": [
            {
                "formulaType": "withRate",
                "fundingRate": f"{r:.16f}",
                "fundingTime": str(t),
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "method": "current_period",
                "realizedRate": f"{r:.16f}",
            }
            for t, r in zip(times_ms, rates)
        ],
        "msg": "",
    }


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeSession:
    """Serves newest-first pages like OKX, honoring the ``after`` cursor."""

    def __init__(self, times_ms, rates, page_size=4):
        self.rows = sorted(zip(times_ms, rates), reverse=True)
        self.page_size = page_size
        self.calls = []

    def get(self, url, params=None, timeout=None):
        assert url == funding_rate.OKX_FUNDING_HISTORY_URL and timeout
        self.calls.append(dict(params))
        rows = self.rows
        if "after" in params:
            rows = [r for r in rows if r[0] < int(params["after"])]
        page = rows[: self.page_size]
        return _Response(_okx_page([t for t, _ in page], [r for _, r in page]))


EIGHT_HOURS_MS = 8 * 3600 * 1000
T0 = int(pd.Timestamp("2024-03-01", tz="UTC").value // 1_000_000)


def test_fetch_okx_funding_history_pages_with_after_cursor():
    times = [T0 + i * EIGHT_HOURS_MS for i in range(10)]
    session = _FakeSession(times, [0.0001 * i for i in range(10)], page_size=4)
    rows = funding_rate.fetch_okx_funding_history("BTC-USDT-SWAP", pages=3, session=session)
    assert [t for t, _ in rows] == times  # oldest first, all three pages
    assert "after" not in session.calls[0] and session.calls[1]["after"] == str(times[6])


def test_funding_rate_indicator_never_reads_a_future_rate():
    times = [T0 + i * EIGHT_HOURS_MS for i in range(6)]
    rates = [0.0001, -0.0002, 0.0003, 0.0004, -0.0005, 0.0006]

    class Recorded(funding_rate.FundingRate):
        loader = staticmethod(lambda inst_id, pages: list(zip(times, rates)))

    hours = pd.date_range(pd.Timestamp(T0, unit="ms", tz="UTC") - pd.Timedelta(hours=1), periods=50, freq="1h")
    df = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=hours)
    out = Recorded().compute(df)["rate_pct"]
    assert out[0] is None  # before the first funding event
    for ts, value in zip(hours[1:], out[1:]):
        ts_ms = ts.value // 1_000_000
        expected = max(i for i, t in enumerate(times) if t <= ts_ms)
        assert value == pytest.approx(100 * rates[expected])


@pytest.mark.apitest
@pytest.mark.public_http
def test_funding_rate_against_live_okx_public_api():
    rows = funding_rate.fetch_okx_funding_history("BTC-USDT-SWAP", pages=1)
    assert len(rows) >= 10 and all(abs(rate) < 0.05 for _, rate in rows)
    assert rows == sorted(rows)


# ---- live crypto stream ---------------------------------------------------------


class _FakeExchange:
    """CCXT-shaped exchange: ``timeframes`` and ``fetch_ohlcv`` rows ``[ms, o, h, l, c, v]``."""

    def __init__(self, timeframes, minutes=200, start="2024-05-01 00:00"):
        self.timeframes = timeframes
        start_ms = int(pd.Timestamp(start, tz="UTC").value // 1_000_000)
        self.minute_rows = []
        price = 100.0
        for i in range(minutes):
            o = price
            price = price + math.sin(i / 5)
            self.minute_rows.append([start_ms + i * 60_000, o, max(o, price) + 0.1, min(o, price) - 0.1, price, 1.0 + i])
        self.visible = minutes - 3  # rows the "exchange" has published so far
        self.requests = []

    def fetch_ohlcv(self, symbol, timeframe, limit=None):
        self.requests.append((symbol, timeframe, limit))
        rows = self.minute_rows[: self.visible]
        self.visible = min(self.visible + 1, len(self.minute_rows))  # time passes between polls
        step = int(pd.Timedelta(timeframe).total_seconds() // 60) if timeframe != "1m" else 1
        if step == 1:
            out = [list(r) for r in rows]
        else:
            frame = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
            frame.index = pd.to_datetime(frame["t"], unit="ms", utc=True)
            from lumibot.tools.candles import build_candles

            candles = build_candles(frame[["open", "high", "low", "close", "volume"]], timeframe)
            out = [[int(ts.value // 1_000_000), *row] for ts, row in zip(candles.index, candles.values.tolist())]
        return out[-limit:] if limit else out


@pytest.mark.parametrize("native", [True, False])
def test_live_stream_forming_then_closed_values(native):
    timeframes = {"1m": "1", "15m": "15"} if native else {"1m": "1"}
    exchange = _FakeExchange(timeframes)
    indicators = {"sma": registry.create("sma", length=3)}
    events = list(stream(exchange, "BTC/USD", "15m", indicators, history=50, polls=6, sleep=lambda s: None))
    assert exchange.requests[0][1] == ("15m" if native else "1m")
    assert events, "the stream must yield updates"
    assert any(not candle.closed for candle, _ in events)
    times = [candle.time for candle, _ in events]
    assert all(t.minute % 15 == 0 for t in times)
    for candle, values in events:
        assert set(values) == {"sma"}


def test_live_stream_built_candles_match_native_candles():
    native = list(stream(_FakeExchange({"1m": "1", "15m": "15"}), "X", "15m", {"sma": registry.create("sma", length=2)},
                         history=50, polls=1, sleep=lambda s: None))
    built = list(stream(_FakeExchange({"1m": "1"}), "X", "15m", {"sma": registry.create("sma", length=2)},
                        history=50, polls=1, sleep=lambda s: None))
    assert native[-1][0].time == built[-1][0].time
    assert native[-1][1]["sma"]["sma"] == pytest.approx(built[-1][1]["sma"]["sma"])


@pytest.mark.apitest
@pytest.mark.public_http
def test_live_stream_against_a_real_public_exchange():
    ccxt = pytest.importorskip("ccxt")
    exchange = ccxt.kraken({"enableRateLimit": True})
    events = list(stream(exchange, "BTC/USD", "15m", {"rsi": registry.create("rsi", length=14)},
                         history=100, polls=2, poll_seconds=2))
    assert events and events[-1][1]["rsi"]["rsi"] is not None


# ---- strategy example: backtest -------------------------------------------------


@pytest.mark.usefixtures("disable_datasource_override")
def test_indicator_strategy_backtests_offline(tmp_path, monkeypatch):
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, Data
    from lumibot.example_indicators.indicator_strategy import CustomIndicatorStrategy

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    index = pd.date_range("2024-10-01", periods=90, freq="B", tz="America/New_York")
    closes = [50 + 4 * math.sin(i / 6) + 0.05 * i for i in range(90)]
    frame = pd.DataFrame(
        {"open": closes, "high": [c + 0.5 for c in closes], "low": [c - 0.5 for c in closes], "close": closes, "volume": [1000] * 90},
        index=index,
    )
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    _, strategy = CustomIndicatorStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2024, 11, 18),
        backtesting_end=datetime(2025, 1, 31),
        pandas_data={asset: Data(asset, frame, timestep="day")},
        parameters={"symbol": "SPY", "length": 10},
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    log = strategy.vars.log
    assert len(log) >= 20
    expected = donchian_midline.DonchianMidline(length=10).compute(frame)["mid"]
    by_time = dict(zip(frame.index, expected))
    for when, mid, ratio in log:
        bar_time = max(t for t in frame.index if t <= when)
        assert mid == pytest.approx(by_time[bar_time])
        assert 0 <= ratio <= 100


def test_quickstart_runs():
    from lumibot.example_indicators.quickstart import main

    series, dcm = main()
    assert series["mid"][-1] is not None and dcm.bars_seen == 61


def test_funding_rate_demo_runs_with_recorded_data(capsys):
    from lumibot.example_indicators.funding_rate_demo import main

    now_ms = int(pd.Timestamp.now(tz="UTC").value // 1_000_000)
    rows = [(now_ms - i * EIGHT_HOURS_MS, 0.0001 * (i + 1)) for i in range(10)][::-1]

    class Recorded(funding_rate.FundingRate):
        loader = staticmethod(lambda inst_id, pages: rows)

    rates = main("BTC-USDT-SWAP", hours=24, indicator_cls=Recorded)
    assert rates[-1] is not None and "funding" in capsys.readouterr().out
