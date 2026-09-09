"""Actual indicator tools with independently computed numerical expectations.

These are deterministic contracts, not real-model eval passes. Market data is
synthetic; no provider, broker, or inference network calls are required.
"""

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from lumibot.components.agents.builtins import _bind_get_indicators
from lumibot.entities import Asset
from lumibot.indicators import Indicators


def _fixture(future_price=900.0):
    daily_index = pd.date_range("2024-01-02", periods=260, freq="B", tz="America/New_York")
    close = 100 + np.arange(260) * 0.3 + np.sin(np.arange(260)) * 2
    daily = pd.DataFrame(
        {"open": close - 0.2, "high": close + 1, "low": close - 1, "close": close, "volume": 1000},
        index=daily_index,
    )
    now = daily_index[-2] + pd.Timedelta(hours=16)
    daily.iloc[-1] = future_price
    minutes = pd.date_range(now.normalize() + pd.Timedelta(hours=9, minutes=30), periods=20, freq="min")
    minute_close = np.arange(20) + 200.0
    minute = pd.DataFrame(
        {
            "open": minute_close - 0.5,
            "high": minute_close + 1,
            "low": minute_close - 2,
            "close": minute_close,
            "volume": np.arange(20) + 1,
        },
        index=minutes,
    )
    asset = Asset("SPY")
    strategy = SimpleNamespace(
        broker=SimpleNamespace(
            data_source=SimpleNamespace(
                _data_store={
                    (asset, "day"): SimpleNamespace(df=daily, timestep="day"),
                    (asset, "minute"): SimpleNamespace(df=minute, timestep="minute"),
                }
            )
        ),
        get_datetime=lambda: now,
    )
    strategy.indicators = Indicators(strategy)
    return strategy, daily.iloc[:-1], minute


def _batch(strategy, requests):
    response = _bind_get_indicators(strategy, None).function("SPY", requests_json=json.dumps(requests))
    assert response["complete"] is True
    assert all(row["ok"] for row in response["results"])
    json.dumps(response, allow_nan=False)
    return {row["id"]: row["value"] for row in response["results"]}


def test_indicator_tool_preserves_crypto_quote_and_exchange_identity():
    now = pd.Timestamp("2026-09-08T16:00:00Z")
    stock = Asset("BTC", asset_type="stock")
    crypto = Asset("BTC", asset_type="crypto")
    quote = Asset("USD", asset_type="crypto")
    stock_frame = pd.DataFrame(
        {"close": [10.0, 11.0, 12.0]},
        index=pd.date_range("2026-09-06", periods=3, freq="D", tz="UTC"),
    )
    crypto_frame = pd.DataFrame(
        {"close": [100_000.0, 101_000.0, 102_000.0]},
        index=pd.date_range("2026-09-06", periods=3, freq="D", tz="UTC"),
    )
    strategy = SimpleNamespace(
        broker=SimpleNamespace(
            data_source=SimpleNamespace(
                _data_store={
                    (stock, "day"): SimpleNamespace(df=stock_frame, timestep="day"),
                    ((crypto, quote), "day"): SimpleNamespace(df=crypto_frame, timestep="day"),
                }
            )
        ),
        get_datetime=lambda: now,
    )
    strategy.indicators = Indicators(strategy)

    response = _bind_get_indicators(strategy, None).function(
        "BTC",
        asset_type="crypto",
        quote_symbol="USD",
        exchange="COINBASE",
        requests_json=json.dumps(
            [{"id": "crypto-sma", "indicator": "sma", "parameters": {"length": 2, "talib": False}}]
        ),
    )

    row = response["results"][0]
    assert row["asset_type"] == "crypto"
    assert row["quote_symbol"] == "USD"
    assert row["exchange"] == "COINBASE"
    assert row["value"] == pytest.approx(101_500.0)


def _adjusted_average(values, alpha):
    # Independent scalar recurrence, not pandas-ta or the production calculation.
    weighted = denominator = 0.0
    output = []
    for value in values:
        weighted = value + (1 - alpha) * weighted
        denominator = 1 + (1 - alpha) * denominator
        output.append(weighted / denominator)
    return output


def _seeded_ema(values, length):
    average = sum(values[:length]) / length
    output = [average]
    for value in values[length:]:
        average += 2 / (length + 1) * (value - average)
        output.append(average)
    return output


@pytest.mark.parametrize("future_price", [900.0, 9_000_000.0])
def test_rsi_vwap_sma50_sma200_one_batch_matches_independent_values(future_price):
    strategy, daily, minute = _fixture(future_price)
    result = _batch(
        strategy,
        [
            {"id": "sma50", "indicator": "sma", "parameters": {"length": 50, "talib": False}},
            {"id": "sma200", "indicator": "sma", "parameters": {"length": 200, "talib": False}},
            {"id": "rsi", "indicator": "rsi", "parameters": {"length": 14, "talib": False}},
            {"id": "vwap", "indicator": "vwap", "timestep": "minute"},
        ],
    )
    assert result["sma50"] == pytest.approx(sum(daily.close[-50:]) / 50)
    assert result["sma200"] == pytest.approx(sum(daily.close[-200:]) / 200)
    changes = np.diff(daily.close)
    gains = _adjusted_average([max(value, 0) for value in changes], 1 / 14)[-1]
    losses = _adjusted_average([max(-value, 0) for value in changes], 1 / 14)[-1]
    assert result["rsi"] == pytest.approx(100 * gains / (gains + losses))
    typical = (minute.high + minute.low + minute.close) / 3
    assert result["vwap"] == pytest.approx(sum(typical * minute.volume) / sum(minute.volume))


def test_bollinger_atr_and_macd_are_finite_and_future_invariant():
    values = []
    for future in (900.0, 9_000_000.0):
        strategy, daily, _ = _fixture(future)
        response = _batch(
            strategy,
            [
                {"id": "bands", "indicator": "bbands", "parameters": {"length": 20, "std": 2, "talib": False}},
                {"id": "atr", "indicator": "atr", "parameters": {"length": 14, "talib": False}},
                {
                    "id": "macd",
                    "indicator": "macd",
                    "parameters": {"fast": 12, "slow": 26, "signal": 9, "talib": False},
                },
            ],
        )
        bands = response["bands"]
        mean, deviation = np.mean(daily.close[-20:]), np.std(daily.close[-20:], ddof=0)
        assert bands["BBM_20_2.0"] == pytest.approx(mean)
        assert bands["BBU_20_2.0"] == pytest.approx(mean + 2 * deviation)
        assert bands["BBL_20_2.0"] == pytest.approx(mean - 2 * deviation)
        true_ranges = [
            max(high - low, abs(high - previous), abs(low - previous))
            for high, low, previous in zip(daily.high[1:], daily.low[1:], daily.close[:-1])
        ]
        assert response["atr"] == pytest.approx(_adjusted_average(true_ranges, 1 / 14)[-1])
        closes = daily.close.tolist()
        fast, slow = _seeded_ema(closes, 12), _seeded_ema(closes, 26)
        macd = [short - long for short, long in zip(fast[14:], slow)]
        signal = _seeded_ema(macd, 9)[-1]
        assert response["macd"]["MACD_12_26_9"] == pytest.approx(macd[-1])
        assert response["macd"]["MACDs_12_26_9"] == pytest.approx(signal)
        assert response["macd"]["MACDh_12_26_9"] == pytest.approx(macd[-1] - signal)
        values.append(response)
    assert values[0] == values[1]


def test_missing_long_window_warmup_does_not_borrow_earlier_rows():
    strategy, daily, _ = _fixture()
    result = _batch(
        strategy,
        [
            {"id": "full", "indicator": "sma", "parameters": {"length": 200}},
            {
                "id": "bounded",
                "indicator": "sma",
                "parameters": {"length": 200},
                "start": daily.index[-50].isoformat(),
                "end": daily.index[-1].isoformat(),
            },
        ],
    )
    assert result["full"] is not None
    assert result["bounded"] is None


def test_monthly_and_annual_fibonacci_windows_are_independent():
    strategy, daily, _ = _fixture()
    windows = [(f"month-{month}", group) for month, group in daily.groupby(daily.index.month)]
    windows.append(("annual", daily))
    result = _batch(
        strategy,
        [
            {
                "id": name,
                "indicator": "fibonacci",
                "parameters": {"direction": "up"},
                "start": frame.index[0].isoformat(),
                "end": frame.index[-1].isoformat(),
            }
            for name, frame in windows
        ],
    )
    for name, frame in windows:
        high, low = max(frame.high), min(frame.low)
        assert result[name]["high"] == high
        assert result[name]["low"] == low
        for ratio in (0, 0.236, 0.382, 0.5, 0.618, 0.786, 1):
            assert result[name][f"retracement_{ratio:g}"] == pytest.approx(high - (high - low) * ratio)


@pytest.mark.parametrize(
    "parameters",
    [
        {"direction": "sideways"},
        {"length": 0},
        {"length": 1.5},
        {"length": True},
        {"offset": 1},
    ],
)
def test_fibonacci_rejects_ambiguous_or_unsupported_parameters(parameters):
    strategy, _, _ = _fixture()
    with pytest.raises(ValueError):
        strategy.indicators.fibonacci(Asset("SPY"), **parameters)


def test_fibonacci_down_flat_range_and_missing_warmup():
    strategy, daily, _ = _fixture()
    values = strategy.indicators.fibonacci(Asset("SPY"), direction="down", length=20)
    high, low = max(daily.high[-20:]), min(daily.low[-20:])
    assert values["retracement_0.618"] == pytest.approx(low + (high - low) * 0.618)
    assert strategy.indicators.fibonacci(Asset("SPY"), length=500) is None
    frame = pd.DataFrame({"high": [100.0], "low": [100.0]}, index=daily.index[:1])
    assert set(Indicators._fibonacci_range(frame).iloc[0]) == {100.0}


@pytest.mark.parametrize("high,low", [(float("nan"), 1), (float("inf"), 1), (1, 2)])
def test_fibonacci_rejects_corrupt_ohlc(high, low):
    frame = pd.DataFrame({"high": [high], "low": [low]}, index=pd.date_range("2024-01-01", periods=1))
    with pytest.raises(ValueError, match="finite"):
        Indicators._fibonacci_range(frame)
