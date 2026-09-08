"""Use actual indicator/tool code; fixture only the underlying OHLCV source."""

import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.components.agents.builtins import _bind_get_indicator, _bind_get_indicators
from lumibot.entities import Asset
from lumibot.indicators import Indicators


def make_strategy(future_close=30):
    asset = Asset("SPY")
    dates = pd.date_range("2024-01-02", periods=3, tz="UTC")
    frame = pd.DataFrame({"close": [10.0, 20.0, future_close]}, index=dates)
    state = {"now": dates[1]}
    strategy = SimpleNamespace(
        broker=SimpleNamespace(data_source=SimpleNamespace(_data_store={(asset, None): SimpleNamespace(df=frame)})),
        get_datetime=lambda: state["now"],
    )
    strategy.indicators = Indicators(strategy)
    return strategy, asset, frame, state


@pytest.mark.parametrize("future_close", [30, 3000])
def test_custom_calculation_never_receives_future_rows(future_close):
    strategy, asset, frame, _ = make_strategy(future_close)
    received = []

    def total_so_far(history):
        received.extend(history.index)
        return pd.Series(history.close.sum(), index=history.index)

    assert strategy.indicators.custom("total", total_so_far, asset) == 30
    assert received == list(frame.index[:2])


@pytest.mark.parametrize(
    "parameters",
    [
        {"offset": -1},
        {"offset": -0.5},
        {"offset": "-1"},
        {"center": True},
        {"lookahead": True},
    ],
)
def test_noncausal_requests_rejected_by_actual_tool(parameters):
    strategy, _, _, _ = make_strategy()
    tool = _bind_get_indicator(strategy, None).function
    with pytest.raises(ValueError, match="causal"):
        tool("SPY", "sma", parameters_json=json.dumps({"length": 2, **parameters}))


def test_causal_sma_is_future_invariant_at_actual_tool_boundary():
    values = []
    for future in (30, 3000):
        strategy, _, _, _ = make_strategy(future)
        result = _bind_get_indicator(strategy, None).function("SPY", "sma", parameters_json='{"length": 2}')
        values.append(result["value"])
    assert values == [15, 15]


def test_cache_recomputes_when_time_rewinds_or_observed_bar_is_corrected():
    strategy, asset, frame, state = make_strategy()
    calls = []

    def total(history):
        calls.append(len(history))
        return pd.Series(history.close.sum(), index=history.index)

    assert strategy.indicators.custom("total", total, asset) == 30
    assert strategy.indicators.custom("total", total, asset) == 30
    assert calls == [2]
    frame.iloc[1, 0] = 25
    assert strategy.indicators.custom("total", total, asset) == 35
    state["now"] = frame.index[0]
    assert strategy.indicators.custom("total", total, asset) == 10
    assert calls == [2, 2, 1]


def test_custom_mutation_does_not_change_source_bars():
    strategy, asset, frame, _ = make_strategy()

    def mutating(history):
        history.loc[:, "close"] = 0
        return history.close

    strategy.indicators.custom("mutating", mutating, asset)
    assert frame.close.tolist() == [10, 20, 30]


def test_positive_offset_remains_supported_without_future_data():
    strategy, asset, _, _ = make_strategy()
    assert strategy.indicators.sma(asset, length=1, offset=1) == 10


def test_missing_warmup_is_json_null_not_zero_or_nan():
    strategy, _, _, _ = make_strategy()
    result = _bind_get_indicator(strategy, None).function("SPY", "sma", parameters_json='{"length": 200}')
    assert result["value"] is None
    json.dumps(result, allow_nan=False)


def test_parameterized_batch_preserves_ids_and_per_item_failures():
    strategy, _, _, _ = make_strategy()
    tool = _bind_get_indicators(strategy, None).function
    result = tool(
        "SPY",
        requests_json=json.dumps(
            [
                {"id": "short", "indicator": "sma", "parameters": {"length": 1}},
                {"id": "long", "indicator": "sma", "parameters": {"length": 2}},
                {"id": "invalid", "indicator": "sma", "parameters": {"offset": -1}},
            ]
        ),
    )
    assert [row["id"] for row in result["results"]] == ["short", "long", "invalid"]
    assert [row.get("value") for row in result["results"][:2]] == [20, 15]
    assert result["results"][2]["ok"] is False
    assert result["complete"] is False


@pytest.mark.parametrize(
    "requests",
    [
        [],
        [{"id": "same", "indicator": "sma"}] * 2,
        [{"id": "x", "indicator": "sma", "parameters": []}],
        [{"id": "x", "indicator": "sma", "made_up_option": 1}],
    ],
)
def test_invalid_batch_rejected_before_calculation(requests):
    strategy, _, _, _ = make_strategy()
    with pytest.raises(ValueError):
        _bind_get_indicators(strategy, None).function("SPY", requests_json=json.dumps(requests))
    assert strategy.indicators.cache_size == 0
