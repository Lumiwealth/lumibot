"""Unit fixtures for lazy indicator history, not backtest execution."""
import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.indicators import Indicators
from lumibot.strategies.strategy import Strategy
from lumibot.tools.ibkr_history_health import (
    ibkr_history_health_snapshot, record_required_history,
    reset_ibkr_history_health_for_testing,
)


@pytest.fixture(autouse=True)
def clean_history_requirements():
    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def fixture_strategy():
    asset = Asset("SPY")
    frame = pd.DataFrame({"close": [10., 12., 14., 1000.]},
        index=pd.date_range("2024-01-01", periods=4, tz="UTC"))
    now = frame.index[2]
    class Source:
        def __init__(self):
            self.inputs = []
        def get_historical_prices(self, asset, length, **kwargs):
            return SimpleNamespace(pandas_df=frame, df=frame)
        def record_history_requirement(self, *, asset, timestep, requested_bars, bars, **kwargs):
            record_required_history(asset=asset, timestep=timestep, requested_bars=requested_bars,
                                    frame=bars.pandas_df, when=now)
        def record_indicator_input(self, *, asset, timestep, frame):
            self.inputs.append(frame.copy())
    source = Source()
    strategy = SimpleNamespace(broker=SimpleNamespace(data_source=source, option_source=None),
        quote_asset=Asset("USD", "forex"), is_backtesting=True,
        _logged_get_historical_prices_assets=set(), logger=logging.getLogger("unit-fixture"),
        _parse_timestep=lambda value: (1, value), _sanitize_user_asset=lambda value: value,
        crypto_assets_to_tuple=lambda value, quote: value, get_datetime=lambda: now)
    strategy.get_historical_prices = lambda *a, **kw: Strategy.get_historical_prices(strategy, *a, **kw)
    if hasattr(Strategy, "_prefetch_indicator_history"):
        strategy._prefetch_indicator_history = lambda *a, **kw: Strategy._prefetch_indicator_history(strategy, *a, **kw)
    return asset, frame, source, strategy


def test_optional_indicator_warmup_does_not_demand_ten_thousand_required_bars():
    asset, frame, source, strategy = fixture_strategy()
    result = Indicators(strategy).custom("mean", lambda df: df.close.rolling(2).mean(), asset)
    assert result == 13
    assert ibkr_history_health_snapshot()["required_complete"] is True
    assert len(source.inputs) == 1
    assert source.inputs[0].equals(frame.iloc[:3])  # Actual causal inputs, no future row.


def test_explicit_customer_history_request_remains_strict():
    asset, _, _, strategy = fixture_strategy()
    strategy.get_historical_prices(asset, 10_000, timestep="day")
    assert len(ibkr_history_health_snapshot()["required_failures"]) == 1


def test_indicator_consumed_input_still_rejects_real_missing_rows():
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    asset, frame, _, _ = fixture_strategy()
    frame = frame.iloc[:3].copy()
    frame.loc[frame.index[1], "close"] = float("nan")
    source = SimpleNamespace(_provider_spec_for_asset=lambda _: SimpleNamespace(provider="ibkr"),
        get_datetime=lambda: frame.index[-1], _registry=SimpleNamespace(_adapters={}))
    RoutedBacktestingPandas.record_indicator_input(source, asset=asset, timestep="day", frame=frame)
    assert len(ibkr_history_health_snapshot()["required_failures"]) == 1


def test_string_asset_is_normalized_before_input_validation():
    asset, _, source, strategy = fixture_strategy()
    strategy._sanitize_user_asset = lambda value: asset if value == "SPY" else value
    original = source.record_indicator_input
    def record(**kwargs):
        assert kwargs["asset"] == asset
        original(**kwargs)
    source.record_indicator_input = record
    assert Indicators(strategy).custom("mean", lambda df: df.close.rolling(2).mean(), "SPY") == 13


def test_option_inputs_are_validated_by_the_selected_option_source():
    asset, _, option_source, strategy = fixture_strategy()
    asset = Asset("SPY", "option", strike=500, right="call")
    strategy.broker.option_source = option_source
    strategy.broker.data_source = SimpleNamespace(record_indicator_input=lambda **_: pytest.fail("wrong source"))
    assert Indicators(strategy).custom("mean", lambda df: df.close.rolling(2).mean(), asset) == 13
    assert len(option_source.inputs) == 1
