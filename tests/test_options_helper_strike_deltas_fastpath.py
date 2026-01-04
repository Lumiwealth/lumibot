from __future__ import annotations

from datetime import date, datetime

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset


class _Vars:
    pass


class _StrategyStub:
    def __init__(self, *, target_call_delta: float | None = None, target_put_delta: float | None = None):
        self.vars = _Vars()
        if target_call_delta is not None:
            self.vars.target_call_delta = target_call_delta
        if target_put_delta is not None:
            self.vars.target_put_delta = target_put_delta

    def log_message(self, *_args, **_kwargs):
        return None

    def get_datetime(self):
        return datetime(2025, 1, 1, 9, 30)

    def get_last_price(self, _asset):
        return 100.0


def _closest_strike(mapping: dict[float, float | None], target: float) -> float:
    best_strike = None
    best_diff = None
    for strike, delta in mapping.items():
        if delta is None:
            continue
        diff = abs(delta - target)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_strike = strike
    assert best_strike is not None
    return best_strike


def test_get_strike_deltas_fast_path_uses_few_calls_for_calls():
    strategy = _StrategyStub(target_call_delta=0.4)
    helper = OptionsHelper(strategy)

    call_count = 0

    def fake_get_delta_for_strike(_underlying_asset, _underlying_price, strike, _expiry, right):
        nonlocal call_count
        call_count += 1
        assert str(right).lower().startswith("c")
        # Linear monotonic delta: strike 50 -> 1.0, strike 150 -> 0.0
        return 1.0 - ((float(strike) - 50.0) / 100.0)

    helper.get_delta_for_strike = fake_get_delta_for_strike  # type: ignore[assignment]

    underlying = Asset("SPX", asset_type="stock")
    strikes = list(range(50, 151))  # 101 strikes (>= 80 triggers fast-path)

    deltas = helper.get_strike_deltas(underlying, date(2025, 1, 17), strikes, "call")

    # The fast-path should not evaluate every strike.
    assert call_count < 40
    chosen = _closest_strike(deltas, 0.4)
    # For the linear mapping, strike=110 yields delta=0.4
    assert abs(chosen - 110.0) <= 2.0


def test_get_strike_deltas_fast_path_uses_few_calls_for_puts():
    strategy = _StrategyStub(target_put_delta=-0.4)
    helper = OptionsHelper(strategy)

    call_count = 0

    def fake_get_delta_for_strike(_underlying_asset, _underlying_price, strike, _expiry, right):
        nonlocal call_count
        call_count += 1
        assert str(right).lower().startswith("p")
        # Linear monotonic delta: strike 50 -> 0.0, strike 150 -> -1.0
        return -((float(strike) - 50.0) / 100.0)

    helper.get_delta_for_strike = fake_get_delta_for_strike  # type: ignore[assignment]

    underlying = Asset("SPX", asset_type="stock")
    strikes = list(range(50, 151))  # 101 strikes (>= 80 triggers fast-path)

    deltas = helper.get_strike_deltas(underlying, date(2025, 1, 17), strikes, "put")

    assert call_count < 40
    chosen = _closest_strike(deltas, -0.4)
    # For the linear mapping, strike=90 yields delta=-0.4
    assert abs(chosen - 90.0) <= 2.0

