"""Implied volatility for Black-Scholes: same answers as the original solver, without its cost.

The original solver built a pricing object through ``eval`` on every bisection step and kept
stepping up to 10,000 times when no volatility reproduces the price to its printed decimals.
One greeks call measured 12 to 15 ms and one delta strike search about 105 ms in a minute-level
option backtest (scenario timings, 2026-11-10).
"""
import random

import numpy as np
import pytest

from lumibot.tools import black_scholes


def _reference_implied_volatility(args, callPrice=None, putPrice=None, high=500.0, low=0.0):
    """The original algorithm, kept verbatim as the oracle."""
    BS = black_scholes.BS
    if callPrice:
        target = callPrice
        restimate = BS(args, volatility=high, performance=True).callPrice
        if restimate < target:
            return high
        if args[0] > args[1] + callPrice:
            return 0.001
    if putPrice:
        target = putPrice
        restimate = BS(args, volatility=high, performance=True).putPrice
        if restimate < target:
            return high
        if args[1] > args[0] + putPrice:
            return 0.001
    decimals = len(str(target).split(".")[1])
    for _ in range(10000):
        mid = (high + low) / 2
        if mid < 0.00001:
            mid = 0.00001
        if callPrice:
            estimate = BS(args, volatility=mid, performance=True).callPrice
        if putPrice:
            estimate = BS(args, volatility=mid, performance=True).putPrice
        if np.round(estimate, decimals) == target:
            break
        elif estimate > target:
            high = mid
        elif estimate < target:
            low = mid
    return mid


def _cases(count, seed):
    rng = random.Random(seed)
    for _ in range(count):
        underlying = round(rng.uniform(20, 700), 2)
        strike = float(round(underlying * rng.uniform(0.7, 1.3)))
        rate = round(rng.uniform(0.0, 6.0), 2)
        days = rng.choice([0.02, 0.3, 1.0, 2.5, 7.0, 30.0, 90.0, 365.0])
        price = round(rng.choice([0.01, 0.05, 0.37, 1.1, 2.63, 9.4, 25.0, 80.0]) * rng.uniform(0.5, 1.5), 2)
        yield [underlying, strike, rate, days], max(price, 0.01)


def test_implied_volatility_matches_the_original_solver_exactly():
    for args, price in _cases(400, seed=7):
        for kwargs in ({"callPrice": price}, {"putPrice": price}):
            expected = _reference_implied_volatility(args, **kwargs)
            actual = black_scholes.impliedVolatility("BS", args, **kwargs)
            assert actual == expected, (args, kwargs, actual, expected)


def test_bs_object_reports_the_same_implied_volatility_and_greeks():
    args = [585.25, 590.0, 4.5, 3.2]
    solved = black_scholes.BS(args, callPrice=2.63)
    assert solved.impliedVolatility == _reference_implied_volatility(args, callPrice=2.63)
    greeks = black_scholes.BS(args, volatility=solved.impliedVolatility)
    assert 0 < greeks.callDelta < 1


def test_implied_volatility_does_not_build_a_pricing_object_per_step(monkeypatch):
    """A price no volatility can reproduce used to cost 10,000 object builds."""
    built = []
    real_init = black_scholes.BS.__init__

    def counting_init(self, *args, **kwargs):
        built.append(1)
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(black_scholes.BS, "__init__", counting_init)
    args = [585.25, 590.0, 4.5, 3.2]
    value = black_scholes.impliedVolatility("BS", args, callPrice=2.6349999)
    assert value > 0
    assert len(built) == 0


def test_other_models_keep_the_original_path():
    gk = black_scholes.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359)
    assert gk.impliedVolatility == pytest.approx(20.0, abs=0.5)
