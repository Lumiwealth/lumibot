"""One-contract option pricing takes a scalar path with bit-identical results.

Why: a one-year minute SPY iron condor backtest spends a large share of its portfolio
valuation in ``implied_volatility_forward`` and ``black_scholes_forward_price`` on ONE
contract at a time (every held leg without a real print is estimated every minute). The
vectorized formulas pay numpy broadcasting and errstate overhead on every one-element call,
and implied volatility makes 50 of them per contract.

Invariant under test: for plain scalar inputs the answer is bit-for-bit the answer of the
vectorized formula on a one-element array (same ufunc loops, correctly rounded arithmetic),
including no-solution cases, and the scalar path never builds broadcast arrays. Inputs that
are not clean positive finite numbers keep the vectorized path. Counts, never seconds.
"""

import math
import random

import numpy as np
import pytest

import lumibot.tools.option_price_estimate as ope


def _vector(fn, *args, **kwargs):
    # One-element arrays always take the vectorized path.
    arrays = [np.array([a]) for a in args]
    return float(np.asarray(fn(*arrays, **kwargs)).reshape(-1)[0])


def _same(a, b):
    a, b = float(a), float(b)
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return a == b and math.copysign(1.0, a) == math.copysign(1.0, b)


def _cases(n=3000):
    rng = random.Random(20261122)
    for _ in range(n):
        forward = rng.choice([rng.uniform(1, 50), rng.uniform(50, 700), rng.uniform(700, 6000)])
        strike = forward * math.exp(rng.uniform(-0.8, 0.8))
        years = rng.choice([1 / (365 * 24 * 60), rng.uniform(1e-4, 0.02), rng.uniform(0.02, 2.0)])
        vol = rng.choice([1e-4, rng.uniform(0.01, 0.2), rng.uniform(0.2, 1.5), rng.uniform(1.5, 8.0)])
        yield forward, strike, years, vol, rng.random() < 0.5, rng.choice([1.0, rng.uniform(0.9, 1.0)])


def test_scalar_price_is_bit_identical_to_vectorized():
    for forward, strike, years, vol, is_call, discount in _cases():
        scalar = ope.black_scholes_forward_price(forward, strike, years, vol, is_call, discount)
        vector = _vector(ope.black_scholes_forward_price, forward, strike, years, vol, is_call, discount)
        assert _same(scalar, vector), (forward, strike, years, vol, is_call, discount, scalar, vector)


def test_scalar_implied_volatility_is_bit_identical_including_no_solution():
    checked = solved = 0
    for forward, strike, years, vol, is_call, discount in _cases(1500):
        fair = float(ope.black_scholes_forward_price(forward, strike, years, vol, is_call, discount))
        # Real prices, prices below intrinsic (no solution) and absurd prices (no solution).
        for price in (fair, round(fair, 2), fair * 0.5, fair * 3.0 + 1.0):
            if not (price > 0):
                continue
            scalar = ope.implied_volatility_forward(price, forward, strike, years, is_call, discount)
            vector = _vector(ope.implied_volatility_forward, price, forward, strike, years, is_call, discount)
            assert _same(scalar, vector), (price, forward, strike, years, vol, is_call, discount, scalar, vector)
            checked += 1
            solved += not math.isnan(float(scalar))
    assert checked > 4000 and 0 < solved < checked


def test_scalar_path_builds_no_broadcast_arrays(monkeypatch):
    calls = {"n": 0}
    original = np.broadcast_arrays

    def counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ope.np, "broadcast_arrays", counting)
    ope.implied_volatility_forward(4.2, 640.0, 645.0, 0.08, True)
    ope.black_scholes_forward_price(640.0, 645.0, 0.08, 0.2, False)
    assert calls["n"] == 0
    ope.black_scholes_forward_price(np.array([640.0]), 645.0, 0.08, 0.2, False)
    assert calls["n"] == 1


@pytest.mark.parametrize(
    "args",
    [
        (float("nan"), 645.0, 0.08, 0.2, True),
        (640.0, 645.0, 0.0, 0.2, True),
        (640.0, 645.0, -0.01, 0.2, True),
        (640.0, 645.0, 0.08, 0.0, False),
        (640.0, 0.0, 0.08, 0.2, False),
        (float("inf"), 645.0, 0.08, 0.2, True),
    ],
)
def test_edge_inputs_keep_the_vectorized_answer(args):
    with np.errstate(all="ignore"):
        assert _same(ope.black_scholes_forward_price(*args), _vector(ope.black_scholes_forward_price, *args))
