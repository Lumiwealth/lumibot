"""Black-Scholes greeks use scipy's own normal functions without the per-call argument checks.

``scipy.stats.norm.cdf(x)`` on one number spends most of its time validating arguments and
building arrays before it calls ``scipy.special.ndtr(x)``; ``norm.pdf`` likewise ends in
``exp(-x**2/2)/sqrt(2*pi)``. A delta pick prices every listed strike, which made these checks
the largest cost of a backtest that picks strikes by delta. The shortcut must return the
same bits as scipy for every input, including infinities and NaN, and every BS greek must be
unchanged.
"""

import math
import random

import numpy as np
import pytest
from scipy.stats import norm as scipy_norm

from lumibot.tools import black_scholes
from lumibot.tools.black_scholes import BS


def _same(a, b):
    if isinstance(a, float) and math.isnan(a):
        return isinstance(b, float) and math.isnan(b) and type(a) is type(b)
    return a == b and type(a) is type(b) and math.copysign(1, a) == math.copysign(1, b)


SPECIAL = [0.0, -0.0, 1e-300, -1e-300, 8.0, -8.0, 38.5, -38.5, 40.0, -40.0, math.inf, -math.inf, math.nan]


def test_cdf_and_pdf_are_bit_identical_to_scipy():
    rng = random.Random(3)
    xs = SPECIAL + [rng.uniform(-12, 12) for _ in range(20_000)] + [np.float64(rng.gauss(0, 3)) for _ in range(2_000)]
    for x in xs:
        assert _same(black_scholes._norm.cdf(x), scipy_norm.cdf(x)), x
        assert _same(black_scholes._norm.pdf(x), scipy_norm.pdf(x)), x


def test_arrays_still_go_through_scipy():
    x = np.array([-1.0, 0.0, 2.5])
    assert np.array_equal(black_scholes._norm.cdf(x), scipy_norm.cdf(x))
    assert np.array_equal(black_scholes._norm.pdf(x), scipy_norm.pdf(x))


FIELDS = ["callPrice", "putPrice", "callDelta", "putDelta", "callDelta2", "putDelta2", "callTheta", "putTheta",
          "callRho", "putRho", "vega", "gamma", "exerciceProbability"]


@pytest.mark.parametrize("seed", range(4))
def test_every_bs_greek_is_unchanged(monkeypatch, seed):
    rng = random.Random(seed)
    cases = []
    for _ in range(400):
        s = rng.uniform(20, 700)
        cases.append(([s, s * rng.uniform(0.6, 1.4), rng.uniform(0, 6), rng.uniform(0.05, 200)], rng.uniform(5, 150)))
    fast = [BS(args, volatility=vol) for args, vol in cases]
    monkeypatch.setattr(black_scholes, "_norm", scipy_norm)
    slow = [BS(args, volatility=vol) for args, vol in cases]
    for a, b in zip(fast, slow):
        for field in FIELDS:
            if hasattr(b, field):
                assert _same(getattr(a, field), getattr(b, field)), field
