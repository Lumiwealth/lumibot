import math
import pytest

from lumibot.tools.black_scholes import (
    call_price,
    put_price,
    delta,
    gamma,
    vega,
    theta,
    rho,
)

def almost_equal(a, b, tol=1e-6):
    return abs(a - b) < tol

def test_call_put_parity():
    S = 100.0
    K = 100.0
    t = 30 / 365
    r = 0.01
    sigma = 0.2
    # compute call and put via Black–Scholes
    c = call_price(S, K, r, t, sigma)
    p = put_price(S, K, r, t, sigma)
    # parity: P = C - S + K e^{-r t}
    parity_put = c - S + K * math.exp(-r * t)
    assert almost_equal(p, parity_put)

def test_greeks_positive_bounds():
    S = 100.0
    K = 100.0
    t = 30 / 365
    r = 0.01
    sigma = 0.2
    # delta call ∈ (0,1)
    d_call = delta(S, K, r, t, sigma, option_type="call")
    assert 0 < d_call < 1
    # delta put ∈ (-1,0)
    d_put = delta(S, K, r, t, sigma, option_type="put")
    assert -1 < d_put < 0
    # gamma > 0
    assert gamma(S, K, r, t, sigma) > 0
    # vega > 0
    assert vega(S, K, r, t, sigma) > 0
    # rho(call) > 0, rho(put) < 0
    assert rho(S, K, r, t, sigma, option_type="call") > 0
    assert rho(S, K, r, t, sigma, option_type="put") < 0
