"""Many options' greeks in one array pass, identical to one-by-one calculate_greeks.

The chain snapshot prices every strike of an expiry. Solving implied volatility one strike
at a time cost about 1 ms each; the array solver runs the same bisection on all strikes at
once. Results must be bit-identical (same arithmetic, same stopping rules) and the shared
per-day greeks cache must end up exactly as the one-by-one path leaves it.
"""

import random
from datetime import date, datetime

import pytest
import pytz

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset
from lumibot.tools.black_scholes import _implied_volatility_bs, implied_volatility_bs_many


def test_array_implied_volatility_is_bit_identical_to_the_scalar_solver():
    rng = random.Random(7)
    cases = []
    for _ in range(12_000):
        s = rng.uniform(50, 700)
        k = s * rng.uniform(0.7, 1.3)
        r = rng.uniform(0, 6)
        d = rng.uniform(0.1, 120)
        call = rng.random() < 0.5
        p = round(rng.uniform(0.01, 0.3) * s * rng.random(), rng.choice([2, 3, 4, 6])) or 0.01
        cases.append((s, k, r, d, p, call))
    expected = [
        _implied_volatility_bs([s, k, r, d], callPrice=p if c else None, putPrice=None if c else p)
        for s, k, r, d, p, c in cases
    ]
    got = implied_volatility_bs_many(*zip(*cases))
    assert [float(x) for x in got] == expected


def _source():
    tz = pytz.timezone("America/New_York")
    source = PandasDataBacktesting(
        datetime_start=tz.localize(datetime(2026, 10, 19)), datetime_end=tz.localize(datetime(2026, 10, 23)),
        pandas_data={},
    )
    source._datetime = tz.localize(datetime(2026, 10, 19, 10, 0))
    return source


def test_calculate_greeks_many_matches_one_by_one_including_the_cache():
    strikes = [480, 490, 495, 500, 505, 510, 520]
    prices = [21.37, 12.05, 8.1, 5.0, 2.95, 1.6, 0.42]
    calls = [Asset("SPY", asset_type="option", expiration=date(2026, 10, 23), strike=k, right="CALL") for k in strikes]
    puts = [Asset("SPY", asset_type="option", expiration=date(2026, 10, 23), strike=k, right="PUT") for k in strikes]
    assets = calls + puts
    asset_prices = prices + list(reversed(prices))

    one = _source()
    expected = [one.calculate_greeks(a, p, 500.0, 0.0412) for a, p in zip(assets, asset_prices)]
    many = _source()
    got = many.calculate_greeks_many(assets, asset_prices, 500.0, 0.0412)
    assert got == expected
    assert list(many._greeks_cache.items()) == list(one._greeks_cache.items())
    # A second call is served from the cache, unchanged.
    assert many.calculate_greeks_many(assets, asset_prices, 500.0, 0.0412) == expected


@pytest.mark.parametrize("bad", [None, 0.0, float("nan"), 1e-05])
def test_unusual_prices_take_the_one_by_one_path(bad):
    asset = Asset("SPY", asset_type="option", expiration=date(2026, 10, 23), strike=500, right="CALL")
    one, many = _source(), _source()
    try:
        expected = one.calculate_greeks(asset, bad, 500.0, 0.0412)
    except Exception as exc:  # the one-by-one path's own error is kept
        with pytest.raises(type(exc)):
            many.calculate_greeks_many([asset], [bad], 500.0, 0.0412)
        return
    assert many.calculate_greeks_many([asset], [bad], 500.0, 0.0412) == [expected]
