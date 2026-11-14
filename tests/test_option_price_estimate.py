"""Tests for lumibot.tools.option_price_estimate (pure functions, synthetic data only)."""

import time

import numpy as np
import pytest

from lumibot.tools.option_price_estimate import (
    SOURCE_NEIGHBOR,
    SOURCE_NONE,
    SOURCE_OWN_TRADE,
    SOURCE_SMILE,
    SOURCE_SMILE_PARITY,
    black_scholes_forward_price,
    estimate_option_prices,
    implied_volatility_forward,
    parity_forward_factor,
)

RATE = 0.04
DIVIDEND = 0.01
YEARS = 20 / 365


def smile(strike, forward):
    """A skewed, curved volatility smile used to manufacture consistent test prices."""
    k = np.log(strike / forward)
    return 0.18 - 0.9 * k + 6.0 * k * k


def make_chain(spot=500.0, years=YEARS, lo=440, hi=561, step=5):
    strikes = np.arange(lo, hi, step, dtype=float)
    strike = np.concatenate([strikes, strikes])
    is_call = np.concatenate([np.ones(len(strikes), bool), np.zeros(len(strikes), bool)])
    forward = spot * np.exp((RATE - DIVIDEND) * years)
    true = black_scholes_forward_price(forward, strike, years, smile(strike, forward), is_call, np.exp(-RATE * years))
    return strike, is_call, forward, true


def known(strike, spot=500.0, years=YEARS, age=0.0):
    """Trade context where every contract traded `age` minutes ago at `spot`."""
    return dict(
        trade_underlying_price=np.full(len(strike), spot),
        trade_years_to_expiry=np.full(len(strike), years),
        trade_age_minutes=np.full(len(strike), age),
    )


def test_price_and_implied_volatility_round_trip():
    strike, is_call, forward, price = make_chain()
    vol = implied_volatility_forward(price, forward, strike, YEARS, is_call, np.exp(-RATE * YEARS))
    assert np.allclose(vol, smile(strike, forward), atol=1e-6)


def test_call_put_parity_of_pricer():
    strike, is_call, forward, price = make_chain()
    half = len(strike) // 2
    assert np.allclose(price[:half] - price[half:], np.exp(-RATE * YEARS) * (forward - strike[:half]), atol=1e-9)


def test_implied_volatility_is_nan_below_intrinsic():
    assert np.isnan(implied_volatility_forward([4.0], 110.0, 100.0, 0.1, [True])[0])


def test_untraded_strikes_are_filled_from_the_smile():
    """Every other strike never traded; the rest traded just now. Estimates must be close to truth."""
    strike, is_call, _, truth = make_chain()
    traded = (np.arange(len(strike)) % 2) == 0
    result = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_price=np.where(traded, truth, np.nan), rate=RATE, dividend_yield=DIVIDEND, **known(strike),
    )
    assert np.all(np.isfinite(result.price))
    assert set(result.source) == {SOURCE_SMILE}
    # the outermost strikes are extrapolated flat (see the wings test); judge the interior
    inside = (strike > strike.min()) & (strike < strike.max())
    assert np.max(np.abs(result.price[inside] - truth[inside])) < 0.05
    fill = inside & ~traded & (truth > 0.25)
    assert np.max(np.abs(result.price[fill] - truth[fill]) / truth[fill]) < 0.05


def test_stale_trades_are_repriced_at_the_new_underlying_price():
    """Trades happened 30 minutes ago at spot 500; spot is now 503. A stale price would be badly off."""
    strike, is_call, old_forward, old = make_chain(spot=500.0)
    years_now = YEARS - 30 / (365 * 24 * 60)
    forward_now = 503.0 * np.exp((RATE - DIVIDEND) * years_now)
    # sticky-strike truth: same vol per strike, new underlying price
    truth = black_scholes_forward_price(
        forward_now, strike, years_now, smile(strike, old_forward), is_call, np.exp(-RATE * years_now)
    )
    result = estimate_option_prices(
        strike, is_call, 503.0, years_now,
        trade_price=old, rate=RATE, dividend_yield=DIVIDEND, **known(strike, age=30.0),
    )
    model_error = np.abs(result.price - truth)
    stale_error = np.abs(old - truth)
    assert np.max(model_error) < 0.05
    assert np.median(stale_error) > 10 * np.median(model_error)


def test_smoothing_reduces_bid_ask_bounce_noise():
    """Trades print half a spread above or below fair value; the smile averages that noise out."""
    strike, is_call, _, truth = make_chain(step=1)
    rng = np.random.default_rng(7)
    noisy = truth + rng.choice([-0.05, 0.05], size=len(truth))
    result = estimate_option_prices(
        strike, is_call, 500.0, YEARS, trade_price=noisy, rate=RATE, dividend_yield=DIVIDEND, **known(strike),
    )
    core = (strike > 470) & (strike < 530)
    assert np.mean(np.abs(result.price[core] - truth[core])) < 0.6 * np.mean(np.abs(noisy[core] - truth[core]))


def test_recent_trades_outweigh_old_ones():
    """Old nodes carry a wrong (lower) vol level; fresh nodes carry the right one."""
    strike, is_call, forward, truth = make_chain()
    stale_price = black_scholes_forward_price(
        forward, strike, YEARS, smile(strike, forward) - 0.05, is_call, np.exp(-RATE * YEARS)
    )
    fresh = (np.arange(len(strike)) % 2) == 0
    context = known(strike)
    context["trade_age_minutes"] = np.where(fresh, 1.0, 120.0)
    result = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_price=np.where(fresh, truth, stale_price), rate=RATE, dividend_yield=DIVIDEND, **context,
    )
    inside = (strike > strike.min()) & (strike < strike.max())
    assert np.max(np.abs(result.price[inside] - truth[inside])) < 0.05 * np.max(np.abs(stale_price - truth))


def test_wings_are_flat_extrapolated_and_never_negative():
    strike, is_call, forward, truth = make_chain()
    traded = (strike >= 480) & (strike <= 520)
    result = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_price=np.where(traded, truth, np.nan), rate=RATE, dividend_yield=DIVIDEND, **known(strike),
    )
    assert np.all(result.price >= 0)
    low_put = (~is_call) & (strike == strike.min())
    edge_put = (~is_call) & (strike == 480)
    assert result.volatility[low_put][0] == pytest.approx(result.volatility[edge_put][0], abs=0.02)
    traded_vols = smile(strike[traded], forward)
    assert result.volatility.max() <= traded_vols.max() + 1e-9
    assert result.volatility.min() >= traded_vols.min() - 1e-9


def test_fallbacks_neighbor_own_trade_and_none():
    strike = np.array([95.0, 100.0, 105.0, 100.0])
    is_call = np.array([True, True, True, False])
    years = 10 / 365
    fair = black_scholes_forward_price(100.0, strike, years, 0.25, is_call)
    # one fresh call trade only -> the other calls take its vol; the put last traded days ago
    result = estimate_option_prices(
        strike, is_call, 100.0, years,
        trade_price=np.array([np.nan, fair[1], np.nan, fair[3]]),
        trade_underlying_price=np.full(4, 100.0),
        trade_years_to_expiry=np.full(4, years),
        trade_age_minutes=np.array([np.nan, 2.0, np.nan, 3000.0]),
    )
    assert list(result.source) == [SOURCE_NEIGHBOR, SOURCE_NEIGHBOR, SOURCE_NEIGHBOR, SOURCE_OWN_TRADE]
    assert np.allclose(result.price, fair, atol=1e-6)

    nothing = estimate_option_prices(
        strike, is_call, 100.0, years,
        trade_price=np.full(4, np.nan),
        trade_underlying_price=np.full(4, np.nan),
        trade_years_to_expiry=np.full(4, np.nan),
        trade_age_minutes=np.full(4, np.nan),
    )
    assert np.all(np.isnan(nothing.price))
    assert set(nothing.source) == {SOURCE_NONE}


def test_wrong_carry_assumption_cancels_within_a_right():
    """Prices were made with 4% rate / 1% dividend; the estimator is told 0% / 0%.

    Near the money the error cancels. Deep in-the-money European puts can trade below their
    undiscounted intrinsic value, which a 0% rate cannot explain, so pass a real rate when you
    have one.
    """
    strike, is_call, _, truth = make_chain()
    result = estimate_option_prices(strike, is_call, 500.0, YEARS, trade_price=truth, **known(strike))
    near = np.abs(strike - 500) <= 15
    assert np.max(np.abs(result.price[near] - truth[near])) < 0.05


def test_parity_forward_factor_and_in_the_money_pricing():
    strike, is_call, forward, truth = make_chain()
    half = len(strike) // 2
    factor = parity_forward_factor(truth[:half], truth[half:], strike[:half], 500.0, YEARS, rate=RATE)
    assert factor == pytest.approx(forward / 500.0, rel=1e-9)
    assert parity_forward_factor([1.0], [1.0], [100.0], 100.0, 0.1) is None

    # only out-of-the-money contracts traded; in-the-money ones must come from the other side
    out_of_the_money = np.where(is_call, strike >= forward, strike <= forward)
    result = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_price=np.where(out_of_the_money, truth, np.nan), rate=RATE, forward_factor=factor, **known(strike),
    )
    deep = (~out_of_the_money) & (np.abs(strike - 500) >= 25)
    assert set(result.source[deep]) == {SOURCE_SMILE_PARITY}
    assert np.max(np.abs(result.price[deep] - truth[deep])) < 0.05


def test_precomputed_trade_volatility_gives_the_same_answer():
    strike, is_call, forward, truth = make_chain()
    traded = (np.arange(len(strike)) % 3) != 0
    ages = np.linspace(0, 40, len(strike))
    context = known(strike)
    context["trade_age_minutes"] = ages
    from_prices = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_price=np.where(traded, truth, np.nan), rate=RATE, dividend_yield=DIVIDEND, **context,
    )
    solved = implied_volatility_forward(
        np.where(traded, truth, np.nan), forward, strike, YEARS, is_call, np.exp(-RATE * YEARS)
    )
    from_vols = estimate_option_prices(
        strike, is_call, 500.0, YEARS,
        trade_age_minutes=ages, trade_volatility=solved, rate=RATE, dividend_yield=DIVIDEND,
    )
    assert np.allclose(from_prices.price, from_vols.price, atol=1e-9)
    assert list(from_prices.source) == list(from_vols.source)
    with pytest.raises(ValueError):
        estimate_option_prices(strike, is_call, 500.0, YEARS, trade_age_minutes=ages)


def test_empty_and_expired_inputs():
    empty = estimate_option_prices(
        [], [], 100.0, 0.1, trade_price=[], trade_underlying_price=[], trade_years_to_expiry=[], trade_age_minutes=[]
    )
    assert empty.price.shape == (0,)
    expired = estimate_option_prices(
        [100.0], [True], 100.0, 0.0,
        trade_price=[1.0], trade_underlying_price=[100.0], trade_years_to_expiry=[0.01], trade_age_minutes=[1.0],
    )
    assert np.isnan(expired.price[0]) and expired.source[0] == SOURCE_NONE


def test_is_vectorized_and_fast_enough_for_a_full_chain():
    strike, is_call, _, truth = make_chain(lo=300, hi=701, step=1)  # 802 contracts
    context = known(strike)
    context["trade_age_minutes"] = np.linspace(0, 60, len(strike))
    kwargs = dict(trade_price=truth, rate=RATE, dividend_yield=DIVIDEND, **context)
    estimate_option_prices(strike, is_call, 500.0, YEARS, **kwargs)
    started = time.perf_counter()
    for _ in range(5):
        estimate_option_prices(strike, is_call, 500.0, YEARS, **kwargs)
    per_call = (time.perf_counter() - started) / 5
    assert per_call < 0.25  # generous bound; measured about 10 ms on a laptop
