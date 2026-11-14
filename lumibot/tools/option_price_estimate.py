"""Estimate option prices for a moment when a contract has no real trade or quote.

This module is pure numpy: no data source, broker or strategy objects. Give it what is known
about one expiration at one instant (each contract's most recent real trade, the underlying
price then and now) and it returns a model price for every contract, with a label saying how
each price was obtained. Every number it returns is an ESTIMATE and must be labelled as such
by the caller (for example ``price_source="estimated"``).

Method (one expiration, one instant):

1. Back out an implied volatility from each contract's most recent real trade, using the
   underlying price and time to expiry at that trade.
2. Fit a smooth volatility smile across strikes from the contracts that traded recently
   ("nodes"). Recent trades count more (exponential time decay), contracts whose price is
   insensitive to volatility count less (vega weight). Calls and puts are fitted separately so
   that an imperfect interest/dividend assumption cancels out. The fit is a local-linear kernel
   regression in standardized log-moneyness; outside the traded strikes the smile is held flat.
   A node keeps its volatility at its strike ("sticky strike").
3. Price every contract with Black-Scholes at the current underlying price, the current time
   to expiry and its smile volatility.
4. Optional, European exercise only: when a forward factor implied by put-call parity is
   supplied, in-the-money contracts are priced from the out-of-the-money side of the smile
   (the liquid side) instead of their own, thinly traded, side.

Fallbacks, in order: smile of recent trades -> smile of all of today's trades of that right ->
nearest traded strike of that right -> the other right's smile -> the contract's own older
trade -> no estimate (NaN).

All inputs are arrays over the contracts of ONE expiration. Times are in years (ACT/365).
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
from scipy.special import ndtr

SOURCE_SMILE = "smile"
SOURCE_SMILE_PARITY = "smile_parity"
SOURCE_SMILE_OTHER_SIDE = "smile_other_side"
SOURCE_NEIGHBOR = "neighbor"
SOURCE_OWN_TRADE = "own_trade"
SOURCE_NONE = "none"

_SQRT_2PI = float(np.sqrt(2.0 * np.pi))


@dataclass(frozen=True)
class OptionPriceEstimates:
    """Result of :func:`estimate_option_prices`. Arrays are aligned with the input contracts."""

    price: np.ndarray  # estimated price per share; NaN where no estimate is possible
    volatility: np.ndarray  # volatility used for the price (decimal, 0.20 = 20%)
    source: np.ndarray  # one of the SOURCE_* labels per contract
    support: np.ndarray  # effective number of nodes behind a smile value (kernel weight sum)


def black_scholes_forward_price(forward, strike, years, volatility, is_call, discount=1.0):
    """Black (1976) price on a forward. Vectorized. Discounted intrinsic when vol*sqrt(T) is 0."""
    forward, strike, years, volatility, is_call, discount = np.broadcast_arrays(
        np.asarray(forward, dtype=float),
        np.asarray(strike, dtype=float),
        np.asarray(years, dtype=float),
        np.asarray(volatility, dtype=float),
        np.asarray(is_call, dtype=bool),
        np.asarray(discount, dtype=float),
    )
    sign = np.where(is_call, 1.0, -1.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        total = volatility * np.sqrt(years)
        d1 = np.log(forward / strike) / total + 0.5 * total
        d2 = d1 - total
        price = discount * sign * (forward * ndtr(sign * d1) - strike * ndtr(sign * d2))
    intrinsic = discount * np.maximum(sign * (forward - strike), 0.0)
    usable = (total > 0) & np.isfinite(price)
    out = np.where(usable, price, intrinsic)
    return np.where(np.isfinite(volatility) & np.isfinite(forward) & (years >= 0), out, np.nan)


def implied_volatility_forward(price, forward, strike, years, is_call, discount=1.0, low=1e-4, high=8.0, steps=48):
    """Implied volatility by vectorized bisection. NaN where the price has no solution in [low, high]."""
    price, forward, strike, years, is_call, discount = [
        np.array(a)
        for a in np.broadcast_arrays(
            np.asarray(price, dtype=float),
            np.asarray(forward, dtype=float),
            np.asarray(strike, dtype=float),
            np.asarray(years, dtype=float),
            np.asarray(is_call, dtype=bool),
            np.asarray(discount, dtype=float),
        )
    ]
    lo = np.full(price.shape, float(low))
    hi = np.full(price.shape, float(high))
    with np.errstate(all="ignore"):
        at_low = black_scholes_forward_price(forward, strike, years, lo, is_call, discount)
        at_high = black_scholes_forward_price(forward, strike, years, hi, is_call, discount)
        solvable = (price > at_low + 1e-9) & (price < at_high) & (years > 0) & (price > 0) & np.isfinite(forward)
    for _ in range(int(steps)):
        mid = 0.5 * (lo + hi)
        with np.errstate(all="ignore"):
            below = black_scholes_forward_price(forward, strike, years, mid, is_call, discount) < price
        lo = np.where(below, mid, lo)
        hi = np.where(below, hi, mid)
    return np.where(solvable, 0.5 * (lo + hi), np.nan)


def parity_forward_factor(call_price, put_price, strike, underlying_price, years, rate=0.0, min_pairs=4):
    """Forward/spot ratio implied by put-call parity from call and put prices at the same strikes.

    Use pairs observed at (nearly) the same moment and near the money, for European-style
    options. Returns the median over pairs, or None when fewer than ``min_pairs`` are usable.
    """
    call_price, put_price, strike, underlying_price, years = np.broadcast_arrays(
        *[np.asarray(a, dtype=float) for a in (call_price, put_price, strike, underlying_price, years)]
    )
    with np.errstate(all="ignore"):
        factor = (strike + np.exp(rate * years) * (call_price - put_price)) / underlying_price
    factor = factor[np.isfinite(factor) & (factor > 0.9) & (factor < 1.1)]
    if factor.size < int(min_pairs):
        return None
    return float(np.median(factor))


def _local_linear(x_nodes, y_nodes, w_nodes, x_targets, bandwidth):
    """Kernel-weighted local-linear fit of y on x, flat outside the node range. Returns (y, weight)."""
    clipped = np.clip(x_targets, x_nodes.min(), x_nodes.max())
    dist = clipped[:, None] - x_nodes[None, :]
    kern = np.exp(-0.5 * (dist / bandwidth) ** 2) * w_nodes[None, :]
    s0 = kern.sum(axis=1)
    s1 = (kern * dist).sum(axis=1)
    s2 = (kern * dist * dist).sum(axis=1)
    t0 = (kern * y_nodes[None, :]).sum(axis=1)
    t1 = (kern * dist * y_nodes[None, :]).sum(axis=1)
    det = s0 * s2 - s1 * s1
    with np.errstate(divide="ignore", invalid="ignore"):
        linear = (s2 * t0 - s1 * t1) / det
        constant = t0 / s0
    use_linear = (det > 1e-12 * np.maximum(s0 * s2, 1e-300)) & np.isfinite(linear)
    fitted = np.where(use_linear, linear, constant)
    fitted = np.clip(fitted, y_nodes.min(), y_nodes.max())  # never leave the range of observed vols
    return np.where(s0 > 1e-3, fitted, np.nan), s0


def estimate_option_prices(
    strike,
    is_call,
    underlying_price: float,
    years_to_expiry: float,
    *,
    trade_age_minutes,
    trade_price=None,
    trade_underlying_price=None,
    trade_years_to_expiry=None,
    trade_volatility=None,
    rate: float = 0.0,
    dividend_yield: float = 0.0,
    forward_factor: Optional[float] = None,
    decay_minutes: float = 10.0,
    bandwidth: float = 0.25,
    min_nodes: int = 3,
    max_node_age_minutes: float = 390.0,
    in_the_money_cutoff: float = 0.5,
    in_the_money_node_weight: float = 0.25,
) -> OptionPriceEstimates:
    """Estimate the price of every contract of one expiration at one instant.

    Parameters
    ----------
    strike, is_call : arrays, one entry per contract (same expiration).
    underlying_price : underlying price now.
    years_to_expiry : time to expiry now, in years (> 0).
    trade_price : each contract's most recent REAL trade price known now; NaN if none.
    trade_underlying_price : underlying price at that trade.
    trade_years_to_expiry : time to expiry at that trade, in years.
    trade_age_minutes : minutes between that trade and now.
    trade_volatility : optional implied volatility of each contract's most recent real trade
        (decimal), if the caller already solved it (for example once per trade bar with
        :func:`implied_volatility_forward`). When given, the three ``trade_*`` price inputs are
        not needed and no volatility is solved here, which is several times faster when the
        function is called for every minute of a day.
    rate, dividend_yield : continuously compounded carry assumption (decimals).
    forward_factor : optional forward/spot ratio from put-call parity
        (:func:`parity_forward_factor`). Replaces the rate/dividend carry and switches on
        pricing of in-the-money contracts from the out-of-the-money side. European-style only.
    decay_minutes : time decay of a node's weight (weight halves about every 0.7 * decay).
    bandwidth : kernel width in standardized moneyness (standard deviations to expiry).
    min_nodes : nodes needed (per fit) before a smile is used.
    max_node_age_minutes : older trades are not smile nodes (still usable as own-trade fallback).
    """
    strike = np.asarray(strike, dtype=float)
    is_call = np.asarray(is_call, dtype=bool)
    n = strike.shape[0]
    if trade_volatility is None and (
        trade_price is None or trade_underlying_price is None or trade_years_to_expiry is None
    ):
        raise ValueError(
            "Give either trade_volatility, or trade_price with trade_underlying_price and trade_years_to_expiry."
        )
    if trade_volatility is None:
        trade_price = np.asarray(trade_price, dtype=float)
        trade_underlying_price = np.asarray(trade_underlying_price, dtype=float)
        trade_years = np.asarray(trade_years_to_expiry, dtype=float)
    age = np.asarray(trade_age_minutes, dtype=float)
    years_now = float(years_to_expiry)
    spot_now = float(underlying_price)

    price = np.full(n, np.nan)
    vol = np.full(n, np.nan)
    source = np.full(n, SOURCE_NONE, dtype=object)
    support = np.zeros(n)
    if n == 0 or not np.isfinite(spot_now) or spot_now <= 0 or not np.isfinite(years_now) or years_now <= 0:
        return OptionPriceEstimates(price, vol, source, support)

    def carry(years):
        years = np.asarray(years, dtype=float)
        if forward_factor is not None:
            return float(forward_factor) * np.ones_like(years)
        return np.exp((rate - dividend_yield) * years)

    forward_now = spot_now * float(carry(years_now))
    discount_now = float(np.exp(-rate * years_now))
    if trade_volatility is not None:
        own_vol = np.array(trade_volatility, dtype=float)
        own_vol[~(own_vol > 0)] = np.nan
    else:
        safe_years = np.where(np.isfinite(trade_years) & (trade_years > 0), trade_years, np.nan)
        filled_years = np.nan_to_num(safe_years, nan=years_now)
        own_vol = implied_volatility_forward(
            trade_price,
            trade_underlying_price * carry(filled_years),
            strike,
            safe_years,
            is_call,
            np.exp(-rate * filled_years),
        )
    has_vol = np.isfinite(own_vol)
    node = has_vol & np.isfinite(age) & (age >= 0) & (age <= max_node_age_minutes)

    sign = np.where(is_call, 1.0, -1.0)
    log_moneyness = np.log(strike / forward_now)

    live = node.copy()
    recent_weight = np.zeros(n)  # recent trades count more
    today_weight = np.zeros(n)  # every trade of the session counts the same
    if node.any():
        with np.errstate(all="ignore"):
            total = np.where(node, own_vol, np.nan) * np.sqrt(years_now)
            d1 = -log_moneyness / total + 0.5 * total
            vega = np.nan_to_num(np.where(node, np.exp(-0.5 * d1 * d1) / _SQRT_2PI, 0.0), nan=0.0)
        today_weight = np.where(node, np.clip(vega / vega.max(), 0.02, 1.0) if vega.max() > 0 else 1.0, 0.0)
        recent_weight = np.nan_to_num(today_weight * np.exp(-np.where(node, age, 0.0) / float(decay_minutes)), nan=0.0)

    reference_vol = float("nan")
    if live.any():
        ranking = recent_weight if recent_weight.max() > 0 else today_weight
        near = np.argsort(np.where(ranking > 0.05 * ranking.max(), np.abs(log_moneyness), np.inf))[:5]
        reference_vol = float(np.nanmedian(own_vol[near]))

    if np.isfinite(reference_vol) and reference_vol > 0:
        x = log_moneyness / (reference_vol * np.sqrt(years_now))

        def fit(nodes, targets, node_weight, label):
            """Fill the still-empty targets from the given nodes (writes vol, support, source)."""
            nodes = nodes & (node_weight > 0)
            todo = targets & ~np.isfinite(vol)
            if not todo.any() or int(nodes.sum()) < int(min_nodes):
                return
            fitted, s0 = _local_linear(x[nodes], own_vol[nodes], node_weight[nodes], x[todo], float(bandwidth))
            ok = np.isfinite(fitted)
            index = np.nonzero(todo)[0][ok]
            vol[index] = fitted[ok]
            support[index] = s0[ok]
            source[index] = label

        everyone = np.ones(n, dtype=bool)
        liquid_side = np.where(sign * log_moneyness >= 0, 1.0, float(in_the_money_node_weight))
        if forward_factor is not None:
            # with a parity forward, in-the-money contracts take the vol of the liquid (OTM) side
            deep = -sign * x > float(in_the_money_cutoff)
            fit(live, deep, recent_weight * liquid_side, SOURCE_SMILE_PARITY)
        for right in (True, False):
            members = is_call == right
            nodes = members & live
            # 1) this right's own smile, recent trades first
            fit(nodes, members, recent_weight, SOURCE_SMILE)
            # 2) nothing recent near this strike: all of today's trades of this right
            fit(nodes, members, today_weight, SOURCE_SMILE)
            # 3) too few traded strikes for a fit, or a wide gap between them: straight line
            #    between the nearest traded strikes of this right, flat outside them
            todo = members & ~np.isfinite(vol)
            if todo.any() and nodes.any():
                order = np.argsort(x[nodes])
                vol[todo] = np.interp(x[todo], x[nodes][order], own_vol[nodes][order])
                support[todo] = float(nodes.sum())
                source[todo] = SOURCE_NEIGHBOR
        # 4) this right has not traded today: borrow the other side's smile
        other = SOURCE_SMILE_PARITY if forward_factor is not None else SOURCE_SMILE_OTHER_SIDE
        fit(live, everyone, recent_weight * liquid_side, other)
        fit(live, everyone, today_weight * liquid_side, other)

    # 5) the contract's own (older) trade
    fallback = ~np.isfinite(vol) & has_vol
    vol[fallback] = own_vol[fallback]
    source[fallback] = SOURCE_OWN_TRADE

    priced = np.isfinite(vol)
    price[priced] = black_scholes_forward_price(
        forward_now, strike[priced], years_now, vol[priced], is_call[priced], discount_now
    )
    bad = ~np.isfinite(price) | (price < 0)
    price[bad] = np.nan
    vol[bad] = np.nan
    source[bad] = SOURCE_NONE
    support[bad] = 0.0
    return OptionPriceEstimates(price, vol, source, support)
