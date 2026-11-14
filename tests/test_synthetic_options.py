"""The synthetic option model: listing calendar, prices, greeks, bid/ask and labels.

Everything is computed from made-up underlying bars; there is no market data in these tests.
"""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from lumibot.tools import synthetic_options as model

NY = "America/New_York"
EXPIRY = date(2025, 1, 17)


def _bars(day="2025-01-13", level=590.0, minutes=390, seed=3):
    rng = np.random.default_rng(seed)
    index = pd.date_range(f"{day} 09:30", periods=minutes, freq="min", tz=NY)
    opens = level + np.cumsum(rng.normal(0, 0.08, minutes))
    return pd.DataFrame({"open": opens, "high": opens + 0.1, "low": opens - 0.1, "close": opens + 0.02}, index=index)


def _daily(level=590.0, days=260, seed=5, end="2025-01-10"):
    rng = np.random.default_rng(seed)
    index = pd.bdate_range(end=end, periods=days)
    closes = np.exp(np.cumsum(rng.normal(0, 0.01, days)))
    return pd.Series(level * closes / closes[-1], index=index.date)


def _surface(symbol="SPY", **kwargs):
    return model.build_surface(_bars(**kwargs), _daily(), symbol=symbol, mode="realized")


def _chain(surface, symbol="SPY"):
    strikes = np.array(model.strikes_for(symbol, 590.0, 4))
    both = np.concatenate([strikes, strikes])
    is_call = np.concatenate([np.ones(len(strikes), bool), np.zeros(len(strikes), bool)])
    return strikes, model.price_contracts(surface, both, is_call, EXPIRY)


def test_expiration_calendar_rules():
    spy = model.expirations_for("SPY", date(2025, 1, 13))
    assert spy[:5] == [date(2025, 1, d) for d in (13, 14, 15, 16, 17)]  # daily expirations
    assert date(2025, 1, 20) not in spy  # market holiday
    stock = model.expirations_for("ABCD", date(2025, 1, 13))
    assert stock[0] == date(2025, 1, 17) and date(2025, 1, 14) not in stock  # weeklies only
    assert date(2025, 2, 21) in stock and date(2025, 12, 19) in stock  # monthlies, third Friday
    # Good Friday 2025-04-18 is a holiday: that week's expiration moves to Thursday
    assert date(2025, 4, 17) in model.expirations_for("ABCD", date(2025, 4, 14))
    assert date(2025, 4, 18) not in model.expirations_for("ABCD", date(2025, 4, 14))
    assert model.expirations_for("ABCD", date(2025, 1, 13), max_date=date(2025, 1, 31))[-1] == date(2025, 1, 31)


def test_strike_grid_rules():
    assert model.strike_step("SPX", 5900) == 5.0 and model.strike_step("SPY", 590) == 1.0
    assert model.strike_step("ABCD", 20) == 0.5 and model.strike_step("ABCD", 150) == 2.5
    strikes = model.strikes_for("ABCD", 150.0, 30)
    assert strikes[0] <= 105.0 and strikes[-1] >= 195.0 and 150.0 in strikes
    assert len(model.strikes_for("ABCD", 150.0, 3)) < len(strikes)  # short-dated chains are narrower
    chain = model.build_chain("ABCD", date(2025, 1, 13), 150.0)
    assert chain["Chains"]["CALL"] == chain["Chains"]["PUT"] and chain["price_source"] == "synthetic"


def test_prices_respect_no_arbitrage_basics():
    surface = _surface()
    strikes, priced = _chain(surface)
    n = len(strikes)
    calls, puts = priced["open"][:, :n], priced["open"][:, n:]
    assert (priced["open"] >= 0).all() and np.isfinite(priced["open"]).all()
    # monotonic in strike
    assert (np.diff(calls, axis=1) <= 1e-9).all() and (np.diff(puts, axis=1) >= -1e-9).all()
    # call - put = discounted (forward - strike)
    years = model.years_to_expiry(surface.index, EXPIRY)[:, None]
    forward = surface.open[:, None] * np.exp((surface.rate - surface.dividend_yield) * years)
    parity = np.exp(-surface.rate * years) * (forward - strikes[None, :])
    assert np.allclose(calls - puts, parity, atol=1e-8)
    # never below intrinsic value, and bid < ask on a cent grid around the mid
    assert (calls >= np.maximum(parity, 0) - 1e-9).all()
    assert (priced["ask"] > priced["bid"]).all() and (priced["bid"] >= 0).all()
    assert (priced["bid"] <= priced["open"] + 1e-9).all() and (priced["ask"] >= priced["open"] - 1e-9).all()
    assert np.allclose(priced["bid"] * 100, np.round(priced["bid"] * 100))
    # greeks have the right signs
    assert (priced["delta"][:, :n] >= 0).all() and (priced["delta"][:, n:] <= 0).all()
    assert (priced["gamma"] >= 0).all() and (priced["vega"] >= 0).all() and (priced["theta"] <= 0).all()


def test_prices_converge_to_intrinsic_at_expiry():
    surface = model.build_surface(_bars(day="2025-01-17"), _daily(end="2025-01-16"), symbol="SPY", mode="realized")
    frame_call = model.contract_frame(surface, 585.0, True, EXPIRY)
    frame_put = model.contract_frame(surface, 595.0, False, EXPIRY)
    last = surface.close[-1]
    assert frame_call["close"].iloc[-1] == pytest.approx(max(last - 585.0, 0.0), abs=0.05)
    assert frame_put["close"].iloc[-1] == pytest.approx(max(595.0 - last, 0.0), abs=0.05)
    # at the closing bell exactly: intrinsic value
    at_close = model.black_price(590.0, np.array([585.0, 595.0]), 0.0, 0.2, True)
    assert at_close.tolist() == [5.0, 0.0]


def test_same_inputs_give_the_same_prices():
    first = model.contract_frame(_surface(), 590.0, True, EXPIRY)
    second = model.contract_frame(_surface(), 590.0, True, EXPIRY)
    pd.testing.assert_frame_equal(first, second)
    assert (first["price_source_code"] == model.CODE_SYNTHETIC).all()
    assert first["volume"].isna().all() and first["open_interest"].isna().all()  # marked not available


def test_skew_term_structure_and_level():
    surface = _surface()
    years = 30 / 365
    vols = model.surface_volatility(0.20, 0.20, 590.0, np.array([540.0, 590.0, 640.0]), years, "index")
    assert vols[0] > vols[1] > vols[2]  # index put skew
    assert vols[1] == pytest.approx(0.20, abs=1e-6)
    stock = model.surface_volatility(0.20, 0.20, 590.0, np.array([540.0, 590.0]), years, "stock")
    assert stock[0] - stock[1] < vols[0] - vols[1]  # single stocks: flatter skew
    # stressed level above the long-run level: short maturities are higher than long ones
    term = model.surface_volatility(0.40, 0.20, 590.0, 590.0, np.array([7 / 365, 30 / 365, 1.0]), "index")
    assert term[0] > term[1] > term[2] > 0.20
    # the level only uses days before the session
    assert np.allclose(surface.previous_close, _daily().iloc[-1])


def test_volatility_input_order():
    closes = _daily().to_numpy()
    realized = model.volatility_input(closes, None, mode="auto")
    assert realized.source == "realized_volatility" and 0.10 < realized.level < 0.30
    index = model.volatility_input(closes, np.array([18.0, 25.0]), mode="auto")
    assert index.source == "volatility_index" and index.level == pytest.approx(0.25)
    assert model.volatility_input(closes, np.array([18.0, 25.0]), mode="realized").source == "realized_volatility"
    assert model.volatility_input(closes[:3], None) is None  # too little history: no price, not a guess


def test_spread_model_shape():
    def half(**kw):
        base = dict(mid=30.0, abs_delta=0.27, years=7 / 365, level=0.15, underlying_price=6000.0, kind="index")
        base.update(kw)
        return float(model.half_spread(**base))

    assert half() == pytest.approx(0.10)  # measured calm median
    assert half(level=0.30) == pytest.approx(0.125)  # measured volatile median
    assert half(abs_delta=0.5) > half() > half(abs_delta=0.05)
    assert half(abs_delta=0.9) > 4 * half()
    assert half(underlying_price=600.0, mid=3.0) < half() / 5
    assert half(years=30 / 365) > half()
    assert half(dislocation=3.0) == pytest.approx(0.30)
    assert half(mid=0.02, abs_delta=0.01) == pytest.approx(0.008)  # capped at 40% of the mid
    bar_years = 60 / (365 * 86400)
    assert model.dislocation_multiplier(590.1, 589.9, 590.0, 0.16, bar_years) == 1.0
    assert model.dislocation_multiplier(593.0, 587.0, 590.0, 0.16, bar_years) == 4.0


def test_chain_day_is_fast():
    import time

    surface = _surface()
    strikes, _ = _chain(surface)
    started = time.perf_counter()
    for _ in range(3):
        _chain(surface)
    per_expiration_day = (time.perf_counter() - started) / 3
    assert len(strikes) * 2 > 300
    assert per_expiration_day < 0.3, f"{per_expiration_day * 1000:.0f} ms for one expiration-day"


def _real(index, rows):
    frame = pd.DataFrame(np.nan, index=index, columns=["open", "high", "low", "close", "volume", "bid", "ask"])
    for position, values in rows.items():
        for key, value in values.items():
            frame.iloc[position, frame.columns.get_loc(key)] = value
    return frame


def test_real_bars_override_synthetic_bars_and_keep_their_own_label():
    surface = _surface()
    base = model.contract_frame(surface, 590.0, True, EXPIRY)
    traded = float(base["open"].iloc[10]) + 0.40  # the market trades this contract richer than the model
    real = _real(base.index, {
        10: dict(open=traded, high=traded, low=traded, close=traded, volume=7),
        50: dict(bid=1.11, ask=1.19),
    })
    mixed = model.anchor_to_real_trades(surface, base, 590.0, True, EXPIRY, real)
    codes = mixed["price_source_code"]
    assert codes.iloc[10] == model.CODE_TRADE and codes.iloc[50] == model.CODE_QUOTE
    assert (codes.drop(codes.index[[10, 50]]) == model.CODE_SYNTHETIC).all()
    assert mixed["open"].iloc[10] == traded and np.isnan(mixed["bid"].iloc[10])  # a real trade has no quote
    assert (mixed["bid"].iloc[50], mixed["ask"].iloc[50]) == (1.11, 1.19)
    # before the trade nothing changes (no look-ahead); after it the synthetic bars follow the trade
    pd.testing.assert_series_equal(mixed["open"].iloc[:10], base["open"].iloc[:10])
    assert mixed["open"].iloc[11] - base["open"].iloc[11] == pytest.approx(0.40, abs=0.05)
    assert model.CODE_LABELS[model.CODE_TRADE] == "trade"


def test_synthetic_spread_on_real_trades_is_opt_in(monkeypatch):
    surface = _surface()
    base = model.contract_frame(surface, 590.0, True, EXPIRY)
    traded = float(base["open"].iloc[10])
    real = _real(base.index, {10: dict(open=traded, high=traded, low=traded, close=traded, volume=7)})
    monkeypatch.delenv(model.SPREAD_ON_REAL_TRADES_ENV, raising=False)
    assert model.env_flag(model.SPREAD_ON_REAL_TRADES_ENV) is False  # default off
    filled = model.anchor_to_real_trades(surface, base, 590.0, True, EXPIRY, real, spread_on_real_trades=True)
    row = filled.iloc[10]
    assert row["price_source_code"] == model.CODE_TRADE_SYNTHETIC_SPREAD
    assert row["bid"] < traded < row["ask"]
    assert model.CODE_LABELS[row["price_source_code"]] == "trade_synthetic_spread"


def test_cache_file_names_say_synthetic_and_carry_the_model_version():
    path = model.cache_relative_path("SPY", "minute", date(2025, 1, 13))
    version = model.MODEL_VERSION
    assert path == f"synthetic_options/{version}/SPY/minute/SPY_20250113_minute_synthetic_{version}_default.parquet"
    assert model.parameters_for("SPY").identity != model.parameters_for("ABCD").identity


def test_parameter_file_replaces_defaults_per_symbol(tmp_path, monkeypatch):
    import json

    assert model.parameters_for("ABCD").version == "default"
    path = tmp_path / "parameters.json"
    path.write_text(json.dumps({
        "version": "example-1",
        "default": {"risk_premium": 1.2},
        "kinds": {"index": {"skew_slope": -0.2}},
        "symbols": {"ABCD": {"spot_vol_beta": 1.0, "term_multiplier": [[0, 1.5], [30, 1.0]], "spread_scale_calm": 2.0}},
    }))
    monkeypatch.setenv(model.PARAMS_ENV, str(path))
    abcd, spy = model.parameters_for("ABCD"), model.parameters_for("SPY")
    assert (abcd.version, abcd.risk_premium, abcd.spot_vol_beta, abcd.skew_slope) == ("example-1", 1.2, 1.0, -0.06)
    assert (spy.risk_premium, spy.skew_slope, spy.spot_vol_beta) == (1.2, -0.2, 6.0)
    # the term table lifts short-dated volatility; the spread scale doubles the calm base
    assert model.surface_volatility(0.2, 0.2, 100.0, 100.0, 1 / 365, abcd) > model.surface_volatility(0.2, 0.2, 100.0, 100.0, 1 / 365, "stock")
    wide = model.half_spread(3.0, 0.27, 7 / 365, 0.15, 6000.0, abcd)
    assert float(wide) == pytest.approx(2 * float(model.half_spread(3.0, 0.27, 7 / 365, 0.15, 6000.0, "stock")))
    monkeypatch.setenv(model.PARAMS_ENV, str(tmp_path / "missing.json"))
    assert model.parameters_for("ABCD").version == "default"  # an unreadable file changes nothing


# ----------------------------------------------------------------------------------------
# Multi-leg structures: the package price must be arbitrage-consistent
# ----------------------------------------------------------------------------------------
NEAR, FAR = date(2025, 1, 17), date(2025, 2, 21)


def _quotes(strikes, expiration=NEAR, level=590.0, stressed=False):
    """{(strike, 'call'|'put'): (bid, mid, ask)} at the first bar, plus the discount factor."""
    daily = _daily(level=level)
    if stressed:  # a violent last month: realized volatility far above its long-run level
        tail = np.exp(np.cumsum(np.random.default_rng(11).normal(0, 0.035, 20)))
        daily.iloc[-20:] = daily.iloc[-21] * tail
        daily = daily * level / daily.iloc[-1]
    surface = model.build_surface(_bars(level=level), daily, symbol="SPY", mode="realized")
    strikes = np.asarray(strikes, dtype=float)
    both = np.concatenate([strikes, strikes])
    is_call = np.concatenate([np.ones(len(strikes), bool), np.zeros(len(strikes), bool)])
    priced = model.price_contracts(surface, both, is_call, expiration)
    out = {}
    for i, (strike, call) in enumerate(zip(both, is_call)):
        out[(float(strike), "call" if call else "put")] = (priced["bid"][0, i], priced["open"][0, i], priced["ask"][0, i])
    years = model.years_to_expiry(surface.index[:1], expiration)[0]
    return out, float(np.exp(-surface.rate * years)), float(surface.open[0] * np.exp(surface.rate * years))


def _package(quotes, legs):
    """(bid, mid, ask) of a package: legs are (quantity, strike, right); buy at ask, sell at bid."""
    mid = sum(q * quotes[(k, r)][1] for q, k, r in legs)
    ask = sum(q * quotes[(k, r)][2 if q > 0 else 0] for q, k, r in legs)
    bid = sum(q * quotes[(k, r)][0 if q > 0 else 2] for q, k, r in legs)
    assert bid <= mid <= ask
    return bid, mid, ask


@pytest.mark.parametrize("stressed", [False, True])
def test_vertical_spreads_and_box(stressed):
    quotes, discount, _forward = _quotes([580, 585, 590, 595, 600], stressed=stressed)
    for low, high in ((580.0, 585.0), (585.0, 595.0), (590.0, 600.0)):
        width = (high - low) * discount
        call_debit = _package(quotes, [(1, low, "call"), (-1, high, "call")])[1]
        put_debit = _package(quotes, [(1, high, "put"), (-1, low, "put")])[1]
        assert 0 < call_debit < width and 0 < put_debit < width
        # credit spreads are the same packages sold
        assert _package(quotes, [(-1, low, "call"), (1, high, "call")])[1] == pytest.approx(-call_debit)
        # a call debit spread plus a put debit spread on the same strikes is a box: worth the width
        assert call_debit + put_debit == pytest.approx(width, abs=1e-8)


@pytest.mark.parametrize("stressed", [False, True])
def test_iron_condor_and_butterfly(stressed):
    quotes, discount, _forward = _quotes([570, 575, 585, 590, 595, 605, 610], stressed=stressed)
    condor = [(1, 570.0, "put"), (-1, 575.0, "put"), (-1, 605.0, "call"), (1, 610.0, "call")]
    bid, mid, ask = _package(quotes, condor)
    credit = -mid
    assert 0 < credit < 5.0 * discount  # never more than the wing width
    assert -bid > credit > -ask  # selling it at the bid/ask collects less than the mid credit
    for right in ("call", "put"):
        fly = _package(quotes, [(1, 585.0, right), (-2, 590.0, right), (1, 595.0, right)])[1]
        assert 0 <= fly <= 5.0 * discount  # convex in strike, worth at most the wing width
    iron_fly = -_package(quotes, [(1, 585.0, "put"), (-1, 590.0, "put"), (-1, 590.0, "call"), (1, 595.0, "call")])[1]
    assert 0 < iron_fly < 5.0 * discount


@pytest.mark.parametrize("stressed", [False, True])
def test_straddle_and_strangle(stressed):
    quotes, discount, forward = _quotes([580, 590, 600], stressed=stressed)
    straddle = _package(quotes, [(1, 590.0, "call"), (1, 590.0, "put")])[1]
    strangle = _package(quotes, [(1, 600.0, "call"), (1, 580.0, "put")])[1]
    assert straddle > strangle > 0
    assert straddle >= abs(forward - 590.0) * discount  # at least its intrinsic value
    assert straddle - strangle < 10.0 * discount  # the gap is bounded by the strike distance


@pytest.mark.parametrize("stressed", [False, True])
def test_calendar_spread_is_worth_something(stressed):
    near, _d, _f = _quotes([580, 590, 600], NEAR, stressed=stressed)
    far, _d, _f = _quotes([580, 590, 600], FAR, stressed=stressed)
    for strike in (580.0, 590.0, 600.0):
        # the later expiration is worth more: total variance grows with time on this surface
        assert far[(strike, "call")][1] > near[(strike, "call")][1]
    assert far[(590.0, "put")][1] > near[(590.0, "put")][1]
    if stressed:
        # stressed markets: short-dated volatility is above long-dated volatility (term structure matters)
        near_vol = model.surface_volatility(0.45, 0.20, 590.0, 590.0, 4 / 365, "index")
        far_vol = model.surface_volatility(0.45, 0.20, 590.0, 590.0, 39 / 365, "index")
        assert near_vol > far_vol


@pytest.mark.parametrize("stressed", [False, True])
def test_ratio_spread_bounds(stressed):
    quotes, discount, _forward = _quotes([580, 590], stressed=stressed)
    # sell 1 deeper in the money, buy 2 at the money (calls); mirror image with puts
    calls = _package(quotes, [(-1, 580.0, "call"), (2, 590.0, "call")])[1]
    assert -10.0 * discount <= calls <= quotes[(590.0, "call")][1]
    quotes_put, discount, _forward = _quotes([590, 600], stressed=stressed)
    puts = _package(quotes_put, [(-1, 600.0, "put"), (2, 590.0, "put")])[1]
    assert -10.0 * discount <= puts <= quotes_put[(590.0, "put")][1]
