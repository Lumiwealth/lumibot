"""Synthetic option data: option chains, prices, bid/ask and greeks computed from the underlying.

Everything here is a MODEL. No number produced by this module is a market observation, and
every caller must label it ``price_source="synthetic"``.

The module is pure numpy/pandas: no data source, broker or strategy objects.

Pieces (each one documented where it is defined):

* ``expirations_for`` / ``strikes_for`` / ``build_chain``: a rule-based listing calendar.
* ``realized_volatility`` / ``VolatilityInput``: the volatility level for each bar.
* ``surface_volatility``: term structure and skew on top of that level.
* ``price_contracts``: Black (1976) prices and greeks, vectorized over bars and contracts.
* ``half_spread``: the synthetic bid/ask model.

Model changes must bump ``MODEL_VERSION``: cached files carry it in their name.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Iterable, Optional, Sequence

import numpy as np
import pandas as pd
from scipy.special import ndtr

MODEL_VERSION = "v1"
PRICE_SOURCE_SYNTHETIC = "synthetic"
NOTICE = (
    "Option prices in this backtest are synthetic: computed from the underlying price and "
    "volatility. They are estimates."
)

# Row labels stored in the numeric ``price_source_code`` column of an option frame.
CODE_QUOTE = 0.0  # real bid/ask from another route
CODE_TRADE = 1.0  # real trade from another route, no bid/ask
CODE_TRADE_SYNTHETIC_SPREAD = 2.0  # real trade, synthetic bid/ask around it (gap filler)
CODE_SYNTHETIC = 3.0  # fully synthetic
CODE_LABELS = {
    CODE_QUOTE: "quote",
    CODE_TRADE: "trade",
    CODE_TRADE_SYNTHETIC_SPREAD: "trade_synthetic_spread",
    CODE_SYNTHETIC: PRICE_SOURCE_SYNTHETIC,
}

RATE_ENV = "LUMIBOT_SYNTHETIC_OPTION_RATE"
DIVIDEND_ENV = "LUMIBOT_SYNTHETIC_OPTION_DIVIDEND_YIELD"
VOL_INPUT_ENV = "LUMIBOT_SYNTHETIC_OPTION_VOL_INPUT"  # auto | index | realized
RISK_PREMIUM_ENV = "LUMIBOT_SYNTHETIC_OPTION_VOL_RISK_PREMIUM"
SPREAD_ON_REAL_TRADES_ENV = "LUMIBOT_SYNTHETIC_OPTION_SPREAD_ON_REAL_TRADES"  # default off
PARAMS_ENV = "LUMIBOT_SYNTHETIC_OPTION_PARAMS"  # path of an optional JSON parameter file

DEFAULT_RATE = 0.04
DEFAULT_DIVIDEND_YIELD = 0.0
DEFAULT_RISK_PREMIUM = 1.15  # implied volatility is usually above realized volatility

_NY = "America/New_York"
_SQRT_2PI = float(np.sqrt(2.0 * np.pi))
_YEAR_SECONDS = 365.0 * 86400.0


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def env_flag(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


# ----------------------------------------------------------------------------------------
# Underlying classes (small tables, meant to be extended)
# ----------------------------------------------------------------------------------------
# Underlyings that list an expiration every trading day.
DAILY_EXPIRATION_SYMBOLS = frozenset({"SPX", "SPXW", "XSP", "NDX", "NDXP", "RUT", "RUTW", "SPY", "QQQ", "IWM"})
# Underlyings priced as a broad index (index skew, tight relative spreads).
INDEX_LIKE_SYMBOLS = frozenset(
    {"SPX", "SPXW", "XSP", "NDX", "NDXP", "RUT", "RUTW", "DJX", "OEX", "VIX", "SPY", "QQQ", "IWM", "DIA"}
)
# Symbols that are cash indexes (their bars come from the index route).
INDEX_SYMBOLS = frozenset({"SPX", "SPXW", "XSP", "NDX", "NDXP", "RUT", "RUTW", "DJX", "OEX", "VIX"})
# Bars of a weekly/PM-settled root are the bars of its index.
UNDERLYING_ROOT = {"SPXW": "SPX", "NDXP": "NDX", "RUTW": "RUT"}
# Volatility index of an underlying, when one exists.
VOLATILITY_INDEX = {
    "SPX": "VIX", "SPXW": "VIX", "XSP": "VIX", "SPY": "VIX",
    "NDX": "VXN", "NDXP": "VXN", "QQQ": "VXN",
    "RUT": "RVX", "RUTW": "RVX", "IWM": "RVX",
    "DJX": "VXD", "DIA": "VXD",
}
# Fixed strike step for a symbol. Others use STRIKE_STEP_BY_PRICE.
STRIKE_STEP_BY_SYMBOL = {
    "SPX": 5.0, "SPXW": 5.0, "XSP": 1.0, "NDX": 25.0, "NDXP": 25.0, "RUT": 5.0, "RUTW": 5.0,
    "SPY": 1.0, "QQQ": 1.0, "IWM": 1.0, "DIA": 1.0,
}
# (underlying price below, strike step)
STRIKE_STEP_BY_PRICE = ((5.0, 0.5), (25.0, 0.5), (100.0, 1.0), (250.0, 2.5), (1000.0, 5.0), (3000.0, 10.0), (np.inf, 25.0))
# Strikes are listed this far from the underlying price: (days to expiry up to, fraction).
STRIKE_RANGE_BY_DAYS = ((7, 0.15), (45, 0.30), (10_000, 0.50))
# Expiration calendar.
DAILY_EXPIRATION_DAYS = 35  # every trading day this far out (daily-expiration underlyings)
WEEKLY_EXPIRATION_WEEKS = 8  # every Friday this far out
MONTHLY_EXPIRATION_MONTHS = 12  # third Friday of each month this far out
# Skew: volatility(z) = at-the-money volatility * (1 + slope*z + curve*z^2), z = standardized
# log-moneyness ln(K/F) / (vol * sqrt(T)), clipped to +-SKEW_MAX_Z, factor floored at SKEW_FLOOR.
# Slope and curve are in ModelParameters (index-like: -0.12, 0.03; others: -0.06, 0.03).
SKEW_MAX_Z = 4.0
SKEW_FLOOR = 0.75
# Term structure: variance reverts from the 30-day level to the long-run level at this speed.
TERM_MEAN_REVERSION = 4.0  # per year
TERM_REFERENCE_YEARS = 30.0 / 365.0
# Intraday: volatility rises when the underlying falls since the previous close
# (ModelParameters.spot_vol_beta: index-like 6, others 3).
SPOT_VOL_MAX_SHIFT = 0.9  # |change of ln(vol)| is capped here
VOL_FLOOR, VOL_CAP = 0.03, 5.0
# Spread model (see half_spread).
SPREAD_REFERENCE_PRICE = 6000.0  # the measured medians are for options on an index near 6000
SPREAD_HALF_CALM_CENTS = 10.0
SPREAD_HALF_VOLATILE_CENTS = 12.5
SPREAD_VOLATILE_LEVEL = 0.22  # 30-day volatility at or above this is "volatile"
SPREAD_BY_ABS_DELTA = ((0.0, 0.25), (0.10, 0.5), (0.27, 1.0), (0.50, 1.5), (0.80, 5.0), (1.0, 9.5))
SPREAD_REFERENCE_DAYS = 7.0
SPREAD_MAX_FRACTION_OF_MID = 0.40
SPREAD_DISLOCATION_START = 2.0  # bar range this many times the expected range starts widening
SPREAD_DISLOCATION_MAX = 4.0
TICK = 0.01


def skew_class(symbol: str, asset_type: str = "") -> str:
    token = str(symbol or "").upper()
    return "index" if token in INDEX_LIKE_SYMBOLS or str(asset_type).lower().endswith("index") else "stock"


def underlying_symbol(symbol: str) -> str:
    token = str(symbol or "").upper()
    return UNDERLYING_ROOT.get(token, token)


# ----------------------------------------------------------------------------------------
# Model parameters: neutral defaults, optionally replaced per underlying by a parameter file
# ----------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ModelParameters:
    """Shape parameters of the model for one underlying.

    The defaults are neutral, generic values. A parameter file (JSON, path in the
    ``LUMIBOT_SYNTHETIC_OPTION_PARAMS`` environment variable) can replace any of them for all
    underlyings (``"default"``), for a class (``"kinds": {"index": ..., "stock": ...}``) or for
    one symbol (``"symbols": {"XYZ": ...}``). The file holds parameters only, never prices.
    """

    kind: str = "stock"
    skew_slope: float = -0.06
    skew_curve: float = 0.03
    risk_premium: float = DEFAULT_RISK_PREMIUM  # implied / realized volatility (input c)
    volatility_index_scale: float = 1.0  # 30-day level / volatility index (input b)
    spot_vol_beta: float = 3.0
    # (days to expiry, multiplier on at-the-money volatility), linear in between, flat outside
    term_multiplier: tuple = ((0.0, 1.0), (45.0, 1.0))
    spread_scale_calm: float = 1.0
    spread_scale_volatile: float = 1.0
    spread_min_fraction: float = 0.015
    version: str = "default"

    @property
    def identity(self) -> str:
        """Short fingerprint, part of cache file names so different parameters never mix."""
        import hashlib

        text = repr((self.kind, self.skew_slope, self.skew_curve, self.risk_premium, self.volatility_index_scale,
                     self.spot_vol_beta, self.term_multiplier, self.spread_scale_calm, self.spread_scale_volatile,
                     self.spread_min_fraction))
        return hashlib.sha1(text.encode()).hexdigest()[:8]


_KIND_DEFAULTS = {
    "index": dict(kind="index", skew_slope=-0.12, skew_curve=0.03, spot_vol_beta=6.0, spread_min_fraction=0.0025),
    "stock": dict(kind="stock"),
}
_PARAMETER_FILE_CACHE: dict = {}


def _parameter_file() -> dict:
    path = os.environ.get(PARAMS_ENV)
    if not path:
        return {}
    try:
        stamp = (path, os.path.getmtime(path))
    except OSError:
        return {}
    if _PARAMETER_FILE_CACHE.get("stamp") != stamp:
        import json

        try:
            with open(path, encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (OSError, ValueError):
            loaded = {}
        _PARAMETER_FILE_CACHE.update(stamp=stamp, data=loaded if isinstance(loaded, dict) else {})
    return _PARAMETER_FILE_CACHE["data"]


def parameters_for(symbol: str = "", asset_type: str = "", kind: Optional[str] = None) -> ModelParameters:
    """Parameters for an underlying: built-in defaults, then the parameter file (default, kind, symbol)."""
    kind = kind or skew_class(symbol, asset_type)
    values = dict(_KIND_DEFAULTS[kind])
    risk_premium = os.environ.get(RISK_PREMIUM_ENV)
    if risk_premium not in (None, ""):
        values["risk_premium"] = _env_float(RISK_PREMIUM_ENV, DEFAULT_RISK_PREMIUM)
    loaded = _parameter_file()
    layers = [loaded.get("default"), (loaded.get("kinds") or {}).get(kind),
              (loaded.get("symbols") or {}).get(str(symbol or "").upper())]
    allowed = set(ModelParameters.__dataclass_fields__) - {"kind", "version"}
    for layer in layers:
        if isinstance(layer, dict):
            for key, value in layer.items():
                if key in allowed:
                    values[key] = tuple(tuple(float(x) for x in row) for row in value) if key == "term_multiplier" else float(value)
    if loaded:
        values["version"] = str(loaded.get("version") or "custom")
    return ModelParameters(**values)


def _resolve(params) -> ModelParameters:
    return params if isinstance(params, ModelParameters) else parameters_for(kind=str(params or "stock"))


# ----------------------------------------------------------------------------------------
# Listing calendar
# ----------------------------------------------------------------------------------------
@lru_cache(maxsize=8)
def _trading_days(start_year: int, end_year: int) -> tuple:
    import pandas_market_calendars as mcal

    days = mcal.get_calendar("NYSE").valid_days(f"{start_year}-01-01", f"{end_year}-12-31")
    return tuple(d.date() for d in days)


def trading_days_between(start: date, end: date) -> list:
    days = _trading_days(start.year, end.year)
    return [d for d in days if start <= d <= end]


def _on_or_before_trading_day(day: date, open_days: set) -> date:
    while day not in open_days:
        day -= timedelta(days=1)
    return day


def expirations_for(symbol: str, as_of: date, max_date: Optional[date] = None) -> list:
    """Expirations listed for ``symbol`` on ``as_of``: dailies (some underlyings), weeklies, monthlies.

    A Friday that is a holiday moves to the trading day before it.
    """
    horizon = as_of + timedelta(days=31 * MONTHLY_EXPIRATION_MONTHS + 7)
    days = trading_days_between(as_of, horizon)
    open_days = set(days)
    out = set()
    if str(symbol or "").upper() in DAILY_EXPIRATION_SYMBOLS:
        out.update(d for d in days if (d - as_of).days <= DAILY_EXPIRATION_DAYS)
    friday = as_of + timedelta(days=(4 - as_of.weekday()) % 7)
    for week in range(WEEKLY_EXPIRATION_WEEKS):
        out.add(_on_or_before_trading_day(friday + timedelta(days=7 * week), open_days | {as_of}))
    year, month = as_of.year, as_of.month
    for _ in range(MONTHLY_EXPIRATION_MONTHS + 1):
        first = date(year, month, 1)
        third_friday = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
        if as_of <= third_friday <= horizon:
            out.add(_on_or_before_trading_day(third_friday, open_days | {as_of}))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return sorted(d for d in out if d >= as_of and (max_date is None or d <= max_date))


def strike_step(symbol: str, price: float) -> float:
    token = str(symbol or "").upper()
    if token in STRIKE_STEP_BY_SYMBOL:
        return STRIKE_STEP_BY_SYMBOL[token]
    for below, step in STRIKE_STEP_BY_PRICE:
        if price < below:
            return step
    return STRIKE_STEP_BY_PRICE[-1][1]


def strikes_for(symbol: str, reference_price: float, days_to_expiry: int) -> list:
    """Listed strikes around ``reference_price`` (use the previous close, so it is known in advance)."""
    if not np.isfinite(reference_price) or reference_price <= 0:
        return []
    step = strike_step(symbol, float(reference_price))
    width = next(fraction for days, fraction in STRIKE_RANGE_BY_DAYS if days_to_expiry <= days)
    low = max(step, np.floor(reference_price * (1.0 - width) / step) * step)
    high = np.ceil(reference_price * (1.0 + width) / step) * step
    return [round(float(k), 4) for k in np.arange(low, high + step / 2.0, step)]


def build_chain(symbol: str, as_of: date, reference_price: float, max_date: Optional[date] = None) -> dict:
    """Chain in the standard LumiBot shape: {"Multiplier", "Exchange", "Chains": {"CALL": {date: strikes}, "PUT": ...}}."""
    calls = {}
    for expiration in expirations_for(symbol, as_of, max_date):
        calls[expiration.strftime("%Y-%m-%d")] = strikes_for(symbol, reference_price, (expiration - as_of).days)
    return {
        "Multiplier": 100,
        "Exchange": "SYNTHETIC",
        "Chains": {"CALL": calls, "PUT": {key: list(value) for key, value in calls.items()}},
        "price_source": PRICE_SOURCE_SYNTHETIC,
    }


# ----------------------------------------------------------------------------------------
# Volatility level
# ----------------------------------------------------------------------------------------
def realized_volatility(daily_close: Sequence[float], decay: float = 0.94, long_window: int = 252) -> tuple:
    """(recent, long_run) annualized close-to-close volatility from daily closes, oldest first.

    ``recent`` is an exponentially weighted volatility (decay 0.94 a day). ``long_run`` is the
    plain volatility of up to ``long_window`` days. Both are NaN with fewer than 5 returns.
    """
    close = np.asarray(daily_close, dtype=float)
    close = close[np.isfinite(close) & (close > 0)]
    if close.size < 6:
        return float("nan"), float("nan")
    returns = np.diff(np.log(close))[-int(long_window):]
    weights = decay ** np.arange(returns.size - 1, -1, -1)
    recent = float(np.sqrt(252.0 * np.sum(weights * returns * returns) / np.sum(weights)))
    long_run = float(np.sqrt(252.0 * np.mean(returns * returns)))
    return recent, long_run


@dataclass(frozen=True)
class VolatilityInput:
    """The 30-day volatility level known before a session, and where it came from."""

    level: float  # 30-day at-the-money volatility at the previous close (decimal)
    long_run: float  # long-run level the term structure reverts to
    source: str  # "volatility_index" or "realized_volatility"


def volatility_input(
    daily_close: Sequence[float],
    volatility_index_close: Optional[Sequence[float]] = None,
    mode: Optional[str] = None,
    risk_premium: Optional[float] = None,
    volatility_index_scale: float = 1.0,
) -> Optional[VolatilityInput]:
    """Volatility level for the next session from data up to and including the previous close.

    Order: a volatility index series when one is given (previous close / 100; long-run level is
    its mean over the series), otherwise realized volatility of the underlying's own daily
    closes times a risk premium (long-run: 70% long window, 30% recent).
    """
    mode = (mode or os.environ.get(VOL_INPUT_ENV) or "auto").strip().lower()
    premium = float(risk_premium) if risk_premium is not None else _env_float(RISK_PREMIUM_ENV, DEFAULT_RISK_PREMIUM)
    if mode in {"auto", "index"} and volatility_index_close is not None:
        series = np.asarray(volatility_index_close, dtype=float)
        series = series[np.isfinite(series) & (series > 0)]
        if series.size:
            scale = float(volatility_index_scale) / 100.0
            return VolatilityInput(float(series[-1]) * scale, float(np.mean(series[-252:])) * scale, "volatility_index")
    if mode == "index":
        return None
    recent, long_run = realized_volatility(daily_close)
    if not np.isfinite(recent):
        return None
    level = float(np.clip(np.sqrt(0.7 * recent**2 + 0.3 * long_run**2) * premium, VOL_FLOOR, VOL_CAP))
    return VolatilityInput(level, float(np.clip(long_run * premium, VOL_FLOOR, VOL_CAP)), "realized_volatility")


def intraday_level(level, price, previous_close, kind="index"):
    """Shift the previous-close volatility level for the move of the underlying since then."""
    with np.errstate(all="ignore"):
        move = np.log(np.asarray(price, dtype=float) / np.asarray(previous_close, dtype=float))
    shift = np.clip(-_resolve(kind).spot_vol_beta * np.nan_to_num(move), -SPOT_VOL_MAX_SHIFT, SPOT_VOL_MAX_SHIFT)
    return np.clip(np.asarray(level, dtype=float) * np.exp(shift), VOL_FLOOR, VOL_CAP)


def surface_volatility(level, long_run, forward, strike, years, kind="index"):
    """Volatility for each (bar, contract): term structure, then skew. All inputs broadcast."""
    params = _resolve(kind)
    level, long_run, forward, strike, years = np.broadcast_arrays(
        *[np.asarray(a, dtype=float) for a in (level, long_run, forward, strike, years)]
    )
    safe_years = np.maximum(years, 1.0 / (365.0 * 24.0 * 60.0))

    def weight(t):
        x = TERM_MEAN_REVERSION * t
        return (1.0 - np.exp(-x)) / x

    variance = long_run**2 + (level**2 - long_run**2) * weight(safe_years) / weight(TERM_REFERENCE_YEARS)
    variance = np.maximum(variance, 0.25 * np.minimum(level, long_run) ** 2)  # short-dated floor
    atm = np.sqrt(np.clip(variance, VOL_FLOOR**2, VOL_CAP**2))
    table = np.asarray(params.term_multiplier, dtype=float)
    atm = atm * np.interp(safe_years * 365.0, table[:, 0], table[:, 1])
    slope, curve = params.skew_slope, params.skew_curve
    with np.errstate(all="ignore"):
        z = np.clip(np.log(strike / forward) / (atm * np.sqrt(safe_years)), -SKEW_MAX_Z, SKEW_MAX_Z)
    factor = np.maximum(1.0 + slope * z + curve * z * z, SKEW_FLOOR)
    return np.clip(atm * factor, VOL_FLOOR, VOL_CAP)


# ----------------------------------------------------------------------------------------
# Prices and greeks
# ----------------------------------------------------------------------------------------
def years_to_expiry(when, expiration: date):
    """Years (ACT/365) from each timestamp to 16:00 New York on the expiration date; <= 0 after it."""
    index = pd.DatetimeIndex(when)
    index = index.tz_localize(_NY) if index.tz is None else index.tz_convert(_NY)
    close = pd.Timestamp(datetime(expiration.year, expiration.month, expiration.day, 16, 0)).tz_localize(_NY)
    return (close - index).total_seconds().to_numpy(dtype=float) / _YEAR_SECONDS


def black_price(forward, strike, years, volatility, is_call, discount=1.0):
    """Black (1976) price. Discounted intrinsic value at or after expiry."""
    forward, strike, years, volatility, is_call, discount = np.broadcast_arrays(
        *[np.asarray(a, dtype=float) for a in (forward, strike, years, volatility)],
        np.asarray(is_call, dtype=bool),
        np.asarray(discount, dtype=float),
    )
    sign = np.where(is_call, 1.0, -1.0)
    with np.errstate(all="ignore"):
        total = volatility * np.sqrt(np.maximum(years, 0.0))
        d1 = np.log(forward / strike) / total + 0.5 * total
        price = discount * sign * (forward * ndtr(sign * d1) - strike * ndtr(sign * (d1 - total)))
    intrinsic = discount * np.maximum(sign * (forward - strike), 0.0)
    return np.where((total > 0) & np.isfinite(price), np.maximum(price, intrinsic), intrinsic)


def black_greeks(spot, forward, strike, years, volatility, is_call, discount=1.0) -> dict:
    """delta, gamma, theta (per calendar day), vega (per 1 vol point), all per share."""
    spot, forward, strike, years, volatility, is_call, discount = np.broadcast_arrays(
        *[np.asarray(a, dtype=float) for a in (spot, forward, strike, years, volatility)],
        np.asarray(is_call, dtype=bool),
        np.asarray(discount, dtype=float),
    )
    sign = np.where(is_call, 1.0, -1.0)
    live = years > 0
    with np.errstate(all="ignore"):
        root = np.sqrt(np.maximum(years, 1e-12))
        total = volatility * root
        d1 = np.log(forward / strike) / total + 0.5 * total
        density = np.exp(-0.5 * d1 * d1) / _SQRT_2PI
        carry = forward / spot  # = exp((r - q) T)
        delta = sign * discount * carry * ndtr(sign * d1)
        gamma = discount * carry * density / (spot * total)
        vega = discount * forward * density * root / 100.0
        theta = -discount * forward * density * volatility / (2.0 * root) / 365.0
    expired_delta = np.where(sign * (forward - strike) > 0, sign, 0.0)
    return {
        "delta": np.where(live, np.nan_to_num(delta), expired_delta),
        "gamma": np.where(live, np.nan_to_num(gamma), 0.0),
        "vega": np.where(live, np.nan_to_num(vega), 0.0),
        "theta": np.where(live, np.nan_to_num(theta), 0.0),
    }


def half_spread(mid, abs_delta, years, level, underlying_price, kind="index", dislocation=1.0):
    """Synthetic half-spread in dollars per share.

    Starts from the measured median half-spread of index options (10.0 cents on calm days, 12.5
    cents on volatile days, for an index near 6000) and scales it:

    * price level: linearly with the underlying price (an underlying at 600 gets a tenth);
    * moneyness: by absolute delta (far out of the money a quarter, at the money 1.5x, deep in
      the money up to 9.5x), from the same measurement;
    * time: square root of days to expiry over 7, between 0.5x and 2.5x;
    * dislocation: up to 4x when the underlying's bar range is far above what its volatility
      implies (``dislocation`` is that multiplier, computed by ``dislocation_multiplier``).

    Then it is kept between a minimum share of the mid price (0.25% index-like, 1.5% others)
    and 40% of the mid, and never below half a cent. A parameter file can scale the calm and
    volatile base per underlying.
    """
    params = _resolve(kind)
    mid, abs_delta, years, level, underlying_price, dislocation = np.broadcast_arrays(
        *[np.asarray(a, dtype=float) for a in (mid, abs_delta, years, level, underlying_price, dislocation)]
    )
    base = np.where(
        level >= SPREAD_VOLATILE_LEVEL,
        SPREAD_HALF_VOLATILE_CENTS * params.spread_scale_volatile,
        SPREAD_HALF_CALM_CENTS * params.spread_scale_calm,
    ) / 100.0
    points = np.array(SPREAD_BY_ABS_DELTA)
    moneyness = np.interp(np.clip(abs_delta, 0.0, 1.0), points[:, 0], points[:, 1])
    days = np.maximum(years, 0.0) * 365.0
    time_factor = np.clip(np.sqrt(days / SPREAD_REFERENCE_DAYS), 0.5, 2.5)
    half = base * (underlying_price / SPREAD_REFERENCE_PRICE) * moneyness * time_factor * dislocation
    half = np.maximum(half, params.spread_min_fraction * mid * np.minimum(dislocation, 2.0))
    half = np.minimum(half, SPREAD_MAX_FRACTION_OF_MID * mid)
    return np.maximum(half, TICK / 2.0)


def dislocation_multiplier(high, low, reference, level, bar_years):
    """Spread multiplier from the size of the underlying's bar range against its volatility."""
    with np.errstate(all="ignore"):
        actual = (np.asarray(high, dtype=float) - np.asarray(low, dtype=float)) / np.asarray(reference, dtype=float)
        expected = 1.6 * np.asarray(level, dtype=float) * np.sqrt(np.asarray(bar_years, dtype=float))
        ratio = np.nan_to_num(actual / expected, nan=1.0, posinf=SPREAD_DISLOCATION_MAX * SPREAD_DISLOCATION_START)
    return np.clip(ratio / SPREAD_DISLOCATION_START, 1.0, SPREAD_DISLOCATION_MAX)


def quote_from_mid(mid, half):
    """(bid, ask) on a one-cent grid around ``mid``. The bid can be 0 for worthless contracts."""
    mid = np.asarray(mid, dtype=float)
    bid = np.maximum(np.floor((mid - half) / TICK + 1e-9) * TICK, 0.0)
    ask = np.maximum(np.ceil((mid + half) / TICK - 1e-9) * TICK, bid + TICK)
    return np.round(bid, 2), np.round(ask, 2)


@dataclass(frozen=True)
class SurfaceDay:
    """Per-bar model inputs for one underlying. This, not prices, is what gets cached."""

    index: pd.DatetimeIndex
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    previous_close: np.ndarray  # last daily close known before each bar's session
    level: np.ndarray  # 30-day volatility at the previous close
    long_run: np.ndarray
    bar_years: float
    kind: ModelParameters
    volatility_source: str
    rate: float
    dividend_yield: float

    COLUMNS = ("open", "high", "low", "close", "previous_close", "level", "long_run")

    def to_frame(self) -> pd.DataFrame:
        frame = pd.DataFrame({name: getattr(self, name) for name in self.COLUMNS}, index=self.index)
        frame.attrs.update(
            price_source=PRICE_SOURCE_SYNTHETIC, model_version=MODEL_VERSION, kind=self.kind.kind,
            parameters=self.kind.version,
            volatility_source=self.volatility_source, rate=self.rate, dividend_yield=self.dividend_yield,
            bar_years=self.bar_years,
        )
        return frame


def price_contracts(surface: SurfaceDay, strikes, is_call, expiration: date) -> dict:
    """Price contracts of one expiration on every bar of ``surface``.

    Returns arrays shaped (bars, contracts): open/high/low/close mids, bid, ask (quoted at the
    bar's open), volatility, delta, gamma, theta, vega. Bars after expiry hold intrinsic value.
    """
    strikes = np.atleast_1d(np.asarray(strikes, dtype=float))[None, :]
    is_call = np.broadcast_to(np.atleast_1d(np.asarray(is_call, dtype=bool))[None, :], strikes.shape)
    years = years_to_expiry(surface.index, expiration)[:, None]
    live_years = np.maximum(years, 0.0)
    carry = np.exp((surface.rate - surface.dividend_yield) * live_years)
    discount = np.exp(-surface.rate * live_years)
    previous = surface.previous_close[:, None]
    level_open = intraday_level(surface.level[:, None], surface.open[:, None], previous, surface.kind)
    long_run = surface.long_run[:, None]

    def value(price, level):
        forward = price * carry
        vol = surface_volatility(level, long_run, forward, strikes, live_years, surface.kind)
        return black_price(forward, strikes, live_years, vol, is_call, discount), vol, forward

    mid_open, vol_open, forward_open = value(surface.open[:, None], level_open)
    out = {"open": mid_open, "volatility": vol_open}
    extremes = []
    for name in ("high", "low", "close"):
        price = getattr(surface, name)[:, None]
        extremes.append(value(price, intraday_level(surface.level[:, None], price, previous, surface.kind))[0])
    out["close"] = extremes[2]
    stacked = np.stack([mid_open, extremes[0], extremes[1], extremes[2]])
    out["high"], out["low"] = stacked.max(axis=0), stacked.min(axis=0)
    greeks = black_greeks(surface.open[:, None], forward_open, strikes, years, vol_open, is_call, discount)
    out.update(greeks)
    widen = dislocation_multiplier(surface.high, surface.low, surface.open, level_open[:, 0], surface.bar_years)[:, None]
    half = half_spread(mid_open, np.abs(greeks["delta"]), live_years, level_open, surface.open[:, None], surface.kind, widen)
    out["half_spread"] = half
    out["bid"], out["ask"] = quote_from_mid(mid_open, half)
    return out


def contract_frame(surface: SurfaceDay, strike: float, is_call: bool, expiration: date) -> pd.DataFrame:
    """One contract's bars: open/high/low/close, bid, ask, greeks; volume and open interest NaN."""
    priced = price_contracts(surface, [strike], [is_call], expiration)
    frame = pd.DataFrame({key: value[:, 0] for key, value in priced.items()}, index=surface.index)
    frame["volume"] = np.nan  # not available: nothing traded
    frame["open_interest"] = np.nan
    for name in ("bid_size", "ask_size", "bid_condition", "ask_condition", "bid_exchange", "ask_exchange"):
        frame[name] = np.nan  # not available
    frame["price_source_code"] = CODE_SYNTHETIC
    return frame


def implied_volatility(price, forward, strike, years, is_call, discount=1.0, steps=40):
    """Implied volatility by bisection in [VOL_FLOOR, VOL_CAP]; NaN when there is no solution."""
    price, forward, strike, years = [np.asarray(a, dtype=float) for a in np.broadcast_arrays(price, forward, strike, years)]
    low, high = np.full(price.shape, VOL_FLOOR), np.full(price.shape, VOL_CAP)
    ok = (price > black_price(forward, strike, years, low, is_call, discount)) & (
        price < black_price(forward, strike, years, high, is_call, discount)
    ) & (years > 0)
    for _ in range(steps):
        mid = 0.5 * (low + high)
        below = black_price(forward, strike, years, mid, is_call, discount) < price
        low, high = np.where(below, mid, low), np.where(below, high, mid)
    return np.where(ok, 0.5 * (low + high), np.nan)


def build_surface(
    bars: pd.DataFrame,
    daily_close: pd.Series,
    *,
    symbol: str,
    asset_type: str = "",
    volatility_index_close: Optional[pd.Series] = None,
    bar_minutes: float = 1.0,
    mode: Optional[str] = None,
    params: Optional[ModelParameters] = None,
) -> Optional[SurfaceDay]:
    """Model inputs for every bar of ``bars`` (the underlying's open/high/low/close).

    ``daily_close`` (and the optional volatility index closes) are daily series indexed by
    date. For each bar only days strictly before the bar's date are used.
    """
    if bars is None or bars.empty or daily_close is None or len(daily_close) == 0:
        return None
    params = params or parameters_for(symbol, asset_type)
    index = pd.DatetimeIndex(bars.index)
    index = index.tz_localize(_NY) if index.tz is None else index.tz_convert(_NY)
    bar_dates = np.array([d for d in index.date])
    daily = pd.Series(np.asarray(daily_close, dtype=float), index=[pd.Timestamp(d).date() for d in daily_close.index])
    daily = daily[~daily.index.duplicated(keep="last")].sort_index().dropna()
    daily_dates = np.array(list(daily.index))
    vol_index = None
    if volatility_index_close is not None and len(volatility_index_close):
        vol_index = pd.Series(
            np.asarray(volatility_index_close, dtype=float),
            index=[pd.Timestamp(d).date() for d in volatility_index_close.index],
        )
        vol_index = vol_index[~vol_index.index.duplicated(keep="last")].sort_index().dropna()
    n = len(index)
    previous_close, level, long_run = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    sources = set()
    for day in sorted(set(bar_dates)):
        known = int(np.searchsorted(daily_dates, day, side="left"))  # days strictly before
        if known == 0:
            continue
        index_closes = None
        if vol_index is not None:
            cut = int(np.searchsorted(np.array(list(vol_index.index)), day, side="left"))
            index_closes = vol_index.to_numpy()[:cut] if cut else None
        found = volatility_input(
            daily.to_numpy()[:known], index_closes, mode=mode, risk_premium=params.risk_premium,
            volatility_index_scale=params.volatility_index_scale,
        )
        if found is None:
            continue
        rows = bar_dates == day
        previous_close[rows] = float(daily.to_numpy()[known - 1])
        level[rows], long_run[rows] = found.level, found.long_run
        sources.add(found.source)
    keep = np.isfinite(level) & np.isfinite(pd.to_numeric(bars["open"], errors="coerce").to_numpy(dtype=float))
    if not keep.any():
        return None

    def column(name):
        return pd.to_numeric(bars[name], errors="coerce").to_numpy(dtype=float)[keep]

    opens = column("open")
    closes = np.where(np.isfinite(column("close")), column("close"), opens)
    return SurfaceDay(
        index=index[keep], open=opens,
        high=np.fmax(column("high"), opens), low=np.fmin(column("low"), opens), close=closes,
        previous_close=previous_close[keep], level=level[keep], long_run=long_run[keep],
        bar_years=float(bar_minutes) * 60.0 / _YEAR_SECONDS,
        kind=params,
        volatility_source="+".join(sorted(sources)) or "none",
        rate=_env_float(RATE_ENV, DEFAULT_RATE), dividend_yield=_env_float(DIVIDEND_ENV, DEFAULT_DIVIDEND_YIELD),
    )


def anchor_to_real_trades(surface: SurfaceDay, frame: pd.DataFrame, strike: float, is_call: bool,
                          expiration: date, real: pd.DataFrame, spread_on_real_trades: bool = False) -> pd.DataFrame:
    """Overlay real bars from another route on a synthetic contract frame.

    * A bar with a real bid and ask keeps them (label ``quote``).
    * A bar with a real trade keeps its real open/high/low/close (label ``trade``); it gets a
      synthetic bid/ask around the real open only when ``spread_on_real_trades`` is set.
    * Bars with neither stay synthetic, but their volatility is shifted by the difference
      between the last real trade's implied volatility and the model volatility at that trade
      (carried forward only, never backward).
    """
    if real is None or real.empty:
        return frame
    real = real.copy()
    real.index = pd.DatetimeIndex(real.index).tz_convert(_NY) if pd.DatetimeIndex(real.index).tz is not None else pd.DatetimeIndex(real.index).tz_localize(_NY)
    real = real[~real.index.duplicated(keep="last")].reindex(frame.index)

    def numeric(name):
        return pd.to_numeric(real[name], errors="coerce").to_numpy(dtype=float) if name in real.columns else np.full(len(frame), np.nan)

    r_open, r_close, r_bid, r_ask = numeric("open"), numeric("close"), numeric("bid"), numeric("ask")
    has_trade = np.isfinite(r_close) & (r_close > 0) & np.isfinite(r_open)
    has_quote = np.isfinite(r_bid) & np.isfinite(r_ask) & (r_ask > 0) & (r_ask >= r_bid)
    years = np.maximum(years_to_expiry(frame.index, expiration), 0.0)
    carry, discount = np.exp((surface.rate - surface.dividend_yield) * years), np.exp(-surface.rate * years)
    out = frame.copy()
    if has_trade.any():
        # implied volatility of each real trade (bar close against the underlying close)
        model_close_vol = surface_volatility(
            intraday_level(surface.level, surface.close, surface.previous_close, surface.kind),
            surface.long_run, surface.close * carry, strike, years, surface.kind,
        )
        trade_vol = implied_volatility(np.where(has_trade, r_close, np.nan), surface.close * carry, strike, years, is_call, discount)
        offset = pd.Series(np.where(has_trade, trade_vol - model_close_vol, np.nan)).shift(1).ffill().fillna(0.0).to_numpy()
        vol = np.clip(out["volatility"].to_numpy() + offset, VOL_FLOOR, VOL_CAP)
        forward = surface.open * carry
        mid = black_price(forward, strike, years, vol, is_call, discount)
        shift = mid - out["open"].to_numpy()
        for name in ("open", "high", "low", "close"):
            out[name] = np.maximum(out[name].to_numpy() + shift, 0.0)
        out["volatility"] = vol
        half = out["half_spread"].to_numpy()
        out["bid"], out["ask"] = quote_from_mid(out["open"].to_numpy(), half)
        for name, values in (("open", r_open), ("high", numeric("high")), ("low", numeric("low")), ("close", r_close)):
            out[name] = np.where(has_trade & np.isfinite(values), values, out[name].to_numpy())
        out["volume"] = np.where(has_trade, numeric("volume"), np.nan)
        if spread_on_real_trades:
            bid, ask = quote_from_mid(r_open, half)
            out["bid"] = np.where(has_trade, bid, out["bid"].to_numpy())
            out["ask"] = np.where(has_trade, ask, out["ask"].to_numpy())
            out["price_source_code"] = np.where(has_trade, CODE_TRADE_SYNTHETIC_SPREAD, out["price_source_code"].to_numpy())
        else:
            out["bid"] = np.where(has_trade, np.nan, out["bid"].to_numpy())
            out["ask"] = np.where(has_trade, np.nan, out["ask"].to_numpy())
            out["price_source_code"] = np.where(has_trade, CODE_TRADE, out["price_source_code"].to_numpy())
    if has_quote.any():
        out["bid"] = np.where(has_quote, r_bid, out["bid"].to_numpy())
        out["ask"] = np.where(has_quote, r_ask, out["ask"].to_numpy())
        out["price_source_code"] = np.where(has_quote, CODE_QUOTE, out["price_source_code"].to_numpy())
    return out


def cache_relative_path(symbol: str, timestep: str, day: date, parameters: str = "default") -> str:
    """Cache file of one underlying-day of model inputs. The name says it is synthetic.

    ``parameters`` is ``ModelParameters.identity``: files made with different parameters never mix.
    """
    token = "".join(ch for ch in str(symbol).upper() if ch.isalnum() or ch in "._-")
    name = f"{token}_{day:%Y%m%d}_{timestep}_synthetic_{MODEL_VERSION}_{parameters}.parquet"
    return f"synthetic_options/{MODEL_VERSION}/{token}/{timestep}/{name}"
