"""fin-primitives backend for the most common ``self.indicators`` calls.

``sma``, ``ema``, ``rsi``, ``macd``, ``bbands`` and ``atr`` run through
``fin_primitives.fast``: one float64 pass in Rust over the bar columns, with
pandas-ta-classic's (TA-Lib's) seeding. The result has the same type, index,
name and column names as the pandas-ta-classic call it replaces, and the same
values (``tests/test_indicator_fast_backend.py`` checks them against
pandas-ta-classic).

Only the plain form of each call is handled here. Any other parameter
(``talib``, ``offset`` > 0, ``fillna``, ``mamode``, ``ddof``, signal columns...),
missing, non-finite or non-numeric input (strings that happen to parse as numbers
included), a series shorter than the period, a missing ``fin_primitives`` install,
or a pandas-ta-classic older than 0.9.0 returns ``None`` and the caller uses
pandas-ta-classic as before. The kernels follow pandas-ta-classic 0.9.0; older
releases seed RSI's average one bar earlier and give a flat window's Bollinger %B
another value, so the fast path stays off there and results never depend on which
backend ran.
"""

from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np
import pandas as pd

try:  # pragma: no cover - exercised by the import itself
    from fin_primitives import fast as _fast
except Exception:  # ImportError, or an older fin-primitives without ``fast``
    _fast = None

# Flat-window tolerance pandas-ta-classic uses for the band width (utils.degenerate_zero).
_DEGENERATE_ATOL = 1e-12

# The pandas-ta-classic release whose numbers the fin_primitives kernels reproduce.
PANDAS_TA_BASELINE = (0, 9, 0)


def _version_tuple(text: str) -> tuple:
    parts = []
    for piece in text.split("."):
        digits = ""
        for ch in piece:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        parts.append(int(digits))
        if len(digits) != len(piece):
            break
    return tuple(parts)


def _installed_pandas_ta_version():
    try:
        from importlib.metadata import version

        return version("pandas-ta-classic")
    except Exception:
        return None


def baseline_matches(installed=None) -> bool:
    """True when the installed pandas-ta-classic is at least ``PANDAS_TA_BASELINE``."""
    text = installed if installed is not None else _installed_pandas_ta_version()
    if not text:
        return False
    return _version_tuple(text) >= PANDAS_TA_BASELINE


_BASELINE_OK = baseline_matches()


def _period(kwargs: dict, name: str, default: int, *, minimum: int = 1):
    value = kwargs.pop(name, None)
    if value is None:
        return default
    if type(value) is not int or value < minimum:
        return None
    return value


def _number(kwargs: dict, name: str, default: float):
    value = kwargs.pop(name, None)
    if value is None:
        return float(default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) and value > 0 else None


def _column(df: pd.DataFrame, name: str):
    if name not in df.columns:
        return None
    source = df[name]
    # Only real numeric columns: object or string columns ("1", "2", ...) would convert
    # to float64 here, while pandas-ta-classic works on the original column.
    if not pd.api.types.is_numeric_dtype(source) or pd.api.types.is_bool_dtype(source):
        return None
    try:
        values = source.to_numpy(dtype=np.float64)
    except (TypeError, ValueError):
        return None
    return values if np.isfinite(values).all() else None


def _series(values, index, name: str, category: str) -> pd.Series:
    series = pd.Series(values, index=index, name=name)
    series.category = category
    return series


def _sma(df, kwargs):
    length = _period(kwargs, "length", 10)
    close = _column(df, "close")
    if length is None or close is None or len(close) < length:
        return None
    return _series(_fast.sma(close, length), df.index, f"SMA_{length}", "overlap")


def _ema(df, kwargs):
    length = _period(kwargs, "length", 10)
    close = _column(df, "close")
    if length is None or close is None or len(close) < length:
        return None
    return _series(_fast.ema(close, length), df.index, f"EMA_{length}", "overlap")


def _rsi(df, kwargs):
    length = _period(kwargs, "length", 14)
    close = _column(df, "close")
    if length is None or close is None or len(close) < length:
        return None
    return _series(_fast.rsi(close, length), df.index, f"RSI_{length}", "momentum")


def _atr(df, kwargs):
    length = _period(kwargs, "length", 14)
    high, low, close = (_column(df, name) for name in ("high", "low", "close"))
    if length is None or high is None or low is None or close is None or len(close) < length:
        return None
    values = _fast.atr(high, low, close, length)
    return _series(values, df.index, f"ATRr_{length}", "volatility")


def _macd(df, kwargs):
    fast = _period(kwargs, "fast", 12)
    slow = _period(kwargs, "slow", 26)
    signal = _period(kwargs, "signal", 9)
    close = _column(df, "close")
    if None in (fast, slow, signal) or close is None:
        return None
    if slow < fast:  # pandas-ta-classic swaps them, names included
        fast, slow = slow, fast
    if len(close) < max(fast, slow, signal):
        return None
    line, signal_line, hist = _fast.macd(close, fast, slow, signal)
    props = f"_{fast}_{slow}_{signal}"
    frame = pd.DataFrame(
        {f"MACD{props}": line, f"MACDh{props}": hist, f"MACDs{props}": signal_line},
        index=df.index,
    )
    frame.name = f"MACD{props}"
    frame.category = "momentum"
    return frame


def _bbands(df, kwargs):
    length = _period(kwargs, "length", 5, minimum=2)
    std = _number(kwargs, "std", 2.0)
    close = _column(df, "close")
    if length is None or std is None or close is None or len(close) < length:
        return None
    lower, mid, upper = _fast.bbands(close, length, std)
    # Band width and %B exactly as pandas-ta-classic derives them from the bands.
    width = upper - lower
    width = np.where(np.isclose(width, 0, atol=_DEGENERATE_ATOL), 0.0, width)
    with np.errstate(divide="ignore", invalid="ignore"):
        bandwidth = 100 * width / mid
        percent = np.where(width == 0, 0.0, (close - lower) / np.where(width != 0, width, np.nan))
    percent = np.where(np.isnan(width), np.nan, percent)
    suffix = f"_{length}_{std}"
    frame = pd.DataFrame(
        {
            f"BBL{suffix}": lower,
            f"BBM{suffix}": mid,
            f"BBU{suffix}": upper,
            f"BBB{suffix}": bandwidth,
            f"BBP{suffix}": percent,
        },
        index=df.index,
    )
    frame.name = f"BBANDS{suffix}"
    frame.category = "volatility"
    return frame


_HANDLERS: dict[str, Callable[[pd.DataFrame, dict], Any]] = {
    "sma": _sma,
    "ema": _ema,
    "rsi": _rsi,
    "atr": _atr,
    "macd": _macd,
    "bbands": _bbands,
}

FAST_INDICATORS = frozenset(_HANDLERS)


def compute(df: pd.DataFrame, name: str, kwargs: dict):
    """pandas-ta-classic's result for ``name(**kwargs)`` over ``df``, or ``None``
    when this backend does not handle the call (the caller then uses pandas-ta)."""
    handler = _HANDLERS.get(name)
    if handler is None or _fast is None or not _BASELINE_OK:
        return None
    remaining = dict(kwargs)
    if remaining.pop("offset", None) not in (None, 0):
        return None
    result = handler(df, remaining)
    if remaining:  # any parameter this backend does not model
        return None
    return result
