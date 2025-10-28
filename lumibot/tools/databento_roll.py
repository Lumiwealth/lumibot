"""
Shared utilities for handling DataBento continuous futures roll logic.

This module centralizes symbol resolution and roll schedule computation so that
both the pandas and polars implementations stay in sync.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Tuple

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset

# Number of calendar days before expiration to roll into the next contract.
# This defaults to 7 (~5 business days) but can be overridden with an env var.
ROLL_DAYS_BEFORE_EXPIRATION = int(os.getenv("LUMIBOT_FUTURES_ROLL_DAYS", "7"))

# Caches used for symbol resolution so repeated lookups are cheap.
_DATETIME_NORMALIZATION_CACHE: Dict[float, datetime] = {}
_SYMBOL_RESOLUTION_CACHE: Dict[Tuple[str, str, float], str] = {}

NY_TZ = pytz.timezone("America/New_York")
UTC = timezone.utc


def _ensure_tz(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware, defaulting to the platform TZ."""
    if dt.tzinfo is None:
        return LUMIBOT_DEFAULT_PYTZ.localize(dt)
    return dt


def _normalize_reference_datetime(dt: datetime) -> datetime:
    """Normalize datetimes for use in caches when resolving symbols."""
    if dt is None:
        return dt

    cache_key = dt.timestamp() if hasattr(dt, "timestamp") else None
    if cache_key is not None and cache_key in _DATETIME_NORMALIZATION_CACHE:
        return _DATETIME_NORMALIZATION_CACHE[cache_key]

    if dt.tzinfo is not None:
        normalized = dt.astimezone(LUMIBOT_DEFAULT_PYTZ).replace(tzinfo=None)
    else:
        normalized = dt

    if cache_key is not None:
        _DATETIME_NORMALIZATION_CACHE[cache_key] = normalized

    return normalized


def resolve_symbol_for_datetime(asset: Asset, dt: datetime) -> str:
    """
    Resolve the continuous futures symbol for a specific datetime using the
    asset's roll rules.
    """
    dt_norm = _normalize_reference_datetime(dt)
    cache_key = (
        asset.symbol,
        asset.asset_type,
        dt_norm.timestamp() if dt_norm is not None else float("inf"),
    )

    if cache_key in _SYMBOL_RESOLUTION_CACHE:
        return _SYMBOL_RESOLUTION_CACHE[cache_key]

    variants = asset.resolve_continuous_futures_contract_variants(reference_date=dt_norm)
    contract = variants[2]  # two-digit year variant

    # DataBento prefers the short year format (single digit); reuse helper.
    month_code = contract[len(asset.symbol)]
    year_char = contract[-1]
    resolved_symbol = f"{asset.symbol}{month_code}{year_char}"

    _SYMBOL_RESOLUTION_CACHE[cache_key] = resolved_symbol
    return resolved_symbol


def resolve_symbols_for_range(asset: Asset, start: datetime, end: datetime) -> List[str]:
    """
    Resolve the list of DataBento contract symbols required to cover a datetime range.
    """
    if start is None or end is None:
        return []

    start_ref = _normalize_reference_datetime(start)
    end_ref = _normalize_reference_datetime(end)

    if start_ref is None or end_ref is None:
        return [
            resolve_symbol_for_datetime(asset, _ensure_tz(start)),
        ]

    symbols: List[str] = []
    seen = set()
    cursor = start_ref
    step = timedelta(days=45)  # ensures we hop across quarter rolls

    while cursor <= end_ref + timedelta(days=45):
        symbol = resolve_symbol_for_datetime(asset, cursor)
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
        cursor += step

    # Ensure the last contract covers the end reference.
    final_symbol = resolve_symbol_for_datetime(asset, end_ref)
    if final_symbol not in seen:
        symbols.append(final_symbol)

    return symbols


def _parse_expiration(definition: Dict) -> datetime:
    """
    Parse the expiration field from a DataBento instrument definition.
    """
    expiration = (
        definition.get("expiration")
        or definition.get("maturity_date")
        or definition.get("last_trade_date")
    )
    if expiration is None:
        raise ValueError("Instrument definition missing expiration information")

    if isinstance(expiration, datetime):
        dt_local = expiration
    else:
        expiration_str = str(expiration)
        # Handle ISO strings with optional timezone offset.
        if "T" in expiration_str:
            expiration_str = expiration_str.replace("Z", "+00:00")
            dt_local = datetime.fromisoformat(expiration_str)
        else:
            dt_local = datetime.strptime(expiration_str, "%Y-%m-%d")

    if dt_local.tzinfo is None:
        dt_local = NY_TZ.localize(dt_local)
    else:
        dt_local = dt_local.astimezone(NY_TZ)

    # Futures generally stop trading in the afternoon; rolling on midnight is fine.
    return dt_local


def build_roll_schedule(
    asset: Asset,
    start: datetime,
    end: datetime,
    definition_provider: Callable[[str], Dict],
    roll_days: int = ROLL_DAYS_BEFORE_EXPIRATION,
) -> List[Tuple[str, datetime, datetime]]:
    """
    Build a list of (symbol, start_utc, end_utc) windows indicating which contract
    should be used at each point in time.
    """
    if roll_days < 0:
        raise ValueError("roll_days must be non-negative")

    start = _ensure_tz(start)
    end = _ensure_tz(end)
    symbols = resolve_symbols_for_range(asset, start, end)

    if not symbols:
        return []

    schedule: List[Tuple[str, datetime, datetime]] = []
    current_start = datetime.min.replace(tzinfo=UTC)

    for idx, symbol in enumerate(symbols):
        definition = definition_provider(symbol)
        if not definition:
            continue

        expiration_local = _parse_expiration(definition)
        roll_local = expiration_local - timedelta(days=roll_days)
        roll_local = max(roll_local, start)
        roll_utc = roll_local.astimezone(UTC)

        if idx < len(symbols) - 1:
            end_utc = roll_utc
        else:
            end_utc = datetime.max.replace(tzinfo=UTC)

        schedule.append((symbol, current_start, end_utc))
        current_start = roll_utc

    if not schedule:
        return []

    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)

    clipped: List[Tuple[str, datetime, datetime]] = []
    for symbol, window_start, window_end in schedule:
        s = max(window_start, start_utc)
        e = min(window_end, end_utc)
        if e <= s:
            continue
        clipped.append((symbol, s, e))

    if not clipped:
        clipped.append((schedule[-1][0], start_utc, end_utc))
    else:
        last_symbol, s, e = clipped[-1]
        if e < end_utc:
            clipped[-1] = (last_symbol, s, end_utc)

    return clipped

