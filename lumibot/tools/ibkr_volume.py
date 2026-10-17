"""Validate IBKR historical-volume metadata without changing market prices."""
from collections.abc import Mapping
from decimal import Decimal
import math
from typing import Any, Optional


class IbkrVolumeMetadataError(ValueError):
    """Historical volume cannot be interpreted safely from this response."""


_NON_VOLUME_SOURCES = frozenset({"BID", "ASK", "BID_ASK", "MIDPOINT"})


def validated_volume_factor(payload: Mapping[str, Any], asset_type: Any, source: str) -> Optional[float]:
    """Return the explicit factor converting provider volume to canonical units.

    Stock trade volume requires response metadata; a route, currency, ticker,
    or price pattern cannot establish shares versus lots. Nonstock instruments retain
    their legacy units when no factor is supplied. Non-trade quote bars have no
    volume and need no conversion. This function neither changes the payload
    nor normalizes it: callers apply the returned factor exactly once and must
    preserve unavailable-volume sentinels separately.
    """
    if not isinstance(payload, Mapping):
        raise IbkrVolumeMetadataError("IBKR volumeFactor response must be a mapping")

    factor = None
    if "volumeFactor" in payload:
        value = payload["volumeFactor"]
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, str)):
            raise IbkrVolumeMetadataError("IBKR volumeFactor must be a finite positive number")
        try:
            factor = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise IbkrVolumeMetadataError("IBKR volumeFactor must be a finite positive number") from exc
        if not math.isfinite(factor) or factor <= 0:
            raise IbkrVolumeMetadataError("IBKR volumeFactor must be a finite positive number")

    source_name = str(source).strip().upper().replace(" ", "_")
    if source_name in _NON_VOLUME_SOURCES:
        return 1.0
    if factor is not None:
        return factor

    asset_name = str(getattr(asset_type, "value", asset_type)).strip().lower()
    if asset_name not in {"stock", "etf"}:
        return 1.0
    # Stock trade volume with no factor has unknown units (shares or 100-share
    # lots). The prices in the same response are still real, so the caller must
    # keep them and mark only the volume as unavailable. Raising here discarded
    # whole pages of older IBKR history (LumiBot 4.6.19 incident, 2026-10-17).
    return None
