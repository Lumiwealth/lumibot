"""Typed IBKR history outcomes, repair planning, and safe backtest telemetry.

This module intentionally contains no broker calls.  It keeps the policy that
decides what may become durable negative-cache state separate from the large
IBKR orchestration helper so the policy can be unit tested without a gateway.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from threading import Lock
from typing import Any, Iterable, Optional, Sequence

import pandas as pd

IBKR_HISTORY_HEALTH_MISSING_SESSION_LIMIT = 100


class HistoryOutcome(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    CONFIRMED_NO_DATA = "confirmed_no_data"
    TRANSIENT_FAILURE = "transient_failure"


@dataclass(frozen=True)
class HistoryFailureClassification:
    outcome: HistoryOutcome
    reason: str
    persist_negative_cache: bool = False
    identity_related: bool = False


def classify_history_failure(exc: BaseException) -> HistoryFailureClassification:
    """Classify a provider/downloader failure without treating ambiguity as fact."""

    message = str(exc or "").strip()
    normalized = message.lower()

    identity_tokens = (
        "chart data unavailable",
        "invalid conid",
        "unknown conid",
        "contract not found",
    )
    if any(token in normalized for token in identity_tokens):
        return HistoryFailureClassification(
            outcome=HistoryOutcome.TRANSIENT_FAILURE,
            reason="identity_related_history_failure",
            identity_related=True,
        )

    confirmed_tokens = (
        "asset does not exist",
        "unable to resolve ibkr conid",
        "ibkr conid lookup is negatively cached",
        "secdef/search returned no",
        "contract is not available for trading",
    )
    if any(token in normalized for token in confirmed_tokens):
        return HistoryFailureClassification(
            outcome=HistoryOutcome.CONFIRMED_NO_DATA,
            reason="confirmed_no_data",
            persist_negative_cache=True,
        )

    partial_tokens = (
        "malformed_history_payload",
        "overlap_bar_mismatch",
        "partial_history",
        "pagination returned empty data before covering",
        "pagination returned an empty frame before covering",
        "remained invalid after rebuild",
    )
    if any(token in normalized for token in partial_tokens):
        return HistoryFailureClassification(
            outcome=HistoryOutcome.PARTIAL,
            reason="partial_history",
        )

    return HistoryFailureClassification(
        outcome=HistoryOutcome.TRANSIENT_FAILURE,
        reason="transient_history_failure",
    )


def group_contiguous_missing_sessions(
    expected_sessions: Sequence[pd.Timestamp],
    missing_sessions: Iterable[pd.Timestamp],
) -> list[list[pd.Timestamp]]:
    """Group missing sessions that are adjacent in the exchange calendar."""

    expected = [pd.Timestamp(value) for value in expected_sessions]
    positions = {value.date(): index for index, value in enumerate(expected)}
    missing = sorted(
        {pd.Timestamp(value) for value in missing_sessions},
        key=lambda value: positions.get(value.date(), len(expected)),
    )
    groups: list[list[pd.Timestamp]] = []
    for session in missing:
        position = positions.get(session.date())
        if position is None:
            continue
        if not groups:
            groups.append([session])
            continue
        previous_position = positions.get(groups[-1][-1].date())
        if previous_position is not None and position == previous_position + 1:
            groups[-1].append(session)
        else:
            groups.append([session])
    return groups


def coalesce_nearby_session_groups(
    groups: Sequence[Sequence[pd.Timestamp]],
    *,
    max_calendar_span_days: int = 10,
) -> list[list[pd.Timestamp]]:
    """Coalesce nearby gaps when one small request is cheaper than two."""

    combined: list[list[pd.Timestamp]] = []
    for raw_group in groups:
        group = [pd.Timestamp(value) for value in raw_group]
        if not group:
            continue
        if not combined:
            combined.append(group)
            continue
        proposed_span = (group[-1].normalize() - combined[-1][0].normalize()).days
        if proposed_span <= max_calendar_span_days:
            combined[-1].extend(group)
        else:
            combined.append(group)
    return combined


def split_session_groups(
    groups: Sequence[Sequence[pd.Timestamp]],
    *,
    max_sessions: int = 10,
) -> list[list[pd.Timestamp]]:
    """Split large gaps so one repair request never spans an unbounded range."""

    if max_sessions <= 0:
        raise ValueError("max_sessions must be positive")
    result: list[list[pd.Timestamp]] = []
    for group in groups:
        values = [pd.Timestamp(value) for value in group]
        for offset in range(0, len(values), max_sessions):
            result.append(values[offset : offset + max_sessions])
    return result


def padded_repair_window(
    sessions: Sequence[pd.Timestamp],
    *,
    padding_days: int = 1,
) -> tuple[datetime, datetime]:
    """Return a small end-exclusive calendar window around missing sessions."""

    if not sessions:
        raise ValueError("sessions must not be empty")
    first = pd.Timestamp(min(sessions)).normalize() - pd.Timedelta(days=padding_days)
    last = pd.Timestamp(max(sessions)).normalize() + pd.Timedelta(days=padding_days + 1)
    return first.to_pydatetime(), last.to_pydatetime()


_HEALTH_LOCK = Lock()
_HEALTH_BY_SERIES: dict[str, dict[str, Any]] = {}


def record_history_health(
    *,
    symbol: str,
    asset_type: str,
    timestep: str,
    requested_start: datetime,
    requested_end: datetime,
    outcome: HistoryOutcome,
    expected_sessions: Optional[int] = None,
    returned_sessions: Optional[int] = None,
    missing_sessions: Optional[Iterable[Any]] = None,
    repair_attempts: int = 0,
    transient_failures: int = 0,
    conid_refreshes: int = 0,
    reason: Optional[str] = None,
) -> None:
    """Record bounded, credential-free health state for settings.json."""

    key = "|".join((str(asset_type), str(symbol).upper(), str(timestep)))
    missing = sorted({str(value)[:10] for value in (missing_sessions or [])})
    payload = {
        "symbol": str(symbol).upper(),
        "asset_type": str(asset_type),
        "timestep": str(timestep),
        "requested_start": requested_start.astimezone(timezone.utc).isoformat(),
        "requested_end": requested_end.astimezone(timezone.utc).isoformat(),
        "outcome": outcome.value,
        "expected_sessions": expected_sessions,
        "returned_sessions": returned_sessions,
        "missing_sessions": missing[:IBKR_HISTORY_HEALTH_MISSING_SESSION_LIMIT],
        "missing_session_count": len(missing),
        "repair_attempts": int(repair_attempts),
        "transient_failures": int(transient_failures),
        "conid_refreshes": int(conid_refreshes),
        "reason": str(reason)[:500] if reason else None,
    }
    with _HEALTH_LOCK:
        previous = _HEALTH_BY_SERIES.get(key)
        if previous:
            payload["repair_attempts"] += int(previous.get("repair_attempts") or 0)
            payload["transient_failures"] += int(previous.get("transient_failures") or 0)
            payload["conid_refreshes"] += int(previous.get("conid_refreshes") or 0)
        _HEALTH_BY_SERIES[key] = payload


def ibkr_history_health_snapshot() -> dict[str, Any]:
    with _HEALTH_LOCK:
        series = [dict(value) for _, value in sorted(_HEALTH_BY_SERIES.items())]
    incomplete = sum(1 for value in series if value.get("outcome") != HistoryOutcome.COMPLETE.value)
    return {
        "provider": "ibkr",
        "series_count": len(series),
        "incomplete_series_count": incomplete,
        "complete": incomplete == 0,
        "series": series,
    }


def reset_ibkr_history_health() -> None:
    with _HEALTH_LOCK:
        _HEALTH_BY_SERIES.clear()


def reset_ibkr_history_health_for_testing() -> None:
    reset_ibkr_history_health()


def retry_after_datetime(*, ttl_seconds: int, now: Optional[datetime] = None) -> datetime:
    base = now or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base.astimezone(timezone.utc) + timedelta(seconds=int(ttl_seconds))


def audit_ibkr_cache_frame(frame: pd.DataFrame) -> dict[str, Any]:
    """Return structural health for one cached parquet frame."""

    if frame is None:
        frame = pd.DataFrame()
    index = pd.DatetimeIndex(frame.index) if len(frame.index) else pd.DatetimeIndex([])
    missing_mask = (
        frame["missing"].fillna(False).astype(bool)
        if "missing" in frame.columns
        else pd.Series(False, index=frame.index)
    )
    real = frame.loc[~missing_mask]
    ohlc = [column for column in ("open", "high", "low", "close") if column in real.columns]
    real_ohlc_null_rows = int(real[ohlc].isna().any(axis=1).sum()) if ohlc else 0
    return {
        "rows": int(len(frame)),
        "real_rows": int((~missing_mask).sum()),
        "placeholder_rows": int(missing_mask.sum()),
        "all_placeholder": bool(len(frame) > 0 and bool(missing_mask.all())),
        "duplicate_timestamps": int(index.duplicated().sum()),
        "monotonic": bool(index.is_monotonic_increasing),
        "real_ohlc_null_rows": real_ohlc_null_rows,
        "first_timestamp": index.min().isoformat() if len(index) else None,
        "last_timestamp": index.max().isoformat() if len(index) else None,
    }
