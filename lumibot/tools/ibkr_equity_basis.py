"""Immutable action-ledger provenance; never infer adjustment from price shape.

An action snapshot identifies the split basis a caller expects. It does not prove
that a provider has applied those splits. The provider integration must establish
that separately before decorating its returned prices.
"""

from dataclasses import dataclass
import hashlib
import json
import math

import numpy as np
import pandas as pd


class EquityBasisError(ValueError):
    """Missing, contradictory, or invalid equity adjustment provenance."""


def _session_date(value):
    if value is None:
        raise EquityBasisError("An explicit date is required")
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp):
            raise ValueError("NaT")
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert("America/New_York")
        return stamp.date().isoformat()
    except (TypeError, ValueError, OverflowError) as exc:
        raise EquityBasisError("Invalid action or coverage date") from exc


def _action_value(value):
    if isinstance(value, (bool, np.bool_)):
        raise EquityBasisError("Action values must be finite nonnegative numbers")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise EquityBasisError("Action values must be finite nonnegative numbers") from exc
    if not math.isfinite(number) or number < 0:
        raise EquityBasisError("Action values must be finite nonnegative numbers")
    return number


@dataclass(frozen=True)
class EquityBasisSnapshot:
    """Verified effective events and explicit observation/coverage metadata.

    ``events`` contains immutable (New York session date, dividend, split) tuples.
    A caller must verify the supplied ledger and its coverage; this pure class
    validates consistency but performs no provider access or price verification.
    """

    as_of: str
    coverage_through: str
    events: tuple

    def __post_init__(self):
        # The public dataclass constructor cannot bypass the factory's invariants.
        try:
            observed = pd.Timestamp(self.as_of)
            if pd.isna(observed) or observed.tzinfo is None:
                raise ValueError("as_of")
        except (TypeError, ValueError, OverflowError) as exc:
            raise EquityBasisError("An explicit timezone-aware as_of is required") from exc
        coverage = _session_date(self.coverage_through)
        if coverage != self.coverage_through or coverage > _session_date(observed):
            raise EquityBasisError("Invalid explicit action coverage")
        if not isinstance(self.events, tuple):
            raise EquityBasisError("Action events must be immutable tuples")
        previous = None
        for event in self.events:
            if not isinstance(event, tuple) or len(event) != 3:
                raise EquityBasisError("Action events must be immutable triples")
            date, dividend, split = event
            if _session_date(date) != date or date > coverage or (previous and date <= previous):
                raise EquityBasisError("Effective action dates must be unique, ordered and within coverage")
            _action_value(dividend)
            _action_value(split)
            if not isinstance(dividend, (int, float)) or not isinstance(split, (int, float)):
                raise EquityBasisError("Frozen actions must contain numeric scalars")
            previous = date

    @classmethod
    def from_verified_actions(cls, actions, *, as_of, coverage_through):
        try:
            observed = pd.Timestamp(as_of)
            if pd.isna(observed) or observed.tzinfo is None:
                raise ValueError("as_of must be a timezone-aware timestamp")
        except (TypeError, ValueError, OverflowError) as exc:
            raise EquityBasisError("An explicit timezone-aware as_of is required") from exc
        cutoff = _session_date(observed)
        coverage = _session_date(coverage_through)
        if coverage > cutoff:
            raise EquityBasisError("Action coverage cannot be after as_of")
        if not isinstance(actions, pd.DataFrame) or not {"Dividends", "Stock Splits"}.issubset(actions.columns):
            raise EquityBasisError("Verified actions require Dividends and Stock Splits columns")
        if not actions.columns.is_unique:
            raise EquityBasisError("Action columns must be unique")
        dates = [_session_date(value) for value in actions.index]
        if len(dates) != len(set(dates)):
            raise EquityBasisError("Action session dates must be unique")
        events = []
        for date, dividend, split in zip(dates, actions["Dividends"], actions["Stock Splits"]):
            dividend, split = _action_value(dividend), _action_value(split)
            # Validate future rows too, but announcements do not change today's basis.
            if date > cutoff:
                continue
            if date > coverage and (dividend != 0 or split not in (0, 1)):
                raise EquityBasisError("Effective event lies outside verified action coverage")
            if dividend != 0 or split not in (0, 1):
                events.append((date, dividend, split))
        return cls(observed.tz_convert("UTC").isoformat(), coverage, tuple(sorted(events)))

    @property
    def split_fingerprint(self):
        # Neither cash dividends nor refresh timestamps invalidate historical prices.
        splits = [(date, float(split)) for date, _, split in self.events if split not in (0, 1)]
        encoded = json.dumps(splits, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return "SPLIT_V1_" + hashlib.sha256(encoded).hexdigest()

    def to_dict(self):
        return {
            "schema": "ibkr-equity-actions-v1",
            "as_of": self.as_of,
            "coverage_through": self.coverage_through,
            "events": [list(event) for event in self.events],
            "split_fingerprint": self.split_fingerprint,
        }

    def actions_frame(self):
        dates = pd.DatetimeIndex([event[0] for event in self.events], tz="America/New_York")
        return pd.DataFrame([(event[1], event[2]) for event in self.events], index=dates,
                            columns=["Dividends", "Stock Splits"], dtype=float)


def _frame_basis(frame):
    metadata = frame.attrs.get("ibkr_equity_basis")
    attr_basis = metadata.get("split_fingerprint") if isinstance(metadata, dict) else None
    if metadata is not None and (not isinstance(attr_basis, str) or not attr_basis):
        raise EquityBasisError("Invalid frame adjustment-basis metadata")
    if "_ibkr_split_basis" not in frame:
        if attr_basis and len(frame):
            raise EquityBasisError("Missing row adjustment basis")
        return attr_basis
    relevant = frame
    if "missing" in frame:
        # Only actual boolean placeholders can waive row provenance validation.
        placeholders = frame["missing"].map(lambda value: isinstance(value, (bool, np.bool_)) and bool(value))
        relevant = frame.loc[~placeholders]
    values = relevant["_ibkr_split_basis"]
    if not values.map(lambda value: isinstance(value, str) and bool(value)).all():
        raise EquityBasisError("Missing row adjustment basis")
    unique = set(values)
    if len(unique) > 1:
        raise EquityBasisError("Mixed row adjustment bases")
    row_basis = next(iter(unique), None)
    if attr_basis and row_basis and attr_basis != row_basis:
        raise EquityBasisError("Conflicting frame and row adjustment bases")
    return row_basis or attr_basis


def assert_same_basis(existing, new):
    """Reject an unsafe merge, including known/unknown provenance mixtures."""
    if _frame_basis(existing) != _frame_basis(new):
        raise EquityBasisError("Cannot combine different or unknown equity adjustment bases")


def decorate_adjusted_frame(frame, snapshot, *, provider_prices_verified):
    """Attach caller-attested provenance without altering any market-data values."""
    if provider_prices_verified is not True:
        raise EquityBasisError("The caller must verify provider price adjustment separately")
    current = _frame_basis(frame)
    if current is not None and current != snapshot.split_fingerprint:
        raise EquityBasisError("Cannot relabel prices from a different adjustment basis")
    result = frame.copy(deep=True)
    result["_split_adjusted"] = True
    result["_ibkr_split_basis"] = snapshot.split_fingerprint
    result.attrs["ibkr_equity_basis"] = snapshot.to_dict()
    return result
