"""Account/window evidence for normalized cash history, separate from delivery."""

import datetime as dt
import math
import uuid
from dataclasses import dataclass, field

from .cash_event import CashEvent


@dataclass(frozen=True)
class CashEventSync:
    """One immutable history read; unresolved capital never certifies coverage.

    Times are aware UTC instants. Requested windows are start inclusive and end
    exclusive. This records retrieval only; the receiver must prove persistence
    of every batch before using coverage for performance calculations.
    """

    events: tuple[CashEvent, ...]
    broker_name: str
    account_id: str
    mode: str
    start: dt.datetime
    end: dt.datetime
    fetched_at: dt.datetime
    state: str
    reason_code: str | None = None
    sync_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def __post_init__(self):
        for value in (self.start, self.end, self.fetched_at):
            if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("Cash history timestamps must have explicit timezones")
        if not self.start < self.end <= self.fetched_at:
            raise ValueError("Cash history requires an increasing observed window")
        if self.state not in {"complete", "partial", "unsupported", "unavailable", "unknown"}:
            raise ValueError("Invalid cash history state")
        if self.state != "complete" and not self.reason_code:
            raise ValueError("Incomplete cash history requires a reason")
        if self.mode not in {"paper", "live"}:
            raise ValueError("Cash history account mode is required")
        for value in (self.account_id, self.broker_name):
            if not isinstance(value, str) or not value.strip() or len(value) > 256:
                raise ValueError("Cash history account identity is required")
        if "*" in self.account_id or "…" in self.account_id:
            raise ValueError("Cash history requires an unmasked account identity")
        if not isinstance(self.events, tuple) or len(self.events) > 1000000:
            raise ValueError("Cash history events must be a bounded tuple")
        ids = set()
        for event in self.events:
            if not isinstance(event, CashEvent) or not self.start <= event.occurred_at < self.end:
                raise ValueError("Cash history contains an invalid or out-of-window event")
            if event.broker_name != self.broker_name.lower() or event.event_id in ids:
                raise ValueError("Cash history event identities conflict")
            ids.add(event.event_id)

    def to_dict(self, *, producer_kind, batch_index, batch_size):
        if producer_kind not in {"managed_bot", "external_bot", "account_snapshot"}:
            raise ValueError("Invalid cash history producer kind")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("Cash history batch size must be positive")
        count = max(1, math.ceil(len(self.events) / batch_size))
        if count > 10000 or type(batch_index) is not int or not 0 <= batch_index < count:
            raise ValueError("Cash history batch is outside its bound")

        def iso(value):
            return value.astimezone(dt.timezone.utc).isoformat()

        requested = {"start": iso(self.start), "end": iso(self.end)}
        return {
            "schema_version": 1,
            "sync_id": self.sync_id,
            "producer_kind": producer_kind,
            "account_ref": {"broker": self.broker_name.lower(), "account_id": self.account_id, "mode": self.mode},
            "requested": requested,
            "covered_intervals": [dict(requested)] if self.state == "complete" else [],
            "state": self.state,
            "pagination_exhausted": self.state in {"complete", "partial"},
            "fetched_at": iso(self.fetched_at),
            "completed_at": iso(self.fetched_at) if self.state in {"complete", "partial"} else None,
            "reason_code": self.reason_code,
            "batch": {"index": batch_index, "count": count, "total_event_count": len(self.events)},
        }
