"""Durable subscription-only usage ledger. No API prices or API credentials.

Incomplete requests still consume the call allowance on resume. The provider
owns its plan quota; a local call cap bounds each qualification attempt.
"""

import json
from datetime import datetime, timezone

from scripts.agent_eval_call_budget import EvalCallBudget

MAX_SUBSCRIPTION_QUALIFICATION_CALLS = 2500


def subscription_call_state(rows):
    """Validate append-only allowance history without forgiving consumed calls."""
    if (not rows or not isinstance(rows[0], dict)
            or rows[0].get("kind") != "budget"
            or rows[0].get("funding") != "chatgpt-plan" or rows[0].get("version") != 2):
        raise ValueError("Native subscription call ledger is required")
    limit = rows[0].get("max_calls")
    if type(limit) is not int or not 0 < limit <= MAX_SUBSCRIPTION_QUALIFICATION_CALLS:
        raise ValueError("Native model-call allowance is invalid or exceeded")
    records = []
    reserved = 0
    for row in rows[1:]:
        if not isinstance(row, dict):
            raise ValueError("Invalid subscription ledger event")
        if row.get("kind") == "allowance_amendment":
            if (set(row) != {"kind", "previous_max_calls", "max_calls", "reason", "timestamp"}
                    or type(row.get("previous_max_calls")) is not int
                    or row["previous_max_calls"] != limit
                    or type(row.get("max_calls")) is not int
                    or not limit < row["max_calls"] <= MAX_SUBSCRIPTION_QUALIFICATION_CALLS
                    or not isinstance(row.get("reason"), str) or not row["reason"].strip()
                    or len(row["reason"]) > 1024):
                raise ValueError("Invalid subscription allowance amendment")
            try:
                instant = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            except (AttributeError, TypeError, ValueError):
                raise ValueError("Invalid subscription allowance amendment timestamp") from None
            if instant.tzinfo is None or instant.utcoffset() is None:
                raise ValueError("Subscription allowance amendment requires a timezone")
            limit = row["max_calls"]
        else:
            if row.get("kind") == "reserve":
                reserved += 1
                if reserved > limit:
                    raise ValueError("Native model-call allowance is invalid or exceeded")
            records.append(row)
    return limit, EvalCallBudget._calls(records)


class SubscriptionCallLimitError(ValueError):
    code = "eval_call_limit_exceeded"


class SubscriptionCallBudget(EvalCallBudget):
    def __init__(self, path, *, max_calls=500):
        if type(max_calls) is not int or not 0 < max_calls <= MAX_SUBSCRIPTION_QUALIFICATION_CALLS:
            raise ValueError(
                f"Subscription call limit must be a positive integer at most {MAX_SUBSCRIPTION_QUALIFICATION_CALLS}"
            )
        self.max_calls = max_calls
        from pathlib import Path
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.prices = {}
        self.header = {"kind": "budget", "version": 2, "funding": "chatgpt-plan",
                       "max_calls": max_calls, "max_input_tokens": 1_048_576}
        if self.path.exists() and self.path.stat().st_size:
            with self.path.open(encoding="utf-8") as stream:
                original_header = json.loads(stream.readline())
            if ({key: value for key, value in original_header.items() if key != "max_calls"}
                    != {key: value for key, value in self.header.items() if key != "max_calls"}):
                raise ValueError("Subscription ledger contract changed")
            self.header = original_header
        with self._ledger() as (stream, rows):
            if not rows:
                self._append(stream, self.header)
                rows = [self.header]
            limit, _ = subscription_call_state(rows)
            if limit != max_calls:
                raise ValueError("Subscription ledger contract changed; an explicit amendment is required")

    def _calls(self, rows):
        return subscription_call_state([self.header, *rows])[1]

    @classmethod
    def amend_limit(cls, path, *, previous_limit, max_calls, reason):
        """Record a disclosed new allowance; never edit the old header or calls."""
        from pathlib import Path
        if not Path(path).is_file() or not Path(path).stat().st_size:
            raise ValueError("An existing subscription ledger is required for amendment")
        owner = cls(path, max_calls=previous_limit)
        with owner._ledger() as (stream, rows):
            current, _ = subscription_call_state(rows)
            if current != previous_limit:
                raise ValueError("Subscription allowance changed concurrently")
            amendment = {"kind": "allowance_amendment", "previous_max_calls": current,
                         "max_calls": max_calls, "reason": reason,
                         "timestamp": datetime.now(timezone.utc).isoformat()}
            subscription_call_state([*rows, amendment])
            owner._append(stream, amendment)

    def _cost(self, model, inputs, cached, outputs):
        if not str(model).startswith("chatgpt/"):
            raise ValueError("Eval inference requires subscription funding; API fallback is forbidden")
        if model != "chatgpt/gpt-6-luna":
            raise ValueError("Eval inference requires Luna; unapproved model escalation is forbidden")
        return 0

    def reserve(self, model, max_output_tokens, *, scope=None):
        # Lock and append under the same transaction as the cap check.
        if type(max_output_tokens) is not int or max_output_tokens <= 0:
            raise ValueError("A positive request output allowance is required")
        self._cost(model, 0, 0, 0)
        from datetime import datetime, timezone
        from uuid import uuid4
        with self._ledger() as (stream, rows):
            if len(self._calls(rows[1:])) >= self.max_calls:
                raise SubscriptionCallLimitError("Subscription call limit reached; resume without resetting the cap")
            token = str(uuid4())
            self._append(stream, {"kind": "reserve", "id": token, "model": model,
                "max_output_tokens": max_output_tokens, "nano_usd": 0, "reserved_nano_usd": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(), "scope": scope})
        return token

    def for_scope(self, case_id, repetition, role, *, run_id=None, fingerprint=None):
        from scripts.agent_eval_call_budget import _ScopedBudget
        class Scope(_ScopedBudget):
            def before_request(self, model, llm_request):
                # Native Gemini token counting would itself bill an API. The
                # subscription provider enforces its catalog's context limit.
                self.owner._cost(model, 0, 0, 0)
        scope = {"case_id": case_id, "repetition": repetition, "role": role}
        if run_id is not None or fingerprint is not None:
            if not run_id or not fingerprint:
                raise ValueError("Native qualification scope requires both attempt and source identities")
            scope.update(run_id=run_id, fingerprint=fingerprint)
        return Scope(self, scope)

    def snapshot(self):
        return {**super().snapshot(), "funding": "chatgpt-plan", "max_calls": self.max_calls,
                "api_spend_usd": 0}
