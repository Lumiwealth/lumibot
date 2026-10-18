"""Durable subscription-only call/token admission; no API credentials or billing.

Unknown usage keeps its full provider envelope across interruptions. API-equivalent
upper bounds are estimates, never observed ChatGPT credits or actual charges.
"""

import json
from datetime import datetime, timezone

from scripts.agent_eval_call_budget import EvalCallBudget

MAX_SUBSCRIPTION_QUALIFICATION_CALLS = 5000
# Standard Luna rates verified 2026-10-16; use the highest long-context and
# cache-write input rate conservatively, independent of actual cache behavior.
# https://developers.openai.com/api/docs/pricing
# 60M input keeps the worst case ($15 input + $0.75 output) under the $20
# allowance, which stays the binding limit. 10M stopped the 4.6.20 release
# catalog after 36 of 90 required repetitions (2026-10-17).
MAX_SUBSCRIPTION_INPUT_TOKENS = 60_000_000
MAX_SUBSCRIPTION_OUTPUT_TOKENS = 1_000_000
SUBSCRIPTION_EQUIVALENT_ALLOWANCE_USD = 20
PROVIDER_INPUT_BOUND = 1_050_000
PROVIDER_OUTPUT_BOUND = 128_000


def subscription_token_state(calls):
    inputs = outputs = reserved_inputs = reserved_outputs = 0
    for call in calls.values():
        if call["kind"] == "reserve":
            reserved_inputs += PROVIDER_INPUT_BOUND
            reserved_outputs += PROVIDER_OUTPUT_BOUND
        else:
            usage = call.get("usage")
            fields = {"input_tokens", "cached_input_tokens", "output_tokens", "thinking_tokens"}
            if (not isinstance(usage, dict) or set(usage) != fields
                    or any(type(n) is not int or n < 0 for n in usage.values())
                    or usage["cached_input_tokens"] > usage["input_tokens"]):
                raise ValueError("Invalid authoritative subscription usage; preserve the reservation")
            # ADK candidate tokens exclude its separately reported thoughts.
            actual_output = usage["output_tokens"] + usage["thinking_tokens"]
            if usage["input_tokens"] > PROVIDER_INPUT_BOUND or actual_output > PROVIDER_OUTPUT_BOUND:
                raise ValueError("Provider exceeded the token reservation; reconcile before inference")
            inputs += usage["input_tokens"]
            outputs += actual_output
    return {"input_tokens": inputs, "output_tokens": outputs,
            "reserved_input_tokens": reserved_inputs, "reserved_output_tokens": reserved_outputs}


def equivalent_upper_bound(inputs, outputs):
    return (inputs * 0.25 + outputs * 0.75) / 1_000_000



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
        legacy_header = dict(self.header)
        self.header["token_policy"] = {
            "max_input_tokens": MAX_SUBSCRIPTION_INPUT_TOKENS,
            "max_output_tokens": MAX_SUBSCRIPTION_OUTPUT_TOKENS,
            "equivalent_usd_allowance": SUBSCRIPTION_EQUIVALENT_ALLOWANCE_USD,
            "provider_input_bound": PROVIDER_INPUT_BOUND,
            "provider_output_bound": PROVIDER_OUTPUT_BOUND,
            "rate_source": "https://developers.openai.com/api/docs/pricing",
            "rates_verified_at": "2026-10-16",
        }
        if self.path.exists() and self.path.stat().st_size:
            with self.path.open(encoding="utf-8") as stream:
                original_header = json.loads(stream.readline())
            comparable = {key: value for key, value in original_header.items() if key != "max_calls"}
            expected = self.header if "token_policy" in original_header else legacy_header
            if comparable != {key: value for key, value in expected.items() if key != "max_calls"}:
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
            calls = self._calls(rows[1:])
            if any(call.get("model") != "chatgpt/gpt-6-luna" for call in calls.values()):
                raise SubscriptionCallLimitError("Unqualified historical model pricing; reconcile the existing allowance before inference")
            if len(calls) >= self.max_calls:
                raise SubscriptionCallLimitError("Subscription call limit reached; resume without resetting the cap")
            tokens = subscription_token_state(calls)
            inputs = tokens["input_tokens"] + tokens["reserved_input_tokens"] + PROVIDER_INPUT_BOUND
            outputs = tokens["output_tokens"] + tokens["reserved_output_tokens"] + PROVIDER_OUTPUT_BOUND
            if (inputs > MAX_SUBSCRIPTION_INPUT_TOKENS or outputs > MAX_SUBSCRIPTION_OUTPUT_TOKENS
                    or equivalent_upper_bound(inputs, outputs) > SUBSCRIPTION_EQUIVALENT_ALLOWANCE_USD):
                raise SubscriptionCallLimitError("Cumulative subscription token limit reached; resume without resetting usage")
            token = str(uuid4())
            self._append(stream, {"kind": "reserve", "id": token, "model": model,
                "max_output_tokens": PROVIDER_OUTPUT_BOUND, "nano_usd": 0, "reserved_nano_usd": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(), "scope": scope})
        return token

    def settle(self, token, usage):
        # LiteLLM's OpenAI bridge maps completion_tokens (which already include
        # reasoning) into ADK candidates, then reports thoughts separately.
        # Preserve the canonical ledger's exclusive output/thinking convention.
        if isinstance(usage, dict) and "candidates_token_count" in usage and "output_tokens" not in usage:
            completion = usage["candidates_token_count"]
            thinking = usage.get("thoughts_token_count") or 0
            if (type(completion) is not int or type(thinking) is not int
                    or thinking < 0 or completion < thinking):
                raise ValueError("Invalid authoritative subscription reasoning usage")
            usage = {**usage, "output_tokens": completion - thinking, "thinking_tokens": thinking}
        super().settle(token, usage)
        self.snapshot()  # A provider-bound breach must fail this call too.

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
        # Reuse the same locked journal and parser as admission. Never count
        # unreported calls as zero tokens merely because API billing is zero.
        with self._ledger() as (_, rows):
            calls = self._calls(rows[1:])
            tokens = subscription_token_state(calls)
            unpriced = sorted({str(call.get("model")) for call in calls.values()
                               if call.get("model") != "chatgpt/gpt-6-luna"})
            return {"committed_usd": 0, "funding": "chatgpt-plan", "max_calls": self.max_calls,
                    "settled_calls": sum(c["kind"] == "settle" for c in calls.values()),
                    "reserved_calls": sum(c["kind"] == "reserve" for c in calls.values()),
                    "usage": {key: sum(c.get("usage", {}).get(key, 0) for c in calls.values())
                              for key in ("input_tokens", "cached_input_tokens", "output_tokens", "thinking_tokens")},
                    **{key: value for key, value in tokens.items() if key.startswith("reserved_")},
                    "max_total_input_tokens": MAX_SUBSCRIPTION_INPUT_TOKENS,
                    "max_total_output_tokens": MAX_SUBSCRIPTION_OUTPUT_TOKENS,
                    "equivalent_usd_upper_bound": equivalent_upper_bound(
                        tokens["input_tokens"] + tokens["reserved_input_tokens"],
                        tokens["output_tokens"] + tokens["reserved_output_tokens"]) if not unpriced else None,
                    "unpriced_historical_models": unpriced,
                    "equivalent_usd_allowance": SUBSCRIPTION_EQUIVALENT_ALLOWANCE_USD,
                    "rate_source": "https://developers.openai.com/api/docs/pricing",
                    "rates_verified_at": "2026-10-16",
                    "api_spend_usd": 0, "actual_subscription_credit_debit": None}
