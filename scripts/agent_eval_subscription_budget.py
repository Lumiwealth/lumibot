"""Durable subscription-only usage ledger. No API prices or API credentials.

Incomplete requests still consume the call allowance on resume. The provider
owns its plan quota; a local call cap bounds each qualification attempt.
"""

from scripts.agent_eval_call_budget import EvalCallBudget

MAX_SUBSCRIPTION_QUALIFICATION_CALLS = 1500


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
        with self._ledger() as (stream, rows):
            if not rows:
                self._append(stream, self.header)
            elif rows[0] != self.header:
                raise ValueError("Subscription ledger contract changed")
            self._calls(rows[1:])

    def _cost(self, model, inputs, cached, outputs):
        if not str(model).startswith("chatgpt/"):
            raise ValueError("Eval inference requires subscription funding; API fallback is forbidden")
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
