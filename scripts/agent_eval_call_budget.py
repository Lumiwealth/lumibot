"""Durable, fail-closed per-inference budget for the release eval runner.

Unsettled calls retain their full reservation after timeout or process death.
This ledger contains model/usage/cost metadata only, never prompts or secrets.
"""

import fcntl
import json
import math
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from uuid import uuid4

_LOCK = threading.RLock()


def _nano_usd(value):
    return int((Decimal(str(value)) * 1_000_000_000).to_integral_value(rounding=ROUND_CEILING))


class EvalCallBudget:
    def __init__(self, path, *, cap_usd, prices, max_input_tokens):
        if not math.isfinite(cap_usd) or cap_usd <= 0:
            raise ValueError("Eval spending limit must be finite and positive.")
        if type(max_input_tokens) is not int or max_input_tokens <= 0:
            raise ValueError("A positive model input-token bound is required.")
        for rate in prices.values():
            if set(rate) != {"input", "cached_input", "output"} or any(
                not math.isfinite(value) or value < 0 for value in rate.values()
            ):
                raise ValueError("Invalid model pricing; no inference is allowed.")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.prices = prices
        from scripts.agent_eval_rate_pacing import InputPacer

        self.input_pacer = InputPacer(self.path.with_name("input_rate.jsonl"))
        self.header = {
            "kind": "budget",
            "version": 1,
            "cap_nano_usd": _nano_usd(cap_usd),
            "prices": prices,
            "max_input_tokens": max_input_tokens,
        }
        with self._ledger() as (stream, rows):
            if not rows:
                self._append(stream, self.header)
            elif rows[0] != self.header:
                raise ValueError("Eval ledger cap or pricing contract changed; do not reset release spending.")
            self._calls(rows[1:])

    @contextmanager
    def _ledger(self):
        # flock covers other processes; the RLock also covers threads/platforms
        # where locks are shared within one process.
        with _LOCK, self.path.open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            try:
                stream.seek(0)
                try:
                    rows = [json.loads(line) for line in stream if line.strip()]
                except (ValueError, TypeError) as exc:
                    raise ValueError("Corrupt eval call ledger; spending cannot be reconstructed.") from exc
                if rows and rows[0] != self.header:
                    raise ValueError("Eval ledger cap or pricing contract changed.")
                yield stream, rows
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    @staticmethod
    def _append(stream, row):
        import os

        stream.seek(0, 2)
        stream.write(json.dumps(row, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())

    @staticmethod
    def _calls(rows):
        calls = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("kind") not in {"reserve", "settle"}:
                raise ValueError("Invalid eval call ledger event.")
            token = row.get("id")
            cost = row.get("nano_usd")
            if not isinstance(token, str) or type(cost) is not int or cost < 0:
                raise ValueError("Invalid eval call ledger identity or cost.")
            if row["kind"] == "reserve":
                if token in calls:
                    raise ValueError("Duplicate eval ledger reservation.")
                calls[token] = row
            else:
                if token not in calls or calls[token]["kind"] == "settle":
                    raise ValueError("Unlinked eval ledger settlement.")
                calls[token] = {**calls[token], **row}
        return calls

    def _cost(self, model, inputs, cached, outputs):
        if model not in self.prices:
            raise ValueError(f"Unknown eval model pricing: {model}")
        rate = self.prices[model]
        amount = (
            Decimal(str(rate["input"])) * (inputs - cached)
            + Decimal(str(rate["cached_input"])) * cached
            + Decimal(str(rate["output"])) * outputs
        ) / 1_000_000
        return _nano_usd(amount)

    def for_scope(self, case_id, repetition, role):
        return _ScopedBudget(self, {"case_id": case_id, "repetition": repetition, "role": role})

    def reserve(self, model, max_output_tokens, *, scope=None):
        if type(max_output_tokens) is not int or max_output_tokens <= 0:
            raise ValueError("A finite positive output-token limit is required.")
        maximum = self._cost(model, self.header["max_input_tokens"], 0, max_output_tokens)
        with self._ledger() as (stream, rows):
            calls = self._calls(rows[1:])
            if any(row["nano_usd"] > row["reserved_nano_usd"] for row in calls.values()):
                raise ValueError("Provider usage exceeded a spending limit reservation; reconcile before continuing.")
            if sum(row["nano_usd"] for row in calls.values()) + maximum > self.header["cap_nano_usd"]:
                raise ValueError("Eval spending limit reached before model call; resume without resetting the cap.")
            token = str(uuid4())
            self._append(
                stream,
                {
                    "kind": "reserve",
                    "id": token,
                    "model": model,
                    "max_output_tokens": max_output_tokens,
                    "nano_usd": maximum,
                    "reserved_nano_usd": maximum,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "scope": scope,
                },
            )
        return token

    def settle(self, token, usage):
        fields = {
            "input_tokens": "prompt_token_count",
            "cached_input_tokens": "cached_content_token_count",
            "output_tokens": "candidates_token_count",
            "thinking_tokens": "thoughts_token_count",
        }
        if not isinstance(usage, dict) or not any(key in usage for key in ("input_tokens", "prompt_token_count")):
            raise ValueError("Missing authoritative usage; keep the full eval reservation.")
        counts = {key: usage.get(key, usage.get(alias, 0)) or 0 for key, alias in fields.items()}
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("Invalid usage; keep the full eval reservation.")
        if counts["cached_input_tokens"] > counts["input_tokens"]:
            raise ValueError("Cached usage exceeds input; keep the full eval reservation.")
        with self._ledger() as (stream, rows):
            calls = self._calls(rows[1:])
            previous = calls.get(token)
            if previous is None:
                raise ValueError("Unknown eval reservation.")
            cost = self._cost(
                previous["model"],
                counts["input_tokens"],
                counts["cached_input_tokens"],
                counts["output_tokens"] + counts["thinking_tokens"],
            )
            if previous["kind"] == "settle":
                if previous["usage"] != counts:
                    raise ValueError("Conflicting usage for a settled eval call.")
                return
            self._append(
                stream,
                {
                    "kind": "settle",
                    "id": token,
                    "nano_usd": cost,
                    "usage": counts,
                    "settled_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if cost > previous["nano_usd"]:
                raise ValueError("Provider usage exceeded its reserved spending limit; further inference must stop.")

    def snapshot(self):
        with self._ledger() as (_, rows):
            calls = self._calls(rows[1:])
            return {
                "committed_usd": sum(row["nano_usd"] for row in calls.values()) / 1_000_000_000,
                "settled_calls": sum(row["kind"] == "settle" for row in calls.values()),
                "reserved_calls": sum(row["kind"] == "reserve" for row in calls.values()),
                "usage": {
                    key: sum(row.get("usage", {}).get(key, 0) for row in calls.values())
                    for key in ("input_tokens", "cached_input_tokens", "output_tokens", "thinking_tokens")
                },
            }

    @property
    def committed_usd(self):
        return self.snapshot()["committed_usd"]


class _ScopedBudget:
    def __init__(self, owner, scope):
        self.owner = owner
        self.scope = scope

    def reserve(self, model, max_output_tokens):
        return self.owner.reserve(model, max_output_tokens, scope=self.scope)

    def settle(self, token, usage):
        return self.owner.settle(token, usage)

    def before_request(self, model, llm_request):
        from scripts.agent_eval_rate_pacing import count_native_request

        self.owner.input_pacer.admit(model, count_native_request(model, llm_request))
