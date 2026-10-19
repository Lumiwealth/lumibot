"""Nonrenewable caller-owned plan credentials with durable per-request admission.

This is a client-side usage boundary, not an OpenAI-issued scoped credential.
The host supplies a previously authorized OAuth token through its secret store;
this module neither exports credentials nor starts jobs or browser sign-in.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from filelock import FileLock

from .chatgpt_auth import PLAN_SCOPE, read_private


def is_run_profile(path: Path) -> bool:
    # Missing files still fail in the existing access_token owner. This permits
    # transport contract tests that replace that owner without a credential file.
    return path.exists() and read_private(path).get("credential_kind") == "run"


def run_profile(path: Path, *, require_current: bool = True) -> dict:
    value = read_private(path)
    if value.get("credential_kind") != "run":
        raise ValueError("Not a ChatGPT run credential")
    if "refresh_token" in value:
        raise ValueError("Run credentials must not include a refresh token")
    scopes = value.get("scopes")
    if (value.get("status") != "connected" or not isinstance(scopes, list)
            or PLAN_SCOPE not in scopes):
        raise ValueError("Run credential has no ChatGPT plan permission")
    token = value.get("access_token")
    if not isinstance(token, str) or not token.strip() or token.startswith("sk-"):
        raise ValueError("Run credentials require an authorized OAuth access token, never an API key")
    expiry = value.get("expires_at")
    if type(expiry) not in (int, float) or not math.isfinite(expiry):
        raise ValueError("Run credential requires a finite expiry")
    if require_current and expiry <= time.time():
        raise RuntimeError("ChatGPT run credential expired; no refresh or API fallback")
    if expiry - time.time() > 3600:
        raise ValueError("Run credential may be valid for at most one hour")
    if not isinstance(value.get("run_id"), str) or not value["run_id"].strip():
        raise ValueError("Run credential requires a run identity")
    model = value.get("model")
    if not isinstance(model, str) or not model.strip() or "/" in model:
        raise ValueError("Run credential requires one explicit model slug")
    limits = value.get("limits")
    if (not isinstance(limits, dict) or set(limits) != {"calls", "input_tokens", "output_tokens"}
            or any(type(n) is not int or n <= 0 for n in limits.values())):
        raise ValueError("Run credential requires finite positive call and token limits")
    return value


class RunBudget:
    """One immutable owner policy; unknown requests retain their full envelope."""

    def __init__(self, profile: Path):
        self.profile = Path(profile)
        value = run_profile(self.profile, require_current=False)
        self.path = self.profile.with_suffix(".usage.jsonl")
        self.header = {
            "kind": "chatgpt-run-ledger", "version": 1, "run_id": value["run_id"],
            "model": value["model"], "limits": value["limits"], "service_tier": "default",
            "credential_fingerprint": hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest(),
        }
        with self._ledger():
            pass

    @contextmanager
    def _ledger(self, *, require_current: bool = False):
        # Validate freshness/permissions even when a long-lived adapter is reused.
        value = run_profile(self.profile, require_current=require_current)
        fingerprint = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
        if fingerprint != self.header["credential_fingerprint"]:
            raise ValueError("Run credential contract changed; prior usage must not reset")
        with FileLock(str(self.path) + ".lock", mode=0o600, timeout=30):
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self.path, flags, 0o600)
            with os.fdopen(descriptor, "r+", encoding="utf-8") as stream:
                if os.name == "posix" and os.fstat(stream.fileno()).st_mode & 0o077:
                    raise ValueError("Run usage ledger must be owner-only")
                rows = [json.loads(line) for line in stream]
                if not rows:
                    self._append(stream, self.header)
                    rows = [self.header]
                if rows[0] != self.header:
                    raise ValueError("Run credential contract differs from the saved ledger")
                yield stream, rows[1:]

    @staticmethod
    def _append(stream, row):
        stream.seek(0, os.SEEK_END)
        stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())

    @staticmethod
    def _calls(rows):
        calls = {}
        for row in rows:
            ticket = row.get("ticket")
            if row.get("kind") == "reserve" and ticket and ticket not in calls:
                if any(type(row.get(k)) is not int or row[k] <= 0 for k in ("input_bound", "output_bound")):
                    raise ValueError("Invalid run reservation; preserve the ledger")
                calls[ticket] = row
            elif row.get("kind") == "settle" and ticket in calls and calls[ticket]["kind"] == "reserve":
                RunBudget._usage(row.get("usage"), calls[ticket])
                calls[ticket] = row
            else:
                raise ValueError("Invalid run ledger; preserve unknown usage")
        return calls

    @staticmethod
    def _usage(usage, reservation):
        if not isinstance(usage, dict):
            raise ValueError("Missing authoritative run usage")
        inputs, outputs = usage.get("input_tokens"), usage.get("output_tokens")
        details = usage.get("input_tokens_details") or {}
        cached = details.get("cached_tokens", 0) if isinstance(details, dict) else None
        if (any(type(n) is not int or n < 0 for n in (inputs, outputs, cached))
                or cached > inputs or inputs > reservation["input_bound"]
                or outputs > reservation["output_bound"]):
            raise ValueError("Invalid or excessive run usage; preserve reservation")
        # Responses output_tokens already includes reasoning_tokens.
        return {"input_tokens": inputs, "output_tokens": outputs,
                "input_tokens_details": {"cached_tokens": cached}}

    def _snapshot(self, rows):
        calls = self._calls(rows)
        state = {"calls": len(calls), "input_tokens": 0, "cached_input_tokens": 0,
                 "output_tokens": 0, "reserved_input_tokens": 0, "reserved_output_tokens": 0}
        for row in calls.values():
            if row["kind"] == "reserve":
                state["reserved_input_tokens"] += row["input_bound"]
                state["reserved_output_tokens"] += row["output_bound"]
            else:
                state["input_tokens"] += row["usage"]["input_tokens"]
                state["output_tokens"] += row["usage"]["output_tokens"]
                state["cached_input_tokens"] += row["usage"]["input_tokens_details"]["cached_tokens"]
        return state

    def snapshot(self):
        with self._ledger() as (_, rows):
            return self._snapshot(rows)

    def reserve(self, model: str, *, input_bound: int, output_bound: int) -> str:
        if model != self.header["model"]:
            raise ValueError("Run model differs from the authorized model")
        if any(type(n) is not int or n <= 0 for n in (input_bound, output_bound)):
            raise ValueError("Run requests require known provider token bounds")
        with self._ledger(require_current=True) as (stream, rows):
            state, limits = self._snapshot(rows), self.header["limits"]
            if state["calls"] >= limits["calls"]:
                raise RuntimeError("ChatGPT run call limit reached before request")
            if (state["input_tokens"] + state["reserved_input_tokens"] + input_bound > limits["input_tokens"]
                    or state["output_tokens"] + state["reserved_output_tokens"] + output_bound > limits["output_tokens"]):
                raise RuntimeError("ChatGPT run token limit reached before request")
            ticket = str(uuid4())
            self._append(stream, {"kind": "reserve", "ticket": ticket,
                "input_bound": input_bound, "output_bound": output_bound, "at": time.time()})
            return ticket

    def settle(self, ticket: str, usage: dict):
        with self._ledger() as (stream, rows):
            calls = self._calls(rows)
            reservation = calls.get(ticket)
            if reservation is None:
                raise ValueError("Unknown run reservation")
            if reservation["kind"] == "settle":
                if reservation["usage"] != {"input_tokens": usage.get("input_tokens"),
                        "output_tokens": usage.get("output_tokens"),
                        "input_tokens_details": {"cached_tokens": (usage.get("input_tokens_details") or {}).get("cached_tokens", 0)}}:
                    raise ValueError("Run usage conflicts with prior receipt")
                return
            measured = self._usage(usage, reservation)
            self._append(stream, {"kind": "settle", "ticket": ticket, "usage": measured, "at": time.time()})
