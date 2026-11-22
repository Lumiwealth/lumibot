"""Optional Drex answers for decision-only agents (``NACE_API_KEY``).

Drex (https://console.nace.ai/docs/drex) is a decision model: it reads a state
and returns a probability for each label of a typed question, never free text.
An agent qualifies when it has no tools or skills and every ``output_schema``
property is an unconstrained string enum or boolean. Drex then answers instead of the agent's model, and
the agent's model stays the fallback for any Drex error.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx

from lumibot import __version__

from .runtime import GoogleADKRuntime, _utc_iso_timestamp
from .schemas import AgentRunResult, AgentTraceEvent

# Drex answers each property on its own, so object-level constraints (oneOf, not, ...) go to the agent's model.
_OBJECT_KEYWORDS = {
    "$schema", "$defs", "type", "title", "description", "properties", "required", "additionalProperties",
}
# Drex picks any label in the enum, so value constraints (const, pattern, ...) go to the agent's model too.
_PROPERTY_KEYWORDS = {"$ref", "type", "title", "description", "default", "enum"}


def drex_model() -> str:
    return os.environ.get("NACE_DEFAULT_DECISION_MODEL") or "drex-max-finance"


def drex_questions(schema_json: dict[str, Any], system_prompt: str) -> dict[str, Any] | None:
    """Map a bounded output schema to Drex questions, or None when Drex is off or cannot answer it."""
    properties = schema_json.get("properties")
    if not os.environ.get("NACE_API_KEY") or not isinstance(properties, dict) or not properties:
        return None
    if not set(schema_json) <= _OBJECT_KEYWORDS:
        return None
    required = schema_json.get("required", [])
    if not isinstance(required, list) or any(not isinstance(name, str) or name not in properties for name in required):
        return None
    defs = schema_json.get("$defs") or {}
    questions: dict[str, Any] = {}
    for name, prop in properties.items():
        if not isinstance(prop, dict):
            return None
        if "$ref" in prop:
            # Pydantic keeps an Enum field's labels in $defs.
            prop = {**defs.get(str(prop["$ref"]).removeprefix("#/$defs/"), {}), **prop}
        if not set(prop) <= _PROPERTY_KEYWORDS:
            return None
        instructions = f"{system_prompt.strip()}\n\n{prop.get('description') or name}".strip()
        enum = prop.get("enum")
        if isinstance(enum, list) and enum and all(isinstance(label, str) for label in enum):
            questions[name] = {"type": "choice", "instructions": instructions, "criteria": dict.fromkeys(enum)}
        elif prop.get("type") == "boolean" and "enum" not in prop:
            questions[name] = {"type": "noul", "instructions": instructions}
        else:
            return None
    return questions


def _answer(question: dict[str, Any], answer: dict[str, Any]) -> str | bool:
    if question["type"] == "choice":
        if answer["choice"] not in question["criteria"]:
            raise ValueError(f"Drex returned an unknown label {answer['choice']!r}")
        return answer["choice"]
    probability = float(answer["noul"])
    if not 0.0 <= probability <= 1.0:
        raise ValueError(f"Drex returned an invalid probability {answer['noul']!r}")
    return probability >= 0.5


def run_drex_decision(request: Any, questions: dict[str, Any]) -> AgentRunResult:
    # The state is the same user message the agent's model would receive.
    state = GoogleADKRuntime._build_user_text(request)
    base_url = os.environ.get("NACE_BASE_URL") or "https://console.nace.ai"
    client = f"lumibot/{__version__}"
    headers = {"Authorization": f"Bearer {os.environ['NACE_API_KEY']}", "X-Nace-Client": client, "User-Agent": client}
    request_timeout = GoogleADKRuntime._model_request_timeout_seconds_for_request(request)
    run_timeout = GoogleADKRuntime._run_timeout_seconds_for_request(request)
    # HTTPX timeouts bound inactivity; the outer deadline also stops a body
    # that keeps sending data. Cancellation exits and closes the async client.
    timeouts = [
        timeout
        for timeout in (request_timeout, run_timeout)
        if timeout is not None
    ]

    async def post():
        async with httpx.AsyncClient() as client:
            return await client.post(
                f"{base_url.rstrip('/')}/v1/systemone",
                headers=headers,
                json={"model": drex_model(), "state": state, "questions": questions},
                timeout=min(timeouts) if timeouts else None,
            )

    # Match the synchronous GoogleADKRuntime.run -> asyncio.run contract.
    response = asyncio.run(asyncio.wait_for(post(), timeout=run_timeout))
    response.raise_for_status()
    body = response.json()
    answers = body["answers"]
    answer = {name: _answer(question, answers[name]) for name, question in questions.items()}
    summary = json.dumps(answer, sort_keys=True)
    # The probabilities stay in the trace so a strategy can audit or re-threshold a verdict.
    event = AgentTraceEvent(
        kind="text",
        text=summary,
        payload={"drex_answers": answers, "request_id": body.get("request_id")},
        timestamp=_utc_iso_timestamp(),
    )
    return AgentRunResult(summary=summary, model=f"nace/{drex_model()}", events=[event], usage=body.get("usage"))
