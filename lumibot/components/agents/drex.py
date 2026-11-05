"""Optional Drex answers for decision-only agents (``NACE_API_KEY``).

Drex (https://console.nace.ai/docs/drex) is a decision model: it reads a state
and returns a probability for each label of a typed question, never free text.
An agent qualifies when it has no tools and every ``output_schema`` property is
a string enum or a boolean. Drex then answers instead of the agent's model, and
the agent's model stays the fallback for any Drex error.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from lumibot import __version__

from .runtime import GoogleADKRuntime, _utc_iso_timestamp
from .schemas import AgentRunResult, AgentTraceEvent


def drex_model() -> str:
    return os.environ.get("NACE_DEFAULT_DECISION_MODEL") or "drex-max-finance"


def drex_questions(schema_json: dict[str, Any], system_prompt: str) -> dict[str, Any] | None:
    """Map a bounded output schema to Drex questions, or None when Drex is off or cannot answer it."""
    properties = schema_json.get("properties")
    if not os.environ.get("NACE_API_KEY") or not isinstance(properties, dict) or not properties:
        return None
    questions: dict[str, Any] = {}
    for name, prop in properties.items():
        if not isinstance(prop, dict):
            return None
        instructions = f"{system_prompt.strip()}\n\n{prop.get('description') or name}".strip()
        enum = prop.get("enum")
        if isinstance(enum, list) and enum and all(isinstance(label, str) for label in enum):
            questions[name] = {"type": "choice", "instructions": instructions, "criteria": dict.fromkeys(enum)}
        elif prop.get("type") == "boolean":
            questions[name] = {"type": "noul", "instructions": instructions}
        else:
            return None
    return questions


def run_drex_decision(request: Any, questions: dict[str, Any]) -> AgentRunResult:
    # The state is the same user message the agent's model would receive.
    state = GoogleADKRuntime._build_user_text(request)
    base_url = os.environ.get("NACE_BASE_URL") or "https://console.nace.ai"
    client = f"lumibot/{__version__}"
    headers = {"Authorization": f"Bearer {os.environ['NACE_API_KEY']}", "X-Nace-Client": client, "User-Agent": client}
    response = httpx.post(
        f"{base_url.rstrip('/')}/v1/systemone",
        headers=headers,
        json={"model": drex_model(), "state": state, "questions": questions},
        timeout=request.model_request_timeout_seconds or 60.0,
    )
    response.raise_for_status()
    body = response.json()
    answers = body["answers"]
    answer = {
        name: answers[name]["choice"] if question["type"] == "choice" else answers[name]["noul"] >= 0.5
        for name, question in questions.items()
    }
    summary = json.dumps(answer, sort_keys=True)
    # The probabilities stay in the trace so a strategy can audit or re-threshold a verdict.
    event = AgentTraceEvent(
        kind="text",
        text=summary,
        payload={"drex_answers": answers, "request_id": body.get("request_id")},
        timestamp=_utc_iso_timestamp(),
    )
    return AgentRunResult(summary=summary, model=f"nace/{drex_model()}", events=[event], usage=body.get("usage"))
