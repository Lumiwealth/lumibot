"""Structured output for AI agent runs (``output_schema=`` / ``result.parsed``).

Added 2026-10-07 after a customer investigation: strategies asked an
agent for a PASS/VETO verdict and had to parse free text, because
``result.payload`` is run bookkeeping, not the answer. One GPT-4o verdict came
back wrapped in a markdown code fence and broke the strategy's parser.

How it works (provider-agnostic, so it also works with tools on every model):

1. The schema is appended to the agent's system prompt as an exact final-answer
   format instruction.
2. After the run, the final answer text is parsed: code fences are removed and
   the first complete JSON value is extracted.
3. The value is validated against the JSON Schema (or the pydantic model), and
   stored on ``result.parsed``. A mismatch leaves ``parsed`` as None, sets
   ``result.parse_error`` and adds a ``structured_output_invalid`` warning. No
   extra model call is made.
"""

from __future__ import annotations

import json
import re
from typing import Any

_FENCE_PATTERN = re.compile(r"```(?:json|JSON)?\s*(.*?)```", re.DOTALL)


def normalize_output_schema(schema: Any) -> tuple[dict[str, Any], Any | None]:
    """Return ``(json_schema, pydantic_model_or_None)`` for a user schema.

    Accepts a JSON Schema ``dict`` or a pydantic ``BaseModel`` subclass.
    """
    if schema is None:
        raise ValueError("output_schema is None")
    model_json_schema = getattr(schema, "model_json_schema", None)
    if isinstance(schema, type) and callable(model_json_schema):
        return dict(model_json_schema()), schema
    if isinstance(schema, dict):
        if not schema:
            raise ValueError("output_schema must not be empty.")
        # Round-trip to make sure it is plain JSON (and stable for cache keys).
        return json.loads(json.dumps(schema, sort_keys=True)), None
    raise TypeError("output_schema must be a JSON Schema dict or a pydantic BaseModel class.")


def structured_output_instruction(json_schema: dict[str, Any]) -> str:
    return (
        "FINAL ANSWER FORMAT (required): after any tool calls, reply with exactly one JSON value "
        "that matches this JSON Schema. Output only the JSON: no markdown code fences, no prose "
        "before or after it.\n"
        f"JSON Schema: {json.dumps(json_schema, sort_keys=True)}"
    )


def _extract_json_value(text: str) -> Any:
    candidates: list[str] = []
    stripped = text.strip()
    candidates.append(stripped)
    candidates.extend(match.strip() for match in _FENCE_PATTERN.findall(text))
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except (TypeError, ValueError):
            pass
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char not in "{[":
            continue
        try:
            value, _end = decoder.raw_decode(text[index:])
        except ValueError:
            continue
        return value
    raise ValueError("the final answer contained no JSON value")


_JSON_TYPES = {
    "object": dict,
    "array": list,
    "string": str,
    "boolean": bool,
    "null": type(None),
}


def _type_matches(value: Any, expected: str) -> bool:
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    python_type = _JSON_TYPES.get(expected)
    return True if python_type is None else isinstance(value, python_type)


def _minimal_validate(value: Any, schema: dict[str, Any], path: str = "$") -> None:
    """Small fallback validator used only when ``jsonschema`` is not installed."""
    expected = schema.get("type")
    if isinstance(expected, str) and not _type_matches(value, expected):
        raise ValueError(f"{path} should be {expected}")
    if isinstance(expected, list) and not any(_type_matches(value, item) for item in expected):
        raise ValueError(f"{path} should be one of {expected}")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{path} must be one of {schema['enum']}")
    if isinstance(value, dict):
        for key in schema.get("required", []) or []:
            if key not in value:
                raise ValueError(f"{path} is missing required property {key!r}")
        for key, sub_schema in (schema.get("properties") or {}).items():
            if key in value and isinstance(sub_schema, dict):
                _minimal_validate(value[key], sub_schema, f"{path}.{key}")
    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for index, item in enumerate(value):
            _minimal_validate(item, schema["items"], f"{path}[{index}]")


def parse_structured_output(text: str | None, json_schema: dict[str, Any], model_class: Any | None) -> tuple[Any, str | None]:
    """Parse and validate the final answer. Returns ``(parsed, error)``."""
    if not isinstance(text, str) or not text.strip():
        return None, "the agent returned no final answer text"
    try:
        value = _extract_json_value(text)
    except ValueError as exc:
        return None, str(exc)
    if model_class is not None:
        try:
            return model_class.model_validate(value), None
        except Exception as exc:  # pydantic.ValidationError
            return None, f"answer did not match {getattr(model_class, '__name__', 'the model')}: {exc}"[:800]
    try:
        import jsonschema  # type: ignore[import-not-found]
    except ImportError:
        jsonschema = None
    try:
        if jsonschema is not None:
            jsonschema.validate(value, json_schema)
        else:
            _minimal_validate(value, json_schema)
    except Exception as exc:
        message = getattr(exc, "message", None) or str(exc)
        return None, f"answer did not match output_schema: {message}"[:800]
    return value, None
