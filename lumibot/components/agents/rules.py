"""Load BotSpot-compatible strategy rules for every LumiBot agent call."""

from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RULES_FILE_NAME = "rules.json"
RULES_ALLOWED_STATUSES = {"active", "disabled", "deleted"}
RULES_FORBIDDEN_TOP_LEVEL_KEYS = {"audit", "overall", "verdicts"}
RULES_FORBIDDEN_RULE_KEYS = {
    "check",
    "verdict",
    "overall",
    "max_entries_per_day",
    "min_entry_credit_usd",
    "min_premium",
}


class StrategyRulesError(ValueError):
    """Raised before a model call when a discovered rules ledger is invalid."""


@dataclass(frozen=True)
class StrategyRulesSnapshot:
    """Validated active rules and provenance safe to expose to the model."""

    document: dict[str, Any]
    content_hash: str | None
    source: str
    file_name: str | None

    def runtime_context(self) -> dict[str, Any]:
        return {
            "document": self.document,
            "content_hash": self.content_hash,
            "source": self.source,
            "file_name": self.file_name,
        }


def _nested_check_path(value: Any, trail: str) -> str | None:
    if isinstance(value, list):
        for index, child in enumerate(value):
            nested = _nested_check_path(child, f"{trail}[{index}]")
            if nested:
                return nested
        return None
    if not isinstance(value, dict):
        return None
    for key, child in value.items():
        if str(key).lower() == "check":
            return f"{trail}.{key}"
        nested = _nested_check_path(child, f"{trail}.{key}")
        if nested:
            return nested
    return None


def validate_rules_document(value: Any) -> dict[str, Any]:
    """Validate the canonical BotSpot plain-English rules ledger contract."""
    if not isinstance(value, dict):
        raise StrategyRulesError("Invalid rules.json: the file must contain a JSON object")
    if not isinstance(value.get("version"), (int, float)) or isinstance(value.get("version"), bool):
        raise StrategyRulesError("Invalid rules.json: version must be a number")
    forbidden_top_level = sorted(RULES_FORBIDDEN_TOP_LEVEL_KEYS.intersection(value))
    if forbidden_top_level:
        joined = ", ".join(forbidden_top_level)
        raise StrategyRulesError(
            f"Invalid rules.json: audit outcome fields are not allowed in the rules ledger: {joined}"
        )
    rules = value.get("rules")
    if not isinstance(rules, list):
        raise StrategyRulesError("Invalid rules.json: rules must be an array")

    seen_ids: set[str] = set()
    for index, rule in enumerate(rules):
        label = f"rules[{index}]"
        if not isinstance(rule, dict):
            raise StrategyRulesError(f"Invalid rules.json: {label} must be an object")
        rule_id = rule.get("id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            raise StrategyRulesError(f"Invalid rules.json: {label}.id must be a non-empty string")
        if rule_id in seen_ids:
            raise StrategyRulesError(f"Invalid rules.json: {label}.id duplicates an earlier rule id: {rule_id}")
        seen_ids.add(rule_id)
        if rule.get("status") not in RULES_ALLOWED_STATUSES:
            raise StrategyRulesError(
                f"Invalid rules.json: {label}.status must be one of active, disabled, deleted"
            )
        interpretation = rule.get("interpretation")
        if not isinstance(interpretation, str) or not interpretation.strip():
            raise StrategyRulesError(
                f"Invalid rules.json: {label}.interpretation must be a non-empty plain English string"
            )
        title = rule.get("title")
        if title is not None and (not isinstance(title, str) or not title.strip()):
            raise StrategyRulesError(f"Invalid rules.json: {label}.title must be a non-empty string when present")
        raw_quote = rule.get("raw_quote")
        if raw_quote is not None and not isinstance(raw_quote, str):
            raise StrategyRulesError(f"Invalid rules.json: {label}.raw_quote must be a string when present")
        forbidden_rule_keys = sorted(RULES_FORBIDDEN_RULE_KEYS.intersection(rule))
        if forbidden_rule_keys:
            joined = ", ".join(forbidden_rule_keys)
            raise StrategyRulesError(
                f"Invalid rules.json: {label} contains machine checks or verdict fields: {joined}"
            )
        nested_check = _nested_check_path(rule, label)
        if nested_check:
            raise StrategyRulesError(
                f"Invalid rules.json: {nested_check} is not allowed in the plain-English rules ledger"
            )
    return value


def resolve_rules_path(strategy: Any, explicit_path: str | Path | None = None) -> Path | None:
    """Resolve rules without searching unrelated working directories."""
    candidate = explicit_path
    if candidate is None:
        candidate = getattr(strategy, "rules_path", None)
    if candidate is not None:
        return Path(candidate).expanduser().resolve()

    try:
        source_path = inspect.getsourcefile(strategy.__class__) or inspect.getfile(strategy.__class__)
    except (OSError, TypeError):
        source_path = None
    if not source_path or str(source_path).startswith("<"):
        return None
    return Path(source_path).resolve().with_name(RULES_FILE_NAME)


def load_strategy_rules(
    strategy: Any,
    explicit_path: str | Path | None = None,
) -> StrategyRulesSnapshot:
    """Load and reduce the ledger to active rules for one agent call."""
    path = resolve_rules_path(strategy, explicit_path)
    if path is None or not path.exists():
        return StrategyRulesSnapshot(
            document={"version": 1, "rules": []},
            content_hash=None,
            source="missing",
            file_name=None,
        )
    if not path.is_file():
        raise StrategyRulesError(f"Invalid rules.json: expected a file at {path.name}")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise StrategyRulesError(f"Could not read {path.name}: {exc}") from exc
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StrategyRulesError(f"Invalid rules.json: not valid UTF-8 JSON ({exc})") from exc

    document = validate_rules_document(parsed)
    active_document = {
        "version": document["version"],
        "rules": [rule for rule in document["rules"] if rule.get("status") == "active"],
    }
    return StrategyRulesSnapshot(
        document=active_document,
        content_hash=hashlib.sha256(raw).hexdigest(),
        source="rules.json",
        file_name=path.name,
    )
