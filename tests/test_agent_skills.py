import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent
from lumibot.components.agents.skills import (
    BUILTIN_SKILL_NAMES,
    build_builtin_skill_toolset,
    builtin_skill_directories,
    builtin_skill_fingerprint,
    load_builtin_skills,
)
from lumibot.components.agents.rules import StrategyRulesError, load_strategy_rules


class _Vars(dict):
    def set(self, key, value):
        self[key] = value


class _Strategy:
    is_backtesting = False

    def __init__(self):
        self.parameters = {}
        self.vars = _Vars()

    def get_datetime(self):
        return datetime(2026, 8, 11, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None


class _AccountSnapshotStrategy(_Strategy):
    def get_cash(self):
        return 100_000.0

    def get_portfolio_value(self):
        return 125_000.0

    def get_positions(self, include_cash_positions=True):
        return [
            SimpleNamespace(
                asset=SimpleNamespace(symbol=f"SYM{index:02d}", asset_type="stock"),
                quantity=index + 1,
            )
            for index in range(30)
        ]

    def get_orders(self):
        return []


class _CaptureRuntime:
    def __init__(self):
        self.requests = []

    def run(self, request):
        self.requests.append(request)
        return AgentRunResult(
            summary="Captured.",
            model=request.model,
            events=[AgentTraceEvent(kind="text", text="Captured.")],
        )


def test_builtin_agent_skills_are_packaged_and_loadable():
    directories = builtin_skill_directories()
    assert tuple(path.name for path in directories) == BUILTIN_SKILL_NAMES
    assert all((path / "SKILL.md").is_file() for path in directories)

    skills = load_builtin_skills()
    assert tuple(skill.name for skill in skills) == BUILTIN_SKILL_NAMES
    assert "broad trading mandate" in skills[0].description
    assert "broad mandate" in skills[1].description
    assert len(builtin_skill_fingerprint()) == 64


def test_options_skill_requires_atomic_multileg_or_no_trade():
    options_skill = next(skill for skill in load_builtin_skills() if skill.name == "options-trading")
    instructions = " ".join(options_skill.instructions.split())

    assert "Never submit related legs independently" in instructions
    assert "If atomic package submission is unavailable" in instructions
    assert "make a no-trade decision" in instructions


def test_stock_skill_defines_opening_range_boundaries_and_order_truth():
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    instructions = " ".join(stock_skill.instructions.split())
    intraday = " ".join(stock_skill.resources.references["intraday-setups.md"].split())

    assert "Write down the decisive condition" in instructions
    assert "call `risk_calculate_stock_quantity`" in instructions
    assert "quantity unchanged" in instructions
    assert "notional is at or below both the cap and available cash" in instructions
    assert "never say that no order was entered" in instructions
    assert "three five-minute bars starting at 09:30, 09:35, and 09:40 form the range" in intraday
    assert "starting at 09:45 is the first later candidate" in intraday
    assert "aggregate the exact non-overlapping intervals" in intraday
    assert "Never treat the first one-minute constituent" in intraday
    assert "as a completed five-minute bar" in intraday


def test_builtin_skill_toolset_exposes_progressive_loading_tools():
    toolset = build_builtin_skill_toolset()
    tools = asyncio.run(toolset.get_tools())
    assert {tool.name for tool in tools} == {
        "list_skills",
        "load_skill",
        "load_skill_resource",
        "run_skill_script",
    }


def test_agent_runtime_enables_builtin_skills_and_fingerprints_cache(monkeypatch):
    import lumibot.components.agents.skills as skills_module

    runtime = _CaptureRuntime()
    manager = AgentManager(_Strategy())
    agent = manager.create(
        name="trader",
        model="gemini-3.5-flash-lite",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    monkeypatch.setattr(skills_module, "builtin_skill_fingerprint", lambda: "a" * 64)
    agent.run(task_prompt="Consider the best available trade.")
    monkeypatch.setattr(skills_module, "builtin_skill_fingerprint", lambda: "b" * 64)
    agent.run(task_prompt="Consider the best available trade.")

    first, second = runtime.requests
    assert first.include_builtin_skills is True
    assert first.builtin_skill_fingerprint == "a" * 64
    assert second.builtin_skill_fingerprint == "b" * 64
    assert first.model_call_id != second.model_call_id
    assert first.provider_prompt_cache_key != second.provider_prompt_cache_key
    assert "load_skill" in first.system_prompt
    assert "managing any stock, ETF, or option position or related pending order" in first.system_prompt
    assert "MUST load the matching skill" in first.system_prompt
    assert "Never claim that no order was submitted" in first.system_prompt
    assert "use risk_calculate_stock_quantity" in first.system_prompt


def test_agent_can_disable_builtin_skills_explicitly():
    runtime = _CaptureRuntime()
    manager = AgentManager(_Strategy())
    agent = manager.create(
        name="plain",
        tools=[],
        include_builtin_tools=False,
        include_builtin_skills=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="Do nothing.")

    request = runtime.requests[0]
    assert request.include_builtin_skills is False
    assert request.builtin_skill_fingerprint is None
    assert "MUST load the matching skill" not in request.system_prompt


def test_rules_json_is_reloaded_and_injected_into_every_agent_call(tmp_path):
    rules_path = tmp_path / "rules.json"
    strategy = _Strategy()
    strategy.rules_path = rules_path
    runtime = _CaptureRuntime()
    agent = AgentManager(strategy).create(
        name="ruled",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    rules_path.write_text(
        json.dumps(
            {
                "version": 1,
                "rules": [
                    {
                        "id": "daily-entry",
                        "status": "active",
                        "interpretation": "Open at most one new position each day.",
                    },
                    {
                        "id": "old-rule",
                        "status": "disabled",
                        "interpretation": "This instruction no longer applies.",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    agent.run(task_prompt="First call.")

    rules_path.write_text(
        json.dumps(
            {
                "version": 1,
                "rules": [
                    {
                        "id": "daily-entry",
                        "status": "active",
                        "interpretation": "Do not open a new position today.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    agent.run(task_prompt="Second call.")

    first, second = runtime.requests
    assert first.runtime_context["strategy_rules"]["document"]["rules"][0]["interpretation"] == (
        "Open at most one new position each day."
    )
    assert "old-rule" not in first.system_prompt
    assert "Do not open a new position today." in second.system_prompt
    assert first.model_call_id != second.model_call_id
    assert first.provider_prompt_cache_key != second.provider_prompt_cache_key


def test_missing_rules_json_injects_an_empty_active_ledger():
    runtime = _CaptureRuntime()
    agent = AgentManager(_Strategy()).create(
        name="no_rules",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="Call without a rules file.")

    request = runtime.requests[0]
    assert request.runtime_context["strategy_rules"] == {
        "document": {"version": 1, "rules": []},
        "content_hash": None,
        "source": "missing",
        "file_name": None,
    }
    assert '"rules": []' in request.system_prompt


def test_runtime_context_injects_bounded_account_snapshot_with_completeness_flags():
    runtime = _CaptureRuntime()
    agent = AgentManager(_AccountSnapshotStrategy()).create(
        name="snapshot",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="Inspect the account.")

    context = runtime.requests[0].runtime_context
    assert context["account"] == {"cash": 100_000.0, "portfolio_value": 125_000.0}
    assert len(context["positions"]) == 30
    assert context["account_snapshot"] == {
        "as_of": "2026-08-11T00:00:00+00:00",
        "account_complete": True,
        "positions_total": 30,
        "positions_included": 30,
        "positions_omitted": 0,
        "positions_complete": True,
        "open_orders_total": 0,
        "open_orders_included": 0,
        "open_orders_omitted": 0,
        "open_orders_complete": True,
    }


def test_invalid_rules_json_stops_before_model_call(tmp_path):
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        json.dumps(
            {
                "version": 1,
                "rules": [
                    {
                        "id": "machine-check",
                        "status": "active",
                        "interpretation": "Trade once.",
                        "check": {"kind": "max_entries_per_day"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    strategy = _Strategy()
    strategy.rules_path = rules_path
    runtime = _CaptureRuntime()
    agent = AgentManager(strategy).create(
        name="invalid_rules",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    with pytest.raises(StrategyRulesError, match="machine checks or verdict fields"):
        agent.run(task_prompt="This must not reach the model.")

    assert runtime.requests == []


def test_rules_loader_uses_only_active_rules(tmp_path):
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        json.dumps(
            {
                "version": 1,
                "rules": [
                    {"id": "a", "status": "active", "interpretation": "Active."},
                    {"id": "b", "status": "deleted", "interpretation": "Deleted."},
                ],
            }
        ),
        encoding="utf-8",
    )

    snapshot = load_strategy_rules(_Strategy(), rules_path)

    assert [rule["id"] for rule in snapshot.document["rules"]] == ["a"]
    assert snapshot.file_name == "rules.json"
    assert len(snapshot.content_hash or "") == 64
