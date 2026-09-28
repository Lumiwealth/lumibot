import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent
from lumibot.components.agents.rules import StrategyRulesError, load_strategy_rules
from lumibot.components.agents.skills import (
    BUILTIN_SKILL_LOADING_INSTRUCTION,
    BUILTIN_SKILL_NAMES,
    build_builtin_skill_toolset,
    builtin_skill_directories,
    builtin_skill_fingerprint,
    load_builtin_skills,
)


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


def test_default_agent_model_is_current_and_explicit_pins_are_preserved():
    manager = AgentManager(_Strategy())
    default = manager.create(name="default", _runtime=_CaptureRuntime())
    assert default.default_model == "openai/gpt-6-luna"
    # Rob, 2026-09-23: the default is GPT-6 Luna on medium reasoning.
    assert default.reasoning_effort == "medium"
    explicit_default = manager.create(name="explicit", model="openai/gpt-6-luna", _runtime=_CaptureRuntime())
    assert explicit_default.reasoning_effort == "medium"
    raised = manager.create(name="raised", reasoning_effort="high", _runtime=_CaptureRuntime())
    assert raised.reasoning_effort == "high"
    lowered = manager.create(name="lowered", reasoning_effort="low", _runtime=_CaptureRuntime())
    assert lowered.reasoning_effort == "low"
    pinned = manager.create(name="pinned", model="pinned-model", _runtime=_CaptureRuntime())
    assert pinned.default_model == "pinned-model"
    assert pinned.reasoning_effort is None
    assert manager.create(name="family", model="google/gemini-pro", _runtime=_CaptureRuntime()).default_model == "google/gemini-pro"


def test_builtin_agent_skills_are_packaged_and_loadable():
    directories = builtin_skill_directories()
    assert tuple(path.name for path in directories) == BUILTIN_SKILL_NAMES
    assert all((path / "SKILL.md").is_file() for path in directories)

    skills = load_builtin_skills()
    assert tuple(skill.name for skill in skills) == BUILTIN_SKILL_NAMES
    assert "broad trading mandate" in skills[0].description
    assert "BotSpot public macro" in skills[1].description
    assert "broad mandate" in skills[2].description
    assert len(builtin_skill_fingerprint()) == 64


def test_research_skill_requires_query_after_catalog_discovery():
    skill = (
        Path(__file__).resolve().parents[1]
        / "lumibot/components/agents/skills/research-data/SKILL.md"
    )
    text = skill.read_text(encoding="utf-8")

    assert "Catalog results are metadata, not observations" in text
    assert "call `query_data`" in text
    assert "MUST call `query_data` for at least one relevant macro dataset" in text


def test_options_skill_requires_atomic_multileg_or_no_trade():
    options_skill = next(skill for skill in load_builtin_skills() if skill.name == "options-trading")
    instructions = " ".join(options_skill.instructions.split())

    assert "Never submit related legs independently" in instructions
    assert "If atomic package submission is unavailable" in instructions
    assert "make a no-trade decision" in instructions


def test_options_skill_requires_explicit_account_reads_and_measured_deltas():
    """Release eval options_iron_condor_atomic_open failed when the agent ordered
    from the injected snapshot without account tool calls, and when it declined a
    supported condor by inventing a 30-day minimum and guessing deltas from strike
    distance instead of measuring them."""
    options_skill = next(skill for skill in load_builtin_skills() if skill.name == "options-trading")
    instructions = " ".join(options_skill.instructions.split())

    assert "Call `account_portfolio`, `account_positions`, and `orders_open_orders`" in instructions
    assert "the injected account snapshot does not replace these calls" in instructions
    assert "Never judge a delta target unreachable from strike distance alone" in instructions
    assert "Do not add your own days-to-expiration minimum" in instructions
    assert "A short strike list is not by itself a reason to decline" in instructions


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


def test_stock_skill_prices_limits_from_current_price_and_loads_rule_bars_with_historical_prices():
    """Release eval stock_orb_completed_bars: one run priced a buy limit at a
    breakout bar's close (228.60) while the current price was 230.00, so it never
    filled; another built the range from market_load_history_table rows instead
    of the completed rule-interval bars from market_historical_prices."""
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    instructions = " ".join(stock_skill.instructions.split())
    intraday = " ".join(stock_skill.resources.references["intraday-setups.md"].split())

    assert "Price a limit order from the current `market_last_price` result" in instructions
    assert "never from a historical bar's close" in instructions
    # The limit-price rule is for a new order. It made agents reprice an
    # already-pending exit (release eval stock_pending_exit_no_duplicate).
    assert "This is for a new order; it is never a reason to modify an order that is already pending" in instructions
    assert "Do not cancel and replace a pending order, or modify it, to make it fill sooner" in instructions
    assert "Load the rule-interval bars with `market_historical_prices`" in intraday
    assert "pass `table_name` to query them with `duckdb_query`" in intraday


def test_stock_skill_reads_price_history_through_market_historical_prices():
    """stock_orb_completed_bars (GitHub run 35931107571): an agent built the
    range from market_load_history_table and ordered without the
    market_historical_prices read the stock workflow expects."""
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    instructions = " ".join(stock_skill.instructions.split())

    assert "Read that history with `market_historical_prices`, also for a single symbol" in instructions
    assert "`market_load_history_table` does not replace it before a stock order" in instructions


def test_orb_volume_confirmation_compares_regular_session_bars_and_keeps_an_earlier_breakout():
    """stock_orb_completed_bars (six-final rep 2): with the 09:45 bar closing at
    230.00 above the 228.50 range high on 2,400 shares against 1,000-1,100 in the
    range, the agent declined, measuring volume against pre-market bars or
    treating the 09:45 breakout as stale at 10:35."""
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    intraday = " ".join(stock_skill.resources.references["intraday-setups.md"].split())

    assert "compare the candidate bar with the opening-range bars" in intraday
    assert "Pre-market and after-hours bars are not part of that comparison" in intraday
    assert "The first completed bar after the range that meets the rule is the breakout" in intraday


def test_options_skill_takes_spread_limits_from_the_user_not_an_invented_threshold():
    """options_iron_condor_atomic_open on GPT-6 Luna: the agent passed its own
    max_spread_pct=0.20, flagged the cheap protective wings, and declined a
    package every leg of which the tool marked usable_for_limit_pricing."""
    options_skill = next(skill for skill in load_builtin_skills() if skill.name == "options-trading")
    quality = " ".join(options_skill.resources.references["contracts-greeks-liquidity.md"].split())

    assert "Pass `max_spread_pct` only when the user or active rules set a spread limit" in quality
    assert "A cheap protective wing often has a wide percentage spread" in quality


def test_stock_skill_leaves_a_pending_exit_in_place():
    """stock_pending_exit_no_duplicate on GPT-6 Luna: the agent cancelled the
    pending 40-share exit and sent a new market sell, which the skill allowed."""
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    instructions = " ".join(stock_skill.instructions.split())

    assert "Do not cancel and replace a pending order, or modify it, to make it fill sooner" in instructions
    assert "Let it resolve or cancel it deliberately before replacing it" not in instructions


def test_skill_loading_instruction_names_every_builtin_skill_exactly():
    for name in BUILTIN_SKILL_NAMES:
        assert f"`{name}`" in BUILTIN_SKILL_LOADING_INSTRUCTION
    assert "exact name" in BUILTIN_SKILL_LOADING_INSTRUCTION


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
    assert "MUST load the matching asset-class skill" in first.system_prompt
    assert "MUST load the research-data skill" in first.system_prompt
    assert "Never claim that no order was submitted" in first.system_prompt
    assert "risk_calculate_stock_quantity is unavailable" in first.system_prompt
    assert "make a no-trade decision" in first.system_prompt


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
    assert "MUST load the matching asset-class skill" not in request.system_prompt
    assert "MUST load the research-data skill" not in request.system_prompt


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


def test_evaluate_market_example_does_not_teach_an_invented_spread_limit():
    from lumibot.components.agents import BuiltinTools

    tool = BuiltinTools.options.evaluate_market().binder(object(), None)
    description = " ".join(tool.description.split())
    assert "max_spread_pct=0.20)" not in description
    assert "only when the user or active rules set a spread limit" in description


def test_indicator_values_come_from_tools_not_hand_arithmetic():
    """GPT-6 Luna evals: crypto_instrument_identity computed a two-period SMA by
    hand from market_historical_prices, and stock_price_before_order misstated
    a five-day average (227.80 and 226 for closes averaging 227.00)."""
    from lumibot.components.agents import BuiltinTools

    history = BuiltinTools.market.historical_prices().binder(object(), None)
    description = " ".join(history.description.split())
    assert "For an indicator value such as an SMA, EMA or RSI, call get_indicator or get_indicators" in description

    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    instructions = " ".join(stock_skill.instructions.split())
    assert "Compute averages and indicators with a tool" in instructions
    assert "never by mental arithmetic" in instructions



def test_options_skill_keeps_its_own_pending_package_instead_of_cancelling_it():
    # Release eval options_iron_condor_atomic_open (run 36034047544, repetition 3)
    # submitted a valid atomic condor, then cancelled its own pending package and
    # ended with no trade. The stock skill already forbids this; options did not.
    options_skill = next(skill for skill in load_builtin_skills() if skill.name == "options-trading")
    text = " ".join(options_skill.instructions.split())

    assert "In backtests, a short bounded `orders_wait_for_terminal` is appropriate" in text
    assert "Do not cancel, replace, or modify your own pending package" in text


class TestUserSuppliedSkills:
    """Users must be able to bring their own skills, not only switch ours off.

    Before this, `include_builtin_skills` was an on/off switch: you got our
    three skills or nothing. Someone building a trading system needs to teach
    the agent their own rules without forking the library.
    """

    def _write_skill(self, root, name, body="Always size to 1% of the book."):
        skill_dir = root / name
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text(
            f"---\nname: {name}\ndescription: {name} rules\n---\n\n{body}\n"
        )
        return skill_dir

    def test_user_skill_directories_are_resolved(self, tmp_path):
        from lumibot.components.agents import skills as skills_module

        mine = self._write_skill(tmp_path, "my-house-rules")
        resolved = skills_module.resolve_skill_directories(
            skill_dirs=[mine], include_builtin=False
        )
        assert resolved == (mine,)

    def test_builtin_and_user_skills_compose(self, tmp_path):
        from lumibot.components.agents import skills as skills_module

        mine = self._write_skill(tmp_path, "my-house-rules")
        resolved = skills_module.resolve_skill_directories(
            skill_dirs=[mine], include_builtin=True
        )
        assert mine in resolved
        assert len(resolved) == len(skills_module.builtin_skill_directories()) + 1
        # Built-ins keep their catalog order and the user's skill comes last,
        # so a user skill can build on ours rather than being shadowed.
        assert resolved[: len(skills_module.builtin_skill_directories())] == (
            skills_module.builtin_skill_directories()
        )

    def test_a_directory_without_skill_md_is_rejected_by_name(self, tmp_path):
        from lumibot.components.agents import skills as skills_module

        empty = tmp_path / "not-a-skill"
        empty.mkdir()
        with pytest.raises(ValueError, match="SKILL.md"):
            skills_module.resolve_skill_directories(skill_dirs=[empty], include_builtin=False)

    def test_a_missing_directory_says_which_one(self, tmp_path):
        from lumibot.components.agents import skills as skills_module

        with pytest.raises(ValueError, match="does-not-exist"):
            skills_module.resolve_skill_directories(
                skill_dirs=[tmp_path / "does-not-exist"], include_builtin=False
            )

    def test_strings_are_accepted_as_well_as_paths(self, tmp_path):
        from lumibot.components.agents import skills as skills_module

        mine = self._write_skill(tmp_path, "my-house-rules")
        resolved = skills_module.resolve_skill_directories(
            skill_dirs=[str(mine)], include_builtin=False
        )
        assert resolved == (mine,)

    def test_user_skills_change_the_fingerprint(self, tmp_path):
        """Provenance must notice a user skill, or an eval receipt would lie."""
        from lumibot.components.agents import skills as skills_module

        mine = self._write_skill(tmp_path, "my-house-rules")
        builtin_only = skills_module.skill_fingerprint(
            skills_module.resolve_skill_directories(skill_dirs=None, include_builtin=True)
        )
        with_mine = skills_module.skill_fingerprint(
            skills_module.resolve_skill_directories(skill_dirs=[mine], include_builtin=True)
        )
        assert builtin_only != with_mine

    def test_create_accepts_skill_dirs(self, tmp_path):
        """The public API is agents.create(skill_dirs=[...])."""
        import inspect

        from lumibot.components.agents.manager import AgentManager

        params = inspect.signature(AgentManager.create).parameters
        assert "skill_dirs" in params
        assert params["skill_dirs"].default is None
