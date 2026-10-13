"""Packaged ADK resource contracts; native model evals assess trading behavior."""

import asyncio
from types import SimpleNamespace

import pytest

from lumibot.components.agents.skills import (
    build_builtin_skill_toolset,
    builtin_skill_directories,
)


@pytest.fixture(scope="module")
def intraday_resource():
    toolset = build_builtin_skill_toolset()
    resource_tool = next(tool for tool in asyncio.run(toolset.get_tools()) if tool.name == "load_skill_resource")
    context = SimpleNamespace(state={}, agent_name="intraday-resource-contract", invocation_id="intraday-contract")
    result = asyncio.run(
        resource_tool.run_async(
            args={"skill_name": "stock-trading", "file_path": "references/intraday-setups.md"},
            tool_context=context,
        )
    )
    return resource_tool, result


def test_intraday_reference_reaches_actual_adk_resource_loader_unchanged(intraday_resource):
    resource_tool, result = intraday_resource
    schema = resource_tool._get_declaration().parameters_json_schema
    assert schema["required"] == ["skill_name", "file_path"]
    stock_directory = next(path for path in builtin_skill_directories() if path.name == "stock-trading")
    packaged = (stock_directory / "references" / "intraday-setups.md").read_text(encoding="utf-8")
    assert result == {
        "skill_name": "stock-trading",
        "file_path": "references/intraday-setups.md",
        "content": packaged,
    }


def test_loaded_orb_guidance_counts_interval_end_at_clock_as_completed(intraday_resource):
    # Preserved native stock_orb_completed_bars failures excluded a valid
    # completed breakout because the current, separate interval was unfinished.
    content = " ".join(intraday_resource[1]["content"].split())
    assert "compute each interval end as its bar start plus the rule interval" in content
    assert "interval end is at or before the runtime clock (inclusive)" in content
    assert "A bar ending exactly at the clock is completed" in content
    assert "Do not discard a completed candidate because the current interval is still forming" in content


def test_loaded_orb_guidance_separates_completed_confirmation_from_current_quote(intraday_resource):
    content = " ".join(intraday_resource[1]["content"].split())
    assert "Evaluate the exact user predicate on the completed opening-range and candidate bars first" in content
    assert "Then check the current `market_last_price` quote separately" in content
    assert "The candidate's completed close and the current quote are separate checks" in content
    assert "Unless the user explicitly requires a current-bar close, do not add that confirmation" in content
    assert "do not wait for the current unfinished bar to close" in content


def test_loaded_orb_guidance_continues_authorized_signal_to_existing_quantity_and_order_checks(intraday_resource):
    content = " ".join(intraday_resource[1]["content"].split())
    assert (
        "When execution is authorized and the user's predicate is true, proceed to the core sizing and order checks"
        in content
    )
    assert (
        "For a notional cap, call `risk_calculate_stock_quantity` and use its returned whole-share quantity" in content
    )
    assert "do not skip that calculation because a later bar is unfinished" in content
    assert "Do not invent an additional entry condition" in content


def test_loaded_intraday_guidance_retains_data_and_execution_safeguards(intraday_resource):
    content = " ".join(intraday_resource[1]["content"].split())
    assert "Do not infer an interval merely from sparse timestamps" in content
    assert (
        "Never treat the first one-minute constituent of a five-minute window as a completed five-minute bar" in content
    )
    assert "Do not invent an opening range from incomplete bars" in content
    assert "Respect the user's maximum entries, positions, stops, targets, and session boundaries" in content
    assert "Reread positions and open orders, respect cooldown and entry-frequency rules" in content
