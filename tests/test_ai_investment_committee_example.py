from lumibot.example_strategies.ai_investment_committee import (
    AIInvestmentCommitteeStrategy,
    DEFAULT_CASH_PARKING_SYMBOLS,
    _prepare_handoff_text,
)


def test_ai_committee_example_uses_normal_lumibot_iteration_flow():
    parameter_names = set(AIInvestmentCommitteeStrategy.parameters)

    assert "run_once" not in parameter_names
    assert "committee_start_on_or_after" not in parameter_names
    assert "run_every_n_iterations" not in parameter_names
    assert not hasattr(AIInvestmentCommitteeStrategy, "_before_committee_start")


def test_ai_committee_example_exposes_expected_risk_controls():
    assert AIInvestmentCommitteeStrategy.parameters["max_position_pct"] == 0.20
    assert AIInvestmentCommitteeStrategy.parameters["max_new_positions_per_run"] == 2
    assert AIInvestmentCommitteeStrategy.parameters["handoff_target_tokens"] == 24000
    assert AIInvestmentCommitteeStrategy.parameters["cash_parking_symbols"] == DEFAULT_CASH_PARKING_SYMBOLS
    assert AIInvestmentCommitteeStrategy.parameters["enable_notifications"] is False


def test_ai_committee_prompt_allows_cash_parking_without_expanding_growth_universe():
    evidence_prompt = AIInvestmentCommitteeStrategy._evidence_researcher_prompt(object())
    portfolio_prompt = AIInvestmentCommitteeStrategy._portfolio_manager_prompt(object())

    assert "context.cash_parking_symbols" in evidence_prompt
    assert "Do not run corporate SEC fundamental analysis on cash-parking ETFs" in evidence_prompt
    assert "context.tradable_symbols" in portfolio_prompt
    assert "Growth-equity trades must come from context.universe" in portfolio_prompt
    assert "Defensive cash-parking trades must come from context.cash_parking_symbols" in portfolio_prompt


def test_ai_committee_uses_bounded_agent_tool_surfaces():
    strategy = object()
    evidence_tool_names = {tool.name for tool in AIInvestmentCommitteeStrategy._evidence_tools(strategy)}
    debate_tool_names = {tool.name for tool in AIInvestmentCommitteeStrategy._debate_tools(strategy)}

    assert "search_filing" not in evidence_tool_names
    assert "get_filing_section" not in evidence_tool_names
    assert debate_tool_names == set()


def test_ai_committee_handoff_text_is_not_truncated():
    text = "word " * 10000

    handoff = _prepare_handoff_text(text)

    assert handoff == text


def test_ai_committee_handoff_text_handles_empty_values():
    assert _prepare_handoff_text(None) == ""
