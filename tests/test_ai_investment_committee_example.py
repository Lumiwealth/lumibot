from lumibot.example_strategies.ai_investment_committee import (
    AIInvestmentCommitteeStrategy,
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
    assert AIInvestmentCommitteeStrategy.parameters["handoff_max_tokens"] == 32000
    assert AIInvestmentCommitteeStrategy.parameters["enable_notifications"] is False


def test_ai_committee_handoff_text_is_token_budgeted():
    text = "word " * 10000

    handoff = _prepare_handoff_text(text, label="evidence_pack", max_tokens=1000)

    assert len(handoff) < len(text)
    assert "Token-budget safety truncation" in handoff
    assert "evidence_pack" in handoff


def test_ai_committee_handoff_text_passes_without_truncation():
    text = "source-backed summary"

    assert _prepare_handoff_text(text, label="evidence_pack", max_tokens=4000) == text
