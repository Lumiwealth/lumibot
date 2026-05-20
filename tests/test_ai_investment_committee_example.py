from lumibot.example_strategies.ai_investment_committee import (
    AIInvestmentCommitteeStrategy,
    _compact_handoff_text,
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
    assert AIInvestmentCommitteeStrategy.parameters["enable_notifications"] is False


def test_ai_committee_handoff_text_is_compacted(monkeypatch):
    monkeypatch.setenv("COMMITTEE_HANDOFF_MAX_CHARS", "2000")

    text = "a" * 5000
    compacted = _compact_handoff_text(text, label="evidence_pack")

    assert len(compacted) < len(text)
    assert "Truncated evidence_pack handoff" in compacted
    assert compacted.startswith("a" * 100)
    assert compacted.endswith("a" * 100)
