from lumibot.example_strategies.ai_investment_committee import AIInvestmentCommitteeStrategy


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
