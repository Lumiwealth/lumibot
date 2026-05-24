from lumibot.example_strategies.ai_trading_team import AITradingTeamStrategy, MODEL, UNIVERSE


def test_ai_trading_team_is_copy_paste_focused():
    assert MODEL == "gemini-3.1-flash-lite"
    assert AITradingTeamStrategy.parameters["universe"] == UNIVERSE
    assert AITradingTeamStrategy.parameters["rebalance_every"] == 5
    assert "model" not in AITradingTeamStrategy.parameters


def test_ai_trading_team_uses_leveraged_etfs():
    assert {"TQQQ", "SQQQ", "SOXL", "SOXS"}.issubset(set(UNIVERSE))


def test_ai_trading_team_rotate_tool_is_marked_mutating():
    metadata = getattr(AITradingTeamStrategy.rotate_portfolio, "_lumibot_agent_tool")

    assert metadata["name"] == "rotate_portfolio"
    assert metadata["mutates_trading"] is True
