import inspect

import lumibot.example_strategies.ai_trading_team as example
from lumibot.example_strategies.ai_trading_team import AITradingTeamStrategy
from lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated import (
    AITradingTeamBillAckmanConcentratedStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf import (
    AITradingTeamBullBearLeveragedETFStrategy,
)
from lumibot.example_strategies.ai_trading_team_citadel_sector_pods import (
    AITradingTeamCitadelSectorPodsStrategy,
)
from lumibot.example_strategies.ai_trading_team_ray_dalio_all_weather import (
    AITradingTeamRayDalioAllWeatherStrategy,
)
from lumibot.example_strategies.ai_trading_team_warren_buffett_value import (
    AITradingTeamWarrenBuffettValueStrategy,
)


def test_ai_trading_team_is_bare_bones():
    assert list(AITradingTeamStrategy.parameters) == ["universe"]
    assert AITradingTeamStrategy is AITradingTeamBullBearLeveragedETFStrategy
    assert not hasattr(example, "MODEL")
    assert not hasattr(example, "UNIVERSE")
    assert not hasattr(AITradingTeamStrategy, "rotate_portfolio")


def test_ai_trading_team_uses_leveraged_etfs():
    universe = set(AITradingTeamStrategy.parameters["universe"])

    assert {"TQQQ", "SQQQ", "SOXL", "SOXS"}.issubset(universe)


def test_ai_trading_team_avoids_example_knobs():
    source = inspect.getsource(example)

    assert "@agent_tool" not in source
    assert "benchmark_asset" not in source
    assert "TradingFee" not in source
    assert "budget=" not in source
    assert "quiet_logs" not in source


def test_ai_trading_team_uses_agent_run_keywords():
    source = inspect.getsource(AITradingTeamStrategy.on_trading_iteration)

    assert ".run(\"" not in source
    assert "task_prompt=" in source


def test_ai_trading_team_variants_import_and_define_universes():
    variants = {
        AITradingTeamRayDalioAllWeatherStrategy: {"SPY", "TLT", "GLD", "DBC"},
        AITradingTeamWarrenBuffettValueStrategy: {"AAPL", "KO", "AXP", "COST"},
        AITradingTeamBillAckmanConcentratedStrategy: {"GOOGL", "CMG", "HLT", "QSR"},
        AITradingTeamCitadelSectorPodsStrategy: {"XLK", "XLF", "XLV", "XLE"},
    }

    for strategy_class, expected_symbols in variants.items():
        universe = set(strategy_class.parameters["universe"])
        assert expected_symbols.issubset(universe)
        assert hasattr(strategy_class, "initialize")
        assert hasattr(strategy_class, "on_trading_iteration")


def test_ai_trading_team_variants_keep_one_trading_agent():
    variant_classes = [
        AITradingTeamRayDalioAllWeatherStrategy,
        AITradingTeamWarrenBuffettValueStrategy,
        AITradingTeamBillAckmanConcentratedStrategy,
        AITradingTeamCitadelSectorPodsStrategy,
    ]

    for strategy_class in variant_classes:
        source = inspect.getsource(strategy_class.initialize)
        assert source.count("allow_trading=True") == 1
        assert source.count("allow_trading=False") == 3
