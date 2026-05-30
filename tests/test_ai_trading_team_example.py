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
from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
)
from lumibot.example_strategies.ai_trading_team_ray_dalio_idea_meritocracy import (
    AITradingTeamRayDalioIdeaMeritocracyStrategy,
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
        AITradingTeamBullBearLargeCapStocksStrategy: {"AAPL", "MSFT", "NVDA", "GOOGL"},
        AITradingTeamRayDalioIdeaMeritocracyStrategy: {"SPY", "QQQ", "TLT", "GLD"},
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
    variant_classes = {
        AITradingTeamBullBearLargeCapStocksStrategy: 3,
        AITradingTeamRayDalioIdeaMeritocracyStrategy: 4,
        AITradingTeamWarrenBuffettValueStrategy: 2,
        AITradingTeamBillAckmanConcentratedStrategy: 3,
        AITradingTeamCitadelSectorPodsStrategy: 6,
    }

    for strategy_class, read_only_count in variant_classes.items():
        source = inspect.getsource(strategy_class.initialize)
        assert source.count("allow_trading=True") == 1
        assert source.count("allow_trading=False") == read_only_count
