"""Backward-compatible import for the large-cap stock AI trading team example."""

from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
    AIInvestmentCommitteeStrategy,
    DEFAULT_CASH_PARKING_SYMBOLS,
    DEFAULT_HANDOFF_TARGET_TOKENS,
    DEFAULT_UNIVERSE,
    _prepare_handoff_text,
)


__all__ = [
    "AITradingTeamBullBearLargeCapStocksStrategy",
    "AIInvestmentCommitteeStrategy",
    "DEFAULT_CASH_PARKING_SYMBOLS",
    "DEFAULT_HANDOFF_TARGET_TOKENS",
    "DEFAULT_UNIVERSE",
    "_prepare_handoff_text",
]
