"""Run each stock AI example through a real LumiBot backtest with scripted agents.

The scripted runtime stands in for the model. Everything else is real: the
Strategy class as published, the backtest clock, the broker, order submission,
and fills. It proves the research reaches the trader and the trader's order
fills, and that only the trader can reach the order tools.
"""

import json
from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.manager import AgentManager
from lumibot.entities import Asset, Data
from lumibot.example_strategies.ai_fear_and_greed_trading_bot import FearAndGreedTradingBot
from lumibot.example_strategies.ai_insider_trading_bot import InsiderTradingBot
from lumibot.example_strategies.ai_nancy_pelosi_copy_trading_bot import NancyPelosiCopyTradingBot
from lumibot.example_strategies.ai_nancy_pelosi_trading_bot import NancyPelosiTradingBot
from lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated import (
    AITradingTeamBillAckmanConcentratedStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf import (
    AITradingTeamBullBearLeveragedETFStrategy,
)
from lumibot.example_strategies.ai_trading_team_warren_buffett_value import AITradingTeamWarrenBuffettValueStrategy
from tests.backtest.test_agent_runtime_backtest import _invoke_tool

CASES = [
    (NancyPelosiTradingBot, "NVDA"),
    (NancyPelosiCopyTradingBot, "NVDA"),
    (InsiderTradingBot, "AAPL"),
    (FearAndGreedTradingBot, "SPY"),
    (AITradingTeamWarrenBuffettValueStrategy, "KO"),
    (AITradingTeamBillAckmanConcentratedStrategy, "GOOGL"),
    (AITradingTeamBullBearLargeCapStocksStrategy, "AAPL"),
    (AITradingTeamBullBearLeveragedETFStrategy, "TQQQ"),
]


class _ScriptedRuntime:
    symbol = "SPY"
    requests = []

    def run(self, request):
        type(self).requests.append(request)
        tools = {tool.name for tool in request.bound_tools}
        events = []
        if "orders_submit_order" not in tools:
            summary = json.dumps({"ticker": type(self).symbol})
            return AgentRunResult(summary=summary, model=request.model, events=events)

        # The trading agent gets the other agents' work under whatever key the example uses.
        research = next(json.loads(v) for v in request.context.values() if isinstance(v, str) and '"ticker"' in v)
        _invoke_tool(request, events, "account_positions")
        _invoke_tool(request, events, "orders_open_orders")
        portfolio = _invoke_tool(request, events, "account_portfolio")
        held = _invoke_tool(request, events, "account_positions")
        if any(str((row.get("asset") or {}).get("type") or "").lower() == "stock" for row in held.get("positions") or []):
            return AgentRunResult(summary="already invested", model=request.model, events=events)
        quote = _invoke_tool(request, events, "market_last_price", symbol=research["ticker"], asset_type="stock")
        quantity = int(float(portfolio["portfolio_value"]) * 0.95 / float(quote["price"]))
        _invoke_tool(
            request, events, "orders_submit_order",
            symbol=research["ticker"], quantity=quantity, side="buy", asset_type="stock", order_type="market",
        )
        return AgentRunResult(summary="bought", model=request.model, events=events)


@pytest.fixture
def scripted_agents(monkeypatch):
    _ScriptedRuntime.requests = []
    original_create = AgentManager.create

    def create_with_runtime(self, *args, **kwargs):
        return original_create(self, *args, **kwargs, _runtime=_ScriptedRuntime())

    monkeypatch.setattr(AgentManager, "create", create_with_runtime)
    return _ScriptedRuntime


@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("strategy_class,symbol", CASES, ids=[case[0].__name__ for case in CASES])
def test_example_trades_from_the_research_in_a_real_backtest(strategy_class, symbol, scripted_agents, monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    scripted_agents.symbol = symbol
    asset = Asset(symbol, Asset.AssetType.STOCK)
    frame = pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 2_000_000},
        index=pd.bdate_range("2026-02-09", periods=7, tz="America/New_York"),
    )
    _, strategy = strategy_class.run_backtest(
        PandasDataBacktesting,
        datetime(2026, 2, 9),
        datetime(2026, 2, 13),
        pandas_data={asset: Data(asset, frame, timestep="day")},
        budget=10_000,
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )

    trader_calls = [
        request for request in scripted_agents.requests
        if "orders_submit_order" in {tool.name for tool in request.bound_tools}
    ]
    assert trader_calls
    assert {request.agent_name for request in trader_calls} != {request.agent_name for request in scripted_agents.requests}
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert list(fills["symbol"]) == [symbol]
    assert float(strategy.get_position(asset).quantity) >= 90
