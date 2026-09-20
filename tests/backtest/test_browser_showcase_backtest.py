"""Real backtest engine proof for the browser research showcase handoff."""

import json
from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.manager import AgentManager
from lumibot.entities import Asset, Data
from lumibot.example_strategies.ai_browser_research_showcase import (
    AIBrowserResearchShowcaseStrategy,
)
from tests.backtest.test_agent_runtime_backtest import _invoke_tool


@pytest.mark.usefixtures("disable_datasource_override")
def test_browser_showcase_research_handoff_places_real_backtest_trade(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    calls = []

    class ScriptedRuntime:
        def run(self, request):
            calls.append(request.agent_name)
            events = []
            if request.agent_name == "browser_researcher":
                assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
                return AgentRunResult(
                    summary="Authorized portal evidence: cautious bullish; screenshot_sha256=fixture-proof",
                    model=request.model,
                    events=events,
                )
            if request.agent_name == "trading_risk_manager":
                assert request.context["research_evidence"].startswith("Authorized portal evidence:")
                for tool in ("account_portfolio", "account_positions", "orders_open_orders"):
                    _invoke_tool(request, events, tool)
                _invoke_tool(request, events, "market_last_price", symbol="SHOW", asset_type="stock")
                submitted = _invoke_tool(
                    request,
                    events,
                    "orders_submit_order",
                    symbol="SHOW",
                    quantity=1,
                    side="buy",
                    asset_type="stock",
                    order_type="market",
                )
                assert submitted["execution_outcome"]["certainty"] == "confirmed_submitted"
                return AgentRunResult(
                    summary=json.dumps({"sandbox_order": submitted["order"]}, sort_keys=True),
                    model=request.model,
                    events=events,
                )
            assert request.agent_name == "trade_publisher"
            outcome = json.loads(request.context["trade_outcome"])
            assert outcome["sandbox_order"]["identifier"]
            assert request.context["publish_enabled"] is True
            return AgentRunResult(
                summary=f"Published idempotent receipt for {outcome['sandbox_order']['identifier']}",
                model=request.model,
                events=events,
            )

    original_create = AgentManager.create

    def create_with_scripted_model(self, *args, **kwargs):
        return original_create(self, *args, **kwargs, _runtime=ScriptedRuntime())

    monkeypatch.setattr(AgentManager, "create", create_with_scripted_model)
    asset = Asset("SHOW", Asset.AssetType.STOCK)
    frame = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10_000, 10_000, 10_000],
        },
        index=pd.date_range("2025-01-06", periods=3, tz="America/New_York"),
    )

    _, strategy = AIBrowserResearchShowcaseStrategy.run_backtest(
        PandasDataBacktesting,
        datetime(2025, 1, 7),
        datetime(2025, 1, 8),
        parameters={
            "symbol": "SHOW",
            "research_url": "https://owned-research.example.test/dashboard",
            "publish_enabled": True,
            "publish_url": "https://owned-community.example.test/new-post",
            "max_position_pct": 5,
        },
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

    assert calls == ["browser_researcher", "trading_risk_manager", "trade_publisher"]
    assert float(strategy.get_position("SHOW").quantity) == 1
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert len(fills) == 1
    assert float(fills.iloc[0]["filled_quantity"]) == 1
