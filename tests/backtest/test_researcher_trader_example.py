"""Real engine and tools, scripted model substitute: not a model eval."""
from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.manager import AgentManager
from lumibot.entities import Asset, Data
from lumibot.example_strategies.ai_researcher_trader import ResearcherTraderStrategy
from .test_agent_runtime_backtest import _invoke_tool


@pytest.mark.usefixtures("disable_datasource_override")
def test_canonical_two_agent_example_hands_off_and_executes_in_real_engine(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    calls = []

    class ScriptedRuntime:
        def run(self, request):
            calls.append(request.agent_name)
            events = []
            if request.agent_name == "researcher":
                assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
                quote = _invoke_tool(request, events, "market_last_price", symbol="AGST", asset_type="stock")
                return AgentRunResult(summary=f"Observed fixture quote: {quote}", model=request.model, events=events)
            assert request.context["research_evidence"].startswith("Observed fixture quote:")
            assert request.context["max_position_pct"] == 10
            for tool in ("account_portfolio", "account_positions", "orders_open_orders"):
                _invoke_tool(request, events, tool)
            _invoke_tool(request, events, "market_last_price", symbol="AGST", asset_type="stock")
            _invoke_tool(request, events, "orders_submit_order", symbol="AGST", quantity=1,
                         side="buy", asset_type="stock", order_type="market")
            return AgentRunResult(summary="Submitted one fixture intent; fill checked by engine assertion.",
                                  model=request.model, events=events)

    original_create = AgentManager.create

    def create_with_scripted_model(self, *args, **kwargs):
        return original_create(self, *args, **kwargs, _runtime=ScriptedRuntime())

    monkeypatch.setattr(AgentManager, "create", create_with_scripted_model)
    # The canonical example uses daily data; a minute fixture exercises a different engine cadence.
    asset = Asset("AGST", Asset.AssetType.STOCK)
    frame = pd.DataFrame({"open": [100., 101., 102.], "high": [101., 102., 103.],
                          "low": [99., 100., 101.], "close": [100., 101., 102.], "volume": 1000},
                         index=pd.date_range("2025-01-06", periods=3, tz="America/New_York"))
    _, strategy = ResearcherTraderStrategy.run_backtest(
        PandasDataBacktesting, datetime(2025, 1, 7), datetime(2025, 1, 8),
        parameters={"symbol": "AGST", "max_position_pct": 10},
        pandas_data={asset: Data(asset, frame, timestep="day")}, budget=10_000,
        benchmark_asset=None, analyze_backtest=False, show_plot=False,
        show_tearsheet=False, save_tearsheet=False, show_indicators=False,
        save_logfile=False, show_progress_bar=False, quiet_logs=True,
    )
    assert calls == ["researcher", "trader"]
    assert float(strategy.get_position("AGST").quantity) == 1
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert len(fills) == 1
    assert float(fills.iloc[0]["filled_quantity"]) == 1
