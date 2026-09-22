"""Real backtest engine proof for the browser research showcase handoff."""

import json
from datetime import datetime
from pathlib import Path

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
from tests.test_agent_browser_patchright_apitest import _FixtureHandler

pytest_plugins = ("tests.test_agent_browser_patchright_apitest",)


@pytest.mark.usefixtures("disable_datasource_override")
def test_browser_showcase_research_handoff_places_real_backtest_trade(
    monkeypatch,
    tmp_path,
    browser_fixture_server,
):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    calls = []
    browser_receipts = {}

    class ScriptedRuntime:
        def run(self, request):
            calls.append(request.agent_name)
            events = []
            if request.agent_name == "browser_researcher":
                assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
                real_page = request.context["research_url"]
                assert real_page.startswith("https://disclosures-clerk.house.gov/")
                opened = _invoke_tool(request, events, "browser_session_open", profile="strategy-research")
                session_id = opened["session_id"]
                _invoke_tool(
                    request,
                    events,
                    "browser_navigate",
                    session_id=session_id,
                    url=real_page,
                )
                observed = _invoke_tool(request, events, "browser_observe", session_id=session_id)
                screenshot = _invoke_tool(
                    request,
                    events,
                    "browser_screenshot",
                    session_id=session_id,
                    name="strategy-research",
                )
                closed = _invoke_tool(request, events, "browser_session_close", session_id=session_id)
                observed_text = observed["text"].lower()
                assert "disclosures-clerk.house.gov" in observed["url"]
                assert "just a moment" not in observed_text
                assert "financial disclosure" in observed_text or "clerk" in observed_text
                browser_receipts["research"] = {"screenshot": screenshot, "session": closed}
                return AgentRunResult(
                    summary=json.dumps(
                        {
                            "finding": "House disclosure page evidence: cautious bullish",
                            "url": observed["url"],
                            "screenshot_path": screenshot["path"],
                            "screenshot_sha256": screenshot["sha256"],
                            "trace_path": closed["trace_path"],
                            "trace_sha256": closed["trace_sha256"],
                        },
                        sort_keys=True,
                    ),
                    model=request.model,
                    events=events,
                )
            if request.agent_name == "trading_risk_manager":
                evidence = json.loads(request.context["research_evidence"])
                assert evidence["finding"].startswith("House disclosure page evidence:")
                assert Path(evidence["screenshot_path"]).is_file()
                assert request.context["max_position_pct"] == 5
                assert request.context["max_position_fraction"] == 0.05
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
                assert submitted["execution_outcome"]["broker_state_certainty"] == "confirmed_submitted"
                browser_receipts["order_id"] = submitted["order"]["identifier"]
                return AgentRunResult(
                    summary=json.dumps({"sandbox_order": submitted["order"]}, sort_keys=True),
                    model=request.model,
                    events=events,
                )
            assert request.agent_name == "trade_publisher"
            outcome = json.loads(request.context["trade_outcome"])
            assert outcome["sandbox_order"]["identifier"]
            assert request.context["publish_enabled"] is True
            selectors = request.context["publish_form_selectors"]
            assert selectors == {
                "idempotency_key": "#idempotency-key",
                "receipt": "#receipt",
                "submit": "#publish",
            }
            opened = _invoke_tool(request, events, "browser_session_open", profile="strategy-publisher")
            session_id = opened["session_id"]
            _invoke_tool(
                request,
                events,
                "browser_navigate",
                session_id=session_id,
                url=f"{browser_fixture_server}/community",
            )
            _invoke_tool(
                request,
                events,
                "browser_act",
                session_id=session_id,
                action="fill",
                selector=selectors["idempotency_key"],
                value=outcome["sandbox_order"]["identifier"],
            )
            _invoke_tool(
                request,
                events,
                "browser_act",
                session_id=session_id,
                action="fill",
                selector=selectors["receipt"],
                value="Sandbox trade submitted: SHOW",
            )
            _invoke_tool(
                request,
                events,
                "browser_act",
                session_id=session_id,
                action="click",
                selector=selectors["submit"],
            )
            screenshot = _invoke_tool(
                request,
                events,
                "browser_screenshot",
                session_id=session_id,
                name="strategy-published-receipt",
            )
            closed = _invoke_tool(request, events, "browser_session_close", session_id=session_id)
            browser_receipts["publisher"] = {"screenshot": screenshot, "session": closed}
            return AgentRunResult(
                summary=json.dumps(
                    {
                        "published_order_id": outcome["sandbox_order"]["identifier"],
                        "screenshot_path": screenshot["path"],
                        "screenshot_sha256": screenshot["sha256"],
                        "trace_path": closed["trace_path"],
                        "trace_sha256": closed["trace_sha256"],
                    },
                    sort_keys=True,
                ),
                model=request.model,
                events=events,
            )

    original_create = AgentManager.create

    def create_with_scripted_model(self, *args, **kwargs):
        self.strategy.browser_state_root = tmp_path / "browser"
        self.strategy.browser_credential_profiles = {
            "fixture": {
                "allowed_hosts": ["127.0.0.1"],
                "username": "browser-user",
                "password": "browser-password",
            }
        }
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
            "research_url": "https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure",
            "publish_enabled": True,
            "publish_url": f"{browser_fixture_server}/community",
            "publish_form_selectors": {
                "idempotency_key": "#idempotency-key",
                "receipt": "#receipt",
                "submit": "#publish",
            },
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
    assert _FixtureHandler.publications == [
        {
            "idempotency_key": [browser_receipts["order_id"]],
            "receipt": ["Sandbox trade submitted: SHOW"],
        }
    ]
    for receipt in (browser_receipts["research"], browser_receipts["publisher"]):
        assert Path(receipt["screenshot"]["path"]).stat().st_size > 0
        assert len(receipt["screenshot"]["sha256"]) == 64
        assert Path(receipt["session"]["trace_path"]).is_file()
    fills.to_csv(tmp_path / "browser-showcase-trades.csv", index=False)
    (tmp_path / "browser-showcase-proof.json").write_text(
        json.dumps(
            {
                "agent_calls": calls,
                "order_id": browser_receipts["order_id"],
                "research": browser_receipts["research"],
                "publisher": browser_receipts["publisher"],
                "publication": _FixtureHandler.publications[0],
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )
