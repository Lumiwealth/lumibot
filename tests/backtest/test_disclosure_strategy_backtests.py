"""End-to-end backtest proofs for the disclosure Strategy examples.

These tests run the public Strategy subclasses through PandasDataBacktesting.
The deterministic runtime chooses the agent tools, while LumiBot itself owns
the clock, visibility boundary, broker, order submission, and fill lifecycle.
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.manager import AgentManager
from lumibot.entities import Asset, Data
from lumibot.example_strategies.ai_congress_disclosures import AICongressDisclosuresStrategy
from lumibot.example_strategies.ai_sec_insider_filings import AISECInsiderFilingsStrategy
from tests.backtest.test_agent_runtime_backtest import _invoke_tool


class _DisclosureRuntime:
    requests = []

    def run(self, request):
        type(self).requests.append(request)
        events = []
        if request.agent_name in {"bull", "bear", "interpreter"}:
            assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
            return AgentRunResult(summary=f"{request.agent_name} note", model=request.model, events=events)
        if request.agent_name in {"congress_researcher", "insider_trade_researcher"}:
            assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
            symbol = "NVDA" if request.agent_name == "congress_researcher" else "AAPL"
            return AgentRunResult(
                summary=json.dumps({"ticker": symbol, "side": "buy"}, sort_keys=True),
                model=request.model,
                events=events,
            )

        assert request.agent_name == "trading_risk_manager"
        _invoke_tool(request, events, "account_portfolio")
        held = _invoke_tool(request, events, "account_positions")
        _invoke_tool(request, events, "orders_open_orders")
        stock_rows = [
            row
            for row in held.get("positions") or []
            if str((row.get("asset") or {}).get("type") or "").lower() == "stock"
        ]
        if stock_rows:
            return AgentRunResult(summary="already invested", model=request.model, events=events)
        symbol = json.loads(request.context["research_evidence"])["ticker"]
        portfolio = _invoke_tool(request, events, "account_portfolio")
        quote = _invoke_tool(request, events, "market_last_price", symbol=symbol, asset_type="stock")
        price = float(quote["price"])
        quantity = int(float(portfolio["portfolio_value"]) * 0.95 / price)
        assert quantity >= 50
        submitted = _invoke_tool(
            request,
            events,
            "orders_submit_order",
            symbol=symbol,
            quantity=quantity,
            side="buy",
            asset_type="stock",
            order_type="market",
        )
        return AgentRunResult(
            summary=json.dumps({"submitted_order": submitted["order"]}, sort_keys=True),
            model=request.model,
            events=events,
        )


def _stock_data(symbol: str, start: str, periods: int = 7):
    asset = Asset(symbol, Asset.AssetType.STOCK)
    frame = pd.DataFrame(
        {
            "open": [100.0 + index for index in range(periods)],
            "high": [101.0 + index for index in range(periods)],
            "low": [99.0 + index for index in range(periods)],
            "close": [100.0 + index for index in range(periods)],
            "volume": [2_000_000] * periods,
        },
        index=pd.bdate_range(start, periods=periods, tz="America/New_York"),
    )
    return asset, {asset: Data(asset, frame, timestep="day")}


def _run_strategy(strategy_class, *, pandas_data, start, end, parameters):
    return strategy_class.run_backtest(
        PandasDataBacktesting,
        start,
        end,
        parameters=parameters,
        pandas_data=pandas_data,
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


def _write_proof(tmp_path: Path, *, name: str, strategy, requests, evidence: dict):
    fills = strategy.broker._trade_event_log_df
    fills[fills["status"] == "fill"].to_csv(tmp_path / f"{name}-trades.csv", index=False)
    (tmp_path / f"{name}-proof.json").write_text(
        json.dumps(
            {
                "evidence": evidence,
                "agent_calls": [
                    {
                        "agent_name": request.agent_name,
                        "as_of": request.context.get("as_of"),
                        "available_record_ids": [
                            row["id"]
                            for row in request.context.get("disclosures", request.context.get("transactions", []))
                        ],
                    }
                    for request in requests
                ],
                "filled_orders": fills[fills["status"] == "fill"].to_dict(orient="records"),
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.fixture
def deterministic_agent_runtime(monkeypatch):
    _DisclosureRuntime.requests = []
    original_create = AgentManager.create

    def create_with_runtime(self, *args, **kwargs):
        return original_create(self, *args, **kwargs, _runtime=_DisclosureRuntime())

    monkeypatch.setattr(AgentManager, "create", create_with_runtime)
    return _DisclosureRuntime


@pytest.mark.usefixtures("disable_datasource_override")
def test_congress_strategy_agent_order_is_account_sized(
    deterministic_agent_runtime,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    asset, pandas_data = _stock_data("NVDA", "2026-02-12")
    _, strategy = _run_strategy(
        AICongressDisclosuresStrategy,
        pandas_data=pandas_data,
        start=datetime(2026, 2, 12),
        end=datetime(2026, 2, 23),
        parameters={},
    )

    research_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "congress_researcher"
    ]
    trading_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "trading_risk_manager"
    ]
    assert len(research_requests) == len(trading_requests) >= 1
    assert "report date" in research_requests[0].context["clock_rule"]
    assert "disclosures" not in research_requests[0].context
    assert float(strategy.get_position(asset).quantity) >= 50
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert len(fills) == 1
    assert fills.iloc[0]["symbol"] == "NVDA"
    _write_proof(
        tmp_path,
        name="congress-report-date-clock",
        strategy=strategy,
        requests=deterministic_agent_runtime.requests,
        evidence={
            "first_agent_as_of": research_requests[0].context["as_of"],
            "clock_rule": research_requests[0].context["clock_rule"],
        },
    )


@pytest.mark.usefixtures("disable_datasource_override")
def test_form4_strategy_agent_order_is_account_sized(
    deterministic_agent_runtime,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    asset, pandas_data = _stock_data("AAPL", "2026-02-10")
    _, strategy = _run_strategy(
        AISECInsiderFilingsStrategy,
        pandas_data=pandas_data,
        start=datetime(2026, 2, 10),
        end=datetime(2026, 2, 20),
        parameters={},
    )

    research_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "insider_trade_researcher"
    ]
    trading_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "trading_risk_manager"
    ]
    assert len(research_requests) == len(trading_requests) >= 1
    assert "after as_of" in research_requests[0].context["clock_rule"]
    assert "transactions" not in research_requests[0].context
    assert float(strategy.get_position(asset).quantity) >= 50
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert len(fills) == 1
    assert fills.iloc[0]["symbol"] == "AAPL"
    _write_proof(
        tmp_path,
        name="sec-form4",
        strategy=strategy,
        requests=deterministic_agent_runtime.requests,
        evidence={
            "first_agent_as_of": research_requests[0].context["as_of"],
            "clock_rule": research_requests[0].context["clock_rule"],
        },
    )
