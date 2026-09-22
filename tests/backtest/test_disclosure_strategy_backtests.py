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
        if request.agent_name in {"disclosure_researcher", "form4_researcher"}:
            assert "orders_submit_order" not in {tool.name for tool in request.bound_tools}
            evidence_key = "disclosures" if request.agent_name == "disclosure_researcher" else "transactions"
            return AgentRunResult(
                summary=json.dumps(
                    {
                        "as_of": request.context["as_of"],
                        "evidence": request.context[evidence_key],
                        "availability_rule": request.context["availability_rule"],
                    },
                    sort_keys=True,
                ),
                model=request.model,
                events=events,
            )

        assert request.agent_name == "trading_risk_manager"
        _invoke_tool(request, events, "account_portfolio")
        _invoke_tool(request, events, "account_positions")
        _invoke_tool(request, events, "orders_open_orders")
        evidence = json.loads(request.context["research_evidence"])["evidence"]
        symbol = evidence[0].get("ticker")
        _invoke_tool(request, events, "market_last_price", symbol=symbol, asset_type="stock")
        submitted = _invoke_tool(
            request,
            events,
            "orders_submit_order",
            symbol=symbol,
            quantity=1,
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
def test_congress_strategy_waits_for_report_date_then_places_and_fills_order(
    deterministic_agent_runtime,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    asset, pandas_data = _stock_data("NVDA", "2026-02-12")
    disclosure = {
        "id": "clock-report-date-proof",
        "Politician": "Clock Test Member",
        "Ticker": "NVDA",
        "Transaction": "Purchase",
        "TransactionDate": "2026-01-05",
        "ReportDate": "2026-02-17T14:00:00+00:00",
        "fetched_at": "2026-02-17T14:00:01+00:00",
        "Amount": "$100,001 - $250,000",
        "source_url": "https://example.invalid/clock-proof",
        "data_rights": "clock_test_not_a_filing",
    }

    _, strategy = _run_strategy(
        AICongressDisclosuresStrategy,
        pandas_data=pandas_data,
        start=datetime(2026, 2, 12),
        end=datetime(2026, 2, 23),
        parameters={"disclosures": [disclosure]},
    )

    research_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "disclosure_researcher"
    ]
    trading_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "trading_risk_manager"
    ]
    assert len(research_requests) == len(trading_requests) == 1
    assert research_requests[0].context["as_of"] >= disclosure["ReportDate"]
    assert research_requests[0].context["disclosures"][0]["transaction_date"] == "2026-01-05"
    assert research_requests[0].context["disclosures"][0]["published_at"] == disclosure["ReportDate"]
    assert float(strategy.get_position(asset).quantity) == 1
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
            "transaction_date": disclosure["TransactionDate"],
            "published_at": disclosure["ReportDate"],
            "first_agent_as_of": research_requests[0].context["as_of"],
            "lookahead_prevented": research_requests[0].context["as_of"] >= disclosure["ReportDate"],
        },
    )


@pytest.mark.usefixtures("disable_datasource_override")
def test_form4_strategy_waits_for_sec_acceptance_filters_grant_then_fills_order(
    deterministic_agent_runtime,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    asset, pandas_data = _stock_data("AAPL", "2026-02-10")
    transactions = [
        {
            "id": "form4-aapl-proof",
            "accession_number": "0000320193-26-000001",
            "document_type": "4",
            "amendment": False,
            "ticker": "AAPL",
            "owner_name": "Example Executive",
            "transaction_date": "2026-02-10",
            "published_at": "2026-02-11T21:30:00+00:00",
            "fetched_at": "2026-02-11T21:30:01+00:00",
            "source": "sec_edgar_form4",
            "transaction_code": "P",
            "transaction_kind": "open_market_purchase",
            "automatic_plan": False,
            "open_market": True,
            "acquired_disposed": "A",
            "shares": 1000,
            "price_per_share": 220,
            "transaction_value": 220000,
            "derivative": False,
            "ownership": "direct",
            "source_url": "https://example.invalid/frozen-form4-proof",
        },
        {
            "id": "form4-aapl-grant-proof",
            "accession_number": "0000320193-26-000002",
            "document_type": "4",
            "amendment": False,
            "ticker": "AAPL",
            "owner_name": "Example Executive",
            "transaction_date": "2026-02-10",
            "published_at": "2026-02-11T21:30:00+00:00",
            "fetched_at": "2026-02-11T21:30:01+00:00",
            "source": "sec_edgar_form4",
            "transaction_code": "A",
            "transaction_kind": "grant_or_award",
            "automatic_plan": False,
            "open_market": False,
            "acquired_disposed": "A",
            "shares": 500,
            "price_per_share": 0,
            "transaction_value": 0,
            "derivative": False,
            "ownership": "direct",
            "source_url": "https://example.invalid/frozen-form4-proof",
        },
    ]

    _, strategy = _run_strategy(
        AISECInsiderFilingsStrategy,
        pandas_data=pandas_data,
        start=datetime(2026, 2, 10),
        end=datetime(2026, 2, 20),
        parameters={"transactions": transactions},
    )

    research_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "form4_researcher"
    ]
    trading_requests = [
        request for request in deterministic_agent_runtime.requests if request.agent_name == "trading_risk_manager"
    ]
    assert len(research_requests) == len(trading_requests) == 1
    assert research_requests[0].context["as_of"] >= transactions[0]["published_at"]
    assert [row["id"] for row in research_requests[0].context["transactions"]] == ["form4-aapl-proof"]
    assert float(strategy.get_position(asset).quantity) == 1
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
            "transaction_date": transactions[0]["transaction_date"],
            "published_at": transactions[0]["published_at"],
            "first_agent_as_of": research_requests[0].context["as_of"],
            "lookahead_prevented": research_requests[0].context["as_of"] >= transactions[0]["published_at"],
            "filtered_non_open_market_ids": [transactions[1]["id"]],
        },
    )
