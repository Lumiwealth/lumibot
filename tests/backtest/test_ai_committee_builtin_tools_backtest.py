import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def _event(kind: str, *, text: str | None = None, tool_name: str | None = None, payload: dict | None = None):
    from lumibot.components.agents import AgentTraceEvent

    return AgentTraceEvent(
        kind=kind,
        text=text,
        tool_name=tool_name,
        payload=payload,
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def _invoke_tool(request, events, tool_name: str, **kwargs):
    from lumibot.components.agents.runtime import _wrap_tool_callable

    tool_map = {tool.name: _wrap_tool_callable(tool) for tool in request.bound_tools}
    events.append(_event("tool_call", tool_name=tool_name, payload=kwargs))
    result = tool_map[tool_name](**kwargs)
    payload = result if isinstance(result, dict) else {"value": result}
    events.append(_event("tool_result", tool_name=tool_name, payload=payload))
    return result


class _Response:
    def __init__(self, *, payload=None, text="", content=None):
        self._payload = payload if payload is not None else {}
        self.text = text
        self.content = content if content is not None else (text.encode() if text else b"{}")

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class CommitteeToolRuntime:
    call_count = 0
    last_results = None

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Exercising the AI committee built-in tools.")]
        symbol = request.context["symbol"]

        results = {}
        results["portfolio"] = _invoke_tool(request, events, "account_portfolio")
        results["positions"] = _invoke_tool(request, events, "account_positions")
        results["last_price"] = _invoke_tool(request, events, "market_last_price", symbol=symbol)
        history = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=symbol,
            length=4,
            timestep="minute",
            table_name="committee_history",
        )
        results["history"] = history
        results["query"] = _invoke_tool(
            request,
            events,
            "duckdb_query",
            sql=f"SELECT COUNT(*) AS rows_seen, MAX(close) AS max_close FROM {history['table_name']}",
        )
        results["docs"] = _invoke_tool(request, events, "lumibot_docs_search", query="agents allow_trading", limit=2)
        results["news"] = _invoke_tool(request, events, "alpaca_news", symbols=symbol, limit=2)
        results["indicator_list"] = _invoke_tool(request, events, "list_indicators")
        results["indicator"] = _invoke_tool(
            request,
            events,
            "get_indicator",
            symbol=symbol,
            indicator="sma",
            timestep="minute",
            parameters_json='{"length": 2}',
        )
        results["indicators"] = _invoke_tool(
            request,
            events,
            "get_indicators",
            symbol=symbol,
            indicators=["sma", "ema"],
            timestep="minute",
        )
        results["income"] = _invoke_tool(request, events, "get_income_statement", symbol=symbol)
        results["balance"] = _invoke_tool(request, events, "get_balance_sheet", symbol=symbol)
        results["cash_flow"] = _invoke_tool(request, events, "get_cash_flow", symbol=symbol)
        results["facts"] = _invoke_tool(request, events, "get_company_facts", symbol=symbol)
        results["fred_list"] = _invoke_tool(request, events, "list_fred_series", category="rates")
        results["fred_latest"] = _invoke_tool(request, events, "get_fred_latest", series_id="DGS10")
        results["fred_snapshot"] = _invoke_tool(request, events, "get_fred_snapshot", series_ids=["DGS10", "UNRATE"])
        filings = _invoke_tool(request, events, "get_filings", symbol=symbol, form="10-K", limit=5)
        if "filings" not in filings:
            raise AssertionError(filings)
        results["filings"] = filings
        filing = filings["filings"][0]
        results["filing_search"] = _invoke_tool(
            request,
            events,
            "search_filing",
            symbol=symbol,
            accession_number=filing["accession_number"],
            primary_document=filing["primary_document"],
            query="customer concentration",
        )
        results["filing_document"] = _invoke_tool(
            request,
            events,
            "get_filing_document",
            symbol=symbol,
            accession_number=filing["accession_number"],
            primary_document=filing["primary_document"],
            max_chars=500,
        )
        results["remember"] = _invoke_tool(request, events, "remember", text="Committee saw AAPL revenue strength.", tags=["AAPL"])
        results["decision"] = _invoke_tool(
            request,
            events,
            "remember_decision",
            text="Committee bought one share after evidence review.",
            symbol=symbol,
            action="buy",
        )
        results["lesson"] = _invoke_tool(request, events, "remember_lesson", text="Confirm filings before trading.", symbol=symbol)
        thesis = _invoke_tool(request, events, "open_thesis", text="AAPL long thesis based on services resilience.", symbol=symbol)
        results["thesis"] = thesis
        results["thesis_update"] = _invoke_tool(request, events, "update_thesis", thesis_id=thesis["id"], text="Bull case strengthened.")
        results["thesis_close"] = _invoke_tool(request, events, "close_thesis", thesis_id=thesis["id"], text="Closed after smoke test.")
        results["memory_search"] = _invoke_tool(request, events, "search_memory", query="services resilience", limit=5)
        results["notify"] = _invoke_tool(
            request,
            events,
            "notify_user",
            title="Committee decision",
            message="Backtest smoke bought one share.",
            severity="info",
            enabled=True,
        )
        results["open_orders_before"] = _invoke_tool(request, events, "orders_open_orders")
        results["order"] = _invoke_tool(
            request,
            events,
            "orders_submit_order",
            symbol=symbol,
            quantity=1,
            side="buy",
            order_type="market",
        )
        results["open_orders_after"] = _invoke_tool(request, events, "orders_open_orders")

        type(self).last_results = results
        summary = "AI committee built-in tool smoke completed."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class CommitteeToolBacktestStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"
        self.asset = Asset("AAPL", Asset.AssetType.STOCK)
        self.notifications.enabled = True
        self.notifications.configure_telegram(bot_token="test-token", chat_id="test-chat")
        self.agents.create(
            name="committee",
            system_prompt="Use every read-only research tool, record memory, notify, then place one final long-only order.",
            model="stub-committee-tools",
            _runtime=CommitteeToolRuntime(),
        )

    def on_trading_iteration(self):
        if self.vars.get("ran_committee_tools"):
            return
        self.vars.ran_committee_tools = True
        result = self.agents["committee"].run(context={"symbol": self.asset.symbol})
        self.vars.committee_summary = result.summary


def _build_committee_pandas_data():
    index = pd.date_range("2025-01-06 09:30", periods=8, freq="min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
            "high": [101.0, 101.2, 101.8, 102.2, 102.8, 103.2, 103.8, 104.2],
            "low": [99.5, 100.0, 100.4, 101.0, 101.5, 102.0, 102.4, 103.0],
            "close": [100.4, 100.9, 101.4, 101.9, 102.4, 102.9, 103.4, 103.9],
            "volume": [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700],
        },
        index=index,
    )
    asset = Asset("AAPL", Asset.AssetType.STOCK)
    return {asset: Data(asset, df, timestep="minute")}


def _fake_sec_get(url, **kwargs):
    if url.endswith("company_tickers.json"):
        return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
    if "companyfacts" in url:
        return _Response(
            payload={
                "facts": {
                    "us-gaap": {
                        "RevenueFromContractWithCustomerExcludingAssessedTax": {
                            "units": {"USD": [{"val": 383285000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                        "NetIncomeLoss": {
                            "units": {"USD": [{"val": 93736000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                        "Assets": {
                            "units": {"USD": [{"val": 364980000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                        "Liabilities": {
                            "units": {"USD": [{"val": 308030000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                        "NetCashProvidedByUsedInOperatingActivities": {
                            "units": {"USD": [{"val": 118254000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                        "PaymentsToAcquirePropertyPlantAndEquipment": {
                            "units": {"USD": [{"val": 9447000000, "filed": "2024-11-01", "form": "10-K", "fy": 2024, "fp": "FY", "accn": "0000320193-24-000123"}]}
                        },
                    }
                }
            }
        )
    if "submissions" in url:
        return _Response(
            payload={
                "cik": "0000320193",
                "filings": {
                    "recent": {
                        "form": ["10-K", "10-Q"],
                        "accessionNumber": ["0000320193-24-000123", "0000320193-26-000001"],
                        "filingDate": ["2024-11-01", "2026-01-30"],
                        "reportDate": ["2024-09-30", "2025-12-31"],
                        "acceptanceDateTime": ["2024-11-01T12:00:00.000Z", "2026-01-30T12:00:00.000Z"],
                        "primaryDocument": ["aapl-20240930.htm", "aapl-20251231.htm"],
                        "primaryDocDescription": ["10-K", "10-Q"],
                    }
                },
            }
        )
    if "Archives/edgar/data" in url:
        return _Response(text="<html><body>Customer concentration risk is low. Services margin resilience remains important.</body></html>")
    raise AssertionError(url)


def _fake_alpaca_news_get(url, headers=None, params=None, timeout=None):
    assert "data.alpaca.markets/v1beta1/news" in url
    return _Response(
        payload={
            "news": [
                {
                    "id": 1,
                    "headline": "AAPL committee smoke headline",
                    "summary": "AAPL news summary",
                    "source": "fixture",
                    "created_at": "2025-01-06T14:30:00Z",
                    "updated_at": "2025-01-06T14:30:00Z",
                    "url": "https://example.com/aapl",
                    "symbols": ["AAPL"],
                }
            ],
            "next_page_token": None,
        }
    )


def _fake_fred_get(url, params=None, **kwargs):
    if "series/observations" in url:
        series_id = params["series_id"]
        assert params["api_key"] == "fixture-fred-key"
        assert params["observation_end"] == "2025-01-06"
        assert params["realtime_start"] == "2025-01-06"
        assert params["realtime_end"] == "2025-01-06"
        if series_id == "DGS10":
            return _Response(
                payload={
                    "observations": [
                        {"date": "2025-01-03", "value": "4.50", "realtime_start": "2025-01-06", "realtime_end": "2025-01-06"},
                        {"date": "2025-01-06", "value": "4.60", "realtime_start": "2025-01-06", "realtime_end": "2025-01-06"},
                        {"date": "2025-01-07", "value": "4.70", "realtime_start": "2025-01-06", "realtime_end": "2025-01-06"},
                    ]
                }
            )
        if series_id == "UNRATE":
            return _Response(
                payload={
                    "observations": [
                        {"date": "2024-12-01", "value": "4.1", "realtime_start": "2025-01-06", "realtime_end": "2025-01-06"},
                        {"date": "2025-02-01", "value": "4.2", "realtime_start": "2025-01-06", "realtime_end": "2025-01-06"},
                    ]
                }
            )
        raise AssertionError(series_id)

    raise AssertionError(f"unexpected FRED URL: {url}")


def _fake_requests_get(url, **kwargs):
    if "data.alpaca.markets/v1beta1/news" in url:
        return _fake_alpaca_news_get(url, **kwargs)
    if "api.stlouisfed.org/fred" in url:
        return _fake_fred_get(url, **kwargs)
    return _fake_sec_get(url, **kwargs)


def _fake_telegram_post(url, json=None, timeout=None):
    assert url == "https://api.telegram.org/bottest-token/sendMessage"
    assert json["chat_id"] == "test-chat"
    assert "Backtest smoke bought one share." in json["text"]
    return _Response(payload={"ok": True}, content=b'{"ok": true}')


@pytest.mark.usefixtures("disable_datasource_override")
def test_ai_committee_builtin_tools_execute_inside_backtest(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setenv("LUMIBOT_MEMORY_DIR", str(tmp_path / "memory"))
    monkeypatch.setenv("LUMIBOT_SEC_CACHE_DIR", str(tmp_path / "sec"))
    monkeypatch.setenv("LUMIBOT_FRED_CACHE_DIR", str(tmp_path / "fred"))
    monkeypatch.setenv("FRED_API_KEY", "fixture-fred-key")
    monkeypatch.setenv("ALPACA_NEWS_API_KEY", "fixture-key")
    monkeypatch.setenv("ALPACA_NEWS_API_SECRET", "fixture-secret")
    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", _fake_requests_get)
    monkeypatch.setattr("lumibot.macro.fred.requests.get", _fake_fred_get)
    monkeypatch.setattr("lumibot.components.agents.builtins.requests.get", _fake_requests_get)
    monkeypatch.setattr("lumibot.components.notifications.telegram.requests.post", _fake_telegram_post)

    CommitteeToolRuntime.call_count = 0
    CommitteeToolRuntime.last_results = None
    _, strategy = CommitteeToolBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6, 9, 30),
        backtesting_end=datetime(2025, 1, 6, 9, 37),
        pandas_data=_build_committee_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )

    assert CommitteeToolRuntime.call_count == 1
    results = CommitteeToolRuntime.last_results
    assert strategy.vars.committee_summary == "AI committee built-in tool smoke completed."
    assert results["last_price"]["price"] == pytest.approx(100.0)
    assert results["query"]["rows"][0]["rows_seen"] >= 1
    assert results["news"]["ok"] is True
    assert results["news"]["articles"][0]["headline"] == "AAPL committee smoke headline"
    assert results["indicator"]["ok"] is True
    assert results["indicator"]["no_lookahead"] is True
    assert results["income"]["values"]["revenue"]["value"] == 383285000000
    assert results["balance"]["values"]["assets"]["value"] == 364980000000
    assert results["cash_flow"]["values"]["free_cash_flow"]["derived"] is True
    assert results["facts"]["facts"]["NetIncomeLoss"]["value"] == 93736000000
    assert results["fred_list"]["series"]
    assert results["fred_latest"]["latest"]["value"] == 4.6
    assert results["fred_snapshot"]["values"]["DGS10"]["value"] == 4.6
    assert results["fred_snapshot"]["values"]["UNRATE"]["value"] == 4.1
    assert len(results["filings"]["filings"]) == 1
    assert results["filings"]["filings"][0]["form"] == "10-K"
    assert results["filing_search"]["match_count"] >= 1
    assert "Services margin resilience" in results["filing_document"]["text"]
    assert results["memory_search"]["count"] >= 1
    assert results["notify"]["ok"] is True
    assert results["order"]["order"]["side"] == "buy"
    assert strategy.get_position(Asset("AAPL", Asset.AssetType.STOCK)) is not None

    memory_files = list((Path(tmp_path) / "memory").rglob("*.jsonl"))
    assert memory_files
    assert any("Committee bought one share" in path.read_text() for path in memory_files)

    summary_file = Path(tmp_path / "cache" / "agent_runtime" / "agent_run_summaries.jsonl")
    assert summary_file.exists()
    summary_rows = [json.loads(line) for line in summary_file.read_text().splitlines()]
    assert summary_rows[-1]["agent_name"] == "committee"
