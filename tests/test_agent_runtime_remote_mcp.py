import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult, MCPServer
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def _utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event(kind: str, *, text: str | None = None, tool_name: str | None = None, payload: dict | None = None):
    from lumibot.components.agents import AgentTraceEvent

    return AgentTraceEvent(
        kind=kind,
        text=text,
        tool_name=tool_name,
        payload=payload,
        timestamp=_utc_iso_timestamp(),
    )


class RemoteMCPRuntime:
    def run(self, request):
        tool_map = {tool.name: tool.function for tool in request.bound_tools}
        events = [_event("tool_call", tool_name="echo_market_state", payload={"payload": {"symbol": "RMCP"}})]
        result = tool_map["echo_market_state"]({"symbol": "RMCP"})
        payload = result if isinstance(result, dict) else {"value": result}
        events.append(_event("tool_result", tool_name="echo_market_state", payload=payload))
        summary = f"Remote MCP said: {payload.get('structuredContent', {}).get('message', 'ok') if isinstance(payload, dict) else 'ok'}"
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class RemoteMCPStrategy(Strategy):
    def log_message(self, message, *args, **kwargs):
        warnings = self.vars.get("agent_warnings", [])
        if "[agents] external MCP tool" in str(message):
            warnings.append(str(message))
            self.vars.agent_warnings = warnings
        return super().log_message(message, *args, **kwargs)

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="research",
            system_prompt="Call the external MCP tool once.",
            default_model="stub-remote",
            tools=[],
            mcp_servers=[
                MCPServer(
                    name="remote",
                    url=self.parameters["mcp_url"],
                    allowed_tools=["echo_market_state"],
                )
            ],
            _runtime=RemoteMCPRuntime(),
        )

    def on_trading_iteration(self):
        self.vars.agent_result = self.agents["research"].run(context={"symbol": "RMCP"}).summary


class _MCPHandler(BaseHTTPRequestHandler):
    calls = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        data = json.loads(body.decode("utf-8"))
        method = data.get("method")
        if method == "tools/list":
            response = {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "result": {
                    "tools": [
                        {
                            "name": "echo_market_state",
                            "description": "Echo a small structured market state payload.",
                        }
                    ]
                },
            }
        elif method == "tools/call":
            self.__class__.calls.append(data)
            response = {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "result": {
                    "structuredContent": {
                        "message": "stub-mcp-ok",
                        "arguments": data.get("params", {}).get("arguments", {}),
                    }
                },
            }
        else:
            response = {"jsonrpc": "2.0", "id": data.get("id"), "error": {"message": "unknown method"}}
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):  # noqa: A003
        return


@pytest.fixture
def mcp_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MCPHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    _MCPHandler.calls = []
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/mcp"
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.usefixtures("disable_datasource_override")
def test_external_mcp_tool_is_allowlisted_and_invoked(monkeypatch, tmp_path, mcp_server):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    index = pd.date_range("2025-01-06", periods=2, freq="D", tz="America/New_York")
    asset = Asset("RMCP", Asset.AssetType.STOCK)
    df = pd.DataFrame(
        {
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.8, 10.1],
            "close": [10.2, 10.8],
            "volume": [1000, 1000],
        },
        index=index,
    )
    _, strategy = RemoteMCPStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data={asset: Data(asset, df, timestep="day")},
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        parameters={"mcp_url": mcp_server},
    )
    assert _MCPHandler.calls
    assert strategy.vars.agent_result == "Remote MCP said: stub-mcp-ok"
    assert strategy.vars.agent_warnings


@pytest.mark.usefixtures("disable_datasource_override")
def test_external_mcp_warm_replay_avoids_second_provider_call(monkeypatch, tmp_path, mcp_server):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    index = pd.date_range("2025-01-06", periods=2, freq="D", tz="America/New_York")
    asset = Asset("RMCP", Asset.AssetType.STOCK)
    df = pd.DataFrame(
        {
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.8, 10.1],
            "close": [10.2, 10.8],
            "volume": [1000, 1000],
        },
        index=index,
    )

    _MCPHandler.calls = []
    RemoteMCPStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data={asset: Data(asset, df, timestep="day")},
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        parameters={"mcp_url": mcp_server},
    )
    cold_call_count = len(_MCPHandler.calls)
    assert cold_call_count > 0

    RemoteMCPStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data={asset: Data(asset, df, timestep="day")},
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        parameters={"mcp_url": mcp_server},
    )
    assert len(_MCPHandler.calls) == cold_call_count
