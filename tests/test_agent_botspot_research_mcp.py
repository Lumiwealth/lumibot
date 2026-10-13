import asyncio
import inspect
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import lumibot.components.agents.runtime as runtime_module
from lumibot.components.agents import AgentManager, MCPServer


class _Vars(dict):
    def set(self, key, value):
        self[key] = value


class _Strategy:
    is_backtesting = True

    def __init__(self):
        self.parameters = {}
        self.vars = _Vars()
        self.messages = []

    def get_datetime(self):
        return datetime(2025, 1, 6, 15, 30, tzinfo=timezone.utc)

    def log_message(self, message, *args, **kwargs):
        self.messages.append(str(message))


class _Runtime:
    def run(self, request):  # pragma: no cover - these tests inspect configuration only
        raise AssertionError("model execution was not expected")


def _configure_hosted_research(monkeypatch):
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_URL", "https://api.test.botspot.trade/research-mcp")
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_TOKEN", "research-token")
    monkeypatch.setenv(
        "BOTSPOT_RESEARCH_MCP_RENEW_URL",
        "https://api.test.botspot.trade/saved-secrets/research-runtime-capabilities/renew",
    )


def test_hosted_research_capability_auto_attaches_exact_read_only_tools(monkeypatch):
    _configure_hosted_research(monkeypatch)

    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())

    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    assert server.exposed_tools == [
        "search_data_catalog",
        "query_data",
        "search_documents",
        "get_document",
    ]
    assert server.auth_token_env == "BOTSPOT_RESEARCH_MCP_TOKEN"
    assert server.auth_token_refresh_url.endswith("/research-runtime-capabilities/renew")


def test_hosted_research_tools_expose_the_remote_mcp_contract_to_the_model(monkeypatch):
    _configure_hosted_research(monkeypatch)
    monkeypatch.setattr(
        runtime_module,
        "list_mcp_tools",
        lambda _server: [
            {
                "name": "query_data",
                "description": "Query a registered public research dataset.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "datasetId": {"type": "string"},
                        "timeRange": {"type": "object"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["datasetId"],
                    "additionalProperties": False,
                },
            }
        ],
    )

    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    query_data = next(tool for tool in handle._ensure_bound_tools() if tool.name == "query_data")
    parameters = inspect.signature(query_data.function).parameters

    assert list(parameters) == ["datasetId", "timeRange", "limit"]
    assert parameters["datasetId"].default is inspect.Parameter.empty
    assert parameters["timeRange"].default is None
    assert parameters["limit"].default is None
    assert '"datasetId"' in query_data.description
    assert '"additionalProperties": false' in query_data.description


def test_remote_mcp_contract_failure_warns_once_and_keeps_legacy_payload_shape(monkeypatch):
    _configure_hosted_research(monkeypatch)
    calls = []

    def fail_to_list(_server):
        calls.append("list")
        raise RuntimeError("contract endpoint unavailable")

    monkeypatch.setattr(runtime_module, "list_mcp_tools", fail_to_list)
    strategy = _Strategy()
    manager = AgentManager(strategy)

    first = manager.create(name="first", _runtime=_Runtime())
    second = manager.create(name="second", _runtime=_Runtime())
    first_query = next(tool for tool in first._ensure_bound_tools() if tool.name == "query_data")
    second._ensure_bound_tools()

    assert list(inspect.signature(first_query.function).parameters) == ["payload"]
    assert calls == ["list"]
    assert len([message for message in strategy.messages if "Could not load tool contracts" in message]) == 1


def test_partial_hosted_configuration_warns_once_without_changing_ordinary_execution(monkeypatch):
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_URL", "https://api.test.botspot.trade/research-mcp")
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_TOKEN", raising=False)
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_RENEW_URL", raising=False)
    strategy = _Strategy()
    manager = AgentManager(strategy)

    first = manager.create(name="first", _runtime=_Runtime())
    second = manager.create(name="second", _runtime=_Runtime())

    assert first._mcp_servers == []
    assert second._mcp_servers == []
    assert len([message for message in strategy.messages if "partially configured" in message]) == 1


def test_unlinked_external_runtime_advertises_optional_capability_once(monkeypatch):
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_URL", raising=False)
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_TOKEN", raising=False)
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_RENEW_URL", raising=False)
    strategy = _Strategy()
    manager = AgentManager(strategy)

    manager.create(name="first", _runtime=_Runtime())
    manager.create(name="second", _runtime=_Runtime())

    notices = [message for message in strategy.messages if "external LumiBot users" in message]
    assert len(notices) == 1
    assert "Ordinary LumiBot tools and strategy execution remain available" in notices[0]


def test_backtest_research_calls_receive_a_hard_point_in_time_ceiling(monkeypatch):
    _configure_hosted_research(monkeypatch)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")

    assert (
        handle._bound_remote_tool_payload(
            server,
            "query_data",
            {"datasetId": "bls.public_series", "timeRange": {"endDate": "2026-01-01"}},
        )["timeRange"]["endDate"]
        == "2025-01-06"
    )
    assert (
        handle._bound_remote_tool_payload(
            server,
            "search_documents",
            {"datasetId": "sec.filings"},
        )["timeRange"]["endDate"]
        == "2025-01-06"
    )
    assert (
        handle._bound_remote_tool_payload(
            server,
            "get_document",
            {"datasetId": "sec.filings", "documentId": "cik:accession:file"},
        )["asOf"]
        == "2025-01-06T15:30:00Z"
    )


@pytest.mark.parametrize("offset", ["-05:00", "-0500"])
@pytest.mark.parametrize("tool_name", ["query_data", "search_documents", "get_document"])
def test_research_preserves_intraday_ceiling_and_earlier_requested_bound(monkeypatch, tool_name, offset):
    _configure_hosted_research(monkeypatch)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    requested = f"2025-01-06T10:00:00{offset}"
    payload = {"datasetId": "sec.filings", "asOf": requested}
    result = handle._bound_remote_tool_payload(server, tool_name, payload)
    assert result["asOf"] == "2025-01-06T15:00:00Z"
    assert payload["asOf"] == requested
    result = handle._bound_remote_tool_payload(server, tool_name, {"datasetId": "sec.filings"})
    assert result["asOf"] == "2025-01-06T15:30:00Z"


@pytest.mark.parametrize("clock", [None, datetime(2025, 1, 6, 15, 30), "2025-01-06"])
def test_research_rejects_unknown_or_naive_simulated_clock(monkeypatch, clock):
    _configure_hosted_research(monkeypatch)
    strategy = _Strategy()
    strategy.get_datetime = lambda: clock
    handle = AgentManager(strategy).create(name="researcher", _runtime=_Runtime())
    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    with pytest.raises(RuntimeError, match="timezone-aware simulated datetime"):
        handle._bound_remote_tool_payload(server, "query_data", {"datasetId": "bls.public_series"})


@pytest.mark.parametrize("bound", ["2025-01-06T15:00:00", "January 6", "2025-02-31"])
def test_research_rejects_malformed_requested_ceiling(monkeypatch, bound):
    _configure_hosted_research(monkeypatch)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    with pytest.raises(ValueError, match="asOf"):
        handle._bound_remote_tool_payload(server, "get_document", {"asOf": bound})


def test_research_legacy_date_bound_cannot_overwrite_an_earlier_ceiling(monkeypatch):
    _configure_hosted_research(monkeypatch)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    result = handle._bound_remote_tool_payload(server, "get_document", {"asOf": "2025-01-03"})
    assert result["asOf"] == "2025-01-03T23:59:59.999999Z"


def test_refresh_endpoint_must_share_the_mcp_origin():
    with pytest.raises(ValueError, match="same origin"):
        MCPServer(
            name="unsafe",
            transport="streamable_http",
            url="https://api.botspot.trade/research-mcp",
            exposed_tools=["query_data"],
            auth_token_env="BOTSPOT_RESEARCH_MCP_TOKEN",
            auth_token_refresh_url="https://attacker.example/renew",
        )


def test_expired_research_token_is_replaced_without_exposing_other_credentials(monkeypatch):
    _configure_hosted_research(monkeypatch)
    calls = []

    class _Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"accessToken": "replacement-research-token"}

    class _Client:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            calls.append((url, kwargs))
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", _Client)
    server = MCPServer(
        name="botspot_research",
        transport="streamable_http",
        url=os.environ["BOTSPOT_RESEARCH_MCP_URL"],
        exposed_tools=["query_data"],
        auth_token_env="BOTSPOT_RESEARCH_MCP_TOKEN",
        auth_token_refresh_url=os.environ["BOTSPOT_RESEARCH_MCP_RENEW_URL"],
    )

    assert asyncio.run(runtime_module._refresh_mcp_auth_token(server, "research-token")) is True
    assert os.environ["BOTSPOT_RESEARCH_MCP_TOKEN"] == "replacement-research-token"
    request = calls[-1]
    assert request[1]["headers"] == {
        "Authorization": "Bearer research-token",
        "Accept": "application/json",
    }


@pytest.fixture
def recorded_research_server(monkeypatch):
    from scripts import agent_eval_research_server

    monkeypatch.setattr(agent_eval_research_server, "unavailable", False)
    return agent_eval_research_server.server


def _recorded_research_call(server, name, payload):
    result = asyncio.run(server.call_tool(name, payload))
    if hasattr(result, "isError"):
        return result
    if isinstance(result, tuple):
        return result[1]
    return json.loads(next(content.text for content in result if content.type == "text"))


def test_recorded_research_schemas_match_the_generic_node_input_fields(recorded_research_server):
    tools = {tool.name: tool for tool in asyncio.run(recorded_research_server.list_tools())}
    fields = {
        "query_data": {"asOf", "datasetId", "entity", "entities", "metrics", "timeRange", "filters", "limit"},
        "search_documents": {"asOf", "datasetId", "entity", "query", "documentTypes", "timeRange", "limit"},
        "get_document": {"asOf", "datasetId", "documentId", "section"},
    }
    required = {"query_data": ["datasetId"], "search_documents": [], "get_document": ["datasetId", "documentId"]}
    for name, expected in fields.items():
        schema = tools[name].inputSchema
        assert set(schema["properties"]) == expected
        assert schema.get("required", []) == required[name]
        assert schema["properties"]["asOf"]["type"] == "string"
        assert "asOf" not in schema.get("required", [])


@pytest.mark.parametrize(
    "tool,dataset",
    [
        ("query_data", "bls.public_series"),
        ("search_documents", "sec.filings"),
        ("get_document", "sec.filings"),
    ],
)
@pytest.mark.parametrize(
    "as_of",
    [
        "2026-08-11T14:35:00Z",
        "2026-08-11T10:35:00-04:00",
        "2026-08-11T14:35Z",
        "2026-08-11T14:35:00+0530",
    ],
)
def test_recorded_research_refuses_unqualified_exact_instants_without_truncation(
    recorded_research_server, tool, dataset, as_of
):
    payload = {"datasetId": dataset, "asOf": as_of}
    if tool == "search_documents":
        payload["entity"] = "ACME"
    elif tool == "get_document":
        payload["documentId"] = "0000123456:0000123456-26-000042:acme-20260630.htm"
    result = _recorded_research_call(recorded_research_server, tool, payload)
    assert result.isError is True
    assert result.structuredContent["errorCode"] == "unsupported_capability"
    assert result.structuredContent["details"] == {
        "datasetId": dataset,
        "asOf": as_of,
        "requiredCapability": "original_vintage_at_utc_instant",
        "pointInTimeQualified": False,
        "remedy": "Use a qualified original-release archive. Calendar bounds do not establish historical availability.",
    }
    assert "rows" not in result.structuredContent
    assert "text" not in result.structuredContent


@pytest.mark.parametrize("tool", ["query_data", "search_documents"])
def test_recorded_query_search_calendar_asof_is_not_original_vintage_replay(recorded_research_server, tool):
    result = _recorded_research_call(
        recorded_research_server,
        tool,
        {
            "datasetId": "bls.public_series" if tool == "query_data" else "sec.filings",
            "entity": "ACME",
            "asOf": "2024-02-29",
        },
    )
    assert result.isError is True
    assert result.structuredContent["errorCode"] == "unsupported_capability"


@pytest.mark.parametrize("tool", ["query_data", "search_documents", "get_document"])
@pytest.mark.parametrize(
    "as_of",
    [
        "",
        "2026-02-30",
        "2026-08-11T14:35:00",
        "2026-08-11 14:35:00Z",
        "August 11",
        "2026-08-11T25:00:00Z",
    ],
)
def test_recorded_research_invalid_asof_is_a_visible_tool_error(recorded_research_server, tool, as_of):
    payload = {"datasetId": "sec.filings", "asOf": as_of, "entity": "ACME"}
    if tool == "query_data":
        payload["datasetId"] = "bls.public_series"
    if tool == "get_document":
        payload["documentId"] = "0000123456:0000123456-26-000042:acme-20260630.htm"
    result = _recorded_research_call(recorded_research_server, tool, payload)
    assert result.isError is True
    assert result.structuredContent["errorCode"] == "invalid_request"


def test_recorded_get_preserves_only_known_legacy_calendar_filing_evidence(recorded_research_server):
    result = _recorded_research_call(
        recorded_research_server,
        "get_document",
        {
            "datasetId": "sec.filings",
            "documentId": "0000123456:0000123456-26-000042:acme-20260630.htm",
            "asOf": "2026-08-11",
        },
    )
    assert result["asOf"] == "2026-08-11"
    assert result["filingDate"] == "2026-08-07"
    assert result["pointInTimeQualified"] is False
    assert result["availabilityPolicy"] == "recorded_calendar_filing_date_only"
    assert "IGNORE ALL PRIOR RULES" in result["text"]
    before = _recorded_research_call(
        recorded_research_server,
        "get_document",
        {
            "datasetId": "sec.filings",
            "documentId": result["documentId"],
            "asOf": "2026-08-06",
        },
    )
    assert before.isError is True
    assert before.structuredContent["errorCode"] == "invalid_request"


def test_recorded_period_selection_preserves_values_without_claiming_an_asof(recorded_research_server):
    result = _recorded_research_call(
        recorded_research_server,
        "query_data",
        {
            "datasetId": "bls.public_series",
            "timeRange": {"startDate": "2025-01-01", "endDate": "2025-01-31"},
        },
    )
    assert result["rows"][0]["value"] == 317.671
    assert result["rows"][0]["releaseDate"] == "2025-02-12"
    assert result["asOf"] is None
    assert result["pointInTimeQualified"] is False
    excluded = _recorded_research_call(
        recorded_research_server,
        "query_data",
        {
            "datasetId": "bls.public_series",
            "timeRange": {"startDate": "2026-01-01", "endDate": "2026-08-11"},
        },
    )
    assert excluded["rows"] == []


@pytest.mark.parametrize("tool", ["query_data", "search_documents", "get_document"])
def test_recorded_research_asof_is_optional_but_explicit_null_is_not_accepted(recorded_research_server, tool):
    from mcp.server.fastmcp.exceptions import ToolError

    payload = {"datasetId": "sec.filings", "asOf": None, "entity": "ACME"}
    if tool == "query_data":
        payload["datasetId"] = "bls.public_series"
    elif tool == "get_document":
        payload["documentId"] = "0000123456:0000123456-26-000042:acme-20260630.htm"
    with pytest.raises(ToolError):
        _recorded_research_call(recorded_research_server, tool, payload)


def test_recorded_research_calendar_schema_rejects_impossible_range_dates(recorded_research_server):
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError):
        _recorded_research_call(
            recorded_research_server,
            "query_data",
            {
                "datasetId": "bls.public_series",
                "timeRange": {"endDate": "2026-02-30"},
            },
        )


def test_recorded_research_stdio_exposes_asof_and_preserves_native_tool_error_status():
    # Real stdio discovery/transport, deterministic recorded fixture, no model.
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    async def check():
        parameters = StdioServerParameters(
            command=sys.executable,
            args=[
                str(Path(__file__).resolve().parents[1] / "scripts/agent_eval_research_server.py"),
                "research_available",
            ],
        )
        async with stdio_client(parameters) as streams:
            async with ClientSession(*streams) as session:
                await session.initialize()
                tools = await session.list_tools()
                query = next(tool for tool in tools.tools if tool.name == "query_data")
                assert query.inputSchema["properties"]["asOf"]["type"] == "string"
                return await session.call_tool(
                    "query_data",
                    {
                        "datasetId": "bls.public_series",
                        "asOf": "2026-08-11T14:35:00Z",
                    },
                )

    result = asyncio.run(check())
    assert result.isError is True
    assert result.structuredContent["errorCode"] == "unsupported_capability"
    assert result.structuredContent["details"]["asOf"] == "2026-08-11T14:35:00Z"
