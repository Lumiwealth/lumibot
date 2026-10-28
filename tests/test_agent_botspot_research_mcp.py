import asyncio
import inspect
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import lumibot.components.agents.manager as manager_module
import lumibot.components.agents.runtime as runtime_module
from lumibot.components.agents import AgentManager, MCPServer

_RESEARCH_PRESET_TOOL_NAMES = [
    "search_data_catalog",
    "query_data",
    "search_documents",
    "get_document",
    "list_marketplace_strategies",
    "get_marketplace_strategy",
    "get_marketplace_strategy_code",
    "list_marketplace_accounts",
    "get_marketplace_account",
    "community",
]


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
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_URL", "https://research.example.test/research-mcp")
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_TOKEN", "research-token")
    monkeypatch.setenv(
        "BOTSPOT_RESEARCH_MCP_RENEW_URL",
        "https://research.example.test/renew",
    )


def test_hosted_research_capability_auto_attaches_the_shared_research_preset(monkeypatch):
    _configure_hosted_research(monkeypatch)

    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())

    server = next(server for server in handle._mcp_servers if server.name == "botspot_research")
    # The shared Research preset now includes Marketplace and Community reads.
    # Community is one mixed-action tool; its write scopes are enforced by Node.
    assert server.exposed_tools == _RESEARCH_PRESET_TOOL_NAMES
    assert server.auth_token_env == "BOTSPOT_RESEARCH_MCP_TOKEN"
    assert server.auth_token_refresh_url.endswith("/renew")


@pytest.mark.parametrize("renew_url", [None, "", "  "])
def test_external_static_research_key_auto_attaches_without_a_renewal_endpoint(monkeypatch, renew_url):
    _configure_hosted_research(monkeypatch)
    if renew_url is None:
        monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_RENEW_URL")
    else:
        monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_RENEW_URL", renew_url)
    strategy = _Strategy()

    handle = AgentManager(strategy).create(name="researcher", _runtime=_Runtime())

    assert len(handle._mcp_servers) == 1
    server = handle._mcp_servers[0]
    assert server.name == "botspot_research"
    assert server.exposed_tools == _RESEARCH_PRESET_TOOL_NAMES
    assert server.auth_token_env == "BOTSPOT_RESEARCH_MCP_TOKEN"
    assert server.auth_token_refresh_url is None
    assert server.headers is None
    assert "research-token" not in repr(server)
    assert all("research-token" not in message for message in strategy.messages)
    assert asyncio.run(runtime_module._refresh_mcp_auth_token(server, "research-token")) is False


@pytest.mark.parametrize(
    "renew_url",
    ["https://attacker.example/renew", "http://research.example.test/renew", "not-a-url"],
)
def test_auto_attachment_rejects_a_supplied_unsafe_renewal_endpoint(monkeypatch, renew_url):
    _configure_hosted_research(monkeypatch)
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_RENEW_URL", renew_url)
    strategy = _Strategy()
    manager = AgentManager(strategy)

    first = manager.create(name="first", _runtime=_Runtime())
    second = manager.create(name="second", _runtime=_Runtime())

    assert first._mcp_servers == second._mcp_servers == []
    warnings = [message for message in strategy.messages if "configuration is invalid" in message]
    assert len(warnings) == 1
    assert "research-token" not in warnings[0]


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://research.example.test/research-mcp",
        "https://user:private-url-secret@research.example.test/research-mcp",
        "https://research.example.test/research-mcp?region=us&tenant=research",
        "https://research.example.test/research-mcp#research",
        "http://10.0.0.10:8080/research-mcp?region=us",
    ],
)
def test_external_research_connection_preserves_supported_mcp_endpoint_configuration(monkeypatch, endpoint):
    # Rob removed the extra BotSpot-specific URL policy. Normal MCP transport
    # owns endpoint handling; private HTTP and query parameters are valid inputs.
    _configure_hosted_research(monkeypatch)
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_URL", endpoint)
    monkeypatch.delenv("BOTSPOT_RESEARCH_MCP_RENEW_URL")
    strategy = _Strategy()
    manager = AgentManager(strategy)

    handle = manager.create(name="researcher", _runtime=_Runtime())
    assert len(handle._mcp_servers) == 1
    assert handle._mcp_servers[0].url == endpoint
    assert handle._mcp_servers[0].auth_token_env == "BOTSPOT_RESEARCH_MCP_TOKEN"
    assert not any("configuration is invalid" in message for message in strategy.messages)


@pytest.mark.parametrize("scope,parent_post_id", [("community:post", None), ("community:reply", "public-post")])
def test_mixed_community_tool_preserves_server_write_denial(monkeypatch, scope, parent_post_id):
    _configure_hosted_research(monkeypatch)
    calls = []
    monkeypatch.setattr(
        runtime_module,
        "list_mcp_tools",
        lambda _server: [
            {
                "name": "community",
                "description": "Read the feed or publish only with an explicit posting grant.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string", "enum": ["feed", "post"]},
                        "body": {"type": "string"},
                        "parentPostId": {"type": "string"},
                    },
                    "required": ["action"],
                },
            }
        ],
    )
    denial = runtime_module.RemoteMCPToolError(
        "community",
        {"isError": True, "structuredContent": {"errorCode": "forbidden", "message": f"Missing {scope}"}},
    )

    def denied_by_server(server, name, payload):
        calls.append((server.name, name, payload))
        raise denial

    # This is a client propagation contract, not proof of server authorization.
    # No local intent gate or live Community write is involved.
    monkeypatch.setattr(runtime_module, "call_mcp_tool", denied_by_server)
    monkeypatch.setattr(manager_module, "_RUNTIME_IMPORTS", None)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    community = next(tool for tool in handle._ensure_bound_tools() if tool.name == "community")
    assert list(inspect.signature(community.function).parameters) == ["action", "body", "parentPostId"]
    payload = {"action": "post", "body": "Public research summary."}
    if parent_post_id is not None:
        payload["parentPostId"] = parent_post_id

    with pytest.raises(runtime_module.RemoteMCPToolError) as raised:
        community.function(**payload)

    assert raised.value is denial
    assert raised.value.code == "forbidden"
    assert calls == [("botspot_research", "community", payload)]


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
    monkeypatch.setenv("BOTSPOT_RESEARCH_MCP_URL", "https://research.example.test/research-mcp")
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
    from lumibot.components.agents import botspot_hint

    # The hint is once per process and silent in CI; isolate it for this test.
    for name in ("LUMIBOT_NO_BOTSPOT_HINT", "DO_NOT_TRACK", "CI", "GITHUB_ACTIONS", "JENKINS_URL"):
        monkeypatch.delenv(name, raising=False)
    botspot_hint._reset_for_tests()
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
            {"datasetId": "sec.filings", "timeRange": {"endDate": "2026-01-01"}},
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


@pytest.mark.parametrize("tool_name", ["query_data", "search_documents"])
def test_backtest_research_does_not_invent_an_optional_period_selector(monkeypatch, tool_name):
    # Some registered releases accept asOf but do not accept period selection.
    # Preserve the exact temporal ceiling without adding an unsupported field.
    _configure_hosted_research(monkeypatch)
    handle = AgentManager(_Strategy()).create(name="researcher", _runtime=_Runtime())
    server = handle._mcp_servers[0]
    payload = {"datasetId": "provider.release", "asOf": "2025-01-03"}

    result = handle._bound_remote_tool_payload(server, tool_name, payload)

    assert result == {"datasetId": "provider.release", "asOf": "2025-01-03T23:59:59.999999Z"}
    assert payload == {"datasetId": "provider.release", "asOf": "2025-01-03"}


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
        "search_data_catalog": {"query", "source", "limit"},
        "query_data": {"asOf", "datasetId", "entity", "entities", "metrics", "timeRange", "filters", "limit"},
        "search_documents": {"asOf", "datasetId", "entity", "query", "documentTypes", "timeRange", "limit"},
        "get_document": {"asOf", "datasetId", "documentId", "section"},
    }
    required = {
        "search_data_catalog": [],
        "query_data": ["datasetId"],
        "search_documents": [],
        "get_document": ["datasetId", "documentId"],
    }
    for name, expected in fields.items():
        schema = tools[name].inputSchema
        assert set(schema["properties"]) == expected
        assert schema.get("required", []) == required[name]
        if name != "search_data_catalog":
            assert schema["properties"]["asOf"]["type"] == "string"
            assert "asOf" not in schema.get("required", [])
    catalog = tools["search_data_catalog"].inputSchema["properties"]
    assert catalog["query"]["maxLength"] == 200
    assert catalog["source"]["enum"] == ["SEC", "Treasury", "BLS", "BEA", "OFR", "CFTC", "FRB", "BotSpot"]
    assert catalog["limit"]["minimum"] == 1
    assert catalog["limit"]["maximum"] == 50


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


def test_recorded_get_refuses_calendar_bound_without_last_modified_metadata(recorded_research_server):
    result = _recorded_research_call(
        recorded_research_server,
        "get_document",
        {
            "datasetId": "sec.filings",
            "documentId": "0000123456:0000123456-26-000042:acme-20260630.htm",
            "asOf": "2026-08-11",
        },
    )
    # Node's archive-index last-modified check cannot be replaced by filingDate.
    assert result.isError is True
    assert result.structuredContent["errorCode"] == "invalid_request"
    assert result.structuredContent["message"] == (
        "The document availability date could not be verified for the requested asOf bound"
    )
    before = _recorded_research_call(
        recorded_research_server,
        "get_document",
        {
            "datasetId": "sec.filings",
            "documentId": "0000123456:0000123456-26-000042:acme-20260630.htm",
            "asOf": "2026-08-06",
        },
    )
    assert before.isError is True
    assert before.structuredContent["errorCode"] == "invalid_request"


def test_recorded_catalog_matches_node_ids_and_unqualified_temporal_capabilities(recorded_research_server):
    result = _recorded_research_call(recorded_research_server, "search_data_catalog", {})
    assert set(result) == {"datasets", "count"}
    assert result["count"] == len(result["datasets"])
    assert {item["id"] for item in result["datasets"]} == {"bls.public_series", "treasury.fiscaldata", "sec.filings"}
    for item in result["datasets"]:
        assert "datasetId" not in item
        assert item["source"] in {"BLS", "Treasury", "SEC"}
        assert item["temporalCapabilities"] == {
            "pointInTimeQualified": False,
            "originalVintageReplay": False,
            "exactPublicationTime": False,
            "historicalAvailability": "unqualified",
        }
    limited = _recorded_research_call(recorded_research_server, "search_data_catalog", {"limit": 1})
    assert limited["count"] == len(limited["datasets"]) == 1


@pytest.mark.parametrize(
    "dataset_id,title,description,categories",
    [
        (
            "bls.public_series",
            "BLS public time series",
            "Bureau of Labor Statistics time series for CPI, unemployment, payrolls, wages, and PPI.",
            ["macro", "labor", "inflation"],
        ),
        (
            "treasury.fiscaldata",
            "Treasury FiscalData",
            "Public U.S. Treasury FiscalData datasets including debt to the penny, average interest rates, "
            "and Daily Treasury Statement tables.",
            ["macro", "treasury", "fiscal"],
        ),
        (
            "sec.filings",
            "SEC filings",
            "Recent EDGAR filing metadata and document URLs for public companies and reporting entities.",
            ["documents", "sec", "filings"],
        ),
    ],
)
def test_recorded_catalog_discovery_wire_contains_actual_node_metadata(
    recorded_research_server, dataset_id, title, description, categories
):
    # Actual Node catalog metadata, not an invented available/qualified source.
    result = _recorded_research_call(recorded_research_server, "search_data_catalog", {})
    dataset = next(item for item in result["datasets"] if item["id"] == dataset_id)
    assert dataset["title"] == title
    assert dataset["description"] == description
    assert dataset["categories"] == categories
    assert dataset["temporalCapabilities"]["historicalAvailability"] == "unqualified"


@pytest.mark.parametrize(
    "payload,ids",
    [
        ({"query": "  CPI unemployment  "}, ["bls.public_series"]),
        ({"query": "debt to the penny"}, ["treasury.fiscaldata"]),
        # Production token scoring also matches the "10" in Treasury yields.
        ({"query": "10-K"}, ["sec.filings", "treasury.fiscaldata"]),
        ({"query": "nonfarm"}, ["bls.public_series"]),
        ({"query": "daily treasury rates"}, ["treasury.fiscaldata"]),
        ({"query": "unemployment", "source": "SEC"}, []),
        ({"query": "zzzzxyzzzz"}, []),
        # Ties keep the production catalog order before the limit is applied.
        ({"query": "macro", "limit": 1}, ["treasury.fiscaldata"]),
        ({"query": "   ", "limit": 1}, ["sec.filings"]),
    ],
)
def test_recorded_catalog_honors_actual_node_topic_filter_order_and_limit(recorded_research_server, payload, ids):
    result = _recorded_research_call(recorded_research_server, "search_data_catalog", payload)
    assert [item["id"] for item in result["datasets"]] == ids
    assert result["count"] == len(ids)


def test_recorded_discovery_metadata_does_not_upgrade_unavailable_observations(recorded_research_server, monkeypatch):
    from scripts import agent_eval_research_server

    monkeypatch.setattr(agent_eval_research_server, "unavailable", True)
    catalog = _recorded_research_call(recorded_research_server, "search_data_catalog", {"query": "CPI"})
    assert [item["id"] for item in catalog["datasets"]] == ["bls.public_series"]
    assert catalog["datasets"][0]["temporalCapabilities"]["pointInTimeQualified"] is False
    result = _recorded_research_call(recorded_research_server, "query_data", {"datasetId": "bls.public_series"})
    assert result["available"] is False
    assert result["error"] == "managed_research_unavailable"
    assert "rows" not in result


@pytest.mark.parametrize(
    "source,ids",
    [("BLS", ["bls.public_series"]), ("Treasury", ["treasury.fiscaldata"]), ("SEC", ["sec.filings"]), ("BEA", [])],
)
def test_recorded_catalog_honors_node_source_and_limit_inputs(recorded_research_server, source, ids):
    result = _recorded_research_call(recorded_research_server, "search_data_catalog", {"source": source, "limit": 1})
    assert [item["id"] for item in result["datasets"]] == ids
    assert result["count"] == len(ids)


@pytest.mark.parametrize("payload", [{"source": "unknown"}, {"limit": 0}, {"limit": 51}, {"query": "x" * 201}])
def test_recorded_catalog_rejects_inputs_outside_the_node_schema(recorded_research_server, payload):
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError):
        _recorded_research_call(recorded_research_server, "search_data_catalog", payload)


def test_recorded_document_omitted_asof_retains_live_unqualified_read(recorded_research_server):
    result = _recorded_research_call(
        recorded_research_server,
        "get_document",
        {"datasetId": "sec.filings", "documentId": "0000123456:0000123456-26-000042:acme-20260630.htm"},
    )
    assert result["asOf"] is None
    assert result["pointInTimeQualified"] is False
    assert result["availabilityPolicy"] == "calendar_last_modified_only"
    assert result["text"]


def test_recorded_treasury_filters_the_actual_node_date_value_projection(recorded_research_server, monkeypatch):
    from scripts import agent_eval_research_server

    # Ordinary transport contract for the Node projection of the preserved raw
    # Jan 2, 2025 observation. This adds no observation or publication proof.
    recorded = json.loads(json.dumps(agent_eval_research_server.recorded))
    source = recorded["sources"]["treasury.fiscaldata"]
    assert source["rows"] == [{"date": "2025-01-02", "value": 4.57, "maturity": "BC_10YEAR", "label": "10-year"}]
    monkeypatch.setattr(agent_eval_research_server, "recorded", recorded)
    result = _recorded_research_call(
        recorded_research_server,
        "query_data",
        {
            "datasetId": "treasury.fiscaldata",
            "metrics": ["10-year"],
            "filters": {"topic": "yield_curve"},
            "timeRange": {"startDate": "2025-01-01", "endDate": "2025-01-31"},
        },
    )
    assert result["rows"] == source["rows"]
    assert result["asOf"] is None
    assert result["pointInTimeQualified"] is False
    empty = _recorded_research_call(
        recorded_research_server,
        "query_data",
        {"datasetId": "treasury.fiscaldata", "timeRange": {"startDate": "2026-01-01", "endDate": "2026-08-11"}},
    )
    assert empty["rows"] == []
    assert agent_eval_research_server.recorded["sources"]["treasury.fiscaldata"]["rows"] == source["rows"]


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
