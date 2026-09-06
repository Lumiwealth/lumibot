import asyncio
import os
from datetime import datetime, timezone

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

    assert handle._bound_remote_tool_payload(
        server,
        "query_data",
        {"datasetId": "bls.public_series", "timeRange": {"endDate": "2026-01-01"}},
    )["timeRange"]["endDate"] == "2025-01-06"
    assert handle._bound_remote_tool_payload(
        server,
        "search_documents",
        {"datasetId": "sec.filings"},
    )["timeRange"]["endDate"] == "2025-01-06"
    assert handle._bound_remote_tool_payload(
        server,
        "get_document",
        {"datasetId": "sec.filings", "documentId": "cik:accession:file"},
    )["asOf"] == "2025-01-06"


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
