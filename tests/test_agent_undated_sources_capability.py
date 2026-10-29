"""Capability first: agents may use undated sources, and live trading has no date rules.

Rob, 2026-10-22 (ABSOLUTE): never make LumiBot less capable. In a backtest an
agent prefers dated sources and never uses anything known to be from after the
simulated time, but a source with no date at all is still allowed when the
agent estimates when it became public and says so. Live trading has no
publication-date restrictions.

These checks fail if any model-visible instruction (built-in skills, manager
prompts, built-in tool descriptions) tells agents again that undated sources
cannot be used in a backtest, or if date gates leak into live mode. The real
behavior is covered by the agent evals web_documents_undated_source_estimated_date,
web_documents_known_future_document_not_opened and
web_documents_live_undated_source_used_freely.
"""

import re
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from lumibot.components.agents import BuiltinTools
from lumibot.components.agents.duckdb_tools import DuckDBQueryLayer
from lumibot.components.agents.manager import AgentManager
from lumibot.components.agents.skills import builtin_skill_directories
from lumibot.components.agents.web_tools import WebClient
from tests.test_agent_tool_permissions import _Strategy

# Phrases that turned "no date" into "cannot use". Each was live text before
# 2026-10-23 and made real agents refuse undated sources in backtests.
_BANNED_PATTERNS = [
    r"null or malformed availability cannot pass",
    r"unknown original[- ]version availability means unavailable",
    r"report missing original[- ]version evidence as unavailable",
    r"require original[- ]version availability",
    r"require original[- ]release proof",
    r"undated (sources?|documents?|pages?|files?) (cannot|can ?not|must not|may not|should not|are not allowed)",
    r"(never|do not|don't) (use|open|read) (an )?undated",
    r"(skip|exclude|ignore) (any |all )?undated",
    r"missing (a )?(publication )?date (means|makes) (the )?(evidence|source|document) unavailable",
]


def _normalized(text: str) -> str:
    return " ".join(str(text).lower().split())


def _skill_texts() -> dict[str, str]:
    return {path.name: (path / "SKILL.md").read_text(encoding="utf-8") for path in builtin_skill_directories()}


def _tool_descriptions() -> dict[str, str]:
    strategy = SimpleNamespace(is_backtesting=True, get_datetime=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc))
    manager = SimpleNamespace(duckdb=DuckDBQueryLayer(strategy))
    descriptions = {}
    for definition in BuiltinTools.all():
        descriptions[definition.name] = definition.description
        try:
            bound = definition.binder(strategy, manager)
        except Exception:
            continue
        descriptions[f"{definition.name}:bound"] = getattr(bound, "description", "") or ""
    return descriptions


def _prompt(*, backtesting: bool) -> str:
    strategy = _Strategy()
    strategy.is_backtesting = backtesting
    agent = AgentManager(strategy).create(name="agent", allow_trading=True)
    return agent._base_system_prompt(agent._runtime_context())


def _instruction_surfaces() -> dict[str, str]:
    surfaces = {f"skill:{name}": text for name, text in _skill_texts().items()}
    surfaces.update({f"tool:{name}": text for name, text in _tool_descriptions().items()})
    surfaces["prompt:backtest"] = _prompt(backtesting=True)
    surfaces["prompt:live"] = _prompt(backtesting=False)
    return surfaces


@pytest.mark.parametrize("pattern", _BANNED_PATTERNS)
def test_no_agent_instruction_forbids_undated_sources(pattern):
    offenders = [name for name, text in _instruction_surfaces().items() if re.search(pattern, _normalized(text))]
    assert offenders == [], f"{pattern!r} found in {offenders}"


def test_web_documents_skill_allows_undated_sources_with_a_disclosed_estimate():
    skill = _normalized(_skill_texts()["web-documents"])
    assert "a source with no date at all is still allowed" in skill
    assert "say plainly in the answer that the date is an estimate" in skill
    assert "never invent a publication date" in skill
    assert "never present an estimate as an exact date" in skill
    # Real no-lookahead stays: known-future documents remain off limits.
    assert "never use anything known to have become public after the simulated time" in skill
    assert "never open a document known to have become public after the simulated time" in skill
    assert "live trading:** there are no date rules" in skill


def test_backtest_prompt_allows_undated_sources_and_keeps_the_future_wall():
    prompt = _normalized(_prompt(backtesting=True))
    assert "a source with no date at all is allowed" in prompt
    assert "say in your answer that the date is an estimate and why" in prompt
    assert "never invent a date or present an estimate as exact" in prompt
    assert "treat the current simulated datetime as a hard wall" in prompt


def test_research_can_finish_with_disclosed_coverage_and_valuation_estimates():
    # A native backtest read a baseline and updates but refused to produce any
    # weights because the shared instructions demanded an exhaustive manifest.
    skill = _normalized(_skill_texts()["web-documents"])
    prompt = _normalized(_prompt(backtesting=True))
    assert "an exhaustive index or original-version archive is not a prerequisite" in skill
    assert "provide the best supported reconstruction" in skill
    assert "missing exact quantities, version hashes or certified coverage do not prevent a useful, explicitly approximate answer" in skill
    assert "query its eligible rows with typed sql before following its report links" in prompt
    assert "reasonable approximations" in prompt
    assert "if you are unsure whether information was available yet, say the evidence is insufficient and do nothing" not in prompt


def test_live_prompt_has_no_publication_date_gates():
    prompt = _normalized(_prompt(backtesting=False))
    for gate in (
        "first public availability",
        "simulated datetime",
        "backtesting safety rules",
        "eligible manifest",
        "available_at",
    ):
        assert gate not in prompt, gate


def _read_document_tool(*, backtesting: bool, body: bytes = b"<html><body>Undated note: index 112.</body></html>"):
    requests = []
    strategy = SimpleNamespace(
        is_backtesting=backtesting, get_datetime=lambda: datetime(2026, 8, 11, 14, 35, tzinfo=timezone.utc)
    )

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=body, headers={"content-type": "text/html"})

    strategy._agent_web_client = WebClient(
        transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"]
    )
    tool = BuiltinTools.web.read_document().binder(strategy, SimpleNamespace(duckdb=DuckDBQueryLayer(strategy)))
    return tool, requests


def test_live_read_document_reads_undated_and_current_documents_freely():
    tool, requests = _read_document_tool(backtesting=False)
    undated = tool.function(url="https://notes.example.net/research/note.html")
    assert undated["ok"] is True and "index 112" in undated["text"]
    assert "historical_as_of" not in undated
    assert "no publication-date rules" in undated["dates"]
    # Even a declared date after the strategy clock is fine live: the page is current.
    current = tool.function(url="https://notes.example.net/research/revised.html", available_at="2099-01-01T00:00:00Z")
    assert current["ok"] is True
    assert len(requests) == 2


def test_backtest_read_document_reads_an_undated_source():
    tool, requests = _read_document_tool(backtesting=True)
    result = tool.function(url="https://notes.example.net/research/note.html")
    assert result["ok"] is True and "index 112" in result["text"]
    assert result["historical_as_of"] == "2026-08-11T14:35:00+00:00"
    assert "an undated source may be used" in result["dates"].lower()
    assert [url.rsplit("/", 2)[-2:] for url in requests] == [["research", "note.html"]]


def test_backtest_read_document_still_refuses_a_declared_future_version_before_http():
    tool, requests = _read_document_tool(backtesting=True)
    result = tool.function(url="https://notes.example.net/research/revised.html", available_at="2026-09-02T00:00:00Z")
    assert result["ok"] is False
    assert result["error"]["code"] == "document_after_cutoff"
    assert requests == []
