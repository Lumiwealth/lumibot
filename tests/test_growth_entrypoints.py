"""Public entry points must be useful without changing the Strategy API."""
from pathlib import Path
import importlib

ROOT = Path(__file__).resolve().parents[1]


def test_ai_examples_are_root_navigation_entries():
    index = (ROOT / "docsrc/index.rst").read_text()
    tree = index.split(".. toctree::", 1)[1]
    assert "   AI Quickstart <agents_quickstart>" in tree
    assert "   AI Examples <agents_examples>" in tree
    assert (ROOT / "docsrc/agent_start_here.rst").exists()


def test_quickstart_includes_one_canonical_trading_example():
    text = (ROOT / "docsrc/agents_quickstart.rst").read_text()
    assert "literalinclude:: ../lumibot/example_strategies/ai_researcher_trader.py" in text
    assert "gemini-3.5-flash-lite" in text
    assert "gpt-4.1" not in text
    guide = (ROOT / "docs/AI_TRADING_AGENTS.md").read_text()
    assert "gpt-4.1" not in guide
    assert "ai_researcher_trader.py" in guide


def test_example_import_does_not_start_agents(monkeypatch):
    from lumibot.components.agents.manager import AgentManager

    def unexpected(*args, **kwargs):
        raise AssertionError("Import must not create or run an agent")

    monkeypatch.setattr(AgentManager, "create", unexpected)
    module = importlib.import_module("lumibot.example_strategies.ai_researcher_trader")
    assert module.ResearcherTraderStrategy.parameters["symbol"] == "SPY"


def test_two_roles_preserve_evidence_and_trading_ownership():
    from types import SimpleNamespace
    from lumibot.example_strategies.ai_researcher_trader import ResearcherTraderStrategy

    created, calls = [], []

    class Agents(dict):
        def create(self, **kwargs):
            created.append(kwargs)
            name = kwargs["name"]
            self[name] = SimpleNamespace(run=lambda **args: (
                calls.append((name, args)) or SimpleNamespace(summary=f"{name} evidence")
            ))

    from datetime import datetime
    ctx = SimpleNamespace(agents=Agents(), parameters={"symbol": "SPY", "max_position_pct": 10},
                          get_datetime=lambda: datetime(2026, 4, 6), log_message=lambda *a, **k: None)
    ResearcherTraderStrategy.initialize(ctx)
    ResearcherTraderStrategy.on_trading_iteration(ctx)
    assert [(a["name"], a["allow_trading"]) for a in created] == [("researcher", False), ("trader", True)]
    assert all(a["default_model"] == "gemini-3.5-flash-lite" for a in created)
    assert [name for name, _ in calls] == ["researcher", "trader"]
    assert calls[1][1]["context"]["research_evidence"] == "researcher evidence"
    assert calls[1][1]["context"]["max_position_pct"] == 10
