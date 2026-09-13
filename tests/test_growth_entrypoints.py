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


def test_readme_leads_with_runnable_ai_before_education():
    text = (ROOT / 'README.md').read_text()
    opening = text.split('## What You Can Build')[0]
    assert 'LumiBot AI Trading' in opening
    assert 'Try the development example without API keys' not in opening
    assert 'ResearcherTraderStrategy.backtest(' in opening
    assert 'export GEMINI_API_KEY=' in opening
    assert opening.index('```python') < opening.index('Join the free challenge')
    assert 'width="360"' in opening
    assert 'ai-trading-hero.png' in opening
    assert 'learn-with-rob.png' not in opening


def test_navigation_has_task_groups_and_education_follows_quickstart():
    index = (ROOT / 'docsrc/index.rst').read_text()
    for group in ('Start here', 'AI trading', 'Build strategies', 'Backtest and trade', 'Community and learning'):
        assert f':caption: {group}' in index
    assert index.index('.. _first-python-backtest:') < index.index('.. include:: _includes/learn_with_rob.rst')
    invitation = (ROOT / 'docsrc/_includes/learn_with_rob.rst').read_text()
    assert 'free challenge' in invitation
    assert ':width: 360px' in invitation
