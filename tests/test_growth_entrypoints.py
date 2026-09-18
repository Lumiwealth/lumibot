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
    assert 'width="640"' in opening
    assert 'benefit-hero.png' in opening
    assert 'learn-with-rob.png' not in opening


def test_navigation_has_task_groups_and_education_follows_quickstart():
    index = (ROOT / 'docsrc/index.rst').read_text()
    for group in ('Start here', 'AI trading', 'Build strategies', 'Backtest and trade', 'Community and learning'):
        assert f':caption: {group}' in index
    assert index.index('.. _first-python-backtest:') < index.index('Want help building your first AI trading bot?')
    invitation = (ROOT / 'docsrc/_includes/learn_with_rob.rst').read_text()
    assert 'free challenge' in invitation
    assert ':width: 640px' in invitation


def test_homepage_and_readme_offer_direct_ai_runner_before_reference_choices():
    """Rob requested competitor-style install/run entry points above the fold."""
    home = (ROOT / 'docsrc/index.rst').read_text()
    readme = (ROOT / 'README.md').read_text()
    runner = 'python -m lumibot.example_strategies.ai_researcher_trader'
    assert home.index(runner) < home.index('lumibot-entry-grid')
    assert home.index(runner) < home.index('.. _first-python-backtest:')
    assert readme.index(runner) < readme.index('Save as `my_ai_strategy.py`')


def test_generated_artwork_has_consistent_centered_placements():
    text = (ROOT / 'README.md').read_text()
    assert '<p align="center">\n<a href="https://botspot.trade/challenges?' in text
    for page, asset in [('agents_examples.rst', 'example-gallery.png'),
                        ('agents_quickstart.rst', 'backtest-benefit.png'),
                        ('standalone_components.rst', 'component-research.png')]:
        source = (ROOT / 'docsrc' / page).read_text()
        assert asset in source
        assert ':align: center' in source
    css = (ROOT / 'docsrc/_html/custom.css').read_text()
    assert '.lumibot-learning-image' in css
    assert 'width: min(100%, 640px)' in css


def test_traditional_strategies_are_first_screen_and_start_here_choices():
    readme = (ROOT / 'README.md').read_text().split('## Run your first AI backtest')[0]
    assert '[Python quickstart](#backtest-a-strategy)' in readme
    assert 'No AI model or model API key is required' in readme
    home = (ROOT / 'docsrc/index.rst').read_text()
    assert home.index('Python quickstart <first-python-backtest>') < home.index('Run an AI strategy')
    nav = home.split(':caption: Start here')[1].split('.. toctree::')[0]
    assert 'Python Strategy Examples <examples>' in nav
    examples = (ROOT / 'docsrc/examples.rst').read_text()
    assert examples.index('Traditional Python strategies') < examples.index('AI Agents\n')
    assert 'buy_and_hold.py' in examples


def test_benefit_copy_and_tracked_learning_placements():
    home = (ROOT / 'docsrc/index.rst').read_text()
    assert 'Turn trading ideas into working strategies' in home
    for page, content in [('index.rst', 'home_challenge_image'),
                          ('getting_started.rst', 'setup_challenge_image'),
                          ('agents_examples.rst', 'examples_challenge_image'),
                          ('agents_quickstart.rst', 'quickstart_bootcamp_image'),
                          ('standalone_components.rst', 'components_bootcamp_image')]:
        source = (ROOT / 'docsrc' / page).read_text()
        assert content in source
        assert 'utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading' in source
    assert 'blob/dev/' not in (ROOT / 'docsrc/agent_start_here.rst').read_text()
    template = (ROOT / 'docsrc/_templates/base.html').read_text()
    assert 'benefit-hero.png' in template
    assert 'name="description"' in template


def test_rejected_mascot_outputs_cannot_return():
    import hashlib
    import json
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assets = root / "docs/assets/ai-trading"
    rejected = set(json.loads((assets / "rejected-mascot-hashes.json").read_text()).values())
    for name in ("benefit-hero", "example-gallery", "backtest-benefit", "component-research", "python-strategies", "broker-connections"):
        assert hashlib.sha256((assets / f"{name}.png").read_bytes()).hexdigest() not in rejected
    assert "Do not generate robot or mascot variations" in (root / "AGENTS.md").read_text()


def test_creator_campaign_placements_have_distinct_tracking():
    import json
    import re
    from pathlib import Path
    from urllib.parse import urlparse, parse_qs
    root = Path(__file__).resolve().parents[1]
    rows = json.loads((root / "docs/assets/ai-trading/creator-placements.json").read_text())
    challenge = set()
    bootcamp = set()
    tags = set()
    for filename, image, tag, destination in rows:
        source = (root / filename).read_text()
        assert image + ".png" in source
        links = re.findall(r'https://botspot\.trade/[^\s<>"\)]+', source)
        found = [url for url in links if parse_qs(urlparse(url).query).get("utm_content") == [tag]]
        assert found, (filename, tag)
        for url in found:
            parsed = urlparse(url)
            assert parsed.path == "/" + destination
            query = parse_qs(parsed.query)
            assert all(query.get(key) for key in ("utm_source", "utm_medium", "utm_campaign", "utm_content"))
        assert tag not in tags
        tags.add(tag)
        (challenge if destination == "challenges" else bootcamp).add(image)
    assert len(challenge) >= 6
    assert len(bootcamp) >= 3
