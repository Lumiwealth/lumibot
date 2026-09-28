from datetime import datetime
from types import SimpleNamespace

from lumibot.example_strategies.ai_browser_research_showcase import AIBrowserResearchShowcaseStrategy


def test_browser_showcase_separates_research_trading_and_publishing_roles():
    created = []
    calls = []

    class Agents(dict):
        def create(self, **kwargs):
            created.append(kwargs)
            name = kwargs["name"]

            def run(**run_kwargs):
                calls.append((name, run_kwargs))
                return SimpleNamespace(summary=f"{name} result")

            self[name] = SimpleNamespace(run=run)

    context = SimpleNamespace(
        agents=Agents(),
        parameters={
            **AIBrowserResearchShowcaseStrategy.parameters,
            "research_url": "https://research.example.test/dashboard",
            "publish_enabled": True,
            "publish_url": "https://community.example.test/new-post",
        },
        get_datetime=lambda: datetime(2026, 9, 20),
        log_message=lambda *args, **kwargs: None,
    )

    AIBrowserResearchShowcaseStrategy.initialize(context)
    AIBrowserResearchShowcaseStrategy.on_trading_iteration(context)

    assert [(item["name"], item["allow_trading"]) for item in created] == [
        ("browser_researcher", False),
        ("trading_risk_manager", True),
        ("trade_publisher", False),
    ]
    assert [name for name, _ in calls] == ["browser_researcher", "trading_risk_manager", "trade_publisher"]
    assert calls[1][1]["context"]["research_evidence"] == "browser_researcher result"
    assert calls[2][1]["context"]["trade_outcome"] == "trading_risk_manager result"
    assert calls[2][1]["context"]["publish_url"] == "https://community.example.test/new-post"


def test_browser_showcase_does_not_publish_without_explicit_strategy_configuration():
    calls = []

    class Agents(dict):
        def create(self, **kwargs):
            name = kwargs["name"]
            self[name] = SimpleNamespace(
                run=lambda **run_kwargs: calls.append(name) or SimpleNamespace(summary=f"{name} result")
            )

    context = SimpleNamespace(
        agents=Agents(),
        parameters={**AIBrowserResearchShowcaseStrategy.parameters, "research_url": "https://example.test"},
        get_datetime=lambda: datetime(2026, 9, 20),
        log_message=lambda *args, **kwargs: None,
    )

    AIBrowserResearchShowcaseStrategy.initialize(context)
    AIBrowserResearchShowcaseStrategy.on_trading_iteration(context)

    assert calls == ["browser_researcher", "trading_risk_manager"]
