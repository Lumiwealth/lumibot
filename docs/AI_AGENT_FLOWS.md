# AI Agent Flows

> How to design single-agent, multi-agent, committee, debate, and hybrid AI trading workflows in Lumibot.

**Last Updated:** 2026-05-09
**Status:** Active
**Audience:** Developers and AI Agents

---

## Overview

An agent flow is the Python control flow your strategy uses to call one or more agents. It is not a fixed framework shape. Lumibot gives agents model routing, built-in tools, point-in-time backtesting, replay artifacts, memory, notifications, and order permissions. Your strategy decides how those pieces are arranged.

The AI investment committee is one useful example. It is not the only pattern.

---

## Strategy Shapes

Lumibot supports three broad styles:

- **Deterministic strategies:** Python code makes the decision with fixed rules, thresholds, indicators, schedules, position sizing, and risk checks.
- **Agent-powered strategies:** one or more agents reason over evidence, call tools, write memory, and optionally place orders.
- **Hybrid strategies:** Python gates the setup, agents research or review the context, and deterministic Python may still size, filter, or submit orders.

The same strategy can be backtested first and then run against paper or live brokers.

## Common Flow Patterns

**Single analyst:** one agent reads the current market state and returns a decision or structured recommendation.

**Research then trade:** a read-only research agent gathers evidence, then a trading-enabled agent reviews that evidence and decides whether to submit orders.

**Bull, bear, neutral:** multiple read-only agents receive the same evidence pack and argue from different perspectives. A final portfolio manager weighs the views.

**Specialist research desk:** separate agents gather different inputs: news, SEC filings, macro data, technical indicators, sector context, valuation, or risk.

**Multi-model committee:** several providers or models produce independent cases. A final synthesis step compares them.

**Iterated debate:** agents run in sequence more than once, such as bull, bear, bull rebuttal, bear rebuttal, risk review, final decision.

**Deterministic execution:** agents stop at research. Normal Python code places the final order after checking strict rules.

**Risk gate:** Python applies hard limits after an agent recommendation, such as max position size, no shorting, symbol allowlists, drawdown stops, or per-trade dollar limits.

## Minimal Two-Agent Flow

```python
def initialize(self):
    self.agents.create(
        name="researcher",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        system_prompt="Gather evidence. Do not trade.",
    )
    self.agents.create(
        name="trader",
        model="openai/gpt-5.5",
        allow_trading=True,
        system_prompt="Review evidence, check risk, and trade only when justified.",
    )

def on_trading_iteration(self):
    evidence = self.agents["researcher"].run(
        task_prompt="Research the current setup for AAPL, MSFT, and NVDA."
    )
    decision = self.agents["trader"].run(
        task_prompt="Make the final decision.",
        context={"evidence": evidence.summary or evidence.text},
    )
    self.log_message(decision.summary)
```

## Specialist Flow

This is still normal Python. Create whichever agents the strategy needs and pass outputs forward.

```python
def initialize(self):
    specialists = {
        "news_researcher": "Find recent news and catalysts.",
        "filing_researcher": "Search SEC filings for risks and opportunities.",
        "macro_researcher": "Review rates, inflation, labor, liquidity, and credit.",
        "technical_researcher": "Review trend, volatility, RSI, MACD, and moving averages.",
        "bull_case": "Build the strongest long thesis.",
        "bear_case": "Find the strongest reasons not to trade.",
        "neutral_case": "Give a balanced probability-weighted view.",
    }

    for name, prompt in specialists.items():
        self.agents.create(
            name=name,
            model="openai/gpt-5.4-mini",
            allow_trading=False,
            system_prompt=prompt,
        )

    self.agents.create(
        name="portfolio_manager",
        model="openai/gpt-5.5",
        allow_trading=True,
        system_prompt="Weigh the research, check risk limits, then place orders only if justified.",
    )
```

## Hybrid Deterministic And Agent Flow

Agents do not need to place trades. Use them for judgment and explanation, then keep final execution deterministic.

```python
import json

def on_trading_iteration(self):
    if not self.indicators.crossed_above("SPY", "sma_20", "sma_50"):
        return

    review = self.agents["risk_reviewer"].run(
        task_prompt=(
            "Review whether this SMA crossover is worth trading today. "
            "Return only JSON with this shape: "
            '{"approved": true|false, "reason": "short explanation"}'
        )
    )

    decision = json.loads(review.summary or review.text or "{}")
    if decision.get("approved") is True:
        order = self.create_order("SPY", 10, "buy")
        self.submit_order(order)
```

## Model Selection Per Agent

Every agent can use its own model. That does not mean every strategy needs many models.

Common choices:

- cheaper model for data gathering and summarization
- stronger model for adversarial reasoning or final trade decisions
- different providers when you want independent perspectives

For a four-agent committee:

```python
self.agents.create(name="evidence_researcher", model="openai/gpt-5.4-mini", allow_trading=False)
self.agents.create(name="bull_researcher", model="openai/gpt-5.5", allow_trading=False)
self.agents.create(name="bear_researcher", model="google/gemini-3.1-pro", allow_trading=False)
self.agents.create(name="portfolio_manager", model="openai/gpt-5.5", allow_trading=True)
```

## Safety Rule

Use `allow_trading=False` for every agent that should not mutate broker state. That agent can still inspect read-only state and research tools. Only the final trader, portfolio manager, or deterministic Python code should submit, modify, or cancel orders.

Do not combine those execution paths by accident. If the final trader or
portfolio manager has `allow_trading=True`, it owns order submission; the
strategy should not parse the agent's prose and submit a second Python order
afterward. Use deterministic Python execution only for an explicit hybrid design
where the final agent is advisory or returns validated structured JSON.

## Related Docs

- `docs/AI_INVESTMENT_COMMITTEE.md`: one concrete four-agent example.
- `docs/AI_AGENT_BUILTIN_TOOLS.md`: built-in tools and permissions.
- `docs/AI_AGENT_MEMORY.md`: local memory tools.
- `docs/AI_AGENT_NOTIFICATIONS.md`: notification tools.
