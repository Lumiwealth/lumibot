Lumibot vs OpenAlice
====================

OpenAlice has a strong "one-person Wall Street" concept: an AI agent that
researches, sizes, manages, and exits trades across markets.

Lumibot is different because it starts from the Python strategy and broker
framework. You can build a simple deterministic strategy, a single AI agent, or
a multi-agent trading team, then backtest and inspect that strategy before
moving toward paper or live trading. You can also keep the parts that should
never be left to a model, such as hard risk limits and order sizing rules, in
normal Python code.

Where OpenAlice Fits
********************

OpenAlice is useful when you want an agent-product concept around a full
trading lifecycle: research, entry, management, risk, and exit.

Where Lumibot Fits
******************

Use Lumibot when you want developer control over the strategy code and the
ability to test the same logic in a backtest and broker context.

Lumibot supports:

- **Copy-paste Python strategy examples** that can be modified like normal
  code.
- **Deterministic gates plus AI reasoning** so a model can advise, debate, or
  choose, while Python enforces the universe, sizing, risk, cash, and order
  rules.
- **Multi-agent teams** such as researcher, bull, bear, reviewer, risk, and
  trader agents.
- **Backtest observability** with charts, orders, logs, traces, replay cache,
  memory, and tearsheets.
- **Supported broker integrations** for paper and live workflows.
- **BotSpot managed runtime** when you want hosted backtests, deployment,
  monitoring, alerts, MCP tools, broker connections, and kill-switch controls.

Why Backtesting Matters
***********************

An AI trader that can research, enter, manage, and exit trades sounds powerful,
but the important question is how it behaves across many market days. Lumibot
lets you test the agent inside a historical trading loop, inspect each decision,
and improve the strategy before connecting it to a broker workflow.

Short Version
*************

OpenAlice is a strong agent-product concept. Lumibot is the Python trading
framework for developers who want code control, backtests, artifacts,
guardrails, and broker paths behind an AI trading agent.
