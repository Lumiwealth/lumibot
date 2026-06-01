Lumibot vs TradingAgents
========================

TradingAgents helped prove that people want multi-agent financial research
workflows. It is a strong research/demo project for showing how analyst,
trader, and risk-style agents can reason together.

Lumibot is different because the AI team runs inside a Python trading framework
that already supports backtests, broker objects, orders, positions, artifacts,
and live execution paths. The agent conversation is not the final product. The
strategy lifecycle is the product.

Where TradingAgents Fits
************************

TradingAgents is useful when you want to study or prototype a multi-agent LLM
financial research flow. Its core appeal is the agent structure: analysts,
debate, and portfolio-style decision making.

Where Lumibot Fits
******************

Use Lumibot when you want that kind of agent structure to become a strategy you
can backtest, inspect, guardrail with Python, paper trade, and connect to
supported brokers.

Lumibot supports:

- **Flexible agent flows:** copy TradingAgents-style research/debate patterns
  when they fit, or build your own single-agent, specialist-desk, bull/bear,
  neutral, review, or hybrid team.
- **Python guardrails:** keep hard trading rules in code: symbol universe,
  cash checks, position limits, sizing, order type, risk filters, and kill
  conditions.
- **Backtestable decisions:** run the agents inside the backtest loop and
  inspect charts, orders, trade files, logs, traces, replay cache, and
  tearsheets.
- **Broker-aware execution:** use supported broker paths for paper or live
  workflows instead of rewriting the agent demo into a separate trading stack.
- **BotSpot managed runtime:** hosted backtests, broker connections,
  deployment, monitoring, alerts, audit history, MCP tools, and kill-switch
  controls.

Why Backtesting Matters
***********************

Without backtesting, a multi-agent trading workflow is mostly a prompt
experiment. You can read a transcript and hope the agents behave well later.
With Lumibot, the same team must make decisions inside a repeatable historical
simulation, using broker-like cash, positions, orders, and market data. That is
how you find bad prompts, missing tool data, poor sizing, unwanted trades, and
weak risk rules before moving toward real execution.

Short Version
*************

TradingAgents is a strong multi-agent research framework. Lumibot is the
practical strategy framework when you want AI trading teams that can be
customized, backtested, guarded by Python, and operated with real broker paths.
