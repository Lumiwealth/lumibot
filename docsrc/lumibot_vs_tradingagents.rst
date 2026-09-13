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

TradingAgents' current README documents ticker/date analysis, checkpoints,
and the limits of reproducibility when live news and social data are used.
LumiBot's execution loop adds order and account artifacts to inspect alongside
agent reasoning. For either project, record the data cutoff, model, source
revision and actual evidence before comparing results.

This description was checked against the `TradingAgents README
<https://github.com/TauricResearch/TradingAgents>`_ on September 12, 2026.
It is a documentation comparison, not an independently run benchmark.

Short Version
*************

TradingAgents is a strong multi-agent research framework. Lumibot is the
practical strategy framework when you want AI trading teams that can be
customized, backtested, guarded by Python, and operated with real broker paths.
