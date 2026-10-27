Lumibot vs TradingAgents
========================

.. meta::
   :description: TradingAgents helped prove that people want multi-agent financial research workflows.

TradingAgents helped prove that people want multi-agent financial research
workflows. It is a strong research/demo project for showing how analyst,
trader, and risk-style agents can reason together.

Lumibot is different because the AI team runs inside a Python trading framework
that already supports backtests, broker objects, orders, positions, artifacts,
and live execution paths. The agent conversation is not the final product. The
strategy lifecycle is the product.

At a glance
***********

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - TradingAgents
     - LumiBot
   * - What it is
     - Multi-agent LLM financial trading framework that mirrors a trading firm
     - Python trading framework with built-in AI agents, backtests and brokers
   * - Language and license
     - Python 3.11+, Apache-2.0
     - Python 3.10+, GPL-3.0
   * - Agents
     - Analysts, bull and bear researchers, trader, risk team and portfolio manager, built on LangGraph
     - Any flow you design: one agent, research then trade, bull and bear teams, or hybrid Python plus AI
   * - Backtesting
     - Runs the agents over a grid of tickers and dates and scores their decisions
     - Runs the agents inside a full market simulation with orders, fills, cash and positions
   * - Live trading
     - No broker integration; orders go to a simulated exchange. Described as designed for research.
     - Yes, paper and live through Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate and other supported brokers

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

This description was first checked against the `TradingAgents README
<https://github.com/TauricResearch/TradingAgents>`_ on September 12, 2026, and
the at-a-glance table was checked on October 24, 2026 (TradingAgents v0.7.0).
It is a documentation comparison, not an independently run benchmark.

Short Version
*************

TradingAgents is a strong multi-agent research framework. Lumibot is the
practical strategy framework when you want AI trading teams that can be
customized, backtested, guarded by Python, and operated with real broker paths.

Try LumiBot with your AI assistant
**********************************

The fastest way to try LumiBot is to let your AI coding assistant do the setup.
Install the free
`LumiBot trading skills <https://github.com/Lumiwealth/lumibot-trading-skills>`_
in Claude Code, Codex or Cursor with one command, then connect the free BotSpot
MCP so your assistant can run backtests in the cloud. See
:doc:`claude_code_codex_cursor` for the exact commands.

.. include:: _snippets/botspot_mcp_callout.rst
