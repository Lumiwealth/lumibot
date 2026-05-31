AI Trading Project Comparison
=============================

AI trading projects have proved that people want agentic trading workflows.
Lumibot's edge is that those workflows run inside a real Python trading
framework: you can backtest the agent decisions, inspect artifacts, paper
trade, and connect to brokers without rewriting the strategy.

Quick Comparison
****************

.. list-table::
   :class: wide-comparison-table
   :header-rows: 1
   :widths: 18 20 12 14 14 14 16

   * - Project
     - Main angle
     - AI agents / teams
     - Backtest agent decisions
     - Paper/live broker path
     - Deterministic Python strategies
     - Hosted data/deploy/monitoring
   * - Lumibot + BotSpot
     - Python strategies, AI trading teams, backtests, brokers, hosted deployment
     - Yes
     - Yes
     - Yes: Alpaca, IBKR, Tradier, Schwab, Tradovate, ProjectX, Bitunix, selected CCXT
     - Yes
     - Yes, through BotSpot
   * - TradingAgents
     - Multi-agent LLM trading research framework
     - Yes
     - Research/demo oriented
     - Not the main focus
     - Limited
     - No
   * - ai-hedge-fund
     - Educational AI hedge fund with named investor-style agents
     - Yes
     - Demo/backtest oriented
     - Not the main focus
     - Limited
     - No
   * - OpenAlice
     - One-person Wall Street agent concept
     - Yes
     - Emerging/experimental
     - Local/self-run focus
     - Limited
     - No
   * - QuantDinger
     - Self-hosted AI quant operating system
     - Yes
     - Yes
     - Crypto, IBKR, MT5, Alpaca
     - Yes
     - Self-hosted
   * - Vibe-Trading
     - Personal trading agent
     - Yes
     - Yes
     - Agent trading platform focus
     - Limited
     - Platform-specific
   * - AI-Trader
     - Agent-native trading platform
     - Yes
     - Platform focus
     - Platform focus
     - Limited
     - Platform-specific
   * - OpenBB
     - Financial data platform for analysts, quants, and AI agents
     - Tooling for agents
     - Not a strategy backtester
     - No broker execution framework
     - No
     - OpenBB workspace/platform
   * - Qlib
     - AI-oriented quant research platform
     - Research/ML agents
     - Quant research backtests
     - Limited live focus
     - Research pipelines
     - No

Detailed Comparisons
********************

.. toctree::
   :maxdepth: 1

   lumibot_vs_tradingagents
   lumibot_vs_ai_hedge_fund
   lumibot_vs_openalice
   lumibot_vs_quantdinger

Why Lumibot Is Different
************************

Most AI trading demos stop at research, a notebook, or a simulated agent
conversation. Lumibot is built around the strategy lifecycle:

- write normal Python strategy code
- add one or more AI agents where reasoning helps
- backtest the agent decisions inside the same loop used by normal strategies
- inspect orders, traces, memory, charts, logs, and tearsheets
- paper trade or run live with supported brokers
- use BotSpot when you want hosted data, parallel backtests, deployment,
  monitoring, alerts, audit history, and kill switches

That combination matters because the hard part is not only getting an AI model
to say what it would buy. The hard part is turning the idea into a strategy
that can be replayed, inspected, connected to broker state, and operated over
time.
