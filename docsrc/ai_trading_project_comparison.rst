AI Trading Project Comparison
=============================

AI trading projects have proved that people want agentic trading workflows.
Lumibot's edge is that those workflows run inside a real Python trading
framework. You can backtest the agent decisions, inspect artifacts, add
normal Python guardrails, paper trade, and connect to brokers without
rewriting the strategy.

This is the practical difference: a research agent that sounds smart is still
not a trading system. Lumibot lets you test the same agent flow against
historical data, inspect what it would have done, tighten the rules around
cash, positions, risk, and order submission, then move the same strategy toward
paper or live trading when it is ready.

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
     - Python strategies, flexible AI trading teams, hybrid guardrails, backtests, brokers, hosted deployment
     - Yes: single agents, teams, debates, specialist desks, deterministic gates
     - Yes: replayable decisions, orders, traces, artifacts, charts, logs
     - Yes: Alpaca, IBKR, Tradier, Schwab, Tradovate, ProjectX, Bitunix, selected CCXT
     - Yes
     - Yes: hosted data, parallel backtests, deployment, monitoring, MCP tools, alerts, kill switches
   * - TradingAgents
     - Multi-agent LLM trading research framework
     - Yes, with a specific research/debate structure
     - Research/demo oriented
     - Not the main focus
     - Limited
     - No
   * - ai-hedge-fund
     - Educational AI hedge fund with named investor-style agents
     - Yes, with investor-style personas
     - Demo/backtest oriented
     - Not the main focus
     - Limited
     - No
   * - OpenAlice
     - One-person Wall Street agent concept
     - Yes, with an end-to-end agent-product concept
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
   lumibot_vs_lean

Why Lumibot Is Different
************************

Most AI trading demos stop at research, a notebook, or a simulated agent
conversation. Lumibot is built around the strategy lifecycle:

- **Design the agent flow you want:** single agent, research-to-trade,
  bull/bear/neutral team, specialist desk, model debate, or a hybrid flow.
- **Keep Python in control:** use deterministic strategy code for hard gates,
  cash checks, position limits, symbol filters, order sizing, and risk rules.
- **Backtest the actual decisions:** run the agents inside the backtest loop,
  then inspect orders, traces, replay cache, charts, logs, and tearsheets.
- **Move from test to operation:** paper trade or run live with supported
  brokers using the same strategy lifecycle.
- **Use BotSpot when you want the managed layer:** hosted data, parallel
  backtests, broker connections, deployment, monitoring, alerts, audit history,
  MCP tools, and kill switches.

That combination matters because the hard part is not only getting an AI model
to say what it would buy. The hard part is turning the idea into a strategy
that can be replayed, inspected, connected to broker state, and operated over
time.

Backtesting does not prove that a strategy will make money in the future. It
does something more practical: it forces the agent, tools, prompts, Python
guardrails, and broker-facing order logic to run through a repeatable trading
lifecycle before you trust it with real execution.

Source And Verification Method
******************************

This page compares product roles and documented capabilities, not investment
returns. Project claims were checked against each project's public
documentation or source repository on July 28, 2026. Capabilities can change,
so use the linked primary sources before making a technical decision.

- `Lumibot documentation <https://lumibot.lumiwealth.com/>`_
- `TradingAgents source repository <https://github.com/TauricResearch/TradingAgents>`_
- `ai-hedge-fund source repository <https://github.com/virattt/ai-hedge-fund>`_
- `OpenAlice documentation <https://www.openalice.ai/docs/getting-started/what-is-openalice>`_
- `QuantDinger source repository <https://github.com/brokermr810/QuantDinger>`_
- `Vibe-Trading source repository <https://github.com/HKUDS/Vibe-Trading>`_
- `AI-Trader source repository <https://github.com/HKUDS/AI-Trader>`_
- `OpenBB source repository <https://github.com/OpenBB-finance/OpenBB>`_
- `Qlib source repository <https://github.com/microsoft/qlib>`_
- `QuantConnect LEAN documentation <https://www.quantconnect.com/docs/v2/lean-engine>`_
