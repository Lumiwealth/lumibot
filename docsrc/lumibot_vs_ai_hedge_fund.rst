Lumibot vs ai-hedge-fund
========================

ai-hedge-fund is a compelling educational project because it makes AI investing
easy to understand: different investor-style agents debate ideas from different
perspectives.

Lumibot is different because it focuses on the trading strategy lifecycle. The
goal is not only to create an interesting set of agents. The goal is to run
agent decisions through backtests, inspect the artifacts, add deterministic
Python guardrails, and move the same strategy toward paper or live trading
when appropriate.

Where ai-hedge-fund Fits
************************

ai-hedge-fund is useful when you want an educational, investor-style AI demo.
The named-agent pattern is easy to understand and easy to share.

Where Lumibot Fits
******************

Use Lumibot when you want to turn an AI investing idea into a Python strategy
that can be tested and operated. You are not locked into one investor-persona
workflow. You can build a single agent, bull/bear/neutral team, specialist
research desk, model debate, or a hybrid strategy where Python controls the
hard trading rules and AI handles the reasoning.

Lumibot supports:

- **Normal Python plus AI:** deterministic strategy code and agent reasoning
  live in the same strategy lifecycle.
- **Custom team structures:** researcher, bull, bear, trader, reviewer,
  specialist, risk, or any other agent role you want to define.
- **Broker-aware state:** orders, positions, cash, portfolio value, open
  orders, and supported broker paths are part of the framework.
- **Backtesting with artifacts:** inspect agent traces, replay cache, logs,
  orders, charts, trade files, and tearsheets.
- **Paper and live paths:** use the same strategy shape when you move from
  historical testing to broker-connected operation.
- **BotSpot managed layer:** hosted data, parallel backtests, broker
  connections, deployment, monitoring, MCP tools, alerts, audit history, and
  kill switches.

Why Backtesting Matters
***********************

Named agents are easy to understand, but they are not enough by themselves.
Trading systems need repeatable tests. Lumibot lets you run the AI team through
historical market conditions and see the exact trades, sizing, warnings, logs,
and artifacts. That makes it much easier to improve prompts, tighten risk
rules, and decide whether the idea deserves paper trading.

Short Version
*************

ai-hedge-fund is a great educational AI investing demo. Lumibot is the
framework for building flexible AI trading teams that can be backtested,
guarded by Python, inspected, and connected to real broker workflows.
