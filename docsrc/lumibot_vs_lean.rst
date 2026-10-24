Lumibot vs QuantConnect LEAN
============================

.. meta::
   :description: Lumibot and QuantConnect LEAN are both open-source algorithmic trading frameworks, but they make different architectural choices.

Lumibot and QuantConnect LEAN are both open-source algorithmic trading
frameworks, but they make different architectural choices. Lumibot is a
Python-first library for strategies, backtests, broker connections, and AI
trading teams. LEAN is a larger event-driven engine used by QuantConnect for
research, backtesting, optimization, and live trading across Python and C#.

The useful choice is not which project wins every category. It is which runtime
model, language boundary, data workflow, and operating layer fit your team.

At a glance
***********

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - QuantConnect LEAN
     - LumiBot
   * - Language
     - C# engine; algorithms in Python or C#
     - Python
   * - License
     - Apache-2.0
     - GPL-3.0
   * - Assets
     - Equities, options, futures, future options, indexes, index options, forex, CFDs, crypto and crypto futures
     - Stocks, ETFs, options, futures, forex, crypto and prediction markets
   * - Live trading
     - Yes. The LEAN CLI lists Interactive Brokers, TradeStation, Tastytrade, Alpaca, Charles Schwab, Tradier, Webull, Binance, Bybit, Kraken, Coinbase and others
     - Yes. Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket and many crypto exchanges through CCXT
   * - Running it
     - Locally through the LEAN CLI and Docker, or in QuantConnect's cloud
     - ``pip install lumibot`` and run a Python file, or BotSpot for hosted backtests
   * - AI agents
     - No LLM agents in the engine; QuantConnect's cloud platform has AI assistance
     - Built-in LLM agents that research, call tools and place orders inside the strategy

Where LEAN Fits
***************

Use LEAN when you want QuantConnect's algorithm model, broad engine
infrastructure, Python or C# support, and compatibility with QuantConnect's
cloud research and trading workflow. Teams that already build around LEAN
algorithms and datasets will usually prefer to stay within that ecosystem.

Where Lumibot Fits
******************

Use Lumibot when you want strategy code to remain a normal Python project and
you want to combine deterministic trading logic with AI agents inside the same
strategy lifecycle.

Lumibot supports:

- **Python-first strategies:** strategies are ordinary Python classes that can
  use the broader Python ecosystem.
- **AI agents in the backtest loop:** agents can research, call tools, debate,
  and make decisions on historical bars while traces and orders remain
  inspectable.
- **Deterministic and hybrid designs:** hard rules can stay in Python while AI
  handles evidence gathering or judgment.
- **Broker and data adapters:** the same strategy shape can move from
  historical testing toward paper or live broker workflows.
- **BotSpot as an optional managed layer:** hosted data, parallel backtests,
  broker connections, deployment, monitoring, and MCP access are available
  without changing Lumibot into a closed-source runtime.

Questions To Ask Before Choosing
********************************

1. Does your team want a Python library or a larger algorithm engine?
2. Do you need C# support?
3. Will you supply and operate your own data, scheduling, credentials, and
   monitoring, or use a managed platform?
4. Do AI agents need to run inside the historical simulation loop?
5. Which brokers, asset classes, data providers, and deployment targets are
   required today?

Risk And Limitations
********************

Neither framework guarantees profitable trading. Backtests are historical
simulations and can be distorted by data quality, look-ahead bias, assumptions,
overfitting, fees, slippage, and changing market regimes. Verify current
integrations and operational requirements in each project's official
documentation.

Try LumiBot with your AI assistant
**********************************

The fastest way to try LumiBot is to let your AI coding assistant do the setup.
Install the free
`LumiBot trading skills <https://github.com/Lumiwealth/lumibot-trading-skills>`_
in Claude Code, Codex or Cursor with one command, then connect the free BotSpot
MCP so your assistant can run backtests in the cloud. See
:doc:`claude_code_codex_cursor` for the exact commands.

.. include:: _snippets/botspot_mcp_callout.rst

Sources
*******

Capabilities on this page were checked on July 28, 2026. The at-a-glance
table was checked on October 24, 2026.

- `Lumibot documentation <https://lumibot.lumiwealth.com/>`_
- `Lumibot source repository <https://github.com/Lumiwealth/lumibot>`_
- `QuantConnect LEAN documentation <https://www.quantconnect.com/docs/v2/lean-engine>`_
- `QuantConnect LEAN source repository <https://github.com/QuantConnect/Lean>`_
- `LEAN CLI live trading brokerages <https://www.quantconnect.com/docs/v2/lean-cli/live-trading/brokerages>`_
