:seo_title: Open-Source TradingView Alternative: LumiBot Python Indicators vs Pine Script, NinjaTrader, MT5, QuantConnect

LumiBot indicators compared
===========================

.. meta::
   :description: Open-source TradingView alternative: LumiBot Python indicators compared with Pine Script, LuxAlgo Vela, OpenMarket, NinjaTrader, MetaTrader 5 and QuantConnect LEAN.

How do custom indicators in LumiBot compare with the tools traders already
use? The tables below cover what each one lets an indicator author do. Facts were
checked on October 25, 2026 from each product's own documentation (sources
below). Products change; check their docs before you decide.

Language and where it runs
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Product
     - Language
     - Where it runs
     - Open source
   * - **LumiBot**
     - Python 3
     - Your computer or server
     - Yes, GPL-3.0
   * - **TradingView (Pine Script)**
     - Pine Script v6
     - TradingView's servers
     - No
   * - **LuxAlgo Vela + PineTS**
     - Pine v5/v6 run by PineTS (TypeScript)
     - The viewer's browser (or a server)
     - Vela core Apache-2.0; PineTS AGPL-3.0
   * - **OpenMarket (wrun)**
     - One TypeScript file compiled to WebAssembly
     - Browser; protected code on OpenMarket's servers
     - No
   * - **NinjaTrader 8**
     - NinjaScript (C#)
     - NinjaTrader desktop app
     - No
   * - **MetaTrader 5**
     - MQL5
     - MetaTrader terminal
     - No
   * - **QuantConnect LEAN**
     - Python or C#
     - Locally or QuantConnect's cloud
     - LEAN engine, Apache-2.0

What an indicator can do
------------------------

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Product
     - Any library (numpy, pandas, ML)
     - Call an outside web API
     - Same code places live broker orders
   * - **LumiBot**
     - Yes
     - Yes (:doc:`indicators_outside_data`)
     - Yes, inside a strategy on your broker
   * - **TradingView (Pine Script)**
     - No, Pine built-ins and Pine libraries
     - No
     - Through alerts and webhooks to other services
   * - **LuxAlgo Vela + PineTS**
     - No, PineTS built-ins
     - No; the chart feeds the data
     - No (paper trading)
   * - **OpenMarket (wrun)**
     - No imports
     - Built-in feeds only
     - No ("no orders from the file")
   * - **NinjaTrader 8**
     - No Python. C# on .NET Framework 4.8; third-party .NET DLLs can be referenced
     - Technically possible with standard .NET classes; NinjaTrader support calls it undocumented and unsupported
     - Yes, NinjaScript strategies
   * - **MetaTrader 5**
     - No Python libraries. MQL5 (C++-like); DLL imports if the user turns them on
     - No. ``WebRequest()`` is blocked in indicators (error 4014). It works only in Expert Advisors and scripts, for URLs the user allow-lists, and never in the Strategy Tester
     - Yes, Expert Advisors
   * - **QuantConnect LEAN**
     - Yes, several machine-learning libraries in Python
     - Yes, in the algorithm: ``self.download`` (with headers or a login) and custom data streams. In their cloud each download times out after 10 seconds and the number of remote files per backtest depends on the plan (Free: 25)
     - Yes, LEAN algorithms

Live updates and backtests
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Product
     - Live (forming) bar
     - One-bar updates, no full recompute
     - Backtest the same indicator
   * - **LumiBot**
     - You choose: closed bars, or every update with rollback
     - Yes, built in
     - Yes
   * - **TradingView (Pine Script)**
     - Indicators recalculate on every live update, with rollback
     - Yes (Pine's execution model)
     - Strategy Tester
   * - **LuxAlgo Vela + PineTS**
     - Every streamed update
     - Yes (Pine's execution model)
     - Backtests through the hosted app's AI assistant
   * - **OpenMarket (wrun)**
     - Every update
     - Not covered here
     - Chart replay
   * - **NinjaTrader 8**
     - ``Calculate``: on bar close (default), each tick, or price change
     - Yes (``OnBarUpdate``)
     - Strategy Analyzer
   * - **MetaTrader 5**
     - ``OnCalculate`` runs when price data changes
     - Yes (``prev_calculated``)
     - Strategy Tester
   * - **QuantConnect LEAN**
     - ``update()`` per data point
     - Yes (``update``)
     - LEAN backtests

What this means in plain words
------------------------------

- If you want **Python**, real **machine learning** and **outside data**, and
  the same code to **trade your own broker**, LumiBot and QuantConnect LEAN are
  the two Python options. LEAN is the closest match: also Python, also open
  source, also backtests and live trading. In LEAN, outside data is loaded by
  the algorithm and its cloud has download limits; in LumiBot an indicator
  calls the API itself, updates the forming live bar with rollback, can stream
  live in a plain Python script, and runs on your own machine with
  ``pip install lumibot``.
- **NinjaTrader** indicators are C#. Web calls are possible with .NET classes
  but NinjaTrader does not support them. **MetaTrader 5** blocks web requests
  in indicators entirely.
- If you want the **biggest charting community**, TradingView is it, with Pine
  Script's limits (no outside APIs, no Python libraries).
- **Vela** gives you a fast open-source chart in the browser that runs Pine
  through PineTS. It does not run Python and scripts cannot fetch outside data.
- **NinjaTrader** and **MetaTrader** are desktop platforms with their own
  languages (C# and MQL5) and their own brokers and data connections.

Nothing here says any tool makes money. Indicators describe the past; test
any idea on paper first.

Sources
-------

Checked October 25, 2026.

- TradingView Pine Script v6 manual, "Execution model":
  https://www.tradingview.com/pine-script-docs/language/execution-model/
- LuxAlgo Vela (license and architecture): https://github.com/LuxAlgo/vela ;
  PineTS (license): https://github.com/LuxAlgo/PineTS
- OpenMarket wrun documentation: https://openmarket.xyz
- NinjaTrader ``Calculate`` property: https://docs.ninjatrader.com/ninjascript/calculate
- NinjaTrader outside calls and third-party DLLs (NinjaTrader support forum and
  help guide):
  https://forum.ninjatrader.com/forum/ninjatrader-8/indicator-development/1134931-dynamically-adding-property-values-to-a-script-via-file-api ;
  https://forum.ninjatrader.com/forum/ninjatrader-8/add-on-development/1123485-how-do-i-make-an-external-call ;
  https://ninjatrader-devel.ninjatrader.com/support/helpGuides/nt8/using_3rd_party_indicators.htm ;
  https://forum.ninjatrader.com/forum/ninjatrader-8/indicator-development/1207367-trying-to-add-htmlagilitypack-run-into-number-of-errors
- MQL5 ``OnCalculate``: https://www.mql5.com/en/docs/event_handlers/oncalculate ;
  program events: https://www.mql5.com/en/docs/runtime/running
- MQL5 ``WebRequest`` (not allowed in indicators, not in the Strategy Tester):
  https://www.mql5.com/en/docs/network/webrequest ;
  MetaTrader 5 robots and indicators:
  https://www.metatrader5.com/en/terminal/help/algotrading/trade_robots_indicators
- QuantConnect custom indicators:
  https://www.quantconnect.com/docs/v2/writing-algorithms/indicators/custom-indicators ;
  LEAN source and license: https://github.com/QuantConnect/Lean
- QuantConnect outside data and machine learning:
  https://www.quantconnect.com/docs/v2/writing-algorithms/importing-data/bulk-downloads ;
  https://www.quantconnect.com/docs/v2/writing-algorithms/importing-data/key-concepts ;
  https://www.quantconnect.com/docs/v2/writing-algorithms/machine-learning/key-concepts

Frequently asked questions
--------------------------

Questions about what an indicator can do (machine learning, outside data,
live values, repainting) are answered on :doc:`indicators_overview`.

Is there a free, open-source alternative to TradingView indicators?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. LumiBot is free and open source (GPL-3.0). You write indicators in
Python, run them on your own computer, chart them, backtest them and use them
in live strategies on your own broker. It is not a charting website; it is a
Python library. Install it with ``pip install lumibot``.

Can I write TradingView-style indicators in Python?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. A LumiBot indicator declares its settings, outputs, guide lines and
whether it draws on the price chart, like a Pine indicator, and processes one
bar at a time with Pine-style rollback of the live bar. See
:doc:`indicators_first_custom`.

Do LumiBot's RSI and EMA match TradingView?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The built-ins follow Pine Script v6 formulas, including how averages are
seeded and Wilder smoothing for RSI and ATR. Small differences usually come
from warm-up history or a different data feed. See
:doc:`indicators_tradingview_numbers`.

Can I convert my Pine Script to Python?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, by hand or with an AI coding assistant. :doc:`indicators_tradingview_numbers`
maps Pine functions to LumiBot ones. Only convert code you wrote or have the right to
use; many published scripts have licenses.

Does LumiBot draw charts like TradingView?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot draws indicator charts for backtests. Each indicator's
``describe()`` gives a chart everything it needs (settings, lines, colors,
guide lines, price pane or own pane), so you can also plot the values with
any charting library.

How is LumiBot different from QuantConnect?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both are open source and both run Python strategies in backtests and live.
In LumiBot, an indicator itself can call a web API and give a value for the
still-forming live bar, and it runs on your own computer with
``pip install lumibot``. In QuantConnect, the algorithm loads outside data,
and their cloud limits download time and the number of remote files per
backtest. See the tables above for the sources.
