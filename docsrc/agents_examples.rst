AI Trading Examples
===================

Start with a complete strategy, run a historical backtest, and inspect what the
agents decided. Model calls require a provider key. Each example states its
data requirements and whether the displayed result is a recorded run.

.. container:: lumibot-entry-grid lumibot-start-routes

   .. container:: lumibot-entry-card

      **First AI backtest**

      SPY trend research, risk review, and a trading agent. A small starting point.

      :doc:`Run the SPY example → <agents_quickstart>`

   .. container:: lumibot-entry-card

      **Agents that debate**

      Researcher, bull, bear, and trader working with familiar large-cap stocks.

      :doc:`See the stock team → <agents_example_bull_bear_large_cap_stocks>`

   .. container:: lumibot-entry-card

      **Options strategies**

      Explore an iron condor with its contracts, data setup, and recorded evidence.

      :doc:`Explore the iron condor → <agents_example_ai_iron_condor>`

Compare the workflows below. A fresh model run may produce different decisions
and returns; saved traces show what happened in a particular run.

Stocks
------

.. list-table:: Stock workflows
   :header-rows: 1
   :widths: 25 30 25 20

   * - Example
     - What it does
     - Data and cadence
     - Evidence
   * - :doc:`Large-cap bull/bear team <agents_example_bull_bear_large_cap_stocks>`
     - Researcher, bull, and bear inform one trading agent.
     - Yahoo daily backtest; daily decisions. Alpaca keys only for the broker runner.
     - Saved source and historical screenshot; follow the tutorial's current validation status.
   * - :doc:`Opening range breakout <agents_example_ai_opening_range_breakout>`
     - Inspect completed opening bars and trade a confirmed breakout.
     - ThetaData via Data Downloader; minute evidence, hourly decisions.
     - Earlier mechanics run completed; later bounded run did not finish its full window.
   * - :doc:`VWAP <agents_example_ai_vwap>`
     - Explore VWAP reclaim and mean reversion.
     - Intraday bars and indicator tools; inspect the example's cadence.
     - See the page's recorded mechanics evidence; not a returns claim.
   * - :doc:`Value research team <agents_example_warren_buffett_value>`
     - Research business quality and challenge valuation.
     - Daily stock prices; SEC identity/setup for filing tools.
     - Source example; no new full run performed by this documentation update.
   * - :doc:`Concentrated stock team <agents_example_bill_ackman_concentrated>`
     - Debate one high-conviction large-cap position.
     - Yahoo daily backtest; Alpaca keys for broker execution.
     - Source example; no new full run performed by this documentation update.

ETF and macro teams
-------------------

.. list-table:: Team workflows
   :header-rows: 1
   :widths: 25 30 25 20

   * - Example
     - What it does
     - Data and cadence
     - Evidence
   * - :doc:`Sector pods <agents_example_citadel_sector_pods>`
     - Sector specialists present ideas to a portfolio manager.
     - Daily ETF prices; inspect the published revision for additional data requirements.
     - Source example and public strategy listing; not a performance endorsement.
   * - :doc:`Macro idea meritocracy <agents_example_ray_dalio_idea_meritocracy>`
     - Growth, inflation, and liquidity agents debate allocation.
     - Daily ETF prices; :doc:`FRED/ALFRED <macro_data>` is available for macro extensions.
     - Source example and public strategy listing; inspect the published revision.
   * - :doc:`Leveraged ETF bull/bear team <agents_example_bull_bear_leveraged_etf>`
     - Debate leveraged long and inverse ETFs.
     - Daily prices; advanced instrument and concentration risk.
     - Source example; inspect the page's evidence before running it.

Public strategy listings
~~~~~~~~~~~~~~~~~~~~~~~~

* `Sector Rotation AI Multi-Pod Strategy <https://botspot.trade/marketplace/strategy/0b4576c7-f78b-4477-ba3a-630758fb0168?utm_source=documentation&utm_medium=example_index&utm_campaign=lumibot_ai_examples&utm_content=citadel>`__.
* `Macro Insight AI: Bridgewater-Style Strategy <https://botspot.trade/marketplace/strategy/81af73b8-7dec-4941-ba35-d5a06fee6863?utm_source=documentation&utm_medium=example_index&utm_campaign=lumibot_ai_examples&utm_content=dalio>`__.

These public listings were verified on September 12, 2026. Inspect their
published revisions and available observations; listing availability does not
establish deployment health or performance. BotSpot plans, model usage, data,
and broker requirements may apply.

Options strategies
------------------

.. list-table:: Options workflows
   :header-rows: 1
   :widths: 25 30 25 20

   * - Example
     - What it does
     - Prerequisites
     - Evidence
   * - :doc:`Iron condor <agents_example_ai_iron_condor>`
     - Select and inspect four contracts, submit a multi-leg package, and manage it.
     - Historical option chains/quotes, supported options data, Gemini account.
     - Recorded mechanics evidence includes unavailable-chain/no-order cases.
   * - :doc:`Credit spread <agents_example_ai_credit_spread>`
     - Explore a vertical credit spread with shared option tools.
     - Option-chain and contract-price access; inspect the configured dates/cadence.
     - See the example's recorded evidence and limitations.
   * - :doc:`SPX zero-DTE bear-call team <agents_example_ai_spx_zero_dte_bear_call_team>`
     - Researcher gathers evidence; trading agent refreshes it and decides.
     - SPX option data and supported index-option execution; advanced example.
     - Inspect exact test conditions; no expected-return claim.

Before running a strategy
-------------------------

The original team files default to a broker runner. The stock tutorial supplies
a separate complete backtest runner so learning does not require editing that
mode flag or supplying broker keys. Other examples may use an explicit backtest
runner; read each page before executing its module.

A successful historical run verifies software behavior for that source, data,
and window. It does not establish investment performance. An LLM may know future
facts despite historical market-tool timestamps. These examples are inspired by
public ideas, without affiliation or endorsement from named people or firms.

.. toctree::
   :hidden:

   agents_example_bull_bear_large_cap_stocks
   agents_example_ai_opening_range_breakout
   agents_example_ai_vwap
   agents_example_warren_buffett_value
   agents_example_bill_ackman_concentrated
   agents_example_citadel_sector_pods
   agents_example_ray_dalio_idea_meritocracy
   agents_example_bull_bear_leveraged_etf
   agents_example_ai_iron_condor
   agents_example_ai_credit_spread
   agents_example_ai_spx_zero_dte_bear_call_team
