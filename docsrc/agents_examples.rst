AI Trading Examples
===================

.. meta::
   :description: Start with a complete strategy, run a historical backtest, and inspect what the agents decided. Model calls require a provider key.

Start with a complete strategy, run a historical backtest, and inspect what the
agents decided. Model calls require a provider key. Each example states its
data requirements and whether the displayed result is a recorded run.

.. image:: ../docs/assets/ai-trading/example-gallery.png
   :alt: AI trading with LumiBot: one agent, agents that debate, or AI combined with Python rules.
   :width: 640px
   :align: center
   :class: lumibot-entry-hero

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
     - Researcher, bull, bear, and interpreter set weights; one trading agent rebalances names such as Apple, Microsoft, and Nvidia.
     - Yahoo daily prices, one decision per session.
     - GPT-6 Luna, January 5 to 15, 2026: split the account across four names, rebalanced daily without churn, kept cash positive, and ended down 2.16% while SPY rose about 1%.
   * - :doc:`Opening range breakout <agents_example_ai_opening_range_breakout>`
     - Inspect completed opening bars and trade a confirmed breakout.
     - Alpaca minute bars, evaluated every two hours.
     - GPT-6 Luna, January 5 to 6, 2026: traded DE, DIS, and SPGI at about 10% of the account and finished down 0.08%. One SPGI buy and sell landed in the same bar.
   * - :doc:`VWAP <agents_example_ai_vwap>`
     - Explore VWAP reclaim and mean reversion.
     - Alpaca minute bars.
     - GPT-6 Luna, January 5 to 6, 2026: no dip-and-reclaim setup appeared, so it stayed in cash. That is the intended no-trade result.
   * - :doc:`Value research team <agents_example_warren_buffett_value>`
     - Research business quality and challenge valuation.
     - Yahoo daily prices, then a real EDGAR read through get_filings and get_filing_section.
     - GPT-6 Luna, January 5 to 15, 2026: bought 708 PG with nearly the whole account and finished up 2.64%. Cash never went negative.
   * - :doc:`Concentrated stock team <agents_example_bill_ackman_concentrated>`
     - Debate one high-conviction large-cap position.
     - Yahoo daily prices, then a real SEC company atom fetch for Pershing Square.
     - GPT-6 Luna, January 5 to 15, 2026: opened GOOGL and MSFT, later added UBER, and finished up 3.95%. Cash never went negative.

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
     - Source example. Its public listing was withdrawn because the backing backtest overspent cash.
   * - :doc:`Macro idea meritocracy <agents_example_ray_dalio_idea_meritocracy>`
     - Growth, inflation, and liquidity agents debate allocation.
     - Daily ETF prices; :doc:`FRED/ALFRED <macro_data>` is available for macro extensions.
     - Source example. Its public listing was withdrawn because the backing backtest overspent cash.
   * - :doc:`Leveraged ETF bull/bear team <agents_example_bull_bear_leveraged_etf>`
     - Debate leveraged long and inverse ETFs such as TQQQ against SQQQ and UPRO against SPXU.
     - Yahoo daily prices. The trader holds one direction per index.
     - GPT-6 Luna, January 5 to 15, 2026: held UPRO and later TQQQ, never an ETF and its inverse together, kept cash positive, and ended up 1.47%.

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
     - Open one four-leg package sized to the risk budget and close it after prices can move.
     - Alpaca option history.
     - GPT-6 Luna, January 5 to 15, 2026: opened 38 SPY February 645/650 put and 715/720 call spreads at real Alpaca prices and ended at $99,734.
   * - :doc:`Credit spread <agents_example_ai_credit_spread>`
     - Open one vertical credit spread sized to the risk budget and close it after prices can move.
     - Alpaca option history.
     - GPT-6 Luna, January 5 to 15, 2026: sold 33 SPY February 655/650 put spreads at real Alpaca prices and ended at $100,627.
   * - :doc:`SPX zero-DTE bear-call team <agents_example_ai_spx_zero_dte_bear_call_team>`
     - Open and close one SPXW bear call on the same expiration day.
     - Alpaca SPXW minute history. January 5, 2026 used the 6900 and 6910 calls.
     - Real QuantStats tear sheet. Open and close both filled. Ending value about $99,925.

Public disclosures and browser automation
-----------------------------------------

.. list-table:: Disclosure and browser workflows
   :header-rows: 1
   :widths: 25 30 25 20

   * - Example
     - What it does
     - Availability boundary
     - Evidence
   * - :doc:`Congress disclosures <agents_example_congress_disclosures>`
     - Read the House Clerk yearly index and PTR PDF, then trade the stock or the listed option after the filing is public.
     - ``ReportDate`` or source publication time, never the earlier transaction date. Amounts are ranges. A report can be up to 45 days late. An option row without strike and expiration is skipped.
     - Does not ship sample trades. Official House and Senate filings are public. Point-in-time tests use invented clock rows, not a member portfolio.
   * - :doc:`SEC Form 4 insider filings <agents_example_sec_insider_filings>`
     - Read point-in-time Form 4 filings for a watchlist, then tilt an equal-weight book toward insider buying.
     - SEC EDGAR submissions and filing documents, capped at the backtest clock.
     - Only filings accepted before the backtest clock are visible.
   * - :doc:`Authenticated browser research <agents_example_browser_research_showcase>`
     - Log in to an authorized JavaScript application, research, trade, and optionally publish a truthful receipt.
     - The observed page state and screenshot receipt at strategy time.
     - Local real-browser acceptance test plus a 100-cycle session restart soak.

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
   agents_example_congress_disclosures
   agents_example_sec_insider_filings
   agents_example_browser_research_showcase

Build your own AI trading bot
-----------------------------

Want help turning your idea into a strategy? Learn with Rob in the free challenge.

.. image:: ../docs/assets/ai-trading/rob-examples.png
   :alt: Learn with Rob Grzesik, creator of LumiBot. Join the FREE challenge.
   :width: 640px
   :align: center
   :class: lumibot-learning-image
   :target: https://botspot.trade/challenges?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading&utm_content=examples_challenge_image
