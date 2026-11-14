Fundamental Data (SEC Filings) for Backtests and Live Strategies
================================================================

.. meta::
   :description: Free point-in-time fundamental data for Python trading strategies: income statements, balance sheets, cash flow and filings from SEC EDGAR, with no look-ahead in backtests.
   :keywords: fundamental data, SEC EDGAR, 10-K, 10-Q, income statement, balance sheet, cash flow, point in time, backtest, stock scanner, Python

LumiBot gives every strategy free fundamental data for US-listed companies,
straight from SEC EDGAR: income statements, balance sheets, cash flow, any
reported XBRL fact, and the filings themselves. No API key, no paid data feed.

It works the same in backtests and live trading, and in backtests it is
**point in time**: a strategy only sees numbers that were public at the
simulated moment. A plain Python strategy calls ``self.fundamentals`` directly;
AI agents get the same data as built-in tools.

.. include:: _snippets/botspot_mcp_callout.rst

Quick start: a fundamentals scanner
-----------------------------------

This strategy warms the data for its whole universe once, then each month
ranks companies by revenue growth, keeps the ones with modest debt, and buys
the top five. Copy it, change the universe, and backtest it.

.. code-block:: python

   from datetime import datetime

   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.strategies import Strategy

   UNIVERSE = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "JPM", "JNJ", "PG", "XOM"]
   REVENUE = ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"]


   class RevenueGrowthScanner(Strategy):
       def initialize(self):
           self.sleeptime = "1D"
           self.vars.month = None

       def on_trading_iteration(self):
           now = self.get_datetime()
           if self.vars.month == now.month:
               return
           self.vars.month = now.month
           if not self.vars.get("warmed"):
               self.fundamentals.preload(UNIVERSE)  # one parallel download for the universe
               self.vars.warmed = True

           scores = {}
           for symbol in UNIVERSE:
               # Annual revenue as known today: every period, each with the value public at `now`.
               annual = [
                   row for row in self.fundamentals.get_fact_history(symbol, REVENUE)["values"]
                   if row.get("fp") == "FY" and row.get("start")
               ]
               balance = self.fundamentals.get_balance_sheet(symbol)["values"]
               debt = balance.get("debt", {}).get("value")
               equity = balance.get("equity", {}).get("value")
               if len(annual) < 2 or not debt or not equity:
                   continue  # missing data is skipped, never filled in
               growth = annual[-1]["value"] / annual[-2]["value"] - 1
               if debt / equity < 2:
                   scores[symbol] = growth

           picks = sorted(scores, key=scores.get, reverse=True)[:5]
           for position in self.get_positions():
               if position.asset.symbol not in picks and position.asset != self.quote_asset:
                   self.submit_order(self.create_order(position.asset, position.quantity, "sell"))
           budget = self.get_portfolio_value() / max(len(picks), 1)
           for symbol in picks:
               price = self.get_last_price(symbol)
               held = self.get_position(symbol)
               quantity = int(budget // price) - (int(held.quantity) if held else 0)
               if quantity > 0:
                   self.submit_order(self.create_order(symbol, quantity, "buy"))


   if __name__ == "__main__":
       RevenueGrowthScanner.backtest(YahooDataBacktesting, datetime(2024, 1, 1), datetime(2024, 12, 31))

Methods
-------

.. code-block:: python

   self.fundamentals.preload(["AAPL", "MSFT", ...])        # warm a universe in one call
   self.fundamentals.get_income_statement("AAPL")          # latest statement as of now
   self.fundamentals.get_balance_sheet("AAPL")
   self.fundamentals.get_cash_flow("AAPL")
   self.fundamentals.get_fact_history("AAPL", "Revenues")  # every period, as known now
   self.fundamentals.get_company_facts("AAPL")
   self.fundamentals.get_filings("AAPL", form="10-K")
   self.fundamentals.search_filing("AAPL", accession_number="...", query="risk")
   self.fundamentals.get_filing_document("AAPL", accession_number="...")

Every method takes an optional ``as_of``. In a backtest it defaults to the
strategy's current time, and an ``as_of`` later than that is capped to it.

- **Statements** (``get_income_statement``, ``get_balance_sheet``,
  ``get_cash_flow``) return the newest reporting period that was public, with
  each value's tag, unit, period, form, accession number, ``filed`` date and
  ``available_at`` (the moment LumiBot treats it as public). Fields are never
  mixed across periods: a field missing from the newest period is left out and
  listed in ``warnings``.
- **Fact history** (``get_fact_history``) returns every reported period of a
  us-gaap tag (annual, quarterly and year-to-date, exactly as filed), each with
  the value known at ``as_of``. Pass a list of tags in priority order when
  companies use different names for the same thing, such as revenue.
- **Company facts** (``get_company_facts``) returns the latest value of each
  fact, capped at important fields so agent runs stay small. Pass
  ``max_facts=None`` for all of them or ``raw=True`` for the SEC payload,
  filtered to ``as_of``.
- **Filings** (``get_filings``, ``search_filing``, ``get_filing_document``,
  ``list_filing_sections``, ``get_filing_section``) read the filing documents
  themselves. Prefer ``search_filing`` before opening a whole document.

Point in time: what a backtest can see
--------------------------------------

Three rules keep backtests honest:

1. **A value is usable once its filing is public.** The SEC acceptance time is
   used when it can change the answer. A filing accepted during or before a
   weekday session is usable from that moment; one accepted after the 4:00 PM
   New York close (or on a weekend) is usable from the next session's open.
   When no acceptance time is published, the value is usable from 9:30 AM New
   York on the day after its filing date. A filing made after the close is
   never visible at that morning's open.
2. **Restatements apply only from their own filing.** If a later 10-K revises
   last year's revenue, a backtest dated before that 10-K sees the number as
   first reported, and one dated after sees the revision.
3. **The newest period wins.** An amendment filed later for an older year
   never replaces the current statement.

Speed and caching
-----------------

Each company's facts are parsed once per process into an in-memory table keyed
by when each value became public, so a statement call after that is a fast
lookup. Measured on one machine: 180 statement calls (5 companies, 12 monthly
dates) took 0.24 seconds on a warm cache, against 25 seconds before this table
existed.

- ``preload(symbols)`` downloads a whole universe in parallel, spaced to stay
  under the SEC's limit of 10 requests per second. It returns which symbols
  loaded and which have no SEC data (ETFs, foreign listings, crypto), with a
  reason for each.
- Downloads are cached under ``~/.lumibot/cache/sec`` (override with
  ``LUMIBOT_SEC_CACHE_DIR``). When LumiBot's remote backtest cache is
  configured (see :doc:`environment_variables`), backtests also share the raw
  SEC files through it, so a second backtest machine downloads nothing the
  first one already fetched.
- In a backtest, a cached file fetched after the backtest's end date is used as
  is. An older copy may be missing filings inside the window, so it is
  refetched, at most once a day. Live strategies refresh after five minutes.
- LumiBot sends a contact-style user agent, as the SEC asks. Override it with
  ``LUMIBOT_SEC_USER_AGENT``.

Limits
------

- **US SEC filers only.** ETFs, most foreign companies (which file 20-F or
  40-F without full XBRL), crypto and private companies have no data here.
  ``get_filings`` reports that with ``available: false`` instead of raising.
- **Only what companies tag in XBRL.** Older filings (before about 2009 to 2011,
  depending on company size) have no machine-readable facts. Different
  companies use different tags for the same idea, especially revenue, so check
  more than one tag.
- **Dimensional values are not included.** The SEC company facts API only
  carries totals, not segment or share-class breakdowns.
- **Acceptance times** come from the SEC's recent-submissions list, which holds
  roughly the latest thousand filings per company. Older filings use the
  conservative day-after-filing rule.
- **No analyst estimates, no price data.** Combine with your strategy's own
  price data.

Agent tools
-----------

AI agents receive these built-ins: ``get_income_statement``,
``get_balance_sheet``, ``get_cash_flow``, ``get_company_facts``,
``get_filings``, ``search_filing``, ``get_filing_document``,
``list_filing_sections`` and ``get_filing_section``. They follow the same
point-in-time rules.

.. image:: ../docs/assets/ai_committee/docs_sec_point_in_time_cache.png
   :alt: SEC fundamentals and filing research flow

.. image:: ../docs/assets/ai_committee/docs_filing_search_workflow.png
   :alt: SEC filing search workflow for Lumibot agents

Related
-------

- :doc:`macro_data` for rates, inflation, unemployment and other economy-wide
  data from FRED.
- :doc:`standalone_components` to use ``SECFundamentals`` in scripts and
  notebooks without a strategy.
- Want to run a fundamentals scanner without setting anything up? The free
  `BotSpot MCP <https://botspot.trade/agents>`_ lets Claude, ChatGPT or Cursor
  build, backtest and host LumiBot strategies, and
  `botspot.trade <https://botspot.trade>`_ hosts them in the cloud.
