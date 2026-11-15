Crypto Perpetuals
=================

.. meta::
   :description: Backtest crypto perpetual futures in LumiBot with real perpetual prices, leverage, margin, funding payments and liquidation.

Backtest crypto perpetual futures with real perpetual prices, leverage, margin,
funding payments and liquidation.

A perpetual is a ``crypto_future`` asset. LumiBot's backtesting broker keeps a
futures wallet for it: margin is reserved (not spent), profit and loss settles
in the contract's settlement coin, funding is charged or paid at each real
settlement, and a position that runs out of collateral is liquidated.

Ways to get perpetual data
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Source
     - What you get
     - How to choose it
   * - CCXT, direct or routed
     - The selected exchange's perpetual candles and contract terms, plus
       published funding and historical marks where its public API supports them
     - ``datasource_class=CcxtBacktesting`` with ``exchange_id``, or a route such
       as ``{"crypto_future":"phemex"}``; keep the full settlement symbol
   * - Binance public bulk files (routed backtests)
     - Binance USD-M perpetual candles, hourly mark prices and settled funding,
       from the public file archive. No API key
     - ``BACKTESTING_DATA_SOURCE='{"crypto_future":"binance", ...}'``
   * - Bitunix public market data (routed backtests)
     - Bitunix perpetual candles, contract terms and settled funding (rate and
       mark price at settlement). Public endpoints, no API key
     - ``BACKTESTING_DATA_SOURCE='{"crypto_future":"bitunix", ...}'``
   * - Spot-only sources
     - No native perpetual contract terms or funding. A spot pair cannot stand
       in for an inverse or linear perpetual
     - An exchange that lists only the spot instrument, or a spot-proxy route
       such as ``"ibkr"``; select a venue that actually lists the contract

Data sources
------------

**Native CCXT perpetuals** preserve the exchange's complete instrument identity,
including its quote currency, settlement currency, contract size and native
quantity units. Routed CCXT history uses the same metadata and funding owner as
``CcxtBacktesting``; it does not turn the requested contract into a spot pair.
Both inverse and linear products use provider metadata rather than a guessed
currency suffix. For example, Phemex's ``BTC/USD:BTC`` is priced in USD, settles
in BTC and has a USD face value per contract; ``BTC/USDT:USDT`` settles in USDT.
Choose a reporting wallet deliberately; a BTC balance is not a dollar balance.
Give ``budget`` in that wallet's units, or declare the original denomination and
your supplied conversion rate with :ref:`explicit starting-capital conversion
<backtest-budget-conversion>`. This never converts live collateral automatically.

If the process selects automatic ``public`` crypto routes, the existing
``exchange_id`` backtest option selects the requested CCXT venue without changing
that process's provider map:

.. code-block:: python

   MyStrategy.backtest(datasource_class=None, exchange_id="phemex",
                       quote_asset=Asset("BTC", "crypto"), budget=0.1,
                       benchmark_asset=None)  # 0.1 BTC, not USD 0.1

With no exchange option, the automatic route keeps its Coinbase/Binance defaults.
An explicit provider route such as ``{"crypto_future":"binance"}`` remains
authoritative. ``Asset`` has no ``exchange`` constructor field; declare this
venue through the supported backtest option, not by attaching an attribute.

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"crypto_future":"phemex","crypto":"coinbase","default":"ibkr"}'

.. code-block:: python

   # Public instrument metadata determines native contract units.
   perp = Asset("BTC/USD:BTC", "crypto_future", leverage=3)
   terms = self.broker.get_contract_spec(perp)
   quantity = self.get_quantity_for_notional(perp, 1000, price=50000)
   # On a USD 1-face inverse contract this is 1000 contracts, not 0.02 BTC.

Phemex's public, explicitly bounded history endpoint serves its perpetual trade
and metadata-named mark series. The existing provider parser preserves its native
price scales. Funding uses the public V2 history endpoint and the product's
funding-series metadata. Historical marks use the minute's opening observation
at a settlement boundary, never that minute's later close. Unsupported or missing
marks remain unavailable; traded prices are not relabeled as provider marks.

The shared cache separates exchange, full contract identity, bar size and trade
versus mark data. Public market metadata is shared across instances with a
one-hour refresh. Funding stays a sparse timestamped event series in shared UTC
month partitions, preserving actual payment timestamps and rates. Missing or
failed history is not cached as zero funding or filled with invented events.

Limits: exchange history retention and rate limits apply. Current product
metadata is not an archive of historical contract specification changes. A
provider may expose candles without funding or historical marks; that missing
capability remains visible and does not qualify another venue or a spot proxy.
No authenticated account endpoint is needed for these public data reads.
See Phemex's `Public Contract API documentation
<https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md>`_
for its product fields, history requests and provider limits.

**Binance bulk files** (``"binance"``) read the public archive at
``data.binance.vision``, the same files anyone can download. Every file is checked
against its published SHA-256 checksum before it is used.

- **Covered:** USD-M perpetuals (USDT and USDC margined) from each contract's
  listing on Binance (BTCUSDT 1-minute bars from September 2019): 1-minute, hourly
  and daily candles, hourly mark prices and every funding settlement. The same route
  also serves spot crypto when ``crypto`` is routed to ``binance``.
- **One-day lag:** Binance publishes each day's file the next day. A backtest that
  ends after the last published day stops up front and names that day; nothing is
  filled in. End the backtest on or before yesterday (UTC).
- **Newly published files are retried.** The shared cache covers only the candles
  Binance actually returned. If a daily file was missing on an earlier run, a later
  run requests that missing tail again, including a partial final UTC day. Existing
  cached candles are kept; unavailable prices are never filled or substituted.
- **Funding is published monthly.** Funding is the exchange's own historical rate,
  charged at Binance's mark price at each settlement. Settlements in the current
  month are not published yet, so they are not charged; the log and the data
  provenance say so (``funding_not_published``, with the last covered time).
- **Spot quotes are served exactly.** USDT, USDC and FDUSD pairs come from the
  matching Binance pair. Binance spot has no USD pairs, and USDT does not trade at
  exactly one dollar (about 0.12% below USD in October 2026), so under ``"binance"``
  a ``BTC/USD`` request stops with a message naming ``BTCUSDT`` instead of
  substituting it. Use ``"public"`` (below) to read USD pairs from Coinbase.
- **Contract names:** some low-priced coins trade per 1000 units (``1000PEPEUSDT``);
  use that name, and quantities are in those units. Delisted or renamed contracts
  have no newer files and stop with ``contract_not_listed``.
- **Not covered:** coin-margined (inverse) contracts and dated futures.
- The live Binance API is not used, so this works where that API is blocked.

**Public crypto data, split by quote** (``"public"``). One route for spot and
perpetuals that serves what the strategy asked for:

- USD (and EUR, GBP) pairs such as ``BTC/USD``: Coinbase public candles.
- USDT, USDC and FDUSD pairs such as ``BTC/USDT``: Binance bulk spot files.
- USDT- and USDC-margined perpetuals such as ``BTCUSDT``: Binance USD-M bulk files.

Every source on this route is public: no API key, no broker account and no
Data Downloader. The data provenance names the exchange and the symbol served
(``priceSource``, ``servedSymbol``). Coinbase serves 300 candles per request
under a public limit of about 10 requests a second, so a first load of a year of
1-minute bars takes about three minutes; later runs read the shared bar cache.

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"crypto":"public","crypto_future":"public","default":"ibkr"}'

.. code-block:: python

   # Spot priced in US dollars (Coinbase BTC-USD)
   self.get_historical_prices(Asset("BTC", "crypto"), 60, "minute", quote=Asset("USD", "forex"))

   # Spot priced in USDT (Binance BTCUSDT)
   self.get_historical_prices(Asset("BTC", "crypto"), 60, "minute", quote=Asset("USDT", "crypto"))

   # USDT-margined perpetual (Binance USD-M BTCUSDT, with funding and marks)
   perp = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE, leverage=3)
   self.submit_order(self.create_order(perp, 0.01, "buy"))

**Bitunix** (``"bitunix"``) reads Bitunix's public market API: candles from 2022
(majors) or 2024 (smaller coins) and funding from March 2024. Use it when you want
that venue's own prices and funding.

Prices agree closely between venues in normal markets but can differ by several
percent for a few minutes in a crash, and funding differs by venue. The data
provenance of every backtest names the source it used (``priceSource`` and
``fundingSource``).

**Limits on every route:** maintenance-margin tiers and liquidation fees are not
published, so liquidation happens when collateral runs out and is checked on closed
candles (intrabar liquidation is not modeled).

`BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=backtesting&utm_campaign=lumibot&utm_content=perpetual_data_sources>`_
runs these backtests on hosted workers with the shared data cache already warm, so a
perpetual strategy can go from backtest to paper or live trading without local setup.

Routing perpetuals to Bitunix
-----------------------------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"crypto_future":"bitunix","crypto":"coinbase","default":"ibkr"}'

.. code-block:: python

   from lumibot.entities import Asset
   from lumibot.strategies import Strategy


   class PerpTrend(Strategy):
       def initialize(self):
           self.sleeptime = "1H"
           self.set_market("24/7")
           self.perp = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE, leverage=5)

       def on_trading_iteration(self):
           if not self.get_position(self.perp):
               self.submit_order(self.create_order(self.perp, 0.05, "buy"))


   PerpTrend.backtest(
       None,  # BACKTESTING_DATA_SOURCE chooses the router
       quote_asset=Asset("USDT", Asset.AssetType.CRYPTO),
       budget=10000,
   )

Things to know:

- **Name the contract.** Use the exchange symbol, for example ``BTCUSDT`` or
  ``ETHUSDC``. A bare ``BTC`` needs an explicit USDT or USDC quote; LumiBot never
  guesses which contract you meant.
- **Fund the right wallet.** USDT contracts settle in USDT, so set
  ``quote_asset=Asset("USDT", "crypto")`` and give the budget in USDT. A USD budget
  is not converted silently; orders are rejected with a message saying how to fix it.
- **Quantities are in the coin.** ``0.05`` means 0.05 BTC, as on Bitunix.
- **Coin-margined (inverse) contracts such as ``BTCUSD`` are not supported yet.**
  Bitunix does not publish their contract size or margin coin, so the backtest stops
  before trading with a plain error instead of guessing the payoff.
- **Missing data stays missing.** If Bitunix has no candles for the window (for
  example before a contract was listed) or cannot be reached, the backtest stops
  with that reason. Spot prices are never substituted on this route.
- **Failures are machine-readable.** When the perpetual source cannot serve the
  backtest, the error ends with one line that tools can parse::

     PERPETUAL_DATA_UNAVAILABLE {"alternatives": [{"kind": "spot_proxy", "provides": "...", "routing": "coinbase"}, ...], "asset_class": "crypto_future", "reason": "source_unreachable", "source": "bitunix", "symbol": "BTCUSDT"}

  ``reason`` is one of ``source_unreachable``, ``source_error``,
  ``no_data_for_window``, ``contract_not_listed``, ``coin_margined_unsupported``,
  ``ambiguous_symbol`` or ``asset_class_not_served``. ``alternatives`` lists the
  other ``crypto_future`` routing values and what each provides. The exception is
  ``lumibot.backtesting.routed_backtesting.PerpetualDataUnavailable`` with the same
  values as attributes.
- **Missing funding history is reported, not treated as zero.** Bitunix funding
  history starts on 2024-03-04. When a position is held where the source has no
  funding settlements, nothing is charged there and the run says so in its log
  and backtest assumptions with one machine-readable line::

     PERPETUAL_FUNDING_UNAVAILABLE {"covered_from": "2024-03-04T00:00:00+00:00", "covered_to": "...", "not_covered": [["2024-01-02T14:30:00+00:00", "2024-03-04T00:00:00+00:00"]], "source": "bitunix", "symbol": "BTCUSDT"}

  The same records are in the backtest analysis under
  ``perpetual_funding_unavailable``, and ``perpetual_data_sources`` records which
  source supplied prices and which supplied funding for each contract.
- **Limits.** Maintenance-margin tiers and liquidation fees are not published by
  this source, so liquidation happens when collateral runs out and is checked on
  closed candles (intrabar liquidation is not modeled). The backtest lists these
  assumptions in its log.
- Candles are cached in LumiBot's shared bar cache when it is enabled, so a
  second run of the same window does not download them again.

Provider data-use terms apply to API access and cache reuse; confirm the provider grants the rights required before redistributing market data.
