Interactive Brokers (REST) Backtesting
======================================

LumiBot supports backtesting with **Interactive Brokers data providers**.

The primary data path uses Client Portal (REST) via the LumiBot Data Downloader.

Note: second-level and tick-level history are not yet a fully-supported end-to-end workflow in the open-source
distribution. (Some internal deployments may have additional adapters behind the same external ``/ibkr/*`` contract.)

For **expired futures** contract discovery (conids), IBKR Client Portal cannot reliably discover old contracts. In that
case, LumiBot relies on an **offline conid registry** (populated via a one-time TWS backfill in internal deployments).

Status
------

IBKR REST backtesting is under active development and is not yet a fully-supported public workflow in the open-source
distribution.

If you want early access or have a specific use case, please open an issue (or contact the maintainers) so we can
prioritize it.

Quick Start
-----------

Select IBKR as the backtesting data source:

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE="ibkr"

Supported Data
--------------

- **Futures**: US futures across CME/CBOT/COMEX/NYMEX via IBKR historical endpoints (minute/hour/day bars).
- **Spot crypto**: IBKR crypto bars (availability depends on region and IBKR product support).
- **Stocks / Indexes (day bars)**: supported in routed backtests (for example mixed Theta+IBKR routing).

Daily Stocks/Indexes: Warmup + Corporate Actions
------------------------------------------------

For routed daily stock/index backtests, LumiBot prefetches the full computed lookback window so long
lookbacks (for example 200-day SMA signals) are not under-warmed near the backtest start.

IBKR day-bar payloads do not include corporate-action columns directly. LumiBot enriches cached IBKR
daily equity bars with ``dividend`` and ``stock_splits`` values using Yahoo actions as a best-effort
source so split/dividend accounting remains available in backtests.

Futures Exchange Routing (auto + override)
------------------------------------------

For futures and continuous futures (``asset_type="future"`` / ``asset_type="cont_future"``), LumiBot supports an
optional ``exchange=...`` parameter on the Strategy data methods:

- ``get_historical_prices(..., exchange=...)``
- ``get_last_price(..., exchange=...)``
- ``get_quote(..., exchange=...)``

When ``exchange`` is omitted, LumiBot attempts to resolve the correct futures exchange automatically via IBKR secdef
search (preferring USD + US venues). If results are ambiguous, you must pass ``exchange=...`` explicitly.

Expired Futures Contracts (conids)
----------------------------------

IBKR futures history requires a contract identifier (``conid``). IBKR Client Portal cannot reliably discover ``conid``
values for **expired** futures contracts, which makes explicit-contract backtests fail unless the mapping is already
known.

LumiBot supports an offline conid registry (cache-backed, optionally S3-mirrored):

- ``LUMIBOT_CACHE_FOLDER/ibkr/conids.json``

The registry is expected to **grow automatically over time** for new contracts as backtests run (using IBKR REST
endpoints). Very old expired contracts may still require a one-time offline backfill in internal deployments.

Internal runbook (engineering): ``docs/investigations/2026-01-18_IBKR_EXPIRED_FUTURES_CONID_BACKFILL.md``.

Caching
-------

IBKR backtests cache historical bars as Parquet:

- Local: ``LUMIBOT_CACHE_FOLDER/ibkr/...``
- Optional S3 mirroring: configured via the standard ``LUMIBOT_CACHE_*`` variables (see :ref:`environment_variables`).

For US stock and index day bars, LumiBot checks the expected NYSE sessions after loading the
series. If a completed session is absent, LumiBot makes a small, best-effort request for the
affected month and merges any returned bars into the existing cache. The check runs once per
requested series window, has a process-wide 15-second repair budget, and never turns a partial
data condition into a backtest exception. Complete warm caches do not call the downloader.

No-data markers include a retry timestamp. Until that timestamp expires, the marker prevents
repeated downloads. Legacy markers without a retry timestamp are eligible for one lazy repair
attempt. When a real bar and a no-data marker share a timestamp, the real bar always wins.

Conid lookups also maintain cache files under ``LUMIBOT_CACHE_FOLDER/ibkr``:

- ``conids.json`` stores successful conid resolutions.
- ``conids_negative.json`` stores short-lived negative markers for symbols IBKR cannot resolve.

Negative conid markers prevent long backtests from repeatedly retrying permanently unavailable symbols, such as
defunct equities returned by an external universe API. These lookup failures are treated as terminal no-data
conditions for the affected request window, not as synthetic price data.

Multi-provider routing (Theta + IBKR)
-------------------------------------

To use multiple providers in a single backtest (example: ThetaData for options/stocks/indexes and IBKR for futures/crypto), set a JSON mapping in ``BACKTESTING_DATA_SOURCE``:

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","cont_future":"ibkr","crypto":"ibkr","crypto_future":"ibkr"}'

Routing values are case/whitespace/_/- insensitive. For crypto, you may also route to documented CCXT backtesting paths by using either ``"ccxt"`` (auto-select exchange) or a supported CCXT backtesting exchange id directly (for example: ``"kraken"`` or ``"binance"``).

Crypto futures and perpetuals
-----------------------------

For ``Asset.AssetType.CRYPTO_FUTURE`` backtests, LumiBot can load spot crypto history as a price proxy while preserving the strategy-facing futures asset for orders and positions. USDT contracts such as ``BTCUSDT``, ``ETHUSDT``, and ``SOLUSDT`` resolve to the corresponding USD spot proxy (``BTC/USD``, ``ETH/USD``, ``SOL/USD``) because common crypto perpetual venues keep the futures price close to spot. The log will state when a USD spot proxy is used.

Market Data Subscriptions (IBKR)
--------------------------------

IBKR requires appropriate **market data entitlements** to access market data via the API. IBKR notes that historical bars are part of Level 1 entitlements and that **crypto does not require additional market data subscriptions**:

- https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/

For **CME futures** (ES/MES/NQ/MNQ), note that the cheap **CME S&P Indices** subscription is for index data, not futures contracts. Professional subscriber pricing and package availability can differ, so confirm your exact costs in the IBKR Market Data Subscriptions page and pricing table:

- https://www.interactivebrokers.com/en/pricing/market-data-pricing.php

Authentication / Session Behavior
---------------------------------

The Client Portal Gateway is session-based; if the session becomes unauthenticated, the gateway must be re-authenticated. IBKR documents the expected authentication lifecycle and recommends using ``/iserver/auth/ssodh/init`` to re-authenticate in most scenarios:

- https://www.interactivebrokers.com/campus/trading-lessons/launching-and-authenticating-the-gateway/

Configuration Notes
-------------------

Common environment variables for IBKR REST backtesting:

- ``IBKR_HISTORY_SOURCE`` (default: ``Trades``)
- ``IBKR_FUTURES_EXCHANGE`` (default: ``CME``; fallback when auto-routing fails)
- ``IBKR_CRYPTO_VENUE`` (default: ``ZEROHASH``)
- ``LUMIBOT_IBKR_ENABLE_FUTURES_BID_ASK`` (default: disabled; opt-in quote derivation for futures)

See :ref:`environment_variables` for details.
