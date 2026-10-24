Crypto Exchanges for LumiBot
============================

.. meta::
   :description: Build a crypto trading bot in Python with LumiBot on 100+ exchanges through CCXT, including Binance, Coinbase, Kraken, OKX, Bybit and Hyperliquid, plus Bitunix futures.

LumiBot trades crypto on 100+ exchanges through one shared crypto broker (built on the open source CCXT library), plus a direct Bitunix integration for USDT perpetual futures. Pick your exchange below for a step by step guide with API key setup and a working example.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Crypto broker paths
   :header-rows: 1
   :widths: 26 40 34

   * - Path
     - What it covers
     - Guide
   * - 100+ exchanges (built on the open source CCXT library)
     - Spot, margin, perpetual swaps or futures, depending on the exchange
     - :doc:`How crypto exchange setup works <brokers.ccxt>`
   * - Bitunix
     - USDT perpetual futures only (no spot)
     - :doc:`Bitunix guide <brokers.bitunix>`

.. toctree::
   :maxdepth: 1
   :hidden:

   How crypto exchange setup works <brokers.ccxt>
   Bitunix <brokers.bitunix>
   Alpaca <brokers.alpaca>

.. lumibot-exchange-toctree::

Centralized exchanges
---------------------

You log in to these exchanges with an API key and secret that you create in the exchange's settings.

.. lumibot-broker-index::
   :only: ccxt-centralized

Decentralized exchanges (DEX)
-----------------------------

These exchanges work from a crypto wallet instead of a normal account. Each guide explains which wallet or trading key the bot needs.

.. lumibot-broker-index::
   :only: ccxt-dex

Frequently asked questions
--------------------------

How many crypto exchanges does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The shared crypto broker accepts any exchange id from your installed CCXT version, which lists more than 100 exchanges. The tables above link a step by step guide for each exchange we have written up. Exchange products, API permissions and regions still differ, so read the guide for yours.

What is the difference between a centralized exchange and a DEX?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A centralized exchange holds your account and gives you an API key. A decentralized exchange (DEX) works from a crypto wallet, so the bot signs with a wallet or trading key instead.

Can I test a crypto bot before using real money?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, on exchanges that have a testnet. Each exchange guide says whether a testnet is set up. You can also backtest spot strategies with historical candles. See :doc:`how crypto exchange setup works <brokers.ccxt>`.

Can LumiBot trade crypto perpetual futures?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, on exchanges where CCXT lists perpetual swaps, and on Bitunix. Use the full contract symbol, such as ``BTC/USDT:USDT``.

Can an AI agent trade crypto?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. AI agents send orders through the same LumiBot order path as Python strategies, so they work with every exchange here. See :doc:`agents`.
