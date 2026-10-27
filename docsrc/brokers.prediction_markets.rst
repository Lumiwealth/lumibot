Prediction Market Brokers for LumiBot
=====================================

.. meta::
   :description: Build Kalshi and Polymarket trading bots in Python with LumiBot. Trade and backtest prediction contracts with the same strategy methods.

Trade and backtest prediction contracts from a LumiBot strategy or AI agent. A prediction contract is priced between 0 and 1 and pays out based on how an event resolves.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Prediction market brokers
   :header-rows: 1
   :widths: 25 35 40

   * - Market
     - What LumiBot does there
     - Login
   * - :doc:`Polymarket <brokers.polymarket>`
     - Live trading, order books and historical price backtests
     - Wallet private key, set as ``POLYMARKET_PRIVATE_KEY``
   * - Kalshi
     - Demo or production trading, selected YES/NO quotes and completed-candle backtests
     - API key ID and Ed25519 or RSA private key

.. toctree::
   :maxdepth: 1
   :hidden:

   Polymarket <brokers.polymarket>

Kalshi
------

``Kalshi`` implements the normal broker interface, ``KalshiData`` supplies
market data, and ``KalshiBacktesting`` reuses the Pandas backtester. A market
ticker identifies the event contract; ``outcome`` selects its YES or NO side:

.. code-block:: python

   from lumibot.entities import Asset

   yes = Asset("<market-ticker>", "prediction_contract", outcome="yes")
   no = Asset("<market-ticker>", "prediction_contract", outcome="no")

Inside a Strategy, use ``get_quote``, ``get_last_price``,
``get_historical_prices``, ``create_order``, ``submit_order``, ``get_orders``
and ``get_positions`` as usual. Buying NO buys that selected outcome;
selling NO closes a NO holding. Kalshi nets opposite outcomes in its account,
so buying NO against a YES holding first reduces the YES exposure.
Polymarket's existing outcome token IDs continue working without ``outcome``.

Set ``TRADING_BROKER=kalshi``, ``KALSHI_API_KEY_ID`` and either
``KALSHI_PRIVATE_KEY_PATH`` or ``KALSHI_PRIVATE_KEY``. Demo mode defaults to
``KALSHI_IS_DEMO=true``; demo and production keys belong to separate provider
environments. See :doc:`environment_variables` for optional subaccount,
fund-pool and data-source settings. The account-only check
``python -m lumibot.example_strategies.kalshi_account_check`` places no orders.

For historical simulation, pass ``KalshiBacktesting`` and selected assets to
the normal ``run_backtest`` method. See :doc:`backtesting.how_to_backtest`.
Completed candles, historical bid/ask and cash settlement are modeled;
depth, queue priority and full historical fee schedules remain approximations.
Whole and fractional demo execution is verified. Advanced execution paths and
live settlement reconciliation have separate qualification limits, described
in the `technical integration guide <https://github.com/Lumiwealth/lumibot/blob/dev/docs/KALSHI_ACCOUNT_CONNECTION.md>`_.

Frequently asked questions
--------------------------

Which prediction markets does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kalshi and Polymarket. Both use the existing ``prediction_contract`` asset type and normal Strategy order and data methods. Each has its own broker, data source and backtesting adapter.

Can I backtest a prediction market strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, with Kalshi completed candles or Polymarket price history. See :doc:`backtesting.how_to_backtest` and the :doc:`Polymarket guide <brokers.polymarket>` for their different settlement and execution assumptions.

How is a prediction contract priced?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each outcome token is a ``prediction_contract`` asset priced between 0 and 1 in USD collateral.

Can an AI agent trade on Polymarket?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. AI agents use the same LumiBot order path as Python strategies. See :doc:`agents`.

Does LumiBot support Kalshi?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use ``Kalshi``, ``KalshiData`` and ``KalshiBacktesting`` with a ticker and selected YES/NO outcome. Demo mode is the default; production credentials are configured separately.
