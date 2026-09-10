Kalshi
======

Kalshi is a live broker and data source for binary prediction contracts. It uses
the normal LumiBot ``Asset``, ``Order``, ``Position``, strategy methods, and order
lifecycle. Kalshi backtesting is not included.

Account and API key setup
--------------------------------

1. Create a separate `Kalshi Demo account <https://demo.kalshi.co/>`_ to test
   without real money. Production accounts are created at `Kalshi <https://kalshi.com/>`_.
2. In that environment's account settings, open the API keys section and create
   an API key. Save the key ID and the downloaded RSA private key securely.
   Follow Kalshi's `authenticated request guide <https://docs.kalshi.com/getting_started/quick_start_authenticated_requests>`_
   if the account settings change.
3. Set the environment variables below in your local environment or secret manager.
   Demo keys and production keys are separate and cannot be interchanged.
4. Start with the read-only example. Check the account balances before enabling
   any strategy that submits orders.

.. code-block:: text

   IS_BACKTESTING=false
   TRADING_BROKER=KALSHI
   KALSHI_IS_DEMO=true
   KALSHI_API_KEY_ID=<your-demo-key-id>
   KALSHI_PRIVATE_KEY_PATH=<path-to-your-private-key.pem>

Alternatively, set ``KALSHI_PRIVATE_KEY`` to the PEM key itself. It accepts real
newlines or escaped ``\n`` sequences. The inline value takes precedence over
``KALSHI_PRIVATE_KEY_PATH``. This supports secret managers that inject a key into
the process environment without writing a file. Do not commit either the key or
an environment file containing it.

``KALSHI_IS_DEMO`` defaults to ``true``. Explicitly set it to ``false`` only when
using a production account and intentionally trading real money. An invalid
boolean value raises an error. ``KALSHI_SUBACCOUNT`` defaults to ``0`` and accepts
integers from 0 to 63. Balance, position, order-list, and fill requests are scoped
to the selected subaccount.

``DATA_SOURCE=KALSHI`` is optional when the broker is already Kalshi. The broker
creates and shares its Kalshi data source automatically. Public market data can
also be read with ``KalshiData`` without account credentials.

Connection and account values
-----------------------------

The standard credentials loader exports the configured broker as ``BROKER``:

.. code-block:: python

   from lumibot.credentials import BROKER
   from lumibot.strategies import Strategy

   class AccountCheck(Strategy):
       def on_trading_iteration(self):
           pass

   strategy = AccountCheck(
       broker=BROKER, analyze_backtest=False,
       synchronize_broker_on_start=False,
   )
   try:
       if not strategy.update_broker_balances():
           raise RuntimeError("Kalshi account balance request failed")
       print("Cash:", strategy.get_cash())
       print("Total portfolio value:", strategy.get_portfolio_value())
       print("Positions:", strategy.get_positions())
       print("Orders:", strategy.get_orders())
   finally:
       BROKER.cleanup_streams()

For a one-shot read-only script, ``LUMIBOT_CONNECT_STREAM=false`` avoids starting
the background stream. Trading strategies should retain the default streaming
behavior. See ``lumibot/example_strategies/kalshi_account_check.py`` for a
read-only entry point.

``get_cash()`` is available USD cash. Kalshi reports positions value separately
from cash; LumiBot's ``get_portfolio_value()`` is their sum. Dollar fixed-point
fields are preferred over legacy integer-cent fields. The historical account
value method returns an empty dictionary because Kalshi does not expose that
series through this integration.

Assets, YES and NO
--------------------------------

.. code-block:: python

   from lumibot.entities import Asset

   contract = Asset("<KALSHI-MARKET-TICKER>", asset_type="prediction_contract")

Use a **market ticker**, not an event or series ticker. Copy the ticker for a
specific binary market from Kalshi's market details or documented API. This
integration adds no public market-search method.

The existing prediction-contract asset type is shared with Polymarket, but the
identifiers differ: Polymarket uses an outcome token; Kalshi uses one market
ticker and a signed YES position. Positive quantity means YES, negative means
NO. A ``buy`` increases YES exposure; a ``sell`` decreases it and may establish
NO exposure. Selling is not automatically restricted to closing a holding.

All last prices, quotes, candles, limit prices, and fill prices use the YES price
in dollars from 0 to 1. To buy NO at a maximum cost of 0.40, submit a **sell** at
a YES limit price of 0.60. Read account balances from the broker; ordinary stock
short-sale cash arithmetic does not describe collateralized NO contracts.

Quantities can have up to two decimal places and prices up to four; each market
also has its own price ranges/tick increments. The adapter checks the market's
published rules before submitting. Only binary, non-multivariate markets are
supported in this version.

Supported orders
----------------

.. list-table:: Order support
   :header-rows: 1
   :widths: 30 70

   * - LumiBot request
     - Behavior
   * - Limit, ``gtc``
     - Rests until filled or canceled.
   * - Limit, ``ioc``
     - Fills available quantity immediately and cancels the remainder.
   * - Limit, ``fok``
     - Fills the entire requested quantity immediately or cancels it.
   * - Limit, ``gtd``
     - Requires a future, timezone-aware ``good_till_date``.
   * - Market
     - Unsupported; the current Kalshi V2 create endpoint requires a price.
   * - ``day`` time in force
     - Unsupported. Set ``gtc``, ``ioc``, ``fok``, or ``gtd`` explicitly.
   * - Stop, stop-limit, trailing-stop, smart-limit
     - Unsupported; raises before submission.
   * - Bracket, OCO, OTO, multileg
     - Unsupported; there is no emulation using independent legs.

The LumiBot default time in force is ``day``, so **always specify the supported
time in force explicitly**. Unsupported order types/classes and custom provider
parameters raise an actionable error before an order request is sent.

Inside a normal strategy method:

.. code-block:: python

   # Choose a currently tradable ticker and a deliberate price first.
   order = self.create_order(
       contract, 1, "buy", order_type="limit",
       limit_price=0.25, time_in_force="gtc",
   )
   self.submit_order(order)
   current = self.get_order(order.identifier)
   self.modify_order(order, limit_price=0.24)
   self.cancel_order(order)

``modify_order`` supports limit-price changes only. Quantity changes require
canceling and creating a new order. Modification can execute immediately if the
new price crosses the market. ``cancel_order`` always contacts Kalshi, including
when the local status is already ``CANCELLING``.

Plural methods and lifecycle
--------------------------------

- ``submit_order([first, second])`` uses LumiBot's existing independent-order
  submission. It is not an atomic package; one order can succeed while another
  fails. ``submit_orders`` remains available as the existing deprecated alias.
- ``cancel_orders([first, second])`` and ``cancel_open_orders()`` use the normal
  broker helpers.
- ``get_position(asset)``, ``get_positions()``, ``get_order(identifier)``, and
  ``get_orders()`` use Kalshi account state with pagination.
- ``get_last_price(asset)``, ``get_last_prices(assets)``, ``get_quote(asset)``,
  ``get_historical_prices(...)`` and ``get_historical_prices_for_assets(...)``
  use the standard data-source contract.
- There is no new ``create_orders()`` or ``get_quotes()`` method.

Authenticated ``user_orders`` and ``fill`` WebSocket messages wake the existing
``CustomStream`` dispatcher. The adapter reconciles the corresponding order and
cumulative fills through REST and emits normal LumiBot new, partial-fill, fill,
cancel, modified, and error events. Duplicate messages do not repeat fills.
Disconnects trigger reconnect/subscription recovery and periodic REST repair.
Normal order wait helpers and strategy callbacks use those same events.

Use a dedicated account/subaccount for a strategy. Orders already present on
startup are imported without announcing old fills as new strategy executions.
An uncertain submit response retains its client order ID and is reconciled
against later broker state. Do not create a replacement order merely because
a request timed out; verify the first order's outcome.

Historical prices
-----------------

``minute``/``1minute``, ``hour``/``1hour``/``60minute``, and ``day``/``1day``
are supported. Timeshift must be a ``timedelta``. Bars contain traded YES OHLC
prices and contract volume, with a timezone-aware period-end index. The current
incomplete period is excluded. Intervals without trades are omitted, so sparse
markets can return fewer rows than requested. Bid/ask values are not substituted
for missing trades. Archived markets use Kalshi's historical candlestick path.

The existing plural historical path preserves a result or error for each asset.
Plural last-price reads return ``None`` for unavailable assets while preserving
successful prices in the normal ``AssetsMapping`` result.
Unsupported timesteps, non-prediction assets, foreign exchanges, and non-USD
quotes raise clear errors. ``get_chains`` returns ``{}``.

Troubleshooting and verification
----------------------------------------

- **Authentication failure:** verify the Demo/production setting, key ID, RSA
  PEM key, and system clock. Signing requires a millisecond timestamp.
- **Price validation failure:** use the market's current price range/tick size.
- **No candles:** verify the ticker, timeframe, and that trades occurred. Empty
  candles do not mean a trade price of zero.
- **Order status temporarily pending:** order/fill REST views can lag the
  matching engine. The stream and polling reconciliation retry consistent reads.
- **Rate limits or transport errors:** safe GETs retry with bounded backoff.
  Order mutations are not automatically retried.
- **WebSocket disconnect:** allow outbound HTTPS/WSS access to Kalshi's
  `documented API hosts <https://docs.kalshi.com/getting_started/api_environments>`_.

Offline tests run without credentials. Demo read-only, order mutation, and fill
tests are separate ``apitest`` tiers. See ``docs/KALSHI_BROKER_ARCHITECTURE.md``
for exact commands and test credential settings. Mutation tests never use
production hosts. A test-only selector chooses a current contract so the tests
do not depend on one market staying open forever.
