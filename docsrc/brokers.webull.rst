Webull OpenAPI and LumiBot
==========================

This page describes the development candidate for Webull's individual US
OpenAPI. The qualification environment is Webull PaperTrade. Market access
and available product accounts depend on the individual application.
See the `Webull PaperTrade application guide <https://developer.webull.com/apis/docs/authentication/IndividualApplicationAPI/>`_.

Connect an account
------------------

1. In Webull's Developer Console, choose **PaperTrade**, then **API Keys**.
   Apply for PaperTrade and create a dedicated application.
2. Enable the API scopes used by your strategy. Store the generated App Key
   and App Secret in your local secret store. Webull shows the secret once.
3. Discover your account IDs through the signed account-list endpoint. Choose
   the ID of the product account you intend to trade.
4. Install the optional integration with ``pip install "lumibot[webull]"`` when
   it is included in a published release.

For environment-based initialization supply ``TRADING_BROKER=webull``,
``WEBULL_APP_KEY``, ``WEBULL_APP_SECRET``, ``WEBULL_ACCOUNT_ID`` and
``WEBULL_PAPER=true``. Set ``DATA_SOURCE=webull`` for Webull quotes or specify
your independent data source. The data-source path can use the API key pair
without a trade account ID.

Example configuration
---------------------

.. code-block:: python

   from os import environ

   from lumibot.brokers import Webull
   from lumibot.entities import Asset

   broker = Webull(
       {
           "APP_KEY": environ["WEBULL_APP_KEY"],
           "APP_SECRET": environ["WEBULL_APP_SECRET"],
           "ACCOUNT_ID": environ["WEBULL_ACCOUNT_ID"],
           "PAPER": True,
           "TOKEN_DIR": environ.get("WEBULL_TOKEN_DIR"),
       },
       connect_stream=False,
   )

   try:
       print(broker.data_source.get_quote(Asset("AAPL")).price)
   finally:
       broker.cleanup_streams()

Use the ordinary ``Strategy.create_order``, ``submit_order``, ``modify_order``,
and ``cancel_order`` methods for orders. The adapter tracks the client order ID
through placement and recovery. A placement or cancellation acknowledgment
needs a separate order-detail observation before you rely on the result.
An order preview can return a cost estimate while placement returns a
closed-session error. Webull documents preview for simple orders and
excludes crypto. Check the native order ID and order detail to establish
acceptance. A fill also requires an executed quantity and price.

Choose the equity session and order duration explicitly when your strategy
depends on them. The candidate defaults to ``CORE`` when there is no session
choice. To request extended hours, pass
``custom_params={"support_trading_session": "ALL"}`` and verify current
instrument eligibility. For a regular-hours DAY order, pass
``time_in_force="day"`` explicitly.

Qualification status
--------------------

Locally observed Webull simulator paths include a passive equity limit
through placement, amendment and cancellation, a spot crypto market round
trip, a passive event-contract limit/cancel, and a matching private
cancellation event. In a separate spot crypto test, a new Python process
restored a working order after it filled during a stream outage. Two
order-detail observations produced one fill and one strategy callback. A
reconnected private stream delivered the closing order's final-fill event.
Order checkpoints bind to the selected Webull account and environment.

In a Saturday PaperTrade margin-account session, a stock and ETF GTC limit
were placed, amended and canceled. A GTC buy stop and stop-limit on both
instruments were accepted and canceled before a fill. DAY equity market
orders received a closed-session response. A GTC overnight-only equity limit
was rejected because that session requires DAY. GTC trailing and market
orders also received explicit duration rejections. Supply the intended
duration in the strategy. A resting stop's trigger and fill still need
separate confirmation in an eligible session.

GTC buy-to-open limits for a listed call, a put and a native call vertical
were accepted, repriced and canceled during that Saturday session. Webull
assigns an ID to each option leg. LumiBot reads those IDs when amending
the original order. Core-session option fills and opening/closing position
effects still require execution evidence.

A one-contract YES round trip and a separate NO round trip both filled in
the event account. Independent filled-order details reconciled with the
account's buying power after exchange fees. Other attempts with a displayed
two-sided quote remained open during the observation window. The test
canceled those orders and confirmed zero fills. Contract settlement still needs separate evidence.

Futures executions, linked exits, market-data tick delivery and production
access need additional qualification before use in trading. Use
:doc:`backtesting.webull` for the scope of historical replay.

Webull accounts report cash, exposure and buying power separately. Their
historical security price endpoints do not provide historical account equity.
Read currency-specific buying power with ``broker.get_buying_power("USD")``.
PaperTrade crypto can report zero aggregate cash and equity even when its
simulator reports buying power. Event accounts can omit aggregate cash and
equity.
Webull's app market-data purchases and its OpenAPI non-display entitlements
are separate. Read the `Webull market-data guide <https://developer.webull.com/apis/docs/market-data-api/overview/>`_.
