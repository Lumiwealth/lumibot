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
duration in the strategy. During a regular session both an equity buy stop
and a buy stop-limit triggered, filled and were closed. The stop-limit
execution stayed within its original limit. Market buys and sells of a quarter
share filled for both a stock and an ETF in the cash account. Those orders used
share quantities. A standalone DAY fractional-stock market buy can instead
ask Webull to size the order in dollars. For example, inside a strategy
using the Webull broker:

.. code-block:: python

   from decimal import Decimal

   order = self.create_order(
       "AAPL", 0, "buy", order_type="market", time_in_force="day",
       custom_params={"total_cash_amount": Decimal("25")},
   )
   self.submit_order(order)

The zero quantity means Webull determines the share size. Use the reported
filled share quantity to close. Share rounding can put the execution value
slightly above or below the requested amount before fees. Webull's fractional
eligibility and minimums apply. This parameter is for Webull order execution.
For historical replay, size purchases explicitly in shares.
A DAY sell trailing stop with a five-cent amount filled against an owned
share. A one-percent trail reached the broker with step 0.01 and canceled
unfilled. Webull rejected a 0.001 percentage step in that case. The
intermediate ratchet prices were not independently observed.

For a one-triggers-other order, the stock entry filled and its single profit
or stop exit became a working broker order. Both exits were verified in
separate runs and then canceled. For a one-cancels-other pair protecting
an existing ETF holding, each exit filled in a separate run. The venue
canceled the sibling and the holding returned to its initial quantity.

GTC buy-to-open limits for a listed call, a put and a native call vertical
were accepted, repriced and canceled during that Saturday session. Webull
assigns an ID to each option leg. LumiBot reads those IDs when amending
the original order. Regular-session DAY limits opened and closed a call, a
put, and both debit and credit call and put verticals. The position endpoint
returned the vertical as one signed package. LumiBot recovered the two signed
contract holdings on a fresh position read. Single call and put DAY market buys and
sell-to-close orders also filled in the tested margin account. Session, account and
series eligibility still apply.

Long straddles and strangles also opened and closed through native package
orders. Webull reported each held pair as one position. LumiBot expanded
the signed package quantity into a call and a put with their actual strikes
and expiry. Short-entry executions and partial package fills require separate
qualification.

Native calendar, diagonal, call condor, iron condor and iron butterfly
packages opened and closed in the simulator. A signed position read while
each was open agreed with the reconstructed contracts, dates and quantities.
These checks cover the tested debit calendars, diagonals and call condors
and credit iron structures. Webull rejected a different diagonal structure
submitted as a credit. Covered stock and stock collar orders were accepted,
remained unfilled and were canceled.

A native long call butterfly filled with one contract at each outer strike
and two short contracts at the middle strike. A fresh account read recovered
all three signed holdings. To remove a leg from a grouped holding, pass the
broker position ID and ``custom_params={"leg_in_or_out": "LEG_OUT"}`` on
the closing order. In the simulator the buyback of both middle contracts
left two individual long contracts. Each then sold to close. A native closing
credit limit can rest even after the opening package filled. Two earlier
AAPL closing credits remained unfilled and were canceled by their original
IDs. A subsequent SPY butterfly filled both its opening debit and native
closing credit.

A separate call entry with attached profit and stop exits reached the venue.
All three members had native IDs and were canceled. Sell-to-close stop and
stop-limit orders against an existing call also reached the venue and were
canceled before the holding was closed. These cases establish placement and
cancellation. In another run Webull rejected a sell stop set too close
to the market following a market buy. When the stop was based on the lower
of the observed market reference and the entry fill, the broker accepted it.
Neither that stop nor a separate sell stop-limit triggered in the observation
window. Both were canceled and the position closed.

A one-contract YES round trip and a separate NO round trip both filled in
the event account. Independent filled-order details reconciled with the
account's buying power after exchange fees. Other attempts with a displayed
two-sided quote remained open during the observation window. The test
canceled those orders and confirmed zero fills. Contract settlement still needs separate evidence.

An extended-session equity limit bought and sold one share before the regular
open. In regular hours a stock and an ETF filled both long round trips and
short sales with buybacks. Native equity DAY and GTC brackets were read back
and canceled. In two separate transactions the placing process exited after acknowledgment.
An independent reader then confirmed the entry fill, the winning target or
stop fill, and cancellation of the other exit. A third test kept the entry
position open after the placing process had ended. Two independent reads
five seconds apart found the same two working exits. The observer then
canceled the exits and closed only the test share.

Dated micro-index futures filled in both directions, including a short
sale and buyback. The observed futures cash changes matched the contract
multiplier and the fees on the two executions. A passive futures limit was
repriced and canceled. Separate futures stop, stop-limit and amount-trailing
orders rested at the broker and were canceled without fills. In independent
regular-session runs, a sell stop and a sell stop-limit each triggered and
closed a long dated contract. The stop-limit execution respected the
original sell limit. A separate contract-switch test closed December MES,
opened March MES, then closed March. Each dated symbol and tick price was
read independently. A futures trailing-stop trigger remains unverified.

A US equity trade subscription delivered five-second bars through
``Strategy.get_realtime_bars``. LumiBot timestamps each bar from the full
UTC timestamp in the Webull message header. Snapshot last-trade time and
quote time can differ by an entire weekend. Check the appropriate timestamp
when pricing an extended-hours order. Feed delays vary by instrument.
The tested futures account returned ``MARKET_DATA_NOT_SUBSCRIBED`` for its
dated futures trade stream. A futures REST quote was available. Reading a
denied symbol surfaces the provider code. Other entitled symbols on the
connection can continue to deliver bars. Cancel the subscription when finished.
Removing the last consumer closes the connection.

An equity order can remain pending after a cancel request is rejected. Read
the original client ID again before creating any replacement or close order.
Partial fills, assignment, expiry, crypto trade-tick delivery and production
access still need their own evidence. Use
:doc:`backtesting.webull` for the scope of historical replay.

Webull accounts report cash, exposure and buying power separately. Their
historical security price endpoints do not provide historical account equity.
Read currency-specific buying power with ``broker.get_buying_power("USD")``.
PaperTrade crypto can report zero aggregate cash and equity even when its
simulator reports buying power. Event accounts can omit aggregate cash and
equity.
Webull's app market-data purchases and its OpenAPI non-display entitlements
are separate. Read the `Webull market-data guide <https://developer.webull.com/apis/docs/market-data-api/overview/>`_.
