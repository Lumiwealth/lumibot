Order Types by Broker
=====================

.. include:: _snippets/botspot_mcp_callout.rst

Which brokers can hold which orders, especially the protective ones (stops, trailing
stops, brackets and OCO exits), and what happens to them when your strategy is not
running.

The rule every broker follows: **your order is placed exactly as you asked, or it is
refused with a plain "not supported" error. It is never quietly changed into something
else.** A trailing stop is never sent as a market order, a bracket is never sent without
its stop, and a stop is never sent without its trigger price.

If an order is refused, nothing is sent to the broker. Its ``status`` is ``error``, its
``error_message`` says why, and ``on_error_order`` is called.

Ask the broker in code
----------------------

Brokers add order types over time, so do not copy this table into your strategy. Ask the
broker instead:

.. code-block:: python

   order = self.create_order(asset, 10, Order.OrderSide.SELL, trail_percent=2.0)
   support = self.broker.supports_order(order)
   if not support.supported:
       self.log_message(support.reason)        # plain reason, nothing was sent
   elif support.simulated:
       self.log_message("Only works while this strategy is running")

   # or describe the order without building it
   self.broker.supports_order(order_type="market", order_class="bracket", trailing_leg=True)

``support.mode`` is one of:

* ``"native"``: the broker holds the order. It keeps working even when your strategy is
  stopped.
* ``"simulated"``: LumiBot watches the market and acts for you. It only works while your
  strategy is running.
* ``"unsupported"``: ``submit_order`` refuses it with ``support.reason`` and sends nothing.
* ``"unknown"``: this broker has not declared it yet. LumiBot sends the order as asked.

The answers come from one declaration in each broker class. The same declaration is what
``submit_order`` uses to refuse an order, and a test checks it against each broker's real
order requests, so the code, the refusals and this page cannot drift apart.

Matrix
------

* **Native**: the broker holds it.
* **Simulated**: LumiBot does part of the work while your strategy runs (see below).
* **No**: refused with a "not supported" error.

.. order-support-table-start

====================================  ======  ======  ======  ==========  ========  =========  ======  ============  ======
Broker                                Market  Limit   Stop    Stop-limit  Trailing  Bracket    OTO     Trailing leg  OCO
====================================  ======  ======  ======  ==========  ========  =========  ======  ============  ======
Alpaca                                Native  Native  Native  Native      Native    Native     Native  No            Native
Tradier                               Native  Native  Native  Native      No        Native     Native  No            Native
Schwab                                Native  Native  Native  Native      No        Native     Native  No            Native
Tradovate                             Native  Native  Native  Native      No        No         No      No            No
Bitunix                               Native  Native  No      No          No        Native     Native  No            No
ProjectX                              Native  Native  Native  No          No        Simulated  No      No            No
Interactive Brokers (Client Portal)   Native  Native  Native  Native      Native    No         No      No            No
CCXT exchanges                        Native  Native  Native  Native      Some      No         No      No            No
====================================  ======  ======  ======  ==========  ========  =========  ======  ============  ======

``tests/test_broker_order_support.py`` fails if this table and the brokers' declarations
disagree. ``tests/test_broker_order_type_contract.py`` fails if a declaration and a
broker's real request builder disagree.

Notes
-----

* **Alpaca:** a bracket or OTO entry can be market or limit. Alpaca's stop-loss leg takes
  a fixed stop price only, so a trailing leg is refused; submit a separate trailing stop
  after the entry fills.
* **Bitunix:** the exchange's order endpoint accepts only market and limit orders. Put
  your stop and target on the entry: ``order_class=Order.OrderClass.BRACKET`` (or ``OTO``)
  with ``secondary_stop_price`` and ``secondary_limit_price``. The exchange holds them as
  its stop-loss and take-profit. Standalone stops, trailing stops and OCO are refused.
* **Tradovate:** single market, limit, stop and stop-limit orders. Trailing stops,
  brackets, OTO and OCO are refused for now; submit the entry, then the stop and target as
  separate orders.
* **Tradier and Schwab:** no trailing stops through LumiBot; use a stop or stop-limit.
  The Schwab row is for stocks.
* **ProjectX:** brackets are simulated (see below). Stop-limit, trailing, OTO and OCO are
  refused.
* **Interactive Brokers (Client Portal):** single orders of every type are sent natively,
  including trailing stops. Brackets, OTO and OCO groups are refused until this adapter
  builds parent and child orders.
* **CCXT exchanges:** trailing stops are sent with CCXT's ``trailingPercent`` or
  ``trailingAmount`` only where the exchange says it supports them (for example OKX and
  Binance for percent, Bybit for amount); ``supports_order`` answers per exchange.
  Brackets, OTO and OCO are refused; pass the exchange's own attached take-profit or
  stop-loss fields through ``custom_params`` instead.
* **Interactive Brokers (TWS), Kalshi, Polymarket and MetaMask** have not declared their
  order support yet, so ``supports_order`` answers ``"unknown"`` for them.

If your strategy is not running all the time
--------------------------------------------

Many people run a strategy on a schedule: start it, let it run for a while, stop it, start
it again later. That is fine for orders the broker holds. It is not fine for anything
LumiBot simulates, because **while your strategy process is not running, nothing is
watching the market. Only orders resting at the broker protect you.**

What LumiBot simulates today, and what happens around a stop and restart:

================================  ===========================================  ====================================  ============================================
Simulated by LumiBot              While the strategy is stopped                Survives a restart?                   What the strategy sees on its next start
================================  ===========================================  ====================================  ============================================
ProjectX bracket: placing the     Not placed. If the entry fills while the     The entry and any placed exits come   The filled entry and an open position. If no
stop and target after the entry   strategy is stopped, the position has no     back from the broker and are linked   exit had been placed yet, none is placed after
fills                             stop or target.                              again.                                the restart: protect the position yourself.
ProjectX bracket: cancelling the  The other exit stays open. If the target     Yes. The pair is linked again on the  The filled exit, and the other exit cancelled
other exit when one fills         fills, the stop is still resting.            next start.                           during the first sync.
Smart limit re-pricing            Re-pricing stops. The last limit order       Partly. A strategy that restores its  With restored orders: re-pricing starts over
                                  rests at the broker at its last price.       saved orders keeps the smart limit    from the first step. Without them: a plain
                                                                               settings, but not the step timer.     limit order at the last price.
================================  ===========================================  ====================================  ============================================

Everything marked **Native** in the matrix rests at the broker and keeps protecting your
position while the strategy is stopped. When it starts again, LumiBot reads those orders
back from the broker with their prices and links (stop and target of a bracket or OCO stay
paired), so a fill that happened while it was stopped reaches your strategy.

Backtesting
-----------

The backtester supports every order type and class above, including trailing legs on
brackets and OTO orders. Backtests run continuously, so simulated orders always work
there; check ``supports_order`` before relying on the same orders live. See
:doc:`backtesting.how_orders_fill` for exactly how stops, gaps and "both exits touched in
one bar" are filled.

Frequently asked questions
--------------------------

What happens when I send an order my broker cannot hold?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Nothing is sent. The order's ``status`` becomes ``error``, ``error_message`` explains
which part is not supported, and ``on_error_order`` is called. LumiBot never changes it
into a different order, such as a market order instead of a trailing stop.

How do I check whether a broker supports an order before sending it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call ``self.broker.supports_order(order)``, or describe the order with
``supports_order(order_type=..., order_class=..., trailing_leg=...)``. Check
``support.supported`` and read ``support.reason`` when it is not.

What is the difference between native and simulated orders?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A native order rests at the broker and keeps protecting your position even when your
strategy is stopped. A simulated order is handled by LumiBot and only works while your
strategy is running.

Can I put a trailing stop on a bracket order?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In backtests, yes: pass ``secondary_trail_percent`` or ``secondary_trail_price``. Live,
no broker in the table above holds a trailing leg on a bracket yet, so it is refused.
Submit a separate trailing stop after the entry fills where the broker supports trailing
stops.

How do I protect a Bitunix position with a stop?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Attach it to the entry: use ``order_class=Order.OrderClass.BRACKET`` (or ``OTO``) with
``secondary_stop_price`` and, for a target, ``secondary_limit_price``. Bitunix holds them
as its stop-loss and take-profit. Standalone stop orders are refused on Bitunix.

Is it safe to run my strategy on a schedule instead of all the time?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For native orders, yes: they keep working at the broker while the strategy is stopped.
Simulated orders, such as ProjectX bracket exits and smart limit re-pricing, stop while
the strategy is not running. See the restart table above.
