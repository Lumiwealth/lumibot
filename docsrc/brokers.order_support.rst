Order Types by Broker
=====================

Which brokers can hold which orders, especially the protective ones (stops, trailing
stops, brackets and OCO exits).

The rule every broker follows: **your order is placed exactly as you asked, or it is
refused with a plain "not supported" error. It is never quietly changed into something
else.** A trailing stop is never sent as a market order, a bracket is never sent without
its stop, and a stop is never sent without its trigger price.

If an order is refused, nothing is sent to the broker. Check ``order.status`` (it is
``error``) and ``order.error_message``, or watch ``on_error_order``.

Legend
------

* **Yes**: sent to the broker as that order type. The broker holds it, so it works even
  if your bot is offline.
* **No**: refused with a "not supported" error. Use the alternative in the notes.

Matrix
------

This table is checked by ``tests/test_broker_order_type_contract.py``, which builds
every request offline and fails if a price is dropped or an order is changed. The
Schwab row comes from ``tests/test_order_restart_protective_orders.py`` and a code
review; its stop-limit and trailing cells are not yet in the contract test.

===========================  ======  ======  ======  ==========  ========  =======  =====  ===============  =====
Broker                       Market  Limit   Stop    Stop-limit  Trailing  Bracket  OTO    Trailing leg     OCO
===========================  ======  ======  ======  ==========  ========  =======  =====  ===============  =====
Alpaca                       Yes     Yes     Yes     Yes         Yes       Yes      Yes    No               Yes
Tradier                      Yes     Yes     Yes     Yes         No        Yes      Yes    No               Yes
Schwab (stocks)              Yes     Yes     Yes     Yes         No        Yes      Yes    No               Yes
Tradovate                    Yes     Yes     Yes     Yes         No        No       No     No               No
Bitunix (perpetuals)         Yes     Yes     No      No          No        Yes      Yes    No               No
CCXT exchanges               Yes     Yes     Yes     Yes         Some      No       No     No               No
===========================  ======  ======  ======  ==========  ========  =======  =====  ===============  =====

Notes
-----

* **Alpaca:** a bracket or OTO entry can be market or limit. Alpaca's stop-loss leg takes
  a fixed stop price only, so a trailing leg is refused; submit a separate trailing stop
  after the entry fills.
* **Bitunix:** the exchange's order endpoint accepts only market and limit orders. Put
  your stop and target on the entry: ``order_class=Order.OrderClass.BRACKET`` (or ``OTO``)
  with ``secondary_stop_price`` and ``secondary_limit_price``. A stop-limit exit is sent as
  the exchange's stop-loss with a limit price. Standalone stops, trailing stops and OCO
  are refused.
* **Tradovate:** single market, limit, stop and stop-limit orders are supported. Trailing
  stops, brackets, OTO and OCO are refused for now; submit the entry, then the stop and
  target as separate orders.
* **Tradier and Schwab:** no trailing stops through LumiBot; use a stop or stop-limit.
  Schwab's matrix applies to stocks; options use the same rules where Schwab's option
  templates allow them.
* **CCXT exchanges:** trailing stops are sent with CCXT's ``trailingPercent`` or
  ``trailingAmount`` only where the exchange says it supports them (for example OKX and
  Binance for percent, Bybit for amount); elsewhere they are refused. Brackets, OTO and
  OCO are refused; pass the exchange's own attached take-profit or stop-loss fields
  through ``custom_params`` instead.
* **Interactive Brokers, ProjectX, Kalshi, Polymarket and MetaMask** are not yet covered
  by the contract test. Check that broker's page before relying on a protective order.

Backtesting
-----------

The backtester supports every order type and class above, including trailing legs on
brackets and OTO orders. See :doc:`backtesting.how_orders_fill` for exactly how stops,
gaps and "both exits touched in one bar" are filled.
