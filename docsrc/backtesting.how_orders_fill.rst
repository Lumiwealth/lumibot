How Orders Fill in a Backtest
=============================

.. meta::
   :description: How LumiBot backtests fill stops, limits, gaps and bracket exits from bar data, including which exit fills when one bar touches both.

A backtest only knows each bar's open, high, low and close. It does not know the
order in which prices traded inside the bar. These are the rules LumiBot uses so
results are not better than live trading would have been.

Market orders
-------------

A market order fills at the open of the next bar after it is submitted.

Limit orders
------------

* If the bar opens at or better than your limit (a gap), the order fills at the open.
* Otherwise, if the limit price is inside the bar's range, it fills at the limit price.
* Otherwise it keeps waiting.

Stop orders
-----------

* The stop triggers when the bar's range touches the stop price, and fills at the stop price.
* If the bar opens past the stop (a gap), it fills at the open, not at the stop price.
  A gap can make the loss bigger than you planned, just like in live trading.
* A stop-limit order triggers the same way, then fills only if its limit can be met.
* A trailing stop moves its stop price as the bar's high (for a sell) or low (for a buy)
  moves in your favor, then fills like a stop.

When one bar touches both the stop and the target
-------------------------------------------------

Bracket and OCO orders have a target (a limit) and a stop, and only one may fill. When a
single bar reaches both prices, LumiBot assumes the **stop filled first**. The bar's
high and low cannot tell us which came first, and assuming the target would make
results look better than they were.

The one exception: if the bar **opens beyond the target**, the target fills at the open,
because the open is the first price of the bar.

Examples for a long position with a target at 105 and a stop at 95:

=====================  ===========================  ==================
Bar (open, high, low)  What fills                   Price
=====================  ===========================  ==================
100, 106, 94           Stop (both touched)          95
107, 108, 94           Target (opened past it)      107 (the open)
93, 106, 92            Stop (opened past it)        93 (the open)
100, 106, 99           Target                       105
=====================  ===========================  ==================

The entry bar cannot stop out
-----------------------------

The stop and target of a bracket or OTO order start working on the bar after the entry
fills. A wide entry bar does not stop you out on the same bar you entered.

Intraday data gives more accurate fills
---------------------------------------

All of this is guesswork forced by bar data. Running the backtest on minute bars
(instead of daily bars) makes these "both touched" bars much rarer and the results
closer to live trading.
