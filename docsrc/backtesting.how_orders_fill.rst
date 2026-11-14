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

Resting orders are checked when your strategy runs
--------------------------------------------------

In a backtest, waiting stop, limit and bracket orders are checked each time the strategy
runs (once per ``sleeptime``) and around the market open and close, against the bar at
that moment. Bars between two runs are not checked one by one.

* With daily bars and a daily strategy, each check sees the whole day's range, so a stop
  touched during the day fills at the stop price.
* With minute bars and a strategy that runs less often (for example ``sleeptime = "1D"``
  or ``"15M"``), a stop that is crossed between two runs is only seen at the next run,
  and only if the price is still past the stop then. A dip that recovers before the next
  run does not fill the stop. Live, a stop resting at the broker would have filled.

If your strategy relies on resting stops or limits, run it with a ``sleeptime`` as short
as your bars (``"1M"`` on minute data), or use daily bars for a daily strategy.

Options: every fill price belongs to the fill bar
-------------------------------------------------

Option contracts trade thinly. A contract often has no trade in the minute your order
should fill. LumiBot never fills an option at a price from an earlier bar. For each bar
it uses, in this order:

1. **Quote**: the real bid or ask on that bar, when the data source has quotes (buys pay
   the ask, sells receive the bid).
2. **Trade**: a real trade on that bar (the bar's open).
3. **Estimated**: a model value for that bar, priced with the underlying price on the bar.

   * When minute trade bars for other contracts of the same expiration are already loaded
     in the backtest (at least five that traded earlier in the session), the volatility
     comes from a smooth curve fitted across all of those recent trades, calls and puts
     separately, with newer trades counting more. For index options, in-the-money
     contracts are priced from the more actively traded out-of-the-money side, using the
     forward price implied by calls and puts that traded at the same strike.
   * Otherwise it uses the implied volatility backed out from the contract's own most
     recent real trade. If the contract never traded, it uses the nearest strike with the
     same expiry and type.

   Only trades that happened before the fill bar are used, and nothing extra is downloaded
   for this. The estimate is used as computed: no penalty is added. If you want estimated
   fills to be more conservative, set ``LUMIBOT_BACKTEST_OPTION_ESTIMATE_PENALTY_PCT``
   (percent of the price; buys pay more, sells receive less). The default is ``0``.

Every option fill is marked in the trades file (``price_source`` is ``quote``, ``trade``
or ``estimated``), and ``data_provenance.json`` counts them under ``optionFillPrices``.
If no price can be found (for example nothing has traded yet in that expiry), the order
keeps waiting.

Spreads and condors: submit all legs as one order
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A multi-leg order fills every leg on the same bar, or none of them. A credit or debit
limit is checked against the net of the leg prices. If one leg's time in force ends
first, the whole order is canceled. Closing a spread works the same way.

.. code-block:: python

    legs = [
        self.create_order(long_put, 1, "buy"),
        self.create_order(short_put, 1, "sell"),
        self.create_order(short_call, 1, "sell"),
        self.create_order(long_call, 1, "buy"),
    ]
    self.submit_order(legs, is_multileg=True)

Legs submitted one at a time are separate orders. Each fills on its own, so a spread
submitted that way can be half open for a while.

How close are the estimates?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The estimate was validated against historical quote data (November 2026): 691,306
contract-minutes that had a real bid and ask, on 64 trading days in 2025 (47 calm, 17
volatile), expirations from 0 to 45 days. For each one, the estimate was built only from
trades that happened earlier and compared with the real quote midpoint.

For strikes from 5 delta to at the money (147,293 contract-minutes), error in cents per
share:

=====================================  ============  ===============  ===================
Price used                             Median error  90th percentile  Inside real bid/ask
=====================================  ============  ===============  ===================
Last trade price, unchanged            25.0          390.0            36.8%
Contract's own last trade (model)      17.7          113.1            33.8%
Curve across the expiration (model)    12.1          67.0             44.8%
=====================================  ============  ===============  ===================

The real half-spread in the same sample was 10 cents (median), so the typical estimate is
off by about half a spread. The curve helps most where an estimate matters most: contracts
that have not traded for minutes or days, 1 to 45 days to expiry, and volatile days. For
contracts expiring the same day the two models are about equal (median 4.9 cents both).

Limits of that check, so you know how far to trust it:

* Almost all of the sample (97.6%) is one family of cash-settled index options. The
  remaining rows are calls on one large ETF. Single stocks, and puts on American-style
  options, were not measured.
* The comparison is with the quote midpoint. A real fill is not the midpoint: quote fills
  in LumiBot buy at the ask and sell at the bid, while an estimate is a midpoint-like
  price. Set the penalty above if you want estimated fills to give up some spread.
* Interest and dividends are taken as zero. Fitting calls and puts separately removes most
  of that error for at- and out-of-the-money contracts.
* The curve is only used when the other contracts' trade bars are already loaded. A
  backtest that only ever asked for its own legs uses the contract's own last trade.

An earlier check on SPY 1-minute option trades (November 2026), hiding a real trade and
estimating it from the contract's previous trade, found a median error of about 1% of the
price at the money, 2 to 3% for 5 to 15 delta wings, and about 1 cent for contracts worth
a few cents. Errors grew when the previous trade was 15 minutes or more old (at-the-money
median 13 to 31 cents). Treat estimated fills as approximations.

Spreads held to expiry
~~~~~~~~~~~~~~~~~~~~~~

When options on the same underlying expire in the money and deliver shares in both
directions (for example both legs of a call debit spread, where the long call would
buy shares and the short call would sell them), every one of those options settles in
cash at its intrinsic value. No stock position is left behind.

Example: long the 100 call and short the 105 call, underlying at 110 at expiry. The long
call settles for +10.00 and the short call for -5.00, so the spread pays 5.00 per share
(500 per contract) in cash.

Options that deliver in one direction only (a single long call, a covered call, a
cash-secured put) keep the normal exercise and assignment behavior.
