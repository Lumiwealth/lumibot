Drift rebalancer valuation
==========================

Understand held option values before using drift to trigger a rebalance.

**Last Updated:** 2026-10-22

**Status:** Active

**Audience:** Strategy authors

.. meta::
   :description: How LumiBot's drift rebalancer values held option contracts, calculates drift, and handles existing order-sizing limits.

Overview
--------

``DriftRebalancerLogic.calculate()`` compares held values with target weights.
The example ``DriftRebalancer`` strategy passes that calculation to its order logic.

Held option values
------------------

An option's current value is its signed contract quantity multiplied by the
per-share price and the :doc:`Asset multiplier <entities.asset>`. For example,
two contracts at a $5 price and multiplier 100 are worth $1,000. With $1,000
cash and a $2,000 portfolio value, a 50% option target has zero absolute or
relative drift and does not trigger a rebalance. Short holdings retain their sign.

Prices remain per share and quantities remain contracts. Explicit multipliers
are used for held option valuation; stock values and quote-asset cash are unchanged.
The strategy still supplies the portfolio-value denominator.

Existing limits
---------------

Option order sizing and cash checks still assume 100 shares per contract.
Correct valuation of an explicit multiplier does not establish general
adjusted-contract or short-option execution support. Rows are still matched by
symbol, so multiple option contracts on the same underlying are not represented
independently. Missing-price errors, fees, slippage and rounding are unchanged.
