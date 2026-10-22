# Drift Rebalancer Accounting

> Position valuation and accounting boundaries for the drift rebalancer.

**Last Updated:** 2026-10-22
**Status:** Active
**Audience:** Contributors and strategy authors

## Overview

`DriftRebalancerLogic.calculate()` reads held positions, per-unit prices and the
strategy's portfolio value before calculating target values and drift. The example
`DriftRebalancer` strategy passes this result to the existing order logic.

## Held option valuation

`DriftCalculationLogic._add_positions()` values an option as signed contract
quantity × per-share price × `Asset.multiplier`, using Decimal arithmetic. Asset
construction supplies the default multiplier of 100 and normalizes explicit
positive finite terms. Convert the normalized multiplier through its string
representation so fractional terms do not introduce binary-float artifacts.

For example, two contracts at $5 per share with multiplier 100 represent $1,000.
With $1,000 cash and a $2,000 portfolio value, a 50% target has zero drift in both
absolute and relative modes. A short holding retains its negative value.

Apply the multiplier once at this owner. Do not scale the provider's price,
position quantity, target weight or strategy portfolio value. Quote-asset cash
is its quantity and needs no price lookup. Non-option valuation is unchanged.

## Existing boundaries

- The strategy supplies the portfolio-value denominator; calculation does not
  reconstruct broker NAV or change futures margin accounting.
- Rows are matched by symbol, so distinct option contracts on the same underlying
  are not represented independently.
- Option order sizing and cash checks still assume a multiplier of 100. Correct
  valuation of an explicit multiplier does not establish general adjusted-contract
  execution support or extend short-option execution support.
- Missing-price errors, fees, slippage, rounding and provider selection retain
  their existing behavior.

## Regression owners

`TestDriftHeldOptionValuation` in `tests/test_drift_rebalancer.py` exercises real
Asset/Position and calculation/order owners with synthetic external reads. It
checks signed standard and explicit multipliers, fractional Decimal conversion,
both drift modes, no orders when already on target, standard-contract market/limit
resizing, and unchanged stock/cash valuation. It is deterministic component
evidence, not live broker or customer-strategy qualification.

See the [public component guide](../docsrc/drift_rebalancer.rst) and
[broker execution boundaries](BROKER_ORDER_SEMANTICS.md).
