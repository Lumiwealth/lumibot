# Backtesting position entry basis

An execution investigation found that a strategy adding to a position received
the first fill's price from `position.avg_fill_price` after both orders filled.
Stops and profit targets calculated from that value therefore used the wrong
basis. A unit reproduction confirms the defect without a data source or backtest:
buying ten units at 100, then ten at 120, reported 100 instead of 110.

`BacktestingBroker` owns its simulated positions. Its full-fill path updated
quantity but not average entry price; its partial-fill path also failed to
register a newly opened position. The fix updates position basis from executed
quantity deltas at this owner. Increasing exposure weights the entry prices;
reducing exposure preserves the remaining basis; crossing zero starts the new
side at the crossing fill price. Flat positions have no entry basis. Unknown
pre-existing basis remains unknown rather than inventing one from the latest fill.

The generic `Position.add_order` and live broker position reporting are unchanged.
Parent multi-leg/OCO lifecycle handling remains separate. Position entry basis is
distinct from an order's average fill price and from realized P&L/cash accounting.

Regression coverage: `tests/test_backtesting_position_fill_basis.py` exercises the
actual fill handlers with in-memory order/position trackers, including short
positions, reductions, reversals, fractional quantities and partial-to-full fills.
These are unit tests, not proof that a hosted strategy journey passes. Qualification
of a package containing this fix still requires the normal cloud tests and the
downstream hosted execution; no package release is implied by this change.
