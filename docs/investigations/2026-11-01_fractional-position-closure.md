# Fractional position closure arithmetic

Broker fill quantities commonly arrive as Python floats. `Order.quantity`
already interprets those values through their decimal string representation,
but creating a position from a fill and updating its quantity used
`Decimal(float)` directly. Mixing these conversions left a nonzero residual
after equal opening and closing fills, including a normal perpetual short
round trip with quantity `0.012`. Crypto positions correctly retain small
quantities, so this numerical residue could remain tracked as an open position.

The repair uses the existing order-quantity convention at the three quantity
boundaries: `Order.to_position`, `Position.quantity` and `Position.add_order`.
Explicit Decimal values remain unchanged. There is no epsilon cutoff or removal
of legitimate small crypto positions. This is general position arithmetic for
every applicable broker and strategy, independent of any particular user.

Regression coverage includes float snapshots followed by Decimal fills, long
and short fractional fills, explicitly tiny Decimal positions, and a complete
normal-strategy perpetual short backtest. The quantity and normal-strategy
regressions failed before the responsible conversions were repaired. The
affected entity, cash, futures, CCXT, Bitunix and perpetual-accounting selection
passed 952 tests and 26 subtests, with two existing skips, on the repaired
candidate before reconciling subsequent version-branch changes.

These are native and simulated-execution checks. They do not establish actual
exchange fills, live private-stream recovery or a deployed package release.
