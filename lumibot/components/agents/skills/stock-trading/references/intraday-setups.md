# Intraday Stock Setups

## Opening-range breakout

Build the opening range from regular-session bars beginning at the market open.
Use the exact opening-window duration in the user's rules. A long breakout requires
the selected completed bar to exceed the range high under the user's confirmation
rules. A short breakout requires the corresponding break below the range low.

Do not invent an opening range from incomplete bars. Use batch prices and history
for a universe, then perform deeper analysis only on valid finalists. Respect the
user's maximum entries, positions, stops, targets, and session boundaries.

## VWAP

Use a VWAP value computed from bars visible at the current runtime datetime. Do not
substitute an unlabelled average. Evaluate the user's deviation, reclaim, entry,
exit, cooldown, daily-entry, and holding-period rules explicitly.

Repeated threshold crossings are not automatically new trades. Reread positions
and open orders, respect cooldown and entry-frequency rules, and manage an existing
position before reopening.
