# Intraday Stock Setups

## Opening-range breakout

Build the opening range from regular-session bars beginning at the market open.
Use the exact opening-window duration in the user's rules. A long breakout requires
the selected completed bar to exceed the range high under the user's confirmation
rules. A short breakout requires the corresponding break below the range low.

Request bars at the interval named by the rule whenever the data source supports
that interval; `market_historical_prices` supports multi-minute timesteps such as
`5minute`. A bar timestamp identifies the start of its interval: for a
15-minute opening range beginning at 09:30, three five-minute bars starting at
09:30, 09:35, and 09:40 form the range. A five-minute bar starting at 09:45 is
the first later candidate; do not include it in the opening range. If only
one-minute bars are available, aggregate the exact non-overlapping intervals
before comparing closes or volume. Do not infer an interval merely from sparse
timestamps. Never treat the first one-minute constituent of a five-minute window
as a completed five-minute bar or submit an order from that partial window.

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
