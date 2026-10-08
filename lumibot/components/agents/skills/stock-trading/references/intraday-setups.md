# Intraday Stock Setups

## Opening-range breakout

Build the opening range from regular-session bars beginning at the market open.
Use the exact opening-window duration in the user's rules. A long breakout requires
the selected completed bar to exceed the range high under the user's confirmation
rules. A short breakout requires the corresponding break below the range low.

Load the rule-interval bars with `market_historical_prices`, which returns the
same completed bars the strategy sees and supports multi-minute timesteps such
as `5minute`; pass `table_name` to query them with `duckdb_query`. Request the
interval named by the rule whenever the data source supports it. A bar timestamp identifies the start of its interval: for a
15-minute opening range beginning at 09:30, three five-minute bars starting at
09:30, 09:35, and 09:40 form the range. A five-minute bar starting at 09:45 is
the first later candidate; do not include it in the opening range. If only
one-minute bars are available, aggregate the exact non-overlapping intervals
before comparing closes or volume. Do not infer an interval merely from sparse
timestamps. Never treat the first one-minute constituent of a five-minute window
as a completed five-minute bar or submit an order from that partial window.

When the rule asks for higher volume without naming a baseline, compare the
candidate bar with the opening-range bars of the same session. Pre-market and
after-hours bars are not part of that comparison, and neither are bars that
completed after the candidate. The first completed bar after the range that
meets the rule is the breakout. Unless the user's rules limit how late an entry
may come, it stays a valid signal at a later evaluation while price still holds
above the range high; do not reject it only because later bars have completed.

Do not invent an opening range from incomplete bars. Use batch prices and history
for a universe, then perform deeper analysis only on valid finalists. Respect the
user's maximum entries, positions, stops, targets, and session boundaries.

## VWAP

Use a VWAP value computed from bars visible at the current runtime datetime. Do not
substitute an unlabelled average. Evaluate the user's deviation, reclaim, entry,
exit, cooldown, daily-entry, and holding-period rules explicitly.

A strategy often evaluates less often than its bar interval, for example every 30
minutes on one-minute bars. Unless the user's rules say otherwise, an entry signal
is current when it formed at any completed bar since the previous evaluation and
its condition still holds at the current price. Do not reject it only because
several bars have completed since it formed. A holding period counts bars after
entry; it is not a limit on how old the entry signal may be.

Repeated threshold crossings are not automatically new trades. Reread positions
and open orders, respect cooldown and entry-frequency rules, and manage an existing
position before reopening.
