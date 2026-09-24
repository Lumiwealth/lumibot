# Contracts, Greeks, and Liquidity

## Contract selection

Use `options_get_chain`, `options_find_expiration`, and `options_get_strikes` to
discover contracts that actually exist. Never invent a date or strike from a
rounded target.

`options_find_strike_for_delta` returns a candidate. Verify the exact selected
contract with `options_get_greeks`. Do not reuse the delta of a neighboring strike
or the underlying.

## Quote quality

Call `options_evaluate_market` for every intended leg. Treat unavailable,
non-finite, crossed, or excessively wide quotes as insufficient evidence.
Pass `max_spread_pct` only when the user or active rules set a spread limit;
otherwise use the tool's own `usable_for_limit_pricing` verdict. A cheap
protective wing often has a wide percentage spread on a few cents of width; that
alone is not a reason to skip a package whose legs are all usable. A stale
last trade does not replace a current actionable bid and ask unless the tool
explicitly marks its fallback as usable and the user's rules permit it.

Some backtest data sources record option trades but no bid/ask history. There,
`options_evaluate_market` reports `price_basis: "last_trade"` with
`usable_for_limit_pricing: true`, and option legs fill from those real trade bars.
A missing bid/ask alone is then not a reason to skip a trade. Price from the last
trades, use the net price `options_calculate_multileg_price` returns, and still skip
a leg that has no recent trade. In live trading, `last_trade` is never usable for
pricing.

For a multi-leg structure, compare all leg timestamps and quote-quality flags.
Skip the package when one leg cannot be priced honestly.

## Time and backtests

Use only contracts and evidence visible at the current runtime datetime. During a
backtest, never use an expiration list, quote, Greek, or chain snapshot that was not
available at that simulated time.
