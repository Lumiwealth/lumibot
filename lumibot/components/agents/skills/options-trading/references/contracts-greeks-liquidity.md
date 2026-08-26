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
non-finite, crossed, or excessively wide quotes as insufficient evidence. A stale
last trade does not replace a current actionable bid and ask unless the tool
explicitly marks its fallback as usable and the user's rules permit it.

For a multi-leg structure, compare all leg timestamps and quote-quality flags.
Skip the package when one leg cannot be priced honestly.

## Time and backtests

Use only contracts and evidence visible at the current runtime datetime. During a
backtest, never use an expiration list, quote, Greek, or chain snapshot that was not
available at that simulated time.
