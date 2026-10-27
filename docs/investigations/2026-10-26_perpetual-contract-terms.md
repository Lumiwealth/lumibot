# Shared perpetual instrument terms and historical identity

This is a general capability for every strategy using derivative instruments:
economic sizing and serialized contract terms remain usable when moving between
CCXT contract quantities, adapters with base-coin quantities, and simulations.
It would be needed independently of any particular strategy or account.

`ContractSpec` identifies base, quote, settlement, contract size, linear/inverse
P&L and native quantity units. Assets preserve the optional terms when serialized
without changing equality or the shape of older serialized assets. The common
broker/strategy sizing helpers use instrument quote-currency notional; submission
still owns exchange precision and minimums. Existing Bitunix quantities remain
base-coin units; CCXT contract quantities remain contracts.

Live and historical CCXT market selection now share the same resolver. Historical
requests/cache keys preserve full settlement symbols rather than appending the
quote a second time or confusing perpetual candles with spot candles.

Qualification on October 26, 2026 used Python 3.10, CCXT 4.5.64 and real isolated
DuckDB caches for the history integration tests. The new economic-contract tests
failed collection before the feature existed; history reproduced three malformed
perpetual requests while spot passed. The affected CCXT/Bitunix, asset,
serialization, position-identity and candle-execution group passed 1,229 tests,
with two existing skips and 17 external API/downloader tests excluded. The four
cache integration tests passed again after adding guaranteed disposal of their
own temporary databases. Existing CCXT and Asset reference pages built with
Sphinx 8.1.3 and were inspected at desktop and phone widths.

This establishes sizing, serialization, P&L formulas and candle/cache identity.
It does not establish simulation margin/funding/liquidation accounting or actual
exchange order execution. Those require separate qualification.
