# CCXT cross-currency account valuation

The shared broker can omit a nonzero holding from account equity when the
reporting currency has no direct exchange pair. Its currency converter currently
supports a direct or inverse pair only, then warns and returns zero.

Does this add value for every strategy, every indicator and every user, and
would we build it if this one person did not exist? Yes. Correctly converting
wallets and settlement PnL is a common account-reporting capability for every
applicable CCXT exchange and strategy. The implementation must use provider
market metadata and observed prices without account-specific branches.

The repair retains direct and inverse conversion, then finds a route through
available markets. Prefer a spot-only route. Where the venue has no spot route
to the reporting currency, a contract's base/quote reference price can provide
a derived mark-to-market value. Such a value can reflect a contract/spot basis;
it is not a fiat redemption guarantee. Option premiums are not currency prices.
Do not assume that a stablecoin equals one unit of fiat currency. Native coin
amounts, contract quantities and contract size keep their existing meanings.

Qualification covers neutral hand-calculated cross rates, negative settlement
PnL, spot-route preference, direct/inverse compatibility, and exclusion of
option premiums and inactive intermediate markets. Private real-account
receipts remain outside the public repository. A passing local calculation
does not establish that production has installed the repair.
