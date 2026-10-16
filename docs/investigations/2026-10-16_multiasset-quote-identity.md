# Multi-asset market-data identity and contract multipliers

Alpaca's quote reader previously dispatched every non-option/non-crypto asset
through its stock client. A futures root could therefore return an unrelated
equity quote, and forex could yield an empty result rather than an unsupported
asset error. The same fallthrough existed in historical batches.

The Alpaca adapter now validates the base asset's supported market-data type
before provider I/O. Stocks, options and crypto retain their existing request
classes and timestamps. Crypto's forex quote currency remains supported. A mixed
historical batch containing an unsupported asset fails before any request rather
than returning misleading partial bars. No other provider is substituted.
The provider-generic ``UnsupportedAsset`` exception subclasses ``ValueError``;
``Strategy.get_quote()`` and ``Strategy.get_last_price()`` preserve it while retaining the existing empty-quote
fallback for transient provider failures. This keeps the unsupported error
visible to callers of the public strategy API, including hosted data readers.

Asset construction previously overwrote every explicit option multiplier with
100. Omitted option multipliers still default to 100; explicit mini/adjusted
contract terms are retained through construction and serialization. Other asset
types retain a default of 1 and preserve explicit futures multipliers.
The public ``Strategy.create_asset()`` factory delegates omitted multipliers to
those same defaults instead of supplying an explicit 1 for standard options.
Broker-native numeric strings and decimal terms are normalized to numeric values
before valuation. The IBKR position parser is covered directly with its string
multiplier payload. Explicit zero, negative, nonnumeric and nonfinite terms fail
instead of entering account valuation arithmetic.

Regression tests cover all four Alpaca read entry points across futures,
continuous futures, crypto futures, forex and indices, asserting that rejected
requests perform no provider calls. Positive stock, crypto and option quote
contracts preserve identity, bid/ask, midpoint and timestamps. Asset tests cover
standard, mini, adjusted and single-unit multipliers, serialization and futures.
These deterministic tests do not establish live-provider freshness or grant a
provider support for another asset class. Package publication and downstream
runtime verification remain separate release steps.
