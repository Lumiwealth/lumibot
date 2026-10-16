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

## Local qualification (2026-10-16)

The initial focused baseline reproduced 24 failures; subsequent public API,
factory and native IBKR numeric-term regressions were also observed failing
before their fixes. The final focused set passed 105 tests.

The complete ordinary SDK inventory selected 5,225 cases, with no failures or
missing results: 5,181 passed, 38 skipped, three expected failures and three
nonstrict unexpected passes. Existing external-provider `apitest`/`downloader`
markers and assertions are unchanged. The expected failures concern existing
Yahoo logging/crypto behavior; the unexpected passes also retain their existing
Yahoo markers. These outcomes are not live-provider qualification.

All 5,222 nonbrowser cases ran on source
`430b917ead06c49d95e1cccded4d46fec9d8f548` using three bounded native pytest
workers. The three native Chromium browser cases, including 100 sequential
profile reopen cycles, passed on
`ee4d2e4beb074fbce4531954dd86d761f92417f3`. Their browser owner, tests, fixtures,
lazy-import code, dependency requirements and installed dependency versions
are unchanged in the final candidate; their actual original source is retained.
Total local concurrency never exceeded four test workers. No model inference
was required for this adapter/valuation qualification.

The active version branch advanced to 4.6.20 during qualification. Its only
additional inputs were `setup.py` and `CHANGELOG.md`. After explicit merge into
`3095e1306a152f236bc8573f63f24a079926b6ef`, the owned editable package was installed as
4.6.20 and all 14 affected packaging/release contract tests passed. Application
source remains identical to the full-run candidate. No tag, package release or
deployment was performed by this feature work.

Native report fingerprints:

- `sdk-author-collect.log`: `e72392c51f671f94c3c783efcd44f0c6b26d53e25db07974bf09be7903a6e9a7`
- `sdk-shard-0.xml`: `b9ad4d722dc33a9c87889494ff1e86d253481f6aa024fbd514ee806447cdaab4`
- `sdk-shard-1.xml`: `4d9d9fabe9522cb1cba405b36a21038f012d0c08ac674f641b945fce1774af6b`
- `sdk-shard-2.xml`: `8095bc18303f051deb7f84c79ec955a5a56ff81266dfb39c89729d1b4f29388d`
- `packaging.xml`: `dd36d0a8c3eb2ecb2359288d5c171d4b18b84645633d95f6212b9f005235b656`
