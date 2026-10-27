# IBKR equity cache accuracy

Stock contract lookup checks each candidate's `contract/info` currency against
the requested quote asset before selecting its conid. Secdef search order does
not establish currency. The typed and fallback searches share their inspected
currency results within one lookup. Missing or mismatching currency cannot
establish a match; provider metadata errors remain visible.

The check applies only to stock lookups with an explicit quote currency.
Unspecified-quote callers and other asset-class lookup paths retain their prior
behavior. Historical contracts remain eligible without current-tradability or
country checks. This change does not validate, migrate or rewrite existing
conid mappings or price caches. A known incompatible historical cache remains
separate repair work; passing the resolver tests is not cache or backtest proof.

Currency-selection validation: 12 failing regression cases and two compatible
controls before the fix; 551 affected IBKR tests passed afterward. Real provider
contract reads also confirmed an ordinary USD stock, a historical USD contract,
and an explicitly requested MXN listing. These reads did not submit broker
orders or validate historical price files.

The 4.6.19 stock Trades reader uses the current verified corporate-action ledger, even when replaying an older interval. Its split fingerprint forms part of the cache identity. Newly fetched pages carry that identity before any checkpoint or merge. A running data source freezes its first verified ledger and rejects a later different basis. Dividend corrections do not invalidate price caches; verified event enrichment replaces stale event columns.

IBKR documents TWS Trades prices as split adjusted, not dividend adjusted. The configured REST Trades route is separately qualified with captured real equity windows. This attestation is limited to Trades; index Midpoint and explicit quote data are not relabelled as verified equity Trades. See https://interactivebrokers.github.io/tws-api/historical_bars.html.

The reader never repairs a split by recognizing a large price movement. Old unqualified price caches remain on disk, but cannot enter a new fingerprinted series. Prices, volume, dividends and share accounting have separate semantics: volumeFactor must be explicit for equity Trades and is applied once. The volume cache suffix is VOL_V1. A current effective trading session requires current action evidence; a closed weekend uses the last trading session, avoiding repeated attempts to retrieve nonexistent Saturday candles.

Missing required evidence produces a structured incomplete result. Valid zero-trade strategies remain valid. No missing price is fabricated, no market closure is treated as a traded interval, and no result is made valid by changing the strategy. Recovery must preserve the exact source revision/settings.

Permanent tests cover actual saved NVDA, TSLA, AAPL, SQQQ and SPY events, independent decimal ledgers, adjusted versus raw accounting, pending orders, source failure, corrected events, cache extension, changed split identity, session boundaries and exact repeat outputs. Independent-provider comparisons require matched feed/session/adjustment semantics and recorded tolerances; they are not bit-for-bit price oracles. Qualification receipts distinguish fixture engine proof, live provider checks, cloud CI, deployment and actual cache publication.

The initial cache-key transition requires a cold refill; it is not a claim of an instant cache speedup. Qualified repeats make no unnecessary provider calls and do not rewrite unchanged price objects. The separate downloader warmer owns depth, freshness, provider limits and ranking. No new user action or strategy prompt is needed for the cache identity correction.
