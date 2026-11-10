# MetaMask upgrade and complete-window reporting review

Date: 2026-11-10
PR: https://github.com/Lumiwealth/lumibot/pull/1262
Baseline: `d4a3ebb2494fee16013b1b928a426a55d01b51db` (`dev`)
Previous published candidate: `47253a984a28a1a9bd9ae2272f0468db590a8141`

## Findings and corrections

| Finding | Correction | Behavioral evidence |
| --- | --- | --- |
| Legacy token aliases prevent startup/reconciliation and exact cancellation before a venue read. | Keep registry validation strict by default. Identity verification, perpetual read/recovery and cancellation may parse the previously accepted registry, while retaining every other binding and signing check. New trades, owner authorization and EVM valuation remain strict. | Real client startup, SQLite journal and execution engine recover and cancel the exact accepted order. Covers retained effects and older effect-free history, duplicate aliases and unapproved token chains. New intent and authorization reject; valuation rejects; reservations and binding remain intact. Wrong wallet, network, project and provider policy cannot sign. |
| Seven-day default reports truncate 168 hourly funding events to 100 and cannot certify coverage. | Receipt-capable Strategy collection requests the full bounded provider window with `limit=None`, then uses existing receipt batches. List-only broker limits remain unchanged. | Real Strategy reporting, MetaMask client and venue transport deliver 168 distinct events as 50/50/50/18 with one receipt identity and one read of each ledger. Existing acknowledgement/retry tests retain exact batch identities. No second cash debit. |
| Changelog conflicts with current `dev`. | Reapply the approved PR's net delta onto current `dev`, preserving the released 4.6.48 entries and subsequent upstream changes. | Baseline is an ancestor. MetaMask runtime and qualification-script source remains identical to the prior approved candidate before these scoped corrections. Unrelated work is absent from the PR delta. |

The official [funding contract](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding) confirms hourly settlement. The [Info API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint) retains time-range pagination at the last returned timestamp. Existing pagination saturation, conflicting identity and unresolved-category failures remain explicit. A complete producer receipt is usable only after the receiver persists every batch; no downstream runtime qualification is inferred from synthetic reporting responses.

## Coverage inventory

- Binding parser; client startup/authorization; execution authority, prepared-intent proof, accepted-order reads, batch observation and cancellation release.
- EVM account valuation and new execution retain strict parsing. EVM transaction recovery is unchanged.
- Standard Strategy receipt and legacy-list collection; MetaMask ledger pagination and classification; Schwab receipt implementation and tests.
- Normal reporting batches, retries, repeated IDs, incomplete/unsupported windows, no double debit and immutable wallet identity.
- Companion setup/Sphinx guides, changelog and existing CI admission.

## Fresh local checks

- Before correction: both client startup variants fail with `INVALID_BINDING`. The normal seven-day report exhausts its 100 events after two batches and lacks a receipt on later sends.
- After correction: the three initial regressions pass.
- Affected gate: `tests/test_metamask*.py`, `tests/test_cash_events.py`, `tests/test_schwab_cash_events.py`: **734 passed, 5 opt-in connected skipped**, Python 3.12.12. Existing `websockets.legacy` deprecation warning.
- Final extended legacy-registry parameterization: **4 passed**, covering duplicate aliases and unapproved chains with retained/effect-free history. Selections overlap; counts are not additive.
- MetaMask workflow Ruff and format checks: **35 files passed**. Changed shared cash-event tests lint clean. Whitespace check passed.
- Sphinx HTML build completed with exit 0 and no MetaMask diagnostics. Existing unrelated docutils diagnostics remain, including Schwab/Tradier chain docstrings and options-helper markup.
- Prior whole-integration council evidence is retained in [the earlier ledger](2026-11-05_METAMASK_COUNCIL_REVIEW.md). Old CI and installed-wheel/public-data results are historical, not fresh checks of this candidate.

## Council round 9

Pending Terra, Sol and Astra review of the pinned correction and affected callers, with retained rounds 1–8 for unchanged integration. No local review is substituted for required remote CI or maintainer approval.

## Qualification limits

Fresh authenticated testnet execution, controlled maker partial cancellation, funded mainnet/swaps and downstream reporting remain unqualified. Five opt-in connected tests skip without private configuration. No funded action, release, deployment or version change is included. Fresh Python 3.10/3.12 installation and required CI must pass on the published candidate before merge.
