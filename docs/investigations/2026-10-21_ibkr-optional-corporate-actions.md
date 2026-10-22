# IBKR prices must not depend on optional corporate-action metadata

Date: 2026-10-21 (America/Toronto)
Status: implementation and qualification in progress

## Active repair

The reported stock/ETF backtest ran LumiBot 4.6.20. Its first minute-history request raised `corporate_actions_unavailable: current effective session is unverified` inside `_get_current_equity_basis`, before any signal or order. The first recovery wake was delivered but classified the data failure as a non-retryable strategy failure. The implementation here repairs the LumiBot prerequisite first; the separate recovery behavior still requires its own qualification.

Starting source: released 4.6.20 evidence; implementation based on `origin/version/4.6.24` at `18885b88b`. Owned worktree: `.claude/worktrees/codex-ugl-corporate-actions`. No running strategy, live host, credential, or customer account is a test target.

The authorized generic change is to preserve usable IBKR-native split-adjusted prices when optional action metadata is unavailable. Missing metadata must remain explicitly unknown. It must not become a false verified-empty action ledger, invent a split adjustment, mix incompatible caches, or abort an otherwise valid intraday backtest. Retain best-effort cash-action enrichment where it is available; it is not a price-data prerequisite.

## Why Yahoo existed

The March change named **Harden IBKR mixed routing parity, speed, and prod-readiness harness** (`47f736937fb0e8b6a6a3effe29c73a9d029f35af`) introduced the lookup. Its code states the reasoning: IBKR day-history responses lack action rows; LumiBot's cash-dividend accounting consumes those rows; Yahoo was a free cached enrichment until a first-party IBKR corporate-action endpoint was implemented. That original helper caught errors and returned best-effort data. This is preserved source-comment evidence, not a claim that the original March model reasoning transcript has been recovered.

October hardening changed the contract: **preserve price moves and fail visibly on unverified corporate actions** (`23e6dce0`) stopped hiding failed lookups; **isolate IBKR equity caches by verified split basis** (`831834fd`) required current real-session coverage for every stock `Trades` request. An October test deliberately requires an exception before a split session is covered. The earlier Codex transcript reports green IBKR tests for that contract. October Claude transcripts diagnose the same pre-open failure and leave it unresolved.

IBKR distinguishes split-adjusted `TRADES` prices, split-and-dividend-adjusted `ADJUSTED_LAST` prices, and a dated cash/event ledger. They are not interchangeable. Snapshot dividend totals are not dated historical entitlements. A complete IBKR historical action-ledger API is not implemented by the current downloader. Do not fabricate one, infer dividends from market moves, or subscribe to a paid feed without authorization.

Primary sources consulted October 21:

- [IBKR historical bars](https://www.interactivebrokers.com/docs/tws-api/doc/md/historical-bars): native `TRADES` prices are split-adjusted; `ADJUSTED_LAST` also adjusts dividends.
- [IBKR snapshot market-data fields](https://www.interactivebrokers.com/docs/web-api/v1/endpoints/market-data/market-data-fields): dividend fields are aggregate/expected amounts, not a complete dated historical ledger.
- [IBKR Wall Street Horizon event fields](https://www.interactivebrokers.com/campus/wp-content/uploads/sites/2/2023/09/WSHEclassesandfieldsforIBAPI2022-12-23.pdf): a separate event-data product; presence in documentation is not proof of this account's entitlements or complete historical coverage.

## Deployment-gate evidence

The current production Bot Manager deployment, **October 18 production promotion** (`39145902211`, commit `86467410e111`), consumed the exact Development artifacts. Its **October 18 Development qualification** (`39101550165`, commit `e77851ebfbc8`) passed 26 integration tests, including three stock backtest lifecycle trials and the old-history IBKR price test. The latter passed around 02:21 Eastern on Sunday. It did not inject failed/stale action metadata. October 19 and October 21 weekday pre-open Development runs then failed that old-history test. These are actual Actions log observations, not an assumption that no backtest gate existed.

## Completion conditions

- [x] Preserve a red regression at the price-request/engine boundary with metadata failure injected.
- [x] Unknown metadata yields real IBKR bars, unchanged OHLCV, explicit provenance/warnings, and no fabricated events.
- [x] Verified, unavailable, and revised adjustment bases cannot contaminate one another's cache.
- [x] Daily and minute consumers, routed dividends, repeated reads, actual split protections, and all affected formal tests pass.
- [x] A portable real-engine backtest completes with real engine fills while metadata is unavailable; it is part of the normal release selection.
- [ ] Bot Manager deploy integration includes an explicit supported-stock native-data smoke and checks result artifacts, installed version, completion and fills.
- [ ] Qualified LumiBot release is published; Bot Manager consumes the exact qualified wheel in Development and production through supported promotion.
- [ ] Production readback proves installed version and a complete bounded backtest; existing running bots remain untouched.
- [ ] Recovery classification/continuation and source-reference version mismatch receive separate regression qualification and platform deployment handoff.

## Source checklist and limits

Prior product investigations, released code, Git blame/history, targeted Codex/Claude transcripts, production read-only records and actual Actions logs are the causal sources. The original March full reasoning transcript has not been located; its source rationale is available. Community sources, Grok/X, Reddit, Hacker News, YouTube/vidIQ/transcripts, Medium, Substack, reviews and search-demand/marketing/UI tools cannot establish this execution history and are not substitutes for it. No claim is made that Yahoo itself timed out during the incident: the exact container response/cache is unavailable. Runtime behavior and independent metadata-failure injection are the acceptance evidence.

## Local qualification receipt

- Red baseline: four stock/minute-history trials (two unrelated ETFs, pre-open and post-close real clocks) fail in the existing metadata prerequisite before any IBKR history response is decoded.
- Patched price-request trials: all four pass, preserve prices, make one metadata attempt, reuse the warm price cache, and reject mixing verified and unknown provenance.
- Real adapter/engine test: `tests/backtest/test_ibkr_native_release_backtest.py` completes with a real engine fill and exact position/cash assertions while `YahooHelper.get_symbol_data` raises `TimeoutError`. Only external transports/contract identity are fixtures. This is not live-provider or production proof.
- Affected selection: 55 IBKR/routing/split/corporate-action test files, excluding only the normal external-service markers `apitest` and `downloader`. Python 3.10: 757 passed before three added provenance tests. Python 3.13.9 with the release's exact SDK constraints: 760 passed, 20 external-service cases deselected, 126.92 seconds.
- Full portable release selection and real-model qualification verification remain required. The current model receipt is incompatible because its runtime fingerprint hashes every LumiBot Python file, including this repair; no inference has been purchased and no prior result has been relabeled. The scoped-repeat rule must be reconciled before any new model calls.
- Private red/green logs are in registered artifact run `cf0beddc-bb45-45fe-8b73-56a1a2215eb8`. Preserve only this compact receipt in Git; retire disposable bulk at closeout.
