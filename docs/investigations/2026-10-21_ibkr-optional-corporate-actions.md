# IBKR prices must not depend on optional corporate-action metadata

Date: 2026-10-21 (America/Toronto)
Status: implementation and qualification in progress

## Active repair

The reported stock/ETF backtest ran LumiBot 4.6.20. Its first minute-history request raised `corporate_actions_unavailable: current effective session is unverified` inside `_get_current_equity_basis`, before any signal or order. The first recovery wake was delivered but classified the data failure as a non-retryable strategy failure. The implementation here repairs the LumiBot prerequisite first; the separate recovery behavior still requires its own qualification.

Starting source: released 4.6.20 evidence; implementation based on `origin/version/4.6.24` at `18885b88b`. No running strategy, live host, credential, or customer account is a test target.

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

The production deployment consumed exact Development artifacts qualified on a Sunday. Its real integration suite included several stock backtest lifecycle trials and an old-history IBKR price test. The latter passed while the previous Friday's action coverage was sufficient. The integration did not inject failed/stale action metadata. Weekday pre-open Development runs later failed that same old-history test. Private operational receipts are retained in the BotSpot investigation; this public record describes the qualification boundary. These observations do not establish an underlying Yahoo transport outage.


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
- Full portable release selection remains required. Real-model qualification now independently verifies the original proof against an immutable source-compatibility anchor as described below; no inference was purchased or relabeled.
- Private red/green logs remain in task-owned disposable evidence. Preserve this compact source receipt; retire bulk at closeout.

## Compatible AI proof, without another inference run

The native release receipt is preserved byte-for-byte at its original October 21 source and time: 18 cases with 90 successful actor/judge trials and 87 judge calibration controls. The maintained production fixtures always use `PandasDataBacktesting`, not external IBKR/routed price adapters. This repair changes four unused provider modules and package version metadata; it changes no actor, model, judge, fixture, case, shared strategy code, skill, or SDK tuple.

The compatibility verifier reconstructs the original complete runtime hash from immutable Git blobs, verifies that the native receipt is exactly the one committed there, compares the complete current runtime file inventory, and rejects every changed model-relevant byte. The only validator change allowed is the exact, hash-bound entry-point migration; existing validation functions must remain AST-identical, and an early-success edit is rejected. Normal receipt checks still enforce the current entire case catalog, exact SDKs, original model-call identities/usage, successful repetitions, judge controls, and actual freshness timestamps. Compatibility creates no new model execution or freshness. Negative controls reject changed Agent/fixture/shared trading/dependency code, changed validator assertions, changed package dependencies, changed receipts and missing source. A future change outside this narrow provider boundary needs normal targeted native qualification.

Local source compatibility and native receipt verification passed. No model call was made and no previous usage was dropped. The release Actions checkout retains history so it can independently repeat the immutable-source comparison. Full portable test progress is tracked separately; source compatibility does not replace the new IBKR engine/backtest regressions.

Review regressions additionally cover a second backtest in the same process after the metadata source recovers, preservation of an explicit unknown-action marker through Data repair, no verified-zero caching, and once-per-simulated-day optional enrichment retry with a frozen native-price basis. The initial full portable local run timed out after 25 minutes at approximately 40 percent with no reported failure; it is incomplete, not passing. Normal complete CI shards remain required before release.
