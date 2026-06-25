# Changelog

## 4.5.57 - Unreleased

## 4.5.56 - 2026-06-25

Deploy marker: `deploy 4.5.56`

### Fixed
- **Strict intraday history checks now understand aggregated minute buckets.**
  A routed Coinbase/CCXT request for `5m` BTC/USDT history can use the last
  real minute row inside the requested 5-minute bucket instead of treating the
  normal bucket boundary as stale data, while requests beyond the bucket still
  fail loudly instead of reusing old prices.
- **Resampled intraday bars no longer return incomplete current buckets.** When
  minute/hour data is aggregated into larger intraday bars, LumiBot now drops
  the partially formed current aggregate bucket so strategies do not trade on
  incomplete OHLC values.

### Tests
- **Data entity regressions cover the Greg BTC/USDT 5-minute boundary.** Tests
  assert strict Coinbase-style minute data can satisfy a `5m` history request at
  the bucket boundary, does not include the partial current 5-minute candle, and
  still rejects requests beyond the allowed bucket tolerance.

## 4.5.55 - 2026-06-25

Deploy marker: `deploy 4.5.55`

### Fixed
- **Routed Coinbase/CCXT refreshes no longer keep stale legacy data aliases.**
  When a crypto dataset is refreshed during a routed backtest, LumiBot now
  updates the legacy `(asset, quote)` lookup alias and clears the PandasData
  lookup cache so later strategy history requests use the refreshed bars instead
  of a pre-refresh `Data` object that ended days earlier.

### Tests
- **Routed CCXT regression now covers stale in-memory BTC/USDT aliases.** The
  regression seeds a June 16 BTC/USDT dataset, refreshes June 23 data through
  the routed Coinbase path, and asserts `get_historical_prices()` returns the
  refreshed June 23 bar instead of the stale June 16 bar.

## 4.5.54 - 2026-06-24

Deploy marker: `deploy 4.5.54`

### Fixed
- **Coinbase/CCXT cache ranges now track actual returned candle coverage.**
  Partial provider responses no longer mark the full requested window as cached,
  so a BTC/USDT request that only receives bars through June 16 cannot hide a
  missing June 17 tail from later backtest reads.
- **Empty Coinbase/CCXT responses no longer create fake cache coverage.** Empty
  provider responses leave no executable rows and no coverage metadata, forcing
  future attempts to ask the provider again instead of trusting a phantom range.

### Docs
- **Crypto validation docs now record the post-deploy partial-coverage failure.**
  The runbook explains why cache metadata must be derived from real candles and
  cites the Greg production validation backtest that exposed the issue.

### Tests
- **CCXT cache regressions now cover partial underfills.** Tests assert a
  partial June 16 response for a June 16-17 request does not certify June 17,
  and that a later June 17 read refetches and stores the missing tail.

## 4.5.53 - 2026-06-24

Deploy marker: `deploy 4.5.53`

### Fixed
- **Coinbase/CCXT same-day downloads no longer request future candles.** Crypto
  OHLCV cache downloads clamp current-UTC-day requests to the current time and
  return an empty frame for fully future windows, preventing Coinbase from
  rejecting active backtests with `start must not be in the future`.

### Tests
- **CCXT cache regressions now cover current-day and future-only windows.** Unit
  tests assert current-day downloads are clamped before calling the provider and
  fully future requests do not hit Coinbase.

## 4.5.52 - 2026-06-23

Deploy marker: `deploy 4.5.52`

### Fixed
- **Crypto buy-hold validation now uses full notional exposure.** The controlled
  `buy_hold` validation case explicitly passes `allocation=1.0` so its strategy
  line can be compared directly to the same-asset benchmark instead of looking
  like a half-sized BTC position.
- **Coinbase/CCXT crypto cache pagination now skips sparse empty pages.** Public
  Coinbase OHLCV downloads no longer treat the first empty page as end-of-history
  when later real candles exist inside the requested range, preventing cache
  metadata from claiming full BTC/USDT coverage while the actual candle table
  stops early.
- **Crypto validation now checks actual cache coverage, not just range metadata.**
  The validation runner fails incomplete candle coverage even if
  `cache_dt_ranges` says the requested window is covered.
- **IBKR intraday fills now require the current execution bar.** Direct IBKR and routed-IBKR quote, fast bid/ask, and OHLC fill paths reject minute/hour source rows that do not match the current simulated execution bucket, preventing sparse BTC cache objects from reusing older real rows as current fills.
- **Sparse market-order fills now respect order lifecycle timing.** Stale market
  fill candidates are canceled or left open according to time-in-force semantics
  instead of filling at the current simulation timestamp with a far-away source
  bar.
- **BotSpot snapshot runs can force Schwab and Tradier OAuth token refresh.** Schwab rewrites the existing token file on startup when requested, Tradier OAuth writes the existing BotSpot rotation handoff artifact, and Tradier API-token mode remains unchanged.
- **Tradier OAuth refresh now has a public durable token-file path.** `TRADIER_TOKEN_PATH` loads provider token JSON, writes refreshed token material back atomically, and fails loudly when refreshed token material cannot be persisted. Schwab refreshed-token writes now also fail loudly instead of hiding a failed token-file write.

### Docs
- **Crypto validation docs now call out buy-hold allocation semantics.** The
  note records the corrected BTC/USDT minute artifact where full-allocation
  buy-hold matches the BTC/USDT benchmark to rounding tolerance.
- **Crypto Coinbase validation now records long-run matrix evidence.** The
  validation note lists BTC/USDT minute/hour/day, ETH/USDT, SOL/USDT, and older
  BTC/USD artifact roots with fill-price, fill-time, cache-coverage, and warm
  cache results.
- **Greg BTC investigation now records the local execution-path guard.** The note separates the Crypto Plus data-availability issue from the LumiBot no-stale-fill invariant and lists local targeted test evidence.
- **Tradier docs now describe OAuth token-file durability.** The broker docs and environment-variable reference explain `TRADIER_TOKEN_PATH` separately from manual `TRADIER_ACCESS_TOKEN` usage.
- **TQQQ provider-diff investigations now document execution sensitivity.** The
  notes record the IBKR-first strategy direction, v22/v23 experiments, and
  regular-session helper evidence without changing released strategy behavior.

### Tests
- **Crypto validation runner tests now lock full-allocation buy-hold behavior.**
  The case-plan regression test ensures future buy-hold validation runs remain
  directly comparable with the same-asset benchmark.
- **Crypto CCXT regression tests now cover sparse empty-page pagination and order
  matrices.** Tests cover transient Coinbase market-load retries, sparse-page
  continuation, cache-coverage failure, and market/limit/stop/stop-limit,
  trailing, bracket, OCO, and OTO crypto execution prices.
- **Sparse IBKR crypto regressions now cover stale quote and fast-fill rejection.** Tests assert source-bar quote provenance, stale routed-IBKR quote-fill rejection, exact minute-bucket matching, and a corrected IBKR crypto OCO/OTO fixture that no longer relies on stale prior-minute data.
- **Broker OAuth refresh tests now cover durable write failures.** Tradier tests assert forced refresh updates `TRADIER_TOKEN_PATH` and fails if the file cannot be written; Schwab tests assert forced refresh fails if the refreshed token file cannot be written while preserving the old valid file.

## 4.5.50 - Unreleased

## 4.5.49 - Unreleased

### Fixed
- **Strict intraday data reads now reject stale bars inside sparse cache frames.** When strict end checking is enabled, minute/hour data must resolve to a recent bar near the simulated timestamp instead of walking back days or weeks to the previous real BTC row inside a broader cache object.
- **Malformed rebuilt IBKR history windows are terminal no-data windows.** If the Data Downloader exhausts the rebuild path and still returns a malformed IBKR history payload, LumiBot records the requested range as missing instead of repeatedly refetching the same invalid minute window.

### Docs
- **Greg BTC stale-fill investigation now documents the S3 cache and Data Downloader root cause.** The note records the bad production BTC cache object, the queued downloader proof that stale crypto tails were marked complete/cacheable, and the required cache quarantine path.

### Tests
- **Data entity tests now reproduce stale BTC bars inside sparse intraday frames.** Regression coverage asserts strict minute reads reject March BTC data when an April simulation timestamp is requested, while allowing recent bars inside the configured tolerance.

## 4.5.48 - 2026-06-12

Deploy marker: `pending`

### Fixed
- **Tradier OAuth token rotations now persist back to the broker config.** When a live Tradier request refreshes an expired access token, the broker updates the in-memory auth config so the next request uses the rotated token instead of repeatedly retrying with the stale token.
- **IBKR crypto quote fills now reject underfilled cache windows.** The fast quote-fill path validates that the simulated timestamp is inside the cached data object's actual coverage before reading bid/ask, preventing market orders from filling against out-of-window BTC quote rows when the downloader only returned later history.
- **Routed IBKR crypto prices and quotes now reject stale or future intraday frames.** BotSpot Auto crypto routing marks intraday IBKR data as strict, validates full-window coverage before marking crypto/futures series loaded, and prevents `get_last_price()` or `get_quote()` from treating out-of-window cached BTC rows as the current simulation price.

### Docs
- **Titus Schwab cancel evidence now records deployment and market-hours retest findings.** The investigation note separates the fixed broker cancel path from the remaining strategy-level Titus risks around fills before cancel, rejected stock hedges, retry behavior, and Schwab option smart-limit support.

### Tests
- **Tradier OAuth refresh coverage now asserts rotated tokens are reused.** The regression tests verify refresh state is persisted after retry-layer 401 responses.
- **IBKR crypto backtesting coverage now reproduces an underfilled BTC cache window.** The regression test starts a backtest before the first available BTC minute row and asserts the fast quote-fill helper returns no bid/ask instead of fabricating a fill.
- **Routed IBKR coverage now tests stale-after-end and future-before-start frames.** Direct and routed regressions verify that `get_last_price()` and `get_quote()` return missing data instead of stale/future BTC values when an intraday cache frame does not cover the simulated timestamp.

## 4.5.47 - 2026-06-05

Deploy marker: `e51a0478`

### Fixed
- **Explicit broker cancel/modify calls no longer get suppressed by local order state.** Schwab and Tradier now send `cancel_order()` and `_modify_order()` requests to the broker even when Lumibot's local order status is already `CANCELLING`, filled, canceled, expired, or errored, preventing a strategy-side cancel-pending state from blocking the actual broker API call.
- **Rejected broker-status checks now have a compatibility alias.** `Order.OrderStatus.REJECTED` maps to Lumibot's existing `ERROR` status so generated strategies that check rejected outcomes do not crash while new strategy guidance moves toward `ERROR`.

### Tests
- **Schwab and Tradier regression tests now cover local terminal/cancel-pending states.** The targeted tests include the exact lifecycle where strategy code marks an order `CANCELLING` before broker cancel and assert the broker API is still called.

## 4.5.46 - Unreleased

### Added
- **Schwab cancels now emit order-level diagnostic telemetry.** `Schwab.cancel_order()` logs request metadata, Schwab response status and elapsed time, and a best-effort direct order read after cancel acceptance so support can distinguish missing cancel requests, broker rejection, delayed broker state, and strategy-loop lockups.

### Tests
- **Rob-owned Schwab live cancel diagnostics now cover Titus-style fast option cancels.** Local smoke helpers can load the existing gitignored Schwab token and app metadata, submit a nonmarketable option order, cancel by exact order id, confirm broker state, and optionally compare broad order-list timing outside the hot path.

## 4.5.45 - Unreleased

### Added
- **AI agent model calls now have provider-level request timeouts.** Native Gemini requests pass a Google GenAI HTTP timeout and LiteLLM-backed providers pass a LiteLLM completion timeout, preventing one stalled model request from freezing a strategy for the full agent-run budget.
- **AI agent timeout settings are configurable from strategy code.** `self.agents.create(...)` and `agent.run(...)` now accept `model_request_timeout_seconds` and `run_timeout_seconds`, with documented defaults of 600 seconds per model request and 1800 seconds for the whole agent run.

### Fixed
- **Agent runtime traces now log the effective timeout settings and first ADK event latency.** This makes it clear whether a run is stuck waiting on the model provider or actively making progress through ADK/tool events.

## 4.5.44 - Unreleased

### Added
- **Exact BotManager scheduled runs now wait locally for `targetRunAt`.** Scheduled one-shot live runs can initialize early, wait immediately before `on_trading_iteration()`, write timing telemetry, skip late iterations that exceed the drift budget, and drain post-iteration broker events before exit.

### Fixed
- **Schwab advanced-order cancels now terminalize local child legs.** When Schwab accepts canceling an OTO, OCO, or bracket parent order, LumiBot marks the local parent and child orders canceled so strategy active-order guards do not keep seeing canceled advanced orders as active.
- **Scheduled timing setup now works with lightweight strategy stubs.** `StrategyExecutor` falls back to the module logger when a minimal strategy object has no `logger`, restoring existing unit/backtest test compatibility.

### Tests
- **Rob-owned Schwab live smoke now mirrors Titus-style option cancel and broker-native hedge structures.** The smoke submits a TSLL option limit order, waits four seconds, cancels and verifies the canceled broker state, then submits/read/cancels OTO and bracket option structures against Schwab.

## 4.5.43 - 2026-06-01

Canceled release candidate. Do not deploy.

## 4.5.42 - 2026-06-01

Deploy marker: `deploy 4.5.42`

### Fixed
- **Tradier OAuth deployments now refresh after retry-layer 401 failures.** The Tradier broker catches retry-library errors such as `too many 401 error responses`, forces one OAuth refresh, and retries the broker read instead of failing before the refresh hook can run.

## 4.5.41 - 2026-05-30

Deploy marker: `deploy 4.5.41`

### Added
- **Schwab OTO trigger orders now support explicit parent/child orders.** `Order(order_class=Order.OrderClass.OTO, child_orders=[...])` can now represent a single cross-asset child order, such as an option entry that triggers a stock hedge order, without requiring synthetic secondary limit/stop fields.
- **Schwab OTO, OCO, and bracket submissions now build Schwab advanced-order specs.** The Schwab broker uses `schwab-py` trigger and one-cancels-other builders for single stock/ETF/options parent and child orders. Multi-leg option spreads remain intentionally unsupported until separately implemented and live-tested.
- **AI trading team examples now cover six distinct agent workflows.** Public docs and example files include bull/bear leveraged ETFs, bull/bear large-cap stocks, Ray Dalio-style idea meritocracy, Warren Buffett-style value, Bill Ackman-style concentrated investing, and Citadel-style sector pods with matching workflow diagrams.

### Fixed
- **Schwab trigger-order reads now preserve parent and child order structure.** Broker responses with `orderStrategyType=TRIGGER` parse back into an OTO parent with child orders instead of flattening the response to the first active child.
- **Schwab stop and stop-limit order builders now use the installed `schwab-py` builder API.** This fixes advanced orders with stop exits on current `schwab-py` versions where direct `order_spec` mutation is not available.
- **AI agent order submission now requires fresh account, position, and price context.** Trading agents must check cash, portfolio value, positions, and the ordered asset's last price before `orders_submit_order`; missing context returns a structured error instead of submitting an impossible order or silently resizing it.
- **Long AI agent runs no longer inherit ADK's 500-call default cap.** LumiBot now owns timeout/retry behavior for agent runs instead of letting Google ADK stop complex multi-tool runs with `LlmCallsLimitExceededError`.

### Tests
- **Advanced-order unit coverage now validates OTO, OCO, and bracket spec generation plus Schwab trigger-response parsing.** Live Schwab API smoke tests were also added to submit, read, and cancel nonmarketable OTO, OCO, and bracket orders when credentials are available and the market is closed.
- **Agent order-readiness tests cover missing account context, missing last price, valid submissions, and comma-separated symbol rejection.**

## 4.5.40 - 2026-05-29

Deploy marker: `deploy 4.5.40`

### Fixed
- **Schwab market-order reconciliation no longer leaves missing live hedge orders active forever.** Active local market orders missing from Schwab's broad order list now go through direct broker lookup like other active orders; if the broker still returns no order after the existing new-order grace window, Lumibot marks the local order `UNKNOWN` and non-active instead of letting it block future active-order checks.
- **Schwab simple stock and option order replacement now builds broker specs correctly.** `modify_order()` now reuses the same Schwab stock/option order builders used for new submissions instead of calling missing replacement helper methods, and it safely tracks previous order IDs when Schwab returns a replacement ID.
- **Known Schwab account-history rows are quieter.** Schwab `MUTUAL_FUND` position rows and `EXERCISE` order-history rows are preserved without noisy unknown-warning spam, while truly new/unknown asset and order types still warn and preserve raw broker data.

### Tests
- **Schwab live option replace smoke coverage now exercises the Titus failure path.** The live API test submits a TSLL option limit order, reads it directly and from the active order list, replaces it through Schwab's order-replace endpoint, reads the replacement, cancels it, and confirms the canceled state.
- **Market-order broad-list miss regression coverage was added.** Unit tests cover active market orders missing from non-empty and empty broker order lists, including direct filled reconciliation and unresolved missing-order terminalization to non-active `UNKNOWN`.

## 4.5.39 - 2026-05-29

Canceled release candidate. Do not deploy.

## 4.5.38 - 2026-05-29

Deploy marker: 4.5.38 release commit (`Bump LumiBot to 4.5.38`)

## 4.5.37 - Unreleased

## 4.5.36 - Unreleased

## 4.5.35 - 2026-05-28

Deploy marker: 4.5.35 release commit (`deploy 4.5.35`)

### Fixed
- **Schwab order refresh now skips unsupported mutual-fund and bond order-history legs.** Schwab accounts with sweep-fund activity such as `MUTUAL_FUND` order legs no longer poison order parsing for supported stock, option, future, forex, and index orders.
- **Live order accessors now support active-order filtering with broker refresh.** Strategies can call `get_orders(statuses=Order.ACTIVE_STATUSES)` or `get_order(order_id)` to refresh live broker order state before making cancel/hedge decisions.
- **Broad broker order-list misses no longer create fake local cancels.** If an active local order is missing from a broad broker list, Lumibot now performs a direct broker lookup by order id before updating local state; if that lookup fails, the order remains active with a warning instead of being marked canceled locally.

### Docs
- **Live order and position refresh guidance documents active-order filters and broker reconciliation safety.** The new docs explain when generated/refined strategies should use `Order.ACTIVE_STATUSES` and how Lumibot handles incomplete broker order-list responses.

## 4.5.34 - 2026-05-26

Deploy marker: 4.5.34 release commit (`deploy 4.5.34`)

### Changed
- **AI Investment Committee agents now use explicit role-specific tool surfaces.** Strategies can opt out of automatically adding every built-in tool by passing `include_builtin_tools=False`, letting examples keep research, debate, and trading responsibilities separate.
- **The AI Investment Committee example now keeps Bull and Bear agents argumentative by default.** The Evidence Researcher gathers the bounded evidence pack, Bull and Bear reason over that handoff without reopening the tool loop, and the Portfolio Manager keeps only portfolio/order/price/memory/notification tools.

### Fixed
- **Schwab live order cancellation now calls the broker API.** `Schwab.cancel_order()` now sends the order ID and account hash to `schwab-py` instead of logging an unimplemented-method error, validates missing IDs/setup failures, and dispatches a cancel event after Schwab accepts the cancel.
- **Cached agent tools preserve their callable signatures.** Provider function declarations for cached read-only tools now retain required arguments, preventing missing-argument failures such as `get_fred_snapshot` without `series_ids` or `get_company_facts` without `symbol`.
- **AI order tool results are JSON-safe after broker side effects.** UUID-like order identifiers are serialized before reaching the provider runtime, preventing duplicate-order retry paths caused by post-submit JSON serialization errors.
- **Agent memory search includes append-only event history.** `search_memory` now finds prior thesis, decision, and lesson text even after the current memory projection changes status or text, keeping live observability searchable after updates and closes.
- **SEC normalized statements avoid mixing stale facts across filings.** Fundamental statement helpers now keep fields anchored to a coherent filing/period when possible, preventing old revenue or cash-flow facts from leaking into fresh analysis.
- **Backtest look-ahead warnings no longer flag future SEC proxy meeting dates as future data.** `get_filings` still gates filings by acceptance/filing availability, but a public filing's future `report_date` does not create a false warning.

### Operations
- **AI committee observability validation now has real Gemini artifact proof.** A one-day `gemini-3.5-flash` paid backtest completed end-to-end with four agent traces, role-level tool-call counts, raw trace artifacts, and `stats_agent_detail.parquet` for post-run inspection.
- **TQQQ SMA200 ThetaData acceptance baseline was refreshed for the canonical CI cache result.** The 4.5.34 GitHub Actions S3-backed acceptance run produced `858500` centipercent total return, replacing the stale older baseline while preserving the queue-free ThetaData contract.
- **MELI Deep Drawdown acceptance runtime cap now allows normal CI runner variance.** The metric contract is unchanged, but the runtime ceiling was raised from 600s to 660s after GitHub Actions missed the old cap by 3.6s.

## 4.5.32 - 2026-05-23

Deploy marker: 4.5.32 release commit (`deploy 4.5.32`)

### Changed
- **Lumibot README and docs now frame the agent examples as AI trading teams.** The public docs refresh the BotSpot CTAs, favicon/logo assets, and agent-flow visuals while keeping the existing examples and links intact.

### Fixed
- **Schwab position sync now skips unsupported mutual-fund and bond positions instead of crashing.** Accounts that contain asset classes LumiBot does not model can still sync supported stocks, ETFs, options, futures, and cash-like positions.
- **SEC normalized statements now pick the freshest fact across equivalent tags.** Revenue and similar mapped fields no longer get stuck on an older taxonomy tag when a newer filing reports the same field under another supported SEC company-facts tag.

## 4.5.31 - 2026-05-23

Deploy marker: 4.5.31 release commit (`deploy 4.5.31`)

### Changed
- **AI Investment Committee example now supports explicit cash parking.** The example keeps growth equities and defensive cash-parking ETFs separate, defaults cash-parking symbols to `SGOV`, `BIL`, and `SHV`, and asks the Portfolio Manager to consider short-duration Treasury/cash ETFs when no growth-equity candidate clears the risk/reward bar.

### Fixed
- **Live broker position sync now refreshes broker mark fields on existing positions.** When a broker refresh updates an already-tracked asset, Lumibot now copies current price, market value, P&L, side, and related broker fields instead of only updating quantity, preventing impossible BotSpot position snapshots such as hundreds of shares with a stale two-share market value.
- **Live FRED macro requests no longer use tomorrow's real-time date after UTC midnight.** Default FRED/ALFRED requests clamp the live as-of date to FRED's current Central-time date, preventing after-hours agents from receiving 400 errors while preserving explicit historical `as_of` backtest requests.

### Operations
- **AI committee benchmark notes now record the context-budget follow-up plan.** The investigation doc captures the recommended runtime input-budget layer, affected provider context limits, and the rerun sequence after context-failure review.

## 4.5.30 - Unreleased

### Changed
- **AI agent runs now default to a 30-minute timeout.** `LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS` still overrides the full ADK/model/tool-call timeout, and non-positive values still disable it, but the unset default is now 1800 seconds so slower multi-tool agents are not treated as failed after only five minutes.
- **Trading-enabled AI agent runs no longer retry the whole run by default when mutating order tools are available.** This prevents duplicate broker-side effects when a provider/runtime error happens after `orders_submit_order`, `orders_cancel_order`, or `orders_modify_order` has already executed. Read-only research agents keep the larger retry budget, and `LUMIBOT_AGENT_MAX_RUN_ATTEMPTS` can still explicitly override the default.

### Fixed
- **AI agent tool outputs now coerce UUIDs and other non-JSON scalars before they reach Google ADK/provider serialization.** Submitted order identifiers and similar broker payload fields are converted to JSON-safe values instead of causing `Object of type UUID is not JSON serializable` failures after an order side effect.

## 4.5.29 - 2026-05-20

Deploy marker: 4.5.29 release commit (`deploy 4.5.29`)

### Fixed
- **AI agent runtime no longer blocks tool calls with hidden runtime budgets.** The runtime-enforced tool-call budget introduced in 4.5.28 has been removed because it could block execution tools such as `orders_submit_order` and invalidate agent trading results.
- **AI agent tool results and committee handoffs are no longer silently token-truncated by the benchmark path.** Large handoffs/tool results now preserve the model-visible evidence instead of replacing it with middle-truncated excerpts.
- **AI benchmark usage accounting now aggregates raw traces across all committee roles.** The provider benchmark runner no longer relies on a last-writer detail artifact that undercounted multi-agent token usage and cost.
- **Multi-agent detail parquet files now include all agent rows for stats-file backtests.** Observability artifacts for committee-style strategies include every role instead of only the most recent writer.

### Changed
- **The AI Investment Committee example uses prompt guidance instead of numeric tool-call caps.** The example still asks agents to be concise and targeted, but it does not impose hidden research/follow-up/portfolio tool budgets.
- **The AI committee provider benchmark plan marks the enforced 4.5.28 results invalid for trading conclusions.** The investigation doc records why hidden tool-call and truncation controls polluted the prior 14-day and three-month results, and documents the safer one-model smoke-test plan for Cerebras, direct DeepSeek Flash, and Together after billing propagation.

## 4.5.28 - 2026-05-20

Deploy marker: 4.5.28 release commit (`deploy 4.5.28`)

### Fixed
- **Broker cash-event polling now requests full available history on first successful cloud poll.** Strategies no longer limit the first cloud cash-event fetch to the last seven days, so restarted BotSpot live bots can replay available broker deposit/withdrawal history into the listener.
- **Alpaca journal classifications now match the broker activity contract used for cash-flow adjustment.** `JNLC` cash journals are classified as external deposits/withdrawals by sign, while `JNLS` stock journals remain internal non-cash-flow activity.
- **Tradier transfer/journal classifications now distinguish external funding from internal bookkeeping.** ACH, wire, and check funding activity is treated as deposits/withdrawals, while fee, subscription, and transfer bookkeeping descriptions stay internal unless they match cash/account movement patterns.

### Operations
- **AI committee benchmark notes now record the enforced-qualifier and three-month launch results.** The benchmark plan documents the latest qualifier outcomes and launch artifacts for the provider-comparison work.

## 4.5.27 - 2026-05-20

Deploy marker: 4.5.27 release commit (`deploy 4.5.27`)

### Fixed
- **AI agent runs now have a hard wall-clock timeout.** `LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS` bounds the full ADK/model/tool call, defaulting to 300 seconds, so a live or backtest strategy skips a stalled agent iteration instead of hanging indefinitely inside a provider or built-in tool call.
- **Large AI agent tool results are now token-budgeted before re-entering model context.** Tool responses that would consume too much context are replaced with an explicit bounded excerpt and a truncation notice, preventing raw SEC/fundamental/news payloads from blowing up committee-style agent turns.

### Changed
- **AI agent tool-result budgeting is configurable.** `LUMIBOT_AGENT_TOOL_RESULT_MAX_TOKENS` overrides the default 4,000-token per-result budget for benchmark or large-context-provider runs.
- **The AI Investment Committee example now includes runtime-enforced tool-call discipline.** Research, follow-up, and portfolio-manager agents receive explicit tool-call budgets, and the runtime returns a budget-exceeded notice after the configured call count so benchmark runs measure targeted research instead of uncontrolled tool spraying.

### Operations
- **AI committee provider benchmark tooling was expanded.** The benchmark runner can set per-agent timeout overrides and emit fixed-budget provider reruns, and `scripts/summarize_ai_committee_provider_benchmarks.py` summarizes per-model `result.json` artifacts into compact JSON/Markdown reports.
- **AI committee benchmark documentation now records provider pricing sources, updated cost estimates, observed context-window failures, and the new tool-result/tool-call controls.**

## 4.5.26 - 2026-05-19

Deploy marker: 4.5.26 release commit (`deploy 4.5.26`)

### Operations
- **PyPI project storage was cleaned up for wheel-only releases.** Historical source distributions that had matching wheels were removed from PyPI so new LumiBot releases can publish again under the project storage quota. The package release remains wheel-only.
- **4.5.26 republishes the safe dotenv and lazy-startup release path after the 4.5.25 PyPI quota blocker.** This keeps the deployment path moving without force-moving the stale `v4.5.25` tag.
- **Google ADK is pinned below 2.0 for release stability.** CI exposed a `google-adk` 2.0 function-declaration schema change that drops the expected `parameters` object for the built-in indicator tool. LumiBot keeps the previously passing 1.x range until the 2.x schema contract is reviewed deliberately.

## 4.5.25 - 2026-05-15

Deploy marker: 4.5.25 release commit (`deploy 4.5.25`)

### Changed
- **Package namespace imports are now lazy-loaded.** Top-level namespaces such as `lumibot`, `lumibot.brokers`, `lumibot.data_sources`, `lumibot.entities`, `lumibot.tools`, and `lumibot.traders` defer heavy broker/data/tool imports until first use while preserving common star imports and legacy `entities.*` aliases.
- **Dotenv loading is now safer for runtime-secret deployments.** LumiBot searches upward for the nearest `.env` instead of recursively scanning nested directories, supports sibling `.env.local` overrides for local development, and supports `LUMIBOT_DISABLE_DOTENV` / `LUMIBOT_DISABLE_DOTENV_LOCAL` for injected-env production jobs.

### Fixed
- **CI Ruff checks now point at the current data downloader queue client path.**
- **IBKR REST stock/index conid lookup now filters secdef results by asset type.** Ambiguous tickers such as `MHO` no longer resolve to a futures contract when the strategy requested a stock, preventing false “Chart data unavailable” failures in AlphaPicks backtests.
- **Symbol parsing now uses a single conservative implementation.** `lumibot.tools.parse_symbol` and `lumibot.tools.helpers.parse_symbol` both normalize whitespace/case, require full OCC option-symbol matches, reject empty input, and parse OCC years as 2000-based.

## 4.5.24 - 2026-05-15

Deploy marker: 4.5.24 release commit (`deploy 4.5.24`)

### Fixed
- **IBKR REST backtests now negative-cache unresolvable stock/index conid lookups.** Symbols that IBKR cannot resolve, such as defunct AlphaPicks holdings, are treated as terminal no-data and persisted in the conid negative cache so long BotSpot backtests skip them deterministically instead of hammering secdef/history endpoints until the task times out.
- **PyPI releases now build wheel-only artifacts.** This avoids source-distribution uploads hitting PyPI project storage limits while preserving the artifact BotManager installs for backtest workers.

## 4.5.22 - Unreleased

### Fixed
- **ThetaData option backtests no longer exhaustively download minute quote/OHLC data during delta strike selection.** `OptionsHelper.find_strike_for_delta()` now uses the existing model-based strike selector for ThetaData-routed option backtests, including intraday-cadence strategies, and leaves real historical price validation to `find_next_valid_option()` / `evaluate_option_market()`. This prevents sparse option chains from spending minutes probing empty strikes before any trade is placed.

## 4.5.21 - Unreleased

## 4.5.20 - 2026-05-15

Deploy marker: 4.5.20 release commit (`deploy 4.5.20`)

### Fixed
- **Crypto futures and perpetual backtests can use spot crypto price history through routed backtesting.** `Asset.AssetType.CRYPTO_FUTURE` requests now route through explicit `crypto_future` provider settings, falling back to `crypto` when omitted, and USDT contracts such as `BTCUSDT`, `ETHUSDT`, and `SOLUSDT` can price from the matching USD spot proxy while preserving the original futures asset for strategy-facing orders and positions.
- **Routed backtesting now preserves day-level stock/index data in mixed option workflows.** Stock/index lookups that support option strategies avoid stale minute-frame fallbacks when daily bars are requested.

### Changed
- **Release docs now require prompt review for platform capability changes.** Deployment review includes a token-efficient BotSpot agent prompt/shared-example check alongside the existing documentation and visual asset gates.

## 4.5.19 - 2026-05-14

Deploy marker: 4.5.19 release commit (`deploy 4.5.19`)

### Fixed
- **Alpaca backtests now support multi-timeframe history requests such as `15min`.** `AlpacaBacktesting.get_historical_prices()` now requests native Alpaca minute/hour multiples where supported and only falls back to local aggregation for unsupported aliases such as multi-day bars, so strategy calls like `get_historical_prices(asset, 100, "15min")` match the public strategy API contract instead of failing validation.

### Changed
- **LumiBot release docs now include a Codex-safe remote-first release path for shared checkouts.** AI agents can avoid switching the canonical local checkout when unrelated dirty files are present while preserving the same version-branch, PR, tag, PyPI, and BotManager deployment invariants.

## 4.5.18 - 2026-05-14

Deploy marker: 4.5.18 release commit (`deploy 4.5.18`)

### Fixed
- **IBKR-routed stock/index lookups in option backtests now stay on native daily bars.** Implicit stock/index `get_last_price()` and `get_quote()` calls no longer fall through to IBKR minute history after an option backtest has observed intraday cadence.
- **ThetaData option OHLC downloader waits are now bounded separately.** One stuck sparse option contract/day no longer inherits the broad OHLC history timeout used for larger history downloads.

## 4.5.17 - 2026-05-14

Deploy marker: 4.5.17 release commit (`deploy 4.5.17`)

### Fixed
- **ThetaData quote-history requests now use a bounded quote-specific timeout.** Point-in-time option quote lookups no longer inherit the larger OHLC history timeout, so one stuck quote request fails visibly instead of making BotSpot option backtests appear frozen early in the run.

## 4.5.16 - 2026-05-14

Deploy marker: 4.5.16 release commit (`deploy 4.5.16`)

### Fixed
- **Routed daily stock/index backtests no longer satisfy day lookups from stale minute frames.** `RoutedBacktestingPandas` now pins stock/index day requests to native daily bars, and `ThetaDataBacktestingPandas.get_last_price()` / `get_quote()` preserve daily cadence instead of falling back through base `PandasData` lookups that can resolve May-only minute caches during April AlphaPicks option backtests.

## 4.5.15 - 2026-05-13

Deploy marker: 4.5.15 release commit (`deploy 4.5.15`)

### Fixed
- **IBKR REST daily stock/index price lookups now treat raw ticker strings as stock assets.** Strategies that call `get_last_price("MU")` or `get_quote("MU")` now use the same daily stock/index refresh path as `Asset("MU", "stock")`, preventing AlphaPicks option backtests from falling back to end-anchored IBKR `1min` data when the strategy passes string symbols.

## 4.5.14 - 2026-05-13

Deploy marker: 4.5.14 release commit (`deploy 4.5.14`)

### Fixed
- **IBKR REST daily stock/index backtests no longer fall back to minute data after a stale daily slice.** When daily stock/index data is already loaded but missing the current simulation timestamp, `get_last_price()` and `get_quote()` now refresh a bounded daily slice around the simulation date and return empty/None if that still fails. This prevents daily AlphaPicks option backtests from making unnecessary IBKR `1min` requests anchored near the backtest end date and then trying to use May-only data for April entries.

## 4.5.13 - 2026-05-13

Deploy marker: 4.5.13 release commit (`deploy 4.5.13`)

### Fixed
- **IBKR REST backtesting point-in-time option price lookups now fetch bounded minute slices around the simulation time.** `get_last_price()` and `get_quote()` no longer prefetch the full backtest window for point lookups, which avoids IBKR returning only a later tail segment and then mispricing earlier AlphaPicks option entries.

## 4.5.12 - 2026-05-13

Deploy marker: 4.5.12 release commit (`deploy 4.5.12`)

### Fixed
- **IBKR REST backtesting no longer treats underfilled minute history as full-window coverage.** If a full backtest-window prefetch returns only a later tail slice, `get_last_price()` / `get_quote()` now leave the series unmarked as fully loaded, fetch a bounded slice around the current simulation datetime, and retry. This fixes BotSpot option backtests where an Apr-09 Alpha Picks lookup was incorrectly attempted against a May-only minute cache and logged “outside data range.” Disjoint recovery slices are normalized before merging so timezone object differences cannot leave the stale slice in place.
- **ThetaData option quote caches now recover from stale provider-expiry placeholders.** Placeholder-only option quote caches now record the Theta provider expiration used for Friday/Saturday expiry mapping, and LumiBot refetches once when a legacy placeholder was written before the correct provider mapping was learned. This prevents a no-data placeholder from blocking later valid option quotes for symbols whose Theta history endpoint expects the exact Friday expiry.

## 4.5.11 - 2026-05-12

Deploy marker: 4.5.11 release commit (`deploy 4.5.11`)

### Added
- **Scheduled live execution can run one iteration and exit for BotManager scheduled runs.** When BotManager sets `LUMIBOT_SCHEDULED_EXECUTION`, LumiBot restores persisted `self.vars`, runs one live iteration, persists the updated state file, and exits cleanly.
- **AI agent strategies now support richer multi-agent workflows.** Added per-agent model selection, `allow_trading=False` tool restrictions for research-only agents, and an AI investment committee example that runs researcher, bull-case, bear-case, and portfolio-manager agents inside the normal `on_trading_iteration()` flow.
- **Built-in SEC fundamentals and filing tools for agents and strategies.** Added direct SEC EDGAR access with local caching, rate limiting, SEC user-agent support, company facts, normalized income statement / balance sheet / cash-flow helpers, filing lists, filing document retrieval, and keyword search over cached filings. Backtests gate filings by filed / accepted timestamps so agents do not see future disclosures.
- **Built-in FRED macro tools.** Added official FRED/ALFRED API helpers for series lookup, latest values, historical observations, and point-in-time snapshots using `realtime_start` / `realtime_end`. FRED data fetches require `FRED_API_KEY` and cache under the Lumibot cache directory.
- **Built-in technical-indicator tools for agents.** Agents can list indicators and request current-bar indicator values through the existing fast indicator subsystem, keeping backtests aligned with the simulated datetime.
- **Local AI-agent memory and notifications.** Added JSONL-backed memory tools for decisions, lessons, and thesis lifecycle tracking, plus a Telegram notification provider and `notify_user` tool with backtest notifications disabled unless explicitly enabled.
- **AI-agent smoke and verification tooling.** Added deterministic and real-model AI committee runners, built-in-tool integration coverage, SEC/FRED tests, notification/memory tests, and agent-tool permission tests.
- **New AI-agent documentation and branded visuals.** Added RST and Markdown docs for agent flows, built-in tools, SEC fundamentals, FRED macro data, memory, notifications, observability, and the investment committee example, plus Nano Banana / Spot-branded README and documentation diagrams.
- **Individual CCXT broker documentation pages.** Added separate docs pages for the CCXT brokers Lumibot supports directly, including Binance, BitMEX, Bybit, Coinbase, Kraken, KuCoin, OKX, and WEEX.

### Changed
- **README and public docs now position Lumibot as deterministic Python strategies plus AI-agent strategies on the same backtest / paper / live execution path.** The AI investment committee is presented as one example workflow, not the only possible agent structure.
- **Agent built-in tools are included by default when available.** Account, positions, open orders, history, docs search, indicators, SEC, FRED, memory, notifications, and order tools are available by default; `allow_trading=False` removes only mutating order tools.
- **Alpaca news tool availability now depends on credentials.** The built-in Alpaca news tool is hidden when no Alpaca credentials are configured and uses the standard Alpaca credential environment variables when present.
- **FRED no longer uses public CSV fallbacks.** Revised/no-key CSV access was removed from examples, docs, tests, and implementation; official FRED/ALFRED API access is the only supported macro-data fetch path.
- **AI-agent docs and deployment guidance now require high-quality generated visuals.** Lumibot/BotSpot/Lumiwealth documentation visuals must use Nano Banana/GPT Image 2 quality and the canonical Spot brand reference when a mascot is helpful.

### Fixed
- **BotSpot cloud account snapshots are marked verified before use.** This prevents unverified broker/account reads from being treated as trusted performance data.
- **AI committee example trades are constrained to reviewed symbols and risk limits.** The example no longer places trades outside the researcher/portfolio-manager scope.
- **Filing, macro, indicator, and news examples now document point-in-time behavior more explicitly.** Docs and prompts emphasize using the current strategy datetime during backtests.

## 4.5.10 - 2026-04-30

Deploy marker: 9e73736f (`deploy 4.5.10`)

### Fixed
- **Bitunix futures order cancellation now sends the exchange-required symbol with the order id.** `Bitunix.cancel_order()` now passes both `order_id` and `symbol` into the Bitunix REST client, and `BitUnixClient.cancel_order()` accepts either the broker's keyword shape or a Lumibot order object. This fixes cancellation failures caused by the client method expecting an order object while the broker passed only an identifier.

## 4.5.9 - 2026-04-30

Deploy marker: b0da6a50 (`deploy 4.5.9`)

### Fixed
- **IBKR continuous futures now tolerate IBKR-listed holiday expiration dates for the same contract month.** MNQ June 2026 (`MNQM26`) was computed from the synthetic equity-index anchor as `2026-06-19`, but IBKR lists the contract with `expirationDate/ltd=20260618`; the exact-date miss was then persisted in the negative conid cache and BotSpot backtests saw no MNQ bars. LumiBot now keeps the strict no-front-month-fallback invariant, but when IBKR returns exactly one contract in the requested `YYYYMM`, it resolves that conid, records both the listed date and computed-date alias, clears stale target negative-cache entries, and uses the IBKR-listed date for cont-future history fetches.

## 4.5.8 - 2026-04-29

Deploy marker: eefe772f (`deploy 4.5.8`)

### Fixed
- **Broker cash-event polling is hardened for real Alpaca and Tradier activity-history responses.** Alpaca cash-event polling now requests non-fill account-activity categories separately (`TRANS`, `DIV`, `MISC`) instead of paging through trade fills or letting dividend pages hide funding rows, safely handles wrapped/model/invalid activity rows, and was verified against real Alpaca live API-key/OAuth accounts with actual `CSD` deposits and `CSW` withdrawals. Tradier cash-event polling now parses the raw history response inside LumiBot so the published `lumiwealth_tradier` package's `"history": "null"` bug cannot emit live warnings like `string indices must be integers, not 'str'`; direct-request pagination, wrapper fallback, null-history payloads, the exact TypeError path, and real Tradier live transfer/wire/dividend history were verified.
- **Cash-event warnings now identify the broker and sanitized fetch context.** Strategy warnings now include the broker name/class, and Alpaca/Tradier broker adapters log sanitized activity type, page/limit, pagination, and raw-type counts so future production warnings can be diagnosed without exposing API keys.

## 4.5.7 - 2026-04-29

Deploy marker: c850cd66 (`deploy 4.5.7`)

### Fixed
- **AI agents no longer send unsupported `temperature=0` to LiteLLM-routed providers.** The ADK runtime had hardcoded `temperature=0.0` for every model. OpenAI GPT-5/reasoning-class models reject custom temperature values and only allow the provider default, causing backtests to fail before the first agent call. LumiBot now keeps explicit `temperature=0.0` only on the Gemini-native ADK path and omits temperature for OpenAI/xAI/Anthropic/etc. LiteLLM providers. Verified with a real `openai/gpt-5.4` agent call.
- **Tradier live cash-event polling no longer passes unsupported `page=` to `Account.get_history()`.** The installed `lumiwealth-tradier` client supports `start_date`, `end_date`, `limit`, `activity_type`, and `symbol`, but not `page`. LumiBot now detects whether the client supports pagination and falls back to a single bounded per-activity request when it does not, preventing live warnings like `Account.get_history() got an unexpected keyword argument 'page'` and allowing broker cash events to load.

## 4.5.6 - 2026-04-29

Deploy marker: 61b75f24 (`deploy 4.5.6`)

### Added
- **Canonical AI agent detail artifact is now Parquet-only and token-first.** AI agent backtests now write one canonical `*_agent_detail.parquet` artifact beside the normal backtest outputs. The file contains one `call_summary` row per AI call plus event rows for `thinking`, `text`, `tool_call`, `tool_result`, and `usage`, with exact prompt/context fields, model output text, tool args/results, event payload JSON, input/output/total tokens, thinking tokens, cached/uncached input tokens, cache-write tokens, tool-use prompt tokens, full-call latency, and first-event latency. Pricing is intentionally excluded; the artifact records raw token facts only.
- **Built-in BYOK Alpaca news tool for AI agents.** Added `BuiltinTools.news.alpaca_news()`, which uses the user's own `ALPACA_API_KEY`/`ALPACA_API_SECRET` or `APCA_API_KEY_ID`/`APCA_API_SECRET_KEY`, defaults the news `end` timestamp to the current simulated datetime during backtests, and returns normalized Alpaca news headlines/summaries/sources/URLs/symbols. Full content is opt-in with `include_content=True`.
- **Alpaca news proof script, example strategy, and live quality test.** Added `scripts/run_alpaca_news_ai_proof.py`, which runs a tiny real AI-agent backtest, forces the scan-first/full-article workflow, and verifies the resulting `*_agent_detail.parquet` contains `alpaca_news` tool calls, full article content, thinking rows when exposed by the model, and token/latency fields. Added `lumibot/example_strategies/agent_alpaca_news_builtin.py` as the recommended built-in-tool news example. Added opt-in `apitest` coverage that checks Alpaca/Benzinga historical relevance and real pagination on known broad-market event windows.
- **Real provider prompt-cache probe.** Added `scripts/run_agent_prompt_cache_probe.py`, which bypasses LumiBot's replay cache and sends repeated real provider calls with the same long static prefix. The probe prints input, cached input, uncached input, output, thinking, total token counts, first-event latency, and full-call latency so provider prompt caching can be verified before deployment.

### Changed
- **Alpaca news tool now supports scan-first / full-read workflow without hidden truncation.** `BuiltinTools.news.alpaca_news()` now defaults `limit=30`, supports `page_token`, returns `content_available_count` and `summary_available_count`, exposes `requested_end` / `effective_end` / `lookahead_clamped`, clamps future backtest `end` timestamps to the current simulated datetime, and returns the full article body when `include_content=True` unless `content_max_chars` is explicitly set. The tool description now teaches agents that Alpaca news is symbol/date-window retrieval rather than keyword search, recommends focused scans at `limit=10-20`, broad scans at `limit=30-50`, explains pagination, and lists broad-market/sector ETF proxies for less sparse historical news queries.
- **AI agent prompt layout is more provider-cache friendly.** Runtime-specific fields such as current datetime, timezone, positions, orders, task prompt, user context, and persistent memory now stay in dynamic user-context sections instead of the static system instruction prefix. This keeps the large base prompt and tool surface stable across bars so OpenAI/xAI/Gemini provider prompt caches can reuse more of the prefix during cold backtests and live trading.

### Fixed
- **AI agent prompt caching telemetry and routing keys.** Provider cache routing is now stable for OpenAI (`prompt_cache_key` + 24h retention) and xAI (`x-grok-conv-id`), and LumiBot normalizes cached-token fields from provider usage payloads into `cached_input_tokens` / `uncached_input_tokens` so backtests can prove whether provider prompt caching is actually being used.

## 4.5.5 - 2026-04-24

### Added
- **Anthropic Claude M2 Liquidity demo.** Added `lumibot/example_strategies/agent_m2_liquidity_anthropic.py`, matching the Gemini/OpenAI/Grok variants with `default_model="anthropic/claude-sonnet-4-6"` and `ANTHROPIC_API_KEY` setup instructions.

### Fixed
- **AI agent provider-key naming now matches product-facing env vars.** Gemini examples/docs/error guidance now use `GEMINI_API_KEY`, and Grok examples accept either `XAI_API_KEY` or `GROK_API_KEY`. LumiBot mirrors `GROK_API_KEY` into `XAI_API_KEY` for LiteLLM's xAI provider when the canonical xAI env var is absent. Added regression coverage in `tests/test_agent_runtime_provider_keys.py`.

## 4.5.4 - 2026-04-24

### Added
- **Multi-provider LLM support for AI agents via LiteLLM bridge.** Agents can now use any LLM provider LiteLLM supports (OpenAI GPT, xAI Grok, Anthropic Claude, Groq, Mistral, Cohere, and ~100 others) by passing a provider-prefixed `default_model` to `self.agents.create(...)`. Gemini ids still take Google ADK's native fast path; anything else is transparently wrapped via `google.adk.models.lite_llm.LiteLlm`. New `_resolve_model_for_adk()` helper in `lumibot/components/agents/runtime.py` does the routing. Added `litellm>=1.77.0` to `setup.py` install_requires. Replay cache already keyed on the model id (`manager.py:516`), so swapping providers on the same backtest produces fresh runs rather than stale cross-model replays. Three provider-specific M2 demo strategies mirror the existing Gemini demo with only the `default_model` + provider key changed: `lumibot/example_strategies/agent_m2_liquidity_openai.py` (openai/gpt-5.4-mini), `agent_m2_liquidity_grok.py` (xai/grok-4.20-0309-reasoning = Grok 4.2), and `agent_m2_liquidity_anthropic.py` (anthropic/claude-sonnet-4-6). Public docs updated in `docsrc/agents.rst`, `docsrc/agents_quickstart.rst`, and `docsrc/environment_variables.rst` to document supported providers and required env vars (`GEMINI_API_KEY` / `OPENAI_API_KEY` / `XAI_API_KEY` or `GROK_API_KEY` / `ANTHROPIC_API_KEY`). New unit tests `TestResolveModelForAdk` in `tests/test_agent_runtime_cache.py` cover Gemini passthrough (6 ids incl. case and whitespace variants), LiteLlm wrapping (OpenAI, xAI, Anthropic, Groq, Mistral prefixes), and non-string passthrough.
- **Automatic retry on transient provider errors for AI agents.** `GoogleADKRuntime.run()` now wraps the ADK runner in a 3-attempt retry loop with exponential backoff (2s/4s/8s). Catches transient failures (network blips, 429 rate limits, 500/503/529 server errors, brief timeouts) and retries silently; real configuration errors (auth failures, invalid model ids, context-length-exceeded) still fail fast via a non-retryable error classifier. For the LiteLLM path, `litellm.num_retries = 3` is also set so the LiteLLM layer gets its own provider-aware retry (HTTP-level, 429 Retry-After aware) inside each attempt. Production agents now survive normal cloud-provider hiccups without strategy-level error handling. LiteLLM also configured with `drop_params = True` (silently drops provider-unsupported params instead of raising; needed for xAI rejecting `max_completion_tokens`) and `suppress_debug_info = True` (mutes cosmetic "Provider List" banner for current-generation models not yet in LiteLLM's static cost registry like `gpt-5.4-*` and `grok-4.20-*`).
- **Discretionary Trader demo + multi-provider parallel runner.** New `lumibot/example_strategies/agent_discretionary.py` is the first max-discretion AI agent demo: one-sentence user prompt (`"Make as much money as you possibly can."`), no asset whitelist, shorts allowed, reads `AGENT_MODEL` env var so the same file runs against any supported provider (Gemini, OpenAI, xAI, Anthropic). Tools: FRED macro + Alpaca news (graceful-degrade if keys unset) + yfinance fundamentals snapshot (P/E, forward P/E, market cap, profit margins, earnings dates, analyst targets, short interest, 52W range, sector, industry) + all built-ins. Companion script `scripts/run_discretionary_3way.py` launches three providers in parallel with a memory watchdog (aborts all if combined RSS > 10 GB or any single process > 5 GB), per-provider log capture, and a summary JSON of tearsheet paths + metrics for post-run comparison. Docs updated in `docsrc/agents_canonical_demos.rst` with the full "5 demos" list (v1 discretionary + 4 existing constrained allocators).
- **AI agent error classifier + live-vs-backtest error-handling semantics (AI-agent-scoped only; zero changes to main Lumibot error handling).** New `_classify_agent_error` helper in `lumibot/components/agents/runtime.py` buckets every exception from an agent call into `auth` / `config` / `billing` / `transient` / `unknown`. Classification uses class name match (litellm/openai/anthropic/google-genai), HTTP status code when the provider SDK attaches one, and message-substring keywords (`insufficient_quota`, `credits`, `billing`, `payment`, `no credits`, `team doesn't have any credits`) so e.g. xAI's 403 "your team doesn't have any credits yet" is correctly bucketed as `billing` not `auth`. `GoogleADKRuntime.run()` now only retries `transient` + `unknown`; auth/config/billing errors propagate immediately instead of burning the 10-attempt 5-minute retry budget on errors that cannot self-heal. `AgentHandle.run()` wraps the runtime call with a classifier-aware safety net: in backtest mode, `auth`/`config`/`billing` re-raise loudly with a clean error banner that points at the env-var to fix and the provider billing URL (so users fix and re-run instead of getting a silent +0% tearsheet); in live mode, all categories log + skip the iteration so the bot never stops. Transient and unknown errors always skip gracefully in both modes. This is fully scoped inside `lumibot/components/agents/*` — the main `strategy_executor.py`, `_strategy.py`, broker/trader files, and `_on_bot_crash`/`gracefully_exit` behavior are unchanged. 22 new regression tests in `tests/test_agent_runtime_cache.py` (`TestClassifyAgentError`, `TestAgentHandleRunClassifiedErrorBehavior`) cover classifier buckets, backtest crash semantics, and live-never-crash invariants. Retry schedule retuned from 3 attempts × 14s budget (old) to 10 attempts × ~5-min budget with no single backoff exceeding 60s (`2, 3, 5, 10, 20, 30, 45, 60, 60, 60`). Rich error handling documentation added to `docsrc/agents.rst` in a new "Error Handling and Reliability" section.
- **AI agent model auto-populates into `strategy.parameters`.** Every call to `self.agents.create(name=..., default_model=...)` now auto-sets `strategy.parameters[f"agent_{name}_model"]` to the resolved model id, which surfaces in the tearsheet's "Parameters Used" panel. Multi-agent strategies get one key per agent. Zero per-strategy code changes required — works for any existing or future agent strategy. Three regression tests in `TestAgentManagerParametersAutoPopulate`.
- **AI token usage summary + detailed audit artifacts for backtests.** Every AI agent run now updates cumulative token totals directly in `strategy.parameters`, so the tearsheet's "Parameters Used" panel shows `agent_<name>_calls`, `agent_<name>_input_tokens`, `agent_<name>_output_tokens`, `agent_<name>_total_tokens`, `agent_<name>_thinking_tokens`, `agent_<name>_tool_calls`, `agent_<name>_cache_hits`, `agent_<name>_detail_csv`, and `agent_<name>_detail_parquet` automatically. In addition, LumiBot writes `*_agent_detail.csv` and `*_agent_detail.parquet` beside the normal backtest artifacts, following the existing CSV-compatible / Parquet-fast contract used by trades, stats, trade_events, and indicators. The detail artifact contains one row per model event (`thinking`, `text`, `tool_call`, `tool_result`, `usage`) with the effective prompt, task prompt, user context, runtime context, event text, flattened tool details, warnings, trace path, and token counts for the call. This is intentionally token-only: no pricing logic, no stale model-cost tables. Gemini requests thought summaries when the SDK supports them; other providers still record thinking token counts even if they do not expose thought text. Regression coverage added in `tests/test_agent_runtime_cache.py`.

## 4.5.3 - 2026-04-21

### Changed
- **Agent prompt now instructs strategies on risk and drawdown discipline.** New "RISK AND DRAWDOWN DISCIPLINE" block in `AgentHandle`'s base prompt (`lumibot/components/agents/manager.py`) tells generated strategies to optimize for risk-adjusted return, not headline return; explains recovery math (20% DD needs 25% gain, 50% needs 100%, 80% needs 400%); sizes positions by conviction + volatility, not just cash; cuts broken theses instead of averaging down; does not chase losses with more size; and thinks in Sharpe/Sortino/Calmar terms. Zero code-path changes; purely a behavioral shift in what agent-generated strategies do.

## 4.5.2 - 2026-04-21

### Added
- **WEEX broker support via CCXT.** New `WEEX_CONFIG` in `lumibot/credentials.py` wires WEEX (CCXT exchange id `weex`) into the broker auto-detect chain, the explicit-name branch (`TRADING_BROKER=weex`), and the data-source aliases (`DATA_SOURCE=weex`). WEEX requires three credentials (`WEEX_API_KEY`, `WEEX_API_SECRET`, `WEEX_API_PASSPHRASE`) and has no API sandbox, so `sandbox` is hard-wired to `False`. Added `"weex"` to the exchange-id whitelists in `lumibot/brokers/ccxt.py` (balance parsing at line 104 and open-orders routing at line 224) so the generic Ccxt broker accepts WEEX without raising `NotImplementedError`. Initial scope is **spot trading only**; WEEX's primary business is USDT-margined perpetual swap, but swap support is not wired into the shared Ccxt broker today. Note that WEEX's Terms of Use exclude US and Canadian residents, documented in `docsrc/brokers.ccxt.rst`. New unit test `test_initialize_weex_broker` in `tests/test_ccxt.py`.

### Changed
- **Docs: Coinbase section now documents CDP (Cloud Developer Platform) private-key auth.** `COINBASE_CONFIG` already supported PEM private keys via the `secret` field (CCXT auto-detects PEM vs HMAC), but the docs still described the legacy HMAC + passphrase flow. Updated `docsrc/brokers.ccxt.rst` to explain that `COINBASE_API_KEY_NAME` holds the org-qualified identifier from the CDP JSON and `COINBASE_PRIVATE_KEY` holds the full PEM block (multi-line, `-----BEGIN EC PRIVATE KEY-----` ... `-----END EC PRIVATE KEY-----`). `COINBASE_API_PASSPHRASE` is now documented as legacy-only.

### Fixed
- **`Data.iter_count()` with a microsecond-precision index returned incorrect cursor positions.** `_index_values_ns` assumed the pandas `DatetimeIndex` was nanosecond-precision, but pandas 2.x can build microsecond-precision indexes from ISO strings. Normalize to nanoseconds explicitly so the `np.searchsorted(index_ns, now_ns)` cursor math works regardless of pandas' chosen unit. Regression test in `tests/test_data_iter_count_microsecond_index.py`.

## 4.5.1 - 2026-04-20

### Fixed
- **`Strategy.get_last_price()` returned `None` at sim_time=00:00 on 24/7 markets, breaking strategy rebalance logic.** When a strategy's iteration fires at midnight (e.g. `MARKET="24/7"` plus daily sleeptime), the daily-shortcut's `length=1, timeshift=0` slice returns empty because no day bar exists exactly at sim_time=00:00. Without a retry the shortcut falls through to `broker.get_last_price`, which on the IBKR REST backtest path also fails at non-market-hours timestamps. Net result: strategies see `None` on every iteration and the rebalance path's `if last_price is None: continue` skip means no orders are ever placed. Reported by Rob on the 2026-04-19 local IBKR backtest: Alpha Picks ran for 31 simulated days at `Val: $100,000` with zero trades. Fix: when the length=1 slice comes back empty, retry `get_historical_prices(length=5, timestep="day")` and return the close of the last row with `index <= sim_time`. No guard, no rejection of other slices — this ONLY triggers when the length=1 result is honestly empty. Matches Rob's stated semantics: "if we have no bar at sim_time, the price hasn't changed since the last known bar." New regression test `test_forward_fill_returns_last_prior_bar_when_length1_empty` in `tests/test_get_last_price_sim_time_safety.py` covers the 24/7 midnight case.
- **Tearsheet cumulative-returns plot started the Strategy line below the Benchmark line at the left edge.** QuantStats computes cumulative returns as `(1 + returns).cumprod() - 1`, so the first plotted point equals the first day's return. When the strategy bought at open and closed with a small drawdown on day 1, the Strategy line began at e.g. `-4%` while the Benchmark's first `pct_change` was NaN→0 and started at `0%`. Rob reported this as a recurring visual artifact on Alpha Picks tearsheets (multiple sightings over months). Two-part fix: (1) `lumibot/tools/indicators.py::_prepare_tearsheet_returns` now prepends a day-0 anchor row (first_index − 1 day) with both returns = 0 whenever the first real row has a non-zero return; (2) pass `match_dates=False` to `qs.reports.html` at both call sites in `create_tearsheet`. Without the second part, QuantStats' `_match_dates` explicitly drops leading zero-return rows (`loc = max(returns.ne(0).idxmax(), benchmark.ne(0).idxmax())`, truncates both series to `loc:`) and deletes our anchor before it reaches the plot. With both parts, the Strategy and Benchmark lines share a common 0% starting point on the left edge. Regression test `test_tearsheet_prepends_anchor_when_first_day_has_non_zero_return` in `tests/test_tearsheet.py` pins the anchor prepend; the `match_dates=False` change is validated by visual tearsheet inspection.
- **IBKR helper: `get_price_data()` crashed with `'str' object has no attribute 'symbol'` when callers passed a bare symbol string instead of an `Asset`.** Observed in the 2026-04-19 local Alpha Picks IBKR backtest: upstream path leaked a string through, producing misleading log spam `"IBKR history fetch failed for None/USD timestep=minute ..."` that obscured the real bug during investigation. Fix: defensive coercion at the top of `lumibot/tools/ibkr_helper.py::get_price_data` — if `asset` is a string, wrap it in `Asset(symbol=asset)` before proceeding. One-line safety net at a system boundary; the log message becomes accurate (`IBKR history fetch failed for COP/USD ...`) and the rest of the pipeline works.

## 4.5.0 - 2026-04-19

> Note: the in-flight `4.4.63` work was never published to PyPI (no `v4.4.63`
> tag). Those entries are consolidated into this `4.5.0` release alongside
> the `Strategy.indicators` subsystem.

### Added
- **`Strategy.backtest()` now honours `BACKTESTING_START` / `BACKTESTING_END` env vars as overrides for hardcoded `backtesting_start`/`backtesting_end` args.** Strategies typically pin dates in their `if __name__ == "__main__":` block for local dev convenience. When the same code runs in a managed container (bot_manager backtest ECS task, BotSpot MCP `start_backtest`, etc.) the orchestrator sets these env vars from the caller's requested date range — previously those env vars did nothing, the hardcoded datetimes won, and MCP callers had no runtime control over the window. New precedence: env var (if present and parseable) > passed arg. Accepts ISO date (`YYYY-MM-DD`) or full ISO datetime; malformed values are logged and the passed arg is preserved (no silent fallback to the epoch or `now`). Unit tests in `tests/test_strategy_backtest_env_override.py` cover both shapes, empty-string handling, parse failures, and the no-env-set baseline.
- **`Strategy.indicators` subsystem.** New per-strategy indicator accessor that computes any indicator over the full bar series once, then hands back the value at the current bar in O(1) on every subsequent call. Eliminates the per-iteration `df.rolling(...).apply(...)` full-history recompute pattern that dominated wall-time on indicator-heavy strategies. Exposes the entire pandas-ta-classic surface (~130 indicators) via `self.indicators.<indicator>(asset, **kwargs)` passthrough plus `self.indicators.custom(name, fn, asset, **kwargs)` for user-defined indicators. Same API for backtest and live. See `docsrc/indicators.rst` for the full reference and migration guide.
- `LUMIBOT_CACHE_MISS_DEBUG` env var gates opt-in `[CACHE_MISS]` / `[FETCH]` warning traces in `ibkr_helper.py` for auditing why the cache layer decided to hit the network. Documented in `docs/ENV_VARS.md` and `docsrc/environment_variables.rst`.

### Changed
- **`Strategy.indicators`: per-bar lookup now O(log N) via `searchsorted`.** `_latest_scalar`/`_latest_row` no longer allocate a `.loc[:now]` slice on every call; they compute the integer position of the most-recent bar ≤ now with `Index.searchsorted(now, side="right") - 1` and `iloc` directly. Collapses the per-iteration cost of a memoized indicator from a full label-compare slice to a binary search + one integer index. Observed impact on `tqqq_median` benchmark (13-year daily): 54.71s → 5.59s end-to-end (**~9.8x faster per-iteration lookup** — the memoized compute was already one-shot; this fixes the hot-path indexing cost).
- **`Strategy.indicators`: cache keyed on full-history fingerprint.** `_dispatch` now tags each memoized result with `(len(df), df.index[-1])` and recomputes transparently when the underlying bar series grows (routed/live prefetch). Prevents stale indicator output when the data source extends the frame after the first call.
- **`Strategy.indicators`: prefetch-aware full-history fallback.** `_full_history` now tries `_data_store` first, then — if empty — calls `get_historical_prices(length=self._fallback_length)` purely to trigger the routed/live adapter's prefetch path, and re-reads the now-populated store. This makes the indicator subsystem work correctly on routed/IBKR backtests where `_data_store` is populated lazily on first access. `_fallback_length` reduced from 100_000 to 10_000 to avoid pandas ns-timestamp overflow (100k daily bars ≈ year 1573, below the pandas epoch).
- `lumibot/backtesting/routed_backtesting.py`: narrow short-circuit in `_IbkrRoutingAdapter.update_pandas_data` skips IBKR's 1-minute prefetch for `cont_future`/`future` quote-lookup calls when coarser non-day cadence (15-minute, 30-minute, hour, …) is already loaded. Affects `get_quote(asset)` default-timestep lookups during backtests. Observed impact on `mes_ema_15m` benchmark: 26.6s → 7.03s (**3.78x**) on warm cache; results byte-identical.
- `lumibot/data_sources/pandas_data.py`: module-level `_USD_FOREX = Asset("USD", "forex")` singleton reused by `find_asset_in_data_store()` to avoid constructing a fresh default-quote `Asset` on every call (~200K calls/backtest). Missing-asset warnings now gate to first-encounter per `(asset, timestep)` to avoid flooding 1-minute backtest logs with redundant warnings.

### Fixed
- **CRITICAL: `Strategy.get_last_price()` daily-cadence shortcut had a look-ahead read that returned future bars on IBKR / ThetaData / routed backtests.** The shortcut at `lumibot/strategies/_strategy.py::get_last_price` called `self.get_historical_prices(asset, length=2, timestep="day", timeshift=-1, ...)` and then took `float(bars.df["close"].iloc[-1])`. `Data.get_bars(length=2, timestep="day", timeshift=-1)` computes `end_row = iter_count + 2`, `start_row = iter_count` and returns rows `[iter_count, iter_count + 1]` — i.e. the bar AT sim-time AND the next bar after it — so `iloc[-1]` picked up the future bar. In a backtest the full history exists ahead of the sim clock, so when the underlying frame also contained bars past the backtest window (e.g. a shared S3 cache row stamped at wall-clock "today") the shortcut returned today's close for a 2022 sim-time. Observed incident 2026-04-17 on BotSpot Stock Alpha Picks (backtest `f2d9ca86`, window 2022-07-01 → 2022-08-01): `get_last_price(COP)` returned $97.43 (real-now close) instead of the 2022-07-01 close of $90.98, polluting position sizing across every symbol. Fill prices were unaffected (historical-bars fill path is separately sim-time-safe); only the `get_last_price` shortcut leaked. Fix: change the shortcut to `length=1` with no timeshift so `Data.get_bars` returns exactly the single bar at `iter_count` — guaranteed to be at or before sim_time regardless of what the frame contains past it. New regression coverage in `tests/test_get_last_price_sim_time_safety.py` (7 cases): polluted-frame reproduction replaying the exact Alpha Picks COP/NUE symptom, dense-frame sanity, sim-time-before-any-bar fall-through, and a whitebox guard on the `length=1, timeshift in (None, 0)` call signature so a future edit can't silently restore `length=2, timeshift=-1`. The existing `test_strategy_methods.py::test_get_last_price_prefers_day_bars_for_routed_backtesting_daily_cadence` test — which had been pinning the buggy `iloc[-1]` return of a two-row frame as "correct" — is rewritten to assert the correct sim-time-safe behaviour. Operationally, the shared IBKR daily stock/index S3 cache was wiped on dev + prod (stock/index day + minute; futures/crypto/option preserved per the `ibkr/future/` rule) to clear any lingering wall-clock-stamped rows that may have been written by earlier buggy code paths.
- **CRITICAL: IBKR stock/index daily pagination cap was too tight, producing flat-price backtests over multi-year windows.** `IBKR_STOCK_INDEX_DAILY_MAX_PERIOD` was hardcoded to `"180d"`, which made `_fetch_history_between_dates` request only ~125 bars per call against an endpoint that caps responses at 1000. In practice the backward-walking pagination loop completed in a single iteration: the initial fetch near real-now filled the cache with 180 days of recent data, and the coverage check for every subsequent simulation date declared that "real bars exist" and stopped re-fetching. The strategy then received today's prices for every historical simulation date, producing flat-VIX / flat-SPY / flat-RUT series across the entire backtest window. Observed incident: Peter Credit Spreads backtest `a83663bd-...` (2018-01-01 → 2026-04-16) had `VIX` constant at `14.2` for all 2,010 simulated days (`stddev=0`) and max drawdown of `-94%`. Production verification against IBKR Client Portal REST on 2026-04-17: `period=5y` returns the full 1000-bar cap (~4 trading years of daily data), `period=2y` returns 323 bars, `period=180d` returned only ~125 bars. Fix: raise `IBKR_STOCK_INDEX_DAILY_MAX_PERIOD` to `"5y"` so each call gets the maximum usable response and the backward-pagination loop covers 8-year windows in ~2 iterations per symbol/source. Regression tests in `tests/test_ibkr_helper_unit.py` lock the new value in place. End-to-end verified locally against Peter Credit Spreads: VIX now has 497 distinct values (9.15–82.69) over 2018-01 → 2020-06 with the March 2020 COVID spike to 82.69 captured correctly; strategy max drawdown collapses from `-94%` (broken) to `-5%` (working, regime filter actually firing).
- **Backtesting: persist placeholder markers for IBKR terminal pagination-empty responses.** `_fetch_history_between_dates` raises `"IBKR history pagination returned empty data before covering the requested window"` when IBKR serves partial-then-empty responses (common for entitlement/stitching gaps like CONT_FUTURE 1-minute Trades). The error was caught but `_is_terminal_no_data_error` did not match the message, so no placeholder marker was written and every backtest refetched the same ~23s empty-result call. Fix: extend `_is_terminal_no_data_error` in `lumibot/tools/ibkr_helper.py` to match the pagination-empty tokens, so `_record_missing_window` persists a `missing=True` marker and `_window_is_placeholder_covered` short-circuits on subsequent runs. Observed impact on `vband_mnq_mes_1m` benchmark: 55.8s → 2.94s (**18.98x**) on warm cache; all results byte-identical.
- **CRITICAL: IBKR backward pagination discarded all earlier chunks on a single empty mid-walk page, breaking CME futures backtests.** `_fetch_history_between_dates` walks backwards from `end_dt` in `period`-sized windows (default `1000min` for 1-min bars). CME futures markets have a weekly weekend close (Fri 4pm CT → Sun 5pm CT) and a nightly maintenance break (4–5pm CT); when a `1000min` window lands entirely in a no-trading range, IBKR correctly returns `{"data": []}` even though the surrounding bars are valid. A previous fix (`3c5cbb5c`, 2026-03-05 "ibkr: keep paged history chunks when later page is empty") changed the empty-page handler to `if chunks: break` so earlier chunks survived, but the WIP checkpoint `af8df88b` (2026-03-30, pre-4.4.57 branch reconcile) silently reverted it back to `raise RuntimeError("IBKR history pagination returned empty data before covering the requested window ...")`. The regression was then released on `version/4.4.57`+ and carried into `version/4.5.0`. Impact: every `get_historical_prices()` call for CME futures (MES/MNQ/ES/NQ/…) that needed to walk past a weekend would raise, and the resulting empty-frame return meant the strategy saw no bars for every subsequent iteration. Observed in `mes_ema_15m` benchmark on `version/4.5.0`: 209 pagination-empty errors in a single 6-month backtest, strategy unable to trade after bar 1, backtest runs to 40-min wall-clock cap without completing. Fix: restore the `if chunks: break` behaviour in both the `if not data:` and `if df.empty:` branches of the backward-pagination loop, exactly matching `3c5cbb5c`. New regression test `test_ibkr_fetch_history_keeps_chunks_when_mid_paging_page_is_empty` in `tests/test_ibkr_crypto_daily_series.py` forces three mocked history calls where call 3 returns empty, and asserts that the two prior chunks are returned (verified to raise `RuntimeError` without the fix, pass with it).

## 4.4.62 - 2026-04-15

### Fixed
- **CRITICAL: Flat-price fabrication bug in backtesting fills.** `BacktestingBroker._pandas_fill_path` previously picked `df_original.iloc[-1:]` when the "bars at or after self.datetime" filter came back empty, OR `df_original.iloc[0]` when the filter caught rows far in the future. Both paths could silently price every fill at a single bar's OPEN that had nothing to do with the simulation time, producing deterministic constant prices across multi-year backtests. Observed incident: BotSpot "Alpha Picks" backtest where 72 of 84 stocks froze at the OPEN of 2026-04-10 for every fill across 2022-2024. The fix rejects future bars outside a narrow window (2 days for daily, 2 hours for hourly, 5 minutes for minute), prefers the most recent bar at or before the simulation time, and refuses to fabricate a fill when no bar exists within a reasonable window of `self.datetime`. A second-layer `max_fill_distance` sanity check (7 days for daily, 1 day for intraday) hard-rejects any fill price derived from a bar too far from the simulation clock, cancelling the offending iteration rather than producing a garbage fill. Provider-agnostic; applies to every data source that flows through `PANDAS` / `InteractiveBrokersREST` fill paths (ThetaData, IBKR, Polygon, etc.).
- IBKR history loading now fails open by returning available real bars instead of synthesizing an empty dataset when the cache refresh leaves the requested window underfilled (commit `5de1362a`, April 14).
- Interactive Brokers REST backtesting data source now prefers an already-loaded daily stock/index series for `get_last_price`/`get_quote` before triggering a separate intraday minute fetch, which avoids unnecessary VIX/USD midpoint history requests during daily-cadence backtests (commit `5de1362a`, April 14).
- `Order.to_dict()` now emits `identifier` and `avg_fill_price` consistently so downstream consumers that rely on these fields on serialized orders no longer see missing keys after backtest-time fills (commit `e3cb48fe`, April 15).
- Release the IBKR downloader fail-closed hotfix from the corrected commit so CI and PyPI ship the same behavior validated locally (merged from dev).

### Changed
- README restructured for visitor conversion: Quick Start code block, competitor comparison table (LumiBot is the only open-source Python trading library with options support), supported brokers & data sources matrix, migration guide from Backtrader (merged from dev).
- PyPI metadata overhaul: added 20 search keywords and 12 classifiers (Financial, Investment, AI, Python 3.10-3.12) after being previously empty. Updated package description for search discoverability (merged from dev).
- Author email updated to rob@botspot.trade; added project_urls for documentation, Discord, and BotSpot platform (merged from dev).

### Added
- `docs/MIGRATING_FROM_BACKTRADER.md`: concept mapping and side-by-side code examples for users switching from the now-unmaintained Backtrader library (merged from dev).
- Release readiness script now verifies that every referenced artifact file actually exists on disk (not just that `artifact_path` is non-empty), preventing stale pointer references from passing the deployment gate (commit `323b6488`, April 12).
- LumiBot matrix runner emits `lumibot_runner_state.json` checkpoint files during long runs so in-flight case state is inspectable during multi-hour acceptance runs (commit `6944e22c`, April 12).

## 4.4.61 - 2026-04-01

Release bookkeeping only. No functional code changes from 4.4.60.

## 4.4.60 - 2026-04-01

### Fixed
- Data Downloader queue client now uses a dedicated configurable connect-timeout budget instead of a hardcoded `5s`, which prevents IBKR/VIX history refreshes from failing closed on slow downloader connections.
- IBKR history loading now fails closed when a refresh leaves the requested window underfilled, so stale cached slices are no longer returned as if they were complete history.

## 4.4.58 - 2026-04-01

### Added
- `@agent_tool` decorator now auto-includes function source code in tool descriptions, giving AI agents full visibility into parameters, defaults, and implementation details without manual documentation.
- `AgentHandle` now always merges built-in tools with custom user tools (previously custom tools replaced built-ins).
- Four new canonical agent demo strategies: M2 Liquidity (FRED data), Macro Risk (Alpaca bars), Momentum Allocator (Alpaca bars + news), and News Sentiment (Alpaca news). These replace the previous stress-test examples with production-quality patterns.
- Version logged at startup (`LumiBot v{version} starting`) via `logger.info` for CloudWatch/backtest/live log visibility.
- Version included in backtest `settings.json` artifacts (`lumibot_version` field) for post-deploy verification.
- Auto-create next version branch job in release workflow to prevent team-blocking delays after a release.
- Post-deployment verification steps documented in `DEPLOYMENT.md`.

### Changed
- Improved lookahead-bias guardrails in agent system prompts: agents must now explicitly set end-date bounds on ALL temporal tool parameters, not just known ones.
- Major documentation refresh: agents quickstart, canonical demos, observability, FAQ, getting started page with agent framework introduction.

### Fixed
- `BACKTESTING_QUIET_LOGS` env var parsing was broken (comparing string to `None`); now correctly parses boolean-like strings (`true`, `1`, `yes`, `on`).
- Removed contradictory `set_console_log_level("ERROR")` call when `quiet_logs=False` in `trader.py`.
- IBKR pagination test assertions updated to match current behavior.
- `.gitignore` fix for deployment reliability.

## 4.4.57 - 2026-03-30

### Changed
- Bump `quantstats-lumi` dependency to `>=1.1.3,<1.2.0` so tearsheet consumers require the renamed `Worst 1-Month Return` row and the latest machine-readable contract.

### Fixed
- Backtest console print settings no longer get silently overwritten when `lumibot_logger` re-applies log levels during a backtest run. (PR #981 — @davidlatte)
- Tearsheet summary artifact compatibility with `quantstats-lumi` machine-readable metric contract (typed scalar values, no `%` string leakage in JSON scalar values).
- Removed the duplicate `cash_financing_rates()` strategy hook so cash financing now uses a single public interface centered on `set_cash_financing_rates(...)`.
- Backtest stats, plots, and tearsheet inputs now subtract external cashflows from returns, so deposits and withdrawals no longer distort `total_return`, CAGR, or other performance metrics.
- Backtest runners now honor caller-provided `plot_file_html` and `trades_file` paths instead of silently writing trade artifacts to the default `logs/` directory.

### Added
- Tradier stock shorting support: `sell_short` and `buy_to_cover` order sides now map correctly so short-selling equities works on Tradier. (PR #976 — @brettelliot)
- AI trading agent framework: `self.agents.create(...)` inside strategies with DuckDB query tools, agentic backtesting with replay cache, and external MCP server mounting. New modules under `lumibot/components/agents/`.
- End-to-end tearsheet custom-metrics proof coverage for real backtest runs that generate both `tearsheet.html` and `tearsheet_metrics.json`.
- Backtest cash-accounting coverage for `adjust_cash`, `deposit_cash`, `withdraw_cash`, and strategy-managed financing-rate updates.
- Normalized `cash_events` live payload support in LumiBot for Alpaca and Tradier, including stable event IDs, retry-safe pending emission, and bounded payload serialization.
- Period-delta cash columns in `stats.csv` (`cash_*_period`) for manual inspection of deposits, withdrawals, financing accruals, and cashflow-adjusted return math.
- Cash-event rows in `trades.csv` / parquet and cash-event markers in `trades.html`, including deposits, withdrawals, and financing credits/debits.

### Docs
- Expanded public documentation for `tearsheet_custom_metrics(...)`, including parameter structure, full examples, literal-scalar unit behavior, and release-order guidance for QuantStats/LumiBot metric changes.
- Added public documentation for strategy cash accounting, financing lifecycle usage, broker cash-event normalization, and broker-specific limitations for Alpaca and Tradier.

## 4.4.55 - 2026-03-15

### Added
- `BACKTESTING_PARAMETERS` environment variable support for parameter injection in backtest runs.
- Machine-readable `*_tearsheet_metrics.json` artifacts (summary-first) with placeholder output on insufficient/degenerate returns.
- New strategy lifecycle hook `tearsheet_custom_metrics(...)` for appending custom metrics to tearsheet HTML and JSON artifacts.
- Regression coverage for multi-timeframe day-timestep stock lookup and tearsheet metrics/custom-hook passthrough.

### Changed
- Backtest analysis and trader APIs now accept `tearsheet_metrics_file`; default output filename is `*_tearsheet_metrics.json`.
- QuantStats `metrics_json` generation now runs in `summary_only` mode and forwards custom metrics to both HTML and JSON outputs.
- Documentation updates for tearsheet metrics/lifecycle hooks and TradingFee guidance (`per_contract_fee` usage).

### Fixed
- Day-timestep asset lookup regression for multi-timeframe stock/index backtests (including minute->day fallback paths where appropriate).
- IBKR stale no-data cache reuse now forces refresh when requested windows extend beyond cached coverage.
- ProjectX order processing race-condition and tracking hardening merged from `dev`.

Deploy marker: `15e8e268` ("deploy 4.4.55")

## 4.4.54 - 2026-03-08

### Added
- `TradingFee` now supports `per_contract_fee` for broker-style option commissions charged per contract.
- Regression tests for `per_contract_fee` initialization and trade-cost calculations in backtesting.

### Changed
- `TradingFee` fee fields now coerce through `Decimal(str(...))` for stable decimal handling across float inputs.

### Fixed
- Backtesting trade-cost calculations now apply `per_contract_fee * quantity` for taker and maker fee paths (`market`, `stop`, `limit`, `stop_limit`, `smart_limit`).

## 4.4.53 - 2026-03-06

### Added
- Regression tests for daily-cadence datasource seeding in `StrategyExecutor`, routed `1D` timestep normalization, put-delta normalization/model-path strike selection, and IBKR equity corporate-action cache reuse.
- Regression tests for IBKR paged-history retention when later pages are empty, plus option valuation fallback coverage for off-session stale mark scenarios.

### Changed
- Daily-cadence backtests now seed datasource cadence to `day` during strategy initialization to avoid first-lookup minute prefetch blowups.
- `Strategy.get_last_price()` now consistently prefers daily bars for stock/index assets in daily backtest cadence, including routed IBKR stock/index paths.
- Routed backtesting now treats day-like timestep aliases (`1D`, `1day`, etc.) as daily cadence for non-Theta last-price/quote reads.
- ThetaData daily option fetches now prefetch forward in bounded chunks (capped by expiration/end) to reduce repeated downloader round-trips during long runs.
- Option helper strike selection now normalizes absolute delta inputs by option side and uses a fast model-based strike pick for Theta daily option backtests.
- IBKR equity corporate-action enrichment now uses Yahoo history with coverage hints (`last_needed_datetime`) and date-bucket cache keys for stable reuse.
- Backtest artifact export now always writes CSV/parquet outputs for trades/stats/indicators/trade-events regardless of `show_plot` mode.

### Fixed
- Guarded option MTM valuation against off-session stale marks that could cause transient portfolio-value drops in backtests.
- Fixed IBKR history pagination to preserve already-fetched chunks when a subsequent page returns empty.
- Refreshed acceptance baseline metrics for `aapl_deep_dip_calls` and `leaps_alpha_picks_short` to match current deterministic CI outputs.
- Updated `test_classic_60_40` drift-rebalancer expectations to the corrected daily-cadence fill quantities.

## 4.4.52 - 2026-03-03

### Added
- Regression tests for Yahoo corporate-actions helpers (`get_symbol_actions`, `get_symbols_actions`) and IBKR daily equity action enrichment.
- Regression test for routed IBKR daily stock prefetch to guarantee full lookback warmup coverage.

### Changed
- Production-readiness harness (`scripts/ibkr_theta_prod_readiness.py`) now defaults SPX stress windows to 3 months (`2025-01-01` through `2025-03-31`) with a longer timeout.
- Prod-like runner (`scripts/run_backtest_prodlike.py`) now supports `--perf-mode` for cleaner runtime benchmarking without plot/indicator/progress noise.
- Routed IBKR daily stock/index prefetch now uses the computed bar lookback window (`start_datetime`) instead of a short calendar cap from backtest start.
- Acceptance performance history records were refreshed for ongoing regression tracking.
- Deployment runbook now documents local-timeout fallback and explicit review of local-only commit ranges before release.

### Fixed
- Yahoo helper typo in corporate-actions paths (`get_symbol_actions` / `get_symbols_actions`) that prevented IBKR equity split/dividend enrichment from loading actions.
- Acceptance gate hardening: apply a bounded, case-scoped tolerance override for `ibkr_crypto_acceptance_btc_usd` metric jitter (CI/provider-data drift) to reduce false negatives.
- Router benchmark stats now prefer routed datasource bars for stock benchmarks and only fall back to Yahoo on router fetch failure (removes flaky Yahoo-first behavior in CI).

## 4.4.51 - 2026-02-26

### Added
- Option lifecycle event support in backtesting for option expiration outcomes: `assigned`, `exercised`, and `expired` (in addition to `cash_settled`).
- Regression coverage for equity/ETF physical settlement and index cash settlement paths at expiration.
- Opt-in early-assignment heuristic model for short ITM, physically-settled options (`strategy.parameters`: `option_early_assignment_enabled`, `option_early_assignment_max_dte_days`, `option_early_assignment_max_extrinsic`).

### Changed
- Options expiration behavior now follows broker-style settlement defaults:
  - Equity/ETF options settle physically at expiration (short ITM -> assignment, long ITM -> exercise when account constraints allow).
  - Index options settle to cash at intrinsic value.
- Trade artifacts now preserve option-expiration lifecycle statuses in `trades.csv` / `trades.parquet` and `trade_events` exports so downstream consumers can render assignment/exercise/cash-settlement explicitly.

### Fixed
- ThetaData daily options MTM: prefer snapshot quote marks over stale day marks, and allow forward-fill when snapshot data is unavailable.
- ThetaData backtesting: keep intraday index minute/hour fetch bounds aligned to the simulation timestamp instead of forcing full-window end coverage.
- Long ITM equity option expirations now avoid unrealistic forced delivery when account constraints are not met; these contracts expire unexercised in backtests.
- Acceptance baselines: refresh `aapl_deep_dip_calls` and `leaps_alpha_picks_short` metrics to match current option settlement behavior.

## 4.4.50 - 2026-02-19

### Changed
- Indicators HTML: improve subplot scaling so indicator panels render with sane proportions across mixed plots.
- Indicators export: make HTML export non-fatal so backtests still complete if HTML rendering fails.

### Fixed
- Acceptance baselines: refresh 0DTE backdoor baseline metrics and timing metadata to match current provider data revisions.
- Acceptance CI: allow a bounded queue-fill threshold for `spx_short_straddle_repro` while keeping strict queue-free checks for other ThetaData acceptance cases.

## 4.4.49 - 2026-02-10
### Added
- Backtesting artifacts: add `LUMIBOT_BACKTEST_PARQUET_MODE` with `required` contract mode (fail-fast on parquet export failures) and structured parquet export logs (rows/cols/bytes/duration, coerced columns).

### Changed
- Indicators: always emit `*_indicators.csv` + `*_indicators.parquet`, even when a strategy produced no markers/lines/OHLC (empty indicators = valid artifact).
- Trade events: always emit `*_trade_events.csv` + `*_trade_events.parquet` (empty events = valid artifact).

### Fixed
- Stats: stop embedding raw `Asset` objects in the `positions` stats snapshot; sanitize object-ish stats columns before parquet export to prevent `Conversion failed for column positions with type object`.

## 4.4.48 - 2026-02-10

### Added
- Backtesting artifacts: emit Parquet siblings for `*_indicators.csv`, `*_trades.csv`, `*_stats.csv`, and `*_trade_events.csv` (zstd + PyArrow). CSV remains the compatibility layer.

### Changed
- Tradier: support OAuth payload + access token refresh; add runtime notes for the refresh flow.
- Tests: mark DataBento backtest coverage as `apitest` so the default CI suite stays deterministic without vendor credentials.
- Docs: clarify auto-expiry futures behavior and IBKR crypto roots.

### Fixed
- Data: handle `timeshift=None` in Data bars.
- Futures (auto-expiry): make selection roll-aware and harden IBKR conid negative cache behavior.

## 4.4.47 - 2026-02-07
### Added
- Backtesting: support `BACKTESTING_BUDGET` environment override for strategy backtest cash/budget.

### Changed
- Downloader: rename the downloader queue client module from `thetadata_queue_client` to `data_downloader_queue_client` (provider-agnostic naming).

### Fixed
- IBKR: parse seconds-style timesteps (e.g. `20S`) for history requests where supported.
- IBKR crypto futures: harden continuous futures expiration selection in backtesting.
- Logging: avoid stale env-driven logger levels by re-applying Lumibot logging configuration on each `get_logger()` call (reduces test flakiness when env vars toggle).

## 4.4.46 - 2026-02-04

### Fixed
- Backtesting routing: when `futures`/`future` is configured, default `cont_future` to the same provider so `AssetType.CONT_FUTURE` does not fall back to `default`.
- Backtesting performance: default per-asset fetch throttling (`sleep_time`) to 0 for backtesting data sources (keeps live default throttling unchanged).
- Backtesting performance: bound `get_trading_days()` calendar initialization to the backtest date window to avoid building decades of unused schedules.

## 4.4.45 - 2026-01-30

### Fixed
- Release: include `lumibot/resources/ThetaTerminal.jar` in the PyPI wheel/sdist (required by BotManager and ThetaData setup).
- Backtesting: `BacktestingBroker.process_pending_orders()` now accepts both iterable order buckets and legacy buckets that expose `get_list()`.

## 4.4.44 - 2026-01-30

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Release: correct PyPI packaging so `lumibot==4.4.44` includes `Strategy.add_ohlc()` (the published `4.4.43` wheel was missing it).

## 4.4.43 - 2026-01-30

**NOTE:** The PyPI `lumibot==4.4.43` artifact was published from an older commit and does **not** include the changes
listed below. Upgrade to `lumibot==4.4.44`.

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Backtest executor safe-sleep overload now applies only in backtests and uses real sleep outside backtesting.

## 4.4.42 - 2026-01-30

**NOTE:** The PyPI `lumibot==4.4.42` artifact was published from an older commit and does **not** include the changes
listed below. Upgrade to `lumibot==4.4.43`.

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Backtest executor safe-sleep overload now applies only in backtests and uses real sleep outside backtesting.

## 4.4.41 - 2026-01-28

### Added
- Tests: add regression coverage for futures calendar spreads (same root symbol, different expirations) to prevent margin/PnL ledger collisions.
- Docs: add investigation notes for ThetaData stale-loop behavior and futures “ghost PnL” equity spikes.

### Changed
- Backtesting helpers: cache trading calendar schedules by year and slice to the requested window to reduce repeated schedule computations.
- ThetaData: avoid eager debug string building in hot paths unless debug logging is enabled.

### Fixed
- ThetaData backtesting: normalize legacy/externally-warmed `prefetch_complete` metadata before cache validation to prevent per-bar STALE/REFRESH thrash.
- ThetaData backtesting (day): treat `tail_missing_permanent=True` as satisfying end-coverage validation to prevent per-bar STALE→REFRESH loops on warm caches.
- Backtesting futures: include expiration in futures margin/PnL ledger keys so calendar spreads (same root, different expiries) don't incorrectly net margin/realized PnL, preventing “ghost PnL” equity spikes.

## 4.4.40 - 2026-01-27

### Added
- ThetaData backtesting: coverage-based `prefetch_complete` computation + tests to prevent per-bar STALE/REFRESH thrash when cached datasets are incomplete.

### Changed
- Yahoo helper: when S3 backtest cache is enabled, hydrate cached pickles before falling back to live Yahoo fetches; upload pickles to the cache on write.

### Fixed
- ThetaData EOD: enforce the provider's 365-day window limit per request and keep progress tracking consistent with chunked downloads.

## 4.4.39 - 2026-01-27

### Added

### Changed

### Fixed
- Backtesting router (IBKR futures/cont_future/crypto): prefetch full backtest window once per series and slice from memory to avoid per-iteration history fetches (major warm-cache speedup).
- Indicators: prevent `plot_indicators()` hovertext generation from crashing when `detail_text` is missing/NaN/NA (e.g., mixed indicator points with and without `detail_text`).

## 4.4.38 - 2026-01-26

### Added
- IBKR futures: automatic exchange resolution for futures and continuous futures (via downloader secdef search) with persisted root→exchange cache.
- IBKR futures: regression/unit coverage for exchange routing, per-call exchange overrides, and conid registry bulk updates.

### Changed
- Backtesting router: accept `futures`/`cont_futures` route-key aliases for convenience (maps to `future`/`cont_future`).

### Fixed
- IBKR futures: honor call-time `exchange=` overrides consistently for `get_historical_prices`, `get_last_price`, and `get_quote` (live + backtesting), and include exchange in cache keys to avoid cross-exchange contamination.
- IBKR futures conid registry: bulk-ingest `trsrv/futures` responses and harden S3 persistence with merge-before-upload retry to avoid lost updates under concurrent backtests.
- Continuous futures: add roll rules for COMEX micro gold (`MGC`) and NYMEX crude oil (`CL`/`MCL`) and fix monthly roll selection to avoid hanging on already-rolled contract months.

## 4.4.37 - 2026-01-24

Deploy marker: `174875a8` ("chore: start 4.4.37")

### Added

### Changed

### Fixed
- Backtesting: support `timestep="hour"` in pandas-backed history requests (`Data.get_bars()`), used by routed backtesting (e.g., IBKR futures/crypto).
- ThetaData backtesting: proxy missing NDX underlying/index bars/quotes via scaled `QQQ` so NDX options strategies have a usable underlying series.
- ThetaData (downloader): normalize v3 row-style and nested option-history payloads so option quotes/chains parse correctly and caches hydrate instead of looping.
- ThetaData: stop incorrectly scaling legitimate high strikes (e.g., NDX ~ 18,000) during chain-building; only de-scale clearly thousandths-encoded payloads.
- Backtesting progress: fix progress-bar throttling keying for non-terminal sinks (prevents intermittent missing output under test runners/log capture).
- Backtesting stats: fix `cagr()`/`volatility()` crash during end-of-run stats generation when returns index uses non-nanosecond datetime dtypes (e.g., `datetime64[us]`/`datetime64[s]`).

## 4.4.36 - 2026-01-24

### Changed
- IBKR futures backtesting: accelerate intraday resampling paths to avoid repeated per-iteration recomputation for timesteps like 5minute/15minute/30minute.

### Fixed
- ThetaData EOD: treat all-zero OHLC rows as missing placeholders to prevent one-day portfolio valuation cliffs.

## 4.4.35 - 2026-01-19

### Changed
- IBKR futures backtesting: cut downloader roundtrips by caching history windows across iterations and preferring native bar sizes (e.g., 15-min) when available.

## 4.4.34 - 2026-01-19

### Added
- IBKR futures: add acceptance backtest strategy covering market/limit/stop/stop-limit/trailing/smart-limit and OCO/OTO/bracket semantics.
- IBKR futures: add parity/apitest helpers + scripts to compare IBKR runs against stored DataBento artifact baselines.
- Docs: add IBKR futures backtesting notes and DataBento parity guidance.

### Changed
- IBKR futures backtesting: interpret `get_last_price(dt)` as the last completed bar close (avoid lookahead bias).
- Continuous futures (IBKR): stitch rolled segments with a 1-minute overlap and deterministic de-duplication.
- US futures gap handling (IBKR): replace flaky calendar logic with a simple rule-based “closed interval” detector to reduce repeated downloader fetches.

## 4.4.33 - 2026-01-12

### Fixed
- SMART_LIMIT (live): avoid scanning full tracked order history in the background loop by using the broker’s active-order fast path, preventing high RSS growth in accounts with large historical order lists.
- Backtesting (router): make dataset lookup timestep-aware so minute requests don’t accidentally resolve to daily Data objects, and routed crypto assets passed as `(base, quote)` work reliably.
- Backtesting (router): refactor multi-provider routing to a provider registry + adapters (no hard-coded branching), add `alpaca`/`ccxt` support, and allow CCXT exchange-id aliases like `coinbase`/`kraken` (case/sep-insensitive).
- IBKR (crypto): normalize daily timestep handling (`day`/`1d`/`1day`) so crypto daily bars consistently use the derived-daily path.
- ThetaData: prevent acceptance backtests from hitting the downloader queue by enforcing CI-only warm-cache guardrails consistently (local runs behave like GitHub CI).
- ThetaData: treat **session close** as “complete coverage” for index minute OHLC to avoid perpetual STALE→REFRESH loops when backtest end dates are represented as midnight.
- Backtest cache (S3): speed up warm-cache hydration by streaming small objects via `get_object` instead of `download_file` transfer manager overhead.

## 4.4.32 - 2026-01-10

### Added
- Runtime telemetry: lightweight memory/health JSON lines (`LUMIBOT_TELEMETRY ...`) for diagnosing OOMs in long-running live workers.
- Broker API smoke apitests: basic Alpaca and Tradier connectivity + order lifecycle checks (paper/live as available).

### Fixed
- Live (Tradier): treat `submitted/open/new` as equivalent to reduce repeated NEW events under polling; bound live trade-event history to avoid unbounded memory growth in long-running workers.
- Live (Tradier): avoid heavy DataFrame copy chains when cleaning orders; skip ingesting large historical *closed* order lists on the first poll to prevent startup memory spikes in accounts with long histories.

## 4.4.31 - 2026-01-09

Deploy marker: `d5c6b730` ("deploy 4.4.31")

### Added
- SMART_LIMIT: live matrix apitests + runner scripts; expanded unit coverage for edge cases.
- Investigations/docs: production endpoint breakdown notes and an expanded backtesting performance playbook.
- ThetaData: per-asset download progress reporting for option-chain strike scans (exposed via `download_status`).

### Changed
- Acceptance backtests now run in CI (no longer marked `apitest`); baselines were refreshed for LEAPS + MELI; CI caps were raised for long full-year strategies due to runner variability.
- CI policy: use pytest markers (not env vars) for opt-in/slow tests; some slow ThetaData backtest tests were made opt-in, then re-enabled once bounded.
- Backtests under pytest no longer auto-open HTML artifacts (plots/indicators/tearsheets) in a browser.
- Strategy collaboration workflow: clarified “shared version branch” conventions.

### Fixed
- ThetaData: reduced option-chain fanout and improved warm-cache parity (reuse chain cache under constraints; prefetch strikes only for head+tail expirations when unconstrained; bounded intraday chain defaults).
- ThetaData: improved intraday cache coverage and corrected daily option MTM behavior.
- Polygon: reduced split-cache rate limit thrash.
- SMART_LIMIT: hardened behavior for quote/stream failures.
- Backtesting progress: improved per-asset `download_status` for clearer “what is downloading” diagnostics.

### Removed
- ⚠️ Removed ThetaData chain default-horizon env vars (`THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT*`). Chain default horizons are now fixed and covered by tests.
- Removed the short-lived `LUMIBOT_DISABLE_UI` env var (use `SHOW_PLOT/SHOW_INDICATORS/SHOW_TEARSHEET` + pytest non-interactive behavior instead).

## 4.4.30 - 2026-01-06

Version bump marker: `76b31467` ("Docs/tests: normalize artifacts + bump version")

### Added
- Backtesting performance playbook and production/local parity notes.
- `LUMIBOT_DISABLE_DOTENV` to disable recursive `.env` scanning in prod-like runs.

### Fixed
- ThetaData: filtered intraday parquet loads to reduce memory footprint; daily option MTM fixes.

## 4.4.29 - 2026-01-06

Deploy marker: `b8c6a839` ("deploy 4.4.29")

### Fixed
- Prevent production backtests from OOM-like hard exits (`ERROR_CODE_CRASH`) when refreshing multi-year intraday ThetaData caches by avoiding deep copies during cache load/write and trimming non-option intraday frames in-memory.

## 4.4.28 - 2026-01-05

### Added
- Production backtest runner script (`scripts/run_backtest_prod.py`) plus investigation docs for NVDA/SPX accuracy, parity, and startup latency.

### Fixed
- ThetaData missing-day detection for intraday caches across UTC midnights (prevents “every other trading day missing” forward-fill storms).
- Backtesting: improved intraday fills and cache end handling; deterministic drift ordering for rebalances.

## 4.4.27 - 2026-01-05

### Fixed
- Reduced peak memory usage for ThetaData backtests and tear sheet generation to avoid OOM crashes in production.

## 4.4.26 - 2026-01-05

### Changed
- ThetaData: cache snapshot quotes per session and fetch full-session option quote snapshots to reduce downloader fanout.

### Fixed
- Clamp future backtest end dates instead of failing.

## 4.4.25 - 2026-01-04

Deploy marker: `b7f83088` ("Deploy 4.4.25")

### Added
- Public documentation page for environment variables (`docsrc/environment_variables.rst`) plus engineering notes (`docs/ENV_VARS.md`).
- Backtest audit telemetry can be preserved in a separate `*_trade_events.csv` artifact (see `LUMIBOT_BACKTEST_AUDIT`).
- Investigation docs for ThetaData corporate actions and performance.

### Changed
- ThetaData option chain defaults are now bounded to reduce cold-cache request fanout (configurable via `THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT*`).

### Fixed
- OptionsHelper delta-to-strike selection fast path to prevent per-strike quote storms (SPX Copy2/Copy3 slowness).
- Prevent backtest tear sheet generation from crashing on degenerate/flat returns (NVDA end-of-run failures).
- Reduce ThetaData corporate action request thrash via memoization/negative caching.
- Normalize ThetaData intraday bars for corporate actions in backtests so option strikes and underlying prices stay in the same split-adjusted space (NVDA split issues).
- Improve ThetaData snapshot quote selection near the session open to avoid missing NBBO due to end-of-minute timestamps.

## 4.3.6 - 2024-11-16

- Fixed ThetaData EOD corrections by fetching a real 09:30–09:31 minute window for each trading day, preventing zero-length requests and the resulting terminal hangs.
- Logged the active downloader base URL whenever remote mode is enabled to make it obvious in backtest logs which data path is being used.
- Added regression tests covering the custom session window override plus the fallback path when Theta rejects an invalid minute range.
