# IBKR futures daily close boundaries

At 15:42 UTC on October 9, a fresh read-only process using the deployed LumiBot 4.6.9 read actual v44 S3 prices for MGC, GC and MES without calling either provider. Six completed daily candles matched the hourly bar starting at their closing boundary rather than the final prior hourly bar. For example, MGC's October 7 close was 4131.3, matching its next-session 18:00 bar; the final prior 16:00 bar closed at 4136.9. The October 8 result had the same defect. CME's gold specifications place the daily break at 17:00-18:00 Eastern, so the new 18:00 bar belongs to the following session.

The daily aggregator included both ends of its session window. Intraday timestamps mark bar starts, so the closing boundary must be exclusive. Both the preferred hourly path and the minute fallback now aggregate `[session_open, session_close)`. Existing daily labels, real intraday prices, cache keys and provider routing remain intact. Futures daily candles are derived from intraday files, so no persisted daily price file is shifted or rewritten by this repair.

Two regressions failed with a distinctive next-session price contaminating the prior candle. They verify the corrected close, high and volume and preserve that bar as the following session's legitimate open. All 197 affected IBKR unit tests pass. Exact deployed-package readback must additionally compare the resulting daily prices with the actual prior-session intraday prices.

## Publication gate recovery

Tag v4.6.10 passed its unit and backtest gates but did not publish: its option credit-spread close case failed three machine checks while the model judge and actual broker orders showed correct closing sides and quantities. Long private order journals caused the model-visible tool result to be pruned. The harness then scored side-less close arguments instead of the legs LumiBot had actually built.

The harness now recovers a pruned result only from the unique broker parent whose private journal reason hash matches the exact accepted tool call. Missing, unrelated or ambiguous records are not inferred. Normal unpruned results retain their existing path; expiration accepts the actual order schema's `exp` field. A regression executes the real built-in closing tool and verifies both successful recovery and rejection of an unrelated decision. All 114 tests in the futures daily and complete eval-harness, call-budget, pacing and isolation groups pass. The preserved failed tag remains an audit record; the normal version/4.6.11 release carries these repairs and all inherited 4.6.10 changes forward.

The normal model judge, signed-quantity checks, exact order count and flat-position checks remain release blockers. No agent prompt or strategy API change is needed. No diagram is needed for the two session comparisons or the bounded test-evidence recovery.

Primary references: [CME gold contract hours](https://www.cmegroup.com/trading/metals/files/fact-card-gold-futures-options.pdf) and [IBKR historical bar documentation](https://ibkrcampus.com/docs/web-api/v1/endpoints/market-data/historical-market-data).
