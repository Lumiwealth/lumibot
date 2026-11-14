# SEC_FUNDAMENTALS.md - SEC EDGAR fundamentals in LumiBot

How `self.fundamentals` (`lumibot/fundamentals/sec.py`) loads, indexes, caches and gates SEC data.

**Last Updated:** 2026-11-13
**Status:** Active
**Audience:** Contributors and AI agents working on fundamentals, caching or agent research tools

## Overview

`SECFundamentals` reads public SEC EDGAR JSON (no API key): the ticker map, `companyfacts` (XBRL facts) and `submissions` (filing index with acceptance times), plus filing documents. Public docs: `docsrc/fundamentals.rst`.

```python
self.fundamentals.preload(symbols)                 # warm a universe in parallel
self.fundamentals.get_income_statement("AAPL")     # also get_balance_sheet, get_cash_flow
self.fundamentals.get_fact_history("AAPL", ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"])
self.fundamentals.get_metric("AAPL", "pe_ratio")  # ready-made metrics (FUNDAMENTAL_METRICS)
self.fundamentals.get_metrics("AAPL")              # all metrics + reasons + source periods
self.fundamentals.get_metrics_table(symbols)       # pandas DataFrame for a scanner
self.fundamentals.get_company_facts("AAPL")        # compact; raw=True for the filtered payload
self.fundamentals.get_filings("AAPL", form="10-K") # plus search_filing, get_filing_document, sections
```

Agent tools use the same names (see `lumibot/components/agents/builtins.py`).

## Point-in-time table (parse once)

`_CompanyTimeline` is built once per process per company from the companyfacts file:

- Every filing (all taxonomies) gets an availability timestamp. Without an acceptance time it is 09:30 New York on the day after `filed`. With one (from `submissions`, UTC), it is the acceptance moment, or the next weekday 09:30 if accepted at/after 16:00 New York or on a weekend.
- Rows of the statement tags (`_PRIORITY_COMPANY_FACT_TAGS`) plus any tag requested through `get_fact_history` are kept as interned tuples, sorted by availability. Other tags are read from the cached file on demand (compact `get_company_facts` only) so a 500-company universe stays in memory (measured 1.3 MB per company on average, 64 MB for 50 large caps).
- A lookup is `bisect` on availability plus a scan of the visible rows. Per period `(start, end)` the latest public row wins, so a restatement applies only once its filing is public (issue #1141).
- Statements anchor on the **newest period** (end date), then availability. Sorting by filing date first let a later 10-K/A for an older year become the "current" statement.
- Statement and compact results are memoised per *filing state* (`state_index`): the number of filings public at `as_of`. They only change when a new filing becomes public.
- Acceptance times are loaded lazily: only when `as_of` falls inside a filing's uncertain window (filing day 00:00 to next day 09:30 New York, padded to 36 h). Before that the day-after rule is used; it is never earlier than the acceptance-based time. A failed submissions fetch is not retried in the process (fallback stays).
- In live mode the table and ticker map are rebuilt after `mutable_cache_ttl_seconds` (default 300).

Measured 2026-11-13 (MacBook, load average 12 to 22 from other jobs): 5 companies x 12 monthly dates x 3 statements = 180 calls. Old code 24.8 s warm (27.1 s cold, 6 requests). New code 0.24 s warm including the one-time parse (1.7 s cold, 8 requests including 2 lazy submissions files). Old and new returned identical values for all 180 statements on that real data.

## Ready-made metrics

`get_metrics` (class `_MetricContext`) computes `FUNDAMENTAL_METRICS` from the same timeline. Inputs and tag priority are in `_METRIC_TAGS` (cover-page shares use the `dei:` taxonomy; `add_tags` accepts `"taxonomy:Tag"`).

- TTM flows: an annual period (350 to 380 days) ending at the latest end, else latest FY + the longest year-to-date period ending there - the same-length period ending about 364 days earlier (7 and 10 day tolerances). Diluted EPS uses the same arithmetic.
- Margins and growth use revenue's latest end; the numerator must exist for that same twelve months.
- Balance ratios use one balance sheet date (the denominator's latest). Debt is `LongTermDebt`, else `LongTermDebtNoncurrent` plus the current portion when reported. Missing debt is None, never zero.
- A latest period ending more than 548 days before `as_of` is stale: None with a reason.
- Price metrics use `price=` (as traded on `as_of`) or `strategy.get_last_price(symbol)`.
- Splits (`_SplitBasis`, `_sec_split_events`): every diluted EPS input and share count is converted from the share basis on its filing date to the basis at `as_of`, row by row, so a TTM built from a pre-split 10-K and a post-split 10-Q is not mixed. SEC split windows come from one period's EPS differing by a clean ratio between two filings, confirmed by the weighted diluted share count moving by the inverse (or two periods / large values); an EPS change without a share change is a restatement, not a split. Windows use the whole companyfacts file only to decide whether a basis is known, never to supply a value. A split inside a window, or after the company's latest filing, is dated from the backtest price data's `stock_splits` (cheap, always consulted) or `split_history` (Yahoo by default, called only when needed). A split-adjusted backtest price is converted back to the traded price using the price data's own splits after `as_of`; adjustment is proven by a split inside the data (closes jump or not) or the data source's `should_apply_stock_splits_to_positions`. Anything undatable is None with a reason. Real check: NVDA 2024-06-07 P/E 70.7, 2024-06-12 P/E 73.3 (before the fix 7.3). Tests: `tests/test_sec_fundamental_metrics_splits.py`.
- The SEC ticker map is current, not point in time (for example XOM maps to a new 2026 holding-company CIK with no history). Not fixed here.

## Rate limit

Request starts are spaced by `min_request_interval_seconds` (default 0.11 s, under SEC's 10 requests per second) across all threads, so `preload()` workers share one limit. `request_count` counts actual SEC requests.

## Local and shared cache

Local files live under `LUMIBOT_SEC_CACHE_DIR` (default `~/.lumibot/cache/sec`) with a `<file>.meta.json` sidecar (`fetched_at`, `etag`, `last_modified`).

In **backtest mode only**, raw files also go through LumiBot's remote backtest cache (`lumibot/tools/backtest_cache.py`, `LUMIBOT_CACHE_BACKEND=s3`). Keys describe the data only, never a user, account or strategy:

| Data | Remote key (after `<LUMIBOT_CACHE_S3_PREFIX>/<LUMIBOT_CACHE_S3_VERSION>/`) |
|------|------|
| Ticker map | `sec/company_tickers.json` |
| Company facts | `sec/companyfacts/CIK<10-digit CIK>.json` |
| Submissions | `sec/submissions/CIK<10-digit CIK>.json` |
| Filing document | `sec/filings/<10-digit CIK>/<accession>/<primary document>` |

JSON objects are stored exactly as the SEC serves them, plus one top-level key:

```json
"_lumibot_cache_meta": {"fetched_at": "2026-11-13T02:00:00+00:00", "etag": "...", "last_modified": "..."}
```

`fetched_at` (ISO 8601, timezone required) is when the data left the SEC. LumiBot strips the key on read and writes the local sidecar from it.

Freshness in a backtest: a copy with `fetched_at` at or after the backtest end date is used as is; an older copy is used only if it is less than one day old, otherwise it is refetched from the SEC and re-uploaded. With no known end date the cached copy is used. Live mode never touches the remote cache.

### Writing the cache from SEC bulk files (warmer)

SEC publishes nightly `companyfacts.zip` and `submissions.zip`. A warmer that wants LumiBot to get hits must:

1. Write each `CIK##########.json` from `companyfacts.zip` to `sec/companyfacts/CIK##########.json` (same 10-digit, zero-padded name) with `_lumibot_cache_meta.fetched_at` set to when the zip was downloaded.
2. Write each main `CIK##########.json` from `submissions.zip` to `sec/submissions/CIK##########.json` the same way. The paginated `CIK##########-submissions-NNN.json` files are not read by LumiBot today.
3. Write `https://www.sec.gov/files/company_tickers.json` to `sec/company_tickers.json` (the zips do not contain it).
4. Use the same prefix and version as the backtest containers. Without `_lumibot_cache_meta.fetched_at` a downloaded file looks fresh by its local mtime, so always write it. Running nightly keeps every backtest whose end date is before the warm a hit.

## Tests

- `tests/test_sec_fundamentals.py`: original contracts (gating, caching, filings, sections).
- `tests/test_sec_fundamental_metrics.py`: TTM arithmetic, margins, balance ratios, valuation, no look-ahead on filing day, restatement isolation, strategy price, None-with-reason cases, bulk table.
- `tests/test_sec_fundamentals_point_in_time.py`: parse-once, restatements, after-close availability, lazy acceptance, shared cache across two machines, refresh rule, preload and rate limit.
- SEC tests patch `get_backtest_cache` to a disabled manager: a developer `.env` with a remote cache would otherwise receive the synthetic fixtures.
