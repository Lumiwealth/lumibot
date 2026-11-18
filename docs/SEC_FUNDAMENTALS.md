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
- Balance ratios use one balance sheet date (the denominator's latest). Debt is `LongTermDebt` (or the total filed together with lease obligations), else the noncurrent line plus the current portion when reported; tags in `_METRIC_TAGS`. When the latest balance sheet has no debt line, `debt_to_equity` uses the latest earlier, non-stale date that has both debt and equity (debt totals are often filed only in the 10-K). Equity falls back to the line including noncontrolling interest. Missing debt is None, never zero.
- Shares (`_shares`), whole company, in this order: (1) cover-page or balance sheet count from companyfacts; (2) when none is filed or it is stale (multi-class companies: per-class counts are dimensional and not in companyfacts), the basic weighted average share count of the latest filed period (shortest period ending last); (3) when there is no weighted average either, or the company is in `SHARE_CLASS_UNITS` (classes not one for one, so an average in one class's units is unusable), the per-class cover counts read from the XBRL instance of the latest 10-K/10-Q public at `as_of` (`_cover_share_counts`: the filing's `index.json`, then the instance file; raw files cached like filing documents under `sec/filings/<cik>/<accession>/`, plus a small local `cover_share_counts.json`), summed with `SHARE_CLASS_UNITS` weights (default one for one) and expressed in the asked ticker's class. All converted to the as_of share basis. `periods[...]['inputs'][0]['tag']` names the source.
- Registrant history (`lumibot/fundamentals/company_records.py`, data with sources): `PREDECESSOR_CIK` maps a successor CIK to the CIK that filed before it; `_point_in_time` walks it while the current registrant has no filing public at `as_of`. `get_metrics(...)['cik']` and the table's `cik` column name the CIK actually used (two tickers of one company share it: dedupe on it).
- Top-10 check 2026-11-14 (34 large companies, Yahoo as-traded closes, split lookup on): computed top 10 by market cap equals the reference top 10 on 2015-06-01, 2020-06-01 and 2024-06-03; values within 2% except Visa (about 10% low, classes added one for one). Coverage with split lookup on, 503 members, 2024-06-03: market cap 494, debt to equity 402 (left: 8 new listings, 1 undatable split, D).
- Coverage measured 2026-11-14 on 503 current S&P 500 members as of 2024-06-03, SEC only, split lookup off: market cap 444 -> 481, debt to equity 268 -> 400. Left: 10 tickers mapped to a registrant with no filings yet (8 new listings, XOM, BLK), 7 undatable splits (split lookup was off), 3 with no share tag (STZ, ERIE, TSN), 2 stale (BRK-B, V); debt: 63 with no long-term debt line (mostly financials), 30 with non-positive equity. Tests: recorded rows in `tests/fixtures/sec_companyfacts_coverage.json`.
- A latest period ending more than 548 days before `as_of` is stale: None with a reason.
- Price metrics use `price=` (as traded on `as_of`) or `strategy.get_last_price(symbol)`.
- Splits (`_SplitBasis`, `_sec_split_events`): every diluted EPS input and share count is converted from the share basis on its filing date to the basis at `as_of`, row by row, so a TTM built from a pre-split 10-K and a post-split 10-Q is not mixed. SEC split windows come from one period's EPS differing by a clean ratio between two filings, confirmed by the weighted diluted share count moving by the inverse (or two periods / large values); an EPS change without a share change is a restatement, not a split. Windows use the whole companyfacts file only to decide whether a basis is known, never to supply a value. A split inside a window, or after the company's latest filing, is dated from the backtest price data's `stock_splits` (cheap, always consulted) or `split_history` (Yahoo by default, called only when needed). A split-adjusted backtest price is converted back to the traded price using the price data's own splits after `as_of`; adjustment is proven by a split inside the data (closes jump or not) or the data source's `should_apply_stock_splits_to_positions`. Anything undatable is None with a reason. Real check: NVDA 2024-06-07 P/E 70.7, 2024-06-12 P/E 73.3 (before the fix 7.3). Tests: `tests/test_sec_fundamental_metrics_splits.py`.
- Market cap accuracy (4.6.77, `tests/test_sec_fundamental_market_cap_accuracy.py`). Found by ranking 500 companies in a 2016 to 2026 backtest on adjusted prices: two mid-size companies ranked first and the real largest ones were missing. Four causes, four rules:
  - Filed share counts are cross-checked (`_filed_share_count`, `_share_count_disagreement`). Filers sometimes report `dei:EntityCommonStockSharesOutstanding` 1,000 or 1,000,000 times too large (seen in real 2016 10-Qs). A count is used only when within `_SHARE_COUNT_MAX_DISAGREEMENT` (50x) of the median of the same tag's last 12 filed values, or of the latest weighted average basic shares, all on the as_of basis. A rejected count falls to the next latest instant; when none passes the result is None with a "wrong scale" reason and the weighted average is not used either (nothing says which is wrong). Known limit: a tag a company files in thousands in every filing agrees with itself and is not caught when it is the only share count.
  - Price traded (`_as_traded_prices`, `_AsTradedPrices`). A data source with adjusted backtest prices can implement `get_backtest_as_traded_daily_closes(asset)` (DataSource default None; Alpaca implements it with `adjustment="raw"` daily bars, grouped by `prefetch_backtest_as_traded_daily_closes`, same shared bar cache). price = backtest price x `as_traded_close / close` of that session (exact for splits and dividends; the ratio changes only on ex-dates so it carries no price information). Split-like jumps of that ratio are the price data's split history for `_SplitBasis` (ratios under 3-for-2 only inside a matching SEC window, so a large special dividend is not a split). Without the hook the older path applies (price data `stock_splits`, else None when a later SEC split exists). Before: Alpaca prices (adjustment "all") had no split history, so every company with a later split was None and the rest used a dividend-adjusted price as if traded (about 35% low for a high-dividend stock ten years back).
  - Restatements (`_sec_split_events`): an unconfirmed EPS pair is dropped when a pair of the same ratio in an overlapping filing window was refuted by an unchanged share count (a new accounting standard restates several periods; some have no share count to compare).
  - Wrong-scale filing (4.6.78, `_sec_split_events`): a filing whose EPS differs from the filing before it by a factor and from the filing after it by the inverse is one bad filing, not two splits; pairs touching it are dropped.
  - Tickers (`ticker_to_cik`): `AB.B`, `AB/B`, `AB B` fall back to the SEC spelling `AB-B`.
- The SEC ticker map is current, not point in time (for example XOM maps to a new 2026 holding-company CIK, 0002115436, with no history). Submissions JSON has `formerNames` (old names of the same CIK) but no predecessor-CIK field, so there is nothing reliable to follow. `_no_history_reason`: when the mapped CIK has no filing public at `as_of`, every metric is None with one reason naming the CIK and its first filing date, and statements get the same warning. Test: `tests/test_sec_fundamentals_ticker_history.py`.

## Screeners

No ranking or filtering call exists on purpose: `get_metrics_table` returns the data and the strategy sorts and filters it in pandas (drop missing values, never fill with zero). Market cap as of a date = cover-page shares from the latest filing public then (split-basis corrected) x the strategy's price at the bar, so a backtest screener needs companyfacts for the universe (`preload`, shared cache) and daily bars for the universe from the price source. No index membership data ships with LumiBot: a current-member list in a backtest has survivorship bias (documented in `docsrc/fundamentals.rst`). Examples: `lumibot/example_strategies/stock_top_market_cap.py`, `stock_revenue_growth_low_debt.py`; tests `tests/test_example_fundamental_screeners.py`.

## Stored daily tables (4.6.88)

`lumibot/fundamentals/metrics_history.py`. Why: a 60-to-500-symbol market cap screen spent most of its first day loading a daily price for every candidate (each slow symbol cost 30 to 100 s of provider edge requests). A stored row per (session, symbol) holds every `FUNDAMENTAL_METRICS` value plus `close`, `cik`, `available_at` (session 16:00 New York) and `reasons` (JSON). Built by `build_metrics_history(sec, closes)` = `get_metrics(symbol, as_of=<session close>, price=<traded close>)`; saved per calendar year to `metrics_daily/v1/<year>.parquet` locally and `sec/metrics_daily/v1/<year>.parquet` in the shared backtest cache (`save_metrics_history`, merges by date and symbol). Reader invariants (`MetricsHistory.lookup`): backtest mode only; latest row with session date <= the as-of session (before 16:00 the previous session); rows older than `MAX_ROW_AGE` (7 days) are ignored, never carried forward; every requested metric must be a stored column; symbols with `prices=` skip the table; when stored rows cover the date, a symbol they leave out is None with a reason and is not priced (4.6.90: an S&P 500 screen from 2010 otherwise downloaded history for hundreds of left-out companies); a broken or missing file means compute as before. Builders must pass as-traded closes (not split or dividend adjusted), otherwise market cap is wrong. Tests: `tests/test_sec_metrics_history.py`.

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

## Compact facts files (4.6.65)

`_timeline_for_cik` first tries `companyfacts_compact/CIK<cik>.json.gz` (`_load_compact_facts`; remote key `sec/companyfacts_compact/...`), and only on a miss parses the full companyfacts file and writes the compact one (`_store_compact_facts`). The compact file holds `_COMPACT_TAGS` (statement and metric tags) as row arrays, plus the full file's filing index (accession -> filed, form) and its us-gaap tag list, so the timeline built from it is identical for those tags. It carries the full file's SEC fetch time and is judged by the same `_cache_is_usable` rules. A tag outside the set makes `add_tags` read the full file for that company. A compact file whose tag list lacks a current `_COMPACT_TAGS` entry, or with another `version`, is ignored and rebuilt. Measured 2026-11-14, 503 companies: full files 1,655 MB and 14 s to load from disk; compact 16 MB and 5 s (most of it building the in-memory rows); tables identical. Not built: one shared object per fact for all companies (would remove the per-company objects for a single-metric screen); SEC's `frames` API gives one period per request for all companies but not the filing history needed for point-in-time values. Tests: `tests/test_sec_fundamental_metrics.py` (compact section).

Small derived files (4.6.82). `_load_acceptance_times` keeps accession -> acceptance time in `submissions_acceptance/CIK<cik>.v1.json` (remote key `sec/submissions_acceptance/...`), with the submissions file's fetch time, judged by `_cache_is_usable(mutable=True)` like the compact facts; a missing or stale one is rebuilt from the submissions file. `get_metrics_table` calls `_prefetch_acceptance_times` so the companies that need acceptance times at a date load them on 8 threads before the per-company loop. `_company_cover_counts` keeps accession -> cover-page share counts per company in `cover_share_counts/CIK<cik>.v1.json` (remote key `sec/cover_share_counts/...`); a filing is immutable, so its index and XBRL instance are fetched once. Two runs writing the same company's file at once can drop one entry, which is read again from the filing later. Bump the `v1` in the name when `_cover_share_counts_from_xbrl` changes. Why: a warm hosted 10-year monthly top-N run downloaded 946 MB of SEC files (1,415 objects), most of it XBRL instances and submissions files read inside the monthly loop. Tests: the speed section at the end of `tests/test_sec_fundamental_metrics.py`.
