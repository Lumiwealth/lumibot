# TQQQ SMC Provider Difference Investigation

One-line description: Production artifact comparison for TQQQ Smart Money Concepts v15-v19 across BotSpot Auto/IBKR and ThetaData, with local replay limitations.
Last Updated: 2026-06-14
Status: Production evidence gathered and corrected local replay matrix completed
Audience: LumiBot, BotSpot Node, Bot Manager, and strategy support engineers

## Overview

Rob asked for a full investigation of why the TQQQ Smart Money Concepts strategy
shows materially different behavior between BotSpot Auto/IBKR and ThetaData,
especially around version 19's high ThetaData max drawdown and the much lower
current BotSpot Auto/IBKR result.

The requested historical comparison window is:

- Start: `2016-01-21`
- End: `2026-04-16`
- Strategy: `TQQQ Smart Money Concepts`
- Strategy ID: `b6b0d5a6-375b-4bc5-8e3d-cdffc73e25f2`
- AI strategy ID: `79797013-87f9-4631-b544-b9178753b98f`

The main production-artifact conclusion is that this is not one single provider
bug. There are two separate effects:

1. Version 19 changed strategy behavior versus versions 17/18 by re-allowing
   startup-sync / mid-trend entries. That appears to explain why ThetaData
   v19 has about `-70%` max drawdown while ThetaData v17/v18 had about `-29%`
   max drawdown.
2. Current BotSpot Auto/IBKR v19 has a separate stop-processing problem: it
   creates stop orders, but production logs show those stops being canceled
   when the broker cannot find pandas bars at after-hours hourly timestamps.
   The current v19 BotSpot Auto exact-window run had `196` stop orders created,
   `390` stop cancellations, and `0` stop fills.

## Production Backtests

| Scenario | Backtest ID | Revision | Provider | Period | Total Return | CAGR | Max DD | LumiBot |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |
| v15 BotSpot Auto | `556e25a8-6aa1-4dee-8f64-b29cfd0347de` | v15 `b9224efe-bce2-43ff-9b85-390d80f08829` | `botspot_auto` | `2016-01-21` to `2026-04-16` | `4484.38%` | `45.32%` | `-40.89%` | `4.5.3` |
| v19 ThetaData | `10fcefe2-d4fc-4144-b978-883f12893c43` | v19 `daf149d3-314e-47e4-a611-7a5499cc25a4` | `theta_data` | `2016-01-21` to `2026-04-16` | `3665.96%` | `42.56%` | `-70.36%` | `4.5.3` |
| v19 BotSpot Auto | `80df1b9d-b09a-43f4-a061-a5b311feb0de` | v19 `daf149d3-314e-47e4-a611-7a5499cc25a4` | `botspot_auto` | `2016-01-21` to `2026-04-16` | `191.93%` | `11.04%` | `-41.31%` | `4.5.49` |
| v16 ThetaData | `218cca4b-d28a-4127-91b2-98f4b8019df0` | v16 `656ed75d-3730-4449-b24d-9a4388c208df` | `theta_data` | `2016-01-21` to `2026-04-16` | `3371.92%` | `41.43%` | `-70.34%` | not rechecked in this pass |
| v16 BotSpot Auto | `02d4ddfd-bc3e-4b4f-a673-b8c093c8b455` | v16 `656ed75d-3730-4449-b24d-9a4388c208df` | `botspot_auto` | `2016-01-21` to `2026-04-16` | `-8.75%` | `-0.89%` | `-12.77%` | not rechecked in this pass |
| v17 ThetaData | `ff4c4f29-6210-47b1-9901-0cb3c849d945` | v17 `5747e5bb-82b5-43b0-abc0-4108e9c168e7` | `theta_data` | `2016-01-21` to `2026-04-16` | `8424.40%` | `54.40%` | `-28.92%` | not rechecked in this pass |
| v18 ThetaData | `c4b0c46a-54ca-4a29-9aef-d502245caa73` | v18 `72c46288-d579-4ea6-934b-db0ea6dcf67a` | `theta_data` | `2016-01-21` to `2026-04-16` | `8968.44%` | `55.34%` | `-28.92%` | not rechecked in this pass |

All three primary checked settings files use the normal BotSpot production
remote cache shape: S3 read/write cache with `prod/cache`, version `v1`. The
settings files also show the production data downloader path via
`DATADOWNLOADER_BASE_URL`, but this note intentionally does not record the
private downloader hostname.

There is no existing completed v15 ThetaData run in the first 50 completed
strategy backtests returned by `list_backtests`, and that page contained the
complete strategy history returned by the tool (`total=30`).

## Trade Count Evidence

Trade counts were queried through `mcp__botspot.query_csv` against
`trades.csv`; the tool used the Parquet siblings for all these queries.

### v15 BotSpot Auto

Backtest `556e25a8-6aa1-4dee-8f64-b29cfd0347de`:

| Status | Side | Type | Count |
| --- | --- | --- | ---: |
| canceled | sell_to_close | stop | 54 |
| dividend | in | cash_event | 11 |
| fill | buy | market | 142 |
| fill | sell | market | 54 |
| fill | sell_to_close | stop | 21 |
| new | buy | market | 142 |
| new | sell | market | 54 |

This old high-return BotSpot Auto run had real stop fills (`21`), and the log
query found zero matches for:

- `No pandas bars`
- `No last price`

### v19 ThetaData

Backtest `10fcefe2-d4fc-4144-b978-883f12893c43`:

| Status | Side | Type | Count |
| --- | --- | --- | ---: |
| canceled | sell | stop | 250 |
| dividend | in | cash_event | 11 |
| fill | buy | market | 258 |
| fill | sell | market | 56 |
| fill | sell | stop | 22 |
| new | buy | market | 258 |
| new | sell | market | 56 |
| new | sell | stop | 251 |

The v19 ThetaData run had stop fills (`22`) and zero matches for:

- `No pandas bars`
- `No last price`

### v19 BotSpot Auto

Backtest `80df1b9d-b09a-43f4-a061-a5b311feb0de`:

| Status | Side | Type | Count |
| --- | --- | --- | ---: |
| canceled | sell | stop | 390 |
| dividend | in | cash_event | 11 |
| fill | buy | market | 196 |
| fill | sell | market | 62 |
| new | buy | market | 196 |
| new | sell | market | 62 |
| new | sell | stop | 196 |

This is the clearest current BotSpot Auto divergence. It created stop orders,
but there were no stop fills. The production log query found `195` matches for
the pattern:

```text
No pandas bars for TQQQ ... canceling stop
```

Recent examples from the production log include hourly after-hours timestamps
such as:

- `2025-11-28 19:00:00-05:00`
- `2026-01-27 17:00:00-05:00`
- `2026-01-27 18:00:00-05:00`
- `2026-04-09 17:00:00-04:00`
- `2026-04-09 18:00:00-04:00`
- `2026-04-09 19:00:00-04:00`

### v16 Provider Pair

v16 is useful because it shows an earlier provider split.

Backtest `218cca4b-d28a-4127-91b2-98f4b8019df0` (v16 ThetaData):

| Status | Side | Type | Count |
| --- | --- | --- | ---: |
| canceled | sell | stop | 142 |
| dividend | in | cash_event | 11 |
| fill | buy | market | 143 |
| fill | sell | market | 56 |
| fill | sell | stop | 22 |
| new | buy | market | 143 |
| new | sell | market | 56 |
| new | sell | stop | 143 |

The v16 ThetaData log query found zero matches for:

- `No pandas bars`
- `No last price`

Backtest `02d4ddfd-bc3e-4b4f-a673-b8c093c8b455` (v16 BotSpot Auto):

| Status | Side | Type | Count |
| --- | --- | --- | ---: |
| canceled | sell | stop | 6 |
| fill | buy | market | 6 |
| fill | sell | market | 2 |
| fill | sell | stop | 2 |
| new | buy | market | 6 |
| new | sell | market | 2 |
| new | sell | stop | 6 |

The v16 BotSpot Auto log query found `1,358` matches for:

```text
No last price for entry; skipping
```

That explains why v16 BotSpot Auto barely traded in that historical run. It is
not the exact same surface as current v19 BotSpot Auto, where `No last price`
is no longer the observed production symptom and stop cancellation is.

## Revision Behavior Evidence

The exact exported revision code is retained under:

- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/code/v15_main.py`
- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/code/v19_main.py`
- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/summaries/code_manifest.json`

The exported code manifest records:

- v15 SHA-256:
  `42343209dea4348c155d20ff6fc4d0be041419919073727fd71ba5883b71d0dc`
- v19 SHA-256:
  `eb44c58b9bc1a754902477ee8a997d16ed242a099fa0d33dad9bca0edc130951`

### v15

v15 has date-based gating:

```python
current_date = latest_dt.date()
if self.vars.cached_last_dt is not None and self.vars.cached_last_dt == current_date:
    self.log_message("No new daily bar; skipping redundant calculations for today", color="yellow")
    return
self.vars.cached_last_dt = current_date
```

v15 also creates OTO stop-loss entries directly:

```python
order_class=Order.OrderClass.OTO if stop_price else Order.OrderClass.SIMPLE,
secondary_stop_price=stop_price
```

### v16

v16 introduced the simple-order stop architecture:

- Submit a simple buy.
- Store `pending_stop_price`.
- In `on_filled_order`, submit a separate simple sell stop.
- Track and cancel `active_stop_order`.

v16 still had startup sync plus date-based gating and allowed entry on bullish
trend above EMA even without a fresh signal.

This aligns with the v16 ThetaData result: high return but `-70.34%` max
drawdown.

### v17 and v18

v17/v18 keep the simple-order stop architecture, but require a fresh bullish
CHoCH/BOS for entry. v18's code still contains startup-sync trend detection,
but its entry condition requires a fresh signal, so startup-sync alone does not
cause an immediate mid-trend entry.

The v18 ThetaData production log query found:

- `61` matches for `Entering long: fresh bullish signal`
- `0` matches for `Entering long via startup sync mid-trend`

That lines up with the improved v18 ThetaData result:

- `55.34%` CAGR
- `-28.92%` max drawdown

### v19

v19 re-allowed startup-sync / mid-trend entry:

```python
if self.vars.trend == 1 and above_ema and not has_position:
    ...
    reason = "fresh bullish signal" if has_fresh_signal else "startup sync mid-trend"
```

The v19 ThetaData production log query found:

- `58` matches for `Entering long via fresh bullish signal`
- `14` matches for `Entering long via startup sync mid-trend`

This is the most likely explanation for the v19 ThetaData drawdown regression:
v19 is no longer equivalent to v17/v18. It takes extra mid-trend entries that
v18 did not take.

## Local Replay Attempts

Local artifacts are under:

- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/`

### 2026-06-14 correction

The local smoke matrix for `2016-01-21` to `2016-03-01` completed for the four
requested scenario labels, but it must not be used as provider evidence.
Although the redacted manifests recorded labels such as `v15_theta_data`, every
completed child settings file recorded:

```text
backtesting_data_sources=ibkr
lumibot_version=4.5.52
```

The child stdout explains why:

```text
.env file loaded from: /Users/robertgrzesik/Development/lumibot/.env
.env.local file loaded from: /Users/robertgrzesik/Development/lumibot/.env.local
```

That `.env.local` currently sets `BACKTESTING_DATA_SOURCE=ibkr`, and the custom
runner did not set `LUMIBOT_DISABLE_DOTENV=1`. Result: the four local labels
were effectively IBKR runs, not a valid ThetaData vs BotSpot Auto matrix.

- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/summaries/matrix_2016-01-21_2016-03-01.json`

The full local replay attempts are not valid final comparison results:

- The current local checkout is on `version/4.5.52`, not the historical
  production `4.5.3` or the current deployed Bot Manager `4.5.49` used by the
  fresh v19 BotSpot Auto production run.
- The custom runner merged env from several local files instead of mirroring the
  BotSpot Node -> Bot Manager env chain.
- The local v15 ThetaData full attempt had `BACKTESTING_DATA_SOURCE=ThetaData`
  in its manifest, but the runtime loaded repo `.env`/`.env.local` and entered
  IBKR. That attempt was stopped and must not be counted as a valid v15
  ThetaData result.
- The local v15 BotSpot Auto full attempt also crawled because the current
  local broker guard repeatedly rejected selected minute bars that did not match
  the exact simulated hourly timestamp.

Representative local invalid-run paths:

- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/artifacts/v15_theta_data_2016-01-21_2026-04-16/manifest.redacted.json`
- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/artifacts/v15_theta_data_2016-01-21_2026-04-16/stdout.log`
- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/artifacts/v15_botspot_auto_2016-01-21_2026-04-16/stdout.log`

The local runner script created for this investigation is:

- `/Users/robertgrzesik/Development/lumibot/scripts/run_tqqq_provider_diff.py`

That script is useful for smoke and harness work, but it should not be treated
as production-equivalent. Use `scripts/run_backtest_prodlike.py` instead.

## Corrected Local Matrix Results

Completed on 2026-06-14 with:

- local LumiBot root: `/Users/robertgrzesik/Development/lumibot`
- local LumiBot version: `4.5.52`
- runner: `/Users/robertgrzesik/Development/lumibot/scripts/run_backtest_prodlike.py`
- `LUMIBOT_DISABLE_DOTENV=1`
- production Data Downloader env
- S3 cache: `lumibot-cache-prod/prod/cache/v1`, `readwrite`
- window: `2016-01-21` to `2026-04-16`

Durable artifact folder:

- `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_matrix_20260614_150508/`
- summary markdown: `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_matrix_20260614_150508/summary_matrix.md`
- summary JSON: `/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_matrix_20260614_150508/summary_matrix.json`

The smoke runs first verified that all child settings files recorded the
intended provider, local LumiBot `4.5.52`, no `.env` / `.env.local` loads, and
prod cache identifiers. Then the full matrix was run.

| Scenario | Provider recorded | Runtime | Total return | CAGR | Max DD | Stops new/cancel/fill | No pandas bars | Queue submits |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v15 ThetaData | `thetadata` | `130.2s` | `4851.00%` | `46.40%` | `-41.13%` | `0/55/20` | `0` | `0` |
| v15 BotSpot Auto | router JSON | `44.0s` | `4670.00%` | `45.87%` | `-40.89%` | `0/54/21` | `0` | `0` |
| v19 ThetaData | `thetadata` | `199.7s` | `3666.00%` | `42.54%` | `-70.36%` | `251/250/22` | `0` | `0` |
| v19 BotSpot Auto | router JSON | `99.4s` | `192.00%` | `11.03%` | `-41.31%` | `196/390/0` | `390` | `0` |

Important interpretation:

- The corrected local run confirms v15 is high-return on both ThetaData and
  BotSpot Auto when the provider wiring is correct.
- The corrected local run reproduces the v19 ThetaData high-return/high-drawdown
  profile.
- The corrected local run reproduces the v19 BotSpot Auto low-return profile and
  the IBKR stop-order symptom: stop orders are created, then canceled; there are
  zero stop fills and `390` `No pandas bars` log matches.
- No full scenario submitted downloader queue jobs. These were warm prod-cache
  replays, so this matrix validates provider/execution behavior, not downloader
  throughput under cache misses.

## Correct Local Replay Path

v15 ThetaData is runnable locally. The MCP `set_data_provider` limitation only
applies to cloud BotSpot/MCP starts from a session without a BotSpot
conversation binding; it is not a limitation of local LumiBot code.

For local replay:

1. Run the exported saved revision file directly with
   `/Users/robertgrzesik/Development/lumibot/scripts/run_backtest_prodlike.py`.
2. Pass `--lumibot-root /Users/robertgrzesik/Development/lumibot` so the child
   uses the local checkout, not the pip-installed LumiBot package.
3. Use `LUMIBOT_DISABLE_DOTENV=1` through the canonical runner so repo
   `.env.local` cannot overwrite the provider.
4. Pass `BACKTESTING_DATA_SOURCE` through `--data-source`:
   - ThetaData: `thetadata`
   - BotSpot Auto:
     `{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}`
5. Use a production-like dotenv/env source for downloader and S3 cache settings.
   This Mac's `botspot_node/.env-local` points at the dev cache namespace, so it
   is not exact production parity for the TQQQ comparison.
6. Verify the child `*_settings.json` before trusting the run. It must record
   the intended `backtesting_data_sources`; if it says `ibkr` for a ThetaData
   scenario, the run is invalid.

The cloud MCP issue observed during the production-artifact pass was:

```text
ConversationId is required to change data provider
```

That only means a revision-ID-only cloud MCP start from that session would have
defaulted to BotSpot Auto. It does not block a proper local replay.

Existing production history still does not contain a completed v15 ThetaData
full-window run for `2016-01-21` to `2026-04-16` in the queried BotSpot history.

## Current Working Theory

This investigation supports three separate findings:

1. The v19 ThetaData `-70.36%` max drawdown is most likely strategy behavior,
   not a ThetaData data outage. v19 reintroduced startup-sync mid-trend entries;
   v17/v18 did not take those entries and had about `-28.92%` max drawdown on
   ThetaData.
2. The current v19 BotSpot Auto/IBKR low-return result is a broker/data
   execution-bar issue around simple stop orders. The run creates stops, then
   cancels them because no pandas bars exist for after-hours hourly timestamps.
   That is why it has zero stop fills.
3. v16 BotSpot Auto showed an earlier provider-specific price issue
   (`1,358` `No last price for entry` skips), but the fresh v19 BotSpot Auto run
   no longer shows that exact symptom. Do not conflate the v16 missing-price
   failure with the v19 stop-cancel failure.

## Recommended Next Steps

1. Re-run the four requested local scenarios with the canonical prod-like
   runner, not `scripts/run_tqqq_provider_diff.py`: v15 ThetaData, v15 BotSpot
   Auto, v19 ThetaData, and v19 BotSpot Auto for `2016-01-21` to `2026-04-16`.
   Start with a short smoke window only to validate env/import/cache proof, then
   run the full window.
2. For the v19 ThetaData drawdown regression, compare v18 and v19 trade lists by
   entry reason. The first focus should be the `14` v19 `startup sync mid-trend`
   entries and their subsequent drawdown contribution.
3. For the v19 BotSpot Auto/IBKR low-return result, isolate the simple-stop
   cancellation path. The key invariant is: a submitted stop order should not be
   canceled merely because an after-hours hourly strategy timestamp lacks a
   matching pandas bar, unless that is explicitly intended behavior for that
   broker/data path.
4. Keep the fixes separate. A strategy revision fix may be needed to undo or
   narrow v19 mid-trend entry behavior. A LumiBot/BotSpot Auto fix may be needed
   for IBKR stop-order fill/cancel handling at after-hours hourly timestamps.
