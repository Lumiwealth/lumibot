# Remote Cache Modes

This document captures the design and operational details for the backtest cache
manager that synchronises local parquet files with AWS S3. The initial scope
supports ThetaData caches with two selectable modes:

* `disabled` – the default behaviour; caching remains purely local.
* `s3_readwrite` – reads existing cache objects from S3 and uploads local writes.

`s3_readonly` will be introduced later when the Lambda writer is ready; a TODO
placeholder lives in the code so the follow-on work has a defined landing spot.

## Environment Variables

All variables are loaded by `lumibot.credentials.CACHE_REMOTE_CONFIG`. They can
live in `.env` or be injected via standard environment configuration. Example:

```
LUMIBOT_CACHE_BACKEND=s3
LUMIBOT_CACHE_MODE=readwrite
LUMIBOT_CACHE_S3_BUCKET=lumibot-cache-prod
LUMIBOT_CACHE_S3_PREFIX=prod/cache
LUMIBOT_CACHE_S3_REGION=us-east-1
LUMIBOT_CACHE_S3_ACCESS_KEY_ID=AKIAEXAMPLE
LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY=secretExampleKey
LUMIBOT_CACHE_S3_SESSION_TOKEN=optional-session-token
LUMIBOT_CACHE_S3_VERSION=v1
```

Notes:

* Credentials are optional; omit them if the runtime already has IAM credentials
  (for example an ECS task role).
* Prefix and version are combined when building remote keys. Leaving the prefix
  blank stores objects at `v1/<relative path>`. Use versioning to partition data
  when placeholder semantics or schema changes.
* Encryption, ACLs, and lifecycle policies remain the responsibility of the
  bucket configuration so we avoid adding client-side overhead.

## Remote Key Scheme

`BacktestCacheManager.remote_key_for` mirrors the local cache path structure:

```
<prefix>/<version>/<relative path under LUMIBOT_CACHE_FOLDER>
```

Example ThetaData quote cache on macOS:

```
/Users/<user>/Library/Caches/lumibot/1.0/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet
```

With the example configuration above the remote key becomes:

```
prod/cache/v1/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet
```

This format aligns with the intended IAM policy layout (provider → asset class →
timespan → datastyle) and keeps migration straightforward for other data sources
such as Polygon or DataBento. Option-chain caches now live at
`thetadata/option/option_chains/<symbol>_<date>.parquet`.

## Implementation Overview

* `BacktestCacheSettings.from_env` validates the env contract and resolves
  defaults.
* `BacktestCacheManager` lazily instantiates an S3 client, downloading files on
  demand (`ensure_local_file`) and uploading after cache writes
  (`on_local_update`).
* `thetadata_helper.get_price_data` calls the manager before attempting to read
  from disk, and again after successful cache updates. Payload metadata captures
  provider, symbol, asset type, and option attributes for future auditing.
* Remote uploads run only in `s3_readwrite` mode. The read-only path returns
  early, leaving a TODO hook (`BacktestCacheManager.on_local_update`) for the
  Lambda-triggered workflow.

## Testing Strategy

Automated coverage lives in:

* `tests/test_backtest_cache_manager.py` – validates configuration parsing,
  remote key construction, and stubbed download/upload flows without requiring
  boto3.
* `tests/test_thetadata_helper.py::test_get_price_data_invokes_remote_cache_manager`
  – sanity-checks the ThetaData integration, ensuring we attempt remote fetches
  for cache hits and avoid uploads when no new data is written.

These tests run without real AWS credentials thanks to dependency injection of
stub clients.

## Manual Validation Checklist

1. Provision an S3 bucket with the desired prefix and (optionally) versioning.
2. Export the environment variables listed above or add them to `.env`.
3. Warm the ThetaData cache locally (for example `./run_cache_validation.sh`).
4. Run a cold backtest that touches ThetaData data – e.g.
   `pytest tests/test_thetadata_pandas_verification.py::test_pandas_cold_warm -m apitest`
   – then verify the S3 bucket contains the new parquet files under the expected
   key structure.
5. Re-run the same test to confirm warm execution produces zero ThetaData
   network calls and that no additional uploads occur.
6. (Optional) Inspect the logs tagged `[THETA][DEBUG][CACHE]` for remote download
   or upload lines that confirm the S3 path in use.

Keep the cache folder small during manual tests to avoid large uploads; the
largest observed parquet files are ~2.5 MB (Polygon) and ~500 KB (ThetaData),
well within single-part transfer thresholds.

## Future Enhancements

* `s3_readonly` mode: delegate cache misses to the proprietary Lambda and block
  direct uploads from backtest jobs.
* Extend manager usage to Polygon and DataBento helpers once confidence with the
  ThetaData pipeline is established.
* Consider a tooling script that compares local vs remote cache hashes to help
  operators audit divergence.
