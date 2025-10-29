"""AWS Lambda entrypoint for populating LumiBot backtest cache.

This function is invoked by :class:`lumibot.tools.backtest_cache.BacktestCacheManager`
when a local cache miss occurs and both the cache bucket and Lambda environment
variables are configured.  The payload schema matches the structure produced by
``lumibot.tools.thetadata_helper._build_lambda_payload`` and is intentionally
provider-agnostic so additional data vendors can be hooked in later.

For now, only the ``thetadata`` provider is implemented.  The handler requests
data from the ThetaData v2 REST API, normalises it to the same parquet structure
produced by the local cache writer, and uploads the result to the configured S3
bucket at the cache key supplied in the invocation payload.

Environment variables
---------------------

``BACKTEST_CACHE_BUCKET``
    Name of the S3 bucket that stores backtest cache artefacts (required).

``BACKTEST_CACHE_REGION``
    Optional AWS region hint for the boto3 client.

``THETADATA_USERNAME`` / ``THETADATA_PASSWORD``
    Credentials for the ThetaData REST API (required for thetadata provider).

``THETADATA_BASE_URL``
    Override for the ThetaData REST endpoint.  Defaults to "https://api.thetadata.net".

``THETADATA_HTTP_TIMEOUT``
    Optional request timeout in seconds (float).  Defaults to 30 seconds.

``THETADATA_ALLOW_EMPTY``
    When set to a truthy string, the Lambda will treat empty responses as a
    successful invocation (the parquet file is *not* written).  By default an
    empty response is considered a failure so the caller can decide how to
    proceed.

The module purposefully contains no LumiBot runtime imports in order to keep the
deployment package small and avoid circular dependencies.  Only the payload
contract ties the Lambda to LumiBot.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import boto3
import pandas as pd
import pytz
import requests


LOGGER = logging.getLogger(__name__)
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


EASTERN = pytz.timezone("America/New_York")
UTC = pytz.UTC

TIMESPAN_TO_MS: Dict[str, int] = {
    "second": 1_000,
    "minute": 60_000,
    "5minute": 300_000,
    "10minute": 600_000,
    "15minute": 900_000,
    "30minute": 1_800_000,
    "hour": 3_600_000,
    "2hour": 7_200_000,
    "4hour": 14_400_000,
}


class CacheError(RuntimeError):
    """Raised when the cache population process encounters a fatal error."""


@dataclass
class CacheAsset:
    """Minimal representation of an asset description passed from LumiBot."""

    symbol: str
    asset_type: str
    expiration: Optional[date] = None
    strike: Optional[float] = None
    right: Optional[str] = None
    multiplier: Optional[int] = None
    precision: Optional[str] = None


def handler(event: Any, _context: Any = None) -> Dict[str, Any]:
    """AWS Lambda entrypoint.

    Parameters
    ----------
    event:
        Either a dictionary or a JSON string containing the cache payload.
    _context:
        Lambda context (unused).

    Returns
    -------
    dict
        JSON-serialisable response with a ``success`` flag and metadata.
    """

    try:
        payload = _coerce_event(event)
        LOGGER.info("Received cache request: provider=%s key=%s", payload.get("provider"), payload.get("cache_key"))

        bucket = os.environ.get("BACKTEST_CACHE_BUCKET")
        if not bucket:
            raise CacheError("BACKTEST_CACHE_BUCKET environment variable is required")

        cache_key = payload.get("cache_key")
        if not cache_key or not isinstance(cache_key, str):
            raise CacheError("Invocation payload missing 'cache_key' string")

        provider = payload.get("provider")
        if provider != "thetadata":
            raise CacheError(f"Unsupported provider '{provider}'")

        dataset = _handle_thetadata(payload)

        if dataset is None or dataset.empty:
            if _allow_empty_responses():
                LOGGER.info("ThetaData returned no rows; skipping upload by configuration")
                return {
                    "success": True,
                    "provider": provider,
                    "cache_key": cache_key,
                    "rows": 0,
                    "skipped": True,
                    "message": "No data returned by provider; upload skipped.",
                }
            raise CacheError("ThetaData returned no data for the requested range")

        local_path = _write_parquet_to_tmp(dataset)
        _upload_to_s3(local_path, bucket, cache_key)

        return {
            "success": True,
            "provider": provider,
            "cache_key": cache_key,
            "rows": int(len(dataset)),
            "message": "Cache object written to S3",
        }

    except CacheError as exc:
        LOGGER.error("Cache population failed: %s", exc)
        return {
            "success": False,
            "message": str(exc),
        }
    except Exception as exc:  # pragma: no cover - defensive guard
        LOGGER.exception("Unhandled cache population error")
        return {
            "success": False,
            "message": f"Unhandled error: {exc}",
        }


def _coerce_event(event: Any) -> Dict[str, Any]:
    """Ensure the incoming payload is a dictionary."""

    if isinstance(event, dict):
        return event
    if isinstance(event, str):
        return json.loads(event)
    raise CacheError("Invocation event must be a dict or JSON string")


def _handle_thetadata(payload: Dict[str, Any]) -> pd.DataFrame:
    """Fetch and normalise ThetaData content according to the cache payload."""

    asset = _parse_asset(payload.get("asset"))
    quote_asset_payload = payload.get("quote_asset")
    quote_asset = _parse_asset(quote_asset_payload) if quote_asset_payload else None

    start = _parse_datetime(payload.get("start"))
    end = _parse_datetime(payload.get("end"))
    if start > end:
        raise CacheError("'start' must be on or before 'end'")

    timespan = payload.get("timespan", "minute")
    datastyle = payload.get("datastyle", "ohlc")
    include_after_hours = bool(payload.get("include_after_hours", True))

    LOGGER.info(
        "ThetaData request: asset=%s timespan=%s datastyle=%s start=%s end=%s include_after_hours=%s",
        asset,
        timespan,
        datastyle,
        start,
        end,
        include_after_hours,
    )

    if timespan == "day":
        df = _thetadata_eod(asset, start, end, datastyle)
    else:
        interval_ms = TIMESPAN_TO_MS.get(timespan)
        if interval_ms is None:
            raise CacheError(f"Unsupported ThetaData timespan '{timespan}'")
        df = _thetadata_intraday(asset, start, end, interval_ms, datastyle, include_after_hours, quote_asset)

    if df is None or df.empty:
        return df

    df = df.sort_index()
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize(EASTERN)
    df = df.tz_convert(UTC)

    # Reset to match LumiBot cache format: datetime column + data columns.
    df_reset = df.reset_index().rename(columns={df.index.name or "index": "datetime"})
    df_reset["datetime"] = pd.to_datetime(df_reset["datetime"], utc=True)

    return df_reset


def _thetadata_intraday(
    asset: CacheAsset,
    start: datetime,
    end: datetime,
    interval_ms: int,
    datastyle: str,
    include_after_hours: bool,
    quote_asset: Optional[CacheAsset] = None,
) -> pd.DataFrame:
    """Retrieve intraday bars or quotes from ThetaData."""

    params = _common_query_params(asset, start, end)
    params["ivl"] = interval_ms

    if asset.asset_type == "option":
        if asset.expiration is None or asset.strike is None or asset.right is None:
            raise CacheError("Option request requires expiration, strike, and right")
        params.update(
            {
                "exp": asset.expiration.strftime("%Y%m%d"),
                "strike": int(round(asset.strike * 1000)),
                "right": "C" if asset.right.upper().startswith("C") else "P",
            }
        )
    elif asset.asset_type not in {"index", "crypto"}:
        params["rth"] = "false" if include_after_hours else "true"

    if quote_asset and asset.asset_type == "forex":  # placeholder for future brokers
        params["quote_root"] = quote_asset.symbol

    endpoint = _thetadata_endpoint(asset.asset_type, datastyle)
    response = _call_thetadata(endpoint, params)
    if response is None:
        return pd.DataFrame()

    columns = response["header"].get("format", [])
    data = response.get("response", [])
    df = pd.DataFrame(data, columns=columns)
    if df.empty:
        return df

    if "quote" in datastyle.lower():
        keep_mask = (df.get("bid_size", 0) != 0) | (df.get("ask_size", 0) != 0)
        df = df[keep_mask]
    elif asset.asset_type != "index" and "count" in df.columns:
        df = df[df["count"] != 0]

    if df.empty:
        return df

    df["datetime"] = df.apply(_combine_date_ms, axis=1)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime")
    df = df.drop(columns=[col for col in ("ms_of_day", "date") if col in df.columns])

    return df


def _thetadata_eod(asset: CacheAsset, start: datetime, end: datetime, datastyle: str) -> pd.DataFrame:
    """Retrieve end-of-day data from ThetaData."""

    params = _common_query_params(asset, start, end)
    if asset.asset_type == "option":
        if asset.expiration is None or asset.strike is None or asset.right is None:
            raise CacheError("Option request requires expiration, strike, and right")
        params.update(
            {
                "exp": asset.expiration.strftime("%Y%m%d"),
                "strike": int(round(asset.strike * 1000)),
                "right": "C" if asset.right.upper().startswith("C") else "P",
            }
        )

    endpoint = _thetadata_endpoint(asset.asset_type, "eod")
    response = _call_thetadata(endpoint, params)
    if response is None:
        return pd.DataFrame()

    columns = response["header"].get("format", [])
    df = pd.DataFrame(response.get("response", []), columns=columns)
    if df.empty:
        return df

    df["datetime"] = df.apply(_combine_eod_date, axis=1)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df = df.set_index("datetime")

    # Drop columns the local cache does not persist.
    drop_cols = [
        "ms_of_day",
        "ms_of_day2",
        "date",
        "bid_size",
        "bid_exchange",
        "bid",
        "bid_condition",
        "ask_size",
        "ask_exchange",
        "ask",
        "ask_condition",
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    if asset.asset_type in {"stock", "option"}:
        # Align open price with the official 9:30 auction by fetching the first minute.
        minute_df = _thetadata_intraday(
            asset,
            start,
            end,
            interval_ms=TIMESPAN_TO_MS["minute"],
            datastyle="ohlc",
            include_after_hours=False,
        )
        if minute_df is not None and not minute_df.empty:
            minute_df = minute_df.sort_index()
            if minute_df.index.tzinfo is None:
                minute_df.index = minute_df.index.tz_localize(EASTERN)
            minute_df.index = minute_df.index.tz_convert(UTC)
            minute_df["date"] = minute_df.index.date
            for idx in df.index:
                day_minutes = minute_df[minute_df["date"] == idx.date()]
                if not day_minutes.empty and "open" in df.columns:
                    df.loc[idx, "open"] = day_minutes.iloc[0]["open"]

    return df


def _common_query_params(asset: CacheAsset, start: datetime, end: datetime) -> Dict[str, Any]:
    """Build baseline ThetaData query parameters."""

    start_et = start.astimezone(EASTERN)
    end_et = end.astimezone(EASTERN)

    return {
        "root": asset.symbol,
        "start_date": start_et.strftime("%Y%m%d"),
        "end_date": end_et.strftime("%Y%m%d"),
    }


def _thetadata_endpoint(asset_type: str, datastyle: str) -> str:
    """Resolve the ThetaData REST endpoint for the request."""

    base_url = os.environ.get("THETADATA_BASE_URL", "https://api.thetadata.net")
    asset_type = asset_type.lower()
    datastyle = datastyle.lower()

    if datastyle == "eod":
        return f"{base_url}/v2/hist/{asset_type}/eod"
    return f"{base_url}/v2/hist/{asset_type}/{datastyle}"


def _call_thetadata(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Issue a ThetaData REST request with pagination handling."""

    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")
    if not username or not password:
        raise CacheError("ThetaData credentials (THETADATA_USERNAME/PASSWORD) are required")

    timeout = float(os.environ.get("THETADATA_HTTP_TIMEOUT", "30"))

    responses: List[List[Any]] = []
    header: Optional[Dict[str, Any]] = None
    next_page: Optional[str] = None

    session = requests.Session()

    while True:
        request_url = next_page or url
        request_params = None if next_page else params
        LOGGER.debug("ThetaData GET %s params=%s", request_url, request_params)

        resp = session.get(request_url, params=request_params, timeout=timeout, auth=(username, password))
        if resp.status_code == 472:
            LOGGER.info("ThetaData reported no data for params %s", params)
            session.close()
            return None
        if resp.status_code != 200:
            session.close()
            raise CacheError(f"ThetaData request failed with status {resp.status_code}: {resp.text[:200]}")

        payload = resp.json()
        header = payload.get("header")
        error_type = header.get("error_type") if header else None
        if error_type and error_type != "null":
            if error_type == "NO_DATA":
                LOGGER.info("ThetaData reported NO_DATA for params %s", params)
                session.close()
                return None
            session.close()
            raise CacheError(f"ThetaData error {error_type}: {header}")

        responses.append(payload.get("response", []))
        next_page = header.get("next_page") if header else None
        if not next_page or next_page in ("", "null"):
            break

    session.close()

    combined = []
    for page in responses:
        combined.extend(page)

    return {
        "header": header or {},
        "response": combined,
    }


def _combine_date_ms(row: pd.Series) -> datetime:
    """Combine ThetaData date/ms_of_day columns into a timezone-naive datetime."""

    base_date = datetime.strptime(str(int(row["date"])), "%Y%m%d")
    ms_of_day = int(row["ms_of_day"])
    return base_date + timedelta(milliseconds=ms_of_day)


def _combine_eod_date(row: pd.Series) -> datetime:
    """Convert ThetaData EOD rows into UTC datetimes."""

    base_date = datetime.strptime(str(int(row["date"])), "%Y%m%d")
    return UTC.localize(base_date)


def _parse_asset(data: Dict[str, Any]) -> CacheAsset:
    """Create a :class:`CacheAsset` from the payload dictionary."""

    if not isinstance(data, dict):
        raise CacheError("Asset payload must be a dictionary")

    expiration = data.get("expiration")
    if expiration:
        expiration_date = datetime.fromisoformat(expiration).date()
    else:
        expiration_date = None

    strike = data.get("strike")
    strike_value = float(strike) if strike is not None else None

    right = data.get("right")
    if right:
        right = right.upper()

    multiplier = data.get("multiplier")
    multiplier_value = int(multiplier) if multiplier is not None else None

    precision = data.get("precision")

    symbol = data.get("symbol")
    if not symbol:
        raise CacheError("Asset payload missing 'symbol'")

    asset_type = data.get("asset_type", "stock")
    if not isinstance(asset_type, str):
        raise CacheError("Asset 'asset_type' must be a string")

    return CacheAsset(
        symbol=str(symbol),
        asset_type=asset_type.lower(),
        expiration=expiration_date,
        strike=strike_value,
        right=right,
        multiplier=multiplier_value,
        precision=precision,
    )


def _parse_datetime(value: Any) -> datetime:
    """Parse an ISO 8601 datetime string into an aware UTC datetime."""

    if not value:
        raise CacheError("Datetime value missing in payload")
    if isinstance(value, datetime):
        dt_value = value
    else:
        dt_value = datetime.fromisoformat(str(value))

    if dt_value.tzinfo is None:
        dt_value = UTC.localize(dt_value)
    else:
        dt_value = dt_value.astimezone(UTC)

    return dt_value


def _write_parquet_to_tmp(df: pd.DataFrame) -> Path:
    """Persist the DataFrame to a parquet file under /tmp and return the path."""

    tmp_dir = Path("/tmp/lumibot_cache")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / "cache.parquet"
    df.to_parquet(tmp_path, engine="pyarrow", compression="snappy")
    LOGGER.info("Wrote parquet to %s (%d rows)", tmp_path, len(df))
    return tmp_path


def _upload_to_s3(local_path: Path, bucket: str, key: str) -> None:
    """Upload the local parquet file to S3."""

    session_kwargs: Dict[str, Any] = {}
    region = os.environ.get("BACKTEST_CACHE_REGION")
    if region:
        session_kwargs["region_name"] = region

    s3 = boto3.client("s3", **session_kwargs)

    LOGGER.info("Uploading %s to s3://%s/%s", local_path, bucket, key)
    s3.upload_file(str(local_path), bucket, key)


def _allow_empty_responses() -> bool:
    """Return True when empty provider responses should be treated as success."""

    value = os.environ.get("THETADATA_ALLOW_EMPTY", "")
    return value.lower() in {"1", "true", "yes", "on"}


__all__ = ["handler"]
