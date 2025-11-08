# This file contains helper functions for getting data from Polygon.io
import time
import os
import signal
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import pytz
import pandas as pd
import pandas_market_calendars as mcal
import requests
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.lumibot_logger import get_logger
from lumibot.entities import Asset
from tqdm import tqdm
from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache

logger = get_logger(__name__)

WAIT_TIME = 60
MAX_DAYS = 30
CACHE_SUBFOLDER = "thetadata"
BASE_URL = "http://127.0.0.1:25510"
CONNECTION_RETRY_SLEEP = 1.0
CONNECTION_MAX_RETRIES = 60
BOOT_GRACE_PERIOD = 5.0
MAX_RESTART_ATTEMPTS = 3
MAX_TERMINAL_RESTART_CYCLES = 3


def _resolve_asset_folder(asset_obj: Asset) -> str:
    asset_type = getattr(asset_obj, "asset_type", None) or "stock"
    asset_key = str(asset_type).strip().lower()
    return asset_key


def _normalize_folder_component(value: str, fallback: str) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    return normalized or fallback

# Global process tracking for ThetaTerminal
THETA_DATA_PROCESS = None
THETA_DATA_PID = None
THETA_DATA_LOG_HANDLE = None


class ThetaDataConnectionError(RuntimeError):
    """Raised when ThetaTerminal cannot reconnect to Theta Data after multiple restarts."""

    pass

def reset_connection_diagnostics():
    """Reset ThetaData connection counters (useful for tests)."""
    CONNECTION_DIAGNOSTICS.update({
        "check_connection_calls": 0,
        "start_terminal_calls": 0,
        "network_requests": 0,
        "placeholder_writes": 0,
        "terminal_restarts": 0,
    })


def ensure_missing_column(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Ensure the dataframe includes a `missing` flag column (True for placeholders)."""
    if df is None or len(df) == 0:
        return df
    if "missing" not in df.columns:
        df["missing"] = False
        logger.debug(
            "[THETA][DEBUG][THETADATA-CACHE] added 'missing' column to frame (rows=%d)",
            len(df),
        )
    return df


def restore_numeric_dtypes(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Try to convert object columns back to numeric types after placeholder removal."""
    if df is None or len(df) == 0:
        return df
    for column in df.columns:
        if df[column].dtype == object:
            try:
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                continue
    return df


def append_missing_markers(
    df_all: Optional[pd.DataFrame],
    missing_dates: List[datetime.date],
) -> Optional[pd.DataFrame]:
    """Append placeholder rows for dates that returned no data."""
    if not missing_dates:
        if df_all is not None and not df_all.empty and "missing" in df_all.columns:
            df_all = df_all[~df_all["missing"].astype(bool)].drop(columns=["missing"])
            df_all = restore_numeric_dtypes(df_all)
        return df_all

    base_columns = ["open", "high", "low", "close", "volume"]

    if df_all is None or len(df_all) == 0:
        df_all = pd.DataFrame(columns=base_columns + ["missing"])
        df_all.index = pd.DatetimeIndex([], name="datetime")

    df_all = ensure_missing_column(df_all)

    rows = []
    for d in missing_dates:
        dt = datetime(d.year, d.month, d.day, tzinfo=pytz.UTC)
        row = {col: pd.NA for col in df_all.columns if col != "missing"}
        row["datetime"] = dt
        row["missing"] = True
        rows.append(row)

    if rows:
        CONNECTION_DIAGNOSTICS["placeholder_writes"] = CONNECTION_DIAGNOSTICS.get("placeholder_writes", 0) + len(rows)

        # DEBUG-LOG: Placeholder injection
        logger.debug(
            "[THETA][DEBUG][PLACEHOLDER][INJECT] count=%d dates=%s",
            len(rows),
            ", ".join(sorted({d.isoformat() for d in missing_dates}))
        )

        placeholder_df = pd.DataFrame(rows).set_index("datetime")
        for col in df_all.columns:
            if col not in placeholder_df.columns:
                placeholder_df[col] = pd.NA if col != "missing" else True
        placeholder_df = placeholder_df[df_all.columns]
        if len(df_all) == 0:
            df_all = placeholder_df
        else:
            df_all = pd.concat([df_all, placeholder_df]).sort_index()
        df_all = df_all[~df_all.index.duplicated(keep="last")]
        logger.debug(
            "[THETA][DEBUG][THETADATA-CACHE] recorded %d placeholder day(s): %s",
            len(rows),
            ", ".join(sorted({d.isoformat() for d in missing_dates})),
        )

    return df_all


def remove_missing_markers(
    df_all: Optional[pd.DataFrame],
    available_dates: List[datetime.date],
) -> Optional[pd.DataFrame]:
    """Drop placeholder rows when real data becomes available."""
    if df_all is None or len(df_all) == 0 or not available_dates:
        return df_all

    df_all = ensure_missing_column(df_all)
    available_set = set(available_dates)

    mask = df_all["missing"].eq(True) & df_all.index.map(
        lambda ts: ts.date() in available_set
    )
    if mask.any():
        removed_dates = sorted({ts.date().isoformat() for ts in df_all.index[mask]})
        df_all = df_all.loc[~mask]
        logger.debug(
            "[THETA][DEBUG][THETADATA-CACHE] cleared %d placeholder row(s) for dates: %s",
            mask.sum(),
            ", ".join(removed_dates),
        )

    return df_all


def _clamp_option_end(asset: Asset, dt: datetime) -> datetime:
    """Ensure intraday pulls for options never extend beyond expiration."""
    if isinstance(dt, datetime):
        end_dt = dt
    else:
        end_dt = datetime.combine(dt, datetime.max.time())

    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=pytz.UTC)

    if asset.asset_type == "option" and asset.expiration:
        expiration_dt = datetime.combine(asset.expiration, datetime.max.time())
        expiration_dt = expiration_dt.replace(tzinfo=end_dt.tzinfo)
        if end_dt > expiration_dt:
            return expiration_dt

    return end_dt


def reset_theta_terminal_tracking():
    """Clear cached ThetaTerminal process references."""
    global THETA_DATA_PROCESS, THETA_DATA_PID, THETA_DATA_LOG_HANDLE
    THETA_DATA_PROCESS = None
    THETA_DATA_PID = None
    if THETA_DATA_LOG_HANDLE is not None:
        try:
            THETA_DATA_LOG_HANDLE.close()
        except Exception:
            pass
    THETA_DATA_LOG_HANDLE = None


CONNECTION_DIAGNOSTICS = {
    "check_connection_calls": 0,
    "start_terminal_calls": 0,
    "network_requests": 0,
    "placeholder_writes": 0,
    "terminal_restarts": 0,
}


def get_price_data(
    username: str,
    password: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Asset = None,
    dt=None,
    datastyle: str = "ohlc",
    include_after_hours: bool = True,
    return_polars: bool = False
) -> Optional[pd.DataFrame]:
    """
    Queries ThetaData for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/{CACHE_SUBFOLDER} folder so that it can be reused later and we don't have to query
    ThetaData every time we run a backtest.

    Returns pandas DataFrames for backwards compatibility. Polars output is not
    currently supported; callers requesting polars will receive a ValueError.

    Parameters
    ----------
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    asset : Asset
        The asset we are getting data for
    start : datetime
        The start date/time for the data we want
    end : datetime
        The end date/time for the data we want
    timespan : str
        The timespan for the data we want. Default is "minute" but can also be "second", "hour", "day", "week",
        "month", "quarter"
    quote_asset : Asset
        The quote asset for the asset we are getting data for. This is only needed for Forex assets.
    datastyle : str
        The style of data to retrieve ("ohlc" or "quote")
    include_after_hours : bool
        Whether to include after-hours trading data (default True)
    return_polars : bool
        ThetaData currently supports pandas output only. Passing True raises a ValueError.

    Returns
    -------
    Optional[pd.DataFrame]
        A pandas DataFrame with the pricing data for the asset

    """
    import pytz  # Import at function level to avoid scope issues in nested calls

    # DEBUG-LOG: Entry point for ThetaData request
    logger.debug(
        "[THETA][DEBUG][REQUEST][ENTRY] asset=%s quote=%s start=%s end=%s dt=%s timespan=%s datastyle=%s include_after_hours=%s return_polars=%s",
        asset,
        quote_asset,
        start.isoformat() if hasattr(start, 'isoformat') else start,
        end.isoformat() if hasattr(end, 'isoformat') else end,
        dt.isoformat() if dt and hasattr(dt, 'isoformat') else dt,
        timespan,
        datastyle,
        include_after_hours,
        return_polars
    )

    if return_polars:
        raise ValueError("ThetaData polars output is not available; pass return_polars=False.")

    # Preserve original bounds for final filtering
    requested_start = start
    requested_end = end

    # Check if we already have data for this asset in the cache file
    df_all = None
    df_cached = None
    cache_file = build_cache_filename(asset, timespan, datastyle)
    remote_payload = build_remote_cache_payload(asset, timespan, datastyle)
    cache_manager = get_backtest_cache()

    if cache_manager.enabled:
        try:
            fetched_remote = cache_manager.ensure_local_file(cache_file, payload=remote_payload)
            if fetched_remote:
                logger.debug(
                    "[THETA][DEBUG][CACHE][REMOTE_DOWNLOAD] asset=%s timespan=%s datastyle=%s cache_file=%s",
                    asset,
                    timespan,
                    datastyle,
                    cache_file,
                )
        except Exception as exc:
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_DOWNLOAD_ERROR] asset=%s cache_file=%s error=%s",
                asset,
                cache_file,
                exc,
            )

    # DEBUG-LOG: Cache file check
    logger.debug(
        "[THETA][DEBUG][CACHE][CHECK] asset=%s timespan=%s datastyle=%s cache_file=%s exists=%s",
        asset,
        timespan,
        datastyle,
        cache_file,
        cache_file.exists()
    )

    if cache_file.exists():
        logger.debug(
            "\nLoading '%s' pricing data for %s / %s with '%s' timespan from cache file...",
            datastyle,
            asset,
            quote_asset,
            timespan,
        )
        df_cached = load_cache(cache_file)
        if df_cached is not None and not df_cached.empty:
            df_all = df_cached.copy() # Make a copy so we can check the original later for differences

    cached_rows = 0 if df_all is None else len(df_all)
    placeholder_rows = 0
    if df_all is not None and not df_all.empty and "missing" in df_all.columns:
        placeholder_rows = int(df_all["missing"].sum())

    # DEBUG-LOG: Cache load result
    logger.debug(
        "[THETA][DEBUG][CACHE][LOADED] asset=%s cached_rows=%d placeholder_rows=%d real_rows=%d",
        asset,
        cached_rows,
        placeholder_rows,
        cached_rows - placeholder_rows
    )

    logger.debug(
        "[THETA][DEBUG][THETADATA-CACHE] pre-fetch rows=%d placeholders=%d for %s %s %s",
        cached_rows,
        placeholder_rows,
        asset,
        timespan,
        datastyle,
    )

    # Check if we need to get more data
    logger.debug(
        "[THETA][DEBUG][CACHE][DECISION_START] asset=%s | "
        "calling get_missing_dates(start=%s, end=%s)",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        start.isoformat() if hasattr(start, 'isoformat') else start,
        end.isoformat() if hasattr(end, 'isoformat') else end
    )

    missing_dates = get_missing_dates(df_all, asset, start, end)

    logger.debug(
        "[THETA][DEBUG][CACHE][DECISION_RESULT] asset=%s | "
        "missing_dates=%d | "
        "decision=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        len(missing_dates),
        "CACHE_HIT" if not missing_dates else "CACHE_MISS"
    )

    cache_file = build_cache_filename(asset, timespan, datastyle)
    logger.debug(
        "[THETA][DEBUG][THETADATA-CACHE] asset=%s/%s timespan=%s datastyle=%s cache_file=%s exists=%s missing=%d",
        asset,
        quote_asset.symbol if quote_asset else None,
        timespan,
        datastyle,
        cache_file,
        cache_file.exists(),
        len(missing_dates),
    )
    if not missing_dates:
        if df_all is not None and not df_all.empty:
            logger.debug("ThetaData cache HIT for %s %s %s (%d rows).", asset, timespan, datastyle, len(df_all))
            # DEBUG-LOG: Cache hit
            logger.debug(
                "[THETA][DEBUG][CACHE][HIT] asset=%s timespan=%s datastyle=%s rows=%d start=%s end=%s",
                asset,
                timespan,
                datastyle,
                len(df_all),
                start.isoformat() if hasattr(start, 'isoformat') else start,
                end.isoformat() if hasattr(end, 'isoformat') else end
            )
        # Filter cached data to requested date range before returning
        if df_all is not None and not df_all.empty:
            # For daily data, use date-based filtering (timestamps vary by provider)
            # For intraday data, use precise datetime filtering
            if timespan == "day":
                # Convert index to dates for comparison
                df_dates = pd.to_datetime(df_all.index).date
                start_date = start.date() if hasattr(start, 'date') else start
                end_date = end.date() if hasattr(end, 'date') else end
                mask = (df_dates >= start_date) & (df_dates <= end_date)
                df_all = df_all[mask]
            else:
                # Intraday: use precise datetime filtering
                import datetime as datetime_module  # RENAMED to avoid shadowing dt parameter!

                # DEBUG-LOG: Entry to intraday filter
                rows_before_any_filter = len(df_all)
                max_ts_before_any_filter = df_all.index.max() if len(df_all) > 0 else None
                logger.debug(
                    "[THETA][DEBUG][FILTER][INTRADAY_ENTRY] asset=%s | "
                    "rows_before=%d max_ts_before=%s | "
                    "start_param=%s end_param=%s dt_param=%s dt_type=%s",
                    asset.symbol if hasattr(asset, 'symbol') else str(asset),
                    rows_before_any_filter,
                    max_ts_before_any_filter.isoformat() if max_ts_before_any_filter else None,
                    start.isoformat() if hasattr(start, 'isoformat') else start,
                    end.isoformat() if hasattr(end, 'isoformat') else end,
                    dt.isoformat() if dt and hasattr(dt, 'isoformat') else dt,
                    type(dt).__name__ if dt else None
                )

                # Convert date to datetime if needed
                if isinstance(start, datetime_module.date) and not isinstance(start, datetime_module.datetime):
                    start = datetime_module.datetime.combine(start, datetime_module.time.min)
                    logger.debug(
                        "[THETA][DEBUG][FILTER][DATE_CONVERSION] converted start from date to datetime: %s",
                        start.isoformat()
                    )
                if isinstance(end, datetime_module.date) and not isinstance(end, datetime_module.datetime):
                    end = datetime_module.datetime.combine(end, datetime_module.time.max)
                    logger.debug(
                        "[THETA][DEBUG][FILTER][DATE_CONVERSION] converted end from date to datetime: %s",
                        end.isoformat()
                    )

                # Handle datetime objects with midnight time (users often pass datetime(YYYY, MM, DD))
                if isinstance(end, datetime_module.datetime) and end.time() == datetime_module.time.min:
                    # Convert end-of-period midnight to end-of-day
                    end = datetime_module.datetime.combine(end.date(), datetime_module.time.max)
                    logger.debug(
                        "[THETA][DEBUG][FILTER][MIDNIGHT_FIX] converted end from midnight to end-of-day: %s",
                        end.isoformat()
                    )

                if start.tzinfo is None:
                    start = LUMIBOT_DEFAULT_PYTZ.localize(start).astimezone(pytz.UTC)
                    logger.debug(
                        "[THETA][DEBUG][FILTER][TZ_LOCALIZE] localized start to UTC: %s",
                        start.isoformat()
                    )
                if end.tzinfo is None:
                    end = LUMIBOT_DEFAULT_PYTZ.localize(end).astimezone(pytz.UTC)
                    logger.debug(
                        "[THETA][DEBUG][FILTER][TZ_LOCALIZE] localized end to UTC: %s",
                        end.isoformat()
                    )

                # REMOVED: Look-ahead bias protection was too aggressive
                # The dt filtering was breaking negative timeshift (intentional look-ahead for fills)
                # Look-ahead bias protection should happen at get_bars() level, not cache retrieval
                #
                # NEW APPROACH: Always return full [start, end] range from cache
                # Let Data/DataPolars.get_bars() handle look-ahead bias protection
                logger.debug(
                    "[THETA][DEBUG][FILTER][NO_DT_FILTER] asset=%s | "
                    "using end=%s for upper bound (dt parameter ignored for cache retrieval)",
                    asset.symbol if hasattr(asset, 'symbol') else str(asset),
                    end.isoformat()
                )
                df_all = df_all[(df_all.index >= start) & (df_all.index <= end)]

        # DEBUG-LOG: After date range filtering, before missing removal
        if df_all is not None and not df_all.empty:
            logger.debug(
                "[THETA][DEBUG][FILTER][AFTER] asset=%s rows=%d first_ts=%s last_ts=%s dt_filter=%s",
                asset,
                len(df_all),
                df_all.index.min().isoformat() if len(df_all) > 0 else None,
                df_all.index.max().isoformat() if len(df_all) > 0 else None,
                dt.isoformat() if dt and hasattr(dt, 'isoformat') else dt
            )

        if df_all is not None and not df_all.empty and "missing" in df_all.columns:
            df_all = df_all[~df_all["missing"].astype(bool)].drop(columns=["missing"])


        # DEBUG-LOG: Before pandas return
        if df_all is not None and not df_all.empty:
            logger.debug(
                "[THETA][DEBUG][RETURN][PANDAS] asset=%s rows=%d first_ts=%s last_ts=%s",
                asset,
                len(df_all),
                df_all.index.min().isoformat() if len(df_all) > 0 else None,
                df_all.index.max().isoformat() if len(df_all) > 0 else None
            )
        return df_all

    logger.info("ThetaData cache MISS for %s %s %s; fetching %d interval(s) from ThetaTerminal.", asset, timespan, datastyle, len(missing_dates))

    # DEBUG-LOG: Cache miss
    logger.debug(
        "[THETA][DEBUG][CACHE][MISS] asset=%s timespan=%s datastyle=%s missing_intervals=%d first=%s last=%s",
        asset,
        timespan,
        datastyle,
        len(missing_dates),
        missing_dates[0] if missing_dates else None,
        missing_dates[-1] if missing_dates else None
    )


    fetch_start = missing_dates[0]  # Data will start at 8am UTC (4am EST)
    fetch_end = missing_dates[-1]  # Data will end at 23:59 UTC (7:59pm EST)

    # Initialize tqdm progress bar
    total_days = (fetch_end - fetch_start).days + 1
    total_queries = (total_days // MAX_DAYS) + 1
    description = f"\nDownloading '{datastyle}' data for {asset} / {quote_asset} with '{timespan}' from ThetaData..."
    logger.info(description)
    pbar = tqdm(total=1, desc=description, dynamic_ncols=True)

    delta = timedelta(days=MAX_DAYS)

    # For daily bars, use ThetaData's EOD endpoint for official daily OHLC
    # The EOD endpoint includes the 16:00 closing auction and follows SIP sale-condition rules
    # This matches Polygon and Yahoo Finance EXACTLY (zero tolerance)
    if timespan == "day":
        requested_dates = list(missing_dates)
        logger.info("Daily bars: using EOD endpoint for official close prices")
        logger.debug(
            "[THETA][DEBUG][THETADATA-EOD] requesting %d trading day(s) for %s from %s to %s",
            len(requested_dates),
            asset,
            fetch_start,
            fetch_end,
        )

        # Use EOD endpoint for official daily OHLC
        result_df = get_historical_eod_data(
            asset=asset,
            start_dt=fetch_start,
            end_dt=fetch_end,
            username=username,
            password=password,
            datastyle=datastyle
        )
        logger.debug(
            "[THETA][DEBUG][THETADATA-EOD] fetched rows=%s for %s",
            0 if result_df is None else len(result_df),
            asset,
        )

        if result_df is None or result_df.empty:
            expired_range = (
                asset.asset_type == "option"
                and asset.expiration is not None
                and requested_dates
                and all(day > asset.expiration for day in requested_dates)
            )
            if expired_range:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-EOD] Option %s expired on %s; cache reuse for range %s -> %s.",
                    asset,
                    asset.expiration,
                    fetch_start,
                    fetch_end,
                )
            else:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-EOD] No rows returned for %s between %s and %s; recording placeholders.",
                    asset,
                    fetch_start,
                    fetch_end,
                )
            df_all = append_missing_markers(df_all, requested_dates)
            update_cache(
                cache_file,
                df_all,
                df_cached,
                missing_dates=requested_dates,
                remote_payload=remote_payload,
            )
            df_clean = df_all.copy() if df_all is not None else None
            if df_clean is not None and not df_clean.empty and "missing" in df_clean.columns:
                df_clean = df_clean[~df_clean["missing"].astype(bool)].drop(columns=["missing"])
                df_clean = restore_numeric_dtypes(df_clean)
            logger.info(
                "ThetaData cache updated for %s %s %s with placeholders only (missing=%d).",
                asset,
                timespan,
                datastyle,
                len(requested_dates),
            )

            if df_clean is not None and not df_clean.empty and timespan == "day":
                start_date = requested_start.date() if hasattr(requested_start, "date") else requested_start
                end_date = requested_end.date() if hasattr(requested_end, "date") else requested_end
                dates = pd.to_datetime(df_clean.index).date
                df_clean = df_clean[(dates >= start_date) & (dates <= end_date)]

            return df_clean if df_clean is not None else pd.DataFrame()

        df_all = update_df(df_all, result_df)
        logger.debug(
            "[THETA][DEBUG][THETADATA-EOD] merged cache rows=%d (cached=%d new=%d)",
            0 if df_all is None else len(df_all),
            0 if df_cached is None else len(df_cached),
            len(result_df),
        )

        trading_days = get_trading_dates(asset, fetch_start, fetch_end)
        if "datetime" in result_df.columns:
            covered_index = pd.DatetimeIndex(pd.to_datetime(result_df["datetime"], utc=True))
        else:
            covered_index = pd.DatetimeIndex(result_df.index)
        if covered_index.tz is None:
            covered_index = covered_index.tz_localize(pytz.UTC)
        else:
            covered_index = covered_index.tz_convert(pytz.UTC)
        covered_days = set(covered_index.date)

        df_all = remove_missing_markers(df_all, list(covered_days))
        missing_within_range = [day for day in trading_days if day not in covered_days]
        placeholder_count = len(missing_within_range)
        df_all = append_missing_markers(df_all, missing_within_range)

        update_cache(
            cache_file,
            df_all,
            df_cached,
            missing_dates=missing_within_range,
            remote_payload=remote_payload,
        )

        df_clean = df_all.copy() if df_all is not None else None
        if df_clean is not None and not df_clean.empty and "missing" in df_clean.columns:
            df_clean = df_clean[~df_clean["missing"].astype(bool)].drop(columns=["missing"])
            df_clean = restore_numeric_dtypes(df_clean)

        logger.info(
            "ThetaData cache updated for %s %s %s (rows=%d placeholders=%d).",
            asset,
            timespan,
            datastyle,
            0 if df_all is None else len(df_all),
            placeholder_count,
        )

        if df_clean is not None and not df_clean.empty and timespan == "day":
            start_date = requested_start.date() if hasattr(requested_start, "date") else requested_start
            end_date = requested_end.date() if hasattr(requested_end, "date") else requested_end
            dates = pd.to_datetime(df_clean.index).date
            df_clean = df_clean[(dates >= start_date) & (dates <= end_date)]

        return df_clean if df_clean is not None else pd.DataFrame()

    # Map timespan to milliseconds for intraday intervals
    TIMESPAN_TO_MS = {
        "second": 1000,
        "minute": 60000,
        "5minute": 300000,
        "10minute": 600000,
        "15minute": 900000,
        "30minute": 1800000,
        "hour": 3600000,
        "2hour": 7200000,
        "4hour": 14400000,
    }

    interval_ms = TIMESPAN_TO_MS.get(timespan)
    if interval_ms is None:
        raise ValueError(
            f"Unsupported timespan '{timespan}'. "
            f"Supported values: {list(TIMESPAN_TO_MS.keys())} or 'day'"
        )

    current_start = fetch_start
    current_end = fetch_start + delta

    while current_start <= fetch_end:
        # If we don't have a paid subscription, we need to wait 1 minute between requests because of
        # the rate limit. Wait every other query so that we don't spend too much time waiting.

        if current_end > fetch_end:
            current_end = fetch_end
        if current_end > current_start + delta:
            current_end = current_start + delta

        result_df = get_historical_data(asset, current_start, current_end, interval_ms, username, password, datastyle=datastyle, include_after_hours=include_after_hours)
        chunk_end = _clamp_option_end(asset, current_end)

        if result_df is None or len(result_df) == 0:
            expired_chunk = (
                asset.asset_type == "option"
                and asset.expiration is not None
                and chunk_end.date() >= asset.expiration
            )
            if expired_chunk:
                logger.debug(
                    "[THETA][DEBUG][THETADATA] Option %s considered expired on %s; reusing cached data between %s and %s.",
                    asset,
                    asset.expiration,
                    current_start,
                    chunk_end,
                )
            else:
                logger.warning(
                    f"No data returned for {asset} / {quote_asset} with '{timespan}' timespan between {current_start} and {current_end}"
                )
            missing_chunk = get_trading_dates(asset, current_start, chunk_end)
            df_all = append_missing_markers(df_all, missing_chunk)
            pbar.update(1)

        else:
            df_all = update_df(df_all, result_df)
            available_chunk = get_trading_dates(asset, current_start, chunk_end)
            df_all = remove_missing_markers(df_all, available_chunk)
            if "datetime" in result_df.columns:
                chunk_index = pd.DatetimeIndex(pd.to_datetime(result_df["datetime"], utc=True))
            else:
                chunk_index = pd.DatetimeIndex(result_df.index)
            if chunk_index.tz is None:
                chunk_index = chunk_index.tz_localize(pytz.UTC)
            else:
                chunk_index = chunk_index.tz_convert(pytz.UTC)
            covered_days = {ts.date() for ts in chunk_index}
            missing_within_chunk = [day for day in available_chunk if day not in covered_days]
            if missing_within_chunk:
                df_all = append_missing_markers(df_all, missing_within_chunk)
            pbar.update(1)

        current_start = current_end + timedelta(days=1)
        current_end = current_start + delta

        if asset.expiration and current_start > asset.expiration:
            break

    update_cache(cache_file, df_all, df_cached, remote_payload=remote_payload)
    if df_all is not None:
        logger.debug("[THETA][DEBUG][THETADATA-CACHE-WRITE] wrote %s rows=%d", cache_file, len(df_all))
    if df_all is not None:
        logger.info("ThetaData cache updated for %s %s %s (%d rows).", asset, timespan, datastyle, len(df_all))
    # Close the progress bar when done
    pbar.close()
    if df_all is not None and not df_all.empty and "missing" in df_all.columns:
        df_all = df_all[~df_all["missing"].astype(bool)].drop(columns=["missing"])
        df_all = restore_numeric_dtypes(df_all)

    if df_all is not None and not df_all.empty and timespan == "day":
        start_date = requested_start.date() if hasattr(requested_start, "date") else requested_start
        end_date = requested_end.date() if hasattr(requested_end, "date") else requested_end
        dates = pd.to_datetime(df_all.index).date
        df_all = df_all[(dates >= start_date) & (dates <= end_date)]

    return df_all




def get_trading_dates(asset: Asset, start: datetime, end: datetime):
    """
    Get a list of trading days for the asset between the start and end dates
    Parameters
    ----------
    asset : Asset
        Asset we are getting data for
    start : datetime
        Start date for the data requested
    end : datetime
        End date for the data requested

    Returns
    -------

    """
    # Crypto Asset Calendar
    if asset.asset_type == "crypto":
        # Crypto trades every day, 24/7 so we don't need to check the calendar
        return [start.date() + timedelta(days=x) for x in range((end.date() - start.date()).days + 1)]

    # Stock/Option/Index Asset for Backtesting - Assuming NYSE trading days
    elif asset.asset_type == "stock" or asset.asset_type == "option" or asset.asset_type == "index":
        cal = mcal.get_calendar("NYSE")

    # Forex Asset for Backtesting - Forex trades weekdays, 24hrs starting Sunday 5pm EST
    # Calendar: "CME_FX"
    elif asset.asset_type == "forex":
        cal = mcal.get_calendar("CME_FX")

    else:
        raise ValueError(f"Unsupported asset type for thetadata: {asset.asset_type}")

    # Get the trading days between the start and end dates
    start_date = start.date() if hasattr(start, 'date') else start
    end_date = end.date() if hasattr(end, 'date') else end
    df = cal.schedule(start_date=start_date, end_date=end_date)
    trading_days = df.index.date.tolist()
    return trading_days


def build_cache_filename(asset: Asset, timespan: str, datastyle: str = "ohlc"):
    """Helper function to create the cache filename for a given asset and timespan"""

    provider_root = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER
    asset_folder = _resolve_asset_folder(asset)
    timespan_folder = _normalize_folder_component(timespan, "unknown")
    datastyle_folder = _normalize_folder_component(datastyle, "default")
    base_folder = provider_root / asset_folder / timespan_folder / datastyle_folder

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")

        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    cache_filename = f"{asset.asset_type}_{uniq_str}_{timespan}_{datastyle}.parquet"
    cache_file = base_folder / cache_filename
    return cache_file


def build_remote_cache_payload(asset: Asset, timespan: str, datastyle: str = "ohlc") -> Dict[str, object]:
    """Generate metadata describing the cache entry for remote storage."""
    payload: Dict[str, object] = {
        "provider": "thetadata",
        "timespan": timespan,
        "datastyle": datastyle,
        "asset_type": getattr(asset, "asset_type", None),
        "symbol": getattr(asset, "symbol", str(asset)),
    }

    if getattr(asset, "asset_type", None) == "option":
        payload.update(
            {
                "expiration": getattr(asset, "expiration", None),
                "strike": getattr(asset, "strike", None),
                "right": getattr(asset, "right", None),
            }
        )

    return payload


def get_missing_dates(df_all, asset, start, end):
    """
    Check if we have data for the full range
    Later Query to Polygon will pad an extra full day to start/end dates so that there should never
    be any gap with intraday data missing.

    Parameters
    ----------
    df_all : pd.DataFrame
        Data loaded from the cache file
    asset : Asset
        Asset we are getting data for
    start : datetime
        Start date for the data requested
    end : datetime
        End date for the data requested

    Returns
    -------
    list[datetime.date]
        A list of dates that we need to get data for
    """
    # DEBUG-LOG: Entry to get_missing_dates
    logger.debug(
        "[THETA][DEBUG][CACHE][MISSING_DATES_CHECK] asset=%s | "
        "start=%s end=%s | "
        "cache_rows=%d",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        start.isoformat() if hasattr(start, 'isoformat') else start,
        end.isoformat() if hasattr(end, 'isoformat') else end,
        0 if df_all is None else len(df_all)
    )

    trading_dates = get_trading_dates(asset, start, end)

    logger.debug(
        "[THETA][DEBUG][CACHE][TRADING_DATES] asset=%s | "
        "trading_dates_count=%d first=%s last=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        len(trading_dates),
        trading_dates[0] if trading_dates else None,
        trading_dates[-1] if trading_dates else None
    )

    if df_all is None or not len(df_all):
        logger.debug(
            "[THETA][DEBUG][CACHE][EMPTY] asset=%s | "
            "cache is EMPTY -> all %d trading days are missing",
            asset.symbol if hasattr(asset, 'symbol') else str(asset),
            len(trading_dates)
        )
        return trading_dates

    # It is possible to have full day gap in the data if previous queries were far apart
    # Example: Query for 8/1/2023, then 8/31/2023, then 8/7/2023
    # Whole days are easy to check for because we can just check the dates in the index
    dates = pd.Series(df_all.index.date).unique()
    cached_dates_count = len(dates)
    cached_first = min(dates) if len(dates) > 0 else None
    cached_last = max(dates) if len(dates) > 0 else None

    logger.debug(
        "[THETA][DEBUG][CACHE][CACHED_DATES] asset=%s | "
        "cached_dates_count=%d first=%s last=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        cached_dates_count,
        cached_first,
        cached_last
    )

    missing_dates = sorted(set(trading_dates) - set(dates))

    # For Options, don't need any dates passed the expiration date
    if asset.asset_type == "option":
        before_expiry_filter = len(missing_dates)
        missing_dates = [x for x in missing_dates if x <= asset.expiration]
        after_expiry_filter = len(missing_dates)

        if before_expiry_filter != after_expiry_filter:
            logger.debug(
                "[THETA][DEBUG][CACHE][OPTION_EXPIRY_FILTER] asset=%s | "
                "filtered %d dates after expiration=%s | "
                "missing_dates: %d -> %d",
                asset.symbol if hasattr(asset, 'symbol') else str(asset),
                before_expiry_filter - after_expiry_filter,
                asset.expiration,
                before_expiry_filter,
                after_expiry_filter
            )

    logger.debug(
        "[THETA][DEBUG][CACHE][MISSING_RESULT] asset=%s | "
        "missing_dates_count=%d | "
        "first_missing=%s last_missing=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        len(missing_dates),
        missing_dates[0] if missing_dates else None,
        missing_dates[-1] if missing_dates else None
    )

    return missing_dates


def load_cache(cache_file):
    """Load the data from the cache file and return a DataFrame with a DateTimeIndex"""
    # DEBUG-LOG: Start loading cache
    logger.debug(
        "[THETA][DEBUG][CACHE][LOAD_START] cache_file=%s | "
        "exists=%s size_bytes=%d",
        cache_file.name,
        cache_file.exists(),
        cache_file.stat().st_size if cache_file.exists() else 0
    )

    if not cache_file.exists():
        logger.debug(
            "[THETA][DEBUG][CACHE][LOAD_MISSING] cache_file=%s | returning=None",
            cache_file.name,
        )
        return None

    df = pd.read_parquet(cache_file, engine='pyarrow')

    rows_after_read = len(df)
    logger.debug(
        "[THETA][DEBUG][CACHE][LOAD_READ] cache_file=%s | "
        "rows_read=%d columns=%s",
        cache_file.name,
        rows_after_read,
        list(df.columns)
    )

    # Set the 'datetime' column as the index of the DataFrame
    df.set_index("datetime", inplace=True)

    df.index = pd.to_datetime(
        df.index
    )  # TODO: Is there some way to speed this up? It takes several times longer than just reading the cache file
    df = df.sort_index()

    # Check if the index is already timezone aware
    if df.index.tzinfo is None:
        # Set the timezone to UTC
        df.index = df.index.tz_localize("UTC")
        logger.debug(
            "[THETA][DEBUG][CACHE][LOAD_TZ] cache_file=%s | "
            "localized index to UTC",
            cache_file.name
        )

    df = ensure_missing_column(df)

    min_ts = df.index.min() if len(df) > 0 else None
    max_ts = df.index.max() if len(df) > 0 else None
    placeholder_count = int(df["missing"].sum()) if "missing" in df.columns else 0

    logger.debug(
        "[THETA][DEBUG][CACHE][LOAD_SUCCESS] cache_file=%s | "
        "total_rows=%d real_rows=%d placeholders=%d | "
        "min_ts=%s max_ts=%s",
        cache_file.name,
        len(df),
        len(df) - placeholder_count,
        placeholder_count,
        min_ts.isoformat() if min_ts else None,
        max_ts.isoformat() if max_ts else None
    )

    return df


def update_cache(cache_file, df_all, df_cached, missing_dates=None, remote_payload=None):
    """Update the cache file with the new data and optional placeholder markers."""
    # DEBUG-LOG: Entry to update_cache
    logger.debug(
        "[THETA][DEBUG][CACHE][UPDATE_ENTRY] cache_file=%s | "
        "df_all_rows=%d df_cached_rows=%d missing_dates=%d",
        cache_file.name,
        0 if df_all is None else len(df_all),
        0 if df_cached is None else len(df_cached),
        0 if not missing_dates else len(missing_dates)
    )

    if df_all is None or len(df_all) == 0:
        if not missing_dates:
            logger.debug(
                "[THETA][DEBUG][CACHE][UPDATE_SKIP] cache_file=%s | "
                "df_all is empty and no missing_dates, skipping cache update",
                cache_file.name
            )
            return
        logger.debug(
            "[THETA][DEBUG][CACHE][UPDATE_PLACEHOLDERS_ONLY] cache_file=%s | "
            "df_all is empty, writing %d placeholders",
            cache_file.name,
            len(missing_dates)
        )
        df_working = append_missing_markers(None, missing_dates)
    else:
        df_working = ensure_missing_column(df_all.copy())
        if missing_dates:
            logger.debug(
                "[THETA][DEBUG][CACHE][UPDATE_APPEND_PLACEHOLDERS] cache_file=%s | "
                "appending %d placeholders to %d existing rows",
                cache_file.name,
                len(missing_dates),
                len(df_working)
            )
            df_working = append_missing_markers(df_working, missing_dates)

    if df_working is None or len(df_working) == 0:
        logger.debug(
            "[THETA][DEBUG][CACHE][UPDATE_SKIP_EMPTY] cache_file=%s | "
            "df_working is empty after processing, skipping write",
            cache_file.name
        )
        return

    df_cached_cmp = None
    if df_cached is not None and len(df_cached) > 0:
        df_cached_cmp = ensure_missing_column(df_cached.copy())

    if df_cached_cmp is not None and df_working.equals(df_cached_cmp):
        logger.debug(
            "[THETA][DEBUG][CACHE][UPDATE_NO_CHANGES] cache_file=%s | "
            "df_working equals df_cached (rows=%d), skipping write",
            cache_file.name,
            len(df_working)
        )
        return

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df_to_save = df_working.reset_index()

    placeholder_count = int(df_working["missing"].sum()) if "missing" in df_working.columns else 0
    real_rows = len(df_working) - placeholder_count
    min_ts = df_working.index.min() if len(df_working) > 0 else None
    max_ts = df_working.index.max() if len(df_working) > 0 else None

    def _format_ts(value):
        if value is None:
            return None
        return value.isoformat() if hasattr(value, "isoformat") else value

    logger.debug(
        "[THETA][DEBUG][CACHE][UPDATE_WRITE] cache_file=%s | "
        "total_rows=%d real_rows=%d placeholders=%d | "
        "min_ts=%s max_ts=%s",
        cache_file.name,
        len(df_working),
        real_rows,
        placeholder_count,
        _format_ts(min_ts),
        _format_ts(max_ts)
        )

    df_to_save.to_parquet(cache_file, engine="pyarrow", compression="snappy")

    logger.debug(
        "[THETA][DEBUG][CACHE][UPDATE_SUCCESS] cache_file=%s written successfully",
        cache_file.name
    )

    cache_manager = get_backtest_cache()
    if cache_manager.mode == CacheMode.S3_READWRITE:
        try:
            cache_manager.on_local_update(cache_file, payload=remote_payload)
        except Exception as exc:
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_UPLOAD_ERROR] cache_file=%s error=%s",
                cache_file,
                exc,
            )


def update_df(df_all, result):
    """
    Update the DataFrame with the new data from ThetaData

    Parameters
    ----------
    df_all : pd.DataFrame
        A DataFrame with the data we already have
    result : pandas DataFrame
        A List of dictionaries with the new data from Polygon
        Format:
        {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "high": [3, 4, 5, 6, 7],
                "low": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00",
                    "2023-07-01 09:31:00",
                    "2023-07-01 09:32:00",
                    "2023-07-01 09:33:00",
                    "2023-07-01 09:34:00",
                ],
            }
    """
    ny_tz = LUMIBOT_DEFAULT_PYTZ
    df = pd.DataFrame(result)
    if not df.empty:
        df["missing"] = False
        if "datetime" not in df.index.names:
            # check if df has a column named "datetime", if not raise key error
            if "datetime" not in df.columns:
                raise KeyError("KeyError: update_df function requires 'result' input with 'datetime' column, but not found")

            # if column "datetime" is not index set it as index
            df = df.set_index("datetime").sort_index()
        else:
            df = df.sort_index()

        if not df.index.tzinfo:
            df.index = df.index.tz_localize(ny_tz).tz_convert(pytz.utc)
        else:
            df.index = df.index.tz_convert(pytz.utc)

        if df_all is not None:
            # set "datetime" column as index of df_all
            if isinstance(df.index, pd.DatetimeIndex) and df.index.name == 'datetime':
                df_all = df_all.sort_index()
            else:
                df_all = df_all.set_index("datetime").sort_index()

            # convert df_all index to UTC if not already
            if not df.index.tzinfo:
                df_all.index = df_all.index.tz_localize(ny_tz).tz_convert(pytz.utc)
            else:
                df_all.index = df_all.index.tz_convert(pytz.utc)

        if df_all is None or df_all.empty:
            df_all = df
        else:
            df_all = pd.concat([df_all, df]).sort_index()
            df_all = df_all[~df_all.index.duplicated(keep="last")]  # Keep newest data over placeholders

        # NOTE: Timestamp correction is now done in get_historical_data() at line 569
        # Do NOT subtract 1 minute here as it would double-correct
        # df_all.index = df_all.index - pd.Timedelta(minutes=1)
        df_all = ensure_missing_column(df_all)
    return df_all


def is_process_alive():
    """Check if ThetaTerminal Java process is still running"""
    import os
    import subprocess

    global THETA_DATA_PROCESS, THETA_DATA_PID, THETA_DATA_LOG_HANDLE

    # If we have a subprocess handle, trust it first
    if THETA_DATA_PROCESS is not None:
        if THETA_DATA_PROCESS.poll() is None:
            return True
        # Process exited—clear cached handle and PID
        reset_theta_terminal_tracking()

    # If we know the PID, probe it directly
    if THETA_DATA_PID:
        try:
            # Sending signal 0 simply tests liveness
            os.kill(THETA_DATA_PID, 0)
            return True
        except OSError:
            reset_theta_terminal_tracking()

    return False


def start_theta_data_client(username: str, password: str):
    import subprocess
    import shutil
    global THETA_DATA_PROCESS, THETA_DATA_PID
    CONNECTION_DIAGNOSTICS["start_terminal_calls"] += 1

    # First try shutting down any existing connection
    graceful_shutdown_requested = False
    try:
        requests.get(f"{BASE_URL}/v2/system/terminal/shutdown", timeout=1)
        graceful_shutdown_requested = True
    except Exception:
        pass

    shutdown_deadline = time.time() + 15
    while True:
        process_alive = is_process_alive()
        status_alive = False
        try:
            status_text = requests.get(f"{BASE_URL}/v2/system/mdds/status", timeout=0.5).text
            status_alive = status_text in ("CONNECTED", "DISCONNECTED")
        except Exception:
            status_alive = False

        if not process_alive and not status_alive:
            break

        if time.time() >= shutdown_deadline:
            if process_alive and THETA_DATA_PID:
                kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
                try:
                    os.kill(THETA_DATA_PID, kill_signal)
                except Exception as kill_exc:
                    logger.warning("Failed to force kill ThetaTerminal PID %s: %s", THETA_DATA_PID, kill_exc)
            break

        time.sleep(0.5)

    # Create creds.txt file to avoid passing password with special characters on command line
    # This is the official ThetaData method and avoids shell escaping issues
    # Security note: creds.txt with 0o600 permissions is MORE secure than command-line args
    # which can be seen in process lists. Similar security profile to .env files.
    theta_dir = Path.home() / "ThetaData" / "ThetaTerminal"
    theta_dir.mkdir(parents=True, exist_ok=True)
    creds_file = theta_dir / "creds.txt"

    # Read previous credentials if they exist so we can decide whether to overwrite
    existing_username = None
    existing_password = None
    if creds_file.exists():
        try:
            with open(creds_file, 'r') as f:
                existing_username = (f.readline().strip() or None)
                existing_password = (f.readline().strip() or None)
        except Exception as exc:
            logger.warning(f"Could not read existing creds.txt: {exc}; will recreate the file.")
            existing_username = None
            existing_password = None

    if username is None:
        username = existing_username
    if password is None:
        password = existing_password

    if username is None or password is None:
        raise ValueError(
            "ThetaData credentials are required to start ThetaTerminal. Provide them via backtest() or configure THETADATA_USERNAME/THETADATA_PASSWORD."
        )

    should_write = (
        not creds_file.exists()
        or existing_username != username
        or existing_password != password
    )

    if should_write:
        logger.info(f"Writing creds.txt file for user: {username}")
        with open(creds_file, 'w') as f:
            f.write(f"{username}\n")
            f.write(f"{password}\n")
        os.chmod(creds_file, 0o600)
    else:
        logger.debug(f"Reusing existing creds.txt for {username}")

    # Launch ThetaTerminal directly with --creds-file to avoid shell escaping issues
    # We bypass the thetadata library's launcher which doesn't support this option
    # and has shell escaping bugs with special characters in passwords

    # Verify Java is available
    if not shutil.which("java"):
        raise RuntimeError("Java is not installed. Please install Java 11+ to use ThetaData.")

    # Find ThetaTerminal.jar
    jar_file = theta_dir / "ThetaTerminal.jar"
    if not jar_file.exists():
        # Copy ThetaTerminal.jar from lumibot package to user's ThetaData directory
        logger.info("ThetaTerminal.jar not found, copying from lumibot package...")
        import shutil as shutil_copy

        package_root = Path(__file__).resolve().parent.parent
        candidate_paths = [
            package_root / "resources" / "ThetaTerminal.jar",
            package_root.parent / "ThetaTerminal.jar",  # legacy location fallback
        ]

        lumibot_jar = next((path for path in candidate_paths if path.exists()), None)

        if lumibot_jar is None:
            raise FileNotFoundError(
                "ThetaTerminal.jar not bundled with lumibot installation. "
                f"Searched: {', '.join(str(path) for path in candidate_paths)}. "
                f"Please reinstall lumibot or manually place the jar at {jar_file}"
            )

        logger.info(f"Copying ThetaTerminal.jar from {lumibot_jar} to {jar_file}")
        shutil_copy.copy2(lumibot_jar, jar_file)
        logger.info(f"Successfully copied ThetaTerminal.jar to {jar_file}")

    if not jar_file.exists():
        raise FileNotFoundError(f"ThetaTerminal.jar not found at {jar_file}")

    # Launch ThetaTerminal with --creds-file argument (no credentials on command line)
    # This avoids all shell escaping issues and is the recommended approach
    cmd = ["java", "-jar", str(jar_file), "--creds-file", str(creds_file)]

    logger.info(f"Launching ThetaTerminal with creds file: {cmd}")

    reset_theta_terminal_tracking()

    log_path = theta_dir / "lumibot_launch.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = open(log_path, "ab")
    launch_ts = datetime.now(timezone.utc)
    log_handle.write(f"\n---- Launch {launch_ts.isoformat()} ----\n".encode())
    log_handle.flush()

    global THETA_DATA_LOG_HANDLE
    THETA_DATA_LOG_HANDLE = log_handle

    try:
        THETA_DATA_PROCESS = subprocess.Popen(
            cmd,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            cwd=str(theta_dir)
        )
    except Exception:
        THETA_DATA_LOG_HANDLE = None
        log_handle.close()
        raise

    THETA_DATA_PID = THETA_DATA_PROCESS.pid
    logger.info(f"ThetaTerminal started with PID: {THETA_DATA_PID}")

    # We don't return a ThetaClient object since we're launching manually
    # The connection will be established via HTTP/WebSocket to localhost:25510
    return THETA_DATA_PROCESS


def check_connection(username: str, password: str, wait_for_connection: bool = False):
    """Ensure the local ThetaTerminal is running. Optionally block until it is connected.

    Parameters
    ----------
    username : str
        ThetaData username.
    password : str
        ThetaData password.
    wait_for_connection : bool, optional
        If True, block and retry until the terminal reports CONNECTED (or retries are exhausted).
        If False, perform a lightweight liveness check and return immediately.
    """

    CONNECTION_DIAGNOSTICS["check_connection_calls"] += 1

    max_retries = CONNECTION_MAX_RETRIES
    sleep_interval = CONNECTION_RETRY_SLEEP
    client = None

    def probe_status() -> Optional[str]:
        try:
            res = requests.get(f"{BASE_URL}/v2/system/mdds/status", timeout=1)
            return res.text
        except Exception as exc:
            logger.debug(f"Cannot reach ThetaTerminal status endpoint: {exc}")
            return None

    if not wait_for_connection:
        status_text = probe_status()
        if status_text == "CONNECTED":
            if THETA_DATA_PROCESS is None and THETA_DATA_PID is None:
                logger.debug("ThetaTerminal reports CONNECTED but no process is tracked; restarting to capture handle.")
                client = start_theta_data_client(username=username, password=password)
                new_client, connected = check_connection(
                    username=username,
                    password=password,
                    wait_for_connection=True,
                )
                return client or new_client, connected

            logger.debug("ThetaTerminal already connected.")
            return None, True

        if not is_process_alive():
            logger.debug("ThetaTerminal process not running; launching background restart.")
            client = start_theta_data_client(username=username, password=password)
            new_client, connected = check_connection(
                username=username,
                password=password,
                wait_for_connection=True,
            )
            return client or new_client, connected

        logger.debug("ThetaTerminal running but not yet CONNECTED; waiting for status.")
        return check_connection(username=username, password=password, wait_for_connection=True)

    total_restart_cycles = 0

    while True:
        counter = 0
        restart_attempts = 0

        while counter < max_retries:
            status_text = probe_status()
            if status_text == "CONNECTED":
                if counter or total_restart_cycles:
                    logger.info(
                        "ThetaTerminal connected after %s attempt(s) (restart cycles=%s).",
                        counter + 1,
                        total_restart_cycles,
                    )
                return client, True
            elif status_text == "DISCONNECTED":
                logger.debug("ThetaTerminal reports DISCONNECTED; will retry.")
            elif status_text is not None:
                logger.debug(f"ThetaTerminal returned unexpected status: {status_text}")

            if not is_process_alive():
                if restart_attempts >= MAX_RESTART_ATTEMPTS:
                    logger.error("ThetaTerminal not running after %s restart attempts.", restart_attempts)
                    break
                restart_attempts += 1
                logger.warning("ThetaTerminal process is not running (restart #%s).", restart_attempts)
                client = start_theta_data_client(username=username, password=password)
                CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1
                time.sleep(max(BOOT_GRACE_PERIOD, sleep_interval))
                counter = 0
                continue

            counter += 1
            if counter % 10 == 0:
                logger.info("Waiting for ThetaTerminal connection (attempt %s/%s).", counter, max_retries)
            time.sleep(sleep_interval)

        total_restart_cycles += 1
        if total_restart_cycles > MAX_TERMINAL_RESTART_CYCLES:
            logger.error(
                "Unable to connect to Theta Data after %s restart cycle(s) (%s attempts each).",
                MAX_TERMINAL_RESTART_CYCLES,
                max_retries,
            )
            raise ThetaDataConnectionError(
                f"Unable to connect to Theta Data after {MAX_TERMINAL_RESTART_CYCLES} restart cycle(s)."
            )

        logger.warning(
            "ThetaTerminal still disconnected after %s attempts; restarting (cycle %s/%s).",
            max_retries,
            total_restart_cycles,
            MAX_TERMINAL_RESTART_CYCLES,
        )
        client = start_theta_data_client(username=username, password=password)
        CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1
        time.sleep(max(BOOT_GRACE_PERIOD, sleep_interval))


def get_request(url: str, headers: dict, querystring: dict, username: str, password: str):
    all_responses = []
    next_page_url = None
    page_count = 0
    consecutive_disconnects = 0
    restart_budget = 3

    # Lightweight liveness probe before issuing the request
    check_connection(username=username, password=password, wait_for_connection=False)

    while True:
        counter = 0
        # Use next_page URL if available, otherwise use original URL with querystring
        request_url = next_page_url if next_page_url else url
        request_params = None if next_page_url else querystring

        while True:
            try:
                CONNECTION_DIAGNOSTICS["network_requests"] += 1

                # DEBUG-LOG: API request
                logger.debug(
                    "[THETA][DEBUG][API][REQUEST] url=%s params=%s",
                    request_url if next_page_url else url,
                    request_params if request_params else querystring
                )

                response = requests.get(request_url, headers=headers, params=request_params)
                status_code = response.status_code
                # Status code 472 means "No data" - this is valid, return None
                if status_code == 472:
                    logger.warning(f"No data available for request: {response.text[:200]}")
                    # DEBUG-LOG: API response - no data
                    logger.debug(
                        "[THETA][DEBUG][API][RESPONSE] status=472 result=NO_DATA"
                    )
                    consecutive_disconnects = 0
                    return None
                elif status_code == 474:
                    consecutive_disconnects += 1
                    logger.warning("Received 474 from Theta Data (attempt %s): %s", counter + 1, response.text[:200])
                    if consecutive_disconnects >= 2:
                        if restart_budget <= 0:
                            logger.error("Restart budget exhausted after repeated 474 responses.")
                            raise ValueError("Cannot connect to Theta Data!")
                        logger.warning(
                            "Restarting ThetaTerminal after %s consecutive 474 responses (restart budget remaining %s).",
                            consecutive_disconnects,
                            restart_budget - 1,
                        )
                        restart_budget -= 1
                        start_theta_data_client(username=username, password=password)
                        CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1
                        check_connection(username=username, password=password, wait_for_connection=True)
                        time.sleep(max(BOOT_GRACE_PERIOD, CONNECTION_RETRY_SLEEP))
                        consecutive_disconnects = 0
                        counter = 0
                    else:
                        check_connection(username=username, password=password, wait_for_connection=True)
                        time.sleep(CONNECTION_RETRY_SLEEP)
                    continue
                # If status code is not 200, then we are not connected
                elif status_code != 200:
                    logger.warning(f"Non-200 status code {status_code}: {response.text[:200]}")
                    # DEBUG-LOG: API response - error
                    logger.debug(
                        "[THETA][DEBUG][API][RESPONSE] status=%d result=ERROR",
                        status_code
                    )
                    check_connection(username=username, password=password, wait_for_connection=True)
                    consecutive_disconnects = 0
                else:
                    json_resp = response.json()
                    consecutive_disconnects = 0

                    # DEBUG-LOG: API response - success
                    response_rows = len(json_resp.get("response", [])) if isinstance(json_resp.get("response"), list) else 0
                    logger.debug(
                        "[THETA][DEBUG][API][RESPONSE] status=200 rows=%d has_next_page=%s",
                        response_rows,
                        bool(json_resp.get("header", {}).get("next_page"))
                    )

                    # Check if json_resp has error_type inside of header
                    if "error_type" in json_resp["header"] and json_resp["header"]["error_type"] != "null":
                        # Handle "NO_DATA" error
                        if json_resp["header"]["error_type"] == "NO_DATA":
                            logger.warning(
                                f"No data returned for querystring: {querystring}")
                            return None
                        else:
                            error_label = json_resp["header"].get("error_type")
                            logger.error(
                                f"Error getting data from Theta Data: {error_label},\nquerystring: {querystring}")
                            check_connection(username=username, password=password, wait_for_connection=True)
                            raise ValueError(f"ThetaData returned error_type={error_label}")
                    else:
                        break

            except ThetaDataConnectionError as exc:
                logger.error("Theta Data connection failed after supervised restarts: %s", exc)
                raise
            except ValueError:
                # Preserve deliberate ValueError signals (e.g., ThetaData error_type responses)
                raise
            except Exception as e:
                logger.warning(f"Exception during request (attempt {counter + 1}): {e}")
                check_connection(username=username, password=password, wait_for_connection=True)
                if counter == 0:
                    logger.debug("[THETA][DEBUG][API][WAIT] Allowing ThetaTerminal to initialize for 5s before retry.")
                    time.sleep(5)

            counter += 1
            if counter > 1:
                raise ValueError("Cannot connect to Theta Data!")

        # Store this page's response data
        page_count += 1
        all_responses.append(json_resp["response"])

        # Check for pagination - follow next_page if it exists
        next_page = json_resp["header"].get("next_page")
        if next_page and next_page != "null" and next_page != "":
            logger.info(f"Following pagination: {page_count} page(s) downloaded, fetching next page...")
            next_page_url = next_page
        else:
            # No more pages, we're done
            break

    # Merge all pages if we got multiple pages
    if page_count > 1:
        logger.info(f"Merged {page_count} pages from ThetaData ({sum(len(r) for r in all_responses)} total rows)")
        json_resp["response"] = []
        for page_response in all_responses:
            json_resp["response"].extend(page_response)

    return json_resp


def get_historical_eod_data(asset: Asset, start_dt: datetime, end_dt: datetime, username: str, password: str, datastyle: str = "ohlc"):
    """
    Get EOD (End of Day) data from ThetaData using the /v2/hist/{asset_type}/eod endpoint.

    This endpoint provides official daily OHLC that includes the 16:00 closing auction
    and follows SIP sale-condition rules, matching Polygon and Yahoo Finance exactly.

    NOTE: ThetaData's EOD endpoint has been found to return incorrect open prices for stocks
    that don't match Polygon/Yahoo. We fix this by using the first minute bar's open price.
    Indexes don't have this issue since they are calculated values.

    Parameters
    ----------
    asset : Asset
        The asset we are getting data for
    start_dt : datetime
        The start date for the data we want
    end_dt : datetime
        The end date for the data we want
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    datastyle : str
        The style of data to retrieve (default "ohlc")

    Returns
    -------
    pd.DataFrame
        A DataFrame with EOD data for the asset
    """
    # Convert start and end dates to strings
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    # Use v2 EOD API endpoint (supports stock, index, option)
    url = f"{BASE_URL}/v2/hist/{asset.asset_type}/eod"

    querystring = {
        "root": asset.symbol,
        "start_date": start_date,
        "end_date": end_date
    }

    # For options, add strike, expiration, and right parameters
    if asset.asset_type == "option":
        expiration_str = asset.expiration.strftime("%Y%m%d")
        strike = int(asset.strike * 1000)
        querystring["exp"] = expiration_str
        querystring["strike"] = strike
        querystring["right"] = "C" if asset.right == "CALL" else "P"

    headers = {"Accept": "application/json"}

    # DEBUG-LOG: EOD data request
    logger.debug(
        "[THETA][DEBUG][EOD][REQUEST] asset=%s start=%s end=%s datastyle=%s",
        asset,
        start_date,
        end_date,
        datastyle
    )

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring,
                            username=username, password=password)
    if json_resp is None:
        # DEBUG-LOG: EOD data response - no data
        logger.debug(
            "[THETA][DEBUG][EOD][RESPONSE] asset=%s result=NO_DATA",
            asset
        )
        return None

    # DEBUG-LOG: EOD data response - success
    response_rows = len(json_resp.get("response", [])) if isinstance(json_resp.get("response"), list) else 0
    logger.debug(
        "[THETA][DEBUG][EOD][RESPONSE] asset=%s rows=%d",
        asset,
        response_rows
    )

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    if df is None or df.empty:
        return df

    # Function to combine ms_of_day and date into datetime
    def combine_datetime(row):
        # Ensure the date is in integer format and then convert to string
        date_str = str(int(row["date"]))
        base_date = datetime.strptime(date_str, "%Y%m%d")
        # EOD reports are normalized at ~17:15 ET but represent the trading day
        # We use midnight of the trading day as the timestamp (consistent with daily bars)
        return base_date

    # Apply the function to each row to create a new datetime column
    datetime_combined = df.apply(combine_datetime, axis=1)

    # Assign the newly created datetime column
    df = df.assign(datetime=datetime_combined)

    # Convert the datetime column to a datetime and localize to UTC
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["datetime"] = df["datetime"].dt.tz_localize("UTC")

    # Set datetime as the index
    df = df.set_index("datetime")

    # Drop the ms_of_day, ms_of_day2, and date columns (not needed for daily bars)
    df = df.drop(columns=["ms_of_day", "ms_of_day2", "date"], errors='ignore')

    # Drop bid/ask columns if present (EOD includes NBBO but we only need OHLC)
    df = df.drop(columns=["bid_size", "bid_exchange", "bid", "bid_condition",
                          "ask_size", "ask_exchange", "ask", "ask_condition"], errors='ignore')

    # FIX: ThetaData's EOD endpoint returns incorrect open/high/low prices for STOCKS and OPTIONS
    # that don't match Polygon/Yahoo. We fix this by using minute bar data.
    # Solution: Fetch minute bars for each trading day and aggregate to get correct OHLC
    # NOTE: Indexes don't need this fix since they are calculated values, not traded securities
    if asset.asset_type in ["stock", "option"]:
        logger.info(f"Fetching 9:30 AM minute bars to correct EOD open prices...")

        # Get minute data for the date range to extract 9:30 AM opens
        minute_df = get_historical_data(
            asset=asset,
            start_dt=start_dt,
            end_dt=end_dt,
            ivl=60000,  # 1 minute
            username=username,
            password=password,
            datastyle=datastyle,
            include_after_hours=False  # RTH only
        )

        if minute_df is not None and not minute_df.empty:
            # Group by date and get the first bar's open for each day
            minute_df_copy = minute_df.copy()
            minute_df_copy['date'] = minute_df_copy.index.date

            # For each date in df, find the corresponding 9:30 AM open from minute data
            for idx in df.index:
                trade_date = idx.date()
                day_minutes = minute_df_copy[minute_df_copy['date'] == trade_date]
                if len(day_minutes) > 0:
                    # Use the first minute bar's open (9:30 AM opening auction)
                    correct_open = day_minutes.iloc[0]['open']
                    df.loc[idx, 'open'] = correct_open

    return df


def get_historical_data(asset: Asset, start_dt: datetime, end_dt: datetime, ivl: int, username: str, password: str, datastyle:str = "ohlc", include_after_hours: bool = True):
    """
    Get data from ThetaData

    Parameters
    ----------
    asset : Asset
        The asset we are getting data for
    start_dt : datetime
        The start date/time for the data we want
    end_dt : datetime
        The end date/time for the data we want
    ivl : int
        The interval for the data we want in milliseconds (eg. 60000 for 1 minute)
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    datastyle : str
        The style of data to retrieve ("ohlc" or "quote")
    include_after_hours : bool
        Whether to include after-hours trading data (default True)

    Returns
    -------
    pd.DataFrame
        A DataFrame with the data for the asset
    """

    # Comvert start and end dates to strings
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    # Use v2 API for ALL asset types
    url = f"{BASE_URL}/v2/hist/{asset.asset_type}/{datastyle}"

    if asset.asset_type == "option":
        # Convert the expiration date to a string
        expiration_str = asset.expiration.strftime("%Y%m%d")

        # Convert the strike price to an integer and multiply by 1000
        strike = int(asset.strike * 1000)

        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "strike": strike,  # "140000",
            "exp": expiration_str,  # "20220930",
            "right": "C" if asset.right == "CALL" else "P",
            # include_after_hours=True means extended hours (rth=false)
            # include_after_hours=False means regular hours only (rth=true)
            "rth": "false" if include_after_hours else "true"
        }
    elif asset.asset_type == "index":
        # For indexes (SPX, VIX, etc.), don't use rth parameter
        # Indexes are calculated values, not traded securities
        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl
        }
    else:
        # For stocks, respect include_after_hours parameter
        # rth=false means extended hours (pre-market + regular + after-hours)
        # rth=true means 9:30 AM - 4:00 PM ET (regular market hours only)
        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": "false" if include_after_hours else "true"
        }

    headers = {"Accept": "application/json"}

    # DEBUG-LOG: Intraday data request
    logger.debug(
        "[THETA][DEBUG][INTRADAY][REQUEST] asset=%s start=%s end=%s ivl=%d datastyle=%s include_after_hours=%s",
        asset,
        start_date,
        end_date,
        ivl,
        datastyle,
        include_after_hours
    )

    # Send the request

    json_resp = get_request(url=url, headers=headers, querystring=querystring,
                            username=username, password=password)
    if json_resp is None:
        # DEBUG-LOG: Intraday data response - no data
        logger.debug(
            "[THETA][DEBUG][INTRADAY][RESPONSE] asset=%s result=NO_DATA",
            asset
        )
        return None

    # DEBUG-LOG: Intraday data response - success
    response_rows = len(json_resp.get("response", [])) if isinstance(json_resp.get("response"), list) else 0
    logger.debug(
        "[THETA][DEBUG][INTRADAY][RESPONSE] asset=%s rows=%d",
        asset,
        response_rows
    )

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    # Remove any rows where count is 0 (no data - the prices will be 0 at these times too)
    # NOTE: Indexes always have count=0 since they're calculated values, not traded securities
    if "quote" in datastyle.lower():
        df = df[(df["bid_size"] != 0) | (df["ask_size"] != 0)]
    elif asset.asset_type != "index":
        # Don't filter indexes by count - they're always 0
        df = df[df["count"] != 0]

    if df is None or df.empty:
        return df

    # Function to combine ms_of_day and date into datetime
    def combine_datetime(row):
        # Ensure the date is in integer format and then convert to string
        date_str = str(int(row["date"]))
        base_date = datetime.strptime(date_str, "%Y%m%d")
        # v2 API returns correct start-stamped bars - no adjustment needed
        datetime_value = base_date + timedelta(milliseconds=int(row["ms_of_day"]))
        return datetime_value

    # Apply the function to each row to create a new datetime column

    # Create a new datetime column using the combine_datetime function
    datetime_combined = df.apply(combine_datetime, axis=1)

    # Assign the newly created datetime column
    df = df.assign(datetime=datetime_combined)

    # Convert the datetime column to a datetime and localize to Eastern Time
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Localize to LUMIBOT_DEFAULT_PYTZ (ThetaData returns times in ET)
    df["datetime"] = df["datetime"].dt.tz_localize(LUMIBOT_DEFAULT_PYTZ)

    # Set datetime as the index
    df = df.set_index("datetime")

    # Drop the ms_of_day and date columns
    df = df.drop(columns=["ms_of_day", "date"], errors='ignore')

    return df


def _normalize_expiration_value(raw_value: object) -> Optional[str]:
    """Convert ThetaData expiration payloads to ISO date strings."""
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return None

    if isinstance(raw_value, (int, float)):
        try:
            digits = int(raw_value)
        except (TypeError, ValueError):
            return None
        if digits <= 0:
            return None
        text = f"{digits:08d}"
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"

    text_value = str(raw_value).strip()
    if not text_value:
        return None
    if text_value.isdigit() and len(text_value) == 8:
        return f"{text_value[0:4]}-{text_value[4:6]}-{text_value[6:8]}"
    if len(text_value.split("-")) == 3:
        return text_value
    return None


def _normalize_strike_value(raw_value: object) -> Optional[float]:
    """Convert ThetaData strike payloads to float strikes in dollars."""
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return None

    try:
        strike = float(raw_value)
    except (TypeError, ValueError):
        return None

    if strike <= 0:
        return None

    # ThetaData encodes strikes in thousandths of a dollar for integer payloads
    if strike > 10000:
        strike /= 1000.0

    return round(strike, 4)


def _detect_column(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    """Find the first column name matching the provided candidates (case-insensitive)."""
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        lookup = candidate.lower()
        if lookup in normalized:
            return normalized[lookup]
    return None


def build_historical_chain(
    username: str,
    password: str,
    asset: Asset,
    as_of_date: date,
    max_expirations: int = 120,
    max_consecutive_misses: int = 10,
    chain_constraints: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, List[float]]]:
    """Build an as-of option chain by filtering live expirations against quote availability."""

    if as_of_date is None:
        raise ValueError("as_of_date must be provided to build a historical chain")

    headers = {"Accept": "application/json"}
    expirations_resp = get_request(
        url=f"{BASE_URL}/v2/list/expirations",
        headers=headers,
        querystring={"root": asset.symbol},
        username=username,
        password=password,
    )

    if not expirations_resp or not expirations_resp.get("response"):
        logger.warning(
            "ThetaData returned no expirations for %s; cannot build chain for %s.",
            asset.symbol,
            as_of_date,
        )
        return None

    exp_df = pd.DataFrame(expirations_resp["response"], columns=expirations_resp["header"]["format"])
    if exp_df.empty:
        logger.warning(
            "ThetaData returned empty expiration list for %s; cannot build chain for %s.",
            asset.symbol,
            as_of_date,
        )
        return None

    expiration_values: List[int] = sorted(int(value) for value in exp_df.iloc[:, 0].tolist())
    as_of_int = int(as_of_date.strftime("%Y%m%d"))

    constraints = chain_constraints or {}
    min_hint_date = constraints.get("min_expiration_date")
    max_hint_date = constraints.get("max_expiration_date")

    min_hint_int = (
        int(min_hint_date.strftime("%Y%m%d"))
        if isinstance(min_hint_date, date)
        else None
    )
    max_hint_int = (
        int(max_hint_date.strftime("%Y%m%d"))
        if isinstance(max_hint_date, date)
        else None
    )

    effective_start_int = as_of_int
    if min_hint_int:
        effective_start_int = max(effective_start_int, min_hint_int)

    logger.info(
        "[ThetaData] Building chain for %s @ %s (min_hint=%s, max_hint=%s, expirations=%d)",
        asset.symbol,
        as_of_date,
        min_hint_date,
        max_hint_date,
        len(expiration_values),
    )

    allowed_misses = max_consecutive_misses
    if min_hint_int:
        # Allow a deeper scan when callers request far-dated expirations (LEAPS).
        allowed_misses = max(max_consecutive_misses, 50)

    chains: Dict[str, Dict[str, List[float]]] = {"CALL": {}, "PUT": {}}
    expirations_added = 0
    consecutive_misses = 0
    hint_reached = False

    def expiration_has_data(expiration_str: str, strike_thousandths: int, right: str) -> bool:
        querystring = {
            "root": asset.symbol,
            "exp": expiration_str,
            "strike": strike_thousandths,
            "right": right,
        }
        resp = get_request(
            url=f"{BASE_URL}/list/dates/option/quote",
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )
        if not resp or resp.get("header", {}).get("error_type") == "NO_DATA":
            return False
        dates = resp.get("response", [])
        return as_of_int in dates if dates else False

    for exp_value in expiration_values:
        if exp_value < effective_start_int:
            continue
        if max_hint_int and exp_value > max_hint_int:
            logger.debug(
                "[ThetaData] Reached max hint %s for %s; stopping chain build.",
                max_hint_date,
                asset.symbol,
            )
            break
        if min_hint_int and not hint_reached and exp_value >= min_hint_int:
            hint_reached = True

        expiration_iso = _normalize_expiration_value(exp_value)
        if not expiration_iso:
            continue

        strike_resp = get_request(
            url=f"{BASE_URL}/v2/list/strikes",
            headers=headers,
            querystring={"root": asset.symbol, "exp": str(exp_value)},
            username=username,
            password=password,
        )
        if not strike_resp or not strike_resp.get("response"):
            logger.debug(
                "No strikes for %s exp %s; skipping.",
                asset.symbol,
                expiration_iso,
            )
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        strike_df = pd.DataFrame(strike_resp["response"], columns=strike_resp["header"]["format"])
        if strike_df.empty:
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        strike_values = sorted({round(value / 1000.0, 4) for value in strike_df.iloc[:, 0].tolist()})
        if not strike_values:
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        # Use the median strike to validate whether the expiration existed on the backtest date
        median_index = len(strike_values) // 2
        probe_strike = strike_values[median_index]
        probe_thousandths = int(round(probe_strike * 1000))

        has_call_data = expiration_has_data(str(exp_value), probe_thousandths, "C")
        has_put_data = has_call_data or expiration_has_data(str(exp_value), probe_thousandths, "P")

        if not (has_call_data or has_put_data):
            logger.debug(
                "Expiration %s for %s not active on %s; skipping.",
                expiration_iso,
                asset.symbol,
                as_of_date,
            )
            consecutive_misses += 1
            if consecutive_misses >= allowed_misses:
                if not min_hint_int or hint_reached:
                    logger.debug(
                        "[ThetaData] Encountered %d consecutive inactive expirations for %s (starting near %s); stopping scan.",
                        allowed_misses,
                        asset.symbol,
                        expiration_iso,
                    )
                    break
                # When we're still marching toward the requested hint, keep scanning.
                continue
            continue

        chains["CALL"][expiration_iso] = strike_values
        chains["PUT"][expiration_iso] = list(strike_values)
        expirations_added += 1
        consecutive_misses = 0

        if expirations_added >= max_expirations:
            break

    logger.debug(
        "Built ThetaData historical chain for %s on %s (expirations=%d)",
        asset.symbol,
        as_of_date,
        expirations_added,
    )

    if not chains["CALL"] and not chains["PUT"]:
        logger.warning(
            "No expirations with data found for %s on %s.",
            asset.symbol,
            as_of_date,
        )
        return None

    return {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": chains,
    }


def get_expirations(username: str, password: str, ticker: str, after_date: date):
    """Legacy helper retained for backward compatibility; prefer build_historical_chain."""
    logger.warning(
        "get_expirations is deprecated and provides live expirations only. "
        "Use build_historical_chain for historical backtests (ticker=%s, after=%s).",
        ticker,
        after_date,
    )

    url = f"{BASE_URL}/v2/list/expirations"
    querystring = {"root": ticker}
    headers = {"Accept": "application/json"}
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])
    expirations = df.iloc[:, 0].tolist()
    after_date_int = int(after_date.strftime("%Y%m%d"))
    expirations = [x for x in expirations if x >= after_date_int]
    expirations_final = []
    for expiration in expirations:
        expiration_str = str(expiration)
        expirations_final.append(f"{expiration_str[:4]}-{expiration_str[4:6]}-{expiration_str[6:]}")
    return expirations_final


def get_strikes(username: str, password: str, ticker: str, expiration: datetime):
    """
    Get a list of strike prices for the given ticker and expiration date

    Parameters
    ----------
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    ticker : str
        The ticker for the asset we are getting data for
    expiration : date
        The expiration date for the options we want

    Returns
    -------
    list[float]
        A list of strike prices for the given ticker and expiration date
    """
    # Use v2 API endpoint
    url = f"{BASE_URL}/v2/list/strikes"

    # Convert the expiration date to a string
    expiration_str = expiration.strftime("%Y%m%d")

    querystring = {"root": ticker, "exp": expiration_str}

    headers = {"Accept": "application/json"}

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    # Convert df to a list of the first (and only) column
    strikes = df.iloc[:, 0].tolist()

    # Divide each strike by 1000 to get the actual strike price
    strikes = [x / 1000.0 for x in strikes]

    return strikes


def get_chains_cached(
    username: str,
    password: str,
    asset: Asset,
    current_date: date = None
    ,
    chain_constraints: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Retrieve option chain with caching (MATCHES POLYGON PATTERN).

    This function follows the EXACT same caching strategy as Polygon:
    1. Check cache: LUMIBOT_CACHE_FOLDER/thetadata/<asset-type>/option_chains/{symbol}_{date}.parquet
    2. Reuse files within RECENT_FILE_TOLERANCE_DAYS (default 7 days)
    3. If not found, fetch from ThetaData and save to cache
    4. Use pyarrow engine with snappy compression

    Parameters
    ----------
    username : str
        ThetaData username
    password : str
        ThetaData password
    asset : Asset
        Underlying asset (e.g., Asset("SPY"))
    current_date : date
        Historical date for backtest (required)

    Returns
    -------
    dict : {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": {
            "CALL": {"2025-09-19": [140.0, 145.0, ...], ...},
            "PUT": {"2025-09-19": [140.0, 145.0, ...], ...}
        }
    }
    """
    logger.debug(f"get_chains_cached called for {asset.symbol} on {current_date}")

    # 1) If current_date is None => bail out
    if current_date is None:
        logger.debug("No current_date provided; returning None.")
        return None

    # 2) Build cache folder path
    chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "thetadata" / _resolve_asset_folder(asset) / "option_chains"
    chain_folder.mkdir(parents=True, exist_ok=True)

    constraints = chain_constraints or {}
    hint_present = any(
        constraints.get(key) is not None for key in ("min_expiration_date", "max_expiration_date")
    )

    # 3) Check for recent cached file (within RECENT_FILE_TOLERANCE_DAYS) unless hints require fresh data
    RECENT_FILE_TOLERANCE_DAYS = 7
    if not hint_present:
        earliest_okay_date = current_date - timedelta(days=RECENT_FILE_TOLERANCE_DAYS)
        pattern = f"{asset.symbol}_*.parquet"
        potential_files = sorted(chain_folder.glob(pattern), reverse=True)

        for fpath in potential_files:
            fname = fpath.stem  # e.g., "SPY_2025-09-15"
            parts = fname.split("_", maxsplit=1)
            if len(parts) != 2:
                continue
            file_symbol, date_str = parts
            if file_symbol != asset.symbol:
                continue

            try:
                file_date = date.fromisoformat(date_str)
            except ValueError:
                continue

            # If file is recent enough, reuse it
            if earliest_okay_date <= file_date <= current_date:
                logger.debug(f"Reusing chain file {fpath} (file_date={file_date})")
                df_cached = pd.read_parquet(fpath, engine='pyarrow')

                # Convert back to dict with lists (not numpy arrays)
                data = df_cached["data"][0]
                for right in data["Chains"]:
                    for exp_date in data["Chains"][right]:
                        data["Chains"][right][exp_date] = list(data["Chains"][right][exp_date])

                return data

    # 4) No suitable file => fetch from ThetaData using exp=0 chain builder
    logger.debug(
        f"No suitable cache file found for {asset.symbol} on {current_date}; building historical chain."
    )
    print(
        f"\nDownloading option chain for {asset} on {current_date}. This will be cached for future use."
    )

    chains_dict = build_historical_chain(
        username=username,
        password=password,
        asset=asset,
        as_of_date=current_date,
        chain_constraints=constraints if hint_present else None,
    )

    if chains_dict is None:
        logger.warning(
            "ThetaData returned no option data for %s on %s; skipping cache write.",
            asset.symbol,
            current_date,
        )
        return {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {}, "PUT": {}},
        }

    # 5) Save to cache file for future reuse
    cache_file = chain_folder / f"{asset.symbol}_{current_date.isoformat()}.parquet"
    df_to_cache = pd.DataFrame({"data": [chains_dict]})
    df_to_cache.to_parquet(cache_file, compression='snappy', engine='pyarrow')
    logger.debug(f"Saved chain cache: {cache_file}")

    return chains_dict
