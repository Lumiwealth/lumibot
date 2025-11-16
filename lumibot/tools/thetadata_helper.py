# This file contains helper functions for getting data from Polygon.io
import os
import hashlib
import re
import signal
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from tqdm import tqdm
from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

WAIT_TIME = 60
MAX_DAYS = 30
CACHE_SUBFOLDER = "thetadata"
DEFAULT_THETA_BASE = "http://127.0.0.1:25503"
_downloader_base_env = os.environ.get("DATADOWNLOADER_BASE_URL")
_theta_fallback_base = os.environ.get("THETADATA_BASE_URL", DEFAULT_THETA_BASE)


def _normalize_base_url(raw: Optional[str]) -> str:
    if not raw:
        return DEFAULT_THETA_BASE
    raw = raw.strip()
    if not raw:
        return DEFAULT_THETA_BASE
    if not raw.startswith(("http://", "https://")):
        raw = f"http://{raw}"
    return raw.rstrip("/")


def _is_loopback_url(raw: str) -> bool:
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _coerce_skip_flag(raw: Optional[str], base_url: str) -> bool:
    if raw:
        value = raw.strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
    if _downloader_base_env and not _is_loopback_url(base_url):
        return True
    return False


BASE_URL = _normalize_base_url(_downloader_base_env or _theta_fallback_base)
DOWNLOADER_API_KEY = os.environ.get("DATADOWNLOADER_API_KEY")
DOWNLOADER_KEY_HEADER = os.environ.get("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")
REMOTE_DOWNLOADER_ENABLED = _coerce_skip_flag(os.environ.get("DATADOWNLOADER_SKIP_LOCAL_START"), BASE_URL)
if REMOTE_DOWNLOADER_ENABLED:
    logger.info("[THETA][CONFIG] Remote downloader enabled at %s", BASE_URL)
HEALTHCHECK_SYMBOL = os.environ.get("THETADATA_HEALTHCHECK_SYMBOL", "SPY")
READINESS_ENDPOINT = "/v3/terminal/mdds/status"
READINESS_PROBES: Tuple[Tuple[str, Dict[str, str]], ...] = (
    (READINESS_ENDPOINT, {"format": "json"}),
    ("/v3/option/list/expirations", {"symbol": HEALTHCHECK_SYMBOL, "format": "json"}),
)
READINESS_TIMEOUT = float(os.environ.get("THETADATA_HEALTHCHECK_TIMEOUT", "1.0"))
CONNECTION_RETRY_SLEEP = 1.0
CONNECTION_MAX_RETRIES = 120
BOOT_GRACE_PERIOD = 5.0
MAX_RESTART_ATTEMPTS = 3
MAX_TERMINAL_RESTART_CYCLES = 3
HTTP_RETRY_LIMIT = 3
HTTP_RETRY_BACKOFF_MAX = 5.0
TRANSIENT_STATUS_CODES = {500, 502, 503, 504, 520, 521}

# Mapping between milliseconds and ThetaData interval labels
INTERVAL_MS_TO_LABEL = {
    10: "10ms",
    100: "100ms",
    500: "500ms",
    1000: "1s",
    5000: "5s",
    10000: "10s",
    15000: "15s",
    30000: "30s",
    60000: "1m",
    300000: "5m",
    600000: "10m",
    900000: "15m",
    1800000: "30m",
    3600000: "1h",
}

HISTORY_ENDPOINTS = {
    ("stock", "ohlc"): "/v3/stock/history/ohlc",
    ("stock", "quote"): "/v3/stock/history/quote",
    ("option", "ohlc"): "/v3/option/history/ohlc",
    ("option", "quote"): "/v3/option/history/quote",
    ("index", "ohlc"): "/v3/index/history/ohlc",
    ("index", "quote"): "/v3/index/history/price",
}

EOD_ENDPOINTS = {
    "stock": "/v3/stock/history/eod",
    "option": "/v3/option/history/eod",
    "index": "/v3/index/history/eod",
}

OPTION_LIST_ENDPOINTS = {
    "expirations": "/v3/option/list/expirations",
    "strikes": "/v3/option/list/strikes",
    "dates_quote": "/v3/option/list/dates/quote",
}

DEFAULT_SESSION_HOURS = {
    True: ("04:00:00", "20:00:00"),   # include extended hours
    False: ("09:30:00", "16:00:00"),  # regular session only
}


def _build_request_headers(base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    request_headers: Dict[str, str] = dict(base or {})
    if DOWNLOADER_API_KEY:
        request_headers.setdefault(DOWNLOADER_KEY_HEADER, DOWNLOADER_API_KEY)
    return request_headers


def _interval_label_from_ms(interval_ms: int) -> str:
    label = INTERVAL_MS_TO_LABEL.get(interval_ms)
    if label is None:
        raise ValueError(f"Unsupported ThetaData interval: {interval_ms} ms")
    return label


def _coerce_json_payload(payload: Any) -> Dict[str, Any]:
    """Normalize ThetaData v2/v3 payloads into {'header':{'format':[...]}, 'response': [...] }."""
    if isinstance(payload, dict):
        if "response" in payload and "header" in payload:
            return payload
        # Columnar format -> convert to rows
        columns = list(payload.keys())
        if not columns:
            return {"header": {"format": []}, "response": []}
        lengths = [len(payload[col]) for col in columns]
        length = max(lengths)
        rows: List[List[Any]] = []
        for idx in range(length):
            row = []
            for col, col_values in payload.items():
                try:
                    row.append(col_values[idx])
                except IndexError:
                    row.append(None)
            rows.append(row)
        return {"header": {"format": columns}, "response": rows}
    if isinstance(payload, list):
        return {"header": {"format": None}, "response": payload}
    return {"header": {"format": None}, "response": [payload]}


def _columnar_payload_to_records(payload: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    if not payload:
        return []
    sample_key = next(iter(payload))
    length = len(payload[sample_key])
    records: List[Dict[str, Any]] = []
    for idx in range(length):
        row = {}
        for key, values in payload.items():
            try:
                row[key] = values[idx]
            except IndexError:
                raise ValueError(f"Column '{key}' length mismatch in ThetaData response")
        records.append(row)
    return records


def _localize_timestamps(series: pd.Series) -> pd.DatetimeIndex:
    dt_index = pd.to_datetime(series, errors="coerce")
    tz = LUMIBOT_DEFAULT_PYTZ
    if getattr(dt_index.dt, "tz", None) is None:
        return dt_index.dt.tz_localize(tz)
    return dt_index.dt.tz_convert(tz)


def _format_time(value: datetime) -> str:
    return value.strftime("%H:%M:%S")


def _compute_session_bounds(
    day: date,
    start_dt: datetime,
    end_dt: datetime,
    include_after_hours: bool,
    prefer_full_session: bool = False,
) -> Tuple[str, str]:
    default_start, default_end = DEFAULT_SESSION_HOURS[include_after_hours]
    tz = LUMIBOT_DEFAULT_PYTZ
    start_default = datetime.combine(day, datetime.strptime(default_start, "%H:%M:%S").time(), tz)
    end_default = datetime.combine(day, datetime.strptime(default_end, "%H:%M:%S").time(), tz)

    session_start = start_default
    session_end = end_default

    if not prefer_full_session:
        if start_dt.date() == day:
            session_start = start_dt
        if end_dt.date() == day:
            session_end = end_dt

    if session_end < session_start:
        session_end = session_start

    return _format_time(session_start), _format_time(session_end)


def _normalize_market_datetime(value: datetime) -> datetime:
    """Ensure datetimes are timezone-aware in the default market timezone."""
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if value.tzinfo is None:
        return LUMIBOT_DEFAULT_PYTZ.localize(value)
    return value.astimezone(LUMIBOT_DEFAULT_PYTZ)


def _format_option_strike(strike: float) -> str:
    """Format strikes for v3 requests (decimal string expected)."""
    text = f"{strike:.3f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _extract_timestamp_series(
    df: pd.DataFrame,
    target_tz: timezone = LUMIBOT_DEFAULT_PYTZ,
) -> Tuple[Optional[pd.Series], List[str]]:
    """Return a timezone-localized timestamp series plus the source columns to drop."""
    drop_cols: List[str] = []
    timestamp_col = _detect_column(df, ("timestamp", "datetime", "time"))
    if timestamp_col:
        ts_series = pd.to_datetime(df[timestamp_col], errors="coerce")
        if getattr(ts_series.dt, "tz", None) is None:
            ts_series = ts_series.dt.tz_localize(target_tz)
        else:
            ts_series = ts_series.dt.tz_convert(target_tz)
        drop_cols.append(timestamp_col)
        return ts_series, drop_cols

    date_col = _detect_column(df, ("date",))
    ms_col = _detect_column(df, ("ms_of_day", "msOfDay", "ms_of_day2"))
    if date_col and ms_col:
        date_series = pd.to_datetime(df[date_col].astype(str), format="%Y%m%d", errors="coerce")
        ms_series = pd.to_timedelta(pd.to_numeric(df[ms_col], errors="coerce").fillna(0), unit="ms")
        ts_series = date_series + ms_series
        if getattr(ts_series.dt, "tz", None) is None:
            ts_series = ts_series.dt.tz_localize(target_tz)
        else:
            ts_series = ts_series.dt.tz_convert(target_tz)
        drop_cols.extend([date_col, ms_col])
        return ts_series, drop_cols

    return None, drop_cols


def _finalize_history_dataframe(
    df: pd.DataFrame,
    datastyle: str,
    asset: Asset,
    target_tz: timezone = LUMIBOT_DEFAULT_PYTZ,
) -> Optional[pd.DataFrame]:
    """Apply timestamp indexing and basic filtering so legacy callers keep working."""
    if df is None or df.empty:
        return df

    df = df.copy()
    ts_series, drop_cols = _extract_timestamp_series(df, target_tz=target_tz)
    if ts_series is not None:
        df = df.assign(datetime=ts_series)
        df = df.drop(columns=drop_cols, errors="ignore")
        df = df[~df["datetime"].isna()]
        if df.empty:
            return df
        df = df.set_index("datetime")
        datastyle_key = (datastyle or "").lower()
        index_series = pd.Series(df.index, index=df.index)

        def _empty_timestamp_series() -> pd.Series:
            return pd.Series(pd.NaT, index=df.index, dtype=index_series.dtype)

        if datastyle_key == "ohlc":
            df["last_trade_time"] = index_series
            df["last_bid_time"] = _empty_timestamp_series()
            df["last_ask_time"] = _empty_timestamp_series()
        elif datastyle_key == "quote":
            df["last_trade_time"] = _empty_timestamp_series()
            df["last_bid_time"] = index_series
            df["last_ask_time"] = index_series

    if df.empty:
        return df

    if "quote" in datastyle.lower():
        bid_col = df.get("bid")
        ask_col = df.get("ask")
        if bid_col is not None and ask_col is not None:
            valid_prices_mask = ((bid_col > 0) | (ask_col > 0)).fillna(False)
            df = df[valid_prices_mask]
    elif str(getattr(asset, "asset_type", "")).lower() != "index":
        count_col = _detect_column(df, ("count",))
        if count_col and count_col in df.columns:
            df = df[df[count_col] != 0]

    drop_candidates = ["ms_of_day", "ms_of_day2", "date", "timestamp"]
    df = df.drop(columns=[c for c in drop_candidates if c in df.columns], errors="ignore")

    if df.empty:
        return df

    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    return df


def _terminal_http_alive(timeout: float = 0.3) -> bool:
    """Return True if the local ThetaTerminal responds to HTTP."""
    request_headers = _build_request_headers()
    for endpoint, params in READINESS_PROBES:
        try:
            resp = requests.get(
                f"{BASE_URL}{endpoint}",
                headers=request_headers,
                params=params,
                timeout=timeout,
            )
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            continue
    return False


def _probe_terminal_ready(timeout: float = READINESS_TIMEOUT) -> bool:
    request_headers = _build_request_headers()
    for endpoint, params in READINESS_PROBES:
        request_url = f"{BASE_URL}{endpoint}"
        if params:
            try:
                request_url = f"{request_url}?{urlencode(params)}"
            except Exception:
                pass
        try:
            resp = requests.get(
                request_url,
                headers=request_headers,
                timeout=timeout,
            )
        except Exception:
            continue

        status_code = getattr(resp, "status_code", 200)
        body_text = getattr(resp, "text", "") or ""
        normalized_text = body_text.strip().upper()

        if status_code == 200:
            if "status" in endpoint:
                if not normalized_text or normalized_text in {"CONNECTED", "READY", "OK"}:
                    return True
                # Explicit non-ready signal from status endpoint.
                return False
            else:
                return True

        if status_code == 571 or "SERVER_STARTING" in normalized_text:
            return False
        if status_code in (404, 410):
            continue
        if status_code in (471, 473):
            logger.error(
                "ThetaData readiness probe %s failed with %s: %s",
                endpoint,
                status_code,
                body_text,
            )
    return False


def _ensure_java_runtime(min_major: int = 21) -> None:
    """Ensure a supported Java runtime exists before starting ThetaTerminal."""
    import shutil
    import subprocess

    java_path = shutil.which("java")
    if not java_path:
        raise RuntimeError("Java runtime not found. Install Java 21+ to run ThetaTerminal.")

    try:
        proc = subprocess.run(
            [java_path, "-version"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to execute '{java_path} -version': {exc}") from exc

    version_output = (proc.stderr or proc.stdout or "").splitlines()
    first_line = version_output[0] if version_output else ""
    match = re.search(r"\"(\d+(?:\.\d+)*)\"", first_line)
    version_str = match.group(1) if match else ""
    major_part = version_str.split(".")[0] if version_str else ""
    if major_part == "1" and len(version_str.split(".")) > 1:
        major_part = version_str.split(".")[1]

    try:
        major = int(major_part)
    except (TypeError, ValueError):
        major = None

    if major is None or major < min_major:
        raise RuntimeError(
            f"ThetaData requires Java {min_major}+; detected version '{first_line or 'unknown'}'."
        )


def _request_terminal_shutdown() -> bool:
    """Best-effort request to stop ThetaTerminal via its REST control endpoint."""
    shutdown_paths = (
        "/v3/terminal/shutdown",
        "/v3/system/terminal/shutdown",  # legacy fallback path
    )
    for path in shutdown_paths:
        shutdown_url = f"{BASE_URL}{path}"
        try:
            resp = requests.get(shutdown_url, timeout=1)
        except Exception:
            continue
        status_code = getattr(resp, "status_code", 200)
        if status_code < 500:
            return True
    return False


def shutdown_theta_terminal(timeout: float = 30.0, force: bool = True) -> bool:
    """Request ThetaTerminal shutdown and wait until the process fully exits."""
    global THETA_DATA_PID

    if REMOTE_DOWNLOADER_ENABLED:
        return True

    if not is_process_alive() and not _terminal_http_alive(timeout=0.2):
        reset_theta_terminal_tracking()
        return True

    graceful_requested = _request_terminal_shutdown()
    deadline = time.monotonic() + max(timeout, 0.0)

    while time.monotonic() < deadline:
        process_alive = is_process_alive()
        status_alive = _terminal_http_alive(timeout=0.2)
        if not process_alive and not status_alive:
            reset_theta_terminal_tracking()
            if graceful_requested:
                logger.info("ThetaTerminal shut down gracefully.")
            return True
        time.sleep(0.5)

    if not force:
        logger.warning("ThetaTerminal did not exit within %.1fs; leaving process running.", timeout)
        return False

    kill_pid = THETA_DATA_PID
    if kill_pid:
        kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
        try:
            os.kill(kill_pid, kill_signal)
            logger.warning("Force killed ThetaTerminal PID %s after timeout.", kill_pid)
        except Exception as exc:
            logger.warning("Failed to force kill ThetaTerminal PID %s: %s", kill_pid, exc)
    else:
        logger.warning("ThetaTerminal PID unavailable; cannot force kill after shutdown timeout.")

    reset_theta_terminal_tracking()
    return True


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


class ThetaDataSessionInvalidError(ThetaDataConnectionError):
    """Raised when ThetaTerminal keeps returning BadSession responses after a restart."""

    pass


class ThetaRequestError(ValueError):
    """Raised when repeated ThetaData HTTP requests fail with transient errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, body: Optional[str] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body

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
        if df_all is not None and len(df_all) > 0:
            logger.debug(
                "[THETA][DEBUG][RETURN][PANDAS] asset=%s rows=%d first_ts=%s last_ts=%s",
                asset,
                len(df_all),
                df_all.index.min().isoformat(),
                df_all.index.max().isoformat()
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

    if REMOTE_DOWNLOADER_ENABLED:
        # Remote downloader handles lifecycle; treat as always alive locally.
        return True

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
    global THETA_DATA_PROCESS, THETA_DATA_PID
    CONNECTION_DIAGNOSTICS["start_terminal_calls"] += 1

    if REMOTE_DOWNLOADER_ENABLED:
        logger.debug("Remote Theta downloader configured; skipping local ThetaTerminal launch.")
        return None

    shutdown_theta_terminal(timeout=30.0, force=True)

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
    _ensure_java_runtime()

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

    try:
        jar_stats = jar_file.stat()
        jar_mtime = datetime.fromtimestamp(jar_stats.st_mtime).isoformat()
        jar_size_mb = jar_stats.st_size / (1024 * 1024)
        jar_hash = hashlib.sha256(jar_file.read_bytes()).hexdigest()
        logger.info(
            "Using ThetaTerminal jar at %s (%.2f MB, mtime %s, sha256=%s)",
            jar_file,
            jar_size_mb,
            jar_mtime,
            jar_hash[:16],
        )
    except Exception as exc:
        logger.warning("Unable to fingerprint ThetaTerminal jar %s: %s", jar_file, exc)

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
    # The connection will be established via HTTP on 127.0.0.1:25503 (and FPSS WebSocket on 25520)
    return THETA_DATA_PROCESS

def check_connection(username: str, password: str, wait_for_connection: bool = False):
    """Ensure ThetaTerminal is running and responsive."""

    CONNECTION_DIAGNOSTICS["check_connection_calls"] += 1

    if REMOTE_DOWNLOADER_ENABLED:
        retries = 0
        if not wait_for_connection and _probe_terminal_ready():
            return None, True
        while retries < CONNECTION_MAX_RETRIES:
            if _probe_terminal_ready():
                return None, True
            retries += 1
            time.sleep(CONNECTION_RETRY_SLEEP)
        raise ThetaDataConnectionError("Remote Theta downloader did not become ready in time.")

    def ensure_process(force_restart: bool = False):
        alive = is_process_alive()
        if alive and not force_restart:
            return
        if alive and force_restart:
            logger.warning("ThetaTerminal unresponsive; restarting process.")
            try:
                _request_terminal_shutdown()
            except Exception:
                pass
        logger.info("ThetaTerminal process not found; attempting restart.")
        start_theta_data_client(username=username, password=password)
        CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1

    if not wait_for_connection:
        if _probe_terminal_ready():
            if not is_process_alive():
                ensure_process()
                return check_connection(username=username, password=password, wait_for_connection=True)
            return None, True
        ensure_process(force_restart=True)
        return check_connection(username=username, password=password, wait_for_connection=True)

    retries = 0
    while retries < CONNECTION_MAX_RETRIES:
        if _probe_terminal_ready():
            if not is_process_alive():
                ensure_process()
                retries += 1
                time.sleep(CONNECTION_RETRY_SLEEP)
                continue
            return None, True

        ensure_process(force_restart=True)
        retries += 1
        time.sleep(CONNECTION_RETRY_SLEEP)

    raise ThetaDataConnectionError("ThetaTerminal did not become ready in time.")

def get_request(url: str, headers: dict, querystring: dict, username: str, password: str):
    all_responses = []
    next_page_url = None
    page_count = 0
    consecutive_disconnects = 0
    restart_budget = 3
    querystring = dict(querystring or {})
    querystring.setdefault("format", "json")
    session_reset_budget = 5
    session_reset_in_progress = False
    awaiting_session_validation = False
    http_retry_limit = HTTP_RETRY_LIMIT
    last_status_code: Optional[int] = None
    last_failure_detail: Optional[str] = None

    # Lightweight liveness probe before issuing the request
    check_connection(username=username, password=password, wait_for_connection=False)

    while True:
        counter = 0
        # Use next_page URL if available, otherwise use original URL with querystring
        request_url = next_page_url if next_page_url else url
        request_params = None if next_page_url else querystring
        json_resp = None

        while True:
            sleep_duration = 0.0
            try:
                CONNECTION_DIAGNOSTICS["network_requests"] += 1

                # DEBUG-LOG: API request
                logger.debug(
                    "[THETA][DEBUG][API][REQUEST] url=%s params=%s",
                    request_url if next_page_url else url,
                    request_params if request_params else querystring
                )

                request_headers = _build_request_headers(headers)

                response = requests.get(
                    request_url,
                    headers=request_headers,
                    params=request_params,
                    timeout=30,
                )
                status_code = response.status_code
                # Status code 472 means "No data" - this is valid, return None
                if status_code == 472:
                    logger.warning(f"No data available for request: {response.text[:200]}")
                    logger.debug("[THETA][DEBUG][API][RESPONSE] status=472 result=NO_DATA")
                    consecutive_disconnects = 0
                    session_reset_in_progress = False
                    awaiting_session_validation = False
                    return None
                elif status_code == 571:
                    logger.debug("ThetaTerminal reports SERVER_STARTING; waiting before retry.")
                    check_connection(username=username, password=password, wait_for_connection=True)
                    time.sleep(CONNECTION_RETRY_SLEEP)
                    continue
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
                elif status_code == 500 and "BadSession" in (response.text or ""):
                    if awaiting_session_validation:
                        logger.error(
                            "ThetaTerminal still reports BadSession immediately after a clean restart; manual intervention required."
                        )
                        raise ThetaDataSessionInvalidError(
                            "ThetaData session remained invalid after a clean restart."
                        )
                    if not session_reset_in_progress:
                        if session_reset_budget <= 0:
                            raise ValueError("ThetaData session invalid after multiple restarts.")
                        session_reset_budget -= 1
                        session_reset_in_progress = True
                        logger.warning(
                            "ThetaTerminal session invalid; restarting (remaining attempts=%s).",
                            session_reset_budget,
                        )
                        restart_started = time.monotonic()
                        start_theta_data_client(username=username, password=password)
                        CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1
                        while True:
                            try:
                                check_connection(username=username, password=password, wait_for_connection=True)
                                break
                            except ThetaDataConnectionError as exc:
                                logger.warning("Waiting for ThetaTerminal after restart: %s", exc)
                                time.sleep(CONNECTION_RETRY_SLEEP)
                        wait_elapsed = time.monotonic() - restart_started
                        logger.info(
                            "ThetaTerminal restarted after BadSession (pid=%s, wait=%.1fs).",
                            THETA_DATA_PID,
                            wait_elapsed,
                        )
                    else:
                        logger.warning("ThetaTerminal session still stabilizing after restart; waiting to retry request.")
                        try:
                            check_connection(username=username, password=password, wait_for_connection=True)
                        except ThetaDataConnectionError as exc:
                            logger.warning("ThetaTerminal unavailable while waiting for session reset: %s", exc)
                            time.sleep(CONNECTION_RETRY_SLEEP)
                            continue
                    time.sleep(max(CONNECTION_RETRY_SLEEP, 5))
                    next_page_url = None
                    request_url = url
                    request_params = querystring
                    consecutive_disconnects = 0
                    counter = 0
                    json_resp = None
                    awaiting_session_validation = True
                    continue
                elif status_code == 410:
                    raise RuntimeError(
                        "ThetaData responded with 410 GONE. Ensure all requests use the v3 REST endpoints "
                        "on http://127.0.0.1:25503/v3/..."
                    )
                elif status_code in (471, 473, 476):
                    raise RuntimeError(
                        f"ThetaData request rejected with status {status_code}: {response.text.strip()[:500]}"
                    )
                # If status code is not 200, then we are not connected
                elif status_code != 200:
                    logged_params = request_params if request_params is not None else querystring
                    logger.warning(
                        "Non-200 status code %s for ThetaData request %s params=%s body=%s (attempt %s/%s)",
                        status_code,
                        request_url,
                        logged_params,
                        response.text[:200],
                        counter + 1,
                        http_retry_limit,
                    )
                    last_status_code = status_code
                    last_failure_detail = response.text[:200]
                    # DEBUG-LOG: API response - error
                    logger.debug(
                        "[THETA][DEBUG][API][RESPONSE] status=%d result=ERROR url=%s",
                        status_code,
                        request_url,
                    )
                    check_connection(username=username, password=password, wait_for_connection=True)
                    consecutive_disconnects = 0
                    sleep_duration = min(
                        CONNECTION_RETRY_SLEEP * max(counter + 1, 1),
                        HTTP_RETRY_BACKOFF_MAX,
                    )
                else:
                    json_payload = response.json()
                    json_resp = _coerce_json_payload(json_payload)
                    session_reset_in_progress = False
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
            except RuntimeError:
                raise
            except Exception as e:
                logger.warning(f"Exception during request (attempt {counter + 1}): {e}")
                check_connection(username=username, password=password, wait_for_connection=True)
                last_status_code = None
                last_failure_detail = str(e)
                if counter == 0:
                    logger.debug("[THETA][DEBUG][API][WAIT] Allowing ThetaTerminal to initialize for 5s before retry.")
                    time.sleep(5)

            counter += 1
            if counter >= http_retry_limit:
                raise ThetaRequestError(
                    "Cannot connect to Theta Data!",
                    status_code=last_status_code,
                    body=last_failure_detail,
                )
            if sleep_duration > 0:
                logger.debug(
                    "[THETA][DEBUG][API][WAIT] Sleeping %.2fs before retry (attempt %d/%d).",
                    sleep_duration,
                    counter + 1,
                    http_retry_limit,
                )
                time.sleep(sleep_duration)
        if json_resp is None:
            continue

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
    Get EOD (End of Day) data from ThetaData using the /v3/.../history/eod endpoints.

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

    asset_type = str(getattr(asset, "asset_type", "stock")).lower()
    endpoint = EOD_ENDPOINTS.get(asset_type)
    if endpoint is None:
        raise ValueError(f"Unsupported asset_type '{asset_type}' for ThetaData EOD history")

    url = f"{BASE_URL}{endpoint}"

    base_query = {
        "symbol": asset.symbol,
    }

    if asset_type == "option":
        if not asset.expiration or asset.strike is None:
            raise ValueError(f"Option asset {asset} missing expiration or strike for EOD request")
        base_query["expiration"] = asset.expiration.strftime("%Y-%m-%d")
        base_query["strike"] = _format_option_strike(float(asset.strike))
        right = str(getattr(asset, "right", "CALL")).upper()
        base_query["right"] = "call" if right.startswith("C") else "put"

    headers = {"Accept": "application/json"}

    # Convert to date objects for chunking
    start_day = datetime.strptime(start_date, "%Y%m%d").date()
    end_day = datetime.strptime(end_date, "%Y%m%d").date()
    max_span = timedelta(days=364)

    def _chunk_windows():
        cursor = start_day
        while cursor <= end_day:
            window_end = min(cursor + max_span, end_day)
            yield cursor, window_end
            cursor = window_end + timedelta(days=1)

    def _execute_chunk_request(chunk_start: date, chunk_end: date):
        querystring = base_query.copy()
        querystring["start_date"] = chunk_start.strftime("%Y-%m-%d")
        querystring["end_date"] = chunk_end.strftime("%Y-%m-%d")

        logger.debug(
            "[THETA][DEBUG][EOD][REQUEST][CHUNK] asset=%s start=%s end=%s",
            asset,
            querystring["start_date"],
            querystring["end_date"],
        )

        return get_request(
            url=url,
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )

    def _collect_chunk_payloads(chunk_start: date, chunk_end: date, *, allow_split: bool = True) -> List[Optional[Dict[str, Any]]]:
        try:
            response = _execute_chunk_request(chunk_start, chunk_end)
            return [response]
        except ThetaRequestError as exc:
            span_days = (chunk_end - chunk_start).days + 1
            if not allow_split or span_days <= 1:
                raise
            midpoint = chunk_start + timedelta(days=(span_days // 2) - 1)
            right_start = midpoint + timedelta(days=1)
            logger.warning(
                "[THETA][WARN][EOD][CHUNK] asset=%s start=%s end=%s status=%s retrying with split windows",
                asset,
                chunk_start,
                chunk_end,
                exc.status_code,
            )
            split_payloads: List[Optional[Dict[str, Any]]] = []
            splits = (
                (chunk_start, min(midpoint, chunk_end)),
                (min(right_start, chunk_end), chunk_end),
            )
            for split_idx, (split_start, split_end) in enumerate(splits, start=1):
                if split_start > split_end:
                    continue
                logger.debug(
                    "[THETA][DEBUG][EOD][REQUEST][CHUNK][SPLIT] asset=%s parent=%s-%s split=%d start=%s end=%s",
                    asset,
                    chunk_start,
                    chunk_end,
                    split_idx,
                    split_start,
                    split_end,
                )
                try:
                    split_payloads.extend(
                        _collect_chunk_payloads(split_start, split_end, allow_split=False)
                    )
                except ThetaRequestError as sub_exc:
                    logger.error(
                        "[THETA][ERROR][EOD][CHUNK][SPLIT] asset=%s parent=%s-%s split=%d failed status=%s",
                        asset,
                        chunk_start,
                        chunk_end,
                        split_idx,
                        sub_exc.status_code,
                    )
                    raise
            return split_payloads

    aggregated_rows: List[List[Any]] = []
    header_format: Optional[List[str]] = None
    windows = list(_chunk_windows())

    # DEBUG-LOG: EOD data request (overall)
    logger.debug(
        "[THETA][DEBUG][EOD][REQUEST] asset=%s start=%s end=%s datastyle=%s chunks=%d",
        asset,
        start_date,
        end_date,
        datastyle,
        len(windows)
    )

    for idx, (window_start, window_end) in enumerate(windows, start=1):
        logger.debug(
            "[THETA][DEBUG][EOD][REQUEST][CHUNK] asset=%s chunk=%d/%d start=%s end=%s",
            asset,
            idx,
            len(windows),
            window_start,
            window_end,
        )

        try:
            chunk_payloads = _collect_chunk_payloads(window_start, window_end)
        except ThetaRequestError as exc:
            logger.error(
                "[THETA][ERROR][EOD][CHUNK] asset=%s chunk=%d/%d start=%s end=%s status=%s detail=%s",
                asset,
                idx,
                len(windows),
                window_start,
                window_end,
                exc.status_code,
                exc.body,
            )
            raise
        except ValueError as exc:
            logger.error(
                "[THETA][ERROR][EOD][CHUNK] asset=%s chunk=%d/%d start=%s end=%s error=%s",
                asset,
                idx,
                len(windows),
                window_start,
                window_end,
                exc,
            )
            raise

        for json_resp in chunk_payloads:
            if not json_resp:
                continue

            response_rows = json_resp.get("response") or []
            if response_rows:
                aggregated_rows.extend(response_rows)
            if not header_format and json_resp.get("header", {}).get("format"):
                header_format = json_resp["header"]["format"]

            logger.debug(
                "[THETA][DEBUG][EOD][RESPONSE][CHUNK] asset=%s chunk=%d/%d rows=%d",
                asset,
                idx,
                len(windows),
                len(response_rows),
            )

    if not aggregated_rows or not header_format:
        logger.debug(
            "[THETA][DEBUG][EOD][RESPONSE] asset=%s result=NO_DATA",
            asset
        )
        return None

    # DEBUG-LOG: EOD data response - success
    logger.debug(
        "[THETA][DEBUG][EOD][RESPONSE] asset=%s rows=%d chunks=%d",
        asset,
        len(aggregated_rows),
        len(windows),
    )

    # Convert to pandas dataframe
    df = pd.DataFrame(aggregated_rows, columns=header_format)

    if df is None or df.empty:
        return df

    def combine_datetime(row):
        created_value = row.get("created") or row.get("last_trade")
        if not created_value:
            raise KeyError("ThetaData EOD response missing 'created' timestamp")
        dt_value = pd.to_datetime(created_value, utc=True, errors="coerce")
        if pd.isna(dt_value):
            raise KeyError("ThetaData EOD response provided invalid 'created' timestamp")
        base_date = datetime(dt_value.year, dt_value.month, dt_value.day)
        # EOD reports represent the trading day; use midnight of that day for indexing.
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
    if asset_type in ("stock", "option"):
        logger.info(f"Fetching 9:30 AM minute bars to correct EOD open prices...")

        # Get minute data for the date range to extract 9:30 AM opens
        minute_df = None
        correction_window = ("09:30:00", "09:31:00")
        try:
            minute_df = get_historical_data(
                asset=asset,
                start_dt=start_dt,
                end_dt=end_dt,
                ivl=60000,  # 1 minute
                username=username,
                password=password,
                datastyle=datastyle,
                include_after_hours=False,  # RTH only
                session_time_override=correction_window,
            )
        except ThetaRequestError as exc:
            body_text = (exc.body or "").lower()
            if "start must be before end" in body_text:
                logger.warning(
                    "ThetaData rejected 09:30 correction window for %s; skipping open fix this chunk (%s)",
                    asset.symbol,
                    exc.body,
                )
            else:
                raise

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


def get_historical_data(
    asset: Asset,
    start_dt: datetime,
    end_dt: datetime,
    ivl: int,
    username: str,
    password: str,
    datastyle: str = "ohlc",
    include_after_hours: bool = True,
    session_time_override: Optional[Tuple[str, str]] = None,
):
    """
    Fetch intraday history from ThetaData using the v3 REST endpoints.

    Parameters
    ----------
    session_time_override : Optional[Tuple[str, str]]
        When provided, overrides the computed start/end session times for each trading day
        (HH:MM:SS strings). Useful for requesting specific minute windows such as the 09:30 open.
    """

    asset_type = str(getattr(asset, "asset_type", "stock")).lower()
    endpoint = HISTORY_ENDPOINTS.get((asset_type, datastyle))
    if endpoint is None:
        raise ValueError(f"Unsupported ThetaData history request ({asset_type}, {datastyle})")

    interval_label = _interval_label_from_ms(ivl)
    url = f"{BASE_URL}{endpoint}"
    headers = {"Accept": "application/json"}

    start_is_date_only = isinstance(start_dt, date) and not isinstance(start_dt, datetime)
    end_is_date_only = isinstance(end_dt, date) and not isinstance(end_dt, datetime)

    start_local = _normalize_market_datetime(start_dt)
    end_local = _normalize_market_datetime(end_dt)
    trading_days = get_trading_dates(asset, start_dt, end_dt)

    if not trading_days:
        logger.debug(
            "[THETA][DEBUG][INTRADAY][NO_DAYS] asset=%s start=%s end=%s",
            asset,
            start_dt,
            end_dt,
        )
        return None

    logger.debug(
        "[THETA][DEBUG][INTRADAY][REQUEST] asset=%s start=%s end=%s ivl=%d datastyle=%s include_after_hours=%s",
        asset,
        start_dt,
        end_dt,
        ivl,
        datastyle,
        include_after_hours,
    )

    def build_option_params() -> Dict[str, str]:
        if not asset.expiration:
            raise ValueError(f"Expiration date missing for option asset {asset}")
        if asset.strike is None:
            raise ValueError(f"Strike missing for option asset {asset}")
        right = str(getattr(asset, "right", "CALL")).upper()
        return {
            "symbol": asset.symbol,
            "expiration": asset.expiration.strftime("%Y-%m-%d"),
            "strike": _format_option_strike(float(asset.strike)),
            "right": "call" if right.startswith("C") else "put",
        }

    if asset_type == "index" and datastyle == "ohlc":
        querystring = {
            "symbol": asset.symbol,
            "start_date": start_local.strftime("%Y-%m-%d"),
            "end_date": end_local.strftime("%Y-%m-%d"),
            "interval": interval_label,
        }
        json_resp = get_request(
            url=url,
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )
        if not json_resp:
            return None
        df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])
        return _finalize_history_dataframe(df, datastyle, asset)

    frames: List[pd.DataFrame] = []
    option_params = build_option_params() if asset_type == "option" else None

    for trading_day in trading_days:
        querystring: Dict[str, Any] = {
            "symbol": asset.symbol,
            "date": trading_day.strftime("%Y-%m-%d"),
            "interval": interval_label,
        }
        if option_params:
            querystring.update(option_params)
        if asset_type == "index":
            # Index quote/price endpoint expects 'date' per request similar to options/stocks
            querystring.pop("symbol", None)
            querystring["symbol"] = asset.symbol

        if session_time_override:
            session_start, session_end = session_time_override
        else:
            session_start, session_end = _compute_session_bounds(
                trading_day,
                start_local,
                end_local,
                include_after_hours,
                prefer_full_session=start_is_date_only and end_is_date_only,
            )
        querystring["start_time"] = session_start
        querystring["end_time"] = session_end

        json_resp = get_request(
            url=url,
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )
        if not json_resp:
            continue

        df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])
        df = _finalize_history_dataframe(df, datastyle, asset)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        logger.debug("[THETA][DEBUG][INTRADAY][EMPTY_RESULT] asset=%s", asset)
        return None

    result = pd.concat(frames).sort_index()
    result = result[~result.index.duplicated(keep="last")]
    return result


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
    for normalized_name, original in normalized.items():
        for candidate in candidates:
            lookup = candidate.lower()
            if lookup in normalized_name:
                return original
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
        url=f"{BASE_URL}{OPTION_LIST_ENDPOINTS['expirations']}",
        headers=headers,
        querystring={"symbol": asset.symbol},
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

    expiration_col = _detect_column(exp_df, ("expiration", "exp", "date"))
    if not expiration_col:
        logger.warning("ThetaData expiration payload missing expected columns for %s.", asset.symbol)
        return None

    expiration_values: List[str] = []
    for raw_value in exp_df[expiration_col].tolist():
        normalized = _normalize_expiration_value(raw_value)
        if normalized:
            expiration_values.append(normalized)
    expiration_values = sorted({value for value in expiration_values})

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

    def expiration_has_data(expiration_iso: str, strike_value: float, right: str) -> bool:
        expiration_param = expiration_iso
        querystring = {
            "symbol": asset.symbol,
            "expiration": expiration_param,
            "strike": strike_value,
            "right": "call" if right == "C" else "put",
            "format": "json",
        }
        resp = get_request(
            url=f"{BASE_URL}{OPTION_LIST_ENDPOINTS['dates_quote']}",
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )
        if not resp or resp.get("header", {}).get("error_type") == "NO_DATA":
            return False
        dates = []
        data_rows = resp.get("response", [])
        if data_rows and isinstance(data_rows[0], (list, tuple)):
            # Responses converted via _coerce_json_payload
            date_idx = 0
            dates = [row[date_idx] for row in data_rows]
        elif data_rows:
            dates = data_rows
        ints = []
        for date_value in dates:
            if not date_value:
                continue
            try:
                ints.append(int(str(date_value).replace("-", "")[:8]))
            except ValueError:
                continue
        return as_of_int in ints

    for expiration_iso in expiration_values:
        expiration_int = int(expiration_iso.replace("-", ""))
        if expiration_int < effective_start_int:
            continue
        if max_hint_int and expiration_int > max_hint_int:
            logger.debug(
                "[ThetaData] Reached max hint %s for %s; stopping chain build.",
                max_hint_date,
                asset.symbol,
            )
            break
        if min_hint_int and not hint_reached and expiration_int >= min_hint_int:
            hint_reached = True

        strike_resp = get_request(
            url=f"{BASE_URL}{OPTION_LIST_ENDPOINTS['strikes']}",
            headers=headers,
            querystring={
                "symbol": asset.symbol,
                "expiration": expiration_iso,
                "format": "json",
            },
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

        strike_col = _detect_column(strike_df, ("strike",))
        if not strike_col:
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        strike_values = sorted(
            {
                strike
                for strike in (
                    _normalize_strike_value(value) for value in strike_df[strike_col].tolist()
                )
                if strike
            }
        )
        if not strike_values:
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        # Use the median strike to validate whether the expiration existed on the backtest date
        median_index = len(strike_values) // 2
        probe_strike = strike_values[median_index]

        has_call_data = expiration_has_data(expiration_iso, probe_strike, "C")
        has_put_data = has_call_data or expiration_has_data(expiration_iso, probe_strike, "P")

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

    url = f"{BASE_URL}{OPTION_LIST_ENDPOINTS['expirations']}"
    querystring = {"symbol": ticker}
    headers = {"Accept": "application/json"}
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])
    expiration_col = _detect_column(df, ("expiration", "date", "exp"))
    if not expiration_col:
        return []
    after_date_int = int(after_date.strftime("%Y%m%d"))
    expirations_final: List[str] = []
    for raw_value in df[expiration_col].tolist():
        normalized = _normalize_expiration_value(raw_value)
        if not normalized:
            continue
        try:
            normalized_int = int(normalized.replace("-", ""))
        except (TypeError, ValueError):
            continue
        if normalized_int >= after_date_int:
            expirations_final.append(normalized)
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
    url = f"{BASE_URL}{OPTION_LIST_ENDPOINTS['strikes']}"

    expiration_iso = expiration.strftime("%Y-%m-%d")
    querystring = {"symbol": ticker, "expiration": expiration_iso, "format": "json"}

    headers = {"Accept": "application/json"}

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    strike_col = _detect_column(df, ("strike",))
    if not strike_col:
        return []

    strikes = []
    for raw in df[strike_col].tolist():
        strike = _normalize_strike_value(raw)
        if strike:
            strikes.append(strike)

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
