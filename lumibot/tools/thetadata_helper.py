# This file contains helper functions for getting data from Polygon.io
import os
import functools
import hashlib
import json
import random
import re
import signal
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests
from dateutil import parser as dateutil_parser
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from tqdm import tqdm
from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

# ==============================================================================
# Download Status Tracking
# ==============================================================================
# This module tracks the current download status for ThetaData fetches.
# The status is exposed via get_download_status() for progress reporting.
#
# NOTE: This pattern can be extended to other data sources (Yahoo, Polygon, etc.)
# by implementing similar tracking in their respective helper modules.
# See BACKTESTING_ARCHITECTURE.md for documentation on extending this.
# ==============================================================================

# Thread-safe lock for download status updates
_download_status_lock = threading.Lock()

# Current download status - updated during active fetches
_download_status = {
    "active": False,
    "asset": None,        # Asset.to_minimal_dict() of what's being downloaded
    "quote": None,        # Quote asset symbol (e.g., "USD")
    "data_type": None,    # Type of data being fetched (e.g., "ohlc")
    "timespan": None,     # Timespan (e.g., "minute", "day")
    "progress": 0,        # Progress percentage (0-100)
    "current": 0,         # Current chunk number
    "total": 0,           # Total chunks to download
}


def get_download_status() -> dict:
    """
    Get the current ThetaData download status.

    Returns a dictionary with the current download state, suitable for
    including in progress CSV output for frontend display.

    Returns
    -------
    dict
        Dictionary with keys:
        - active: bool - Whether a download is in progress
        - asset: dict or None - Minimal asset dict being downloaded
        - quote: str or None - Quote asset symbol
        - data_type: str or None - Data type (e.g., "ohlc")
        - timespan: str or None - Timespan (e.g., "minute", "day")
        - progress: int - Progress percentage (0-100)
        - current: int - Current chunk number
        - total: int - Total chunks

    Example
    -------
    >>> status = get_download_status()
    >>> if status["active"]:
    ...     print(f"Downloading {status['asset']['symbol']} - {status['progress']}%")
    """
    with _download_status_lock:
        return dict(_download_status)


def set_download_status(
    asset,
    quote_asset,
    data_type: str,
    timespan: str,
    current: int,
    total: int
) -> None:
    """
    Update the current download status.

    Called during ThetaData fetch operations to track progress.

    Parameters
    ----------
    asset : Asset
        The asset being downloaded
    quote_asset : Asset or str
        The quote asset (e.g., USD)
    data_type : str
        Type of data (e.g., "ohlc")
    timespan : str
        Timespan (e.g., "minute", "day")
    current : int
        Current chunk number (0-based)
    total : int
        Total number of chunks
    """
    with _download_status_lock:
        _download_status["active"] = True
        _download_status["asset"] = asset.to_minimal_dict() if asset and hasattr(asset, 'to_minimal_dict') else {"symbol": str(asset)}
        _download_status["quote"] = str(quote_asset) if quote_asset else None
        _download_status["data_type"] = data_type
        _download_status["timespan"] = timespan
        _download_status["progress"] = int((current / max(total, 1)) * 100)
        _download_status["current"] = current
        _download_status["total"] = total


def clear_download_status() -> None:
    """
    Clear the download status when a fetch completes.

    Should be called after a download finishes (success or failure)
    to indicate no download is currently in progress.
    """
    with _download_status_lock:
        _download_status["active"] = False
        _download_status["asset"] = None
        _download_status["quote"] = None
        _download_status["data_type"] = None
        _download_status["timespan"] = None
        _download_status["progress"] = 0
        _download_status["current"] = 0
        _download_status["total"] = 0


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


_DEFAULT_BASE_URL = _normalize_base_url(_downloader_base_env or _theta_fallback_base)
BASE_URL = _DEFAULT_BASE_URL
DOWNLOADER_API_KEY = os.environ.get("DATADOWNLOADER_API_KEY")
DOWNLOADER_KEY_HEADER = os.environ.get("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")
REMOTE_DOWNLOADER_ENABLED = _coerce_skip_flag(os.environ.get("DATADOWNLOADER_SKIP_LOCAL_START"), BASE_URL)
if REMOTE_DOWNLOADER_ENABLED:
    logger.info("[THETA][CONFIG] Remote downloader enabled at %s", BASE_URL)
    if DOWNLOADER_API_KEY:
        # Log a safe fingerprint so prod runs can confirm the key is present without leaking it.
        key_prefix = DOWNLOADER_API_KEY[:4]
        key_suffix = DOWNLOADER_API_KEY[-4:] if len(DOWNLOADER_API_KEY) > 8 else ""
        logger.info(
            "[THETA][CONFIG] Downloader API key detected (len=%d, prefix=%s..., suffix=...%s)",
            len(DOWNLOADER_API_KEY),
            key_prefix,
            key_suffix,
        )
    else:
        # Use DEBUG level - this fires at module import time before ECS secrets injection.
        # The key is typically available at runtime; a WARNING here creates noise in logs.
        logger.debug("[THETA][CONFIG] Downloader API key not set at import time (DATADOWNLOADER_API_KEY)")
HEALTHCHECK_SYMBOL = os.environ.get("THETADATA_HEALTHCHECK_SYMBOL", "SPY")
READINESS_ENDPOINT = "/v3/terminal/mdds/status"
READINESS_PROBES: Tuple[Tuple[str, Dict[str, str]], ...] = (
    (READINESS_ENDPOINT, {"format": "json"}),
    ("/v3/option/list/expirations", {"symbol": HEALTHCHECK_SYMBOL, "format": "json"}),
)


def _current_base_url() -> str:
    """Return the latest downloader base URL, honoring runtime env overrides."""
    runtime_base = os.environ.get("DATADOWNLOADER_BASE_URL")
    if runtime_base:
        return _normalize_base_url(runtime_base)
    fallback = os.environ.get("THETADATA_BASE_URL", _theta_fallback_base)
    return _normalize_base_url(fallback)
READINESS_TIMEOUT = float(os.environ.get("THETADATA_HEALTHCHECK_TIMEOUT", "1.0"))
CONNECTION_RETRY_SLEEP = 1.0
CONNECTION_MAX_RETRIES = 120
BOOT_GRACE_PERIOD = 5.0
MAX_RESTART_ATTEMPTS = 3
MAX_TERMINAL_RESTART_CYCLES = 3
HTTP_RETRY_LIMIT = 3
HTTP_RETRY_BACKOFF_MAX = 5.0
TRANSIENT_STATUS_CODES = {500, 502, 503, 504, 520, 521}
# Theta caps outstanding REST calls per account (Pro tier = 8, v2 legacy = 4). Keep chunk fan-out below
# that limit so a single bot doesn't starve everyone else.
MAX_PARALLEL_CHUNKS = int(os.environ.get("THETADATA_MAX_PARALLEL_CHUNKS", "8"))
THETADATA_CONCURRENCY_BUDGET = max(1, int(os.environ.get("THETADATA_CONCURRENCY_BUDGET", "8")))
THETADATA_CONCURRENCY_WAIT_LOG_THRESHOLD = float(os.environ.get("THETADATA_CONCURRENCY_WAIT_THRESHOLD", "0.5"))
THETA_REQUEST_SEMAPHORE = threading.BoundedSemaphore(THETADATA_CONCURRENCY_BUDGET)
QUEUE_FULL_BACKOFF_BASE = float(os.environ.get("THETADATA_QUEUE_FULL_BACKOFF_BASE", "1.0"))
QUEUE_FULL_BACKOFF_MAX = float(os.environ.get("THETADATA_QUEUE_FULL_BACKOFF_MAX", "30.0"))
QUEUE_FULL_BACKOFF_JITTER = float(os.environ.get("THETADATA_QUEUE_FULL_BACKOFF_JITTER", "0.5"))
# Circuit breaker: max total time to wait on 503s before failing (default 5 minutes)
SERVICE_UNAVAILABLE_MAX_WAIT = float(os.environ.get("THETADATA_503_MAX_WAIT", "300.0"))

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

# Theta support confirmed (Nov 2025) that dividends/splits live only on the legacy v2 REST surface.
# We therefore source corporate actions from these endpoints regardless of which terminal version is running.
THETA_V2_DIVIDEND_ENDPOINT = "/v2/hist/stock/dividend"
THETA_V2_SPLIT_ENDPOINT = "/v2/hist/stock/split"
EVENT_CACHE_PAD_DAYS = int(os.environ.get("THETADATA_EVENT_CACHE_PAD_DAYS", "60"))
EVENT_CACHE_MIN_DATE = date(1950, 1, 1)
EVENT_CACHE_MAX_DATE = date(2100, 12, 31)
CORPORATE_EVENT_FOLDER = "events"
DIVIDEND_VALUE_COLUMNS = ("amount", "cash", "dividend", "cash_amount")
DIVIDEND_DATE_COLUMNS = ("ex_dividend_date", "ex_date", "ex_dividend", "execution_date")
SPLIT_NUMERATOR_COLUMNS = ("split_to", "to", "numerator", "ratio_to", "after_shares")
SPLIT_DENOMINATOR_COLUMNS = ("split_from", "from", "denominator", "ratio_from", "before_shares")
SPLIT_RATIO_COLUMNS = ("ratio", "split_ratio")

OPTION_LIST_ENDPOINTS = {
    "expirations": "/v3/option/list/expirations",
    "strikes": "/v3/option/list/strikes",
    "dates_quote": "/v3/option/list/dates/quote",
}

DEFAULT_SESSION_HOURS = {
    True: ("04:00:00", "20:00:00"),   # include extended hours
    False: ("09:30:00", "16:00:00"),  # regular session only
}


@contextmanager
def _acquire_theta_slot(label: str = "request"):
    """Enforce the plan-wide concurrency cap for outbound Theta requests."""

    start = time.perf_counter()
    THETA_REQUEST_SEMAPHORE.acquire()
    wait = time.perf_counter() - start
    if wait >= THETADATA_CONCURRENCY_WAIT_LOG_THRESHOLD:
        logger.warning("[THETA][CONCURRENCY] Waited %.2fs for Theta slot (%s)", wait, label)
    try:
        yield
    finally:
        THETA_REQUEST_SEMAPHORE.release()


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
                f"{_current_base_url()}{endpoint}",
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
        request_url = f"{_current_base_url()}{endpoint}"
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
        shutdown_url = f"{_current_base_url()}{path}"
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


def _symbol_cache_component(asset: Asset) -> str:
    symbol = getattr(asset, "symbol", "") or "symbol"
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", str(symbol).upper())
    return cleaned or "symbol"


def _event_cache_paths(asset: Asset, event_type: str) -> Tuple[Path, Path]:
    provider_root = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER
    asset_folder = _resolve_asset_folder(asset)
    symbol_component = _symbol_cache_component(asset)
    event_folder = provider_root / asset_folder / CORPORATE_EVENT_FOLDER / event_type
    cache_path = event_folder / f"{symbol_component}_{event_type}.parquet"
    meta_path = event_folder / f"{symbol_component}_{event_type}.meta.json"
    return cache_path, meta_path


def _load_event_cache_frame(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(cache_path)
    except Exception as exc:
        logger.warning("Failed to load ThetaData %s cache (%s); re-downloading", cache_path, exc)
        return pd.DataFrame()
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce", utc=True)
    return df


def _save_event_cache_frame(cache_path: Path, df: pd.DataFrame) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df_to_save = df.copy()
    if "event_date" in df_to_save.columns:
        df_to_save["event_date"] = pd.to_datetime(df_to_save["event_date"], utc=True)
    df_to_save.to_parquet(cache_path, index=False)


def _load_event_metadata(meta_path: Path) -> List[Tuple[date, date]]:
    if not meta_path.exists():
        return []
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    ranges: List[Tuple[date, date]] = []
    for start_str, end_str in payload.get("ranges", []):
        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt
        ranges.append((start_dt, end_dt))
    return ranges


def _write_event_metadata(meta_path: Path, ranges: List[Tuple[date, date]]) -> None:
    payload = {
        "ranges": [
            (start.isoformat(), end.isoformat())
            for start, end in sorted(ranges, key=lambda pair: pair[0])
        ]
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(payload), encoding="utf-8")


def _merge_coverage_ranges(ranges: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
    if not ranges:
        return []
    sorted_ranges = sorted(ranges, key=lambda pair: pair[0])
    merged: List[Tuple[date, date]] = []
    current_start, current_end = sorted_ranges[0]
    for start, end in sorted_ranges[1:]:
        if start <= current_end + timedelta(days=1):
            current_end = max(current_end, end)
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    merged.append((current_start, current_end))
    return merged


def _calculate_missing_event_windows(
    ranges: List[Tuple[date, date]],
    request_start: date,
    request_end: date,
) -> List[Tuple[date, date]]:
    if request_start > request_end:
        request_start, request_end = request_end, request_start
    if not ranges:
        return [(request_start, request_end)]

    merged = _merge_coverage_ranges(ranges)
    missing: List[Tuple[date, date]] = []
    cursor = request_start
    for start, end in merged:
        if end < cursor:
            continue
        if start > request_end:
            break
        if start > cursor:
            missing.append((cursor, min(request_end, start - timedelta(days=1))))
        cursor = max(cursor, end + timedelta(days=1))
        if cursor > request_end:
            break
    if cursor <= request_end:
        missing.append((cursor, request_end))
    return [window for window in missing if window[0] <= window[1]]


def _pad_event_window(window_start: date, window_end: date) -> Tuple[date, date]:
    pad = timedelta(days=max(EVENT_CACHE_PAD_DAYS, 0))
    padded_start = max(EVENT_CACHE_MIN_DATE, window_start - pad)
    padded_end = min(EVENT_CACHE_MAX_DATE, window_end + pad)
    if padded_start > padded_end:
        padded_start, padded_end = padded_end, padded_start
    return padded_start, padded_end


def _coerce_event_dataframe(json_resp: Optional[Dict[str, Any]]) -> pd.DataFrame:
    if not json_resp:
        return pd.DataFrame()
    rows = json_resp.get("response") or []
    header = json_resp.get("header", {})
    fmt = header.get("format")
    if rows and fmt and isinstance(rows[0], (list, tuple)):
        return pd.DataFrame(rows, columns=fmt)
    if rows and isinstance(rows[0], dict):
        return pd.DataFrame(rows)
    return pd.DataFrame(rows)


def _coerce_event_timestamp(series: pd.Series) -> pd.Series:
    """Coerce Theta event timestamps (string or numeric) into normalized UTC dates."""
    if series is None:
        return pd.Series(dtype="datetime64[ns, UTC]")

    working = series.copy() if isinstance(series, pd.Series) else pd.Series(series)
    if pd.api.types.is_numeric_dtype(working):
        # Theta v2 endpoints return YYYYMMDD integers; stringify before parsing so pandas
        # doesn't treat them as nanosecond offsets from epoch.
        working = pd.to_numeric(working, errors="coerce").astype("Int64").astype(str)
        # Use explicit format for YYYYMMDD strings to avoid pandas format inference warnings
        ts = pd.to_datetime(working, format="%Y%m%d", errors="coerce", utc=True)
    else:
        # For non-numeric data, let pandas infer the format
        ts = pd.to_datetime(working, errors="coerce", utc=True)
    return ts.dt.normalize()


def _normalize_dividend_events(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    working = df.copy()

    # Filter out special distributions (return of capital, etc.) where less_amount > 0
    # Per ThetaData docs: non-zero less_amount indicates special adjustments
    less_amount_col = _detect_column(working, ("less_amount",))
    if less_amount_col and less_amount_col in working.columns:
        less_vals = pd.to_numeric(working[less_amount_col], errors="coerce").fillna(0.0)
        special_mask = less_vals > 0
        if special_mask.any():
            special_count = special_mask.sum()
            logger.info(
                "[THETA][DIVIDENDS] Filtering %d special distribution(s) with less_amount > 0 for %s",
                special_count, symbol
            )
            working = working[~special_mask].copy()

    if working.empty:
        return pd.DataFrame()

    value_col = _detect_column(working, DIVIDEND_VALUE_COLUMNS) or DIVIDEND_VALUE_COLUMNS[0]
    date_col = _detect_column(working, DIVIDEND_DATE_COLUMNS)
    record_col = _detect_column(working, ("record_date", "record"))
    pay_col = _detect_column(working, ("pay_date", "payment_date"))
    declared_col = _detect_column(working, ("declared_date", "declaration_date"))
    freq_col = _detect_column(working, ("frequency", "freq"))

    if date_col is None:
        logger.debug("[THETA][DEBUG][DIVIDENDS] Missing ex-dividend date column for %s", symbol)
        return pd.DataFrame()

    normalized = pd.DataFrame()
    normalized["event_date"] = _coerce_event_timestamp(working[date_col])
    normalized["cash_amount"] = pd.to_numeric(working[value_col], errors="coerce").fillna(0.0)
    if record_col:
        normalized["record_date"] = _coerce_event_timestamp(working[record_col])
    if pay_col:
        normalized["pay_date"] = _coerce_event_timestamp(working[pay_col])
    if declared_col:
        normalized["declared_date"] = _coerce_event_timestamp(working[declared_col])
    if freq_col:
        normalized["frequency"] = working[freq_col]
    normalized["symbol"] = symbol
    normalized = normalized.dropna(subset=["event_date"])

    # Deduplicate by ex_date - ThetaData sometimes returns multiple entries for same ex_date
    # (e.g., 2019-03-20 appears 4 times with different 'date' values in raw response)
    # Keep only the first occurrence per ex_date
    before_dedup = len(normalized)
    normalized = normalized.drop_duplicates(subset=["event_date"], keep="first")
    after_dedup = len(normalized)
    if before_dedup > after_dedup:
        logger.info(
            "[THETA][DIVIDENDS] Deduplicated %d duplicate dividend(s) by ex_date for %s",
            before_dedup - after_dedup, symbol
        )

    return normalized.sort_values("event_date")


def _parse_ratio_value(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return float(raw)
        except Exception:
            return None
    text = str(raw).strip()
    if not text:
        return None
    if ":" in text:
        left, right = text.split(":", 1)
        try:
            left_val = float(left)
            right_val = float(right)
            if right_val == 0:
                return None
            return left_val / right_val
        except Exception:
            return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_split_events(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    working = df.copy()

    # ThetaData v2 returns a row for EVERY trading day with the "most recent" split info.
    # Format: [ms_of_day, split_date, before_shares, after_shares, date]
    # We need to filter to only actual split events where date == split_date
    split_date_col = _detect_column(working, ("split_date",))
    date_col = _detect_column(working, ("execution_date", "ex_date", "date"))

    if split_date_col and date_col and split_date_col != date_col:
        # Filter to only rows where the trading date matches the split date
        # This extracts actual split events from the daily data
        try:
            split_dates = pd.to_datetime(working[split_date_col].astype(str), format="%Y%m%d", errors="coerce")
            trading_dates = pd.to_datetime(working[date_col].astype(str), format="%Y%m%d", errors="coerce")
            actual_split_mask = split_dates.dt.date == trading_dates.dt.date
            working = working[actual_split_mask].copy()
            logger.debug(
                "[THETA][SPLITS] Filtered %s to %d actual split event(s)",
                symbol, len(working)
            )
        except Exception as e:
            logger.debug("[THETA][SPLITS] Could not filter split events for %s: %s", symbol, e)

    if working.empty:
        return pd.DataFrame()

    if date_col is None:
        return pd.DataFrame()
    numerator_col = _detect_column(working, SPLIT_NUMERATOR_COLUMNS)
    denominator_col = _detect_column(working, SPLIT_DENOMINATOR_COLUMNS)
    ratio_col = _detect_column(working, SPLIT_RATIO_COLUMNS)

    def _resolve_ratio(row: pd.Series) -> float:
        numerator = row.get(numerator_col) if numerator_col else None
        denominator = row.get(denominator_col) if denominator_col else None
        ratio_value = _parse_ratio_value(row.get(ratio_col)) if ratio_col else None
        if numerator is not None and denominator not in (None, 0):
            if not (pd.isna(numerator) or pd.isna(denominator)):
                try:
                    numerator = float(numerator)
                    denominator = float(denominator)
                    if denominator != 0:
                        return numerator / denominator
                except Exception:
                    pass
        if ratio_value is not None:
            return ratio_value
        return 1.0

    normalized = pd.DataFrame()
    normalized["event_date"] = _coerce_event_timestamp(working[date_col])
    normalized["ratio"] = working.apply(_resolve_ratio, axis=1)
    normalized["symbol"] = symbol
    normalized = normalized.dropna(subset=["event_date"])

    # Remove rows with ratio 1.0 (no actual split)
    normalized = normalized[normalized["ratio"] != 1.0]

    return normalized.sort_values("event_date")


def _download_corporate_events(
    asset: Asset,
    event_type: str,
    window_start: date,
    window_end: date,
    username: str,
    password: str,
) -> pd.DataFrame:
    """Fetch corporate actions via Theta's v2 REST endpoints."""

    if event_type not in {"dividends", "splits"}:
        return pd.DataFrame()

    if not asset.symbol:
        return pd.DataFrame()

    endpoint = THETA_V2_DIVIDEND_ENDPOINT if event_type == "dividends" else THETA_V2_SPLIT_ENDPOINT
    # v2 endpoints use the legacy parameter names: root, use_csv, pretty_time
    # DO NOT change to v3-style names - they are different APIs
    querystring = {
        "root": asset.symbol,
        "start_date": window_start.strftime("%Y%m%d"),
        "end_date": window_end.strftime("%Y%m%d"),
        "use_csv": "false",
        "pretty_time": "false",
    }
    headers = {"Accept": "application/json"}
    url = f"{_current_base_url()}{endpoint}"

    try:
        response = get_request(
            url=url,
            headers=headers,
            querystring=querystring,
            username=username,
            password=password,
        )
    except ThetaRequestError as exc:
        if exc.status_code in {404, 410}:
            return pd.DataFrame()
        raise

    if not response:
        return pd.DataFrame()

    df = _coerce_event_dataframe(response)
    if event_type == "dividends":
        return _normalize_dividend_events(df, asset.symbol)
    return _normalize_split_events(df, asset.symbol)


def _ensure_event_cache(
    asset: Asset,
    event_type: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
) -> pd.DataFrame:
    cache_path, meta_path = _event_cache_paths(asset, event_type)
    cache_df = _load_event_cache_frame(cache_path)
    coverage = _load_event_metadata(meta_path)
    missing_windows = _calculate_missing_event_windows(coverage, start_date, end_date)
    fetched_ranges: List[Tuple[date, date]] = []
    new_frames: List[pd.DataFrame] = []
    for window_start, window_end in missing_windows:
        padded_start, padded_end = _pad_event_window(window_start, window_end)
        data_frame = _download_corporate_events(
            asset,
            event_type,
            padded_start,
            padded_end,
            username,
            password,
        )
        if data_frame is not None and not data_frame.empty:
            new_frames.append(data_frame)
        fetched_ranges.append((padded_start, padded_end))
    if new_frames:
        combined = pd.concat([cache_df] + new_frames, ignore_index=True) if not cache_df.empty else pd.concat(new_frames, ignore_index=True)
        dedupe_cols = ["event_date", "cash_amount"] if event_type == "dividends" else ["event_date", "ratio"]
        cache_df = combined.drop_duplicates(subset=dedupe_cols, keep="last").sort_values("event_date")
        _save_event_cache_frame(cache_path, cache_df)
    if fetched_ranges:
        updated_ranges = _merge_coverage_ranges(coverage + fetched_ranges)
        _write_event_metadata(meta_path, updated_ranges)
    if cache_df.empty:
        return cache_df
    date_series = cache_df["event_date"].dt.date
    mask = (date_series >= min(start_date, end_date)) & (date_series <= max(start_date, end_date))
    return cache_df.loc[mask].copy()


def _get_theta_dividends(asset: Asset, start_date: date, end_date: date, username: str, password: str) -> pd.DataFrame:
    if str(getattr(asset, "asset_type", "stock")).lower() != "stock":
        return pd.DataFrame()
    return _ensure_event_cache(asset, "dividends", start_date, end_date, username, password)


def _get_theta_splits(asset: Asset, start_date: date, end_date: date, username: str, password: str) -> pd.DataFrame:
    """Fetch split data from ThetaData only. No fallback to other data sources."""
    if str(getattr(asset, "asset_type", "stock")).lower() != "stock":
        return pd.DataFrame()

    try:
        splits = _ensure_event_cache(asset, "splits", start_date, end_date, username, password)
        if splits is not None and not splits.empty:
            logger.info("[THETA][SPLITS] Got %d splits from ThetaData for %s", len(splits), asset.symbol)
            return splits
        else:
            logger.debug("[THETA][SPLITS] No splits found in ThetaData for %s", asset.symbol)
            return pd.DataFrame()
    except Exception as e:
        logger.warning("[THETA][SPLITS] ThetaData split fetch failed for %s: %s", asset.symbol, e)
        return pd.DataFrame()


def _apply_corporate_actions_to_frame(
    asset: Asset,
    frame: pd.DataFrame,
    start_day: date,
    end_day: date,
    username: str,
    password: str,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    if str(getattr(asset, "asset_type", "stock")).lower() != "stock":
        if "dividend" not in frame.columns:
            frame["dividend"] = 0.0
        if "stock_splits" not in frame.columns:
            frame["stock_splits"] = 0.0
        return frame

    # IDEMPOTENCY CHECK: If data has already been split-adjusted, skip adjustment.
    # This prevents double/multiple adjustment when cached data is re-processed.
    # The marker column is set at the end of this function after successful adjustment.
    if "_split_adjusted" in frame.columns and frame["_split_adjusted"].any():
        logger.debug(
            "[THETA][SPLIT_ADJUST] Skipping adjustment for %s - data already split-adjusted",
            asset.symbol
        )
        return frame

    dividends = _get_theta_dividends(asset, start_day, end_day, username, password)
    splits = _get_theta_splits(asset, start_day, end_day, username, password)

    tz_index = frame.index
    if isinstance(tz_index, pd.DatetimeIndex):
        index_dates = tz_index
    else:
        index_dates = pd.to_datetime(tz_index, errors="coerce")
    if getattr(index_dates, "tz", None) is None:
        index_dates = index_dates.tz_localize("UTC")
    else:
        index_dates = index_dates.tz_convert("UTC")
    index_dates = index_dates.date

    if "dividend" not in frame.columns:
        frame["dividend"] = 0.0
    if not dividends.empty:
        dividend_map = dividends.groupby(dividends["event_date"].dt.date)["cash_amount"].sum().to_dict()
        frame["dividend"] = [float(dividend_map.get(day, 0.0)) for day in index_dates]
    else:
        frame["dividend"] = 0.0

    if "stock_splits" not in frame.columns:
        frame["stock_splits"] = 0.0
    if not splits.empty:
        split_map = splits.groupby(splits["event_date"].dt.date)["ratio"].prod().to_dict()
        frame["stock_splits"] = [float(split_map.get(day, 0.0)) for day in index_dates]

        # Apply split adjustments to OHLC prices for backtesting accuracy.
        # For a 3-for-1 split (ratio=3.0), prices BEFORE the split should be divided by 3.
        # This makes historical prices comparable to current prices.
        # IMPORTANT: Only apply splits that have actually occurred (split_date <= data_end_date)
        # Don't adjust for future splits that haven't happened yet.
        price_columns = ["open", "high", "low", "close"]
        available_price_cols = [col for col in price_columns if col in frame.columns]

        if available_price_cols:
            # Sort splits by date (oldest first)
            sorted_splits = splits.sort_values("event_date")

            # Filter out future splits (splits that occur AFTER the data's end date)
            # These haven't happened yet, so prices shouldn't be adjusted for them
            data_end_date = max(index_dates)
            applicable_splits = sorted_splits[sorted_splits["event_date"].dt.date <= data_end_date]

            if len(applicable_splits) < len(sorted_splits):
                skipped = len(sorted_splits) - len(applicable_splits)
                logger.debug(
                    "[THETA][SPLIT_ADJUST] Skipping %d future split(s) after data_end=%s",
                    skipped, data_end_date
                )

            # Calculate cumulative split factor for each date in the frame
            # We need to work from most recent to oldest, accumulating the factor
            split_dates = applicable_splits["event_date"].dt.date.tolist()
            split_ratios = applicable_splits["ratio"].tolist()

            # Create a cumulative adjustment factor series
            # For each date in the frame, calculate how much to divide prices by
            cumulative_factor = pd.Series(1.0, index=frame.index)

            # Work backwards through splits
            for split_date, ratio in zip(reversed(split_dates), reversed(split_ratios)):
                if ratio > 0 and ratio != 1.0:
                    # All dates BEFORE the split date need to be divided by this ratio
                    mask = pd.Series(index_dates) < split_date
                    cumulative_factor.loc[mask.values] *= ratio

            # Apply the adjustment to price columns
            for col in available_price_cols:
                if col in frame.columns:
                    original_values = frame[col].copy()
                    frame[col] = frame[col] / cumulative_factor
                    # Log significant adjustments for debugging
                    max_adjustment = cumulative_factor.max()
                    if max_adjustment > 1.1:  # More than 10% adjustment
                        logger.debug(
                            "[THETA][SPLIT_ADJUST] asset=%s col=%s max_factor=%.2f splits=%d",
                            asset.symbol, col, max_adjustment, len(splits)
                        )

            # Also adjust volume (multiply instead of divide for splits)
            if "volume" in frame.columns:
                frame["volume"] = frame["volume"] * cumulative_factor

            # Also adjust dividends (divide by cumulative_factor like prices)
            # ThetaData returns unadjusted dividend amounts, so a $1.22 dividend
            # from 2015 that occurred before several splits needs to be divided
            # by the cumulative split factor to get the per-share amount in today's terms.
            if "dividend" in frame.columns:
                frame["dividend"] = frame["dividend"] / cumulative_factor
                logger.debug(
                    "[THETA][SPLIT_ADJUST] Adjusted dividends for %s by cumulative split factor",
                    asset.symbol
                )
    else:
        frame["stock_splits"] = 0.0

    # Mark data as split-adjusted to prevent re-adjustment on subsequent calls
    frame["_split_adjusted"] = True

    return frame


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


def _strip_placeholder_rows(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Drop placeholder rows (missing=True) from the dataframe."""
    if df is None or len(df) == 0 or "missing" not in df.columns:
        return df
    cleaned = df[~df["missing"].astype(bool)].drop(columns=["missing"])
    return restore_numeric_dtypes(cleaned)


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
                if col == "missing":
                    placeholder_df[col] = True
                else:
                    # Use np.nan instead of pd.NA to avoid FutureWarning about concat with all-NA columns
                    placeholder_df[col] = np.nan
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
    return_polars: bool = False,
    preserve_full_history: bool = False,
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
    preserve_full_history : bool
        When True, skip trimming the cached frame to [start, end]. Useful for callers (like the backtester)
        that want to keep the full historical coverage in memory.

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
    cache_invalid = False
    cache_file = build_cache_filename(asset, timespan, datastyle)
    remote_payload = build_remote_cache_payload(asset, timespan, datastyle)
    cache_manager = get_backtest_cache()

    sidecar_file = _cache_sidecar_path(cache_file)

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

        try:
            # PERFORMANCE FIX (2025-12-07): Removed force_download=True for sidecar files.
            # This was causing unnecessary S3 downloads on every backtest run.
            # The sidecar will be downloaded if missing; if out-of-sync, validation will catch it.
            cache_manager.ensure_local_file(sidecar_file, payload=remote_payload)
        except Exception as exc:
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_SIDECAR_ERROR] asset=%s sidecar=%s error=%s",
                asset,
                sidecar_file,
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

    sidecar_data = _load_cache_sidecar(cache_file)
    cache_checksum = _hash_file(cache_file)

    # INTEGRITY_FAILURES: Data corruption that requires cache deletion
    # COVERAGE_FAILURES: Cache is valid but doesn't cover requested range - can extend
    INTEGRITY_FAILURES = {"unparseable_index", "duplicate_index", "sidecar_mismatch"}
    COVERAGE_FAILURES = {"empty", "missing_trading_days", "stale_max_date", "too_few_rows"}

    def _validate_cache_frame(
        frame: Optional[pd.DataFrame],
        requested_start_dt: datetime,
        requested_end_dt: datetime,
        span: str,
    ) -> Tuple[bool, str, bool]:
        """Return (is_valid, reason, is_integrity_failure).

        Integrity failures = corrupt/inconsistent data that MUST be deleted.
        Coverage failures = cache is valid but doesn't cover requested range - can be extended.

        This distinction is critical for cache fidelity:
        - Integrity failures (unparseable_index, duplicate_index, sidecar_mismatch): DELETE cache
        - Coverage failures (missing_trading_days, stale_max_date, too_few_rows): KEEP cache, extend it

        Added 2025-12-07 to fix cache fidelity bug where valid cache was deleted when
        it simply didn't cover the requested date range.
        """
        if frame is None or frame.empty:
            return False, "empty", False  # Not an integrity failure - just no data

        frame = ensure_missing_column(frame)

        try:
            frame_index = pd.to_datetime(frame.index)
        except Exception:
            return False, "unparseable_index", True  # INTEGRITY FAILURE

        if frame_index.tz is None:
            frame_index = frame_index.tz_localize(pytz.UTC)
        else:
            frame_index = frame_index.tz_convert(pytz.UTC)

        if frame_index.has_duplicates:
            return False, "duplicate_index", True  # INTEGRITY FAILURE

        min_ts = frame_index.min()
        max_ts = frame_index.max()
        total_rows = len(frame)
        placeholder_mask = frame["missing"].astype(bool) if "missing" in frame.columns else pd.Series(False, index=frame.index)
        placeholder_rows = int(placeholder_mask.sum()) if hasattr(placeholder_mask, "sum") else 0
        real_rows = total_rows - placeholder_rows

        requested_start_date = requested_start_dt.date()
        requested_end_date = requested_end_dt.date()

        # Validate sidecar alignment
        if sidecar_data:
            rows_match = sidecar_data.get("rows") in (None, total_rows) or int(sidecar_data.get("rows", 0)) == total_rows
            placeholders_match = sidecar_data.get("placeholders") in (None, placeholder_rows) or int(sidecar_data.get("placeholders", 0)) == placeholder_rows
            checksum_match = (sidecar_data.get("checksum") is None) or (cache_checksum is None) or (sidecar_data.get("checksum") == cache_checksum)
            min_match = sidecar_data.get("min") is None or sidecar_data.get("min") == (min_ts.isoformat() if hasattr(min_ts, "isoformat") else None)
            max_match = sidecar_data.get("max") is None or sidecar_data.get("max") == (max_ts.isoformat() if hasattr(max_ts, "isoformat") else None)
            if not all([rows_match, placeholders_match, checksum_match, min_match, max_match]):
                return False, "sidecar_mismatch", True  # INTEGRITY FAILURE

        if span == "day":
            trading_days = get_trading_dates(asset, requested_start_dt, requested_end_dt)
            index_dates = pd.Index(frame_index.date)
            placeholder_dates = set(pd.Index(frame_index[placeholder_mask].date)) if hasattr(frame_index, "__len__") else set()

            missing_required: List[date] = []
            for d in trading_days:
                if d not in index_dates:
                    missing_required.append(d)

            # DEBUG: Log detailed cache validation info for OPTIONS
            is_option = getattr(asset, 'asset_type', None) == 'option'
            if is_option or missing_required:
                logger.info(
                    "[THETA][DEBUG][CACHE_VALIDATION] asset=%s | "
                    "requested_range=%s to %s | "
                    "trading_days_count=%d | "
                    "index_dates_count=%d | "
                    "placeholder_dates_count=%d | "
                    "missing_required_count=%d | "
                    "first_5_missing=%s | "
                    "cache_min_date=%s | cache_max_date=%s | "
                    "first_5_index_dates=%s | "
                    "first_5_placeholder_dates=%s",
                    asset,
                    requested_start_date,
                    requested_end_date,
                    len(trading_days),
                    len(index_dates),
                    len(placeholder_dates),
                    len(missing_required),
                    sorted(missing_required)[:5] if missing_required else [],
                    min(index_dates) if len(index_dates) > 0 else None,
                    max(index_dates) if len(index_dates) > 0 else None,
                    sorted(set(index_dates))[:5] if len(index_dates) > 0 else [],
                    sorted(placeholder_dates)[:5] if placeholder_dates else [],
                )

            if missing_required:
                return False, "missing_trading_days", False  # COVERAGE FAILURE - can extend

            # NOTE: Removed "starts_after_requested" check (2025-12-05)
            # This check invalidated cache for assets like TQQQ where the requested start date
            # (e.g., 2011-04-xx for 200-day MA lookback) is before the asset's inception date
            # (TQQQ started 2012-05-31). The missing_required check above already catches
            # actual missing trading days, so this check was redundant and caused cache to be
            # invalidated and re-fetched on EVERY iteration, leading to 40-minute backtests.

            if requested_end_date > max_ts.date():
                return False, "stale_max_date", False  # COVERAGE FAILURE - can extend

            expected_days = len(trading_days)
            # Use total_rows (including placeholders) for coverage check since placeholders
            # represent permanently missing data that we've already identified
            too_few_rows = expected_days > 0 and total_rows < max(5, int(expected_days * 0.9))
            if too_few_rows:
                return False, "too_few_rows", False  # COVERAGE FAILURE - can extend
        return True, "", False

    cache_ok, cache_reason, is_integrity_failure = _validate_cache_frame(df_all, requested_start, requested_end, timespan)
    if cache_ok and df_all is not None and _load_cache_sidecar(cache_file) is None:
        # Backfill a missing sidecar for a valid cache.
        try:
            checksum = _hash_file(cache_file)
            _write_cache_sidecar(cache_file, df_all, checksum)
        except Exception:
            logger.debug(
                "[THETA][DEBUG][CACHE][SIDECAR_BACKFILL_ERROR] cache_file=%s",
                cache_file,
            )

    if not cache_ok and df_all is not None:
        if is_integrity_failure:
            # INTEGRITY FAILURE: Cache is corrupt/inconsistent - must delete and re-fetch all
            cache_invalid = True
            try:
                cache_file.unlink()
            except Exception:
                pass
            try:
                _cache_sidecar_path(cache_file).unlink()
            except Exception:
                pass
            df_all = None
            df_cached = None
            logger.warning(
                "[THETA][CACHE][INTEGRITY_FAILURE] asset=%s span=%s reason=%s rows=%d - deleting corrupt cache",
                asset,
                timespan,
                cache_reason,
                cached_rows,
            )
        else:
            # COVERAGE FAILURE: Cache is valid but doesn't cover requested range - extend it
            # Keep df_all intact so we can use it as a base for fetching missing dates
            cache_invalid = False  # NOT invalid, just incomplete
            logger.info(
                "[THETA][CACHE][COVERAGE_EXTEND] asset=%s span=%s reason=%s rows=%d - will extend cache",
                asset,
                timespan,
                cache_reason,
                cached_rows,
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

    if cache_invalid:
        missing_dates = get_trading_dates(asset, start, end)
    else:
        missing_dates = get_missing_dates(df_all, asset, start, end)

    if (
        timespan == "day"
        and df_all is not None
        and "missing" in df_all.columns
        and missing_dates
    ):
        placeholder_dates = set(pd.Index(df_all[df_all["missing"].astype(bool)].index.date))
        if placeholder_dates:
            before = len(missing_dates)
            missing_dates = [d for d in missing_dates if d not in placeholder_dates]
            after = len(missing_dates)
            logger.debug(
                "[THETA][DEBUG][CACHE][PLACEHOLDER_SUPPRESS] asset=%s timespan=%s removed=%d missing=%d",
                asset.symbol if hasattr(asset, 'symbol') else str(asset),
                timespan,
                before - after,
                after,
            )

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
        result_frame = df_all
        if result_frame is not None and not result_frame.empty:
            if timespan == "day" and not preserve_full_history:
                df_dates = pd.to_datetime(result_frame.index).date
                start_date = start.date() if hasattr(start, 'date') else start
                end_date = end.date() if hasattr(end, 'date') else end
                mask = (df_dates >= start_date) & (df_dates <= end_date)
                result_frame = result_frame[mask]
            elif timespan != "day":
                import datetime as datetime_module  # RENAMED to avoid shadowing dt parameter!

                rows_before_any_filter = len(result_frame)
                max_ts_before_any_filter = result_frame.index.max() if len(result_frame) > 0 else None
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

                if not preserve_full_history:
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

                    if isinstance(end, datetime_module.datetime) and end.time() == datetime_module.time.min:
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

                    logger.debug(
                        "[THETA][DEBUG][FILTER][NO_DT_FILTER] asset=%s | "
                        "using end=%s for upper bound (dt parameter ignored for cache retrieval)",
                        asset.symbol if hasattr(asset, 'symbol') else str(asset),
                        end.isoformat()
                    )
                    result_frame = result_frame[(result_frame.index >= start) & (result_frame.index <= end)]

            if preserve_full_history:
                result_frame = ensure_missing_column(result_frame)
            else:
                result_frame = _strip_placeholder_rows(result_frame)

        if result_frame is not None and len(result_frame) > 0:
            logger.debug(
                "[THETA][DEBUG][RETURN][PANDAS] asset=%s rows=%d first_ts=%s last_ts=%s",
                asset,
                len(result_frame),
                result_frame.index.min().isoformat(),
                result_frame.index.max().isoformat()
            )

        # Apply split adjustments to cached data (the adjustment logic is idempotent)
        # This ensures cached data from before the split adjustment fix is properly adjusted
        if result_frame is not None and not result_frame.empty and timespan == "day":
            start_day = start.date() if hasattr(start, "date") else start
            end_day = end.date() if hasattr(end, "date") else end
            result_frame = _apply_corporate_actions_to_frame(
                asset, result_frame, start_day, end_day, username, password
            )

        return result_frame

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

    delta = timedelta(days=MAX_DAYS)

    # For daily bars, use ThetaData's EOD endpoint for official daily OHLC
    # The EOD endpoint includes the 16:00 closing auction and follows SIP sale-condition rules
    # This matches Polygon and Yahoo Finance EXACTLY (zero tolerance)
    if timespan == "day":
        requested_dates = list(missing_dates)
        today_utc = datetime.now(pytz.UTC).date()
        future_dates: List[date] = []
        effective_start = fetch_start
        effective_end = fetch_end

        if fetch_end > today_utc:
            effective_end = today_utc
            future_dates = [d for d in requested_dates if d > today_utc]
            requested_dates = [d for d in requested_dates if d <= today_utc]
            logger.info(
                "[THETA][INFO][THETADATA-EOD] Skipping %d future trading day(s) beyond %s; placeholders will be recorded.",
                len(future_dates),
                today_utc,
            )

        if effective_start > effective_end:
            # All requested dates are in the future—record placeholders and return.
            df_all = append_missing_markers(df_all, future_dates)
            update_cache(
                cache_file,
                df_all,
                df_cached,
                missing_dates=future_dates,
                remote_payload=remote_payload,
            )
            df_clean = df_all.copy() if df_all is not None else None
            if df_clean is not None and not df_clean.empty:
                if preserve_full_history:
                    df_clean = ensure_missing_column(df_clean)
                else:
                    df_clean = _strip_placeholder_rows(df_clean)
            return df_clean if df_clean is not None else pd.DataFrame()
        logger.info("Daily bars: using EOD endpoint for official close prices")
        logger.debug(
            "[THETA][DEBUG][THETADATA-EOD] requesting %d trading day(s) for %s from %s to %s",
            len(requested_dates),
            asset,
            effective_start,
            effective_end,
        )

        # Use EOD endpoint for official daily OHLC
        result_df = get_historical_eod_data(
            asset=asset,
            start_dt=effective_start,
            end_dt=effective_end,
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
            if future_dates:
                df_all = append_missing_markers(df_all, future_dates)
            update_cache(
                cache_file,
                df_all,
                df_cached,
                missing_dates=requested_dates + future_dates,
                remote_payload=remote_payload,
            )
            df_clean = df_all.copy() if df_all is not None else None
            if df_clean is not None and not df_clean.empty:
                if preserve_full_history:
                    df_clean = ensure_missing_column(df_clean)
                else:
                    df_clean = _strip_placeholder_rows(df_clean)
            logger.info(
                "ThetaData cache updated for %s %s %s with placeholders only (missing=%d).",
                asset,
                timespan,
                datastyle,
                len(requested_dates),
            )

            if (
                not preserve_full_history
                and df_clean is not None
                and not df_clean.empty
                and timespan == "day"
            ):
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

        trading_days = get_trading_dates(asset, effective_start, effective_end)
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
        if future_dates:
            missing_within_range.extend(future_dates)
        placeholder_count = len(missing_within_range)

        # DEBUG: Log placeholder creation for OPTIONS
        is_option = getattr(asset, 'asset_type', None) == 'option'
        if is_option or placeholder_count > 0:
            logger.info(
                "[THETA][DEBUG][PLACEHOLDER_CREATE] asset=%s | "
                "trading_days_count=%d | covered_days_count=%d | "
                "placeholders_to_create=%d | "
                "first_5_missing=%s | last_5_missing=%s | "
                "first_5_covered=%s | last_5_covered=%s | "
                "effective_range=%s to %s",
                asset,
                len(trading_days),
                len(covered_days),
                placeholder_count,
                sorted(missing_within_range)[:5] if missing_within_range else [],
                sorted(missing_within_range)[-5:] if missing_within_range else [],
                sorted(covered_days)[:5] if covered_days else [],
                sorted(covered_days)[-5:] if covered_days else [],
                effective_start.date() if hasattr(effective_start, 'date') else effective_start,
                effective_end.date() if hasattr(effective_end, 'date') else effective_end,
            )

        df_all = append_missing_markers(df_all, missing_within_range)

        update_cache(
            cache_file,
            df_all,
            df_cached,
            missing_dates=missing_within_range,
            remote_payload=remote_payload,
        )

        df_clean = df_all.copy() if df_all is not None else None
        if df_clean is not None and not df_clean.empty:
            if preserve_full_history:
                df_clean = ensure_missing_column(df_clean)
            else:
                df_clean = _strip_placeholder_rows(df_clean)

        logger.info(
            "ThetaData cache updated for %s %s %s (rows=%d placeholders=%d).",
            asset,
            timespan,
            datastyle,
            0 if df_all is None else len(df_all),
            placeholder_count,
        )

        if (
            not preserve_full_history
            and df_clean is not None
            and not df_clean.empty
            and timespan == "day"
        ):
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

    chunk_ranges: List[Tuple[datetime, datetime]] = []
    current_start = fetch_start
    current_end = fetch_start + delta

    while current_start <= fetch_end:
        chunk_upper = min(current_end, fetch_end, current_start + delta)
        chunk_ranges.append((current_start, chunk_upper))
        next_start = chunk_upper + timedelta(days=1)
        if asset.expiration and next_start > asset.expiration:
            break
        current_start = next_start
        current_end = current_start + delta

    if not chunk_ranges:
        logger.debug("[THETA][DEBUG][THETADATA] No chunk ranges generated for %s", asset)
        return df_all

    total_queries = len(chunk_ranges)
    chunk_workers = max(1, min(MAX_PARALLEL_CHUNKS, total_queries))
    logger.info(
        "ThetaData downloader requesting %d chunk(s) with up to %d parallel workers.",
        total_queries,
        chunk_workers,
    )
    pbar = tqdm(total=max(1, total_queries), desc=description, dynamic_ncols=True)

    # Track completed chunks for download status (thread-safe counter)
    completed_chunks = [0]  # Use list to allow mutation in nested scope
    completed_chunks_lock = threading.Lock()

    # Set initial download status
    set_download_status(asset, quote_asset, datastyle, timespan, 0, total_queries)

    def _fetch_chunk(chunk_start: datetime, chunk_end: datetime):
        return get_historical_data(
            asset,
            chunk_start,
            chunk_end,
            interval_ms,
            username,
            password,
            datastyle=datastyle,
            include_after_hours=include_after_hours,
        )

    with ThreadPoolExecutor(max_workers=chunk_workers) as executor:
        future_map: Dict[Any, Tuple[datetime, datetime, float]] = {}
        for chunk_start, chunk_end in chunk_ranges:
            submitted_at = time.perf_counter()
            future = executor.submit(_fetch_chunk, chunk_start, chunk_end)
            future_map[future] = (chunk_start, chunk_end, submitted_at)
        for future in as_completed(future_map):
            chunk_start, chunk_end, submitted_at = future_map[future]
            try:
                result_df = future.result()
            except Exception as exc:
                logger.warning(
                    "ThetaData chunk fetch failed for %s between %s and %s: %s",
                    asset,
                    chunk_start,
                    chunk_end,
                    exc,
                )
                result_df = None

            clamped_end = _clamp_option_end(asset, chunk_end)
            elapsed = time.perf_counter() - submitted_at

            if result_df is None or len(result_df) == 0:
                expired_chunk = (
                    asset.asset_type == "option"
                    and asset.expiration is not None
                    and clamped_end.date() >= asset.expiration
                )
                if expired_chunk:
                    logger.debug(
                        "[THETA][DEBUG][THETADATA] Option %s considered expired on %s; reusing cached data between %s and %s.",
                        asset,
                        asset.expiration,
                        chunk_start,
                        clamped_end,
                    )
                else:
                    logger.warning(
                        "No data returned for %s / %s with '%s' timespan between %s and %s",
                        asset,
                        quote_asset,
                        timespan,
                        chunk_start,
                        chunk_end,
                    )
                missing_chunk = get_trading_dates(asset, chunk_start, clamped_end)
                logger.info(
                    "ThetaData chunk complete (no rows) for %s between %s and %s in %.2fs",
                    asset,
                    chunk_start,
                    clamped_end,
                    elapsed,
                )
                df_all = append_missing_markers(df_all, missing_chunk)
                pbar.update(1)
                # Update download status
                with completed_chunks_lock:
                    completed_chunks[0] += 1
                    set_download_status(asset, quote_asset, datastyle, timespan, completed_chunks[0], total_queries)
                continue

            df_all = update_df(df_all, result_df)
            available_chunk = get_trading_dates(asset, chunk_start, clamped_end)
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
            logger.info(
                "ThetaData chunk complete for %s between %s and %s (rows=%d) in %.2fs",
                asset,
                chunk_start,
                clamped_end,
                len(result_df),
                elapsed,
            )
            pbar.update(1)
            # Update download status
            with completed_chunks_lock:
                completed_chunks[0] += 1
                set_download_status(asset, quote_asset, datastyle, timespan, completed_chunks[0], total_queries)

    # Clear download status when fetch completes
    clear_download_status()
    update_cache(cache_file, df_all, df_cached, remote_payload=remote_payload)
    if df_all is not None:
        logger.debug("[THETA][DEBUG][THETADATA-CACHE-WRITE] wrote %s rows=%d", cache_file, len(df_all))
    if df_all is not None:
        logger.info("ThetaData cache updated for %s %s %s (%d rows).", asset, timespan, datastyle, len(df_all))
    # Close the progress bar when done
    pbar.close()
    if df_all is not None and not df_all.empty:
        if preserve_full_history:
            df_all = ensure_missing_column(df_all)
        else:
            df_all = _strip_placeholder_rows(df_all)

    if (
        not preserve_full_history
        and df_all is not None
        and not df_all.empty
        and timespan == "day"
    ):
        start_date = requested_start.date() if hasattr(requested_start, "date") else requested_start
        end_date = requested_end.date() if hasattr(requested_end, "date") else requested_end
        dates = pd.to_datetime(df_all.index).date
        df_all = df_all[(dates >= start_date) & (dates <= end_date)]

    return df_all




# PERFORMANCE FIX (2025-12-07): Cache calendar objects to avoid rebuilding them.
# mcal.get_calendar() is slow; caching the calendar objects saves significant time.
_CALENDAR_CACHE: Dict[str, object] = {}


def _get_cached_calendar(name: str):
    """Get or create a cached market calendar object."""
    if name not in _CALENDAR_CACHE:
        _CALENDAR_CACHE[name] = mcal.get_calendar(name)
    return _CALENDAR_CACHE[name]


@functools.lru_cache(maxsize=2048)  # Increased from 512 for longer backtests
def _cached_trading_dates(asset_type: str, start_date: date, end_date: date) -> List[date]:
    """Memoized trading-day resolver to avoid rebuilding calendars every call.

    PERFORMANCE FIX (2025-12-07): Increased cache size and use cached calendars.
    """
    if asset_type == "crypto":
        return [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    if asset_type == "stock" or asset_type == "option" or asset_type == "index":
        cal = _get_cached_calendar("NYSE")
    elif asset_type == "forex":
        cal = _get_cached_calendar("CME_FX")
    else:
        raise ValueError(f"Unsupported asset type for thetadata: {asset_type}")
    df = cal.schedule(start_date=start_date, end_date=end_date)
    return df.index.date.tolist()


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
    start_date = start.date() if hasattr(start, 'date') else start
    end_date = end.date() if hasattr(end, 'date') else end
    return list(_cached_trading_dates(asset.asset_type, start_date, end_date))


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

    df_working = ensure_missing_column(df_all.copy())

    # It is possible to have full day gap in the data if previous queries were far apart
    # Example: Query for 8/1/2023, then 8/31/2023, then 8/7/2023
    # Whole days are easy to check for because we can just check the dates in the index
    dates_series = pd.Series(df_working.index.date)
    # Treat placeholder rows as known coverage; missing dates are considered permanently absent once written.
    real_dates = dates_series.unique()
    cached_dates_count = len(real_dates)
    cached_first = min(real_dates) if len(real_dates) > 0 else None
    cached_last = max(real_dates) if len(real_dates) > 0 else None

    logger.debug(
        "[THETA][DEBUG][CACHE][CACHED_DATES] asset=%s | "
        "cached_dates_count=%d first=%s last=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        cached_dates_count,
        cached_first,
        cached_last
    )

    missing_dates = sorted(set(trading_dates) - set(real_dates))

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

    # Filter out bad data from cached ThetaData:
    # Rows where all OHLC values are zero indicates bad/placeholder data from ThetaData.
    # NOTE: We intentionally do NOT filter weekend dates because markets may trade on
    # weekends in the future (futures, crypto, etc.). The issue is zero prices, not weekends.
    if not df.empty and all(col in df.columns for col in ["open", "high", "low", "close"]):
        all_zero = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
        zero_count = all_zero.sum()
        if zero_count > 0:
            # Log the dates of the zero rows for debugging
            zero_dates = df.index[all_zero].tolist()
            logger.warning("[THETA][DATA_QUALITY][CACHE] Filtering %d all-zero OHLC rows: %s",
                          zero_count, [str(d)[:10] for d in zero_dates[:5]])
            df = df[~all_zero]

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


def _cache_sidecar_path(cache_file: Path) -> Path:
    return cache_file.with_suffix(cache_file.suffix + ".meta.json")


_ALLOWED_HISTORICAL_PLACEHOLDER_DATES = {
    date(2019, 12, 4),
    date(2019, 12, 5),
    date(2019, 12, 6),
}


# PERFORMANCE FIX (2025-12-07): Cache file hashes to avoid recomputing for same file.
# Key: (str(path), mtime), Value: hash string
_FILE_HASH_CACHE: Dict[Tuple[str, float], str] = {}


def _hash_file(path: Path) -> Optional[str]:
    """Compute a SHA256 checksum for the given file.

    PERFORMANCE FIX (2025-12-07): Caches hash by (path, mtime) to avoid
    recomputing the same file's hash multiple times in a session.
    """
    if not path.exists() or not path.is_file():
        return None

    try:
        mtime = path.stat().st_mtime
        cache_key = (str(path), mtime)

        # Check cache first
        if cache_key in _FILE_HASH_CACHE:
            return _FILE_HASH_CACHE[cache_key]

        # Compute hash
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
                digest.update(chunk)
        hash_value = digest.hexdigest()

        # Cache for future calls
        _FILE_HASH_CACHE[cache_key] = hash_value

        # Limit cache size to prevent memory bloat
        if len(_FILE_HASH_CACHE) > 1000:
            # Remove oldest entries (first 500)
            keys_to_remove = list(_FILE_HASH_CACHE.keys())[:500]
            for key in keys_to_remove:
                _FILE_HASH_CACHE.pop(key, None)

        return hash_value
    except Exception as exc:
        logger.debug("[THETA][DEBUG][CACHE][HASH_FAIL] path=%s error=%s", path, exc)
        return None


def _load_cache_sidecar(cache_file: Path) -> Optional[Dict[str, Any]]:
    sidecar = _cache_sidecar_path(cache_file)
    if not sidecar.exists():
        return None
    try:
        return json.loads(sidecar.read_text())
    except Exception:
        return None


def _build_sidecar_payload(
    df_working: pd.DataFrame,
    checksum: Optional[str],
) -> Dict[str, Any]:
    min_ts = df_working.index.min() if len(df_working) > 0 else None
    max_ts = df_working.index.max() if len(df_working) > 0 else None
    placeholder_count = int(df_working["missing"].sum()) if "missing" in df_working.columns else 0
    real_rows = len(df_working) - placeholder_count
    payload: Dict[str, Any] = {
        "version": 2,
        "rows": int(len(df_working)),
        "real_rows": int(real_rows),
        "placeholders": int(placeholder_count),
        "min": min_ts.isoformat() if hasattr(min_ts, "isoformat") else None,
        "max": max_ts.isoformat() if hasattr(max_ts, "isoformat") else None,
        "checksum": checksum,
        "updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return payload


def _write_cache_sidecar(
    cache_file: Path,
    df_working: pd.DataFrame,
    checksum: Optional[str],
) -> None:
    sidecar = _cache_sidecar_path(cache_file)
    try:
        payload = _build_sidecar_payload(df_working, checksum)
        sidecar.write_text(json.dumps(payload, indent=2))
        logger.debug(
            "[THETA][DEBUG][CACHE][SIDECAR_WRITE] %s rows=%d real_rows=%d placeholders=%d",
            sidecar.name,
            payload["rows"],
            payload["real_rows"],
            payload["placeholders"],
        )
    except Exception as exc:  # pragma: no cover - sidecar is best-effort
        logger.debug(
            "[THETA][DEBUG][CACHE][SIDECAR_WRITE_ERROR] cache_file=%s error=%s",
            cache_file,
            exc,
        )


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

    # CRITICAL FIX: Merge old cached data with new data to prevent data loss
    # Without this, cache would be overwritten with only new data, losing historical data
    # This is essential for LEAP options where ThetaData may return partial data
    if df_cached is not None and len(df_cached) > 0:
        df_cached_normalized = ensure_missing_column(df_cached.copy())
        # Remove rows from cached that will be replaced by new data
        # Keep cached rows whose index is NOT in the new data
        cached_only = df_cached_normalized[~df_cached_normalized.index.isin(df_working.index)]
        if len(cached_only) > 0:
            logger.debug(
                "[THETA][DEBUG][CACHE][UPDATE_MERGE] cache_file=%s | "
                "merging %d cached rows with %d new rows",
                cache_file.name,
                len(cached_only),
                len(df_working)
            )
            df_working = pd.concat([cached_only, df_working]).sort_index()

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
    checksum = _hash_file(cache_file)
    sidecar_path = None
    try:
        _write_cache_sidecar(cache_file, df_working, checksum)
        sidecar_path = _cache_sidecar_path(cache_file)
    except Exception:
        # Sidecar is best-effort; failures shouldn't block cache writes.
        logger.debug(
            "[THETA][DEBUG][CACHE][SIDECAR_SKIP] cache_file=%s | sidecar write failed",
            cache_file.name,
        )

    logger.debug(
        "[THETA][DEBUG][CACHE][UPDATE_SUCCESS] cache_file=%s written successfully",
        cache_file.name
    )

    cache_manager = get_backtest_cache()

    def _atomic_remote_upload(local_path: Path) -> bool:
        if cache_manager.mode != CacheMode.S3_READWRITE:
            return False
        try:
            client = cache_manager._get_client()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_UPLOAD_ERROR] cache_file=%s error=%s",
                local_path,
                exc,
            )
            return False

        remote_key = cache_manager.remote_key_for(local_path, payload=remote_payload)
        if not remote_key:
            return False

        bucket = cache_manager._settings.bucket if cache_manager._settings else None
        if not bucket:
            return False

        tmp_key = f"{remote_key}.tmp-{int(time.time())}-{random.randint(1000,9999)}"
        try:
            client.upload_file(str(local_path), bucket, tmp_key)
            client.copy({"Bucket": bucket, "Key": tmp_key}, bucket, remote_key)
            client.delete_object(Bucket=bucket, Key=tmp_key)
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_UPLOAD_ATOMIC] %s <- %s (tmp=%s)",
                remote_key,
                local_path.as_posix(),
                tmp_key,
            )
            return True
        except Exception as exc:  # pragma: no cover - relies on boto3
            logger.debug(
                "[THETA][DEBUG][CACHE][REMOTE_UPLOAD_ERROR] cache_file=%s error=%s",
                local_path,
                exc,
            )
            return False
        finally:
            try:
                client.delete_object(Bucket=bucket, Key=tmp_key)
            except Exception:
                pass

    _atomic_remote_upload(cache_file)
    if sidecar_path and sidecar_path.exists():
        _atomic_remote_upload(sidecar_path)


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

        # Filter out bad data from ThetaData:
        # Rows where all OHLC values are zero indicates bad/placeholder data from ThetaData.
        # NOTE: We intentionally do NOT filter weekend dates because markets may trade on
        # weekends in the future (futures, crypto, etc.). The issue is zero prices, not weekends.
        if not df.empty and all(col in df.columns for col in ["open", "high", "low", "close"]):
            all_zero = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
            zero_count = all_zero.sum()
            if zero_count > 0:
                # Log the dates of the zero rows for debugging
                zero_dates = df.index[all_zero].tolist()
                logger.warning("[THETA][DATA_QUALITY] Filtering %d all-zero OHLC rows: %s",
                              zero_count, [str(d)[:10] for d in zero_dates[:5]])
                df = df[~all_zero]

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
        if wait_for_connection:
            for attempt in range(3):
                if _probe_terminal_ready():
                    return None, True
                logger.debug(
                    "Remote downloader readiness probe attempt %d failed; retrying in %.1fs",
                    attempt + 1,
                    CONNECTION_RETRY_SLEEP,
                )
                time.sleep(CONNECTION_RETRY_SLEEP)
            logger.warning("Proceeding despite remote downloader readiness probe failures.")
        return None, True

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

def _convert_columnar_to_row_format(columnar_data: dict) -> dict:
    """Convert ThetaData v3 columnar format to v2-style row format.

    ThetaData v3 returns COLUMNAR format:
        {"col1": [val1, val2, ...], "col2": [val1, val2, ...], ...}

    But our processing code expects v2 ROW format:
        {"header": {"format": ["col1", "col2", ...]}, "response": [[row1], [row2], ...]}

    This function converts between the two formats.
    """
    if not columnar_data or not isinstance(columnar_data, dict):
        return {"header": {"format": []}, "response": []}

    # Get column names (keys) and ensure consistent ordering
    columns = list(columnar_data.keys())

    # Check if this is actually columnar data (all values should be lists of same length)
    first_col = columnar_data.get(columns[0], [])
    if not isinstance(first_col, list):
        # Not columnar data, return as-is wrapped
        return {"header": {"format": []}, "response": columnar_data}

    num_rows = len(first_col)

    # Verify all columns have the same length
    for col in columns:
        if not isinstance(columnar_data[col], list) or len(columnar_data[col]) != num_rows:
            logger.warning(
                "[THETA][QUEUE] Column %s has inconsistent length: expected %d, got %s",
                col,
                num_rows,
                len(columnar_data[col]) if isinstance(columnar_data[col], list) else "not a list",
            )
            # Return as-is, let downstream handle the error
            return {"header": {"format": []}, "response": columnar_data}

    # Convert columns to rows by zipping
    rows = []
    for i in range(num_rows):
        row = [columnar_data[col][i] for col in columns]
        rows.append(row)

    logger.debug(
        "[THETA][QUEUE] Converted columnar format: %d columns x %d rows",
        len(columns),
        num_rows,
    )

    return {"header": {"format": columns}, "response": rows}


def get_request(url: str, headers: dict, querystring: dict, username: str, password: str):
    """Make a request to ThetaData via the queue system.

    This function ONLY uses queue mode - there is no fallback to direct requests.
    Queue mode provides:
    - Reliable retry with exponential backoff for transient errors
    - Dead letter queue for permanent failures
    - Idempotency via correlation IDs
    - Concurrency limiting to prevent overload
    - Automatic pagination following (merges all pages into single response)

    Args:
        url: The ThetaData API URL
        headers: Request headers
        querystring: Query parameters
        username: ThetaData username (unused - auth handled by Data Downloader)
        password: ThetaData password (unused - auth handled by Data Downloader)

    Returns:
        dict: The response from ThetaData with 'header' and 'response' keys
        None: If no data available (status 472)

    Raises:
        Exception: If the request permanently fails (moved to DLQ)
    """
    from lumibot.tools.thetadata_queue_client import queue_request

    logger.debug("[THETA][QUEUE] Making request via queue: %s params=%s", url, querystring)

    # =====================================================================================
    # AUTOMATIC PAGINATION HANDLING (2025-12-07)
    # =====================================================================================
    # ThetaData returns large result sets across multiple pages. Each response includes a
    # 'next_page' URL in the header if more data is available. This loop automatically
    # follows all pagination links and merges results into a single response.
    #
    # Example: A 10-year daily price history might return 3 pages of ~1000 rows each.
    # The caller receives a single response with all ~3000 rows merged.
    # =====================================================================================

    all_responses = []  # Accumulates response data from each page
    page_count = 0
    next_page_url = None

    while True:
        # For first request, use original URL with querystring.
        # For subsequent pages, use the next_page URL (which includes all params)
        request_url = next_page_url if next_page_url else url
        request_params = None if next_page_url else querystring

        result = queue_request(request_url, request_params, headers)

        if result is None:
            # ThetaData returns None for "no data" (HTTP 472 status)
            if page_count == 0:
                logger.debug("[THETA][QUEUE] No data returned for request: %s", url)
                return None
            # If we already have pages, return what we have
            break

        # Normalize response format - ThetaData can return different structures
        if isinstance(result, dict):
            if "header" in result and "response" in result:
                # Standard v2 format: {"header": {...}, "response": [...]}
                processed_result = result
            else:
                # ThetaData v3 columnar format: {"open": [1.0, 2.0], "close": [1.1, 2.1]}
                # Convert to row format that our code expects
                processed_result = _convert_columnar_to_row_format(result)
        else:
            # Unexpected format - wrap for safety
            processed_result = {"header": {"format": []}, "response": result}

        # Accumulate this page's data
        page_count += 1
        if processed_result.get("response"):
            all_responses.append(processed_result["response"])

        # Check for more pages - ThetaData provides 'next_page' URL in header
        next_page = None
        if isinstance(processed_result, dict) and "header" in processed_result:
            next_page = processed_result["header"].get("next_page")

        if next_page and next_page != "null" and next_page != "":
            logger.info("[THETA][PAGINATION] Page %d downloaded, fetching next page: %s", page_count, next_page)
            next_page_url = next_page
        else:
            # No more pages - exit pagination loop
            break

    # Merge all pages into a single response
    if page_count > 1:
        total_rows = sum(len(r) for r in all_responses if isinstance(r, list))
        logger.info("[THETA][PAGINATION] Merged %d pages from ThetaData (%d total rows)", page_count, total_rows)
        processed_result["response"] = []
        for page_response in all_responses:
            if isinstance(page_response, list):
                processed_result["response"].extend(page_response)
            else:
                processed_result["response"].append(page_response)
    elif page_count == 1 and all_responses:
        # Single page - use as-is
        processed_result["response"] = all_responses[0]

    return processed_result


def get_historical_eod_data(
    asset: Asset,
    start_dt: datetime,
    end_dt: datetime,
    username: str,
    password: str,
    datastyle: str = "ohlc",
    apply_corporate_actions: bool = True,
):
    """
    Get EOD (End of Day) data from ThetaData using the /v3/.../history/eod endpoints.

    This endpoint provides official daily OHLC that includes the 16:00 closing auction
    and follows SIP sale-condition rules. Theta's SIP-defined "official" open can differ
    from data vendors that use the first 09:30 trade rather than the auction print.

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

    url = f"{_current_base_url()}{endpoint}"

    base_query = {
        "symbol": asset.symbol,
        # Request JSON to avoid CSV parse errors on thetadata responses.
        "format": "json",
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
        try:
            row_dict = row.to_dict()
        except Exception:
            row_dict = dict(row)
        if isinstance(row_dict.get("response"), dict):
            row_dict = row_dict["response"]
        elif isinstance(row_dict.get("response"), list) and row_dict["response"]:
            first = row_dict["response"][0]
            if isinstance(first, dict):
                row_dict = first

        def _coerce_timestamp(value: Any) -> Optional[pd.Timestamp]:
            if value is None or value == "":
                return None
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is not None and not pd.isna(ts):
                return ts
            # Try parsing without forcing UTC, then localize if needed.
            ts = pd.to_datetime(value, errors="coerce")
            if ts is None or pd.isna(ts):
                try:
                    parsed = dateutil_parser.parse(str(value))
                except Exception:
                    return None
                if parsed.tzinfo is None:
                    parsed = pytz.UTC.localize(parsed)
                else:
                    parsed = parsed.astimezone(pytz.UTC)
                return pd.Timestamp(parsed)
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return ts

        created_value = row_dict.get("created") or row_dict.get("last_trade") or row_dict.get("timestamp")
        dt_value = _coerce_timestamp(created_value)

        if dt_value is None or pd.isna(dt_value):
            fallback_date = row_dict.get("date") or row_dict.get("trade_date")
            dt_value = _coerce_timestamp(fallback_date)

        if dt_value is None or pd.isna(dt_value):
            logger.error("[THETA][ERROR][EOD][TIMESTAMP] missing fields row=%s", row_dict)
            raise KeyError("ThetaData EOD response missing timestamp fields")
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

    if apply_corporate_actions:
        df = _apply_corporate_actions_to_frame(asset, df, start_day, end_day, username, password)

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
    url = f"{_current_base_url()}{endpoint}"
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
            # Ensure we always get JSON; CSV payloads will break json parsing.
            "format": "json",
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
            "format": "json",
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
    """Build an option chain by fetching all future expirations and their strikes from ThetaData.

    This function queries ThetaData for all available expirations and strikes for a given
    underlying asset. It returns a chain structure that can be used to find tradeable options.

    IMPORTANT DESIGN NOTE (2025-12-07):
    This function does NOT validate quote data availability during chain building. This is
    intentional - validation is deferred to point-of-use in get_expiration_on_or_after_date().

    Why no quote validation here?
    - Performance: Quote validation requires an API call per expiration, which is slow
    - LEAPS support: Far-dated expirations (2+ years out) may not have quote data for every
      historical date, but they ARE valid tradeable contracts. Validating during chain build
      caused LEAPS expirations to be incorrectly filtered out.
    - Efficiency: Strategies may only need 1-2 expirations from a chain with 100+ entries.
      Validating all upfront wastes resources.

    The consecutive_strike_misses counter only tracks failures to fetch strike lists (API errors),
    NOT quote data availability. If ThetaData returns strikes for an expiration, it's added to
    the chain regardless of whether quotes exist for the backtest date.

    Args:
        username: ThetaData API username
        password: ThetaData API password
        asset: The underlying asset (e.g., Asset("AAPL"))
        as_of_date: The historical date to build the chain for
        max_expirations: Maximum number of expirations to include (default: 120)
        max_consecutive_misses: Stop scanning after this many consecutive strike fetch failures
        chain_constraints: Optional dict with 'min_expiration_date' and/or 'max_expiration_date'
            to filter the range of expirations included

    Returns:
        Dict with structure:
        {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {expiry: [strikes]}, "PUT": {expiry: [strikes]}},
            "UnderlyingSymbol": "AAPL"
        }
        Returns None if no expirations found.
    """

    if as_of_date is None:
        raise ValueError("as_of_date must be provided to build a historical chain")

    headers = {"Accept": "application/json"}
    expirations_resp = get_request(
        url=f"{_current_base_url()}{OPTION_LIST_ENDPOINTS['expirations']}",
        headers=headers,
        querystring={"symbol": asset.symbol, "format": "json"},
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

    max_hint_int = (
        int(max_hint_date.strftime("%Y%m%d"))
        if isinstance(max_hint_date, date)
        else None
    )

    # Start from as_of_date (only include future expirations)
    effective_start_int = as_of_int

    logger.info(
        "[ThetaData] Building chain for %s @ %s (min_hint=%s, max_hint=%s, expirations=%d)",
        asset.symbol,
        as_of_date,
        min_hint_date,
        max_hint_date,
        len(expiration_values),
    )

    # Initialize the chain structure: {"CALL": {expiry: [strikes]}, "PUT": {expiry: [strikes]}}
    chains: Dict[str, Dict[str, List[float]]] = {"CALL": {}, "PUT": {}}
    expirations_added = 0

    # Track consecutive failures to fetch strike data (API errors only, not quote availability).
    # If ThetaData can't return strikes for 10 consecutive expirations, we stop scanning
    # to avoid wasting API calls on likely invalid/expired contract series.
    consecutive_strike_misses = 0

    for expiration_iso in expiration_values:
        expiration_int = int(expiration_iso.replace("-", ""))

        # Skip expirations that are in the past relative to our backtest date
        if expiration_int < effective_start_int:
            continue

        # If a max_hint_date was provided (e.g., strategy only wants options within 2 years),
        # stop scanning once we exceed it
        if max_hint_int and expiration_int > max_hint_int:
            logger.debug(
                "[ThetaData] Reached max hint %s for %s; stopping chain build.",
                max_hint_date,
                asset.symbol,
            )
            break

        # Fetch the list of available strikes for this expiration from ThetaData
        strike_resp = get_request(
            url=f"{_current_base_url()}{OPTION_LIST_ENDPOINTS['strikes']}",
            headers=headers,
            querystring={
                "symbol": asset.symbol,
                "expiration": expiration_iso,
                "format": "json",
            },
            username=username,
            password=password,
        )

        # Handle strike fetch failures - increment miss counter and potentially stop scanning
        if not strike_resp or not strike_resp.get("response"):
            logger.debug("No strikes for %s exp %s; skipping.", asset.symbol, expiration_iso)
            consecutive_strike_misses += 1
            if consecutive_strike_misses >= 10:
                logger.debug("[ThetaData] 10 consecutive expirations with no strikes; stopping scan.")
                break
            continue

        # Parse the strike response into a DataFrame
        strike_df = pd.DataFrame(strike_resp["response"], columns=strike_resp["header"]["format"])
        if strike_df.empty:
            consecutive_strike_misses += 1
            if consecutive_strike_misses >= 10:
                break
            continue

        strike_col = _detect_column(strike_df, ("strike",))
        if not strike_col:
            consecutive_strike_misses += 1
            if consecutive_strike_misses >= 10:
                break
            continue

        # Extract and normalize strike prices (handles different formats from ThetaData)
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
            consecutive_strike_misses += 1
            if consecutive_strike_misses >= 10:
                break
            continue

        # SUCCESS: Add this expiration and its strikes to the chain
        # NOTE: We do NOT validate quote/price data here - that happens at point-of-use
        # in get_expiration_on_or_after_date(). This is critical for LEAPS support.
        # See docstring for rationale.
        chains["CALL"][expiration_iso] = strike_values
        chains["PUT"][expiration_iso] = list(strike_values)
        expirations_added += 1
        consecutive_strike_misses = 0

        # Limit total expirations to avoid memory issues with very large chains
        if expirations_added >= max_expirations:
            logger.debug("[ThetaData] Chain build hit max_expirations limit (%d)", max_expirations)
            break

    logger.debug(
        "Built ThetaData historical chain for %s on %s (expirations=%d)",
        asset.symbol,
        as_of_date,
        expirations_added,
    )

    if not chains["CALL"] and not chains["PUT"]:
        logger.warning(
            "No expirations found for %s on %s.",
            asset.symbol,
            as_of_date,
        )
        return None

    return {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": chains,
        "UnderlyingSymbol": asset.symbol,  # Add this for easier extraction later
    }


def get_expirations(username: str, password: str, ticker: str, after_date: date):
    """Legacy helper retained for backward compatibility; prefer build_historical_chain."""
    logger.warning(
        "get_expirations is deprecated and provides live expirations only. "
        "Use build_historical_chain for historical backtests (ticker=%s, after=%s).",
        ticker,
        after_date,
    )

    url = f"{_current_base_url()}{OPTION_LIST_ENDPOINTS['expirations']}"
    querystring = {"symbol": ticker, "format": "json"}
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
    url = f"{_current_base_url()}{OPTION_LIST_ENDPOINTS['strikes']}"

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
