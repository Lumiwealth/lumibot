# This file contains helper functions for getting data from Polygon.io
from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
import random
import re
import signal
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import pytz

from lumibot.constants import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset

LUMIBOT_DEFAULT_PYTZ = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)


class _LazyModule(ModuleType):
    def __init__(self, module_name: str):
        super().__init__(module_name)
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self):
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        if name in {"_module_name", "_module"}:
            object.__setattr__(self, name, value)
        else:
            setattr(self._load(), name, value)

    def __delattr__(self, name):
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


pd = _LazyModule("pandas")
mcal = _LazyModule("pandas_market_calendars")
requests = _LazyModule("requests")
_TQDM_FUNC = None
_BACKTEST_CACHE_MODE = None
_BACKTEST_CACHE_GETTER = None
_DATEUTIL_PARSE = None


class _LazyLogger:
    __slots__ = ("_logger",)

    def __init__(self):
        object.__setattr__(self, "_logger", None)

    def _load(self):
        logger = object.__getattribute__(self, "_logger")
        if logger is None:
            from lumibot.tools.lumibot_logger import get_logger

            logger = get_logger(__name__)
            object.__setattr__(self, "_logger", logger)
        return logger

    def __getattr__(self, name):
        return getattr(self._load(), name)


logger = _LazyLogger()


def _dateutil_parse(value):
    global _DATEUTIL_PARSE
    if _DATEUTIL_PARSE is None:
        from dateutil import parser as dateutil_parser

        _DATEUTIL_PARSE = dateutil_parser.parse
    return _DATEUTIL_PARSE(value)


class _LazyCacheMode:
    def __getattr__(self, name):
        global _BACKTEST_CACHE_MODE
        if _BACKTEST_CACHE_MODE is None:
            from lumibot.tools.backtest_cache import CacheMode as _CacheMode

            _BACKTEST_CACHE_MODE = _CacheMode
        return getattr(_BACKTEST_CACHE_MODE, name)


CacheMode = _LazyCacheMode()


def get_backtest_cache():
    global _BACKTEST_CACHE_GETTER
    if _BACKTEST_CACHE_GETTER is None:
        from lumibot.tools.backtest_cache import get_backtest_cache as _get_backtest_cache

        _BACKTEST_CACHE_GETTER = _get_backtest_cache
    return _BACKTEST_CACHE_GETTER()


def _tqdm(*args, **kwargs):
    global _TQDM_FUNC
    if _TQDM_FUNC is None:
        from tqdm import tqdm

        _TQDM_FUNC = tqdm
    return _TQDM_FUNC(*args, **kwargs)


def tqdm(*args, **kwargs):
    return _tqdm(*args, **kwargs)

# ==============================================================================
# Download Status Tracking
# ==============================================================================
# This module tracks the current download status for ThetaData fetches.
# The status is exposed via get_download_status() for progress reporting.
#
# NOTE: This pattern can be extended to other data sources (Yahoo, Polygon, etc.)
# by implementing similar tracking in their respective helper modules.
# See docs/BACKTESTING_ARCHITECTURE.md for documentation on extending this.
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
    # Queue diagnostics (best-effort; may be None if the caller doesn't go through queue mode)
    "request_id": None,
    "correlation_id": None,
    "queue_status": None,
    "queue_position": None,
    "estimated_wait": None,
    "attempts": None,
    "last_error": None,
    "submitted_at": None,
    "last_poll_at": None,
    "timeout_at": None,
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
        - request_id: str or None - Data Downloader queue request id (if available)
        - correlation_id: str or None - Request correlation id (if available)
        - queue_status: str or None - pending/processing/completed/dead (if available)
        - queue_position: int or None - Queue position (if available)
        - estimated_wait: float or None - Estimated wait (seconds; if available)
        - attempts: int or None - Attempts count (if available)
        - last_error: str or None - Last queue error (if available)
        - submitted_at: float or None - Epoch seconds when request submitted (if available)
        - last_poll_at: float or None - Epoch seconds when last polled (if available)
        - timeout_at: float or None - Epoch seconds when client timeout triggers (if available)

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
    total: int,
    timeout_s: Optional[float] = None,
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
    timeout_s : Optional[float]
        If provided, records an absolute timeout_at for the current fetch.
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
        # Reset queue diagnostics at the start of a new download.
        _download_status["request_id"] = None
        _download_status["correlation_id"] = None
        _download_status["queue_status"] = None
        _download_status["queue_position"] = None
        _download_status["estimated_wait"] = None
        _download_status["attempts"] = None
        _download_status["last_error"] = None
        _download_status["submitted_at"] = None
        _download_status["last_poll_at"] = None
        _download_status["timeout_at"] = (time.time() + timeout_s) if (timeout_s is not None and timeout_s > 0) else None


def advance_download_status_progress(
    *,
    asset: Optional[Asset] = None,
    data_type: Optional[str] = None,
    timespan: Optional[str] = None,
    step: int = 1,
) -> None:
    """Increment download progress counters without resetting queue diagnostics."""
    if step <= 0:
        return

    with _download_status_lock:
        if not _download_status.get("active"):
            return

        if data_type is not None and _download_status.get("data_type") != data_type:
            return
        if timespan is not None and _download_status.get("timespan") != timespan:
            return

        if asset is not None:
            try:
                status_asset = _download_status.get("asset") or {}
                status_symbol = status_asset.get("symbol")
                asset_symbol = getattr(asset, "symbol", None)
                if status_symbol is not None and asset_symbol is not None and str(status_symbol) != str(asset_symbol):
                    return

                # Options share the same underlying symbol across many contracts; only treat a
                # progress update as applicable if the contract identity matches.
                status_type = str(status_asset.get("type") or "").lower()
                if status_type == "option" or str(getattr(asset, "asset_type", "")).lower() == "option":
                    status_exp = status_asset.get("exp")
                    status_strike = status_asset.get("strike")
                    status_right = status_asset.get("right")

                    asset_exp = getattr(asset, "expiration", None)
                    asset_exp_str = asset_exp.isoformat() if hasattr(asset_exp, "isoformat") else (str(asset_exp) if asset_exp else None)
                    asset_strike = getattr(asset, "strike", None)
                    asset_right = getattr(asset, "right", None)

                    if status_exp is not None and asset_exp_str is not None and str(status_exp) != str(asset_exp_str):
                        return
                    if status_right is not None and asset_right is not None and str(status_right).upper() != str(asset_right).upper():
                        return
                    if status_strike is not None and asset_strike is not None:
                        try:
                            if float(status_strike) != float(asset_strike):
                                return
                        except Exception:
                            if str(status_strike) != str(asset_strike):
                                return
            except Exception:
                return

        total = _download_status.get("total") or 0
        try:
            total_int = int(total)
        except Exception:
            total_int = 0
        if total_int <= 0:
            return

        current = _download_status.get("current") or 0
        try:
            current_int = int(current)
        except Exception:
            current_int = 0

        new_current = min(current_int + step, total_int)
        _download_status["current"] = new_current
        _download_status["progress"] = int((new_current / max(total_int, 1)) * 100)


def update_download_status_queue_info(
    *,
    request_id: str,
    correlation_id: Optional[str] = None,
    queue_status: Optional[str] = None,
    queue_position: Optional[int] = None,
    estimated_wait: Optional[float] = None,
    attempts: Optional[int] = None,
    last_error: Optional[str] = None,
    submitted_at: Optional[float] = None,
) -> None:
    """Best-effort update of queue diagnostics for the active download.

    Called from the queue client while a ThetaData fetch is in progress.
    It intentionally does nothing if there is no active download, or if the active
    download already has a different request_id (to avoid cross-request noise).
    """
    now = time.time()
    with _download_status_lock:
        if not _download_status.get("active"):
            return

        existing_request_id = _download_status.get("request_id")
        switching_requests = existing_request_id != request_id
        existing_submitted_at = _download_status.get("submitted_at")
        if switching_requests and existing_request_id is not None:
            # We only switch tracked request_ids on a submit event (submitted_at is present).
            if submitted_at is None:
                return
            try:
                if existing_submitted_at is not None and float(submitted_at) < float(existing_submitted_at):
                    return
            except Exception:
                pass

        # Avoid hammering the lock/payload. The queue poll interval can be 200ms;
        # UI only needs coarse updates.
        if not switching_requests:
            last_poll_at = _download_status.get("last_poll_at")
            if last_poll_at is not None and (now - float(last_poll_at)) < 1.0:
                return

        _download_status["request_id"] = request_id
        if correlation_id is not None:
            _download_status["correlation_id"] = correlation_id
        if queue_status is not None:
            _download_status["queue_status"] = queue_status
        if queue_position is not None:
            _download_status["queue_position"] = queue_position
        if estimated_wait is not None:
            _download_status["estimated_wait"] = estimated_wait
        if attempts is not None:
            _download_status["attempts"] = attempts
        if last_error is not None:
            _download_status["last_error"] = last_error
        if submitted_at is not None:
            _download_status["submitted_at"] = submitted_at
        _download_status["last_poll_at"] = now


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
        _download_status["request_id"] = None
        _download_status["correlation_id"] = None
        _download_status["queue_status"] = None
        _download_status["queue_position"] = None
        _download_status["estimated_wait"] = None
        _download_status["attempts"] = None
        _download_status["last_error"] = None
        _download_status["submitted_at"] = None
        _download_status["last_poll_at"] = None
        _download_status["timeout_at"] = None


def finalize_download_status() -> None:
    """Mark download inactive while keeping last progress payload.

    Some UIs poll progress at multi-second intervals; fully resetting to 0 can make
    completed downloads appear as "0/1 then vanished". This keeps the final
    `current/total/progress` visible for at least one subsequent progress.csv read.
    """
    with _download_status_lock:
        _download_status["active"] = False
        _download_status["last_poll_at"] = None
        _download_status["timeout_at"] = None


WAIT_TIME = 60
MAX_DAYS = 30
CACHE_SUBFOLDER = "thetadata"
DEFAULT_THETA_BASE = "http://127.0.0.1:25503"
DEFAULT_DOWNLOADER_BASE_URL = "http://localhost:8080"
_downloader_base_env = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip() or None
_theta_fallback_base = os.environ.get("THETADATA_BASE_URL", DEFAULT_THETA_BASE)


def _normalize_base_url(raw: Optional[str]) -> str:
    if not raw:
        return DEFAULT_THETA_BASE
    raw = raw.strip()
    if not raw:
        return DEFAULT_THETA_BASE
    # Do not rewrite environment-specific endpoints to any baked-in host.
    # If a user has an old hard-coded host/IP, keep it (so it's debuggable) and
    # allow downstream calls to fail loudly.
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
DOWNLOADER_MODE = bool(_downloader_base_env)
# IMPORTANT:
# - Default/public behavior: if DATADOWNLOADER_BASE_URL is NOT set, LumiBot manages a local ThetaTerminal.
# - Internal behavior: if DATADOWNLOADER_BASE_URL IS set (even if it's localhost:8080), LumiBot must NEVER
#   start/stop/kill a local ThetaTerminal process because it can steal/kill the single licensed session.
REMOTE_DOWNLOADER_ENABLED = DOWNLOADER_MODE or _coerce_skip_flag(os.environ.get("DATADOWNLOADER_SKIP_LOCAL_START"), BASE_URL)
if DOWNLOADER_MODE:
    # Avoid leaking private infrastructure hostnames in logs. Loopback URLs are safe to print.
    if _is_loopback_url(BASE_URL):
        logger.info("[DOWNLOADER][CONFIG] Data Downloader enabled at %s", BASE_URL)
    else:
        logger.info("[DOWNLOADER][CONFIG] Data Downloader enabled (remote URL redacted)")
    if DOWNLOADER_API_KEY:
        # Confirm presence without leaking any part of the key.
        logger.info("[DOWNLOADER][CONFIG] Downloader API key detected (len=%d)", len(DOWNLOADER_API_KEY))
    else:
        # Use DEBUG level - this fires at module import time before ECS secrets injection.
        # The key is typically available at runtime; a WARNING here creates noise in logs.
        logger.debug("[DOWNLOADER][CONFIG] Downloader API key not set at import time (DATADOWNLOADER_API_KEY)")
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


def _corporate_action_horizon_date() -> date:
    backtesting_end = os.environ.get("BACKTESTING_END")
    if backtesting_end:
        try:
            parsed = _dateutil_parse(backtesting_end)
            if parsed.tzinfo is None:
                if hasattr(LUMIBOT_DEFAULT_PYTZ, "localize"):
                    parsed = LUMIBOT_DEFAULT_PYTZ.localize(parsed)
                else:
                    parsed = parsed.replace(tzinfo=LUMIBOT_DEFAULT_PYTZ)
            else:
                parsed = parsed.astimezone(LUMIBOT_DEFAULT_PYTZ)
            return parsed.date()
        except (TypeError, ValueError, OverflowError):
            logger.debug("Ignoring unparseable BACKTESTING_END=%r for corporate actions", backtesting_end)
    return date.today()

OPTION_LIST_ENDPOINTS = {
    "expirations": "/v3/option/list/expirations",
    "strikes": "/v3/option/list/strikes",
    "dates_quote": "/v3/option/list/dates/quote",
}

# Bump this to invalidate old chain cache files when the chain schema/normalization changes.
# v3 (2025-12-21): SPX index options need SPXW expirations for 0DTE strategies.
# v5 (2026-01-05): Bound default expiration range to reduce strike-list fanout in cold backtests.
THETADATA_CHAIN_CACHE_VERSION = 5

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
        # ThetaTerminal v3 sometimes returns a dict with ONLY `response`, where the response is
        # a list of row-dicts (already denormalized but without a header/format section).
        if set(payload.keys()) == {"response"} and isinstance(payload.get("response"), list):
            resp_list = payload.get("response") or []
            if resp_list and isinstance(resp_list[0], dict):
                # Preserve the first row's key order for stable column ordering.
                columns: List[str] = list(resp_list[0].keys())
                # Add any new keys encountered later (rare) without reordering existing columns.
                for row in resp_list[1:]:
                    if isinstance(row, dict):
                        for k in row.keys():
                            if k not in columns:
                                columns.append(k)
                rows = [[row.get(col) if isinstance(row, dict) else None for col in columns] for row in resp_list]
                return {"header": {"format": columns}, "response": rows}
            # Unknown response shape; wrap and let downstream handle.
            return {"header": {"format": []}, "response": resp_list}

        if "response" in payload and "header" in payload:
            # Some ThetaData endpoints return a v2-style envelope but keep the v3 columnar
            # payload under `response` (dict-of-lists). Normalize that into row format so
            # downstream callers can always build a DataFrame with `columns=header['format']`.
            resp = payload.get("response")
            if isinstance(resp, dict):
                header = payload.get("header") or {}
                header_format = header.get("format") if isinstance(header, dict) else None
                columns = list(header_format) if isinstance(header_format, (list, tuple)) else list(resp.keys())
                if not columns:
                    return {"header": {"format": []}, "response": []}

                lengths = []
                for col in columns:
                    values = resp.get(col, [])
                    lengths.append(len(values) if isinstance(values, list) else 0)
                num_rows = max(lengths) if lengths else 0

                rows: List[List[Any]] = []
                for idx in range(num_rows):
                    row: List[Any] = []
                    for col in columns:
                        values = resp.get(col, [])
                        if isinstance(values, list) and idx < len(values):
                            row.append(values[idx])
                        else:
                            row.append(None)
                    rows.append(row)

                merged_header = dict(header) if isinstance(header, dict) else {"format": columns}
                merged_header["format"] = columns
                return {"header": merged_header, "response": rows}

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
    start_default_naive = datetime.combine(day, datetime.strptime(default_start, "%H:%M:%S").time())
    end_default_naive = datetime.combine(day, datetime.strptime(default_end, "%H:%M:%S").time())
    if hasattr(tz, "localize"):
        start_default = tz.localize(start_default_naive)
        end_default = tz.localize(end_default_naive)
    else:
        start_default = start_default_naive.replace(tzinfo=tz)
        end_default = end_default_naive.replace(tzinfo=tz)

    session_start = start_default
    session_end = end_default

    if not prefer_full_session:
        if start_dt.date() == day:
            session_start = max(start_default, start_dt)
        if end_dt.date() == day:
            session_end = min(end_default, end_dt)

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


# ThetaData uses distinct “root” symbols for some index option trading classes (e.g. SPXW),
# but index *price/OHLC* history endpoints expect the index root (e.g. SPX).
# This mapping is intentionally ThetaData-only and only applied for AssetType.INDEX history calls.
_THETADATA_INDEX_ROOT_ALIASES: Dict[str, str] = {
    "SPXW": "SPX",
    # Common Cboe-style weekly/PM-settled roots (keep limited + safe; index history typically uses the base root).
    "RUTW": "RUT",
    "VIXW": "VIX",
    # Nasdaq-100 PM-settled program root used by some systems.
    "NDXP": "NDX",
}


def _thetadata_index_root_symbol(asset: Asset) -> str:
    symbol = str(getattr(asset, "symbol", "") or "").strip().upper()
    return _THETADATA_INDEX_ROOT_ALIASES.get(symbol, symbol)


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
    # CI runners (and production-like backtest containers) start with empty disks.
    # If the S3 backtest cache is enabled, hydrate the on-disk event cache before deciding it
    # doesn't exist, so local == CI and we don't re-hit the downloader for warm-cache runs.
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        cache_manager = get_backtest_cache()
        if cache_manager is not None and not cache_path.exists():
            cache_manager.ensure_local_file(cache_path)
    except Exception:
        # Ignore remote cache hydrate failures and fall back to local-only behavior.
        pass

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
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        get_backtest_cache().on_local_update(cache_path)
    except Exception:
        logger.debug("[THETA][EVENT_CACHE] Remote cache upload failed for %s", cache_path, exc_info=True)


def _load_event_metadata(meta_path: Path) -> List[Tuple[date, date]]:
    # See `_load_event_cache_frame`: hydrate event metadata from remote cache when available.
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        cache_manager = get_backtest_cache()
        if cache_manager is not None and not meta_path.exists():
            cache_manager.ensure_local_file(meta_path)
    except Exception:
        pass

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
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        get_backtest_cache().on_local_update(meta_path)
    except Exception:
        logger.debug("[THETA][EVENT_CACHE] Remote cache upload failed for %s", meta_path, exc_info=True)


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
    username: Optional[str] = None,
    password: Optional[str] = None,
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
    username: Optional[str] = None,
    password: Optional[str] = None,
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


# ==============================================================================
# In-memory corporate actions cache (per-process / per-backtest)
# ==============================================================================
#
# Corporate actions (splits/dividends) are used in multiple hot paths:
# - option strike reverse-split adjustments (_get_option_query_strike)
# - OHLC corporate action normalization (_apply_corporate_actions_to_frame)
#
# In production backtests, the strategy code may trigger these helpers many times
# per simulated bar. The on-disk cache prevents repeated network downloads, but
# repeatedly loading/merging/filtering the disk cache can still be expensive and
# creates noisy logs (e.g. "[THETA][SPLITS] Got 1 splits..." on every call).
#
# This in-memory cache ensures we only execute the expensive cache load/refresh
# once per symbol/event_type/range per process (a backtest container runs one
# strategy), and it also rate-limits repeated retries after transient failures.
# ==============================================================================

_event_cache_memory: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _event_cache_failure_ttl_s() -> float:
    """How long to suppress repeated event-cache refresh attempts after a failure."""
    try:
        ttl_s = float(os.environ.get("THETADATA_EVENT_CACHE_FAILURE_TTL_S", "60"))
        return max(ttl_s, 0.0)
    except Exception:
        return 60.0


def _get_theta_events_cached(
    asset: Asset,
    event_type: str,
    start_date: date,
    end_date: date,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """Return Theta corporate actions using an in-memory memoization layer."""
    if not asset.symbol:
        return pd.DataFrame()

    normalized_event_type = str(event_type).lower().strip()
    if normalized_event_type not in {"dividends", "splits"}:
        return pd.DataFrame()

    range_start = min(start_date, end_date)
    range_end = max(start_date, end_date)
    symbol_key = str(asset.symbol).upper()
    cache_key = (normalized_event_type, symbol_key)

    entry = _event_cache_memory.get(cache_key)
    if entry is not None:
        entry_start = entry.get("range_start")
        entry_end = entry.get("range_end")
        cached_df = entry.get("df")
        if (
            isinstance(entry_start, date)
            and isinstance(entry_end, date)
            and isinstance(cached_df, pd.DataFrame)
            and entry_start <= range_start
            and entry_end >= range_end
        ):
            if cached_df.empty:
                return pd.DataFrame()
            date_series = cached_df["event_date"].dt.date
            mask = (date_series >= range_start) & (date_series <= range_end)
            return cached_df.loc[mask].copy()

        last_error_ts = entry.get("last_error_ts")
        if isinstance(last_error_ts, (int, float)):
            ttl_s = _event_cache_failure_ttl_s()
            if ttl_s > 0 and (time.time() - float(last_error_ts)) < ttl_s:
                return pd.DataFrame()

    try:
        events = _ensure_event_cache(asset, normalized_event_type, range_start, range_end, username, password)
    except Exception as exc:
        now = time.time()
        prev_ts = entry.get("last_error_ts") if isinstance(entry, dict) else None
        ttl_s = _event_cache_failure_ttl_s()
        if not isinstance(prev_ts, (int, float)) or ttl_s <= 0 or (now - float(prev_ts)) >= ttl_s:
            logger.warning(
                "[THETA][%s] ThetaData %s fetch failed for %s: %s",
                normalized_event_type.upper(),
                normalized_event_type,
                asset.symbol,
                exc,
            )
        _event_cache_memory[cache_key] = {
            "range_start": entry.get("range_start") if isinstance(entry, dict) else None,
            "range_end": entry.get("range_end") if isinstance(entry, dict) else None,
            "df": entry.get("df") if isinstance(entry, dict) else pd.DataFrame(),
            "last_error_ts": now,
            "last_error": str(exc),
        }
        return pd.DataFrame()

    # Cache the fetched window in-memory (even when empty).
    _event_cache_memory[cache_key] = {
        "range_start": range_start,
        "range_end": range_end,
        "df": events if isinstance(events, pd.DataFrame) else pd.DataFrame(),
        "last_error_ts": None,
        "last_error": None,
    }

    if events is None or events.empty:
        return pd.DataFrame()

    date_series = events["event_date"].dt.date
    mask = (date_series >= range_start) & (date_series <= range_end)
    return events.loc[mask].copy()


def _get_theta_dividends(
    asset: Asset,
    start_date: date,
    end_date: date,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    if str(getattr(asset, "asset_type", "stock")).lower() != "stock":
        return pd.DataFrame()
    events = _get_theta_events_cached(asset, "dividends", start_date, end_date, username, password)
    if events is not None and not events.empty:
        logger.debug("[THETA][DIVIDENDS] Loaded %d dividend events for %s", len(events), asset.symbol)
        return events
    return pd.DataFrame()


def _get_theta_splits(
    asset: Asset,
    start_date: date,
    end_date: date,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch split data from ThetaData only. No fallback to other data sources.

    Note: this function is called from several hot paths (including option strike
    reverse-split adjustments). It uses an in-memory memoization layer to avoid
    repeatedly loading/refreshing the on-disk cache and to suppress retry storms
    after transient downloader/Theta failures.
    """
    if str(getattr(asset, "asset_type", "stock")).lower() != "stock":
        return pd.DataFrame()

    splits = _get_theta_events_cached(asset, "splits", start_date, end_date, username, password)
    if splits is not None and not splits.empty:
        # Avoid log spam in tight loops: emit this at DEBUG level.
        logger.debug("[THETA][SPLITS] Loaded %d split events for %s", len(splits), asset.symbol)
        return splits
    return pd.DataFrame()


def _get_option_query_strike(option_asset: Asset, sim_datetime: datetime = None) -> float:
    """
    Convert a split-adjusted option strike back to the original (unadjusted) strike for ThetaData queries.

    REVERSE SPLIT ADJUSTMENT FOR OPTION PRICE QUERIES (2025-12-11):
    When stock prices are split-adjusted, options strikes in the chain are ALSO adjusted (via build_historical_chain).
    However, ThetaData stores historical option data using the ORIGINAL strikes.

    Example for GOOG (20:1 split in July 2022):
    - Strategy sees split-adjusted stock price: ~$55 (March 2020)
    - build_historical_chain adjusted strikes: $1320 / 20 = $66
    - Strategy wants to buy $66 strike option
    - But ThetaData has the option under $1320 strike
    - This function converts $66 -> $1320 for the API query

    FIX (2025-12-12): Use sim_datetime (not expiration) to determine split range.
    When querying for options in March 2020 for GOOG with expiration in 2024,
    we need splits from March 2020 to today (catching the July 2022 split),
    NOT from 2024 to today (which would miss the split entirely).

    Parameters
    ----------
    option_asset : Asset
        The option asset with a split-adjusted strike
    sim_datetime : datetime, optional
        The simulation datetime - used to determine which splits apply.
        If not provided, falls back to expiration date (legacy behavior).

    Returns
    -------
    float
        The original (unadjusted) strike for ThetaData API queries
    """
    if option_asset.strike is None:
        raise ValueError(f"Option asset {option_asset} missing strike")

    original_strike = float(option_asset.strike)

    # Index options do not have splits. Avoid unnecessary (and potentially invalid)
    # split lookups for SPX/SPXW contracts.
    underlying_symbol = str(getattr(option_asset, "symbol", "") or "").upper()
    if underlying_symbol in {"SPX", "SPXW"}:
        return original_strike

    # Get the underlying stock asset
    underlying_asset = Asset(option_asset.symbol, asset_type="stock")

    today = _corporate_action_horizon_date()

    # FIX (2025-12-12): Use sim_datetime as the reference date for split lookup.
    # This ensures we catch all splits between the simulation date and the
    # corporate-action horizon (BACKTESTING_END for deterministic backtests, today otherwise).
    # Previously, we used option expiration, which missed splits that occurred
    # BEFORE the expiration but AFTER the sim_datetime.
    if sim_datetime is not None:
        # Use the simulation date - this is the date we're "at" in the backtest
        as_of_date = sim_datetime.date() if hasattr(sim_datetime, 'date') else sim_datetime
    elif option_asset.expiration:
        # Fallback to expiration (legacy behavior, but less accurate)
        as_of_date = option_asset.expiration
    else:
        as_of_date = today

    # Fetch splits from as_of_date to today
    splits = _get_theta_splits(underlying_asset, as_of_date, today)

    if splits is not None and not splits.empty:
        # Calculate cumulative split factor for splits after the option's reference date
        if "event_date" in splits.columns:
            as_of_datetime = pd.Timestamp(as_of_date)
            # Convert event_date column to datetime if needed
            if splits["event_date"].dtype != "datetime64[ns]" and not str(splits["event_date"].dtype).startswith("datetime64"):
                splits["event_date"] = pd.to_datetime(splits["event_date"])
            # Make as_of_datetime timezone-aware to match event_date if needed
            if hasattr(splits["event_date"].dt, "tz") and splits["event_date"].dt.tz is not None:
                if as_of_datetime.tzinfo is None:
                    as_of_datetime = as_of_datetime.tz_localize(splits["event_date"].dt.tz)
            future_splits = splits[splits["event_date"] > as_of_datetime]

            if not future_splits.empty:
                cumulative_split_factor = future_splits["ratio"].prod()

                if cumulative_split_factor != 1.0:
                    # FIX (2025-12-12): Handle BOTH forward AND reverse splits
                    # Forward splits: factor > 1 (e.g., GOOG 20:1 → factor=20, strike $127.50 → $2550)
                    # Reverse splits: factor < 1 (e.g., GE 1:8 → factor=0.125, strike $80 → $10)
                    # Multiply current strike by factor to get original pre-split strike
                    original_strike = original_strike * cumulative_split_factor

                    # PERF: Strike conversion can happen many times in option-heavy backtests.
                    # Keep this at DEBUG so production backtests don't pay the logging cost unless
                    # explicitly enabled.
                    logger.debug(
                        "[ThetaData] Split adjustment for option query: %s strike $%.2f -> $%.2f "
                        "(factor %.4f from %d splits)",
                        option_asset.symbol,
                        float(option_asset.strike),
                        original_strike,
                        cumulative_split_factor,
                        len(future_splits),
                    )

    return original_strike


def _is_third_friday(expiration_date: date) -> bool:
    """Return True when the given date is the standard monthly options expiration (3rd Friday)."""
    try:
        # Friday=4; the third Friday falls between the 15th and 21st.
        return expiration_date.weekday() == 4 and 15 <= expiration_date.day <= 21
    except Exception:
        return False


def _thetadata_option_root_symbol(option_asset: Asset) -> str:
    """Resolve ThetaData option root symbol (e.g., SPX vs SPXW) for a given option contract."""
    symbol = str(getattr(option_asset, "symbol", "") or "").upper()
    expiration = getattr(option_asset, "expiration", None)

    # Default behavior for equities/most underlyings.
    if symbol != "SPX":
        return getattr(option_asset, "symbol", symbol)

    if not expiration:
        return "SPX"

    exp_date = expiration
    if isinstance(exp_date, datetime):
        exp_date = exp_date.date()
    elif hasattr(exp_date, "date") and not isinstance(exp_date, date):
        # pandas.Timestamp
        exp_date = exp_date.date()

    if isinstance(exp_date, date) and not _is_third_friday(exp_date):
        # SPX weeklies/0DTE expirations live under SPXW.
        return "SPXW"

    return "SPX"


def _apply_corporate_actions_to_frame(
    asset: Asset,
    frame: pd.DataFrame,
    start_day: date,
    end_day: date,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    asset_type = str(getattr(asset, "asset_type", "stock")).lower()
    if asset_type not in {"stock", "option"}:
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

    if asset_type == "option":
        # Options use split-normalized strikes in strategy code (e.g., GOOG strike 130 post-split).
        # Theta stores historical option data under the pre-split strike (e.g., 2600), so we:
        # 1) query the correct strike for the date range, then
        # 2) split-adjust the returned OHLC/NBBO so the option price series is continuous in
        #    post-split terms (matching split-adjusted underlying prices).
        #
        # This prevents false stop-loss triggers and portfolio cliffs on split dates.
        if "dividend" not in frame.columns:
            frame["dividend"] = 0.0
        else:
            frame["dividend"] = 0.0

        if "stock_splits" not in frame.columns:
            frame["stock_splits"] = 0.0

        underlying_asset = getattr(asset, "underlying_asset", None)
        if underlying_asset is None:
            underlying_asset = Asset(getattr(asset, "symbol", ""), asset_type="stock")

        today = _corporate_action_horizon_date()
        splits = _get_theta_splits(underlying_asset, start_day, today, username, password)

        if splits is None or splits.empty:
            frame["stock_splits"] = frame["stock_splits"].fillna(0.0)
            frame["_split_adjusted"] = True
            return frame

        split_map = splits.groupby(splits["event_date"].dt.date)["ratio"].prod().to_dict()
        frame["stock_splits"] = [float(split_map.get(day, 0.0)) for day in index_dates]

        sorted_splits = splits.sort_values("event_date")
        applicable_splits = sorted_splits[sorted_splits["event_date"].dt.date <= today]
        split_dates = applicable_splits["event_date"].dt.date.tolist()
        split_ratios = applicable_splits["ratio"].tolist()

        cumulative_factor = pd.Series(1.0, index=frame.index)
        for split_date, ratio in zip(reversed(split_dates), reversed(split_ratios)):
            if ratio > 0 and ratio != 1.0:
                mask = pd.Series(index_dates) < split_date
                cumulative_factor.loc[mask.values] *= ratio

        price_columns = ["open", "high", "low", "close", "bid", "ask", "mid_price"]
        available_price_cols = [col for col in price_columns if col in frame.columns]
        for col in available_price_cols:
            frame[col] = pd.to_numeric(frame[col], errors="coerce") / cumulative_factor

        if "strike" in frame.columns:
            frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce") / cumulative_factor

        if "volume" in frame.columns:
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * cumulative_factor

        frame["_split_adjusted"] = True
        return frame

    dividends = _get_theta_dividends(asset, start_day, end_day, username, password)
    # CRITICAL: Fetch splits up to the corporate-action horizon, not the data's end date!
    # When fetching March 2020 data, we still need to know about the July 2022 split
    # so we can adjust historical prices to be comparable to current prices.
    # This matches Yahoo Finance behavior where Adj Close always reflects current splits.
    today = _corporate_action_horizon_date()
    splits = _get_theta_splits(asset, start_day, today, username, password)
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
        # Apply split adjustment to any price-like columns we may have (OHLC, NBBO, etc).
        # Intraday quote requests may return bid/ask instead of OHLC; keep those consistent too.
        price_columns = ["open", "high", "low", "close", "bid", "ask", "mid_price"]
        available_price_cols = [col for col in price_columns if col in frame.columns]

        if available_price_cols:
            # Sort splits by date (oldest first)
            sorted_splits = splits.sort_values("event_date")

            # IMPORTANT: Apply ALL splits up to the corporate-action horizon, not the data's end date.
            # When we fetch March 2020 data in 2025, we need to apply the July 2022 split
            # so that historical prices are comparable to current split-adjusted prices.
            # This matches how Yahoo Finance calculates Adj Close - it always reflects
            # the current share count, not what the shares were worth at that time.
            applicable_splits = sorted_splits[sorted_splits["event_date"].dt.date <= today]

            if len(applicable_splits) < len(sorted_splits):
                skipped = len(sorted_splits) - len(applicable_splits)
                logger.debug(
                    "[THETA][SPLIT_ADJUST] Skipping %d future split(s) after today=%s",
                    skipped,
                    today,
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


def _market_close_utc_for_date(trading_date: date) -> datetime:
    """Return a UTC timestamp for the US equity market close on `trading_date`.

    ThetaData EOD payloads are keyed by trading date, but many responses are timestamped at
    `00:00 UTC`. When later converted to America/New_York this lands on the prior evening,
    which makes the full-day OHLC/NBBO appear available before the market session begins.

    For day-cadence backtests that run at market open, that behavior creates lookahead bias.
    We normalize all day bars (and placeholder rows) to 16:00 America/New_York so the bar is
    associated with the correct trading date and only becomes observable after the session ends.
    """
    close_local = LUMIBOT_DEFAULT_PYTZ.localize(
        datetime(trading_date.year, trading_date.month, trading_date.day, 16, 0)
    )
    return close_local.astimezone(pytz.UTC)


def _align_day_index_to_market_close_utc(frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Normalize a day-cadence ThetaData frame to market-close timestamps in UTC.

    This transform is date-keyed and idempotent: applying it multiple times yields the same
    index values for the same set of trading dates.
    """
    if frame is None or frame.empty:
        return frame

    if not isinstance(frame.index, pd.DatetimeIndex):
        frame = frame.copy()
        frame.index = pd.to_datetime(frame.index, utc=True)

    idx_utc = pd.to_datetime(frame.index, utc=True)
    new_index = pd.DatetimeIndex(
        [_market_close_utc_for_date(d) for d in idx_utc.date],
        name=frame.index.name,
    )

    if frame.index.equals(new_index):
        return frame

    aligned = frame.copy()
    aligned.index = new_index
    if "datetime" in aligned.columns:
        aligned["datetime"] = new_index
    return aligned


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
        df_all.index = pd.DatetimeIndex([], name="datetime", tz="UTC")

    df_all = ensure_missing_column(df_all)

    placeholder_index = pd.DatetimeIndex(
        [_market_close_utc_for_date(d) for d in missing_dates],
        name="datetime",
    )

    if len(placeholder_index):
        CONNECTION_DIAGNOSTICS["placeholder_writes"] = CONNECTION_DIAGNOSTICS.get("placeholder_writes", 0) + len(placeholder_index)

        # DEBUG-LOG: Placeholder injection
        logger.debug(
            "[THETA][DEBUG][PLACEHOLDER][INJECT] count=%d dates=%s",
            len(placeholder_index),
            ", ".join(sorted({d.isoformat() for d in missing_dates}))
        )

        # PERF: Avoid pandas FutureWarning spam from concatenating all-NA placeholder frames.
        # Reindex onto the union of existing + placeholder timestamps instead.
        try:
            df_all_index = df_all.index
            if not isinstance(df_all_index, pd.DatetimeIndex):
                df_all = df_all.copy()
                df_all.index = pd.to_datetime(df_all.index, utc=True)
            elif getattr(df_all_index, "tz", None) is None:
                df_all = df_all.copy()
                df_all.index = pd.to_datetime(df_all.index, utc=True)
        except Exception:
            pass

        df_all = df_all.reindex(df_all.index.union(placeholder_index)).sort_index()
        df_all.loc[placeholder_index, "missing"] = True
        df_all = df_all[~df_all.index.duplicated(keep="last")]
        logger.debug(
            "[THETA][DEBUG][THETADATA-CACHE] recorded %d placeholder day(s): %s",
            len(placeholder_index),
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
    include_eod_nbbo: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Queries ThetaData for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/{CACHE_SUBFOLDER} folder so that it can be reused later and we don't have to query
    ThetaData every time we run a backtest.

    Returns pandas DataFrames for backwards compatibility. Polars output is not
    currently supported; callers requesting polars will receive a ValueError.

    Parameters
    ----------
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
    include_eod_nbbo : bool
        When True, keep NBBO quote columns (bid/ask/etc) returned by Theta's EOD endpoint for day bars.
        This is primarily used to enable `get_quote()` for options in day-mode backtests where trade
        prices can be missing but quotes are still present.
    username : Optional[str]
        ThetaData username (backwards compatible; ignored when using the remote data downloader).
    password : Optional[str]
        ThetaData password (backwards compatible; ignored when using the remote data downloader).

    Returns
    -------
    Optional[pd.DataFrame]
        A pandas DataFrame with the pricing data for the asset

    """
    import pytz  # Import at function level to avoid scope issues in nested calls

    # DEBUG-LOG: Entry point for ThetaData request.
    #
    # PERF: `get_price_data()` can be called tens of thousands of times in option-heavy backtests.
    # Avoid eager `.isoformat()` / string building unless debug logging is actually enabled.
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "[THETA][DEBUG][REQUEST][ENTRY] asset=%s quote=%s start=%s end=%s dt=%s timespan=%s datastyle=%s include_after_hours=%s return_polars=%s",
            asset,
            quote_asset,
            start,
            end,
            dt,
            timespan,
            datastyle,
            include_after_hours,
            return_polars,
        )

    if return_polars:
        raise ValueError("ThetaData polars output is not available; pass return_polars=False.")

    def _truthy_env(name: str, default: str = "true") -> bool:
        value = os.environ.get(name, default)
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    # Corporate-action normalization for intraday frames is primarily needed for *backtests*:
    # option chains (and day bars) are split-normalized to today's share count, and intraday
    # stock OHLC must match that scale or options strategies can select invalid strikes.
    #
    # Default: enabled when `IS_BACKTESTING` is truthy, disabled otherwise. Can be overridden via
    # `THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY` for debugging/backwards compatibility.
    if os.environ.get("THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY") is None:
        apply_intraday_corporate_actions = _truthy_env("IS_BACKTESTING", "false")
    else:
        apply_intraday_corporate_actions = _truthy_env("THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY", "false")

    # Preserve original bounds for final filtering
    requested_start = start
    requested_end = end

    # Defensive: callers should never request an inverted window, but it can occur when clamping
    # to option expiration or applying offsets. Return an empty frame rather than crashing deep
    # inside calendar scheduling.
    try:
        if start is not None and end is not None and start > end:
            logger.warning(
                "[THETA][WARN][REQUEST] Ignoring inverted request window for %s: start=%s end=%s",
                getattr(asset, "symbol", asset),
                start,
                end,
            )
            return pd.DataFrame()
    except Exception:
        pass

    # Check if we already have data for this asset in the cache file
    df_all = None
    df_cached = None
    cache_invalid = False
    cache_file = build_cache_filename(asset, timespan, datastyle)
    remote_payload = build_remote_cache_payload(asset, timespan, datastyle)
    provider_expiration = _option_provider_expiration_for_asset(asset)
    if provider_expiration is not None:
        remote_payload["provider_expiration"] = provider_expiration
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

        # PERF: Sidecars are optional. Downloading them doubles S3 roundtrips for option-heavy
        # strategies (parquet + sidecar per contract). If the sidecar is missing locally we fall
        # back to computing metadata from the parquet itself.

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
        try:
            df_cached = load_cache(
                cache_file,
                start=start,
                end=end,
                preserve_full_history=preserve_full_history,
            )
        except TypeError as exc:
            # Backwards compatibility: many unit tests (and downstream callers) stub `load_cache`
            # as a simple one-arg lambda. Only fall back when the error indicates unsupported
            # keyword arguments, otherwise re-raise.
            message = str(exc)
            if "unexpected keyword argument" not in message:
                raise
            df_cached = load_cache(cache_file)
        if df_cached is not None and not df_cached.empty:
            if timespan == "day":
                # Normalize cached day bars (and placeholders) to market-close timestamps to avoid lookahead.
                df_cached = _align_day_index_to_market_close_utc(df_cached)
            # Memory: avoid deep-copying large cached frames.
            #
            # Production backtests can reuse a cache namespace that already contains multi-year
            # intraday history for a symbol. Deep-copying here can double peak RSS and trigger
            # ECS OOMKills (which surface as BotManager ERROR_CODE_CRASH with no Python traceback).
            df_all = df_cached.copy(deep=False)
            # Ensure cached daily data is corporate-action adjusted BEFORE any merge/update.
            # This prevents mixing adjusted and unadjusted rows (and partial `_split_adjusted` markers)
            # when we append new EOD data over time, especially for options that span splits.
            if timespan == "day":
                try:
                    cache_index_dates = pd.to_datetime(df_all.index, utc=True).date
                    start_for_adjust = min(cache_index_dates) if len(cache_index_dates) else start.date()
                    end_for_adjust = max(cache_index_dates) if len(cache_index_dates) else end.date()
                    df_all = _apply_corporate_actions_to_frame(asset, df_all, start_for_adjust, end_for_adjust)
                except Exception:
                    logger.debug(
                        "[THETA][SPLIT_ADJUST] Failed to apply corporate actions to cached frame for %s",
                        asset,
                        exc_info=True,
                    )

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

    if (
        df_all is not None
        and not df_all.empty
        and cached_rows > 0
        and placeholder_rows == cached_rows
        and _placeholder_option_cache_needs_provider_expiry_refresh(
            asset,
            sidecar_data,
            remote_payload,
            allow_legacy_without_payload=(timespan != "day" and str(datastyle).lower() == "quote"),
        )
    ):
        cache_invalid = True
        logger.info(
            "[THETA][CACHE][PLACEHOLDER_PROVIDER_EXPIRY_REFRESH] asset=%s span=%s datastyle=%s; "
            "placeholder-only cache was written without the current provider expiration mapping",
            asset,
            timespan,
            datastyle,
        )

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
        - Integrity failures (unparseable_index, duplicate_index): DELETE cache
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

        requested_start_date = requested_start_dt.date()
        requested_end_date = requested_end_dt.date()

        # Validate sidecar alignment
        if sidecar_data:
            rows_match = sidecar_data.get("rows") in (None, total_rows) or int(sidecar_data.get("rows", 0)) == total_rows
            placeholders_match = sidecar_data.get("placeholders") in (None, placeholder_rows) or int(sidecar_data.get("placeholders", 0)) == placeholder_rows
            min_match = sidecar_data.get("min") is None or sidecar_data.get("min") == (min_ts.isoformat() if hasattr(min_ts, "isoformat") else None)
            max_match = sidecar_data.get("max") is None or sidecar_data.get("max") == (max_ts.isoformat() if hasattr(max_ts, "isoformat") else None)
            # Checksum validation is intentionally skipped for performance. Parquet corruption is
            # surfaced at read time; the remaining fields catch most logical mismatches cheaply.
            if not all([rows_match, placeholders_match, min_match, max_match]):
                # Sidecar is best-effort metadata; it can become out-of-sync with the parquet
                # (e.g., non-atomic remote uploads, legacy caches). Treat mismatch as a warning,
                # not a cache-corruption signal: keep the parquet and avoid unnecessary refetches.
                logger.debug(
                    "[THETA][CACHE][SIDECAR_MISMATCH] cache_file=%s rows=%d placeholders=%d min=%s max=%s sidecar=%s",
                    cache_file.name,
                    total_rows,
                    placeholder_rows,
                    min_ts.isoformat() if hasattr(min_ts, "isoformat") else min_ts,
                    max_ts.isoformat() if hasattr(max_ts, "isoformat") else max_ts,
                    sidecar_data,
                )
                return True, "sidecar_mismatch", False

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
                logger.debug(
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
    # Sidecar backfill intentionally skipped: sidecars are optional and should not add overhead for
    # option-heavy backtests.

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

    # Schema upgrade: historical option EOD caches created before NBBO support do not contain bid/ask columns.
    # When the caller requires NBBO columns (for option quotes), force a refresh of the requested window so
    # we can populate quote columns and avoid "No valid price" downstream.
    if (
        include_eod_nbbo
        and timespan == "day"
        and datastyle == "ohlc"
        and df_all is not None
        and not df_all.empty
        and str(getattr(asset, "asset_type", "stock")).lower() == "option"
        and not {"bid", "ask"}.issubset(df_all.columns)
    ):
        placeholder_only = False
        try:
            if "missing" in df_all.columns:
                placeholder_only = bool(df_all["missing"].fillna(False).astype(bool).all())
        except Exception:
            placeholder_only = False
        if placeholder_only:
            logger.info(
                "[THETA][CACHE][SCHEMA_UPGRADE] asset=%s span=%s datastyle=%s missing_nbbo_cols=True but cache is placeholder-only; skipping refresh",
                asset,
                timespan,
                datastyle,
            )
        else:
            cache_invalid = True
            logger.info(
                "[THETA][CACHE][SCHEMA_UPGRADE] asset=%s span=%s datastyle=%s missing_nbbo_cols=True; forcing refresh to include bid/ask",
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

    # CI/acceptance runs enforce a strict "no queue submissions" invariant and run with isolated
    # per-test cache folders. For intraday timespans, computing missing coverage all the way to
    # `end` (often the backtest window end) can cause an early backtest iteration to attempt to
    # fetch *future* days that are not needed yet. That both slows tests and can trigger the
    # downloader queue if the warm cache is missing the tail.
    #
    # In CI, bound the "coverage required" horizon to the current simulation timestamp (`dt`) so
    # we only validate/fill what the backtest can actually use at that moment.
    missing_end = end
    try:
        is_ci = (os.environ.get("GITHUB_ACTIONS", "").lower() == "true") or bool(os.environ.get("CI"))
        is_backtesting = _truthy_env("IS_BACKTESTING", "false")
        if is_ci and is_backtesting and cache_manager.enabled and timespan != "day" and dt is not None:
            missing_end = min(end, dt)
    except Exception:
        missing_end = end

    if cache_invalid:
        missing_dates = get_trading_dates(asset, start, missing_end)
    else:
        missing_dates = get_missing_dates(df_all, asset, start, missing_end)

    if (
        timespan == "day"
        and not cache_invalid
        and df_all is not None
        and "missing" in df_all.columns
        and missing_dates
    ):
        placeholder_dates = set(pd.Index(df_all[df_all["missing"].astype(bool)].index.date))
        if placeholder_dates:
            today_utc = datetime.now(pytz.UTC).date()
            suppress_dates: set[date] = {d for d in placeholder_dates if d > today_utc}
            if getattr(asset, "asset_type", None) == "option" and getattr(asset, "expiration", None) is not None:
                try:
                    exp = asset.expiration
                    suppress_dates |= {d for d in placeholder_dates if d > exp}
                except Exception:
                    pass

            if suppress_dates:
                before = len(missing_dates)
                missing_dates = [d for d in missing_dates if d not in suppress_dates]
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

    # Intraday coverage check: for non-day timespans, `get_missing_dates()` only reasons about full
    # trading days (by date), not whether the cached frame actually reaches the requested window.
    #
    # This can produce "partially warm" caches (e.g., only through 15:00 ET) that are treated as hits,
    # causing stale tails and inconsistent interval parity (minute vs 5minute/15minute/hour).
    #
    # Important nuance: most backtesting callers pass `start/end` as full-day bounds (midnight→midnight)
    # while also passing `dt` (the current simulation timestamp). For those calls we only need to
    # validate coverage through `dt`, not through the full-day `end` bound.
    if timespan != "day" and df_all is not None and not df_all.empty:
        # Placeholder-only *option* intraday caches are valid negative caches (e.g., ThetaData 472/no quotes
        # for a contract/day). They deliberately do not contain a "session open" row, so enforcing intraday
        # min/max coverage would incorrectly mark them as missing and trigger repeated downloader requests.
        #
        # Acceptance/CI runs start from empty disks; without this skip, placeholder-only option caches become
        # perpetual cache-misses even when S3 is warm.
        try:
            is_option = str(getattr(asset, "asset_type", "") or "").lower() == "option"
            placeholder_only_option = (
                is_option
                and "missing" in df_all.columns
                and bool(df_all["missing"].fillna(False).astype(bool).all())
            )
        except Exception:
            placeholder_only_option = False
        if placeholder_only_option:
            # Keep `missing_dates` as-is (usually empty after `get_missing_dates()`).
            pass
        else:
            try:
                idx = pd.to_datetime(df_all.index, utc=True, errors="coerce")
                idx = idx[~pd.isna(idx)]
                if len(idx):
                    min_ts = idx.min()
                    max_ts = idx.max()

                    start_utc = pd.to_datetime(requested_start, utc=True, errors="coerce")
                    end_utc = pd.to_datetime(requested_end, utc=True, errors="coerce")
                    if pd.notna(start_utc) and pd.notna(end_utc):
                        # Clamp the "required" coverage end to `dt` when provided.
                        end_check_utc = end_utc
                        if dt is not None:
                            try:
                                dt_check = pd.to_datetime(dt, errors="coerce")
                                if pd.notna(dt_check):
                                    if dt_check.tz is None:
                                        dt_check = dt_check.tz_localize(LUMIBOT_DEFAULT_PYTZ)
                                    else:
                                        dt_check = dt_check.tz_convert(LUMIBOT_DEFAULT_PYTZ)
                                    dt_check_utc = dt_check.tz_convert(pytz.UTC)
                                    if dt_check_utc >= start_utc:
                                        end_check_utc = min(end_check_utc, dt_check_utc)
                            except Exception:
                                pass

                        # If the requested end is exactly midnight, treat the calendar window as
                        # end-exclusive so we don't incorrectly include the next trading day.
                        calendar_end = requested_end
                        try:
                            if getattr(requested_end, "hour", None) == 0 and getattr(requested_end, "minute", None) == 0:
                                calendar_end = requested_end - timedelta(seconds=1)
                        except Exception:
                            calendar_end = requested_end

                        trading_dates = get_trading_dates(asset, requested_start, calendar_end)
                        if trading_dates:
                            try:
                                from lumibot.tools.helpers import get_trading_days

                                def _session_bounds_utc(day: date) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
                                    schedule = get_trading_days(
                                        market="NYSE",
                                        start_date=day,
                                        end_date=day + timedelta(days=1),
                                        tzinfo=LUMIBOT_DEFAULT_PYTZ,
                                    )
                                    if schedule is None or schedule.empty:
                                        return None, None
                                    open_dt = schedule["market_open"].iloc[0]
                                    close_dt = schedule["market_close"].iloc[0]
                                    open_utc = pd.Timestamp(open_dt).tz_convert(pytz.UTC)
                                    close_utc = pd.Timestamp(close_dt).tz_convert(pytz.UTC)
                                    return open_utc, close_utc
                            except Exception:
                                _session_bounds_utc = None

                            tolerance = timedelta(minutes=2)
                            first_day = trading_dates[0]
                            last_day = trading_dates[-1]

                            # Start-of-window: if caller requests midnight, we only require the cache to
                            # include the regular-session open (09:30 ET), not midnight/pre-market.
                            required_start = start_utc
                            if _session_bounds_utc is not None:
                                open_utc, _ = _session_bounds_utc(first_day)
                                if open_utc is not None:
                                    required_start = max(required_start, open_utc)
                            if min_ts > required_start + tolerance:
                                missing_dates = sorted(set(missing_dates or []) | {first_day})

                            # End-of-window: require coverage through the earlier of (dt, end) bounded by
                            # the regular-session close (16:00 ET) for that day.
                            required_end = end_check_utc
                            if _session_bounds_utc is not None:
                                _, close_utc = _session_bounds_utc(last_day)
                                if close_utc is not None:
                                    required_end = min(required_end, close_utc)
                            if max_ts < required_end - tolerance:
                                missing_dates = sorted(set(missing_dates or []) | {last_day})
            except Exception:
                # Coverage checks are best-effort and must never block callers.
                pass

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

        # Corporate actions: day bars have always been split-adjusted (Yahoo-style "Adj Close" behavior).
        #
        # Critical bugfix (NVDA 2022 backtests): option chain strikes are split-normalized using *daily*
        # split-adjusted reference prices, but intraday stock OHLC was previously returned unadjusted.
        # That created a 10x mismatch after NVDA's 2024-06-10 10:1 split (e.g., underlying ~300 vs
        # strikes ~30), causing options strategies to find "no valid strike".
        #
        # Fix: apply the same corporate action normalization to intraday frames during backtests
        # (or when explicitly enabled).
        should_apply_corporate_actions = timespan == "day" or apply_intraday_corporate_actions
        if result_frame is not None and not result_frame.empty and should_apply_corporate_actions:
            start_day = start.date() if hasattr(start, "date") else start
            end_day = end.date() if hasattr(end, "date") else end
            result_frame = _apply_corporate_actions_to_frame(
                asset, result_frame, start_day, end_day, username, password
            )
        if result_frame is not None and not result_frame.empty and timespan == "day":
            result_frame = _align_day_index_to_market_close_utc(result_frame)
        return result_frame

    logger.info(
        "ThetaData cache MISS for %s %s %s; fetching %d interval(s) from ThetaTerminal.",
        asset,
        timespan,
        datastyle,
        len(missing_dates),
    )

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
    quiet_logs = os.environ.get("BACKTESTING_QUIET_LOGS", "true").lower() == "true"
    show_progress_bar = os.environ.get("BACKTESTING_SHOW_PROGRESS_BAR", "true").lower() == "true"
    disable_progress = quiet_logs or not show_progress_bar

    description = f"Downloading '{datastyle}' data for {asset} / {quote_asset} with '{timespan}' from ThetaData..."
    if disable_progress:
        logger.debug(description)
    else:
        logger.info(f"\n{description}")

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
        # Only pass include_nbbo when enabled to preserve backwards compatibility with mocks.
        eod_kwargs = dict(
            asset=asset,
            start_dt=effective_start,
            end_dt=effective_end,
            datastyle=datastyle,
        )
        if username is not None:
            eod_kwargs["username"] = username
        if password is not None:
            eod_kwargs["password"] = password
        if include_eod_nbbo:
            eod_kwargs["include_nbbo"] = True
        result_df = get_historical_eod_data(**eod_kwargs)
        logger.debug(
            "[THETA][DEBUG][THETADATA-EOD] fetched rows=%s for %s",
            0 if result_df is None else len(result_df),
            asset,
        )

        if result_df is not None and not result_df.empty:
            if "datetime" in result_df.columns and not isinstance(result_df.index, pd.DatetimeIndex):
                result_df = result_df.copy()
                result_df["datetime"] = pd.to_datetime(result_df["datetime"], utc=True, errors="coerce")
                result_df = result_df.dropna(subset=["datetime"]).set_index("datetime").sort_index()
            result_df = _align_day_index_to_market_close_utc(result_df)

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

        # EOD history results are already normalized to UTC market-close timestamps above
        # (`_align_day_index_to_market_close_utc`). Do NOT route them through `update_df()`,
        # which assumes naive datetimes are in the default market timezone and will shift
        # them incorrectly. That timezone shift can make a covered trading day look missing,
        # causing repeated downloader queue submissions for the same option/day.
        if result_df is not None and not result_df.empty:
            df_merge = result_df
            if "datetime" in df_merge.columns:
                df_merge = df_merge.copy()
                df_merge["datetime"] = pd.to_datetime(df_merge["datetime"], utc=True, errors="coerce")
                df_merge = df_merge.dropna(subset=["datetime"]).set_index("datetime").sort_index()
            else:
                df_merge = df_merge.copy()
                idx = pd.to_datetime(df_merge.index, utc=True, errors="coerce")
                df_merge = df_merge.loc[~pd.isna(idx)]
                df_merge.index = pd.DatetimeIndex(idx[~pd.isna(idx)], name="datetime")
                df_merge = df_merge.sort_index()
            df_merge = ensure_missing_column(df_merge)
            df_merge.loc[:, "missing"] = False

            if df_all is None or getattr(df_all, "empty", True):
                df_all = df_merge
            else:
                df_all = ensure_missing_column(df_all)
                df_all = pd.concat([df_all, df_merge]).sort_index()
                df_all = df_all[~df_all.index.duplicated(keep="last")]  # Keep newest data over placeholders
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
    if disable_progress:
        logger.debug(
            "ThetaData downloader requesting %d chunk(s) with up to %d parallel workers.",
            total_queries,
            chunk_workers,
        )
    else:
        logger.info(
            "ThetaData downloader requesting %d chunk(s) with up to %d parallel workers.",
            total_queries,
            chunk_workers,
        )
    pbar = tqdm(
        total=max(1, total_queries),
        desc=f"\n{description}" if not disable_progress else description,
        dynamic_ncols=True,
        disable=disable_progress,
    )

    total_download_units = total_queries
    if str(getattr(asset, "asset_type", "")).lower() != "index" or str(datastyle).lower() != "ohlc":
        # Intraday history endpoints issue one queued request per trading day. Surface that
        # granularity to the UI so downloads don't look like "0/1 then done".
        try:
            total_download_units = len(get_trading_dates(asset, fetch_start, fetch_end))
        except Exception:
            total_download_units = total_queries
    try:
        total_download_units = int(total_download_units)
    except Exception:
        total_download_units = total_queries
    total_download_units = max(1, total_download_units)

    # Set initial download status
    set_download_status(asset, quote_asset, datastyle, timespan, 0, total_download_units)

    def _fetch_chunk(chunk_start: datetime, chunk_end: datetime):
        kwargs = {
            "datastyle": datastyle,
            "include_after_hours": include_after_hours,
            "download_timespan": timespan,
        }
        if username is not None:
            kwargs["username"] = username
        if password is not None:
            kwargs["password"] = password
        return get_historical_data(asset, chunk_start, chunk_end, interval_ms, **kwargs)

    def _handle_chunk_result(
        *,
        chunk_start: datetime,
        chunk_end: datetime,
        submitted_at: float,
        result_df: Optional[pd.DataFrame],
    ) -> None:
        nonlocal df_all

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
            return

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

    # Avoid ThreadPoolExecutor overhead (and potential deadlocks) when there's only a single chunk.
    if total_queries == 1:
        chunk_start, chunk_end = chunk_ranges[0]
        submitted_at = time.perf_counter()
        try:
            result_df = _fetch_chunk(chunk_start, chunk_end)
        except Exception as exc:
            logger.warning(
                "ThetaData chunk fetch failed for %s between %s and %s: %s",
                asset,
                chunk_start,
                chunk_end,
                exc,
            )
            result_df = None
        _handle_chunk_result(
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            submitted_at=submitted_at,
            result_df=result_df,
        )
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

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

                _handle_chunk_result(
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    submitted_at=submitted_at,
                    result_df=result_df,
                )

    # Mark download complete (keep final progress payload for UI polling)
    finalize_download_status()
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

    # Apply corporate actions to intraday frames by default so underlying prices live in the same
    # split-adjusted space as option-chain strike normalization (see comment in cache-hit path).
    if (
        apply_intraday_corporate_actions
        and df_all is not None
        and not df_all.empty
        and timespan != "day"
    ):
        try:
            start_day = requested_start.date() if hasattr(requested_start, "date") else requested_start
            end_day = requested_end.date() if hasattr(requested_end, "date") else requested_end
            df_all = _apply_corporate_actions_to_frame(asset, df_all, start_day, end_day, username, password)
        except Exception:
            logger.debug(
                "[THETA][SPLIT_ADJUST] Failed to apply corporate actions to intraday frame for %s",
                asset,
                exc_info=True,
            )

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

    # Cache-miss path parity: intraday requests must be trimmed to [requested_start, requested_end]
    # just like the cache-hit path. Otherwise, callers (and tests) can receive bars outside the
    # requested window when the on-disk cache contains additional days.
    if (
        not preserve_full_history
        and df_all is not None
        and not df_all.empty
        and timespan != "day"
    ):
        import datetime as datetime_module  # Avoid shadowing `dt` parameter.

        start_bound = requested_start
        end_bound = requested_end
        if isinstance(start_bound, datetime_module.date) and not isinstance(start_bound, datetime_module.datetime):
            start_bound = datetime_module.datetime.combine(start_bound, datetime_module.time.min)
        if isinstance(end_bound, datetime_module.date) and not isinstance(end_bound, datetime_module.datetime):
            end_bound = datetime_module.datetime.combine(end_bound, datetime_module.time.max)
        if isinstance(end_bound, datetime_module.datetime) and end_bound.time() == datetime_module.time.min:
            end_bound = datetime_module.datetime.combine(end_bound.date(), datetime_module.time.max)

        if hasattr(start_bound, "tzinfo") and start_bound.tzinfo is None:
            start_bound = LUMIBOT_DEFAULT_PYTZ.localize(start_bound).astimezone(pytz.UTC)
        if hasattr(end_bound, "tzinfo") and end_bound.tzinfo is None:
            end_bound = LUMIBOT_DEFAULT_PYTZ.localize(end_bound).astimezone(pytz.UTC)

        df_all = df_all[(df_all.index >= start_bound) & (df_all.index <= end_bound)]

    return df_all




# PERFORMANCE FIX (2025-12-07): Cache calendar objects to avoid rebuilding them.
# mcal.get_calendar() is slow; caching the calendar objects saves significant time.
#
# NOTE: We intentionally use a single cache dict for both:
#   - calendar objects (key: calendar_name: str)
#   - full-year schedules (key: (calendar_name: str, year: int))
# so that tests (and any debugging code) can clear *all* calendar-related caches by calling
# `_CALENDAR_CACHE.clear()` once.
_CALENDAR_CACHE: Dict[object, object] = {}


def _get_cached_calendar(name: str):
    """Get or create a cached market calendar object."""
    # IMPORTANT (test isolation / monkeypatch safety):
    # Some unit tests monkeypatch `pandas_market_calendars.get_calendar` with a dummy implementation.
    # If we cache calendar objects under a stable key, that dummy calendar can leak into subsequent
    # tests and produce incorrect trading dates (e.g., treating US holidays as business days).
    #
    # Key by the identity of the calendar factory so restoring the original implementation yields a
    # different cache key without requiring explicit cache clears.
    cache_key = ("calendar", name, id(mcal.get_calendar))
    cached = _CALENDAR_CACHE.get(cache_key)
    if cached is None:
        cached = mcal.get_calendar(name)
        _CALENDAR_CACHE[cache_key] = cached
    return cached


# PERFORMANCE (2026-01-03): Cache full-year schedules and slice for sub-ranges.
#
# Why: backtests frequently request trading dates for many overlapping (start, end) pairs
# (e.g., per-bar alignment, session-close alignment). Even with an LRU cache on the exact
# (start_date, end_date) tuple, production can still see hundreds of unique ranges in a
# single backtest, causing repeated `calendar.schedule(...)` calls (expensive holiday logic).
#
# Strategy: cache `schedule()` results per (calendar_name, year) and slice cheaply for the
# requested range. This preserves correctness (NYSE holidays/half-days) while drastically
# reducing calendar.schedule churn.
#
# NOTE: We store these schedules in `_CALENDAR_CACHE` so clearing that dict also clears
# schedule caching (important for legacy tests that monkeypatch the calendar impl).
def _cached_year_schedule(calendar_name: str, year: int) -> pd.DataFrame:
    # Include the calendar factory identity to avoid reusing schedules generated under a monkeypatched
    # calendar in later tests.
    key = (calendar_name, year, id(mcal.get_calendar))
    schedule = _CALENDAR_CACHE.get(key)
    if schedule is not None:
        return schedule  # type: ignore[return-value]

    cal = _get_cached_calendar(calendar_name)
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    schedule = cal.schedule(start_date=start, end_date=end)
    _CALENDAR_CACHE[key] = schedule
    return schedule


@functools.lru_cache(maxsize=2048)  # Increased from 512 for longer backtests
def _cached_trading_dates(asset_type: str, start_date: date, end_date: date, calendar_version: int) -> List[date]:
    """Memoized trading-day resolver to avoid rebuilding calendars every call.

    PERFORMANCE FIX (2025-12-07): Increased cache size and use cached calendars.
    """
    # calendar_version is intentionally unused in the logic below; it exists solely to ensure the
    # LRU cache is invalidated when the calendar factory is monkeypatched (tests).
    if asset_type == "crypto":
        return [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    if asset_type == "stock" or asset_type == "option" or asset_type == "index":
        calendar_name = "NYSE"
    elif asset_type == "forex":
        calendar_name = "CME_FX"
    else:
        raise ValueError(f"Unsupported asset type for thetadata: {asset_type}")

    def _slice_year(year: int, start: date, end: date) -> List[date]:
        schedule = _cached_year_schedule(calendar_name, year)
        # DatetimeIndex slicing accepts ISO date strings.
        df_slice = schedule.loc[str(start) : str(end)]
        return df_slice.index.date.tolist()

    if start_date.year == end_date.year:
        return _slice_year(start_date.year, start_date, end_date)

    dates_out: List[date] = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = start_date if year == start_date.year else date(year, 1, 1)
        year_end = end_date if year == end_date.year else date(year, 12, 31)
        dates_out.extend(_slice_year(year, year_start, year_end))
    return dates_out


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
    try:
        if start_date > end_date:
            logger.debug(
                "[THETA][DEBUG][TRADING_DATES] start_date=%s after end_date=%s for asset=%s; returning empty list",
                start_date,
                end_date,
                getattr(asset, "symbol", asset),
            )
            return []
    except Exception:
        # If dates are not comparable, fall through and let the calendar path raise.
        pass
    return list(_cached_trading_dates(asset.asset_type, start_date, end_date, id(mcal.get_calendar)))


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


def build_snapshot_cache_filename(
    asset: Asset,
    *,
    trading_day: date,
    interval_label: str,
    start_time: str,
    end_time: str,
    datastyle: str,
) -> Path:
    """Build a cache filename for small, point-in-time intraday history windows.

    Why this exists:
    - `get_quote(snapshot_only=True)` intentionally requests only a tiny intraday window around
      the simulation timestamp (e.g., 09:30 → 09:35) to avoid downloading full-day quote history
      for contracts that will never be traded.
    - Those snapshot requests must still be cacheable (S3 warm-cache invariant) so repeated runs
      can avoid downloader-queue usage entirely.

    This cache is stored separately from the normal {timespan}/{datastyle} caches to avoid
    "partial day" coverage being misinterpreted as a full-day cache hit.
    """
    provider_root = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER
    asset_folder = _resolve_asset_folder(asset)
    datastyle_folder = _normalize_folder_component(datastyle, "default")
    base_folder = provider_root / asset_folder / "snapshot" / datastyle_folder

    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    day_string = trading_day.strftime("%Y%m%d")
    start_compact = str(start_time).replace(":", "")
    end_compact = str(end_time).replace(":", "")
    cache_filename = (
        f"{asset.asset_type}_{uniq_str}_snapshot_{day_string}_{interval_label}_{start_compact}_{end_compact}_{datastyle}.parquet"
    )
    return base_folder / cache_filename


def get_historical_data_snapshot_cached(
    asset: Asset,
    start_dt: datetime,
    end_dt: datetime,
    ivl: int,
    *,
    datastyle: str = "quote",
    include_after_hours: bool = True,
    prefer_full_session: bool = True,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Cache-backed wrapper for `get_historical_data()` for tiny single-day windows.

    This is primarily used by `ThetaDataBacktestingPandas.get_quote(snapshot_only=True)` to
    preserve the warm-cache invariant: if S3 is warmed, we must not enqueue downloader work.
    """
    trading_days = get_trading_dates(asset, start_dt, end_dt)
    if len(trading_days) != 1:
        return get_historical_data(
            asset,
            start_dt,
            end_dt,
            ivl,
            datastyle=datastyle,
            include_after_hours=include_after_hours,
            username=username,
            password=password,
        )

    # CI/acceptance backtests run with a strict "no downloader queue" invariant. Snapshot cache
    # filenames are window-specific, so they may not exist in the warm S3 namespace even when the
    # canonical date-based parquet caches are already present.
    #
    # In explicit S3_READONLY mode, prefer the canonical (non-snapshot) cache layout via
    # `get_historical_data` so we reuse warmed objects instead of attempting to create new
    # snapshot objects.
    try:
        from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache

        cache_manager = get_backtest_cache()
        if cache_manager.enabled and cache_manager.mode == CacheMode.S3_READONLY:
            return get_historical_data(
                asset,
                start_dt,
                end_dt,
                ivl,
                datastyle=datastyle,
                include_after_hours=include_after_hours,
                username=username,
                password=password,
            )
    except Exception:
        pass

    interval_label = _interval_label_from_ms(ivl)
    day = trading_days[0]
    start_local = _normalize_market_datetime(start_dt)
    end_local = _normalize_market_datetime(end_dt)

    # Options do not trade during extended hours, and requesting quote history outside the regular
    # session can produce placeholder-only responses (472). For snapshot-only quote probes we keep
    # option caches strictly aligned to the regular session to:
    # - minimize payload size,
    # - avoid "refetch forever" behavior when extended hours are empty,
    # - ensure cache windows are comparable across runs (S3 warm invariant).
    effective_after_hours = include_after_hours
    if str(getattr(asset, "asset_type", "")).lower() == "option":
        effective_after_hours = False

    session_start, session_end = _compute_session_bounds(
        day,
        start_local,
        end_local,
        effective_after_hours,
        prefer_full_session=prefer_full_session,
    )

    cache_file = build_snapshot_cache_filename(
        asset,
        trading_day=day,
        interval_label=interval_label,
        start_time=session_start,
        end_time=session_end,
        datastyle=datastyle,
    )
    remote_payload = build_remote_cache_payload(asset, "snapshot", datastyle)
    remote_payload.update(
        {
            "trading_day": day.isoformat(),
            "interval": interval_label,
            "start_time": session_start,
            "end_time": session_end,
            "include_after_hours": effective_after_hours,
        }
    )

    cache_manager = get_backtest_cache()
    if cache_manager.enabled:
        try:
            cache_manager.ensure_local_file(cache_file, payload=remote_payload)
        except Exception:
            pass

    df_existing = None
    if cache_file.exists():
        try:
            df_existing = load_cache(cache_file)
        except Exception:
            df_existing = None
        if df_existing is not None and not df_existing.empty:
            # IMPORTANT: Placeholder-only snapshot caches (all rows missing=True) are valid negative
            # caches for the full session. Do NOT refetch them.
            #
            # Without this, CI/acceptance runs that start from empty disks will:
            # 1) download the placeholder from S3,
            # 2) decide it "doesn't cover the full session" via index-range heuristics, and then
            # 3) enqueue downloader work every run, violating the warm-cache invariant.
            if prefer_full_session and str(getattr(asset, "asset_type", "")).lower() == "option":
                try:
                    if "missing" in df_existing.columns:
                        missing_flags = df_existing["missing"].fillna(False).astype(bool)
                        if bool(missing_flags.all()):
                            return df_existing
                except Exception:
                    # If the missing column is malformed, treat this as a stable negative cache
                    # rather than risk infinite refetch loops in production backtests.
                    return df_existing

            # Backward-compat: older runs could have written a tiny dt-window payload under a
            # full-session cache key. For options, ensure the cached frame covers the whole
            # regular session; otherwise refetch once and overwrite the cache.
            if df_existing is None:
                # Stale placeholder: fall through to fetch and overwrite the cache.
                pass
            elif prefer_full_session and str(getattr(asset, "asset_type", "")).lower() == "option":
                try:
                    start_t = datetime.strptime(session_start, "%H:%M:%S").time()
                    end_t = datetime.strptime(session_end, "%H:%M:%S").time()
                    expected_start = _normalize_market_datetime(datetime.combine(day, start_t))
                    expected_end = _normalize_market_datetime(datetime.combine(day, end_t))
                    tolerance = timedelta(minutes=2)
                    idx = df_existing.index
                    idx_min = idx.min()
                    idx_max = idx.max()
                    if getattr(idx_min, "tzinfo", None) is None:
                        idx_min = _normalize_market_datetime(idx_min)
                    if getattr(idx_max, "tzinfo", None) is None:
                        idx_max = _normalize_market_datetime(idx_max)

                    covers = idx_min <= expected_start + tolerance and idx_max >= expected_end - tolerance
                    if covers:
                        return df_existing
                except Exception:
                    # If anything about the index is unexpected, prefer using the cached payload
                    # rather than risking repeated refetches in production backtests.
                    return df_existing
            else:
                return df_existing

    fetch_start = start_dt
    fetch_end = end_dt
    if prefer_full_session:
        # PERF: Cache a stable per-(asset, trading_day) quote history payload. Backtests that
        # call `get_quote(snapshot_only=True)` many times per day (hourly strategies, option
        # scanners, SMART_LIMIT) otherwise create one cache file per dt-window and spend most
        # of their time enqueuing/downloading tiny payloads.
        try:
            start_t = datetime.strptime(session_start, "%H:%M:%S").time()
            end_t = datetime.strptime(session_end, "%H:%M:%S").time()
            fetch_start = _normalize_market_datetime(datetime.combine(day, start_t))
            fetch_end = _normalize_market_datetime(datetime.combine(day, end_t))
        except Exception:
            fetch_start = start_dt
            fetch_end = end_dt

    # Acceptance backtests run in CI with a strict warm-cache invariant: never enqueue work to the
    # downloader/queue. If the snapshot object isn't already warm in S3, treat it as missing rather
    # than falling back to a live fetch (which would violate the invariant).
    is_ci = (os.environ.get("GITHUB_ACTIONS", "").lower() == "true") or bool(os.environ.get("CI"))
    if is_ci and cache_manager.enabled and not cache_file.exists():
        return None

    try:
        result_df = get_historical_data(
            asset,
            fetch_start,
            fetch_end,
            ivl,
            datastyle=datastyle,
            include_after_hours=effective_after_hours,
            username=username,
            password=password,
        )
    except Exception as exc:
        # Some ThetaTerminal endpoints occasionally return internal 500s for specific historical
        # option/quote windows (observed for MELI option quote snapshots on expiration day).
        #
        # For acceptance backtests we still need the warm-cache invariant to hold: once a run has
        # observed a terminal "cannot fetch" outcome for a given snapshot window, subsequent runs
        # should not enqueue downloader work repeatedly. Record a placeholder so the cache can go warm.
        logger.warning(
            "[THETA][CACHE][SNAPSHOT] Snapshot history fetch failed; caching placeholder. "
            "asset=%s day=%s start_time=%s end_time=%s datastyle=%s error=%s",
            asset,
            day.isoformat(),
            session_start,
            session_end,
            datastyle,
            exc,
        )
        update_cache(cache_file, df_all=None, df_cached=df_existing, missing_dates=[day], remote_payload=remote_payload)
        return None

    if result_df is None or getattr(result_df, "empty", True):
        update_cache(cache_file, df_all=None, df_cached=df_existing, missing_dates=[day], remote_payload=remote_payload)
        return result_df

    update_cache(cache_file, df_all=result_df, df_cached=df_existing, missing_dates=None, remote_payload=remote_payload)
    return result_df


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

    # Backtesting end-date semantics: many callers (including acceptance backtests) represent an
    # end-exclusive date as a midnight timestamp on the following day (e.g., BACKTESTING_END=YYYY-MM-DD).
    #
    # If we treat that midnight as end-inclusive when computing trading-day coverage, we can
    # incorrectly require the next trading day and enqueue a downloader request even when the S3
    # cache is fully warm for the intended window.
    #
    # Normalize midnight end bounds to be end-exclusive for trading-date coverage.
    end_for_trading_dates = end
    try:
        if isinstance(end_for_trading_dates, datetime) and (
            end_for_trading_dates.hour,
            end_for_trading_dates.minute,
            end_for_trading_dates.second,
            end_for_trading_dates.microsecond,
        ) == (0, 0, 0, 0):
            end_for_trading_dates = end_for_trading_dates - timedelta(seconds=1)
    except Exception:
        end_for_trading_dates = end

    trading_dates = get_trading_dates(asset, start, end_for_trading_dates)

    logger.debug(
        "[THETA][DEBUG][CACHE][TRADING_DATES] asset=%s | "
        "asset_type=%s | "
        "trading_dates_count=%d first=%s last=%s",
        asset.symbol if hasattr(asset, 'symbol') else str(asset),
        getattr(asset, "asset_type", None),
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
    # Whole days are easy to check for because we can just check the dates in the index.
    #
    # IMPORTANT: Use the *market-local* trading day when computing coverage.
    #
    # Intraday ThetaData caches are stored with UTC timestamps (by design), and we typically request
    # extended-hours bars (04:00-20:00 ET). Those after-hours bars cross midnight in UTC, which
    # means `df.index.date` can "leak" into the next calendar day even when the market-local trading
    # day has not advanced yet.
    #
    # If we use UTC `.date` here, we can incorrectly conclude that the *next* trading day is already
    # covered and skip downloading it. This was observed in NVDA backtests where every other trading
    # day was missing (e.g., ET days present: Feb 3 + Feb 5; but UTC dates included Feb 4 + Feb 6 via
    # after-hours spillover), which then caused forward-filled prices and extreme slowness.
    try:
        idx = pd.to_datetime(df_working.index)
        if getattr(idx, "tz", None) is None:
            idx = idx.tz_localize(pytz.UTC)
        idx_local = idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)
        dates_series = pd.Series(idx_local.date, index=df_working.index)
    except Exception:
        # Fall back to the index-native dates if timezone conversion fails for any reason.
        dates_series = pd.Series(df_working.index.date, index=df_working.index)
    placeholder_mask = (
        df_working["missing"].astype(bool) if "missing" in df_working.columns else pd.Series(False, index=df_working.index)
    )
    placeholder_dates = set(dates_series[placeholder_mask].unique()) if hasattr(placeholder_mask, "__len__") else set()
    if hasattr(placeholder_mask, "__len__") and bool(placeholder_mask.all()):
        # If the cache is *only* placeholders:
        # - For stocks/indices this almost always indicates a bad/incomplete cache (e.g., outage) and we
        #   should refetch for full coverage.
        # - For options it can legitimately mean "no prints/quotes for this contract" across the entire
        #   window; repeatedly refetching would cause endless downloader submissions. Treat as "no
        #   missing dates" only for options.
        if asset.asset_type == "option":
            logger.info(
                "[THETA][CACHE][PLACEHOLDER_ONLY] asset=%s | placeholder-only option cache detected; skipping refetch",
                asset.symbol if hasattr(asset, 'symbol') else str(asset),
            )
            return []

        logger.debug(
            "[THETA][DEBUG][CACHE][PLACEHOLDER_ONLY] asset=%s | placeholder-only cache detected; treating as missing coverage",
            asset.symbol if hasattr(asset, 'symbol') else str(asset),
        )
        return trading_dates
    # Placeholder rows should be treated as missing coverage for past dates so we can refetch real data
    # (e.g. when caches were populated during an outage). We suppress future/expired-placeholder refetch
    # later when computing missing_dates.
    real_dates = dates_series[~placeholder_mask].unique()
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

    # Tail-placeholder suppression (options):
    #
    # For many options (especially far OTM/illiquid contracts), ThetaData can legitimately return
    # no quotes/trades on the final trading day(s). In those cases we may record placeholders at
    # the *tail* of the cache to preserve trading-day coverage, but we must not repeatedly refetch
    # those same tail days on every run (it causes endless downloader queue submissions).
    #
    # If a day is represented only by placeholders *after* the last real cached trading day, treat
    # it as "known unavailable" for refetch purposes.
    if placeholder_dates and missing_dates and asset.asset_type == "option" and cached_last is not None:
        tail_placeholder_dates = {d for d in placeholder_dates if d > cached_last}
        if tail_placeholder_dates:
            suppress_tail = tail_placeholder_dates & set(missing_dates)
            if suppress_tail:
                logger.debug(
                    "[THETA][DEBUG][CACHE][TAIL_PLACEHOLDER_SUPPRESS] asset=%s | "
                    "suppressing %d tail placeholder day(s) beyond last_real=%s",
                    asset.symbol if hasattr(asset, 'symbol') else str(asset),
                    len(suppress_tail),
                    cached_last,
                )
                missing_dates = [d for d in missing_dates if d not in suppress_tail]

    if placeholder_dates and missing_dates:
        # Backtesting correctness + performance invariant:
        # - Acceptance backtests (and warm-cache backtests in general) must be deterministic and
        #   queue-free once S3/local caches are populated.
        #
        # ThetaData caches record "missing=True" placeholder rows to mark trading days where the
        # provider returned no data. For options we already treat these placeholders as stable
        # negative caches to avoid endless refetch loops.
        #
        # For indices/stocks, repeatedly trying to "heal" placeholder days during backtests breaks
        # the warm-cache invariant by causing downloader submissions even when S3 is warm. In
        # backtests we instead treat placeholder-only days as "known unavailable" and do not
        # refetch them automatically. (If users want to heal placeholder coverage, they can wipe
        # caches or run an explicit re-warm/cold run.)
        is_backtesting = str(os.environ.get("IS_BACKTESTING", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        if is_backtesting and str(getattr(asset, "asset_type", "") or "").lower() in {"stock", "index"}:
            suppress_placeholder_days = placeholder_dates & set(missing_dates)
            if suppress_placeholder_days:
                logger.info(
                    "[THETA][CACHE][PLACEHOLDER_SUPPRESS] asset=%s | suppressing %d placeholder trading day(s) to preserve warm-cache determinism",
                    asset.symbol if hasattr(asset, "symbol") else str(asset),
                    len(suppress_placeholder_days),
                )
                missing_dates = [d for d in missing_dates if d not in suppress_placeholder_days]

        today_utc = datetime.now(pytz.UTC).date()
        suppress_dates = {d for d in placeholder_dates if d > today_utc}
        # If the cache begins with placeholder coverage before the first real date, treat those dates
        # as "known unavailable" and do not refetch them. This prevents repeated downloader-queue
        # requests for pre-coverage ranges (common for indicator padding before an asset has data).
        if cached_first is not None:
            suppress_dates |= {d for d in placeholder_dates if d < cached_first}
        if suppress_dates:
            missing_dates = [d for d in missing_dates if d not in suppress_dates]

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


def load_cache(cache_file, *, start=None, end=None, preserve_full_history: bool = False):
    """Load the data from the cache file and return a DataFrame with a DateTimeIndex.

    Performance notes
    -----------------
    For multi-year intraday caches (e.g., NVDA minute OHLC), reading the entire parquet frame into
    memory can exceed production ECS task limits (often surfacing as BotManager ERROR_CODE_CRASH
    with no Python traceback and logs ending abruptly).

    When `start`/`end` are provided and `preserve_full_history=False`, we use PyArrow's dataset
    filtering to load only the requested datetime slice.
    """
    debug_enabled = logger.isEnabledFor(logging.DEBUG)

    if not cache_file.exists():
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][CACHE][LOAD_MISSING] cache_file=%s | returning=None",
                cache_file.name,
            )
        return None

    if debug_enabled:
        try:
            size_bytes = cache_file.stat().st_size
        except Exception:
            size_bytes = 0
        logger.debug(
            "[THETA][DEBUG][CACHE][LOAD_START] cache_file=%s | size_bytes=%d",
            cache_file.name,
            size_bytes,
        )

    df = None
    use_arrow_filter = False
    if start is not None and end is not None and not preserve_full_history:
        # Use PyArrow filtering (predicate pushdown) when callers only need a slice.
        #
        # This is primarily a protection against OOM for multi-year *intraday* caches (minute/hour),
        # but we intentionally avoid filtering day bars by default: day caches are small and cheap to
        # read in full, and filtering can be error-prone when callers pass timezone-local midnight
        # bounds (common in day-cadence backtests) while parquet day caches store UTC session
        # boundary timestamps.
        cache_name = cache_file.name.lower()
        is_intraday = any(f"_{unit}_" in cache_name for unit in ("minute", "hour", "second"))
        if is_intraday:
            use_arrow_filter = True
        else:
            # For non-intraday caches, only use filtering when the file is very large.
            try:
                use_arrow_filter = cache_file.stat().st_size >= 50 * 1024 * 1024
            except Exception:
                use_arrow_filter = False

    if use_arrow_filter:
        try:
            from datetime import date as date_type
            from datetime import datetime as datetime_type
            from datetime import time as time_type

            import pyarrow.dataset as ds

            def _coerce_bound(value, *, is_end: bool):
                if value is None:
                    return None

                # Support date-only inputs (common in day-mode backtests).
                if isinstance(value, date_type) and not isinstance(value, datetime_type):
                    value = datetime_type.combine(value, time_type.max if is_end else time_type.min)

                # Pandas Timestamp -> python datetime (keeps tzinfo).
                if hasattr(value, "to_pydatetime"):
                    value = value.to_pydatetime()

                if getattr(value, "tzinfo", None) is None:
                    return LUMIBOT_DEFAULT_PYTZ.localize(value).astimezone(pytz.UTC)
                return value.astimezone(pytz.UTC)

            start_bound = _coerce_bound(start, is_end=False)
            end_bound = _coerce_bound(end, is_end=True)
            if start_bound is not None and end_bound is not None:
                dataset = ds.dataset(cache_file, format="parquet")
                flt = (ds.field("datetime") >= start_bound) & (ds.field("datetime") <= end_bound)
                table = dataset.to_table(filter=flt)
                df = table.to_pandas()
                logger.debug(
                    "[THETA][DEBUG][CACHE][LOAD_FILTER] cache_file=%s start=%s end=%s rows_read=%d",
                    cache_file.name,
                    start_bound.isoformat() if hasattr(start_bound, "isoformat") else start_bound,
                    end_bound.isoformat() if hasattr(end_bound, "isoformat") else end_bound,
                    len(df),
                )
                # If the filtered slice is empty, we can still need to detect placeholder-only
                # caches (e.g., option minute quote caches for contracts with no quotes/trades).
                #
                # When the placeholder marker timestamp falls outside the requested window,
                # a pure slice read returns 0 rows and the caller interprets that as "cache missing",
                # repeatedly submitting downloader work every run.
                #
                # To keep CI runs queue-free once S3 is warm, fall back to reading the full file
                # for *small option cache files* so placeholder-only detection can short-circuit.
                if df is not None and len(df) == 0:
                    try:
                        is_option_cache = cache_file.name.lower().startswith("option_")
                        size_bytes = cache_file.stat().st_size
                    except Exception:
                        is_option_cache = False
                        size_bytes = None
                    if is_option_cache and size_bytes is not None and size_bytes <= 2 * 1024 * 1024:
                        logger.debug(
                            "[THETA][DEBUG][CACHE][LOAD_FILTER_EMPTY_FALLBACK] cache_file=%s size_bytes=%d",
                            cache_file.name,
                            size_bytes,
                        )
                        df = None
        except Exception:
            df = None

    if df is None:
        df = pd.read_parquet(cache_file, engine="pyarrow")

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

    # Filter out bad ThetaData cache rows.
    #
    # IMPORTANT: For options, Theta EOD responses may legitimately contain OHLC=0 while still
    # providing actionable NBBO (bid/ask). We must NOT drop those rows, otherwise quote-based
    # pricing/fills fail and strategies stop trading. Similarly, we must preserve placeholder
    # rows (`missing==1`) used to maintain trading-day coverage in the cache.
    if not df.empty and all(col in df.columns for col in ["open", "high", "low", "close"]):
        all_zero_ohlc = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
        if all_zero_ohlc.any():
            is_placeholder = pd.Series(False, index=df.index)
            if "missing" in df.columns:
                try:
                    is_placeholder = df["missing"].astype(bool)
                except Exception:
                    is_placeholder = df["missing"] == 1

            has_actionable_quote = pd.Series(False, index=df.index)
            if "bid" in df.columns:
                bid_numeric = pd.to_numeric(df["bid"], errors="coerce").fillna(0)
                has_actionable_quote |= bid_numeric > 0
            if "ask" in df.columns:
                ask_numeric = pd.to_numeric(df["ask"], errors="coerce").fillna(0)
                has_actionable_quote |= ask_numeric > 0

            bad_zero_rows = all_zero_ohlc & ~is_placeholder & ~has_actionable_quote
            bad_count = int(bad_zero_rows.sum())
            if bad_count > 0:
                bad_dates = df.index[bad_zero_rows].tolist()
                is_option_payload = all(c in df.columns for c in ("expiration", "strike", "right"))
                if is_option_payload:
                    # For option day/EOD caches, an all-zero OHLC row with no actionable NBBO often
                    # means "no print/quote" (especially on expiry). Dropping it makes the day look
                    # perpetually missing and triggers repeated downloader submissions. Treat it as a
                    # placeholder coverage marker instead.
                    logger.warning(
                        "[THETA][DATA_QUALITY][CACHE] Converting %d all-zero OHLC option row(s) to placeholders: %s",
                        bad_count,
                        [str(d)[:10] for d in bad_dates[:5]],
                    )
                    for col in ("open", "high", "low", "close", "volume"):
                        if col in df.columns:
                            df.loc[bad_zero_rows, col] = float("nan")
                    df.loc[bad_zero_rows, "missing"] = True
                else:
                    logger.warning(
                        "[THETA][DATA_QUALITY][CACHE] Filtering %d all-zero OHLC rows with no quote data: %s",
                        bad_count,
                        [str(d)[:10] for d in bad_dates[:5]],
                    )
                    df = df[~bad_zero_rows]
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
    remote_payload: Optional[Dict[str, object]] = None,
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
    if remote_payload:
        payload["cache_payload"] = {
            str(key): _isoformat_cache_payload_value(value)
            for key, value in remote_payload.items()
        }
    return payload


def _write_cache_sidecar(
    cache_file: Path,
    df_working: pd.DataFrame,
    checksum: Optional[str],
    remote_payload: Optional[Dict[str, object]] = None,
) -> None:
    sidecar = _cache_sidecar_path(cache_file)
    try:
        payload = _build_sidecar_payload(df_working, checksum, remote_payload=remote_payload)
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
        # Memory: avoid deep-copying large frames when updating cache files.
        #
        # `DataFrame.copy()` defaults to deep=True and can double peak RSS for multi-year intraday
        # frames. We only need an owned DataFrame object here; the underlying numeric blocks can
        # be shared safely because we don't mutate existing OHLC columns in-place.
        df_working = ensure_missing_column(df_all.copy(deep=False))
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
        df_cached_normalized = ensure_missing_column(df_cached.copy(deep=False))
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
        # Reuse the normalized view if available; avoid another deep copy for large frames.
        try:
            df_cached_cmp = df_cached_normalized
        except NameError:
            df_cached_cmp = ensure_missing_column(df_cached.copy(deep=False))

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
        _write_cache_sidecar(cache_file, df_working, checksum, remote_payload=remote_payload)
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
            all_zero_ohlc = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
            drop_mask = all_zero_ohlc

            is_option_payload = all(c in df.columns for c in ("expiration", "strike", "right"))

            # If quote columns are present, preserve rows with valid quotes even if OHLC is all zeros
            # for non-option datasets (stocks/indices can legitimately have quote-only prints in some
            # vendor feeds).
            #
            # For option EOD rows, however, ThetaData can return OHLC=0 alongside non-zero NBBO on
            # illiquid/expiry days. Those rows must be treated as placeholders (not real trades) or
            # strategies will mark-to-market at 0.00 and crater.
            if "bid" in df.columns and "ask" in df.columns and not is_option_payload:
                bid = pd.to_numeric(df["bid"], errors="coerce")
                ask = pd.to_numeric(df["ask"], errors="coerce")
                has_quote = ((bid > 0) | (ask > 0)).fillna(False)
                drop_mask = all_zero_ohlc & ~has_quote

            zero_count = int(drop_mask.sum()) if hasattr(drop_mask, "sum") else 0
            if zero_count > 0:
                zero_dates = df.index[drop_mask].tolist()
                if is_option_payload:
                    logger.warning(
                        "[THETA][DATA_QUALITY] Converting %d all-zero OHLC option row(s) to placeholders: %s",
                        zero_count,
                        [str(d)[:10] for d in zero_dates[:5]],
                    )
                    for col in ("open", "high", "low", "close", "volume"):
                        if col in df.columns:
                            df.loc[drop_mask, col] = float("nan")
                    df.loc[drop_mask, "missing"] = True
                else:
                    logger.warning(
                        "[THETA][DATA_QUALITY] Filtering %d all-zero OHLC row(s) with no quotes: %s",
                        zero_count,
                        [str(d)[:10] for d in zero_dates[:5]],
                    )
                    df = df[~drop_mask]

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
            # ThetaData is optional. Provide an actionable message rather than assuming bundling.
            raise FileNotFoundError(
                "ThetaTerminal.jar not available. ThetaData support is optional and not installed by default. "
                f"Searched for a bundled JAR at: {', '.join(str(path) for path in candidate_paths)}. "
                "To enable ThetaData functionality, either:\n"
                " - Install the optional extra: pip install \"lumibot[thetadata]\" (requires Java 11+), or\n"
                f" - Manually download ThetaTerminal.jar from ThetaData and place it at: {jar_file}.\n"
                "After installing, re-run your command."
            )

        logger.info(f"Copying ThetaTerminal.jar from {lumibot_jar} to {jar_file}")
        shutil_copy.copy2(lumibot_jar, jar_file)
        logger.info(f"Successfully copied ThetaTerminal.jar to {jar_file}")

    if not jar_file.exists():
        raise FileNotFoundError(
            "ThetaTerminal.jar not found. ThetaData support is optional and disabled. "
            f"Expected at: {jar_file}. Install with: pip install 'lumibot[thetadata]' or place the JAR manually."
        )

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
                "[DOWNLOADER][QUEUE] Column %s has inconsistent length: expected %d, got %s",
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
        "[DOWNLOADER][QUEUE] Converted columnar format: %d columns x %d rows",
        len(columns),
        num_rows,
    )

    return {"header": {"format": columns}, "response": rows}


def get_request(
    url: str,
    headers: dict,
    querystring: dict,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: Optional[float] = None,
):
    """Make a ThetaData request using either the internal Data Downloader or a local ThetaTerminal.

    Selection rule (strict; no fallback on failure):
    - If ``DATADOWNLOADER_BASE_URL`` is set: use the Data Downloader queue.
      Requires ``DATADOWNLOADER_API_KEY``; LumiBot MUST NOT manage any local ThetaTerminal process.
    - Otherwise: use direct HTTP requests to a locally-managed ThetaTerminal (auto-launches the jar).
    """

    downloader_base_url = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip()
    downloader_mode = bool(downloader_base_url)

    if downloader_mode:
        downloader_api_key = (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip()
        if not downloader_api_key:
            raise RuntimeError(
                "DATADOWNLOADER_BASE_URL is set but DATADOWNLOADER_API_KEY is missing. "
                "Set DATADOWNLOADER_API_KEY or unset DATADOWNLOADER_BASE_URL to use local ThetaTerminal."
            )

        from lumibot.tools.data_downloader_queue_client import queue_request

        logger.debug("[DOWNLOADER][QUEUE] Making request via queue: %s params=%s", url, querystring)

        # -------------------------------------------------------------------------------------
        # BOUNDED WAITS (2025-12-26)
        # -------------------------------------------------------------------------------------
        effective_timeout = timeout
        if effective_timeout is None:
            try:
                list_timeout = float(os.environ.get("THETADATA_QUEUE_LIST_TIMEOUT", "600"))
                history_timeout = float(os.environ.get("THETADATA_QUEUE_HISTORY_TIMEOUT", "1800"))
                default_timeout = float(os.environ.get("THETADATA_QUEUE_DEFAULT_TIMEOUT", "900"))
            except Exception:
                list_timeout = 600.0
                history_timeout = 1800.0
                default_timeout = 900.0

            from urllib.parse import urlparse

            request_path = (urlparse(url).path or "").lower()
            if "/option/list/" in request_path:
                effective_timeout = list_timeout if list_timeout > 0 else None
            elif "/history/" in request_path:
                effective_timeout = history_timeout if history_timeout > 0 else None
            else:
                effective_timeout = default_timeout if default_timeout > 0 else None

        all_responses = []
        page_count = 0
        next_page_url = None
        processed_result = None

        while True:
            request_url = next_page_url if next_page_url else url
            request_params = None if next_page_url else querystring

            try:
                result = queue_request(request_url, request_params, headers, timeout=effective_timeout)
            except TimeoutError as exc:
                raise TimeoutError(
                    f"ThetaData queue request timed out after {effective_timeout}s "
                    f"(url={request_url}, params={request_params or querystring})"
                ) from exc

            if result is None:
                if page_count == 0:
                    logger.debug("[DOWNLOADER][QUEUE] No data returned for request: %s", url)
                    return None
                break

            if isinstance(result, dict):
                # Normalize queue payloads into a consistent v2-style envelope:
                # {"header":{"format":[...]}, "response":[[...], ...]}
                #
                # This must handle both:
                # - v2/v3 columnar payloads (dict-of-lists)
                # - v3 row payloads ({"response": [ {timestamp:..., ...}, ... ]})
                processed_result = _coerce_json_payload(result)
            else:
                processed_result = result

            if isinstance(processed_result, dict) and "response" in processed_result:
                all_responses.append(processed_result["response"])
            else:
                all_responses.append(processed_result)

            page_count += 1

            next_page = None
            if isinstance(processed_result, dict) and "header" in processed_result:
                next_page = processed_result["header"].get("next_page")

            if next_page and next_page != "null" and next_page != "":
                next_page_url = next_page
            else:
                break

        if processed_result is None:
            return None

        if page_count > 1 and isinstance(processed_result, dict):
            processed_result["response"] = []
            for page_response in all_responses:
                if isinstance(page_response, list):
                    processed_result["response"].extend(page_response)
                else:
                    processed_result["response"].append(page_response)
        elif page_count == 1 and all_responses and isinstance(processed_result, dict):
            processed_result["response"] = all_responses[0]

        return processed_result

    # -------------------------------------------------------------------------
    # Local ThetaTerminal mode (direct HTTP)
    # -------------------------------------------------------------------------
    if username is None:
        username = (os.environ.get("THETADATA_USERNAME") or "").strip() or None
    if password is None:
        password = (os.environ.get("THETADATA_PASSWORD") or "").strip() or None
    if username is None or password is None:
        raise ValueError(
            "ThetaData credentials are required to start ThetaTerminal. "
            "Provide them via get_request(..., username=..., password=...) or configure "
            "THETADATA_USERNAME/THETADATA_PASSWORD."
        )

    all_responses = []
    next_page_url = None
    page_count = 0
    consecutive_disconnects = 0
    restart_budget = 3
    querystring = dict(querystring or {})
    if "format" not in querystring:
        is_v2_request = "/v2/" in url
        if not is_v2_request:
            querystring["format"] = "json"
    session_reset_budget = 5
    session_reset_in_progress = False
    awaiting_session_validation = False
    http_retry_limit = HTTP_RETRY_LIMIT
    last_status_code: Optional[int] = None
    last_failure_detail: Optional[str] = None
    queue_full_attempts = 0
    queue_full_wait_total = 0.0
    service_unavailable_attempts = 0
    service_unavailable_wait_total = 0.0

    check_connection(username=username, password=password, wait_for_connection=False)

    while True:
        counter = 0
        request_url = next_page_url if next_page_url else url
        request_params = None if next_page_url else querystring
        json_resp = None

        while True:
            sleep_duration = 0.0
            try:
                CONNECTION_DIAGNOSTICS["network_requests"] += 1

                request_headers = _build_request_headers(headers)

                slot_label = f"local:{request_url.split('?')[0]}"
                with _acquire_theta_slot(slot_label):
                    response = requests.get(
                        request_url,
                        headers=request_headers,
                        params=request_params,
                        timeout=timeout,
                    )
                status_code = response.status_code
                if status_code == 472:
                    symbol = querystring.get("root", querystring.get("symbol", "unknown"))
                    expiration = querystring.get("expiration", querystring.get("exp"))
                    strike = querystring.get("strike")
                    right = querystring.get("right")

                    if expiration and strike:
                        right_str = right.upper() if right else "?"
                        asset_desc = f"{symbol} {expiration} ${strike} {right_str} (option)"
                    elif expiration:
                        asset_desc = f"{symbol} exp:{expiration}"
                    else:
                        asset_desc = f"{symbol} (stock/index)"

                    def format_date(d):
                        if not d or d == "?":
                            return "?"
                        d_str = str(d).replace("-", "")
                        if len(d_str) == 8:
                            return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"
                        return str(d)

                    start_date = format_date(querystring.get("start_date", querystring.get("start", "?")))
                    end_date = format_date(querystring.get("end_date", querystring.get("end", "?")))
                    endpoint = url.split("/")[-1].split("?")[0] if url else "unknown"

                    logger.info(
                        "[THETA][NO_DATA] No data for %s | endpoint: %s | date range: %s to %s | "
                        "ThetaData returned no records for this request. This will be cached to avoid re-fetching.",
                        asset_desc,
                        endpoint,
                        start_date,
                        end_date,
                    )
                    consecutive_disconnects = 0
                    session_reset_in_progress = False
                    awaiting_session_validation = False
                    return None
                elif status_code == 571:
                    check_connection(username=username, password=password, wait_for_connection=True)
                    time.sleep(CONNECTION_RETRY_SLEEP)
                    continue
                elif status_code == 474:
                    consecutive_disconnects += 1
                    if consecutive_disconnects >= 2:
                        if restart_budget <= 0:
                            raise ValueError("Cannot connect to Theta Data!")
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
                        raise ThetaDataSessionInvalidError(
                            "ThetaData session remained invalid after a clean restart."
                        )
                    if not session_reset_in_progress:
                        if session_reset_budget <= 0:
                            raise ValueError("ThetaData session invalid after multiple restarts.")
                        session_reset_budget -= 1
                        session_reset_in_progress = True
                        restart_started = time.monotonic()
                        start_theta_data_client(username=username, password=password)
                        CONNECTION_DIAGNOSTICS["terminal_restarts"] = CONNECTION_DIAGNOSTICS.get("terminal_restarts", 0) + 1
                        while True:
                            try:
                                check_connection(username=username, password=password, wait_for_connection=True)
                                break
                            except ThetaDataConnectionError:
                                time.sleep(CONNECTION_RETRY_SLEEP)
                        wait_elapsed = time.monotonic() - restart_started
                        logger.info(
                            "ThetaTerminal restarted after BadSession (pid=%s, wait=%.1fs).",
                            THETA_DATA_PID,
                            wait_elapsed,
                        )
                    else:
                        try:
                            check_connection(username=username, password=password, wait_for_connection=True)
                        except ThetaDataConnectionError:
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
                elif status_code == 503:
                    payload = {}
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {}

                    is_queue_full = isinstance(payload, dict) and payload.get("error") == "queue_full"
                    active = payload.get("active") if isinstance(payload, dict) else None
                    waiting = payload.get("waiting") if isinstance(payload, dict) else None
                    error_detail = payload.get("detail") if isinstance(payload, dict) else response.text[:200]

                    backoff_delay = min(
                        QUEUE_FULL_BACKOFF_MAX,
                        max(QUEUE_FULL_BACKOFF_BASE, 0.1) * (2 ** min(service_unavailable_attempts, 6)),
                    )
                    backoff_delay += random.uniform(0, max(QUEUE_FULL_BACKOFF_JITTER, 0.0))

                    if is_queue_full:
                        queue_full_attempts += 1
                        queue_full_wait_total += backoff_delay

                    service_unavailable_attempts += 1
                    service_unavailable_wait_total += backoff_delay

                    if service_unavailable_wait_total > SERVICE_UNAVAILABLE_MAX_WAIT:
                        raise ThetaRequestError(
                            f"ThetaData service unavailable after {service_unavailable_wait_total:.0f}s of retries",
                            status_code=503,
                            body=error_detail,
                        )

                    if is_queue_full:
                        logger.warning(
                            "ThetaData 503 queue_full (active=%s waiting=%s). Sleeping %.2fs before retry (attempt=%d).",
                            active,
                            waiting,
                            backoff_delay,
                            service_unavailable_attempts,
                        )
                    else:
                        logger.warning(
                            "ThetaData returned 503 Service Unavailable: %s. Sleeping %.2fs before retry (attempt=%d).",
                            error_detail,
                            backoff_delay,
                            service_unavailable_attempts,
                        )

                    time.sleep(backoff_delay)
                    continue
                elif status_code != 200:
                    check_connection(username=username, password=password, wait_for_connection=True)
                    consecutive_disconnects = 0
                    last_status_code = status_code
                    last_failure_detail = response.text[:200]
                    sleep_duration = min(
                        CONNECTION_RETRY_SLEEP * max(counter + 1, 1),
                        HTTP_RETRY_BACKOFF_MAX,
                    )
                else:
                    try:
                        json_payload = response.json()
                    except ValueError as exc:
                        csv_fallback = None
                        try:
                            import io

                            csv_fallback = pd.read_csv(io.StringIO(response.text))
                        except Exception:
                            csv_fallback = None

                        if csv_fallback is not None and not csv_fallback.empty:
                            json_payload = {
                                "header": {"format": list(csv_fallback.columns)},
                                "response": csv_fallback.values.tolist(),
                            }
                        else:
                            last_status_code = status_code
                            last_failure_detail = str(exc)
                            sleep_duration = min(
                                CONNECTION_RETRY_SLEEP * max(counter + 1, 1),
                                HTTP_RETRY_BACKOFF_MAX,
                            )
                            break

                    json_resp = _coerce_json_payload(json_payload)
                    session_reset_in_progress = False
                    consecutive_disconnects = 0
                    queue_full_attempts = 0
                    queue_full_wait_total = 0.0

                    if "error_type" in json_resp["header"] and json_resp["header"]["error_type"] != "null":
                        if json_resp["header"]["error_type"] == "NO_DATA":
                            return None
                        error_label = json_resp["header"].get("error_type")
                        check_connection(username=username, password=password, wait_for_connection=True)
                        raise ValueError(f"ThetaData returned error_type={error_label}")
                    break

            except ThetaDataConnectionError as exc:
                logger.error("Theta Data connection failed after supervised restarts: %s", exc)
                raise
            except ValueError:
                raise
            except RuntimeError:
                raise
            except Exception as e:
                logger.warning("Exception during request (attempt %s): %s", counter + 1, e)
                check_connection(username=username, password=password, wait_for_connection=True)
                last_status_code = None
                last_failure_detail = str(e)
                if counter == 0:
                    time.sleep(5)

            counter += 1
            if counter >= http_retry_limit:
                raise ThetaRequestError(
                    "Cannot connect to Theta Data!",
                    status_code=last_status_code,
                    body=last_failure_detail,
                )
            if sleep_duration > 0:
                time.sleep(sleep_duration)
        if json_resp is None:
            continue

        page_count += 1
        all_responses.append(json_resp["response"])

        next_page = json_resp["header"].get("next_page")
        if next_page and next_page != "null" and next_page != "":
            next_page_url = next_page
        else:
            break

    if page_count > 1:
        json_resp["response"] = []
        for page_response in all_responses:
            json_resp["response"].extend(page_response)

    return json_resp


def get_historical_eod_data(
    asset: Asset,
    start_dt: datetime,
    end_dt: datetime,
    datastyle: str = "ohlc",
    apply_corporate_actions: bool = True,
    include_nbbo: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
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
    datastyle : str
        The style of data to retrieve (default "ohlc")
    include_nbbo : bool
        When True, keep NBBO quote columns (bid/ask/etc) if present in the EOD response.
    username : Optional[str]
        ThetaData username (backwards compatible; ignored when using the remote data downloader).
    password : Optional[str]
        ThetaData password (backwards compatible; ignored when using the remote data downloader).

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
        # SPX weeklies/0DTE expirations are queried under SPXW on ThetaData.
        base_query["symbol"] = _thetadata_option_root_symbol(asset)
        base_query["expiration"] = asset.expiration.strftime("%Y-%m-%d")
        # REVERSE SPLIT ADJUSTMENT (2025-12-12): Convert split-adjusted strike back to original
        # for ThetaData API query. The strategy sees split-adjusted strikes (e.g., $66), but
        # ThetaData stores data under original strikes (e.g., $1320 for GOOG).
        # FIX: Pass start_dt as sim_datetime so split lookup uses the correct date range.
        # Without this, querying March 2020 options would miss the July 2022 GOOG split.
        query_strike = _get_option_query_strike(asset, sim_datetime=start_dt)
        base_query["strike"] = _format_option_strike(query_strike)
        right = str(getattr(asset, "right", "CALL")).upper()
        base_query["right"] = "call" if right.startswith("C") else "put"

    headers = {"Accept": "application/json"}

    # Convert to date objects for chunking
    start_day = datetime.strptime(start_date, "%Y%m%d").date()
    end_day = datetime.strptime(end_date, "%Y%m%d").date()
    # Provider constraint: Theta's EOD history endpoints enforce a hard 365-day limit per request.
    # Keep windows <= 365 days (inclusive) and use recursive splitting only for transient failures.
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

        try:
            return get_request(
                url=url,
                headers=headers,
                querystring=querystring,
            )
        except ThetaRequestError:
            raise
        except Exception as exc:
            # The downloader queue client historically raises a generic Exception on permanent
            # failures (instead of a typed HTTP error). Translate "window too large" errors into
            # ThetaRequestError so our recursive splitter can reduce the range and retry.
            msg = str(exc)
            if "Too many days between start and end date" in msg or "max 365 days" in msg:
                raise ThetaRequestError(msg, status_code=500, body=msg) from exc
            raise

    def _collect_chunk_payloads(
        chunk_start: date,
        chunk_end: date,
        *,
        depth: int = 0,
        max_depth: int = 16,
    ) -> List[Optional[Dict[str, Any]]]:
        try:
            response = _execute_chunk_request(chunk_start, chunk_end)
            return [response]
        except ThetaRequestError as exc:
            span_days = (chunk_end - chunk_start).days + 1
            if span_days <= 1 or depth >= max_depth:
                raise
            logger.warning(
                "[THETA][WARN][EOD][CHUNK] asset=%s start=%s end=%s status=%s retrying with split windows (depth=%d)",
                asset,
                chunk_start,
                chunk_end,
                exc.status_code,
                depth,
            )
            midpoint = chunk_start + timedelta(days=(span_days // 2) - 1)
            left_end = min(midpoint, chunk_end)
            right_start = min(midpoint + timedelta(days=1), chunk_end)

            split_payloads: List[Optional[Dict[str, Any]]] = []
            if chunk_start <= left_end:
                split_payloads.extend(
                    _collect_chunk_payloads(
                        chunk_start,
                        left_end,
                        depth=depth + 1,
                        max_depth=max_depth,
                    )
                )
            if right_start <= chunk_end:
                split_payloads.extend(
                    _collect_chunk_payloads(
                        right_start,
                        chunk_end,
                        depth=depth + 1,
                        max_depth=max_depth,
                    )
                )
            return split_payloads

    aggregated_rows: List[List[Any]] = []
    header_format: Optional[List[str]] = None
    windows = list(_chunk_windows())

    # Track progress for this single-asset EOD download operation.
    # Each outer window is one "piece" (even if a window needs to be split due to server errors).
    set_download_status(asset, "USD", datastyle, "day", 0, max(1, len(windows)))

    # DEBUG-LOG: EOD data request (overall)
    logger.debug(
        "[THETA][DEBUG][EOD][REQUEST] asset=%s start=%s end=%s datastyle=%s chunks=%d",
        asset,
        start_date,
        end_date,
        datastyle,
        len(windows)
    )

    try:
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

            # Mark one outer window complete.
            advance_download_status_progress(asset=asset, data_type=datastyle, timespan="day", step=1)
    finally:
        finalize_download_status()

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
                    parsed = _dateutil_parse(str(value))
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

    # Theta EOD endpoints may emit duplicate rows for the same trading day (often exact duplicates).
    # Cache consumers expect a unique datetime index; keep the last row for each date.
    if df.index.has_duplicates:
        df = df[~df.index.duplicated(keep="last")]

    # ThetaData EOD sometimes returns placeholder rows with all-zero OHLC values for a valid trading day.
    # If we treat those as real prices, portfolio valuation can collapse to ~0 for a single bar and then recover
    # on the next bar ("portfolio cliff"). Treat the all-zero OHLC bar as missing so downstream repair/ffill
    # can carry forward the last known close instead of valuing at 0.
    #
    # NOTE: We apply this only to stock/index EOD. For option EOD, OHLC may legitimately be 0 when only NBBO
    # fields are populated, and we don't want to mask that.
    if asset_type in {"stock", "index"} and {"open", "high", "low", "close"}.issubset(df.columns):
        df = ensure_missing_column(df)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        zero_ohlc_mask = df[["open", "high", "low", "close"]].eq(0).all(axis=1)
        if zero_ohlc_mask.any():
            zero_dates = sorted({ts.date().isoformat() for ts in df.index[zero_ohlc_mask]})
            preview = ", ".join(zero_dates[:10]) + (" ..." if len(zero_dates) > 10 else "")
            logger.warning(
                "[THETA][WARN][EOD][ZERO_OHLC] asset=%s rows=%d dates=%s",
                asset,
                int(zero_ohlc_mask.sum()),
                preview,
            )
            df.loc[zero_ohlc_mask, ["open", "high", "low", "close"]] = float("nan")
            if "volume" in df.columns:
                df.loc[zero_ohlc_mask, "volume"] = 0
            df.loc[zero_ohlc_mask, "missing"] = True

    # Drop the ms_of_day, ms_of_day2, and date columns (not needed for daily bars)
    df = df.drop(columns=["ms_of_day", "ms_of_day2", "date"], errors='ignore')

    # Drop bid/ask columns unless explicitly requested (EOD includes NBBO).
    if not include_nbbo:
        df = df.drop(
            columns=[
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
            ],
            errors="ignore",
        )

    if apply_corporate_actions:
        df = _apply_corporate_actions_to_frame(asset, df, start_day, end_day, username, password)

    return df


def get_historical_data(
    asset: Asset,
    start_dt: datetime,
    end_dt: datetime,
    ivl: int,
    datastyle: str = "ohlc",
    include_after_hours: bool = True,
    session_time_override: Optional[Tuple[str, str]] = None,
    download_timespan: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
):
    """
    Fetch intraday history from ThetaData using the v3 REST endpoints.

    Parameters
    ----------
    session_time_override : Optional[Tuple[str, str]]
        When provided, overrides the computed start/end session times for each trading day
        (HH:MM:SS strings). Useful for requesting specific minute windows such as the 09:30 open.
    """

    def _build_history_frame(json_resp: Any) -> Optional[pd.DataFrame]:
        """Normalize ThetaData history payloads into a DataFrame.

        Theta's v3 REST surface is not fully stable across terminal versions:
        - some responses are v2-style: {"header": {"format": [...]}, "response": [[...], ...]}
        - others are row-style: {"response": [{"timestamp": "...", ...}, ...]} (no "header")
        - some option history endpoints return a nested payload:
            {"response": [{"contract": {...}, "data": [{...}, {...}, ...]}]}

        LumiBot's downstream merge path expects a DataFrame that can be indexed by a "datetime"
        series during `_finalize_history_dataframe()`, so we must accept both shapes here.
        """
        if not json_resp:
            return None

        if isinstance(json_resp, dict):
            raw = json_resp.get("response")
            header = json_resp.get("header") if isinstance(json_resp.get("header"), dict) else None
            fmt = header.get("format") if header else None
        else:
            raw = json_resp
            fmt = None

        if raw is None:
            return None

        df: pd.DataFrame

        # v3 row-style: list[dict]
        if isinstance(raw, list) and raw and isinstance(raw[0], dict):
            df = pd.DataFrame(raw)
        # v2 columnar: list[list] with header.format
        elif isinstance(raw, list) and raw and isinstance(raw[0], (list, tuple)) and isinstance(fmt, list):
            df = pd.DataFrame(raw, columns=fmt)
        else:
            # Fallback: let pandas infer.
            df = pd.DataFrame(raw)

        if df is None or df.empty:
            return df

        # Some option endpoints return a nested response:
        #   response: [{"contract": {...}, "data": [{timestamp:..., bid:..., ask:...}, ...]}]
        # Our downstream expects one row per timestamp.
        if "timestamp" not in df.columns and "data" in df.columns:
            try:
                nested = df["data"].tolist()
                if len(nested) == 1 and isinstance(nested[0], list) and nested[0] and isinstance(nested[0][0], dict):
                    return pd.DataFrame(nested[0])
            except Exception:
                pass

        return df

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
    query_symbol = asset.symbol
    if asset_type == "index":
        query_symbol = _thetadata_index_root_symbol(asset)
        if query_symbol != str(asset.symbol).strip().upper():
            logger.debug(
                "[THETA][SYMBOL][INDEX_ROOT] Mapping index symbol %s -> %s for history endpoints",
                asset.symbol,
                query_symbol,
            )

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

    def _advance_download_progress() -> None:
        if download_timespan is None:
            return
        try:
            advance_download_status_progress(
                asset=asset,
                data_type=datastyle,
                timespan=download_timespan,
                step=1,
            )
        except Exception:
            return

    def build_option_params() -> Dict[str, str]:
        if not asset.expiration:
            raise ValueError(f"Expiration date missing for option asset {asset}")
        if asset.strike is None:
            raise ValueError(f"Strike missing for option asset {asset}")
        right = str(getattr(asset, "right", "CALL")).upper()
        # FIX (2025-12-12): Convert split-adjusted strike back to original for ThetaData API query
        # Uses start_dt from enclosing scope as the simulation datetime
        query_strike = _get_option_query_strike(asset, sim_datetime=start_dt)
        query_expiration = _thetadata_option_query_expiration(
            asset.expiration,
            symbol=_thetadata_option_root_symbol(asset),
        )
        return {
            "symbol": _thetadata_option_root_symbol(asset),
            "expiration": query_expiration.strftime("%Y-%m-%d"),
            "strike": _format_option_strike(query_strike),
            "right": "call" if right.startswith("C") else "put",
        }

    if asset_type == "index" and datastyle == "ohlc":
        querystring = {
            "symbol": query_symbol,
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
        )
        _advance_download_progress()
        if not json_resp:
            return None
        df = _build_history_frame(json_resp)
        if df is None or df.empty:
            return None
        return _finalize_history_dataframe(df, datastyle, asset)

    frames: List[pd.DataFrame] = []
    option_params = build_option_params() if asset_type == "option" else None

    # DEBUG: Log option params to verify strike conversion (gated at DEBUG for perf).
    if option_params:
        logger.debug(
            "[THETA][OPTION_PARAMS] asset=%s option_params=%s",
            asset,
            option_params,
        )

    for trading_day in trading_days:
        querystring: Dict[str, Any] = {
            "symbol": query_symbol,
            "date": trading_day.strftime("%Y-%m-%d"),
            "interval": interval_label,
            "format": "json",
        }
        if option_params:
            querystring.update(option_params)
        if asset_type == "index":
            # Index quote/price endpoint expects 'date' per request similar to options/stocks
            querystring.pop("symbol", None)
            querystring["symbol"] = query_symbol

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
        )
        _advance_download_progress()
        if not json_resp:
            continue

        df = _build_history_frame(json_resp)
        if df is None or df.empty:
            continue
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


# ---------------------------------------------------------------------------
# ThetaData option expiration mapping (provider-specific)
# ---------------------------------------------------------------------------
#
# ThetaData does not use a single expiration-date convention for all underlyings:
# - some expiration lists use OCC Saturday dates for standard monthlies,
# - others use Friday for the same calendar month.
#
# LumiBot strategies typically reason in terms of "last tradable session" (Friday). To query
# ThetaData history endpoints reliably, we maintain a per-(symbol, tradable_expiry) mapping to the
# provider's expected expiration representation.
#
# We learn this mapping from option chain payloads (which contain the provider expiry keys).
_THETADATA_EXPIRY_MAP: Dict[Tuple[str, date], date] = {}
_THETADATA_EXPIRY_MAP_LOCK = threading.Lock()


def _normalize_symbol_key(symbol: object) -> str:
    return str(symbol or "").strip().upper()


def _thetadata_option_query_expiration_heuristic(expiration: date) -> date:
    """Legacy fallback: map 3rd-Friday trade dates to OCC Saturday (works for some symbols)."""
    if isinstance(expiration, datetime):
        expiration = expiration.date()

    if expiration.weekday() == 4 and 15 <= expiration.day <= 21:
        return expiration + timedelta(days=1)

    return expiration


def _register_thetadata_expiry_map_from_chain(symbol: str, chains_dict: dict) -> None:
    """Populate provider-expiry mapping from a chain payload (best-effort, in-process only)."""
    symbol_key = _normalize_symbol_key(symbol)
    if not symbol_key or not isinstance(chains_dict, dict):
        return

    chains_section = chains_dict.get("Chains")
    if not isinstance(chains_section, dict):
        return

    expiry_strings: set[str] = set()
    for side in ("CALL", "PUT"):
        side_map = chains_section.get(side)
        if isinstance(side_map, dict):
            expiry_strings.update([str(k).strip() for k in side_map.keys() if k is not None])

    def _parse_expiry(expiry_str: str) -> Optional[date]:
        cleaned = str(expiry_str or "").strip()
        if not cleaned:
            return None
        try:
            return date.fromisoformat(cleaned)
        except Exception:
            digits = cleaned.replace("-", "")
            if len(digits) == 8 and digits.isdigit():
                try:
                    return datetime.strptime(digits, "%Y%m%d").date()
                except Exception:
                    return None
        return None

    updates: Dict[Tuple[str, date], date] = {}
    for expiry_str in expiry_strings:
        provider_expiry = _parse_expiry(expiry_str)
        if provider_expiry is None:
            continue

        tradable_expiry = provider_expiry
        if provider_expiry.weekday() == 5:
            tradable_expiry = provider_expiry - timedelta(days=1)
        elif provider_expiry.weekday() == 6:
            tradable_expiry = provider_expiry - timedelta(days=2)

        key = (symbol_key, tradable_expiry)
        existing = updates.get(key)
        if existing is None:
            updates[key] = provider_expiry
            continue

        # Prefer the provider expiry that exactly matches the tradable expiry when available
        # (weekly-style providers). This prevents persistent 472/no-data responses for underlyings
        # whose provider uses Friday expirations even for monthlies (e.g., CVNA).
        if provider_expiry == tradable_expiry and existing != tradable_expiry:
            updates[key] = provider_expiry
            continue
        if existing == tradable_expiry and provider_expiry != tradable_expiry:
            continue

    if not updates:
        return

    with _THETADATA_EXPIRY_MAP_LOCK:
        for key, provider_expiry in updates.items():
            _, tradable_expiry = key
            existing = _THETADATA_EXPIRY_MAP.get(key)
            if existing is None:
                _THETADATA_EXPIRY_MAP[key] = provider_expiry
                continue

            # Prefer exact-Friday provider expirations when both Friday and Saturday are present.
            if existing == tradable_expiry:
                continue
            if provider_expiry == tradable_expiry:
                _THETADATA_EXPIRY_MAP[key] = provider_expiry


def _thetadata_option_query_expiration(expiration: date, *, symbol: Optional[str] = None) -> date:
    """Map LumiBot's tradable option expiry to the value expected by ThetaData endpoints."""
    if isinstance(expiration, datetime):
        expiration = expiration.date()

    symbol_key = _normalize_symbol_key(symbol)
    if symbol_key:
        with _THETADATA_EXPIRY_MAP_LOCK:
            mapped = _THETADATA_EXPIRY_MAP.get((symbol_key, expiration))
        if isinstance(mapped, date):
            return mapped

    return _thetadata_option_query_expiration_heuristic(expiration)


def _isoformat_cache_payload_value(value: object) -> object:
    """Return a JSON-friendly representation for cache sidecar metadata."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _option_provider_expiration_for_asset(asset: Asset) -> Optional[date]:
    """Return the ThetaData provider expiration for an option asset, if known."""
    if getattr(asset, "asset_type", None) != "option" or getattr(asset, "expiration", None) is None:
        return None
    try:
        return _thetadata_option_query_expiration(
            asset.expiration,
            symbol=_thetadata_option_root_symbol(asset),
        )
    except Exception:
        return None


def _placeholder_option_cache_needs_provider_expiry_refresh(
    asset: Asset,
    sidecar_data: Optional[Dict[str, Any]],
    remote_payload: Optional[Dict[str, object]],
    *,
    allow_legacy_without_payload: bool = False,
) -> bool:
    """Detect placeholder-only option caches written with a stale provider expiry convention."""
    if getattr(asset, "asset_type", None) != "option" or getattr(asset, "expiration", None) is None:
        return False

    current_provider_expiration = None
    if remote_payload:
        current_provider_expiration = remote_payload.get("provider_expiration")
    if isinstance(current_provider_expiration, date):
        current_provider_expiration = current_provider_expiration.isoformat()
    elif current_provider_expiration is not None:
        current_provider_expiration = str(current_provider_expiration)

    if not current_provider_expiration:
        provider_expiration = _option_provider_expiration_for_asset(asset)
        if provider_expiration is None:
            return False
        current_provider_expiration = provider_expiration.isoformat()

    stored_provider_expiration = None
    if isinstance(sidecar_data, dict):
        cache_payload = sidecar_data.get("cache_payload")
        if isinstance(cache_payload, dict):
            stored_provider_expiration = cache_payload.get("provider_expiration")
    if isinstance(stored_provider_expiration, date):
        stored_provider_expiration = stored_provider_expiration.isoformat()
    elif stored_provider_expiration is not None:
        stored_provider_expiration = str(stored_provider_expiration)

    if stored_provider_expiration:
        return stored_provider_expiration != current_provider_expiration

    # Legacy placeholder-only sidecars did not record which provider expiry was queried. If a
    # chain-derived mapping now overrides the old Saturday heuristic, refetch once under the
    # learned provider convention and write the provider expiry into the sidecar. Keep this
    # opt-in because option day/EOD placeholder caches are deliberate stable negative caches in
    # acceptance runs; refreshing all legacy EOD placeholders breaks warm-cache determinism.
    if not allow_legacy_without_payload:
        return False

    heuristic_expiration = _thetadata_option_query_expiration_heuristic(asset.expiration)
    return heuristic_expiration.isoformat() != current_provider_expiration


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

    # ThetaData has historically returned strikes in thousandths-of-a-dollar for some payloads
    # (e.g. 4500000 representing 4500.0). However, legitimate index strikes (e.g. NDX ~ 18000)
    # can exceed 10,000 in *dollars*. Only apply the thousandths normalization when the value is
    # clearly too large to be a real strike in dollars.
    if strike >= 100000:
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
    asset: Asset,
    as_of_date: date,
    max_expirations: int = 250,
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
        asset: The underlying asset (e.g., Asset("AAPL"))
        as_of_date: The historical date to build the chain for
        max_expirations: Maximum number of expirations to include (default: 250, increased to support LEAPS 2+ years out)
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

    class _OptionChainStatusAsset:
        def __init__(
            self,
            underlying_symbol: str,
            *,
            expiration: Optional[str] = None,
            strike_symbol: Optional[str] = None,
            as_of: Optional[date] = None,
        ):
            self._underlying_symbol = str(underlying_symbol or "")
            self._expiration = expiration
            self._strike_symbol = strike_symbol
            self._as_of = as_of

        def to_minimal_dict(self) -> Dict[str, Any]:
            payload: Dict[str, Any] = {"type": "option_chain", "symbol": self._underlying_symbol}
            if self._expiration is not None:
                payload["exp"] = self._expiration
            if self._strike_symbol is not None and self._strike_symbol != self._underlying_symbol:
                payload["strike_symbol"] = self._strike_symbol
            if self._as_of is not None:
                payload["as_of"] = self._as_of.isoformat()
            return payload

    def _fetch_expiration_values(symbol: str) -> List[str]:
        expirations_resp = get_request(
            url=f"{_current_base_url()}{OPTION_LIST_ENDPOINTS['expirations']}",
            headers=headers,
            querystring={"symbol": symbol, "format": "json"},
        )
        if not expirations_resp or not expirations_resp.get("response"):
            return []

        exp_df = pd.DataFrame(expirations_resp["response"], columns=expirations_resp["header"]["format"])
        if exp_df.empty:
            return []

        expiration_col = _detect_column(exp_df, ("expiration", "exp", "date"))
        if not expiration_col:
            logger.warning("ThetaData expiration payload missing expected columns for %s.", symbol)
            return []

        values: List[str] = []
        for raw_value in exp_df[expiration_col].tolist():
            normalized = _normalize_expiration_value(raw_value)
            if normalized:
                values.append(normalized)
        return sorted({value for value in values})

    primary_symbol = asset.symbol
    symbols_to_merge = [primary_symbol]
    spxw_symbol = None
    if str(primary_symbol).upper() == "SPX":
        # ThetaData stores SPX weeklies/0DTE expirations under SPXW.
        spxw_symbol = "SPXW"
        symbols_to_merge.append(spxw_symbol)

    expirations_by_symbol: Dict[str, List[str]] = {
        symbol: _fetch_expiration_values(symbol) for symbol in symbols_to_merge
    }

    expiration_values = sorted({exp for exps in expirations_by_symbol.values() for exp in exps})
    if not expiration_values:
        logger.warning(
            "ThetaData returned no expirations for %s; cannot build chain for %s.",
            asset.symbol,
            as_of_date,
        )
        return None

    spx_expirations = set(expirations_by_symbol.get(primary_symbol, []))
    spxw_expirations = set(expirations_by_symbol.get(spxw_symbol, [])) if spxw_symbol else set()

    as_of_int = int(as_of_date.strftime("%Y%m%d"))

    constraints = dict(chain_constraints or {})
    min_hint_date = constraints.get("min_expiration_date")
    max_hint_date = constraints.get("max_expiration_date")

    # PERF/SAFETY: Theta's expirations payload is not truly point-in-time and can include expirations
    # years into the future for historical backtest dates. A naive "build the entire chain" approach
    # forces one strike-list request per expiration, which explodes request volume on cold S3
    # namespaces and makes long-window backtests unusably slow.
    #
    # Default to a bounded max-expiration window unless the caller explicitly provides one.
    if max_hint_date is None:
        # Keep these as stable defaults (no env-var toggles): changing chain horizons is a
        # high-impact behavior change that should be explicit in code + tests.
        default_days_out = 730
        index_days_out = 180

        symbol_upper = str(getattr(asset, "symbol", "") or "").upper()
        asset_type = str(getattr(asset, "asset_type", "") or "").lower()
        is_index_like = asset_type == "index" or symbol_upper in {
            "SPX",
            "SPXW",
            "NDX",
            "NDXP",
            "RUT",
            "RUTW",
            "VIX",
            "VIXW",
            "XSP",
            "DJX",
            "OEX",
            "XEO",
        }
        days_out = index_days_out if is_index_like else default_days_out

        if isinstance(days_out, int) and days_out > 0:
            base_date = min_hint_date if isinstance(min_hint_date, date) else as_of_date
            max_hint_date = base_date + timedelta(days=days_out)
            constraints["max_expiration_date"] = max_hint_date

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

    # Start from as_of_date (only include future expirations), but allow callers to hint
    # that they only care about expirations at/after a later date (performance).
    effective_start_int = max(as_of_int, min_hint_int) if min_hint_int else as_of_int
    effective_start_date = (
        max(as_of_date, min_hint_date) if isinstance(min_hint_date, date) else as_of_date
    )

    logger.info(
        "[ThetaData] Building chain for %s @ %s (min_hint=%s, max_hint=%s, expirations=%d)",
        asset.symbol,
        effective_start_date,
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

    # Use the queue client's ability to keep multiple requests in-flight to dramatically speed up
    # chain building for underlyings with dense expiration schedules (e.g., SPXW daily expirations).
    from lumibot.tools.data_downloader_queue_client import get_queue_client

    queue_client = get_queue_client()
    strikes_path = OPTION_LIST_ENDPOINTS["strikes"].lstrip("/")
    strikes_timeout = float(os.environ.get("THETADATA_CHAIN_STRIKES_TIMEOUT", "300"))
    configured_batch_size = int(os.environ.get("THETADATA_CHAIN_STRIKES_BATCH_SIZE", "0"))
    batch_size = configured_batch_size if configured_batch_size > 0 else getattr(queue_client, "max_concurrent", 8)
    batch_size = max(1, batch_size)

    def _normalize_queue_payload(result: Optional[Any]) -> Optional[Dict[str, Any]]:
        if result is None:
            return None
        if isinstance(result, dict):
            # Normalize queue payloads into a consistent v2-style envelope:
            # {"header":{"format":[...]}, "response":[[...], ...]}
            #
            # Queue responses can be:
            # - v2/v3 columnar payloads (dict-of-lists)
            # - v3 row payloads ({"response": [ {timestamp:..., ...}, ... ]})
            return _coerce_json_payload(result)
        return {"header": {"format": []}, "response": result}

    expiration_candidates: List[Tuple[str, str]] = []
    for expiration_iso in expiration_values:
        expiration_int = int(expiration_iso.replace("-", ""))

        # Skip expirations that are in the past relative to our backtest date (or min-hint).
        if expiration_int < effective_start_int:
            continue

        # If a max_hint_date was provided (e.g., strategy only wants options within 2 years),
        # stop scanning once we exceed it.
        if max_hint_int and expiration_int > max_hint_int:
            logger.debug(
                "[ThetaData] Reached max hint %s for %s; stopping chain build.",
                max_hint_date,
                asset.symbol,
            )
            break

        strike_symbol = primary_symbol
        if spxw_symbol and str(primary_symbol).upper() == "SPX":
            try:
                exp_date = date.fromisoformat(expiration_iso)
            except ValueError:
                exp_date = None

            if exp_date and _is_third_friday(exp_date) and expiration_iso in spx_expirations:
                strike_symbol = "SPX"
            elif expiration_iso in spxw_expirations:
                strike_symbol = "SPXW"

        expiration_candidates.append((expiration_iso, strike_symbol))

    # STRIKE PREFETCH OPTIMIZATION:
    # Many strategies only need the expiration list from a chain (e.g. to choose a far-dated LEAPS
    # expiry) and will only query strikes for 1-2 expirations. Fetching strikes for every
    # expiration can generate hundreds of `option/list/strikes` requests and dominate runtime.
    #
    # When the caller didn't provide explicit chain constraints and the candidate list is large,
    # pre-populate the chain with *all* expiration keys (empty strike lists), but only prefetch
    # strikes for a small, representative subset (head + tail). This keeps common strategies fast
    # while preserving the full expiration list.
    user_constraints = chain_constraints or {}
    user_hint_present = any(
        user_constraints.get(key) is not None for key in ("min_expiration_date", "max_expiration_date")
    )

    expiration_candidates_for_strikes = expiration_candidates
    if not user_hint_present and len(expiration_candidates) > 50:
        for expiration_iso, _strike_symbol in expiration_candidates:
            chains["CALL"].setdefault(expiration_iso, [])
            chains["PUT"].setdefault(expiration_iso, [])

        head = expiration_candidates[:14]
        tail = expiration_candidates[-14:] if len(expiration_candidates) > 14 else []
        seen: set[Tuple[str, str]] = set()
        pruned: List[Tuple[str, str]] = []
        for item in head + tail:
            if item in seen:
                continue
            seen.add(item)
            pruned.append(item)
        expiration_candidates_for_strikes = pruned

    total_strike_units = max(1, min(len(expiration_candidates_for_strikes), max_expirations))
    completed_strike_units = 0
    try:
        # This is a single download operation: building an option chain. Surface progress in
        # terms of completed strike-list requests so the UI doesn't show download_status="{}"
        # during long strike scans (notably SPX/SPXW).
        set_download_status(
            _OptionChainStatusAsset(asset.symbol, as_of=as_of_date),
            quote_asset=None,
            data_type="option_chain",
            timespan="meta",
            current=0,
            total=total_strike_units,
            timeout_s=strikes_timeout,
        )

        idx = 0
        while idx < len(expiration_candidates_for_strikes):
            if expirations_added >= max_expirations:
                logger.debug("[ThetaData] Chain build hit max_expirations limit (%d)", max_expirations)
                break
            if consecutive_strike_misses >= max_consecutive_misses:
                logger.debug(
                    "[ThetaData] %d consecutive expirations with no strikes; stopping scan.",
                    max_consecutive_misses,
                )
                break

            remaining_needed = max_expirations - expirations_added
            batch = expiration_candidates_for_strikes[idx: idx + min(batch_size, remaining_needed)]

            requests: List[Tuple[str, str, str]] = []  # (request_id, expiration_iso, strike_symbol)
            for expiration_iso, strike_symbol in batch:
                set_download_status(
                    _OptionChainStatusAsset(
                        asset.symbol,
                        expiration=expiration_iso,
                        strike_symbol=strike_symbol,
                        as_of=as_of_date,
                    ),
                    quote_asset=None,
                    data_type="option_chain",
                    timespan="meta",
                    current=completed_strike_units,
                    total=total_strike_units,
                    timeout_s=strikes_timeout,
                )
                request_id, _status, _was_pending = queue_client.check_or_submit(
                    method="GET",
                    path=strikes_path,
                    query_params={
                        "symbol": strike_symbol,
                        "expiration": expiration_iso,
                        "format": "json",
                    },
                    headers=headers,
                )
                requests.append((request_id, expiration_iso, strike_symbol))

            for request_id, expiration_iso, strike_symbol in requests:
                strike_resp = None
                try:
                    # Keep context aligned to the request we're currently waiting on.
                    set_download_status(
                        _OptionChainStatusAsset(
                            asset.symbol,
                            expiration=expiration_iso,
                            strike_symbol=strike_symbol,
                            as_of=as_of_date,
                        ),
                        quote_asset=None,
                        data_type="option_chain",
                        timespan="meta",
                        current=completed_strike_units,
                        total=total_strike_units,
                        timeout_s=strikes_timeout,
                    )
                    result, status_code = queue_client.wait_for_result(
                        request_id=request_id,
                        timeout=strikes_timeout,
                    )
                    if status_code == 472:
                        strike_resp = None
                    else:
                        strike_resp = _normalize_queue_payload(result)
                except TimeoutError:
                    logger.warning(
                        "[ThetaData] Timeout waiting for strike list (symbol=%s exp=%s request_id=%s timeout=%.1fs)",
                        strike_symbol,
                        expiration_iso,
                        request_id,
                        strikes_timeout,
                    )
                    strike_resp = None
                except Exception:
                    logger.debug(
                        "[ThetaData] Error fetching strike list (symbol=%s exp=%s request_id=%s)",
                        strike_symbol,
                        expiration_iso,
                        request_id,
                        exc_info=True,
                    )
                    strike_resp = None
                finally:
                    completed_strike_units = min(completed_strike_units + 1, total_strike_units)
                    advance_download_status_progress(data_type="option_chain", timespan="meta", step=1)

                # Handle strike fetch failures - increment miss counter and potentially stop scanning.
                if not strike_resp or not strike_resp.get("response"):
                    logger.debug("No strikes for %s exp %s; skipping.", strike_symbol, expiration_iso)
                    consecutive_strike_misses += 1
                    if consecutive_strike_misses >= max_consecutive_misses:
                        break
                    continue

                strike_df = pd.DataFrame(strike_resp["response"], columns=strike_resp["header"]["format"])
                if strike_df.empty:
                    consecutive_strike_misses += 1
                    if consecutive_strike_misses >= max_consecutive_misses:
                        break
                    continue

                strike_col = _detect_column(strike_df, ("strike",))
                if not strike_col:
                    consecutive_strike_misses += 1
                    if consecutive_strike_misses >= max_consecutive_misses:
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
                    consecutive_strike_misses += 1
                    if consecutive_strike_misses >= max_consecutive_misses:
                        break
                    continue

                chains["CALL"][expiration_iso] = strike_values
                chains["PUT"][expiration_iso] = list(strike_values)
                expirations_added += 1
                consecutive_strike_misses = 0

            idx += len(batch)
    finally:
        finalize_download_status()

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

    # When strike prefetch is disabled (the default), the chain contains only expiration keys
    # with empty strike lists. Strike lists are loaded lazily and normalized (including split
    # adjustments) by the Broker/Chains layer. Skip the expensive split-walk here.
    if expirations_added == 0:
        return {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": chains,
            "UnderlyingSymbol": asset.symbol,  # Add this for easier extraction later
            "_chain_cache_version": THETADATA_CHAIN_CACHE_VERSION,
        }

    # SPLIT ADJUSTMENT FOR OPTIONS STRIKES (2025-12-11)
    # When stock prices are split-adjusted, options strikes must also be adjusted to match.
    # For example, GOOG had a 20:1 split in July 2022. When backtesting March 2020:
    # - Stock price is split-adjusted: ~$55 (original ~$1100)
    # - ThetaData strikes are NOT adjusted: $1320
    # - Strategy calculates target strike: $55 * 1.20 = $66
    # - Without this fix, the strategy tries to buy $1320 strike (wrong!)
    # - With this fix, strikes are adjusted: $1320 / 20 = $66 (correct!)
    #
    # We fetch splits from as_of_date to the corporate-action horizon and apply the cumulative ratio.
    today = _corporate_action_horizon_date()

    # Fetch splits that occurred AFTER the backtest date
    splits = _get_theta_splits(asset, as_of_date, today)

    if splits is not None and not splits.empty:
        # Calculate cumulative split factor for splits after as_of_date
        # Filter to splits that actually occurred after our backtest date
        if "event_date" in splits.columns:
            as_of_datetime = pd.Timestamp(as_of_date)
            if splits["event_date"].dtype != "datetime64[ns]":
                splits["event_date"] = pd.to_datetime(splits["event_date"])
            # Make as_of_datetime timezone-aware if event_date is timezone-aware
            if hasattr(splits["event_date"].dt, "tz") and splits["event_date"].dt.tz is not None:
                if as_of_datetime.tzinfo is None:
                    as_of_datetime = as_of_datetime.tz_localize(splits["event_date"].dt.tz)
            future_splits = splits[splits["event_date"] > as_of_datetime]

            if not future_splits.empty:
                cumulative_split_factor = future_splits["ratio"].prod()

                # FIX (2025-12-12): Handle BOTH forward AND reverse splits
                # Forward splits (e.g., 20:1): factor > 1.0, strikes are divided
                # Reverse splits (e.g., 1:8): factor < 1.0, strikes are divided (increases them)
                if cumulative_split_factor != 1.0:
                    # Theta's strike lists can contain a mix of pre- and post-split scales,
                    # especially for expirations that span a split. We normalize per-strike
                    # by choosing the representation (raw vs raw/factor) that is closer in
                    # log-space to the underlying's split-adjusted price on as_of_date.
                    reference_price = None
                    try:
                        ref_asset = Asset(asset.symbol, asset_type="stock")
                        ref_dt = datetime(as_of_date.year, as_of_date.month, as_of_date.day)
                        ref_df = get_price_data(
                            asset=ref_asset,
                            start=ref_dt,
                            end=ref_dt,
                            timespan="day",
                            datastyle="ohlc",
                            include_after_hours=False,
                        )
                        if ref_df is not None and not ref_df.empty:
                            for col in ("close", "Close", "adj_close", "Adj Close"):
                                if col in ref_df.columns:
                                    reference_price = float(ref_df[col].iloc[-1])
                                    break
                    except Exception as exc:
                        logger.debug(
                            "[ThetaData] Unable to fetch reference price for strike normalization (%s @ %s): %s",
                            asset.symbol,
                            as_of_date,
                            exc,
                        )

                    logger.info(
                        "[ThetaData] Adjusting option strikes for %s: cumulative split factor %.4f "
                        "(from %d splits after %s)",
                        asset.symbol,
                        cumulative_split_factor,
                        len(future_splits),
                        as_of_date,
                    )

                    import math

                    def _select_normalized_strike(raw_strike: float) -> float:
                        adjusted = raw_strike / cumulative_split_factor
                        if reference_price and reference_price > 0:
                            try:
                                raw_score = abs(math.log(raw_strike / reference_price))
                                adjusted_score = abs(math.log(adjusted / reference_price))
                            except (ValueError, ZeroDivisionError):
                                return adjusted
                            return adjusted if adjusted_score < raw_score else raw_strike
                        return adjusted

                    # Normalize strikes per-expiration to match split-adjusted underlying prices.
                    for option_type in ["CALL", "PUT"]:
                        for expiry_date, strikes in chains[option_type].items():
                            normalized = {
                                round(_select_normalized_strike(float(strike)), 5)
                                for strike in strikes
                                if strike is not None
                            }
                            chains[option_type][expiry_date] = sorted(normalized)

                    logger.debug(
                        "[ThetaData] Strike adjustment example for %s: %.2f -> %.2f",
                        asset.symbol,
                        1320.0,  # Example pre-split strike
                        1320.0 / cumulative_split_factor,
                    )

    return {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": chains,
        "UnderlyingSymbol": asset.symbol,  # Add this for easier extraction later
        "_chain_cache_version": THETADATA_CHAIN_CACHE_VERSION,
    }


def get_expirations(ticker: str, after_date: date):
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
    json_resp = get_request(url=url, headers=headers, querystring=querystring)
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


def get_strikes(ticker: str, expiration: datetime):
    """
    Get a list of strike prices for the given ticker and expiration date

    Parameters
    ----------
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
    json_resp = get_request(url=url, headers=headers, querystring=querystring)

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
    asset: Asset,
    current_date: date = None,
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
    cache_file = chain_folder / f"{asset.symbol}_{current_date.isoformat()}.parquet"

    # If the S3 remote cache is enabled, opportunistically hydrate the chain cache file for this
    # exact date. Production backtest containers start with empty disks; without this step every
    # run rebuilds chains from ThetaData even when S3 is warm, which keeps hitting the downloader
    # and makes prod much slower than local warm-cache runs.
    #
    # We intentionally only attempt the exact-date file here:
    # - It is deterministic and keeps the S3 key stable.
    # - Reuse across days is still handled by the local folder scan (tolerance window) once at
    #   least one file exists on disk during the current run.
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        cache_manager = get_backtest_cache()
        if not cache_file.exists():
            try:
                cache_manager.ensure_local_file(cache_file)
            except Exception:
                logger.debug(
                    "[THETA][CHAIN_CACHE] Remote cache hydrate failed for %s on %s",
                    asset.symbol,
                    current_date,
                    exc_info=True,
                )
    except Exception:
        # Ignore remote cache hydrate failures.
        pass

    constraints = chain_constraints or {}
    hint_present = any(
        constraints.get(key) is not None for key in ("min_expiration_date", "max_expiration_date")
    )

    # 3) Check for recent cached file (within RECENT_FILE_TOLERANCE_DAYS) unless hints require fresh data
    recent_days_default = 7
    try:
        recent_days_default = int(os.environ.get("THETADATA_CHAIN_RECENT_FILE_TOLERANCE_DAYS", "7"))
    except Exception:
        recent_days_default = 7

    asset_type = str(getattr(asset, "asset_type", "") or "").lower()
    is_index = asset_type == "index"

    min_expiration_date = constraints.get("min_expiration_date")

    def _coerce_date(value: Any) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except Exception:
            return None

    min_expiration_date_coerced = _coerce_date(min_expiration_date)

    # Cache reuse policy:
    # - Always scan local/remote chain cache files first.
    # - Equities are restricted to a small "recent days" window to minimize any chance of drift
    #   around corporate actions.
    # - Index chains can reuse older cache files as long as they still cover the requested horizon.
    #
    # This is important for performance and CI/prod parity: without it, backtests rebuild chains
    # (one strike-list request per expiration) even when S3 is warm, which can dominate runtime.
    should_scan_cache_files = True

    if should_scan_cache_files:
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

            if file_date > current_date:
                continue

            # For indexes, allow reusing older chain files as long as the cached chain still
            # contains future expirations covering `current_date`. Index underlyings don't
            # have splits, and Theta's expirations payload is not truly point-in-time anyway,
            # so aggressively reusing valid chain files can reduce year-long backtests from
            # dozens of chain rebuilds to just a handful.
            #
            # For equities, stick to the tighter "recent days" window to minimize any chance
            # of subtle strike normalization drift around corporate actions.
            if not is_index:
                earliest_okay_date = current_date - timedelta(days=recent_days_default)
                if file_date < earliest_okay_date:
                    continue

            logger.debug(f"Reusing chain file {fpath} (file_date={file_date})")
            df_cached = pd.read_parquet(fpath, engine="pyarrow")

            data = df_cached["data"][0]
            if isinstance(data, dict):
                cache_version = int(data.get("_chain_cache_version", 0) or 0)
                if cache_version < THETADATA_CHAIN_CACHE_VERSION:
                    logger.debug(
                        "Skipping outdated ThetaData chain cache %s (version=%s < %s)",
                        fpath,
                        cache_version,
                        THETADATA_CHAIN_CACHE_VERSION,
                    )
                    continue

            if isinstance(data, dict):
                try:
                    call_chain = data.get("Chains", {}).get("CALL", {}) or {}
                    expiries = [date.fromisoformat(exp) for exp in call_chain.keys()]
                    if not expiries:
                        continue
                    if max(expiries) < current_date:
                        continue
                    if min_expiration_date_coerced is not None and max(expiries) < min_expiration_date_coerced:
                        continue
                except Exception:
                    continue

            # Backfill for older cache files created before UnderlyingSymbol was added.
            if isinstance(data, dict) and "UnderlyingSymbol" not in data:
                data["UnderlyingSymbol"] = asset.symbol
            for right in data["Chains"]:
                for exp_date in data["Chains"][right]:
                    data["Chains"][right][exp_date] = list(data["Chains"][right][exp_date])

            # Best-effort: ensure locally-present chain cache files are also present in the remote
            # S3 cache. CI runners start from empty disks; without this, a "warm local" run can
            # look healthy while CI rebuilds chains via the downloader (and fails acceptance).
            try:
                from lumibot.tools.backtest_cache import get_backtest_cache

                cache_manager = get_backtest_cache()
                remote_key = cache_manager.remote_key_for(fpath)
                if remote_key:
                    marker_path = fpath.with_suffix(fpath.suffix + ".s3key")
                    marker_value = ""
                    if marker_path.exists():
                        try:
                            marker_value = marker_path.read_text(encoding="utf-8").strip()
                        except Exception:
                            marker_value = ""
                    if marker_value != remote_key:
                        cache_manager.on_local_update(fpath)
            except Exception:
                logger.debug(
                    "[THETA][CHAIN_CACHE] Remote cache upload (reuse) failed for %s on %s",
                    asset.symbol,
                    current_date,
                    exc_info=True,
                )

            # Best-effort: learn provider-specific expiration representation from the chain keys so
            # option history queries can use the correct expiration value (Friday vs OCC Saturday).
            try:
                _register_thetadata_expiry_map_from_chain(asset.symbol, data)
            except Exception:
                pass

            return data

    # 4) No suitable file => fetch from ThetaData using exp=0 chain builder
    logger.debug(
        "No suitable cache file found for %s on %s; building historical chain.",
        asset.symbol,
        current_date,
    )
    print(
        f"\nDownloading option chain for {asset} on {current_date}. This will be cached for future use."
    )

    chains_dict = build_historical_chain(
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

    # Best-effort: learn provider-specific expiration representation from the chain keys so option
    # history queries can use the correct expiration value (Friday vs OCC Saturday).
    try:
        _register_thetadata_expiry_map_from_chain(asset.symbol, chains_dict)
    except Exception:
        pass

    # 5) Save to cache file for future reuse
    df_to_cache = pd.DataFrame({"data": [chains_dict]})
    df_to_cache.to_parquet(cache_file, compression='snappy', engine='pyarrow')
    logger.debug(f"Saved chain cache: {cache_file}")
    try:
        from lumibot.tools.backtest_cache import get_backtest_cache

        get_backtest_cache().on_local_update(cache_file)
    except Exception:
        logger.debug(
            "[THETA][CHAIN_CACHE] Remote cache upload failed for %s on %s",
            asset.symbol,
            current_date,
            exc_info=True,
        )

    return chains_dict
