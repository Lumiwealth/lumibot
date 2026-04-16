"""Queue client for Data Downloader requests.

This module provides a queue-aware client that:
- Tracks all pending requests and their status
- Checks if a request is already in queue before submitting
- Provides visibility into queue position and estimated wait times
- Uses fast polling (200ms default) for responsive updates

Features:
- Submit requests to queue with correlation IDs (idempotency)
- Check queue status before submitting (avoid duplicates)
- Query queue position and estimated wait time
- Local tracking of all pending requests
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests import exceptions as requests_exceptions

logger = logging.getLogger(__name__)

# Lightweight, non-secret telemetry for backtest audit/debugging.
#
# These counters are intended to be recorded into `*_settings.json` at the end of a backtest
# (see Strategy.write_backtest_settings) so we can answer questions like:
# - Did this run touch the Data Downloader at all?
# - How many submit/status/result calls were made?
#
# IMPORTANT: This must never include secret values (API keys). Query params are safe to record
# as key names only.
_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY: Dict[str, Any] = {
    "requests_total": 0,
    "submit_requests": 0,
    "status_requests": 0,
    "result_requests": 0,
    "stats_requests": 0,
    "first_request_at_unix": None,
    "first_request_kind": None,
    "first_request_path": None,
    "first_request_param_keys": None,
    # Best-effort, non-secret param values for the FIRST queued request only.
    # This is intentionally limited and redacted so it is safe to include in backtest settings
    # (and therefore in CI logs when debugging the warm-cache tripwire).
    "first_request_params": None,
}

_SENSITIVE_PARAM_SUBSTRINGS = (
    "key",
    "token",
    "secret",
    "password",
    "auth",
)
_MAX_PARAM_VALUE_LEN = 200


def _sanitize_query_params(query_params: Dict[str, Any]) -> Dict[str, Any]:
    """Return a JSON-safe, non-secret snapshot of query param values.

    Notes:
    - This must never include secrets (API keys). We aggressively redact anything that *looks*
      secret based on the param name.
    - Values are truncated to keep settings.json small and avoid CI log spam.
    """
    safe: Dict[str, Any] = {}
    for key, value in (query_params or {}).items():
        key_str = str(key)
        lowered = key_str.lower()
        if any(fragment in lowered for fragment in _SENSITIVE_PARAM_SUBSTRINGS):
            safe[key_str] = "<redacted>"
            continue
        if value is None or isinstance(value, (bool, int, float)):
            safe[key_str] = value
            continue
        try:
            rendered = str(value)
        except Exception:
            safe[key_str] = "<unprintable>"
            continue
        if len(rendered) > _MAX_PARAM_VALUE_LEN:
            rendered = f"{rendered[:_MAX_PARAM_VALUE_LEN]}...(truncated)"
        safe[key_str] = rendered
    return safe


def _record_telemetry(kind: str, path: str, query_params: Optional[Dict[str, Any]] = None) -> None:
    with _TELEMETRY_LOCK:
        _TELEMETRY["requests_total"] = int(_TELEMETRY.get("requests_total") or 0) + 1
        key = f"{kind}_requests"
        if key in _TELEMETRY:
            _TELEMETRY[key] = int(_TELEMETRY.get(key) or 0) + 1
        if _TELEMETRY.get("first_request_at_unix") is None:
            _TELEMETRY["first_request_at_unix"] = float(time.time())
            _TELEMETRY["first_request_kind"] = str(kind)
            _TELEMETRY["first_request_path"] = str(path)
            if query_params:
                _TELEMETRY["first_request_param_keys"] = sorted(str(k) for k in query_params.keys())
                _TELEMETRY["first_request_params"] = _sanitize_query_params(query_params)


def queue_telemetry_snapshot() -> Dict[str, Any]:
    """Return a copy of current queue client telemetry (numbers only; safe for settings/logs)."""
    with _TELEMETRY_LOCK:
        return dict(_TELEMETRY)

# Configuration from environment
# Queue mode is ALWAYS enabled: all provider requests (Theta/IBKR/etc.) go through the Data Downloader.
# NOTE: Extremely fast polling can overwhelm the downloader (and CloudWatch) when many requests
# are in flight. A 200ms default keeps progress responsive without creating a status-poll storm.
QUEUE_POLL_INTERVAL = float(os.environ.get("THETADATA_QUEUE_POLL_INTERVAL", "0.2"))

# NOTE: Never timing out can cause production backtests to appear "stuck forever" when a single
# downloader request is lost or wedged. Default to a bounded wait; callers can override per-call
# or via env var if they truly want infinite waits.
QUEUE_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_TIMEOUT", "600"))
MAX_CONCURRENT_REQUESTS = int(os.environ.get("THETADATA_MAX_CONCURRENT", "8"))  # Max requests in flight
QUEUE_CONNECT_HTTP_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_CONNECT_HTTP_TIMEOUT", "15"))
QUEUE_SUBMIT_HTTP_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_SUBMIT_HTTP_TIMEOUT", "120"))
QUEUE_STATUS_HTTP_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_STATUS_HTTP_TIMEOUT", "10"))
QUEUE_RESULT_HTTP_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_RESULT_HTTP_TIMEOUT", "120"))
QUEUE_SUBMIT_MAX_WAIT = float(os.environ.get("THETADATA_QUEUE_SUBMIT_MAX_WAIT", "0"))  # 0 = wait forever
QUEUE_SUBMIT_BACKOFF_BASE = float(os.environ.get("THETADATA_QUEUE_SUBMIT_BACKOFF_BASE", "0.5"))
QUEUE_SUBMIT_BACKOFF_MAX = float(os.environ.get("THETADATA_QUEUE_SUBMIT_BACKOFF_MAX", "30"))
QUEUE_SUBMIT_BACKOFF_JITTER_PCT = float(os.environ.get("THETADATA_QUEUE_SUBMIT_BACKOFF_JITTER_PCT", "0.1"))

def _normalize_downloader_base_url(base_url: str) -> str:
    """Normalize the downloader base URL.

    Notes:
    - This function intentionally does **not** rewrite hosts. The downloader base URL is an
      environment-specific setting and must not be hard-coded to any private endpoint.
    """
    normalized = (base_url or "").strip().rstrip("/")
    if not normalized:
        return normalized

    has_scheme = "://" in normalized
    normalized_with_scheme = normalized if has_scheme else f"http://{normalized}"
    parsed = urlparse(normalized_with_scheme)

    host = parsed.hostname or ""
    if host.lower() in {"localhost", "127.0.0.1", "0.0.0.0"}:
        return normalized_with_scheme

    # Numeric IPs are valid; keep them as-is.
    return normalized_with_scheme


def _redact_downloader_base_url_for_logs(base_url: str) -> str:
    """Redact non-local downloader base URLs for logs.

    We intentionally treat the Data Downloader host as infrastructure-private: it should never be
    written into docs or logs (especially in CI/prod where logs may be exported).

    Local development URLs remain readable (localhost/127.0.0.1/0.0.0.0).
    """
    normalized = _normalize_downloader_base_url(base_url)
    if not normalized:
        return normalized

    parsed = urlparse(normalized)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
        return normalized

    port = f":{parsed.port}" if parsed.port else ""
    scheme = parsed.scheme or "http"
    return f"{scheme}://<redacted>{port}"


@dataclass
class QueuedRequestInfo:
    """Information about a request in the queue."""
    request_id: str
    correlation_id: str
    path: str
    status: str  # pending, processing, completed, failed, dead
    queue_position: Optional[int] = None
    estimated_wait: Optional[float] = None
    attempts: int = 0
    created_at: float = field(default_factory=time.time)
    last_checked: float = field(default_factory=time.time)
    result: Optional[Any] = None
    result_status_code: Optional[int] = None
    error: Optional[str] = None


class QueueClient:
    """Queue-aware client for ThetaData requests.

    This client maintains local state about pending requests and provides
    methods to check queue status before submitting new requests.

    Key features:
    - Limits concurrent requests to MAX_CONCURRENT_REQUESTS (default 8)
    - Tracks all pending requests and their queue position
    - Idempotency via correlation IDs (no duplicate submissions)
    - Fast polling (10ms default) for responsive results
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_key_header: str = "X-Downloader-Key",
        poll_interval: float = QUEUE_POLL_INTERVAL,
        timeout: float = QUEUE_TIMEOUT,
        max_concurrent: int = MAX_CONCURRENT_REQUESTS,
        client_id: Optional[str] = None,
    ) -> None:
        """Initialize the queue client.

        Args:
            base_url: Data Downloader base URL (e.g., http://localhost:8080 or https://<your-downloader-host>:8080)
            api_key: API key for Data Downloader
            api_key_header: Header name for API key
            poll_interval: Seconds between status polls (default 10ms)
            timeout: Max seconds to wait for result (0 = wait forever)
            max_concurrent: Max requests allowed in flight at once (default 8)
            client_id: Client identifier for round-robin fairness (e.g., strategy name)
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.client_id = client_id
        # NOTE: requests.Session is not thread-safe. ThetaData backtests can execute multiple
        # concurrent downloader requests (e.g., chunked history fetches), so we must not share a
        # single Session across threads. Use thread-local sessions and a generation counter so we
        # can invalidate all sessions on recovery (network wedges, timeouts).
        self._session_local = threading.local()
        self._session_generation = 0
        self._session_generation_lock = threading.Lock()
        self._last_session_reset_log = 0.0
        self._last_status_refresh_error: Optional[str] = None
        self._last_status_refresh_error_time = 0.0
        self._status_refresh_error_streak = 0

        # Semaphore to limit concurrent requests
        self._concurrency_semaphore = threading.Semaphore(max_concurrent)
        self._in_flight_count = 0
        self._in_flight_lock = threading.Lock()

        # Local tracking of pending requests
        self._pending_requests: Dict[str, QueuedRequestInfo] = {}  # correlation_id -> info
        self._request_id_to_correlation: Dict[str, str] = {}  # request_id -> correlation_id
        self._lock = threading.RLock()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max(10, self.max_concurrent),
            pool_maxsize=max(10, self.max_concurrent),
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_session(self) -> requests.Session:
        session = getattr(self._session_local, "session", None)
        generation = getattr(self._session_local, "generation", None)
        if session is None or generation != self._session_generation:
            self._session_local.session = self._build_session()
            self._session_local.generation = self._session_generation
        return self._session_local.session

    def _invalidate_sessions(self, reason: str) -> None:
        with self._session_generation_lock:
            self._session_generation += 1
        now = time.time()
        if now - self._last_session_reset_log > 30:
            logger.info("[DOWNLOADER][QUEUE] Reset HTTP sessions (reason=%s)", reason)
            self._last_session_reset_log = now

    def _build_correlation_id(
        self,
        method: str,
        path: str,
        query_params: Dict[str, Any],
    ) -> str:
        """Build a deterministic correlation ID for idempotency."""
        sorted_params = sorted(query_params.items())
        key_data = f"{method}:{path}:{json.dumps(sorted_params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def is_request_pending(self, correlation_id: str) -> bool:
        """Check if a request with this correlation ID is already pending.

        Args:
            correlation_id: The correlation ID to check

        Returns:
            True if request is pending/processing, False otherwise
        """
        with self._lock:
            info = self._pending_requests.get(correlation_id)
            if info is None:
                return False
            return info.status in ("pending", "processing")

    def get_request_info(self, correlation_id: str) -> Optional[QueuedRequestInfo]:
        """Get information about a request by correlation ID.

        Args:
            correlation_id: The correlation ID

        Returns:
            QueuedRequestInfo if found, None otherwise
        """
        with self._lock:
            return self._pending_requests.get(correlation_id)

    def get_pending_requests(self) -> List[QueuedRequestInfo]:
        """Get all currently pending requests.

        Returns:
            List of QueuedRequestInfo for pending/processing requests
        """
        with self._lock:
            return [
                info for info in self._pending_requests.values()
                if info.status in ("pending", "processing")
            ]

    def get_in_flight_count(self) -> int:
        """Get the number of requests currently in flight.

        Returns:
            Number of requests currently being processed (max is max_concurrent)
        """
        with self._in_flight_lock:
            return self._in_flight_count

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics about the local request tracking.

        Returns:
            Dictionary with local tracking stats including in-flight count
        """
        with self._lock:
            pending = [i for i in self._pending_requests.values() if i.status == "pending"]
            processing = [i for i in self._pending_requests.values() if i.status == "processing"]
            completed = [i for i in self._pending_requests.values() if i.status == "completed"]
            failed = [i for i in self._pending_requests.values() if i.status in ("failed", "dead")]

            return {
                "total_tracked": len(self._pending_requests),
                "pending": len(pending),
                "processing": len(processing),
                "completed": len(completed),
                "failed": len(failed),
                "in_flight": self.get_in_flight_count(),
                "max_concurrent": self.max_concurrent,
                "oldest_pending": min((i.created_at for i in pending), default=None),
            }

    def fetch_server_queue_stats(self) -> Dict[str, Any]:
        """Fetch queue statistics from the server.

        Returns:
            Server-side queue statistics
        """
        try:
            _record_telemetry("stats", "/queue/stats")
            resp = self._get_session().get(
                f"{self.base_url}/queue/stats",
                headers={self.api_key_header: self.api_key},
                timeout=(QUEUE_CONNECT_HTTP_TIMEOUT, QUEUE_STATUS_HTTP_TIMEOUT),
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("Failed to fetch server queue stats: %s", exc)
            return {"error": str(exc)}

    def check_or_submit(
        self,
        method: str,
        path: str,
        query_params: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        correlation_id_override: Optional[str] = None,
    ) -> Tuple[str, str, bool]:
        """Check if request exists in queue, submit if not.

        This is the primary method to use - it checks if the request is already
        pending before submitting a new one (idempotency).

        Args:
            method: HTTP method
            path: API path
            query_params: Query parameters
            headers: Optional headers
            body: Optional body

        Returns:
            Tuple of (request_id, status, was_already_pending)
        """
        correlation_id = correlation_id_override or self._build_correlation_id(method, path, query_params)

        with self._lock:
            # Check if we already have this request tracked
            existing = self._pending_requests.get(correlation_id)
            if existing and existing.status in ("pending", "processing"):
                # Refresh status from server
                self._refresh_status(existing.request_id)
                existing = self._pending_requests.get(correlation_id)
                if existing and existing.status in ("pending", "processing"):
                    logger.debug(
                        "Request already in queue: correlation=%s request_id=%s status=%s position=%s",
                        correlation_id,
                        existing.request_id,
                        existing.status,
                        existing.queue_position,
                    )
                    return existing.request_id, existing.status, True

        # Not in queue, submit new request
        request_id, status = self._submit_request(
            method=method,
            path=path,
            query_params=query_params,
            headers=headers,
            body=body,
            correlation_id=correlation_id,
        )

        return request_id, status, False

    @staticmethod
    def _compute_backoff_delay(
        attempt: int,
        base_delay: float,
        max_delay: float,
        jitter_pct: float,
        retry_after: Optional[Any] = None,
    ) -> float:
        """Compute exponential backoff delay with jitter and optional retry-after."""
        delay = min(max_delay, base_delay * (2 ** max(0, attempt - 1)))

        retry_after_s: Optional[float] = None
        if retry_after is not None:
            try:
                retry_after_s = float(retry_after)
            except Exception:
                retry_after_s = None

        if retry_after_s is not None and retry_after_s > delay:
            delay = retry_after_s

        if jitter_pct > 0:
            delay += delay * jitter_pct * random.random()

        return max(0.0, delay)

    def _submit_request(
        self,
        method: str,
        path: str,
        query_params: Dict[str, Any],
        headers: Optional[Dict[str, str]],
        body: Optional[bytes],
        correlation_id: str,
    ) -> Tuple[str, str]:
        """Submit a new request to the queue."""
        body_encoded = None
        if body:
            body_encoded = base64.b64encode(body).decode("ascii")

        submit_url = f"{self.base_url}/queue/submit"
        payload = {
            "method": method,
            "path": path,
            "query_params": query_params,
            "headers": headers or {},
            "body": body_encoded,
            "correlation_id": correlation_id,
            "client_id": self.client_id,
        }

        start_time = time.time()
        attempt = 0
        last_error: Optional[BaseException] = None

        while True:
            attempt += 1
            elapsed = time.time() - start_time
            if QUEUE_SUBMIT_MAX_WAIT > 0 and elapsed > QUEUE_SUBMIT_MAX_WAIT:
                raise TimeoutError(
                    f"Timed out submitting request to downloader after {elapsed:.1f}s (attempts={attempt})"
                ) from last_error

            try:
                _record_telemetry("submit", path, query_params=query_params)
                resp = self._get_session().post(
                    submit_url,
                    json=payload,
                    headers={self.api_key_header: self.api_key},
                    timeout=(QUEUE_CONNECT_HTTP_TIMEOUT, QUEUE_SUBMIT_HTTP_TIMEOUT),
                )
            except (
                requests_exceptions.ReadTimeout,
                requests_exceptions.ConnectTimeout,
                requests_exceptions.ConnectionError,
            ) as exc:
                last_error = exc
                self._invalidate_sessions("submit network error")
                delay = self._compute_backoff_delay(
                    attempt=attempt,
                    base_delay=QUEUE_SUBMIT_BACKOFF_BASE,
                    max_delay=QUEUE_SUBMIT_BACKOFF_MAX,
                    jitter_pct=QUEUE_SUBMIT_BACKOFF_JITTER_PCT,
                )
                logger.info(
                    "[DOWNLOADER][QUEUE] Submit network timeout; retrying in %.2fs (attempt=%d): %s",
                    delay,
                    attempt,
                    exc,
                )
                time.sleep(delay)
                continue

            data: Optional[Dict[str, Any]] = None
            try:
                parsed = resp.json()
                if isinstance(parsed, dict):
                    data = parsed
            except Exception:
                data = None

            # Respect downloader "queue_full" backoff contract
            if data and data.get("error") == "queue_full":
                delay = self._compute_backoff_delay(
                    attempt=attempt,
                    base_delay=QUEUE_SUBMIT_BACKOFF_BASE,
                    max_delay=QUEUE_SUBMIT_BACKOFF_MAX,
                    jitter_pct=QUEUE_SUBMIT_BACKOFF_JITTER_PCT,
                    retry_after=data.get("retry_after") or resp.headers.get("Retry-After"),
                )
                logger.info(
                    "[DOWNLOADER][QUEUE] Downloader queue full; retrying submit in %.2fs (attempt=%d)",
                    delay,
                    attempt,
                )
                time.sleep(delay)
                continue

            try:
                resp.raise_for_status()
            except requests_exceptions.HTTPError as exc:
                last_error = exc
                status_code = getattr(resp, "status_code", None)
                should_retry = status_code in (408, 425, 429, 500, 502, 503, 504) or (
                    isinstance(status_code, int) and 500 <= status_code < 600
                )
                if not should_retry:
                    raise

                delay = self._compute_backoff_delay(
                    attempt=attempt,
                    base_delay=QUEUE_SUBMIT_BACKOFF_BASE,
                    max_delay=QUEUE_SUBMIT_BACKOFF_MAX,
                    jitter_pct=QUEUE_SUBMIT_BACKOFF_JITTER_PCT,
                    retry_after=resp.headers.get("Retry-After"),
                )
                logger.info(
                    "[DOWNLOADER][QUEUE] Submit transient HTTP %s; retrying in %.2fs (attempt=%d)",
                    status_code,
                    delay,
                    attempt,
                )
                time.sleep(delay)
                continue

            if not data:
                raise ValueError(f"Downloader submit response was not JSON: {resp.text[:200]}")

            request_id = data["request_id"]
            status = data["status"]
            queue_position = data.get("queue_position")
            break

        # Track locally
        with self._lock:
            info = QueuedRequestInfo(
                request_id=request_id,
                correlation_id=correlation_id,
                path=path,
                status=status,
                queue_position=queue_position,
            )
            self._pending_requests[correlation_id] = info
            self._request_id_to_correlation[request_id] = correlation_id

        try:
            params_json = json.dumps(query_params, sort_keys=True, default=str)
        except Exception:
            params_json = str(query_params)
        if len(params_json) > 500:
            params_json = params_json[:500] + "…"

        logger.info(
            "Submitted to queue: request_id=%s correlation=%s position=%s path=%s params=%s",
            request_id,
            correlation_id,
            queue_position,
            path,
            params_json,
        )
        # Best-effort: surface request_id into the progress UI so a "stall" is diagnosable.
        try:  # pragma: no cover - UI plumbing
            from lumibot.tools.thetadata_helper import update_download_status_queue_info

            update_download_status_queue_info(
                request_id=request_id,
                correlation_id=correlation_id,
                queue_status=status,
                queue_position=queue_position,
                submitted_at=time.time(),
            )
        except Exception:
            pass
        return request_id, status

    def _refresh_status(self, request_id: str) -> Optional[QueuedRequestInfo]:
        """Refresh status of a request from the server."""
        try:
            _record_telemetry("status", f"/queue/status/{request_id}")
            resp = self._get_session().get(
                f"{self.base_url}/queue/status/{request_id}",
                headers={self.api_key_header: self.api_key},
                timeout=(QUEUE_CONNECT_HTTP_TIMEOUT, QUEUE_STATUS_HTTP_TIMEOUT),
            )
            if resp.status_code == 404:
                # Request not found, remove from tracking
                with self._lock:
                    correlation_id = self._request_id_to_correlation.get(request_id)
                    if correlation_id:
                        self._pending_requests.pop(correlation_id, None)
                        self._request_id_to_correlation.pop(request_id, None)
                return None

            resp.raise_for_status()
            data = resp.json()

            with self._lock:
                correlation_id = self._request_id_to_correlation.get(request_id)
                if correlation_id and correlation_id in self._pending_requests:
                    info = self._pending_requests[correlation_id]
                    info.status = data.get("status", info.status)
                    info.queue_position = data.get("queue_position")
                    info.estimated_wait = data.get("estimated_wait")
                    info.attempts = data.get("attempts", info.attempts)
                    info.error = data.get("last_error")
                    info.last_checked = time.time()
                    # Best-effort: surface queue status into the progress UI.
                    try:  # pragma: no cover - UI plumbing
                        from lumibot.tools.thetadata_helper import update_download_status_queue_info

                        update_download_status_queue_info(
                            request_id=info.request_id,
                            correlation_id=info.correlation_id,
                            queue_status=info.status,
                            queue_position=info.queue_position,
                            estimated_wait=info.estimated_wait,
                            attempts=info.attempts,
                            last_error=info.error,
                        )
                    except Exception:
                        pass
                    self._status_refresh_error_streak = 0
                    return info
            return None
        except Exception as exc:
            self._last_status_refresh_error = str(exc)
            self._last_status_refresh_error_time = time.time()
            self._status_refresh_error_streak += 1
            if self._status_refresh_error_streak >= 3:
                self._invalidate_sessions("status refresh failures")
                self._status_refresh_error_streak = 0
            logger.debug("Failed to refresh status for %s: %s", request_id, exc)
            return None

    def get_result(self, request_id: str) -> Tuple[Optional[Any], int, str]:
        """Get the result of a request."""
        try:
            _record_telemetry("result", f"/queue/{request_id}/result")
            resp = self._get_session().get(
                f"{self.base_url}/queue/{request_id}/result",
                headers={self.api_key_header: self.api_key},
                timeout=(QUEUE_CONNECT_HTTP_TIMEOUT, QUEUE_RESULT_HTTP_TIMEOUT),
            )
            data = resp.json()
            status_code = resp.status_code

            if status_code == 200:
                return data.get("result"), status_code, "completed"
            elif status_code == 202:
                return None, status_code, data.get("status", "processing")
            elif status_code == 500:
                return None, status_code, "dead"
            else:
                return None, status_code, data.get("status", "unknown")
        except Exception as exc:
            logger.warning("Failed to get result for %s: %s", request_id, exc)
            return None, 0, "error"

    def wait_for_result(
        self,
        request_id: str,
        timeout: Optional[float] = None,
        poll_interval: Optional[float] = None,
    ) -> Tuple[Optional[Any], int]:
        """Wait for a request to complete.

        Polls the queue for status updates and returns when complete.

        Args:
            request_id: The request ID
            timeout: Max seconds to wait (0 = wait forever)
            poll_interval: Seconds between polls

        Returns:
            Tuple of (result_data, status_code)
        """
        timeout = timeout if timeout is not None else self.timeout
        poll_interval = poll_interval if poll_interval is not None else self.poll_interval
        start_time = time.time()
        last_log_time = 0
        last_position = None
        last_status = None
        last_info_time = 0.0
        missing_info_streak = 0
        terminal_result_error_streak = 0

        while True:
            elapsed = time.time() - start_time

            # Check timeout (0 = wait forever)
            if timeout > 0 and elapsed > timeout:
                info = self._refresh_status(request_id)
                if info:
                    last_status = info.status
                    last_position = info.queue_position
                    last_error = info.error
                    last_attempts = info.attempts
                    last_estimated_wait = info.estimated_wait
                else:
                    last_error = None
                    last_attempts = None
                    last_estimated_wait = None

                raise TimeoutError(
                    "Timed out waiting for %s after %.1fs (status=%s position=%s attempts=%s est_wait=%s error=%s)"
                    % (
                        request_id,
                        elapsed,
                        last_status,
                        last_position,
                        last_attempts,
                        last_estimated_wait,
                        last_error,
                    )
                )

            # Refresh status
            info = self._refresh_status(request_id)

            if info:
                missing_info_streak = 0
                status = info.status
                position = info.queue_position
                last_status = status

                # Emit a low-rate heartbeat at INFO so "no logs for an hour" is diagnosable in prod.
                if elapsed >= 10 and time.time() - last_info_time > 30:
                    logger.info(
                        "[DOWNLOADER][QUEUE] Still waiting: request_id=%s status=%s position=%s attempts=%s est_wait=%.1fs elapsed=%.1fs",
                        request_id,
                        status,
                        position,
                        info.attempts,
                        info.estimated_wait or 0,
                        elapsed,
                    )
                    last_info_time = time.time()

                # Log position changes or periodic updates
                if position != last_position or time.time() - last_log_time > 10:
                    logger.debug(
                        "Queue status: request=%s status=%s position=%s wait=%.1fs elapsed=%.1fs",
                        request_id,
                        status,
                        position,
                        info.estimated_wait or 0,
                        elapsed,
                    )
                    last_position = position
                    last_log_time = time.time()

                # Check terminal states
                if status in ("completed", "failed"):
                    # IBKR may leave result endpoint in 202 for failed history jobs while status
                    # already reports a terminal server-side error. Fast-fail these explicit
                    # no-data failures to avoid multi-minute dead loops.
                    if status == "failed":
                        err_text = str(info.error or "").lower()
                        if (
                            "ibkr/iserver/marketdata/history" in str(info.path or "")
                            and "chart data unavailable" in err_text
                            and int(info.attempts or 0) >= 3
                        ):
                            with self._lock:
                                if info.correlation_id in self._pending_requests:
                                    self._pending_requests[info.correlation_id].status = "dead"
                            raise Exception(f"Request {request_id} permanently failed: {info.error}")

                    # IMPORTANT: The downloader may surface "no data" (ThetaData 472) or other
                    # non-200 terminal outcomes as status=failed. We must treat those as terminal
                    # once the result endpoint is available, otherwise callers can stall until a timeout
                    # and never record a cache placeholder (breaking the warm-cache invariant).
                    result, status_code, result_state = self.get_result(request_id)
                    if status_code == 202:
                        terminal_result_error_streak = 0
                        # Result not ready yet; keep polling.
                        pass
                    elif status_code == 0 or result_state == "error":
                        terminal_result_error_streak += 1
                        logger.warning(
                            "[DOWNLOADER][QUEUE] Failed to fetch terminal result: request_id=%s status=%s streak=%d error=%s",
                            request_id,
                            status,
                            terminal_result_error_streak,
                            info.error,
                        )
                        if terminal_result_error_streak >= 3:
                            self._invalidate_sessions("terminal result fetch failures")
                            raise TimeoutError(
                                f"Failed to fetch terminal result for {request_id} after {terminal_result_error_streak} attempts"
                            )
                    elif result_state == "dead" or status_code == 500:
                        terminal_result_error_streak = 0
                        with self._lock:
                            if info.correlation_id in self._pending_requests:
                                self._pending_requests[info.correlation_id].status = "dead"
                        raise Exception(f"Request {request_id} permanently failed: {info.error}")
                    else:
                        terminal_result_error_streak = 0
                        elapsed = time.time() - start_time
                        result_size = len(result) if isinstance(result, (list, dict)) else 0
                        logger.info(
                            "[DOWNLOADER][QUEUE] Received result: request_id=%s status=%s elapsed=%.1fs status_code=%d size=%d",
                            request_id,
                            status,
                            elapsed,
                            status_code,
                            result_size,
                        )
                        with self._lock:
                            if info.correlation_id in self._pending_requests:
                                self._pending_requests[info.correlation_id].status = status
                                self._pending_requests[info.correlation_id].result = result
                                self._pending_requests[info.correlation_id].result_status_code = status_code
                        return result, status_code

                elif status == "dead":
                    terminal_result_error_streak = 0
                    with self._lock:
                        if info.correlation_id in self._pending_requests:
                            self._pending_requests[info.correlation_id].status = "dead"
                    raise Exception(f"Request {request_id} permanently failed: {info.error}")
            else:
                terminal_result_error_streak = 0
                # If we lose connectivity to the downloader status endpoint, waiting can look like a
                # "silent stall". Emit a low-rate heartbeat and opportunistically reset sessions so
                # the request can recover without user intervention.
                missing_info_streak += 1
                if elapsed >= 10 and time.time() - last_info_time > 30:
                    logger.info(
                        "[DOWNLOADER][QUEUE] Still waiting: request_id=%s (status refresh failing, streak=%d, last_error=%s) elapsed=%.1fs",
                        request_id,
                        missing_info_streak,
                        self._last_status_refresh_error,
                        elapsed,
                    )
                    last_info_time = time.time()

            # Still pending/processing, wait before next poll
            time.sleep(poll_interval)

    def execute_request(
        self,
        method: str,
        path: str,
        query_params: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[Optional[Any], int]:
        """Submit a request and wait for result.

        This is the main method - it handles:
        1. Limiting to max_concurrent requests in flight (default 8)
        2. Idempotency (checking if request already in queue)
        3. Waiting for result with fast polling

        Args:
            method: HTTP method
            path: API path
            query_params: Query parameters
            headers: Optional headers
            body: Optional body
            timeout: Max seconds to wait

        Returns:
            Tuple of (result_data, status_code)
        """
        # Acquire semaphore - this blocks if we already have max_concurrent in flight.
        #
        # Production backtests can appear to "go silent" when the concurrency gate blocks (e.g.,
        # if other in-flight requests are wedged). Use a timed acquire so we can emit a low-rate
        # heartbeat at INFO and avoid silent stalls.
        start_wait = time.monotonic()
        last_wait_log = 0.0
        while True:
            if self._concurrency_semaphore.acquire(timeout=1.0):
                break
            waited = time.monotonic() - start_wait
            if waited >= 10 and (time.monotonic() - last_wait_log) > 30:
                with self._in_flight_lock:
                    current = self._in_flight_count
                logger.info(
                    "[DOWNLOADER][QUEUE] Waiting for request slot (in_flight=%d/%d) waited=%.1fs path=%s",
                    current,
                    self.max_concurrent,
                    waited,
                    path,
                )
                last_wait_log = time.monotonic()
        with self._in_flight_lock:
            self._in_flight_count += 1
            in_flight = self._in_flight_count

        logger.debug("Acquired request slot (%d/%d in flight)", in_flight, self.max_concurrent)

        try:
            # Self-healing retry loop:
            # - Never "go silent" when downloader status polling fails.
            # - Recover from wedged requests via timeouts/session resets/resubmits.
            # - Do not fail-fast: keep retrying with backoff and session resets. Backtest
            #   wall-clock enforcement belongs to the outer orchestrator (ECS/task timeouts).
            base_correlation_id = self._build_correlation_id(method, path, query_params)

            # Choose a finite per-attempt timeout even if the caller requests "forever" waits.
            if timeout is not None and timeout > 0:
                attempt_timeout = timeout
            elif self.timeout and self.timeout > 0:
                attempt_timeout = self.timeout
            else:
                attempt_timeout = 900.0

            timeout_count = 0
            correlation_override: Optional[str] = None

            while True:
                request_id, status, was_pending = self.check_or_submit(
                    method=method,
                    path=path,
                    query_params=query_params,
                    headers=headers,
                    body=body,
                    correlation_id_override=correlation_override,
                )

                if was_pending:
                    logger.debug("Request already in queue, waiting for existing: %s", request_id)

                try:
                    return self.wait_for_result(request_id=request_id, timeout=attempt_timeout)
                except TimeoutError as exc:
                    timeout_count += 1
                    self._invalidate_sessions("wait timeout")

                    # Best-effort: surface the failure into the backtest status payload so the UI
                    # can show what we're stuck on.
                    try:  # pragma: no cover - UI plumbing
                        from lumibot.tools.thetadata_helper import update_download_status_queue_info

                        update_download_status_queue_info(
                            request_id=request_id,
                            correlation_id=correlation_override or base_correlation_id,
                            last_error=str(exc),
                        )
                    except Exception:
                        pass

                    # First few timeouts: keep waiting on the same logical request (idempotent).
                    # After repeated timeouts, force a resubmit with a new correlation id so we can
                    # recover from a wedged downloader queue entry.
                    if timeout_count >= 3:
                        correlation_override = (
                            f"{base_correlation_id}-retry-{timeout_count}-{int(time.time())}"
                        )
                        logger.warning(
                            "[DOWNLOADER][QUEUE] Request %s timed out repeatedly; forcing resubmit (attempt=%d)",
                            request_id,
                            timeout_count,
                        )
                    else:
                        correlation_override = None

                    delay = self._compute_backoff_delay(
                        attempt=timeout_count,
                        base_delay=1.0,
                        max_delay=30.0,
                        jitter_pct=0.2,
                    )
                    time.sleep(delay)
        finally:
            # Release semaphore when done (success or failure)
            with self._in_flight_lock:
                self._in_flight_count -= 1
            self._concurrency_semaphore.release()

    def cleanup_completed(self, max_age_seconds: float = 3600) -> int:
        """Remove old completed requests from local tracking.

        Args:
            max_age_seconds: Remove completed requests older than this

        Returns:
            Number of requests removed
        """
        cutoff = time.time() - max_age_seconds
        removed = 0

        with self._lock:
            to_remove = [
                cid for cid, info in self._pending_requests.items()
                if info.status in ("completed", "dead") and info.last_checked < cutoff
            ]
            for cid in to_remove:
                info = self._pending_requests.pop(cid, None)
                if info:
                    self._request_id_to_correlation.pop(info.request_id, None)
                    removed += 1

        if removed:
            logger.debug("Cleaned up %d old completed requests", removed)
        return removed


# Global client instance
_queue_client: Optional[QueueClient] = None
_client_lock = threading.Lock()


def _get_default_client_id() -> Optional[str]:
    """Get default client_id from environment or script name.

    Priority:
    1. THETADATA_QUEUE_CLIENT_ID env var
    2. Script filename (without path/extension) from sys.argv[0]
    """
    import sys

    # First try environment variable
    env_client_id = os.environ.get("THETADATA_QUEUE_CLIENT_ID")
    if env_client_id:
        return env_client_id

    # Fall back to script name
    try:
        if sys.argv and sys.argv[0]:
            script_path = sys.argv[0]
            # Extract just the filename without path and extension
            script_name = os.path.basename(script_path)
            if script_name.endswith(".py"):
                script_name = script_name[:-3]
            if script_name:
                return script_name
    except Exception:
        pass

    return None


def get_queue_client(client_id: Optional[str] = None) -> QueueClient:
    """Get or create the global queue client.

    Queue mode is ALWAYS enabled - this is the only way to connect to ThetaData.

    Args:
        client_id: Optional client identifier for round-robin fairness.
                   If provided, updates the client_id on the existing client.
                   Auto-detected from script name if not provided.
    """
    global _queue_client

    with _client_lock:
        if _queue_client is None:
            base_url = os.environ.get("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:8080")
            base_url = _normalize_downloader_base_url(base_url)
            api_key = os.environ.get("DATADOWNLOADER_API_KEY", "")
            api_key_header = os.environ.get("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")
            effective_client_id = client_id or _get_default_client_id()

            _queue_client = QueueClient(
                base_url=base_url,
                api_key=api_key,
                api_key_header=api_key_header,
                client_id=effective_client_id,
            )
            base_url_log = _redact_downloader_base_url_for_logs(base_url)
            logger.info(
                "Queue client initialized: base_url=%s poll_interval=%.3fs timeout=%.1fs client_id=%s",
                base_url_log,
                _queue_client.poll_interval,
                _queue_client.timeout,
                _queue_client.client_id,
            )
        elif client_id is not None:
            # Update client_id on existing client
            _queue_client.client_id = client_id

    return _queue_client


def set_queue_client_id(client_id: str) -> None:
    """Set the client_id for round-robin fairness.

    Call this before making requests to identify which strategy/backtest
    the requests belong to. This enables fair scheduling across multiple
    concurrent backtests.

    Args:
        client_id: Client identifier (e.g., strategy name)
    """
    client = get_queue_client()
    client.client_id = client_id
    logger.info("Queue client_id set to: %s", client_id)


def is_queue_enabled() -> bool:
    """Check if queue mode is enabled.

    Always returns True - queue mode is the ONLY way to connect to ThetaData.
    This function is kept for backward compatibility but the answer is always True.
    """
    return True


def queue_request(
    url: str,
    querystring: Optional[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Submit a request via queue and wait for result.

    This is the ONLY way to make ThetaData requests. It handles:
    - Idempotency automatically (same request in queue waits for existing one)
    - Exponential backoff and retries for transient errors
    - Permanent error detection (moves to DLQ, raises exception)

    Args:
        url: Full URL (e.g., http://localhost:8080/v3/stock/history/ohlc)
        querystring: Query parameters
        headers: Optional headers
        timeout: Max seconds to wait (0 = wait forever)

    Returns:
        Response data if request completed successfully
        None if no data (status 472)

    Raises:
        TimeoutError if timeout exceeded
        Exception if request permanently failed (moved to DLQ)
    """
    client = get_queue_client()

    # Extract path from URL
    from urllib.parse import parse_qsl, urlparse
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")
    url_query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    merged_query_params: Dict[str, Any] = {}
    merged_query_params.update(url_query_params)
    if querystring:
        merged_query_params.update(querystring)

    result, status_code = client.execute_request(
        method="GET",
        path=path,
        query_params=merged_query_params,
        headers=headers,
        timeout=timeout,
    )

    # Handle status codes
    if status_code == 472:
        return None  # No data
    elif status_code == 200:
        return result
    else:
        logger.warning("Queue request returned status %d: %s", status_code, result)
        return result
