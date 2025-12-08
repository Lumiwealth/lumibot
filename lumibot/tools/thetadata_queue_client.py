"""Queue client for ThetaData requests via Data Downloader.

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
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

# Configuration from environment
# Queue mode is ALWAYS enabled - it's the only way to connect to ThetaData
QUEUE_POLL_INTERVAL = float(os.environ.get("THETADATA_QUEUE_POLL_INTERVAL", "0.01"))  # 10ms default - fast polling
QUEUE_TIMEOUT = float(os.environ.get("THETADATA_QUEUE_TIMEOUT", "0"))  # 0 = wait forever (never fail)
MAX_CONCURRENT_REQUESTS = int(os.environ.get("THETADATA_MAX_CONCURRENT", "8"))  # Max requests in flight


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
            base_url: Data Downloader base URL (e.g., http://44.192.43.146:8080)
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
        self._session = requests.Session()

        # Semaphore to limit concurrent requests
        self._concurrency_semaphore = threading.Semaphore(max_concurrent)
        self._in_flight_count = 0
        self._in_flight_lock = threading.Lock()

        # Local tracking of pending requests
        self._pending_requests: Dict[str, QueuedRequestInfo] = {}  # correlation_id -> info
        self._request_id_to_correlation: Dict[str, str] = {}  # request_id -> correlation_id
        self._lock = threading.RLock()

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
            resp = self._session.get(
                f"{self.base_url}/queue/stats",
                headers={self.api_key_header: self.api_key},
                timeout=5,
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
        correlation_id = self._build_correlation_id(method, path, query_params)

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

        resp = self._session.post(
            submit_url,
            json=payload,
            headers={self.api_key_header: self.api_key},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        request_id = data["request_id"]
        status = data["status"]
        queue_position = data.get("queue_position")

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

        logger.info(
            "Submitted to queue: request_id=%s correlation=%s position=%s",
            request_id,
            correlation_id,
            queue_position,
        )
        return request_id, status

    def _refresh_status(self, request_id: str) -> Optional[QueuedRequestInfo]:
        """Refresh status of a request from the server."""
        try:
            resp = self._session.get(
                f"{self.base_url}/queue/status/{request_id}",
                headers={self.api_key_header: self.api_key},
                timeout=5,
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
                    return info
            return None
        except Exception as exc:
            logger.debug("Failed to refresh status for %s: %s", request_id, exc)
            return None

    def get_result(self, request_id: str) -> Tuple[Optional[Any], int, str]:
        """Get the result of a request."""
        try:
            resp = self._session.get(
                f"{self.base_url}/queue/{request_id}/result",
                headers={self.api_key_header: self.api_key},
                timeout=30,
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

        while True:
            elapsed = time.time() - start_time

            # Check timeout (0 = wait forever)
            if timeout > 0 and elapsed > timeout:
                raise TimeoutError(f"Timed out waiting for {request_id} after {elapsed:.1f}s")

            # Refresh status
            info = self._refresh_status(request_id)

            if info:
                status = info.status
                position = info.queue_position

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
                if status == "completed":
                    result, status_code, _ = self.get_result(request_id)
                    # Log successful receipt from queue (fills logging gap for individual pieces)
                    elapsed = time.time() - start_time
                    result_size = len(result) if isinstance(result, (list, dict)) else 0
                    logger.info(
                        "[THETA][QUEUE] Received result: request_id=%s elapsed=%.1fs status_code=%d size=%d",
                        request_id,
                        elapsed,
                        status_code,
                        result_size,
                    )
                    # Update local tracking
                    with self._lock:
                        if info.correlation_id in self._pending_requests:
                            self._pending_requests[info.correlation_id].status = "completed"
                            self._pending_requests[info.correlation_id].result = result
                            self._pending_requests[info.correlation_id].result_status_code = status_code
                    return result, status_code

                elif status == "dead":
                    with self._lock:
                        if info.correlation_id in self._pending_requests:
                            self._pending_requests[info.correlation_id].status = "dead"
                    raise Exception(f"Request {request_id} permanently failed: {info.error}")

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
        # Acquire semaphore - this blocks if we already have max_concurrent in flight
        # This ensures we never have more than max_concurrent requests at once
        with self._in_flight_lock:
            current = self._in_flight_count
        if current >= self.max_concurrent:
            logger.debug(
                "At max concurrent requests (%d/%d), waiting for slot...",
                current,
                self.max_concurrent,
            )

        self._concurrency_semaphore.acquire()
        with self._in_flight_lock:
            self._in_flight_count += 1
            in_flight = self._in_flight_count

        logger.debug("Acquired request slot (%d/%d in flight)", in_flight, self.max_concurrent)

        try:
            request_id, status, was_pending = self.check_or_submit(
                method=method,
                path=path,
                query_params=query_params,
                headers=headers,
                body=body,
            )

            if was_pending:
                logger.debug("Request already in queue, waiting for existing: %s", request_id)

            return self.wait_for_result(request_id=request_id, timeout=timeout)
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
            api_key = os.environ.get("DATADOWNLOADER_API_KEY", "")
            api_key_header = os.environ.get("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")
            effective_client_id = client_id or _get_default_client_id()

            _queue_client = QueueClient(
                base_url=base_url,
                api_key=api_key,
                api_key_header=api_key_header,
                client_id=effective_client_id,
            )
            logger.info(
                "Queue client initialized: base_url=%s poll_interval=%.3fs timeout=%.1fs client_id=%s",
                base_url,
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
    querystring: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Submit a request via queue and wait for result.

    This is the ONLY way to make ThetaData requests. It handles:
    - Idempotency automatically (same request in queue waits for existing one)
    - Exponential backoff and retries for transient errors
    - Permanent error detection (moves to DLQ, raises exception)

    Args:
        url: Full URL (e.g., http://44.192.43.146:8080/v3/stock/history/ohlc)
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
    from urllib.parse import urlparse
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")

    result, status_code = client.execute_request(
        method="GET",
        path=path,
        query_params=querystring,
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
