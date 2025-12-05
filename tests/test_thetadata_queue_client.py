"""Tests for the ThetaData queue client.

Tests cover:
- QueueClient initialization and configuration
- Request submission to queue
- Status checking and polling
- Idempotency (checking if request is already in queue)
- Result retrieval
- Local tracking of pending requests
- Error handling
"""
import json
import os
import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import requests

# Need to set env var before importing to control QUEUE_ENABLED
os.environ["THETADATA_USE_QUEUE"] = "true"
os.environ["DATADOWNLOADER_BASE_URL"] = "http://test-server:8080"
os.environ["DATADOWNLOADER_API_KEY"] = "test-api-key"

from lumibot.tools.thetadata_queue_client import (
    QueueClient,
    QueuedRequestInfo,
    get_queue_client,
    is_queue_enabled,
    queue_request,
    QUEUE_POLL_INTERVAL,
)


class TestQueueClientInit:
    """Tests for QueueClient initialization."""

    def test_init_with_defaults(self):
        """Test client initializes with default values."""
        client = QueueClient(
            base_url="http://localhost:8080",
            api_key="test-key",
        )
        assert client.base_url == "http://localhost:8080"
        assert client.api_key == "test-key"
        assert client.api_key_header == "X-Downloader-Key"
        assert client.poll_interval == QUEUE_POLL_INTERVAL
        assert client.timeout == 0  # 0 = wait forever

    def test_init_with_custom_values(self):
        """Test client initializes with custom values."""
        client = QueueClient(
            base_url="http://custom:9000/",  # trailing slash should be stripped
            api_key="custom-key",
            api_key_header="X-Custom-Key",
            poll_interval=0.5,
            timeout=60.0,
        )
        assert client.base_url == "http://custom:9000"  # trailing slash stripped
        assert client.api_key == "custom-key"
        assert client.api_key_header == "X-Custom-Key"
        assert client.poll_interval == 0.5
        assert client.timeout == 60.0


class TestCorrelationId:
    """Tests for correlation ID generation."""

    def test_correlation_id_deterministic(self):
        """Same inputs produce same correlation ID."""
        client = QueueClient("http://test:8080", "key")
        id1 = client._build_correlation_id("GET", "/v3/test", {"a": "1", "b": "2"})
        id2 = client._build_correlation_id("GET", "/v3/test", {"a": "1", "b": "2"})
        assert id1 == id2

    def test_correlation_id_different_params(self):
        """Different params produce different correlation ID."""
        client = QueueClient("http://test:8080", "key")
        id1 = client._build_correlation_id("GET", "/v3/test", {"a": "1"})
        id2 = client._build_correlation_id("GET", "/v3/test", {"a": "2"})
        assert id1 != id2

    def test_correlation_id_different_method(self):
        """Different method produces different correlation ID."""
        client = QueueClient("http://test:8080", "key")
        id1 = client._build_correlation_id("GET", "/v3/test", {"a": "1"})
        id2 = client._build_correlation_id("POST", "/v3/test", {"a": "1"})
        assert id1 != id2

    def test_correlation_id_param_order_doesnt_matter(self):
        """Parameter order shouldn't affect correlation ID."""
        client = QueueClient("http://test:8080", "key")
        id1 = client._build_correlation_id("GET", "/v3/test", {"a": "1", "b": "2"})
        id2 = client._build_correlation_id("GET", "/v3/test", {"b": "2", "a": "1"})
        assert id1 == id2


class TestRequestSubmission:
    """Tests for submitting requests to the queue."""

    @patch.object(requests.Session, 'post')
    def test_submit_request_success(self, mock_post):
        """Test successful request submission."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 5,
        }
        mock_post.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        request_id, status, was_pending = client.check_or_submit(
            method="GET",
            path="v3/stock/history/ohlc",
            query_params={"symbol": "AAPL", "start": "2024-01-01"},
        )

        assert request_id == "req-123"
        assert status == "pending"
        assert was_pending is False
        mock_post.assert_called_once()

    @patch.object(requests.Session, 'post')
    @patch.object(requests.Session, 'get')
    def test_idempotent_submission(self, mock_get, mock_post):
        """Test that same request returns existing request ID."""
        # First submission
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 5,
        }
        mock_post.return_value = mock_post_response

        # Status check returns still pending
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "request_id": "req-123",
            "status": "processing",
            "queue_position": 2,
        }
        mock_get.return_value = mock_get_response

        client = QueueClient("http://test:8080", "test-key")

        # First submission
        request_id1, status1, was_pending1 = client.check_or_submit(
            method="GET",
            path="v3/stock/history/ohlc",
            query_params={"symbol": "AAPL", "start": "2024-01-01"},
        )

        # Second submission with same params - should return existing
        request_id2, status2, was_pending2 = client.check_or_submit(
            method="GET",
            path="v3/stock/history/ohlc",
            query_params={"symbol": "AAPL", "start": "2024-01-01"},
        )

        assert request_id1 == request_id2
        assert was_pending1 is False
        assert was_pending2 is True
        # POST should only be called once
        assert mock_post.call_count == 1


class TestStatusTracking:
    """Tests for tracking request status."""

    def test_is_request_pending_no_requests(self):
        """Test is_request_pending returns False when no requests."""
        client = QueueClient("http://test:8080", "key")
        assert client.is_request_pending("nonexistent") is False

    @patch.object(requests.Session, 'post')
    def test_is_request_pending_after_submit(self, mock_post):
        """Test is_request_pending returns True after submission."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 1,
        }
        mock_post.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        client.check_or_submit("GET", "v3/test", {"symbol": "AAPL"})

        # Get the correlation ID
        correlation_id = client._build_correlation_id("GET", "v3/test", {"symbol": "AAPL"})
        assert client.is_request_pending(correlation_id) is True

    @patch.object(requests.Session, 'post')
    def test_get_pending_requests(self, mock_post):
        """Test get_pending_requests returns all pending requests."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 1,
        }
        mock_post.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        client.check_or_submit("GET", "v3/test", {"symbol": "AAPL"})
        client.check_or_submit("GET", "v3/test", {"symbol": "MSFT"})

        pending = client.get_pending_requests()
        assert len(pending) == 2

    @patch.object(requests.Session, 'post')
    def test_get_queue_stats(self, mock_post):
        """Test get_queue_stats returns correct counts."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 1,
        }
        mock_post.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        client.check_or_submit("GET", "v3/test", {"symbol": "AAPL"})

        stats = client.get_queue_stats()
        assert stats["total_tracked"] == 1
        assert stats["pending"] == 1
        assert stats["processing"] == 0
        assert stats["completed"] == 0


class TestResultRetrieval:
    """Tests for retrieving results from the queue."""

    @patch.object(requests.Session, 'get')
    def test_get_result_completed(self, mock_get):
        """Test getting result for completed request."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "completed",
            "result": {"data": [1, 2, 3]},
        }
        mock_get.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        result, status_code, status = client.get_result("req-123")

        assert result == {"data": [1, 2, 3]}
        assert status_code == 200
        assert status == "completed"

    @patch.object(requests.Session, 'get')
    def test_get_result_still_processing(self, mock_get):
        """Test getting result for still-processing request."""
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "processing",
        }
        mock_get.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        result, status_code, status = client.get_result("req-123")

        assert result is None
        assert status_code == 202
        assert status == "processing"

    @patch.object(requests.Session, 'get')
    def test_get_result_dead(self, mock_get):
        """Test getting result for dead (permanently failed) request."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "dead",
            "error": "Max retries exceeded",
        }
        mock_get.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        result, status_code, status = client.get_result("req-123")

        assert result is None
        assert status_code == 500
        assert status == "dead"


class TestWaitForResult:
    """Tests for waiting for request completion."""

    @patch.object(requests.Session, 'get')
    def test_wait_for_result_immediate_completion(self, mock_get):
        """Test wait_for_result when request completes immediately."""
        # Status check returns completed
        mock_status = MagicMock()
        mock_status.status_code = 200
        mock_status.json.return_value = {
            "request_id": "req-123",
            "status": "completed",
        }

        # Result returns data
        mock_result = MagicMock()
        mock_result.status_code = 200
        mock_result.json.return_value = {
            "result": {"price": 150.0},
        }

        mock_get.side_effect = [mock_status, mock_result]

        client = QueueClient("http://test:8080", "test-key")
        # Manually add to tracking
        client._pending_requests["test-corr"] = QueuedRequestInfo(
            request_id="req-123",
            correlation_id="test-corr",
            path="v3/test",
            status="pending",
        )
        client._request_id_to_correlation["req-123"] = "test-corr"

        result, status_code = client.wait_for_result("req-123", poll_interval=0.01)

        assert result == {"price": 150.0}
        assert status_code == 200

    @patch.object(requests.Session, 'get')
    def test_wait_for_result_timeout(self, mock_get):
        """Test wait_for_result raises TimeoutError."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 10,
        }
        mock_get.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        client._pending_requests["test-corr"] = QueuedRequestInfo(
            request_id="req-123",
            correlation_id="test-corr",
            path="v3/test",
            status="pending",
        )
        client._request_id_to_correlation["req-123"] = "test-corr"

        with pytest.raises(TimeoutError):
            client.wait_for_result("req-123", timeout=0.1, poll_interval=0.01)


class TestServerStats:
    """Tests for fetching server-side queue stats."""

    @patch.object(requests.Session, 'get')
    def test_fetch_server_queue_stats_success(self, mock_get):
        """Test fetching server stats succeeds."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pending_count": 5,
            "processing_count": 2,
            "completed_count": 100,
        }
        mock_get.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        stats = client.fetch_server_queue_stats()

        assert stats["pending_count"] == 5
        assert stats["processing_count"] == 2

    @patch.object(requests.Session, 'get')
    def test_fetch_server_queue_stats_error(self, mock_get):
        """Test fetching server stats handles errors."""
        mock_get.side_effect = requests.RequestException("Connection failed")

        client = QueueClient("http://test:8080", "test-key")
        stats = client.fetch_server_queue_stats()

        assert "error" in stats


class TestCleanup:
    """Tests for cleaning up old requests."""

    @patch.object(requests.Session, 'post')
    def test_cleanup_completed_removes_old(self, mock_post):
        """Test cleanup removes old completed requests."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "request_id": "req-123",
            "status": "pending",
            "queue_position": 1,
        }
        mock_post.return_value = mock_response

        client = QueueClient("http://test:8080", "test-key")
        client.check_or_submit("GET", "v3/test", {"symbol": "AAPL"})

        # Mark as completed and set old timestamp
        correlation_id = client._build_correlation_id("GET", "v3/test", {"symbol": "AAPL"})
        client._pending_requests[correlation_id].status = "completed"
        client._pending_requests[correlation_id].last_checked = time.time() - 7200  # 2 hours ago

        removed = client.cleanup_completed(max_age_seconds=3600)  # 1 hour

        assert removed == 1
        assert correlation_id not in client._pending_requests


class TestGlobalFunctions:
    """Tests for module-level functions."""

    def test_is_queue_enabled(self):
        """Test is_queue_enabled returns correct value."""
        # We set THETADATA_USE_QUEUE=true at module import
        assert is_queue_enabled() is True

    def test_get_queue_client_returns_singleton(self):
        """Test get_queue_client returns same instance."""
        client1 = get_queue_client()
        client2 = get_queue_client()
        assert client1 is client2

    @patch('lumibot.tools.thetadata_queue_client.QueueClient.execute_request')
    def test_queue_request_calls_client(self, mock_execute):
        """Test queue_request uses the client correctly."""
        mock_execute.return_value = ({"data": "test"}, 200)

        result = queue_request(
            url="http://test:8080/v3/stock/history/ohlc",
            querystring={"symbol": "AAPL"},
        )

        assert result == {"data": "test"}
        mock_execute.assert_called_once()


class TestThreadSafety:
    """Tests for thread safety."""

    @patch.object(requests.Session, 'post')
    def test_concurrent_submissions(self, mock_post):
        """Test that concurrent submissions are thread-safe."""
        counter = {"value": 0}

        def mock_post_fn(*args, **kwargs):
            counter["value"] += 1
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "request_id": f"req-{counter['value']}",
                "status": "pending",
                "queue_position": counter["value"],
            }
            return response

        mock_post.side_effect = mock_post_fn

        client = QueueClient("http://test:8080", "test-key")

        threads = []
        results = []

        def submit_request(symbol):
            try:
                request_id, status, _ = client.check_or_submit(
                    "GET", "v3/test", {"symbol": symbol}
                )
                results.append((symbol, request_id, status))
            except Exception as e:
                results.append((symbol, None, str(e)))

        # Create 10 threads submitting different symbols
        for i in range(10):
            t = threading.Thread(target=submit_request, args=(f"SYM{i}",))
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # All should succeed
        assert len(results) == 10
        assert all(r[1] is not None for r in results)


class TestQueuedRequestInfo:
    """Tests for QueuedRequestInfo dataclass."""

    def test_info_default_values(self):
        """Test QueuedRequestInfo has correct defaults."""
        info = QueuedRequestInfo(
            request_id="req-123",
            correlation_id="corr-456",
            path="v3/test",
            status="pending",
        )

        assert info.queue_position is None
        assert info.estimated_wait is None
        assert info.attempts == 0
        assert info.result is None
        assert info.error is None
        assert info.created_at > 0
        assert info.last_checked > 0
