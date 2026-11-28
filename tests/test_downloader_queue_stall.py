"""A queued data request whose queue position never moves fails with a plain reason.

Why: a backtest's history request sat in the data downloader queue at the same position with no
attempt started for 28 minutes. The client waited out its 10-minute attempt timeout, submitted
again and waited again, with nothing shown to the person or the agent. A request whose position
has not moved for a bounded time now fails with a reason both can read.
"""

from types import SimpleNamespace

import pytest

from lumibot.tools import data_downloader_queue_client as queue


def _clock(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(queue, "time", SimpleNamespace(
        monotonic=lambda: clock[0], time=lambda: clock[0],
        sleep=lambda delay: clock.__setitem__(0, clock[0] + delay),
    ))
    return clock


def _pending(position, attempts=0, error_details=None):
    return queue.QueuedRequestInfo(
        request_id="request-1", correlation_id="corr-1", path="ibkr/iserver/marketdata/history",
        status="pending", queue_position=position, attempts=attempts, error_details=error_details,
    )


def test_position_that_never_moves_fails_with_a_plain_reason(monkeypatch):
    clock = _clock(monkeypatch)
    monkeypatch.setattr(queue, "QUEUE_STALL_SECONDS", 300.0)
    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=600, poll_interval=5)
    monkeypatch.setattr(client, "_refresh_status", lambda request_id: _pending(61))

    with pytest.raises(queue.DownloaderQueueStalled) as raised:
        client.wait_for_result("request-1")

    assert 300 <= clock[0] < 600
    message = str(raised.value)
    assert "position 61" in message
    assert "5 minutes" in message
    assert not isinstance(raised.value, TimeoutError)


def test_a_moving_queue_keeps_waiting_and_completes(monkeypatch):
    clock = _clock(monkeypatch)
    monkeypatch.setattr(queue, "QUEUE_STALL_SECONDS", 300.0)
    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=0, poll_interval=60)
    positions = iter(range(20, 0, -1))

    def refresh(request_id):
        position = next(positions, None)
        if position is None:
            info = _pending(None)
            info.status = "completed"
            return info
        return _pending(position)

    monkeypatch.setattr(client, "_refresh_status", refresh)
    monkeypatch.setattr(client, "get_result", lambda request_id: ({"data": [1]}, 200, "ok"))

    assert client.wait_for_result("request-1") == ({"data": [1]}, 200)
    assert clock[0] > 300


def test_provider_rate_limit_waits_are_not_a_stall(monkeypatch):
    clock = _clock(monkeypatch)
    monkeypatch.setattr(queue, "QUEUE_STALL_SECONDS", 300.0)
    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=900, poll_interval=60)
    monkeypatch.setattr(
        client, "_refresh_status",
        lambda request_id: _pending(3, error_details={"classification": "rate_limited"}),
    )

    with pytest.raises(queue.DownloaderQueueTimeout):
        client.wait_for_result("request-1")
    assert clock[0] > 900


def test_a_stalled_request_is_not_resubmitted(monkeypatch):
    _clock(monkeypatch)
    monkeypatch.setattr(queue, "QUEUE_STALL_SECONDS", 300.0)
    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=600, poll_interval=5)
    submits = []

    def submit(**kwargs):
        submits.append(kwargs)
        return "request-1", "pending", False

    monkeypatch.setattr(client, "check_or_submit", submit)
    monkeypatch.setattr(client, "_refresh_status", lambda request_id: _pending(61))

    with pytest.raises(queue.DownloaderQueueStalled):
        client.execute_request(method="GET", path="ibkr/iserver/marketdata/history", query_params={})
    assert len(submits) == 1
    assert client._in_flight_count == 0


def test_a_stall_seen_on_resubmit_check_is_shown_in_the_download_status(monkeypatch):
    """The stall reason must reach the backtest status, not be lost to a swallowed NameError."""
    import lumibot.tools.thetadata_helper as helper

    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=600, poll_interval=5)
    info = _pending(61)
    client._pending_requests["corr-1"] = info

    def stalled(request_id):
        raise queue.DownloaderQueueStalled("queue did not move")

    monkeypatch.setattr(client, "_refresh_status", stalled)
    recorded = []
    monkeypatch.setattr(helper, "update_download_status_queue_info", lambda **kw: recorded.append(kw))

    with pytest.raises(queue.DownloaderQueueStalled):
        client.check_or_submit("GET", info.path, {}, correlation_id_override="corr-1")
    assert recorded == [{"request_id": "request-1", "correlation_id": "corr-1", "last_error": "queue did not move"}]
