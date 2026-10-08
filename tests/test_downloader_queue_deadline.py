from types import SimpleNamespace

import pytest

from lumibot.tools import data_downloader_queue_client as queue


def test_execute_deadline_includes_queue_full_submission(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(queue.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(queue.time, "time", lambda: clock[0])
    monkeypatch.setattr(queue.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    client = queue.QueueClient("http://localhost:8080", "test-key", timeout=30)
    calls = []
    def post(*a, **kwargs):
        calls.append(kwargs)
        if len(calls) > 10:
            raise AssertionError("submission ignored the overall deadline")
        return SimpleNamespace(json=lambda: {"error": "queue_full", "retry_after": 60}, headers={})
    monkeypatch.setattr(client, "_get_session", lambda: SimpleNamespace(post=post))
    with pytest.raises(TimeoutError):
        client.execute_request(method="GET", path="ibkr/tws/secdef/contracts", query_params={},
                               timeout=5, max_timeout_attempts=1)
    assert clock[0] == 5
    assert len(calls) == 1
    assert sum(calls[0]["timeout"]) <= 5
    assert client._in_flight_count == 0


def test_result_wait_receives_remaining_budget_after_submission(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(queue.time, "monotonic", lambda: clock[0])
    client = queue.QueueClient("http://localhost:8080", "test-key")
    def submit(**kwargs):
        clock[0] += 4
        return "request", "pending", False
    waits = []
    def wait(**kwargs):
        waits.append(kwargs["timeout"])
        return {"data": [1]}, 200
    monkeypatch.setattr(client, "check_or_submit", submit)
    monkeypatch.setattr(client, "wait_for_result", wait)
    client.execute_request(method="GET", path="ibkr/iserver/marketdata/history", query_params={},
                           timeout=5, max_timeout_attempts=1)
    assert waits == [1]
