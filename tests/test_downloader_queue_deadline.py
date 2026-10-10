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


def _replacement_session(monkeypatch, *, absent_forever=False, transient=False, result_lost=False):
    import json

    import requests

    clock = [0.0]
    monkeypatch.setattr(queue.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(queue.time, 'time', lambda: clock[0])
    monkeypatch.setattr(queue.time, 'sleep', lambda delay: clock.__setitem__(0, clock[0] + delay))
    calls = {'submits': [], 'statuses': 0}
    def response(status, data):
        result = requests.Response()
        result.status_code = status
        result._content = json.dumps(data).encode()
        return result
    def post(url, **kwargs):
        calls['submits'].append(kwargs['json'])
        return response(200, {'request_id': f"request-{len(calls['submits'])}", 'status': 'pending'})
    def get(url, **kwargs):
        if url.endswith('/result'):
            if result_lost and len(calls['submits']) == 1:
                return response(404, {'error': 'request not found'})
            return response(200, {'result': {'data': [{'o': 100, 'c': 101}]}})
        calls['statuses'] += 1
        if transient and calls['statuses'] == 1:
            return response(503, {'error': 'temporarily unavailable'})
        if not transient and not result_lost and (absent_forever or len(calls['submits']) == 1):
            return response(404, {'error': 'request not found'})
        return response(200, {'status': 'completed'})
    client = queue.QueueClient('http://localhost:8080', 'test-key', poll_interval=.1)
    monkeypatch.setattr(client, '_get_session', lambda: SimpleNamespace(post=post, get=get))
    return client, clock, calls


@pytest.mark.parametrize('result_lost', [False, True])
def test_lost_read_after_downloader_replacement_resubmits_without_full_timeout(monkeypatch, result_lost):
    client, clock, calls = _replacement_session(monkeypatch, result_lost=result_lost)
    result, status = client.execute_request(method='GET', path='ibkr/iserver/marketdata/history',
                                            query_params={'conid': '123'}, timeout=30, max_timeout_attempts=1)
    assert status == 200 and result == {'data': [{'o': 100, 'c': 101}]}
    assert len(calls['submits']) == 2
    assert calls['submits'][0] == calls['submits'][1], 'Keep the logical request idempotent'
    assert clock[0] < 2, 'Confirmed lost request must not consume a 30-second attempt'
    assert client._in_flight_count == 0


def test_transient_status_failure_does_not_duplicate_download(monkeypatch):
    client, _, calls = _replacement_session(monkeypatch, transient=True)
    client.execute_request(method='GET', path='ibkr/iserver/marketdata/history', query_params={},
                           timeout=30, max_timeout_attempts=1)
    assert len(calls['submits']) == 1


def test_repeated_queue_loss_retains_total_deadline(monkeypatch):
    client, clock, calls = _replacement_session(monkeypatch, absent_forever=True)
    with pytest.raises(TimeoutError):
        client.execute_request(method='GET', path='ibkr/iserver/marketdata/history', query_params={},
                               timeout=2, max_timeout_attempts=1)
    assert clock[0] <= 2
    assert 1 < len(calls['submits']) <= 3
    assert client._in_flight_count == 0


@pytest.mark.parametrize('result_lost', [False, True])
def test_confirmed_loss_is_not_automatically_replayed_for_non_read_methods(monkeypatch, result_lost):
    client, _, calls = _replacement_session(monkeypatch, result_lost=result_lost)
    with pytest.raises(queue.DownloaderQueueRequestLost):
        client.execute_request(method='POST', path='ibkr/example', query_params={},
                               timeout=2, max_timeout_attempts=1)
    assert len(calls['submits']) == 1
    assert client._in_flight_count == 0


def test_read_survives_actual_http_server_replacement(monkeypatch):
    """Exercise real HTTP sessions across replacement, without any broker login."""
    import json
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    accepted = threading.Event()
    submissions = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def respond(self, status, payload):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def do_POST(self):
            submissions.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            ident = 'before-replacement' if self.server.generation == 1 else 'after-replacement'
            self.respond(200, {'request_id': ident, 'status': 'pending'})
            accepted.set()
        def do_GET(self):
            if self.server.generation == 1:
                self.respond(200, {'status': 'processing'})
            elif 'before-replacement' in self.path:
                self.respond(404, {'error': 'request not found'})
            elif self.path.endswith('/result'):
                self.respond(200, {'result': {'data': [{'o': 100, 'c': 101}]}})
            else:
                self.respond(200, {'status': 'completed'})
    def start(port, generation):
        server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
        server.daemon_threads = True
        server.generation = generation
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        return server, thread
    server, thread = start(0, 1)
    port = server.server_port
    client = queue.QueueClient(f'http://127.0.0.1:{port}', 'synthetic-key', poll_interval=.05)
    executor = ThreadPoolExecutor(max_workers=1)
    started = time.monotonic()
    try:
        future = executor.submit(client.execute_request, method='GET',
                                 path='ibkr/iserver/marketdata/history', query_params={'conid': 'synthetic'},
                                 timeout=10, max_timeout_attempts=1)
        assert accepted.wait(3), 'Initial server must accept the request'
        server.shutdown()
        server.server_close()
        thread.join(1)
        server, thread = start(port, 2)
        result, status = future.result(timeout=6)
        assert status == 200 and result == {'data': [{'o': 100, 'c': 101}]}
        assert len(submissions) == 2 and submissions[0] == submissions[1]
        assert time.monotonic() - started < 6
        assert client._in_flight_count == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)
        executor.shutdown(wait=True)
