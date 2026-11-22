"""Replay checks for Schwab rate limits (HTTP 429 + Retry-After) and for an
access token that Schwab rejects mid-run (HTTP 401).

Neutral synthetic symbols and recorded-shape Schwab answers only. No broker
credentials, no network.
"""

import importlib.util
import json
import time
from collections import deque
from datetime import date
from email.utils import formatdate
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from lumibot.data_sources.schwab_data import SchwabData, _schwab_market_symbol
from lumibot.entities import Asset, Order

SPEC = importlib.util.spec_from_file_location(
    "schwab_lifecycle_transport",
    Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py",
)
TRANSPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "false")

    def reject(*_args, **_kwargs):
        raise AssertionError("External network is forbidden in this fixture")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def _call():
    return Asset("LWX", asset_type="option", expiration=date(2026, 12, 18), strike=50, right="CALL")


class _QuoteClient:
    """Replays recorded-shape quote responses in order; the last one repeats."""

    def __init__(self, *responses):
        self.responses = deque(responses)
        self.calls = []

    def get_quotes(self, symbols):
        self.calls.append(list(symbols))
        status, headers, payload = self.responses[0] if len(self.responses) == 1 else self.responses.popleft()
        return SimpleNamespace(status_code=status, headers=headers, json=lambda: payload)


def _quote_source(*responses):
    client = _QuoteClient(*responses)
    data = SchwabData(client=client, auto_create_client=False)
    clock = {"value": 1_000.0}
    data._schwab_now = lambda: clock["value"]
    return data, client, clock


def _ok_payload(*assets):
    return {
        _schwab_market_symbol(a): {"symbol": _schwab_market_symbol(a), "quote": {"bidPrice": 1.0, "askPrice": 1.1}}
        for a in assets
    }


# --- Gap 1: quotes honor Retry-After --------------------------------------


def test_quote_429_retry_after_seconds_blocks_calls_until_deadline():
    stock, option = Asset("LW01"), _call()
    data, client, clock = _quote_source(
        (429, {"Retry-After": "2"}, {}),
        (200, {}, _ok_payload(stock, option)),
    )

    with pytest.raises(ConnectionError, match="429"):
        data.get_quotes([stock, option])
    assert len(client.calls) == 1

    # Inside the cooldown: visible failure, no request, no invented quote.
    clock["value"] += 1.0
    with pytest.raises(ConnectionError, match="rate limit"):
        data.get_quotes([stock, option])
    assert data.get_quote(stock) is None
    assert len(client.calls) == 1

    # After the cooldown the next call goes out and succeeds.
    clock["value"] += 1.5
    result = data.get_quotes([stock, option])
    assert len(client.calls) == 2
    assert result[stock].bid == 1.0 and result[option].ask == 1.1


def test_quote_429_retry_after_http_date_is_honored():
    stock = Asset("LW01")
    retry_at = formatdate(time.time() + 5, usegmt=True)
    data, client, clock = _quote_source(
        (429, {"retry-after": retry_at}, {}),
        (200, {}, _ok_payload(stock)),
    )

    with pytest.raises(ConnectionError, match="429"):
        data.get_quotes([stock])
    clock["value"] += 3.0
    with pytest.raises(ConnectionError, match="rate limit"):
        data.get_quotes([stock])
    assert len(client.calls) == 1

    clock["value"] += 3.5
    assert data.get_quotes([stock])[stock] is not None
    assert len(client.calls) == 2


def test_quote_429_retry_after_is_bounded():
    stock = Asset("LW01")
    data, client, clock = _quote_source(
        (429, {"Retry-After": "3600"}, {}),
        (200, {}, _ok_payload(stock)),
    )
    with pytest.raises(ConnectionError, match="429"):
        data.get_quotes([stock])
    clock["value"] += 61.0
    assert data.get_quotes([stock])[stock] is not None
    assert len(client.calls) == 2


# --- Gap 1: order submit honors Retry-After -------------------------------


class _RateLimitedSubmitTransport(TRANSPORT.SchwabLifecycleTransport):
    """First place_order answers 429 with Retry-After; later ones are accepted."""

    def __init__(self, retry_after="30"):
        super().__init__()
        self._retry_after = retry_after
        self._limited_once = False

    def place_order(self, account_hash, order_spec):
        if self._limited_once:
            return super().place_order(account_hash, order_spec)
        self._limited_once = True
        self._check_account(account_hash)
        spec = order_spec.build() if hasattr(order_spec, "build") else order_spec
        body = {"message": "Too many requests"}
        with self._lock:
            self._record("place_order", None, order_spec=spec, status_code=429, response_body=body)
            self._attempts.append({"order_id": None, "order_spec": spec, "status_code": 429})
        return httpx.Response(
            429,
            json=body,
            headers={"Retry-After": self._retry_after},
            request=httpx.Request("POST", "https://schwab-fixture.invalid/trader/v1/accounts/fixture-account/orders"),
        )


def _limit_buy():
    return Order(
        strategy="fixture",
        asset=Asset("LW"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=10.0,
    )


def _place_calls(transport):
    return [c for c in transport.calls if c.get("method") == "place_order"]


def _error_events(queues, broker):
    events = []
    while not queues.priority_queue.empty():
        event = queues.priority_queue.get_nowait()
        if event[0] == broker.ERROR_ORDER:
            events.append(event)
    return events


def test_order_submit_inside_retry_after_cooldown_is_rejected_visibly_without_api_call():
    transport = _RateLimitedSubmitTransport(retry_after="30")
    broker, queues = TRANSPORT.data_only_schwab_broker(transport)
    clock = {"value": 500.0}
    broker._schwab_now = lambda: clock["value"]

    first = _limit_buy()
    assert broker.submit_order(first) is None
    TRANSPORT.drain_stream_events(broker)
    assert first.status == Order.OrderStatus.ERROR
    assert "429" in str(first.error_message)
    assert len(_place_calls(transport)) == 1
    assert len(_error_events(queues, broker)) == 1

    # Inside the cooldown: the order is not sent and not silently dropped.
    clock["value"] += 1.0
    second = _limit_buy()
    assert broker.submit_order(second) is None
    TRANSPORT.drain_stream_events(broker)
    assert len(_place_calls(transport)) == 1
    assert second.status == Order.OrderStatus.ERROR
    assert "rate limit" in str(second.error_message).lower()
    assert "retry after" in str(second.error_message).lower()
    events = _error_events(queues, broker)
    assert len(events) == 1 and "rate limit" in str(events[0][1]["error"]).lower()

    # After the cooldown the next order goes out normally.
    clock["value"] += 30.0
    third = _limit_buy()
    submitted = broker.submit_order(third)
    assert len(_place_calls(transport)) == 2
    assert submitted is third and third.identifier


# --- Gap 2: a 401 mid-run triggers one refresh and one retry ---------------


class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeSession:
    auto_refresh_url = "https://api.schwabapi.com/v1/oauth/token"

    def __init__(self, token, statuses, refresh_result):
        self.token = token
        self.statuses = deque(statuses)
        self.refresh_result = refresh_result
        self.request_calls = []
        self.refresh_calls = []

    def request(self, *args, **kwargs):
        self.request_calls.append(self.token.get("access_token"))
        return _Resp(self.statuses.popleft() if len(self.statuses) > 1 else self.statuses[0])

    def refresh_token(self, url, refresh_token=None, **kwargs):
        self.refresh_calls.append(refresh_token)
        if isinstance(self.refresh_result, Exception):
            raise self.refresh_result
        return dict(self.refresh_result)


def _wrap(session, token):
    from lumibot.brokers.schwab import _install_schwab_unauthorized_refresh

    def _update(new_token):
        token.update(new_token)
        session.token = token

    _install_schwab_unauthorized_refresh(session, token, {"client_id": "app"}, _update)
    return session


def test_401_mid_run_refreshes_once_and_retries_once():
    token = {"access_token": "access-1", "refresh_token": "refresh-1"}
    session = _wrap(_FakeSession(token, [401, 200], {"access_token": "access-2", "refresh_token": "refresh-2"}), token)

    assert session.request("GET", "https://api.schwab.test/quotes").status_code == 200
    assert session.refresh_calls == ["refresh-1"]
    assert session.request_calls == ["access-1", "access-2"]


def test_401_mid_run_with_failing_refresh_surfaces_401_without_loop():
    token = {"access_token": "access-1", "refresh_token": "refresh-1"}
    session = _wrap(_FakeSession(token, [401], RuntimeError("invalid_grant")), token)

    assert session.request("GET", "https://api.schwab.test/quotes").status_code == 401
    assert session.refresh_calls == ["refresh-1"]
    assert session.request_calls == ["access-1"]


def test_401_that_persists_after_refresh_is_not_retried_again():
    token = {"access_token": "access-1", "refresh_token": "refresh-1"}
    session = _wrap(_FakeSession(token, [401], {"access_token": "access-2", "refresh_token": "refresh-2"}), token)

    assert session.request("GET", "https://api.schwab.test/quotes").status_code == 401
    assert session.refresh_calls == ["refresh-1"]
    assert session.request_calls == ["access-1", "access-2"]


def _write_token(path, access="access-1", refresh="refresh-1"):
    issued_at = int(time.time() * 1000)
    token = {
        "access_token": access,
        "issued_at": issued_at,
        "expires_in": 1800,
        "token_type": "Bearer",
        "scope": "api",
    }
    if refresh:
        token.update(refresh_token=refresh, refresh_token_issued_at=issued_at, refresh_token_expires_in=7776000)
    path.write_text(json.dumps({"creation_timestamp": 1, "token": token}), encoding="utf-8")


def _patch_init(monkeypatch, session_cls):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", session_cls)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)
    return schwab_module


def test_internal_refresh_mode_broker_session_refreshes_on_401(monkeypatch, tmp_path):
    token_path = tmp_path / "schwab_token.json"
    _write_token(token_path)

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.token = token
            self.kwargs = kwargs
            self.auto_refresh_url = kwargs.get("auto_refresh_url")
            self.request_tokens = []
            self.refresh_calls = 0

        def register_compliance_hook(self, hook_type, hook):
            return None

        def refresh_token(self, url, refresh_token=None, **kwargs):
            self.refresh_calls += 1
            assert refresh_token == "refresh-1"
            return {"access_token": "access-2", "refresh_token": "refresh-2", "expires_in": 1800}

        def request(self, *args, **kwargs):
            self.request_tokens.append(self.token.get("access_token"))
            return _Resp(401 if len(self.request_tokens) == 1 else 200)

    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    schwab_module = _patch_init(monkeypatch, _OAuth2Session)
    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )
    try:
        session = broker.client.session
        assert session.request("GET", "https://api.schwab.test/quotes").status_code == 200
        assert session.refresh_calls == 1
        assert session.request_tokens == ["access-1", "access-2"]
        saved = json.loads(token_path.read_text(encoding="utf-8"))
        assert saved["token"]["access_token"] == "access-2"
    finally:
        stop = getattr(broker, "_schwab_token_refresh_stop", None)
        if stop is not None:
            stop.set()


def test_external_refresh_mode_401_with_unchanged_file_force_reapplies_once(monkeypatch, tmp_path):
    token_path = tmp_path / "schwab_token.json"
    _write_token(token_path, refresh=None)

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.token = token
            self.request_calls = 0
            self._client = SimpleNamespace(access_token=token.get("access_token"), expires_at=0)

        def register_compliance_hook(self, hook_type, hook):
            pytest.fail("External OAuth refresh mode must not install provider refresh hooks")

        def refresh_token(self, *args, **kwargs):
            pytest.fail("External OAuth refresh mode must not call Schwab refresh_token")

        def request(self, *args, **kwargs):
            self.request_calls += 1
            return _Resp(401 if self.request_calls == 1 else 200)

    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "external")
    schwab_module = _patch_init(monkeypatch, _OAuth2Session)
    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )
    session = broker.client.session
    assert session.request("GET", "https://api.schwab.test/quotes").status_code == 200
    assert session.request_calls == 2
