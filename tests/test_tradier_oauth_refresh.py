from types import SimpleNamespace
import json
import os

import requests

from lumibot.brokers.tradier import Tradier


def test_oauth_refresh_hook_retries_retry_library_401_error():
    broker = Tradier.__new__(Tradier)
    broker._oauth_token_payload_b64 = "present"
    refresh_calls = []

    def refresh_token(*, force=False):
        refresh_calls.append(force)
        return force

    attempts = 0

    def request(endpoint, params=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise requests.exceptions.RetryError("too many 401 error responses")
        return {"ok": True, "endpoint": endpoint, "params": params}

    component = SimpleNamespace(request=request)
    broker._refresh_oauth_token = refresh_token
    broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
    broker.data_source = None

    broker._install_oauth_refresh_hooks()

    assert broker.tradier.account.request("v1/accounts/123/balances", params={"a": "b"}) == {
        "ok": True,
        "endpoint": "v1/accounts/123/balances",
        "params": {"a": "b"},
    }
    assert attempts == 2
    assert refresh_calls == [False, True]


def test_oauth_refresh_hook_does_not_retry_non_auth_error():
    broker = Tradier.__new__(Tradier)
    broker._oauth_token_payload_b64 = "present"
    refresh_calls = []

    def refresh_token(*, force=False):
        refresh_calls.append(force)
        return True

    def request(endpoint):
        raise RuntimeError("network exploded")

    component = SimpleNamespace(request=request)
    broker._refresh_oauth_token = refresh_token
    broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
    broker.data_source = None

    broker._install_oauth_refresh_hooks()

    try:
        broker.tradier.account.request("v1/accounts/123/balances")
    except RuntimeError as exc:
        assert str(exc) == "network exploded"
    else:
        raise AssertionError("Expected non-auth error to propagate")
    assert refresh_calls == [False]


def test_external_oauth_refresh_mode_reloads_token_file_and_retries_auth_error(tmp_path):
    token_path = tmp_path / "tradier_token.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "old-access",
                "refresh_token": "old-refresh",
                "expires_in": 86400,
                "issued_at": 1,
            }
        ),
        encoding="utf-8",
    )

    attempts = 0
    component = SimpleNamespace(
        AUTH_TOKEN="old-access",
        REQUESTS_HEADERS={"Authorization": "Bearer old-access"},
    )

    def request(endpoint):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            token_path.write_text(
                json.dumps(
                    {
                        "access_token": "new-access",
                        "refresh_token": "new-refresh",
                        "expires_in": 86400,
                        "issued_at": 2,
                    }
                ),
                encoding="utf-8",
            )
            raise requests.exceptions.RetryError("too many 401 error responses")
        return {"ok": True, "endpoint": endpoint}

    component.request = request

    broker = Tradier.__new__(Tradier)
    broker._oauth_token_payload_b64 = "present"
    broker._oauth_refresh_mode = "external"
    broker._oauth_token_path = str(token_path)
    broker._oauth_token_path_signature = broker._oauth_token_file_signature()
    broker._oauth_refresh_token = "old-refresh"
    broker._oauth_token_expires_at = None
    broker._tradier_access_token = "old-access"
    broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
    broker.data_source = None

    def fail_refresh(*, force=False):
        raise AssertionError("External OAuth refresh mode must not call provider refresh")

    broker._refresh_oauth_token = fail_refresh

    broker._install_oauth_refresh_hooks()

    assert broker.tradier.account.request("v1/accounts/123/balances") == {
        "ok": True,
        "endpoint": "v1/accounts/123/balances",
    }
    assert attempts == 2
    assert broker._tradier_access_token == "new-access"
    assert broker._oauth_refresh_token is None
    assert "refresh_token" not in broker._decode_base64url_json(broker._oauth_token_payload_b64)
    assert component.AUTH_TOKEN == "new-access"
    assert component.REQUESTS_HEADERS["Authorization"] == "Bearer new-access"


def test_external_oauth_refresh_mode_strips_initial_refresh_token_and_never_calls_provider_refresh(tmp_path):
    token_path = tmp_path / "tradier_token.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "old-access",
                "refresh_token": "stale-refresh-must-not-survive",
                "expires_in": 86400,
                "issued_at": 1,
            }
        ),
        encoding="utf-8",
    )

    component = SimpleNamespace(
        AUTH_TOKEN="old-access",
        REQUESTS_HEADERS={"Authorization": "Bearer old-access"},
    )
    attempts = 0

    def request(endpoint):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            token_path.write_text(
                json.dumps(
                    {
                        "access_token": "new-access",
                        "expires_in": 86400,
                        "issued_at": 2,
                    }
                ),
                encoding="utf-8",
            )
            raise requests.exceptions.RetryError("too many 401 error responses")
        return {"ok": True, "endpoint": endpoint}

    component.request = request

    broker = Tradier.__new__(Tradier)
    broker._oauth_token_payload_b64 = "present"
    broker._oauth_refresh_mode = "external"
    broker._oauth_token_path = str(token_path)
    broker._oauth_token_path_signature = broker._oauth_token_file_signature()
    broker._oauth_refresh_token = "stale-refresh-must-not-survive"
    broker._oauth_token_expires_at = None
    broker._tradier_access_token = "old-access"
    broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
    broker.data_source = None

    assert broker._apply_oauth_token_json(Tradier._load_token_json_from_path(token_path))
    assert broker._oauth_refresh_token is None
    assert "refresh_token" not in broker._decode_base64url_json(broker._oauth_token_payload_b64)

    def fail_refresh(*, force=False):
        raise AssertionError("External OAuth refresh mode must not call provider refresh")

    broker._refresh_oauth_token = fail_refresh
    broker._oauth_token_path_signature = broker._oauth_token_file_signature()
    broker._install_oauth_refresh_hooks()

    assert broker.tradier.account.request("v1/accounts/123/balances") == {
        "ok": True,
        "endpoint": "v1/accounts/123/balances",
    }
    assert attempts == 2
    assert broker._tradier_access_token == "new-access"
    assert broker._oauth_refresh_token is None
    assert "refresh_token" not in broker._decode_base64url_json(broker._oauth_token_payload_b64)


def test_external_oauth_refresh_mode_multiple_brokers_reload_atomic_replacements(tmp_path):
    token_path = tmp_path / "tradier_token.json"

    def write_token(access_token):
        payload = {
            "access_token": access_token,
            "expires_in": 86400,
            "issued_at": 1,
        }
        tmp_file = token_path.with_name(f".{token_path.name}.{access_token}.tmp")
        tmp_file.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp_file, token_path)

    write_token("access-0")

    brokers = []
    components = []
    for _ in range(3):
        component = SimpleNamespace(
            AUTH_TOKEN="access-0",
            REQUESTS_HEADERS={"Authorization": "Bearer access-0"},
        )
        component.request_calls = 0

        def request(endpoint, *, _component=component):
            _component.request_calls += 1
            return {"ok": True, "endpoint": endpoint, "token": _component.AUTH_TOKEN}

        component.request = request

        broker = Tradier.__new__(Tradier)
        broker._oauth_token_payload_b64 = "present"
        broker._oauth_refresh_mode = "external"
        broker._oauth_token_path = str(token_path)
        broker._oauth_token_path_signature = broker._oauth_token_file_signature()
        broker._oauth_refresh_token = "stale-refresh-must-not-survive"
        broker._oauth_token_expires_at = None
        broker._tradier_access_token = "access-0"
        broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
        broker.data_source = None
        assert broker._apply_oauth_token_json(Tradier._load_token_json_from_path(token_path))
        broker._oauth_token_path_signature = broker._oauth_token_file_signature()

        def fail_refresh(*, force=False):
            raise AssertionError("External OAuth refresh mode must not call provider refresh")

        broker._refresh_oauth_token = fail_refresh
        broker._install_oauth_refresh_hooks()
        brokers.append(broker)
        components.append(component)

    for cycle in range(1, 4):
        write_token(f"access-{cycle}")
        for broker, component in zip(brokers, components):
            assert broker.tradier.account.request("v1/accounts/123/balances") == {
                "ok": True,
                "endpoint": "v1/accounts/123/balances",
                "token": f"access-{cycle}",
            }
            assert broker._tradier_access_token == f"access-{cycle}"
            assert broker._oauth_refresh_token is None
            assert "refresh_token" not in broker._decode_base64url_json(broker._oauth_token_payload_b64)
            assert component.REQUESTS_HEADERS["Authorization"] == f"Bearer access-{cycle}"
