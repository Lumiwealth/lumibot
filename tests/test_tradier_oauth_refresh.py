from types import SimpleNamespace
import json

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


def test_disabled_oauth_refresh_reloads_token_file_and_retries_auth_error(tmp_path):
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
    broker._disable_token_refresh = True
    broker._oauth_token_path = str(token_path)
    broker._oauth_token_path_signature = broker._oauth_token_file_signature()
    broker._oauth_refresh_token = "old-refresh"
    broker._oauth_token_expires_at = None
    broker._tradier_access_token = "old-access"
    broker.tradier = SimpleNamespace(account=component, orders=None, market=None)
    broker.data_source = None

    def fail_refresh(*, force=False):
        raise AssertionError("Disabled token refresh must not call provider refresh")

    broker._refresh_oauth_token = fail_refresh

    broker._install_oauth_refresh_hooks()

    assert broker.tradier.account.request("v1/accounts/123/balances") == {
        "ok": True,
        "endpoint": "v1/accounts/123/balances",
    }
    assert attempts == 2
    assert broker._tradier_access_token == "new-access"
    assert broker._oauth_refresh_token == "new-refresh"
    assert component.AUTH_TOKEN == "new-access"
    assert component.REQUESTS_HEADERS["Authorization"] == "Bearer new-access"
