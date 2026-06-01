from types import SimpleNamespace

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
