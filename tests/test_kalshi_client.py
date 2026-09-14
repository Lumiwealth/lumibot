"""Offline transport contracts: sign independently, never send real orders."""

import base64

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa

from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value


@pytest.fixture(scope="module")
def key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def pem(key):
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()


def test_signing_matches_public_key_and_excludes_query(key):
    client = KalshiClient({"API_KEY_ID": "unit-test", "PRIVATE_KEY": pem(key)}, clock=lambda: 1700000000.123)
    headers = client.auth_headers("get", "/trade-api/v2/portfolio/orders?limit=100")
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1700000000123"
    key.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        b"1700000000123GET/trade-api/v2/portfolio/orders",
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )
    client.close()


def test_inline_key_wins_over_path_and_supports_escaped_newlines(key):
    client = KalshiClient(
        {"API_KEY_ID": "test", "PRIVATE_KEY": pem(key).replace("\n", "\\n"), "PRIVATE_KEY_PATH": "does-not-exist"},
        require_auth=True,
    )
    assert client.is_demo
    assert client.auth_headers("POST", client.API_PATH + "/portfolio/events/orders")
    client.close()


def test_key_path_loading(key, tmp_path):
    path = tmp_path / "generated-test-key.pem"
    path.write_text(pem(key))
    client = KalshiClient({"API_KEY_ID": "test", "PRIVATE_KEY": None, "PRIVATE_KEY_PATH": str(path)}, require_auth=True)
    assert client.auth_headers("GET", client.WS_PATH)
    client.close()


@pytest.mark.parametrize(
    "config",
    [
        {"PRIVATE_KEY": "secret-do-not-echo"},
        {"PRIVATE_KEY_PATH": "private-do-not-echo-path", "PRIVATE_KEY": None},
        {"PRIVATE_KEY": pem(ec.generate_private_key(ec.SECP256R1()))},
    ],
)
def test_invalid_key_errors_do_not_disclose_inputs(config):
    with pytest.raises(ValueError) as error:
        KalshiClient(config)
    assert "secret-do-not-echo" not in str(error.value)
    assert "private-do-not-echo-path" not in str(error.value)


def test_missing_auth_is_explicit(monkeypatch):
    for name in ("API_KEY_ID", "PRIVATE_KEY", "PRIVATE_KEY_PATH"):
        monkeypatch.delenv("KALSHI_" + name, raising=False)
    with pytest.raises(ValueError, match="KALSHI_API_KEY_ID"):
        KalshiClient(require_auth=True)


@pytest.mark.parametrize(
    "value,result", [(True, True), (False, False), ("TRUE", True), ("false", False), (1, True), (0, False)]
)
def test_environment_selection(value, result):
    client = KalshiClient({"IS_DEMO": value})
    assert client.is_demo == result
    assert client.base_url == (client.DEMO_URL if result else client.PRODUCTION_URL)
    client.close()


@pytest.mark.parametrize("value", ["yes", "prod", "", None, 3])
def test_invalid_environment_fails_closed(value):
    with pytest.raises(ValueError, match="IS_DEMO"):
        KalshiClient({"IS_DEMO": value})


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", None, "bad"])
def test_non_finite_fields_rejected(value):
    with pytest.raises(ValueError, match="finite"):
        decimal_value(value)


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.parametrize("status", [401, 403, 409, 429, 500, 503])
def test_mutations_are_not_automatically_retried(method, status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"message": "private-account-data"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient(http_client=http)
        with pytest.raises(KalshiAPIError) as error:
            client.request(method, "/test", authenticated=False)
    assert len(calls) == 1
    assert error.value.status_code == status
    assert "private-account-data" not in str(error.value)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_safe_read_retries_are_bounded(status):
    calls, sleeps = [], []

    def handler(request):
        calls.append(request)
        return httpx.Response(status if len(calls) < 3 else 200, json={"balance": 100})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient(http_client=http, sleep=sleeps.append)
        assert client.request("GET", "/portfolio/balance", authenticated=False)["balance"] == 100
    assert len(calls) == 3
    assert sleeps == [0.25, 0.5]


@pytest.mark.parametrize("method,calls_expected", [("GET", 3), ("POST", 1)])
def test_transport_errors_do_not_leak_request(method, calls_expected):
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ConnectError("secret exception body", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient(http_client=http, sleep=lambda _: None)
        with pytest.raises(KalshiAPIError, match="transport") as error:
            client.request(method, "/test", authenticated=False)
    assert "secret" not in str(error.value)
    assert len(calls) == calls_expected


def test_pages_follow_cursor_and_reject_loops():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"orders": [{"order_id": len(calls)}], "cursor": "same"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient(http_client=http)
        iterator = client.pages("/orders", "orders", authenticated=False)
        assert next(iterator)["order_id"] == 1
        assert next(iterator)["order_id"] == 2
        with pytest.raises(KalshiAPIError, match="cursor"):
            next(iterator)
    assert calls[1].url.params["cursor"] == "same"


@pytest.mark.parametrize("body", [b"not-json", b"[]", b"null"])
def test_invalid_responses(body):
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=body))) as http:
        with pytest.raises(KalshiAPIError):
            KalshiClient(http_client=http).request("GET", "/test", authenticated=False)


@pytest.mark.parametrize("path", ["https://another-host/test", "//another-host?key=x", "/test?x=y", "test"])
def test_invalid_path_cannot_redirect_credentials(path):
    with pytest.raises(ValueError, match="path"):
        KalshiClient().request("GET", path)


def test_authenticated_request_uses_full_api_path(key):
    def handler(request):
        key.public_key().verify(
            base64.b64decode(request.headers["KALSHI-ACCESS-SIGNATURE"]),
            (request.headers["KALSHI-ACCESS-TIMESTAMP"] + "GET/trade-api/v2/portfolio/orders").encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
            hashes.SHA256(),
        )
        assert request.url.params["limit"] == "100"
        return httpx.Response(200, json={"orders": []})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient({"API_KEY_ID": "unit", "PRIVATE_KEY": pem(key)}, http_client=http)
        assert client.request("GET", "/portfolio/orders", params={"limit": 100}) == {"orders": []}
