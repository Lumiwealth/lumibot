from dataclasses import dataclass

import pytest
from requests.exceptions import ConnectionError as RequestConnectionError

from lumibot.data_sources.interactive_brokers_rest_data import InteractiveBrokersRESTData


@dataclass
class _Response:
    payload: object
    status_code: int = 200

    @property
    def text(self):
        return "" if self.payload is None else str(self.payload)

    def json(self):
        return self.payload


class _Gateway:
    def __init__(self, base_url="https://gateway.example/v1/api"):
        self.base_url = base_url
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class _HttpClient:
    def __init__(self, *, authenticated=True):
        self.authenticated = authenticated
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        if url.endswith("/iserver/accounts"):
            payload = [] if self.authenticated else {"error": "not authenticated"}
            return _Response(payload)
        if url.endswith("/portfolio/accounts"):
            return _Response([{"id": "DU-PAPER-PLACEHOLDER"}])
        raise AssertionError(f"Unexpected GET {url}")

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if url.endswith("/iserver/questions/suppress"):
            return _Response({})
        raise AssertionError(f"Unexpected POST {url}")

    def delete(self, url, **kwargs):
        self.calls.append(("DELETE", url, kwargs))
        return _Response({})


class _FailingHttpClient:
    def get(self, url, **kwargs):
        raise RequestConnectionError('gateway said "not ready"')

    def post(self, url, **kwargs):
        raise RequestConnectionError('gateway said "not ready"')

    def delete(self, url, **kwargs):
        raise RequestConnectionError('gateway said "not ready"')


def test_rest_data_uses_injected_gateway_and_http_transport():
    gateway = _Gateway()
    client = _HttpClient()

    data = InteractiveBrokersRESTData(
        {
            "IB_ACCOUNT_ID": None,
            "AUTH_TIMEOUT": 5,
            "REQUEST_TIMEOUT": 7,
        },
        gateway=gateway,
        http_client=client,
    )

    assert gateway.started is True
    assert data.account_id == "DU-PAPER-PLACEHOLDER"
    assert data.base_url == "https://gateway.example/v1/api"
    assert all(call[2]["verify"] is True for call in client.calls)
    assert all(call[2]["timeout"] == 7 for call in client.calls)

    data.stop()
    assert gateway.stopped is True


def test_rest_data_authentication_timeout_stops_owned_gateway():
    gateway = _Gateway("https://localhost:4234/v1/api")
    client = _HttpClient(authenticated=False)
    clock = {"now": 0.0}

    def monotonic():
        return clock["now"]

    def sleep(seconds):
        clock["now"] += seconds

    with pytest.raises(TimeoutError, match="IB_AUTH_TIMEOUT"):
        InteractiveBrokersRESTData(
            {
                "IB_ACCOUNT_ID": None,
                "AUTH_TIMEOUT": 2,
                "AUTH_POLL_INTERVAL": 1,
            },
            gateway=gateway,
            http_client=client,
            monotonic_fn=monotonic,
            sleep_fn=sleep,
        )

    assert gateway.started is True
    assert gateway.stopped is True
    assert clock["now"] == 2


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("get_from_endpoint", ("https://gateway.example/get",)),
        ("post_to_endpoint", ("https://gateway.example/post", {})),
        ("delete_to_endpoint", ("https://gateway.example/delete",)),
    ],
)
def test_rest_transport_synthesizes_valid_json_for_quoted_request_errors(method, args):
    data = InteractiveBrokersRESTData(
        {"IB_ACCOUNT_ID": None},
        gateway=_Gateway(),
        http_client=_HttpClient(),
    )
    data.http_client = _FailingHttpClient()
    data.handle_http_errors = lambda response, *_args: (
        False,
        None,
        False,
        response.json(),
    )

    result = getattr(data, method)(*args, max_retries=0)

    assert result == {"error": 'gateway said "not ready"'}
