"""Exercise the maintained SDK over HTTP without vendor credentials or paid data."""

import datetime
import json
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest
from massive.exceptions import AuthError, BadResponse

from lumibot.tools import polygon_helper as ph


@pytest.fixture
def polygon_http():
    responses = deque()
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((urlsplit(self.path), self.headers.get("Authorization")))
            status, payload = responses.popleft() if responses else (500, {"error": "Unexpected request"})
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = ph.PolygonClient.create(
            api_key="test-polygon-key", base=f"http://127.0.0.1:{server.server_port}", retries=0
        )
        try:
            yield client, responses, requests
        finally:
            server.shutdown()
            thread.join(timeout=5)
            client.client.clear()
            client.vx.client.clear()
            assert not thread.is_alive()


def test_polygon_defaults_keep_credentials_origin_and_tls(monkeypatch):
    monkeypatch.setattr(ph, "POLYGON_API_KEY", "test-existing-polygon-key")
    monkeypatch.setenv("MASSIVE_API_KEY", "test-unrelated-massive-key")
    for factory in (ph.PolygonClient, ph.PolygonClient.create):
        client = factory()
        assert client.API_KEY == "test-existing-polygon-key"
        assert client.BASE == client.vx.BASE == "https://api.polygon.io"
        assert client.client.connection_pool_kw["cert_reqs"] == "CERT_REQUIRED"
        client = factory("test-explicit-key", 1, 2, 3, 0, "http://127.0.0.1:1")
        assert client.API_KEY == "test-explicit-key"
        assert client.BASE == client.vx.BASE == "http://127.0.0.1:1"
    monkeypatch.setattr(ph, "POLYGON_API_KEY", None)
    with pytest.raises(AuthError, match="POLYGON_API_KEY"):
        ph.PolygonClient.create()


def test_aggregate_response_reaches_dataframe(polygon_http):
    client, responses, requests = polygon_http
    responses.append((200, {"results": [
        {"o": 100, "h": 105, "l": 99, "c": 103, "v": 42, "t": 1690896600000},
    ]}))
    bars = client.get_aggs("SPY", 1, "minute", "2023-08-01", "2023-08-01", limit=50000)
    frame = ph.update_polygon_data(None, bars)
    assert frame.iloc[0][["open", "high", "low", "close", "volume"]].tolist() == [100, 105, 99, 103, 42]
    assert frame.index[0] == pd.Timestamp("2023-08-01 13:30:00", tz="UTC")
    path, authorization = requests[0]
    assert path.path == "/v2/aggs/ticker/SPY/range/1/minute/2023-08-01/2023-08-01"
    assert parse_qs(path.query)["limit"] == ["50000"]
    assert authorization == "Bearer test-polygon-key"


def test_option_pagination_retains_filters_and_authorization(polygon_http):
    client, responses, requests = polygon_http
    contract = {
        "ticker": "O:SPY230804C00450000", "contract_type": "call",
        "expiration_date": "2023-08-04", "strike_price": 450,
        "shares_per_contract": 100, "primary_exchange": "XOPR",
    }
    responses.extend([
        (200, {"results": [contract], "next_url": f"{client.BASE}/v3/reference/options/contracts?cursor=page2"}),
        (200, {"results": [{**contract, "contract_type": "put", "ticker": "O:SPY230804P00450000"}]}),
    ])
    contracts = list(client.list_options_contracts(
        underlying_ticker="SPY", expiration_date_gte=datetime.date(2023, 8, 1),
        expired=True, limit=1000,
    ))
    assert [item.contract_type for item in contracts] == ["call", "put"]
    assert contracts[0].shares_per_contract == 100
    assert contracts[0].primary_exchange == "XOPR"
    assert contracts[0].expiration_date == "2023-08-04"
    assert contracts[0].strike_price == 450
    assert parse_qs(requests[0][0].query) == {
        "underlying_ticker": ["SPY"], "expiration_date.gte": ["2023-08-01"],
        "expired": ["true"], "limit": ["1000"],
    }
    assert parse_qs(requests[1][0].query) == {"cursor": ["page2"]}
    assert all(auth == "Bearer test-polygon-key" for _, auth in requests)


def test_split_response_retains_cache_fields(polygon_http):
    client, responses, requests = polygon_http
    responses.append((200, {"results": [{
        "ticker": "SPY", "execution_date": "2023-08-01", "split_from": 1, "split_to": 2,
    }]}))
    frame = pd.DataFrame(list(client.list_splits(ticker="SPY", sort="execution_date", order="asc")))
    assert frame.iloc[0][["ticker", "execution_date", "split_from", "split_to"]].tolist() == [
        "SPY", "2023-08-01", 1, 2,
    ]
    assert requests[0][0].path == "/v3/reference/splits"


def test_authorization_error_is_not_converted_to_empty_data(polygon_http):
    client, responses, _ = polygon_http
    responses.append((403, {"status": "NOT_AUTHORIZED", "message": "Invalid API key"}))
    with pytest.raises(BadResponse, match="Invalid API key"):
        client.get_aggs("SPY", 1, "minute", "2023-08-01", "2023-08-01")


def test_rate_limit_still_uses_polygon_retry_policy(polygon_http, monkeypatch):
    client, responses, requests = polygon_http
    responses.extend([(429, {"error": "Rate limit exceeded"}), (200, {"results": []})])
    monkeypatch.setenv("POLYGON_MAX_RETRY_ATTEMPTS", "1")
    monkeypatch.setenv("POLYGON_WAIT_SECONDS_RETRY", "1")
    sleeps = []
    monkeypatch.setattr(ph.time, "sleep", sleeps.append)
    assert client.get_aggs("SPY", 1, "minute", "2023-08-01", "2023-08-01") == []
    assert sleeps == [1]
    assert len(requests) == 2
