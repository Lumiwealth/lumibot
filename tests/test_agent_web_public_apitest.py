import pytest

from lumibot.components.agents.web_tools import WebClient

pytestmark = [pytest.mark.apitest, pytest.mark.public_http]


def test_public_http_transport_reaches_an_owned_stable_example_endpoint():
    client = WebClient(timeout_seconds=15, max_response_bytes=100_000)
    try:
        result = client.request("GET", "https://example.com/")
    finally:
        client.close()

    assert result["ok"] is True
    assert result["status_code"] == 200
    assert result["url"] == "https://example.com/"
    assert "Example Domain" in result["text"]
