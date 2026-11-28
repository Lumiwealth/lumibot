"""ProjectX (TopstepX and other firms) answers bursts with HTTP 429.

Read calls must wait and try again a few times. Order writes (place, modify,
cancel) must never be sent twice. When reads still fail, the broker must keep
the real reason so the customer is not told only "no account ID available".
"""

from unittest.mock import Mock, patch

import pytest
import requests

from lumibot.tools.projectx_helpers import ProjectX


CONFIG = {
    "firm": "TEST",
    "api_key": "k",
    "username": "u",
    "base_url": "https://test.projectx.com",
}


def _response(status, body=b'{"success":true,"accounts":[{"id":7,"name":"P","balance":1}]}', headers=None):
    response = requests.Response()
    response.status_code = status
    response.reason = "Too Many Requests" if status == 429 else "OK"
    response._content = body
    if headers:
        response.headers.update(headers)
    return response


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("lumibot.tools.projectx_helpers.time.sleep") as sleep:
        yield sleep


def test_account_search_retries_429_then_succeeds(no_sleep):
    api = ProjectX(CONFIG, "token")
    with patch("requests.post", side_effect=[_response(429, b"{}"), _response(200)]) as post:
        result = api.account_search()
    assert result["success"] is True
    assert post.call_count == 2
    assert no_sleep.call_count >= 1


def test_retry_after_header_is_honored_but_capped(no_sleep):
    api = ProjectX(CONFIG, "token")
    with patch("requests.post", side_effect=[_response(429, b"{}", {"Retry-After": "120"}), _response(200)]):
        api.account_search()
    waits = [call.args[0] for call in no_sleep.call_args_list if call.args]
    assert max(waits) <= 10


def test_account_search_gives_up_after_a_few_tries_and_keeps_the_reason():
    api = ProjectX(CONFIG, "token")
    with patch("requests.post", return_value=_response(429, b"{}")) as post:
        result = api.account_search()
    assert result["success"] is False
    assert "429" in result["error"]
    assert "Too Many Requests" in result["error"]
    assert 2 <= post.call_count <= 4


@pytest.mark.parametrize("method,args", [
    ("position_search_open", (7,)),
    ("order_search", (7, "2026-11-28T00:00:00Z")),
    ("trade_search", (7, "2026-11-28T00:00:00Z")),
    ("contract_search_id", ("CON.F.US.MES.Z26",)),
    ("contract_search", ("MES",)),
])
def test_other_reads_retry_429(method, args):
    api = ProjectX(CONFIG, "token")
    ok = _response(200, b'{"success":true,"positions":[],"orders":[],"trades":[],"contracts":[]}')
    with patch("requests.post", side_effect=[_response(429, b"{}"), ok]) as post:
        result = getattr(api, method)(*args)
    assert result["success"] is True
    assert post.call_count == 2


@pytest.mark.parametrize("method,args", [
    ("order_place", (7, "CON.F.US.MES.Z26", 1, 0, 1)),
    ("order_cancel", (7, 99)),
    ("order_modify", (7, 99, 1)),
])
def test_order_writes_are_never_retried_on_429(method, args):
    api = ProjectX(CONFIG, "token")
    with patch("requests.post", return_value=_response(429, b'{"success":false}')) as post:
        getattr(api, method)(*args)
    assert post.call_count == 1


def test_connect_failure_keeps_the_real_reason_for_the_customer():
    from lumibot.brokers.projectx import ProjectX as ProjectXBroker

    broker = ProjectXBroker.__new__(ProjectXBroker)
    broker.logger = Mock()
    broker.firm = "TOPSTEPX"
    broker.connect_stream = False
    broker.account_id = None
    broker.client = Mock()
    broker.client.get_preferred_account_id.side_effect = Exception(
        "Failed to retrieve accounts: {'success': False, 'error': 'HTTP 429 Too Many Requests'}"
    )
    assert broker.connect() is False
    assert "429" in broker.last_connect_error
    with pytest.raises(Exception) as excinfo:
        broker._get_balances_at_broker(None, None)
    assert "no account ID available" in str(excinfo.value)
    assert "429" in str(excinfo.value)
