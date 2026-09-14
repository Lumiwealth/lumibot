"""Regressions for the explicit FOK rejection observed on the Demo API."""

import httpx
import pytest

from lumibot.entities import Order
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient
from tests import test_kalshi_apitest as demo_tests
from tests.test_kalshi_broker import ASSET, ContractStrategy, order
from tests.test_kalshi_broker import broker as _broker_fixture

FOK_CODE = "fill_or_kill_insufficient_resting_volume"


@pytest.fixture
def broker():
    yield from _broker_fixture.__wrapped__()


@pytest.mark.parametrize("nested", [False, True])
def test_transport_retains_only_allowlisted_fok_code(nested):
    error = {"code": FOK_CODE, "message": "private-account-data", "details": "secret-detail"}
    body = {"error": error} if nested else error
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(409, json=body)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = KalshiClient(http_client=http)
        with pytest.raises(KalshiAPIError) as caught:
            client.request("POST", "/portfolio/events/orders", authenticated=False)
    assert caught.value.error_code == FOK_CODE
    assert caught.value.status_code == 409
    assert len(calls) == 1
    assert "private-account-data" not in str(caught.value)
    assert "secret-detail" not in str(caught.value)
    assert "private-account-data" not in repr(vars(caught.value))


@pytest.mark.parametrize(
    "body",
    [
        {"code": "private-account-data"},
        {"error": {"code": "duplicate_order_id"}},
        {"code": [FOK_CODE]},
        {"error": "private-account-data"},
        {},
        [],
        None,
    ],
)
def test_unrecognized_error_payload_stays_sanitized_and_uncertain(body):
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(409, json=body))) as http:
        with pytest.raises(KalshiAPIError) as caught:
            KalshiClient(http_client=http).request("POST", "/test", authenticated=False)
    assert caught.value.error_code is None
    assert "private-account-data" not in repr(vars(caught.value))


def test_non_json_error_remains_typed_without_leaking_body():
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(409, text="private-error"))) as http:
        with pytest.raises(KalshiAPIError) as caught:
            KalshiClient(http_client=http).request("POST", "/test", authenticated=False)
    assert caught.value.status_code == 409
    assert caught.value.error_code is None
    assert "private-error" not in str(caught.value)


@pytest.mark.parametrize(
    "tif,status,code,rejected",
    [
        ("fok", 409, FOK_CODE, True),
        ("fok", 409, None, False),
        ("fok", 409, "duplicate_order_id", False),
        ("fok", 500, FOK_CODE, False),
        ("fok", None, FOK_CODE, False),
        ("ioc", 409, FOK_CODE, False),
        ("gtc", 409, FOK_CODE, False),
    ],
)
def test_only_definite_fok_rejection_clears_pending_and_closes_order(broker, tif, status, code, rejected):
    original = broker._client.request
    submissions = []

    def reject(method, path, **kwargs):
        if method == "POST":
            submissions.append(kwargs["json"]["client_order_id"])
            raise KalshiAPIError("provider rejected request", status_code=status, error_code=code)
        return original(method, path, **kwargs)

    broker._client.request = reject
    obj = order(time_in_force=tif)
    with pytest.raises(KalshiAPIError):
        broker.submit_order(obj)
    assert len(submissions) == 1
    assert not obj.transactions
    if rejected:
        assert obj.status == Order.OrderStatus.ERROR
        assert obj._closed_event.is_set()
        assert obj._kalshi_client_order_id not in broker._pending_submissions
        assert obj not in broker.get_active_tracked_orders("test")
    else:
        assert obj.status == Order.OrderStatus.UNKNOWN
        assert not obj._closed_event.is_set()
        assert broker._pending_submissions[obj._kalshi_client_order_id] is obj


def test_demo_harness_accepts_definite_fok_rejection_and_completes_cleanup(broker, monkeypatch):
    original = broker._client.request

    def reject_fok(method, path, **kwargs):
        if method == "POST" and kwargs.get("json", {}).get("time_in_force") == "fill_or_kill":
            raise KalshiAPIError("FOK rejected", status_code=409, error_code=FOK_CODE)
        return original(method, path, **kwargs)

    broker._client.request = reject_fok
    broker._is_stream_subscribed = True  # Harness-only fixture; real subscription is tested on Demo.
    monkeypatch.setattr(demo_tests, "_eligible_contract", lambda _: (ASSET, 0.01, 0.02))
    strategy = ContractStrategy(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
    demo_tests.test_demo_submit_read_modify_cancel_and_ioc_fok(strategy)
    assert not broker._pending_submissions
    assert not broker.get_active_tracked_orders(strategy.name)
    assert len(broker._error_orders) == 1
