"""Synthetic Schwab transaction contracts; no credentials or broker actions."""

import datetime as dt
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lumibot.brokers.schwab import Schwab


def transaction(identifier="funding-1", kind="ACH_RECEIPT", amount=100, **overrides):
    return {
        "activityId": identifier,
        "type": kind,
        "status": "VALID",
        "time": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "netAmount": amount,
        "transferItems": [
            {"instrument": {"assetType": "CURRENCY", "symbol": "CURRENCY_USD"}, "amount": amount}
        ],
        **overrides,
    }


def broker_for(rows, status=200):
    calls = []

    def get_transactions(account_hash, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=status, json=lambda: rows)

    broker = Schwab.__new__(Schwab)
    broker.client = SimpleNamespace(get_transactions=get_transactions)
    broker.hash_value = "synthetic-account-hash"
    broker.name = "Schwab"
    broker.sync_positions = lambda *args, **kwargs: None
    return broker, calls


@pytest.mark.parametrize(
    "kind,amount,event_type,external",
    [
        ("ACH_RECEIPT", 100, "deposit", True),
        ("ACH_DISBURSEMENT", -20, "withdrawal", True),
        ("CASH_RECEIPT", 100, "deposit", True),
        ("CASH_DISBURSEMENT", -20, "withdrawal", True),
        ("WIRE_IN", 100, "deposit", True),
        ("WIRE_OUT", -20, "withdrawal", True),
        ("ELECTRONIC_FUND", -20, "withdrawal", True),
        # Keep the signed reversal; never abs() a provider cash amount.
        ("ACH_RECEIPT", -100, "withdrawal", True),
        ("DIVIDEND_OR_INTEREST", 3, "other_cash", False),
    ],
)
def test_schwab_cash_activity_preserves_financial_meaning(kind, amount, event_type, external):
    broker, calls = broker_for([transaction(kind=kind, amount=amount)])
    events = broker.get_cash_events()
    assert len(events) == 1
    event = events[0]
    assert event.event_type == event_type
    assert event.amount == amount
    assert event.currency == "USD"
    assert event.is_external_cash_flow is external
    assert event.event_id == "schwab:funding-1"
    assert event.broker_event_id == "funding-1"
    assert event.raw_type == kind
    assert len(calls) == 1
    assert "TRADE" not in [value.value for value in calls[0]["transaction_types"]]


def test_schwab_cash_events_skip_trade_cash_and_embedded_fees():
    broker, _ = broker_for([transaction(kind="TRADE", amount=-101)])
    assert broker.get_cash_events() == []


@pytest.mark.parametrize("kind", ["JOURNAL", "RECEIVE_AND_DELIVER", "MEMORANDUM", "UNKNOWN"])
def test_schwab_unclassified_activity_is_not_a_successful_empty_window(kind):
    broker, _ = broker_for([transaction(kind=kind)])
    with pytest.raises(ValueError, match="classification"):
        broker.get_cash_events()


@pytest.mark.parametrize(
    "changes",
    [
        {"netAmount": None},
        {"netAmount": " "},
        {"netAmount": "NaN"},
        {"activityId": None},
        {"time": None},
        {"time": "2026-10-01T12:00:00"},
        {"status": "PENDING"},
        {"status": "REVERSED"},
        {"transferItems": []},
        {"transferItems": [{"instrument": "invalid"}]},
        {"transferItems": [{"instrument": {"assetType": "EQUITY", "symbol": "SYNTH"}}]},
    ],
)
def test_schwab_incomplete_activity_cannot_become_verified_funding(changes):
    broker, _ = broker_for([transaction(**changes)])
    with pytest.raises(ValueError):
        broker.get_cash_events()


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_schwab_provider_errors_are_sanitized(status):
    broker, _ = broker_for({"secret": "do-not-log-provider-body"}, status=status)
    with pytest.raises(ConnectionError, match=f"HTTP {status}") as caught:
        broker.get_cash_events()
    assert "synthetic-account-hash" not in str(caught.value)
    assert "do-not-log-provider-body" not in str(caught.value)


def test_schwab_duplicate_events_are_delivered_once():
    row = transaction()
    broker, _ = broker_for([row, dict(row)])
    assert len(broker.get_cash_events()) == 1


def test_schwab_conflicting_duplicate_is_not_silently_overwritten():
    row = transaction()
    broker, _ = broker_for([row, {**row, "netAmount": 200}])
    with pytest.raises(ValueError, match="conflicting"):
        broker.get_cash_events()


def test_schwab_return_limit_cannot_silently_drop_funding():
    broker, _ = broker_for([transaction(identifier="a"), transaction(identifier="b")])
    with pytest.raises(ValueError, match="limit"):
        broker.get_cash_events(limit=1)


def test_schwab_outside_documented_history_window_is_not_empty_success():
    broker, calls = broker_for([])
    with pytest.raises(ValueError, match="60"):
        broker.get_cash_events(since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=90))
    assert calls == []


def test_schwab_successful_empty_response_is_empty():
    broker, calls = broker_for([])
    assert broker.get_cash_events() == []
    assert len(calls) == 1


def test_schwab_malformed_response_is_not_empty_success():
    broker, _ = broker_for({"transactions": []})
    with pytest.raises(ValueError):
        broker.get_cash_events()


def test_schwab_saturated_response_is_sliced_with_boundary_deduplication():
    broker, calls = broker_for([])
    row = transaction(time=(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat())

    def fetch(account_hash, **kwargs):
        calls.append(kwargs)
        rows = [row] * 3000 if len(calls) == 1 else [row]
        return SimpleNamespace(status_code=200, json=lambda: rows)

    broker.client.get_transactions = fetch
    assert len(broker.get_cash_events()) == 1
    assert len(calls) == 3
    assert calls[1]["start_date"] == calls[2]["end_date"]


def test_schwab_failure_after_one_slice_does_not_return_partial_success():
    broker, calls = broker_for([])
    row = transaction()

    def fetch(account_hash, **kwargs):
        calls.append(kwargs)
        if len(calls) == 3:
            return SimpleNamespace(status_code=403, json=lambda: {})
        return SimpleNamespace(status_code=200, json=lambda: [row] * (3000 if len(calls) == 1 else 1))

    broker.client.get_transactions = fetch
    with pytest.raises(ConnectionError, match="HTTP 403"):
        broker.get_cash_events()


def test_schwab_saturation_at_same_second_is_explicitly_incomplete():
    broker, _ = broker_for([transaction()] * 3000)
    with pytest.raises(ValueError, match="saturated"):
        broker.get_cash_events(since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1))


def test_schwab_provider_timeout_does_not_expose_account_or_response():
    broker, _ = broker_for([])

    def fail(*args, **kwargs):
        raise TimeoutError("secret account hash in request URL")

    broker.client.get_transactions = fail
    with pytest.raises(ConnectionError, match="request failed") as caught:
        broker.get_cash_events()
    assert "secret" not in str(caught.value)


def test_schwab_timezone_boundary_is_preserved():
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    boundary = now - dt.timedelta(days=1)
    local = boundary.astimezone(dt.timezone(dt.timedelta(hours=-4)))
    broker, calls = broker_for([transaction(time=local.isoformat())])
    event, = broker.get_cash_events(since=local)
    assert event.occurred_at == boundary
    assert calls[0]["start_date"] == boundary


def test_schwab_mixed_currency_and_in_kind_flows_need_explicit_classification():
    row = transaction()
    row["transferItems"].append({"instrument": {"assetType": "CURRENCY", "symbol": "CURRENCY_EUR"}})
    broker, _ = broker_for([row])
    with pytest.raises(ValueError, match="currency-conversion"):
        broker.get_cash_events()


def test_schwab_repeated_reads_keep_the_same_provider_event_identity():
    broker, _ = broker_for([transaction()])
    assert broker.get_cash_events()[0].to_dict() == broker.get_cash_events()[0].to_dict()


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_schwab_invalid_limit_does_not_fetch(limit):
    broker, calls = broker_for([])
    with pytest.raises(ValueError, match="limit"):
        broker.get_cash_events(limit=limit)
    assert calls == []


def _probe_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "schwab_cash_event_probe.py"
    spec = importlib.util.spec_from_file_location("schwab_cash_event_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_schwab_probe_uses_adapter_and_never_prints_private_event_values():
    probe = _probe_module()
    broker, _ = broker_for([transaction(identifier="private-activity", amount=123.45)])
    original_get = broker.client.get_transactions
    result = probe.inspect_account(
        broker, {"accountNumber": "synthetic-1234", "hashValue": "private-account-hash"}, 7,
    )
    assert result["normalized_count"] == 1
    assert result["event_types"] == {"deposit": 1}
    assert result["raw_types"] == {"ACH_RECEIPT": 1}
    assert broker.client.get_transactions is original_get
    serialized = json.dumps(result)
    for private in ["private-activity", "123.45", "private-account-hash", "synthetic-1234"]:
        assert private not in serialized


def test_schwab_probe_environment_loads_only_explicit_schwab_app_fields(monkeypatch, tmp_path):
    probe = _probe_module()
    for key in ["SCHWAB_APP_KEY", "SCHWAB_APP_SECRET", "AWS_SECRET_ACCESS_KEY", "SCHWAB_TOKEN"]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SCHWAB_TOKEN", "stale-payload")
    path = tmp_path / "synthetic.env"
    path.write_text("SCHWAB_APP_KEY=test-app\nSCHWAB_APP_SECRET=test-secret\nAWS_SECRET_ACCESS_KEY=must-not-load\n")
    probe.configure_environment(path)
    import os

    assert os.environ["SCHWAB_APP_KEY"] == "test-app"
    assert os.environ["SCHWAB_APP_SECRET"] == "test-secret"
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ
    assert "SCHWAB_TOKEN" not in os.environ


def test_schwab_events_reach_existing_listener_payload_and_acknowledgement(monkeypatch):
    # Use the real shared cloud sender, not a second Schwab-specific sender.
    import requests
    from lumibot.strategies._strategy import _Strategy
    from tests.test_cash_events import _cloud_update_dummy

    broker, _ = broker_for([transaction(identifier=f"event-{i}") for i in range(121)])
    strategy = _cloud_update_dummy(broker.get_cash_events)
    strategy.broker = broker
    payloads = []

    def post(*args, **kwargs):
        payloads.append(json.loads(kwargs["data"]))
        return SimpleNamespace(status_code=200, headers={})

    monkeypatch.setattr(requests, "post", post)
    for _ in range(3):
        assert _Strategy.send_update_to_cloud(strategy) is True
    assert [len(p["cash_events"]) for p in payloads] == [50, 50, 21]
    events = [event for payload in payloads for event in payload["cash_events"]]
    assert len({event["event_id"] for event in events}) == 121
    assert all(event["broker_name"] == "schwab" for event in events)
    assert all(event["amount"] == 100 and event["is_external_cash_flow"] is True for event in events)
    assert strategy._cash_event_pending_for_cloud == []


def test_schwab_listener_delivery_failure_retries_identical_events(monkeypatch):
    import requests
    from lumibot.strategies._strategy import _Strategy
    from tests.test_cash_events import _cloud_update_dummy

    broker, _ = broker_for([transaction()])
    strategy = _cloud_update_dummy(broker.get_cash_events)
    strategy.broker = broker
    payloads = []

    def post(*args, **kwargs):
        payloads.append(json.loads(kwargs["data"]))
        if len(payloads) == 1:
            raise requests.exceptions.Timeout("synthetic timeout")
        return SimpleNamespace(status_code=200, headers={})

    monkeypatch.setattr(requests, "post", post)
    assert _Strategy.send_update_to_cloud(strategy) is False
    assert len(strategy._cash_event_pending_for_cloud) == 1
    assert _Strategy.send_update_to_cloud(strategy) is True
    assert payloads[0]["cash_events"] == payloads[1]["cash_events"]
    assert strategy._cash_event_pending_for_cloud == []
