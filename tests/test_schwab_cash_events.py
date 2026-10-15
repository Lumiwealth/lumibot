"""Synthetic Schwab transaction contracts; no credentials or broker actions."""

import datetime as dt
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
        {"netAmount": "NaN"},
        {"activityId": None},
        {"time": None},
        {"time": "2026-10-01T12:00:00"},
        {"status": "PENDING"},
        {"status": "REVERSED"},
        {"transferItems": []},
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
