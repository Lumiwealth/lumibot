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
        "transferItems": [{"instrument": {"assetType": "CURRENCY", "symbol": "CURRENCY_USD"}, "amount": amount}],
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
    # The real initialized broker always resolves the full selected account.
    broker.account_number = "synthetic-account"
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


def test_schwab_cash_query_does_not_claim_security_transfer_valuation():
    broker, calls = broker_for([])
    assert broker.get_cash_events() == []
    requested = {value.value for value in calls[0]["transaction_types"]}
    assert {"ACH_RECEIPT", "CASH_DISBURSEMENT", "JOURNAL", "DIVIDEND_OR_INTEREST"} <= requested
    assert not requested & {"TRADE", "RECEIVE_AND_DELIVER", "MEMORANDUM", "MONEY_MARKET"}


@pytest.mark.parametrize("provider_time", ["2026-10-01T04:00:00+0000", "2026-10-01T06:00:00+0200"])
def test_schwab_compact_provider_timezone_is_parsed_on_python_310(provider_time):
    event = Schwab._normalize_transaction_to_cash_event(transaction(time=provider_time))
    assert event.occurred_at == dt.datetime(2026, 10, 1, 4, tzinfo=dt.timezone.utc)


@pytest.mark.parametrize(
    "description",
    [
        "TRF FDS TO TYPE 1",
        "TRF FDS FRM TYPE 2",
        "BANK SWEEP TO BROKERAGE",
        "BROKERAGE SWEEP FR BANK",
        "BANK SWEEP FR BROKERAGE",
        "BROKERAGE SWEEP TO BANK",
    ],
)
def test_schwab_internal_cash_journals_are_not_new_capital(description):
    broker, _ = broker_for([transaction(kind="JOURNAL", amount=-25, description=description)])
    (event,) = broker.get_cash_events()
    assert event.event_type == "journal"
    assert event.amount == -25
    assert event.is_external_cash_flow is False
    assert event.description == description


def test_schwab_ira_distribution_withholding_leaves_the_account():
    broker, _ = broker_for(
        [
            transaction("net-distribution", "CASH_DISBURSEMENT", -80),
            transaction("withholding", "JOURNAL", -20, description="FED INC TAX WHLD IRA"),
        ]
    )
    events = broker.get_cash_events()
    assert sum(e.amount for e in events if e.is_external_cash_flow) == -100
    assert next(e for e in events if e.broker_event_id == "withholding").event_type == "tax"


@pytest.mark.parametrize("description", ["BANK SWEEP TO ANOTHER ACCOUNT", "TRF FDS TO TYPE 1 UNKNOWN"])
def test_schwab_similar_unknown_journal_labels_are_not_guessed(description):
    broker, _ = broker_for([transaction(kind="JOURNAL", description=description)])
    with pytest.raises(ValueError, match="classification"):
        broker.get_cash_events()


def test_schwab_timed_out_history_is_retried_in_smaller_complete_windows():
    import httpx

    broker, _ = broker_for([])
    calls = []
    row = transaction()

    def get_transactions(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise httpx.ReadTimeout("private-provider-url")
        return SimpleNamespace(status_code=200, json=lambda: [row])

    broker.client.get_transactions = get_transactions
    events = broker.get_cash_events()
    assert len(events) == 1
    assert len(calls) == 3
    assert calls[1]["start_date"] == calls[2]["end_date"]
    assert calls[1]["end_date"] == calls[0]["end_date"]
    assert calls[2]["start_date"] == calls[0]["start_date"]


def test_schwab_persistent_history_timeouts_have_a_bounded_sanitized_failure():
    import httpx

    broker, _ = broker_for([])
    calls = []

    def get_transactions(*args, **kwargs):
        calls.append(kwargs)
        raise httpx.ReadTimeout("private-provider-url")

    broker.client.get_transactions = get_transactions
    with pytest.raises(ConnectionError, match="request failed") as caught:
        broker.get_cash_events()
    assert len(calls) <= 8
    assert "private-provider-url" not in str(caught.value)


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


def test_schwab_older_history_uses_official_request_interval_contract():
    broker, calls = broker_for([])
    # The official endpoint permits one year per request; the SDK's 60-day
    # description is stale. Neither contract establishes historical retention.
    assert broker.get_cash_events(since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=800)) == []
    assert len(calls) == 3
    assert all(call["end_date"] - call["start_date"] <= dt.timedelta(days=365) for call in calls)
    ordered = sorted(calls, key=lambda call: call["start_date"])
    assert all(a["end_date"] == b["start_date"] for a, b in zip(ordered, ordered[1:]))


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
    (event,) = broker.get_cash_events(since=local)
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


@pytest.mark.parametrize("action", ["ACTIVITY_CORRECTION", "ORDER_ACTION", "UNKNOWN", "NEW_ACTION", [], {}])
def test_schwab_ambiguous_activity_action_requires_reconciliation(action):
    broker, _ = broker_for([transaction(activityType=action)])
    with pytest.raises(ValueError, match="reconciliation"):
        broker.get_cash_events()


@pytest.mark.parametrize("action", [None, "TRANSFER", "EXECUTION"])
def test_schwab_known_activity_action_preserves_signed_cash(action):
    event = Schwab._normalize_transaction_to_cash_event(transaction(activityType=action, amount=-100))
    assert event.amount == -100
    assert event.is_external_cash_flow is True


@pytest.mark.parametrize(
    "changes",
    [
        {"type": []},
        {"type": {}},
        {"description": {"private": "provider-data"}},
        {"netAmount": 10**400},
        {"netAmount": "$"},
        {"netAmount": ","},
        {"netAmount": True},
        {"netAmount": float("inf")},
        {"transferItems": [None], "type": "DIVIDEND_OR_INTEREST"},
        {"transferItems": [{"instrument": {"assetType": "CURRENCY", "symbol": "CURRENCY_1$?"}}]},
        {"time": "2026-10-01"},
        {"time": "2026-10-01X12:00:00+00:00"},
    ],
)
def test_schwab_malformed_fields_fail_with_sanitized_value_error(changes):
    broker, _ = broker_for([transaction(**changes)])
    with pytest.raises(ValueError) as caught:
        broker.get_cash_events()
    assert "provider-data" not in str(caught.value)


@pytest.mark.parametrize("status", [None, "private-response", {"private": "response"}])
def test_schwab_malformed_http_status_cannot_leak_provider_data(status):
    broker, calls = broker_for([], status=status)
    with pytest.raises(ConnectionError) as caught:
        broker.get_cash_events()
    assert "private" not in str(caught.value)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [401, 403, 429, 503])
def test_schwab_http_failure_does_not_loop_or_reinitialize_broker(status):
    broker, calls = broker_for([], status=status)
    original_client = broker.client
    with pytest.raises(ConnectionError):
        broker.get_cash_events()
    assert len(calls) == 1
    assert broker.client is original_client
    assert broker.hash_value == "synthetic-account-hash"


def test_schwab_json_decode_failure_is_sanitized():
    broker, _ = broker_for([])

    def decode():
        raise ValueError("private-response-content")

    broker.client.get_transactions = lambda *a, **k: SimpleNamespace(status_code=200, json=decode)
    with pytest.raises(ValueError, match="Invalid Schwab transaction response") as caught:
        broker.get_cash_events()
    assert caught.value.__suppress_context__ is True


def test_schwab_historical_request_budget_is_checked_before_network_io():
    broker, calls = broker_for([])
    with pytest.raises(ValueError, match="request budget"):
        broker.get_cash_events(since=dt.datetime(1800, 1, 1, tzinfo=dt.timezone.utc))
    assert calls == []


def test_schwab_saturation_budget_is_bounded_even_with_no_progress():
    broker, calls = broker_for([transaction()] * 3000)
    with pytest.raises(ValueError, match="drained|saturated"):
        broker.get_cash_events()
    assert 1 < len(calls) <= 127


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
        broker,
        {"accountNumber": "synthetic-1234", "hashValue": "private-account-hash"},
        7,
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
    monkeypatch.setattr(probe.os, "environ", {"SCHWAB_TOKEN": "stale-payload"})
    path = tmp_path / "synthetic.env"
    path.write_text("SCHWAB_APP_KEY=test-app\nSCHWAB_APP_SECRET=test-secret\nAWS_SECRET_ACCESS_KEY=must-not-load\n")
    probe.configure_environment(path)
    import os

    assert os.environ["SCHWAB_APP_KEY"] == "test-app"
    assert os.environ["SCHWAB_APP_SECRET"] == "test-secret"
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ
    assert "SCHWAB_TOKEN" not in os.environ


def test_schwab_probe_closes_the_actual_supported_broker_lifecycle(monkeypatch, tmp_path):
    import sys

    import lumibot.brokers.schwab as module

    probe = _probe_module()
    token = tmp_path / "synthetic-token.json"
    token.write_text("{}")
    closed = []

    class ReadOnlyBroker:
        def __init__(self, **kwargs):
            # Empty account discovery now fails explicitly. Use an authorized
            # synthetic account to keep this successful-cleanup assertion.
            self.client = SimpleNamespace(
                get_account_numbers=lambda: SimpleNamespace(
                    status_code=200,
                    json=lambda: [
                        {"accountNumber": "synthetic-1234", "hashValue": "synthetic-hash"},
                    ],
                ),
                get_transactions=lambda *args, **kwargs: None,
            )

        def get_cash_events(self, **kwargs):
            return []

        def cleanup_streams(self):
            closed.append(True)

    monkeypatch.setattr(module, "Schwab", ReadOnlyBroker)
    monkeypatch.setattr(probe, "configure_environment", lambda *args: None)
    # main() suppresses private provider logging for its standalone process.
    # Invoking it in pytest must not disable logging for subsequent tests.
    monkeypatch.setattr(probe.logging, "disable", lambda *args: None)
    monkeypatch.setattr(sys, "argv", ["probe", "--token-path", str(token), "--app-env", "synthetic.env"])
    assert probe.main() == 0
    assert closed == [True]


@pytest.mark.parametrize(
    "accounts",
    [
        None,
        {},
        [],
        [None],
        [{"accountNumber": "1234"}],
        [{"accountNumber": True, "hashValue": "h"}],
        [{"accountNumber": "1234", "hashValue": " "}],
    ],
)
def test_schwab_probe_malformed_account_discovery_fails_and_cleans_up(monkeypatch, tmp_path, accounts):
    import sys

    import lumibot.brokers.schwab as module

    probe = _probe_module()
    token = tmp_path / "synthetic-token.json"
    token.write_text("{}")
    closed = []

    class ReadOnlyBroker:
        def __init__(self, **kwargs):
            self.client = SimpleNamespace(
                get_account_numbers=lambda: SimpleNamespace(status_code=200, json=lambda: accounts)
            )

        def cleanup_streams(self):
            closed.append(True)

    monkeypatch.setattr(module, "Schwab", ReadOnlyBroker)
    monkeypatch.setattr(probe, "configure_environment", lambda *args: None)
    monkeypatch.setattr(probe.logging, "disable", lambda *args: None)
    monkeypatch.setattr(probe, "inspect_account", lambda *args: pytest.fail("Invalid discovery must not read history"))
    monkeypatch.setattr(sys, "argv", ["probe", "--token-path", str(token), "--app-env", "synthetic.env"])
    with pytest.raises(RuntimeError, match="account discovery"):
        probe.main()
    assert closed == [True]


def test_schwab_probe_cleanup_failure_does_not_hide_discovery_error(monkeypatch, tmp_path):
    import sys

    import lumibot.brokers.schwab as module

    probe = _probe_module()
    token = tmp_path / "synthetic-token.json"
    token.write_text("{}")

    class ReadOnlyBroker:
        def __init__(self, **kwargs):
            self.client = SimpleNamespace(get_account_numbers=lambda: SimpleNamespace(status_code=401))

        def cleanup_streams(self):
            raise ValueError("private-cleanup-details")

    monkeypatch.setattr(module, "Schwab", ReadOnlyBroker)
    monkeypatch.setattr(probe, "configure_environment", lambda *args: None)
    monkeypatch.setattr(probe.logging, "disable", lambda *args: None)
    monkeypatch.setattr(sys, "argv", ["probe", "--token-path", str(token), "--app-env", "synthetic.env"])
    with pytest.raises(RuntimeError, match="account discovery failed"):
        probe.main()


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


def test_schwab_funding_sync_audits_security_categories_and_empty_window():
    broker, calls = broker_for([])
    broker.account_number = "synthetic-account"
    result = broker.get_cash_event_sync()
    receipt = result.to_dict(producer_kind="account_snapshot", batch_index=0, batch_size=50)
    assert receipt["state"] == "complete"
    assert receipt["pagination_exhausted"] is True
    assert receipt["covered_intervals"] == [receipt["requested"]]
    assert "RECEIVE_AND_DELIVER" in {value.value for value in calls[0]["transaction_types"]}
    assert receipt["account_ref"]["account_id"] == "synthetic-account"


@pytest.mark.parametrize(
    "unresolved",
    [
        transaction(kind="RECEIVE_AND_DELIVER", amount=0),
        transaction(kind="JOURNAL", description="UNRECOGNIZED TRANSFER"),
        transaction(activityType="CORRECTION"),
        transaction(status="PENDING"),
    ],
)
def test_schwab_funding_sync_keeps_known_cash_but_never_certifies_unresolved_transfer(unresolved):
    broker, _ = broker_for([transaction(identifier="known", time="2026-01-01T00:00:00Z"), unresolved])
    broker.account_number = "synthetic-account"
    result = broker.get_cash_event_sync(since=dt.datetime(2025, 12, 1, tzinfo=dt.timezone.utc))
    receipt = result.to_dict(producer_kind="external_bot", batch_index=0, batch_size=50)
    assert [e.event_id for e in result.events] == ["schwab:known"]
    assert receipt["state"] == "partial"
    assert receipt["covered_intervals"] == []
    assert receipt["reason_code"] == "unresolved_transaction"


def test_schwab_funding_sync_outage_is_explicit_and_sanitized():
    broker, _ = broker_for([], status=401)
    broker.account_number = "synthetic-account"
    result = broker.get_cash_event_sync()
    receipt = result.to_dict(producer_kind="managed_bot", batch_index=0, batch_size=50)
    assert receipt["state"] == "unavailable"
    assert receipt["reason_code"] == "broker_history_unavailable"
    assert receipt["pagination_exhausted"] is False
    assert receipt["completed_at"] is None
    assert receipt["covered_intervals"] == []
