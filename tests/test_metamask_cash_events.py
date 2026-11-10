"""Cash history through the actual MetaMask client, venue and Strategy collector."""

import datetime as dt
from decimal import localcontext
from unittest.mock import Mock

import pytest
import requests

from lumibot.brokers import MetaMask
from lumibot.strategies._strategy import _Strategy
from lumibot.tools.metamask.provider import MetaMaskError
from lumibot.tools.metamask.venue import URLS, HyperliquidVenue
from tests.test_cash_events import _cloud_update_dummy

WALLET = "0x" + "1" * 40
OTHER = "0x" + "2" * 40
START = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
STAMP = int(START.timestamp() * 1000)


def row(index, kind="deposit", **delta):
    return {"time": STAMP + index, "hash": "0x" + f"{index:064x}", "delta": {"type": kind, **delta}}


@pytest.fixture
def history():
    broker = MetaMask(
        {"NETWORK": "testnet", "WALLET_ADDRESS": WALLET, "AGENT_ID": "agent"},
        connect_stream=False,
        start_orders_thread=False,
    )
    broker.client.verify = Mock(return_value={"wallet": WALLET, "network": "testnet"})
    venue = HyperliquidVenue("testnet", session=Mock(spec=requests.Session))
    broker.client._venue = venue
    responses = {"userFunding": [], "userNonFundingLedgerUpdates": []}

    def read(method, url, **kw):
        assert method == "POST" and url == URLS[broker.client.network] + "/info"
        payload = kw["json"]
        assert payload["user"] == WALLET and payload["startTime"] >= STAMP
        assert "Authorization" not in kw.get("headers", {})
        result = Mock(status_code=200)
        result.json.return_value = responses[payload["type"]]
        return result

    venue.session.request.side_effect = read
    yield broker, venue, responses
    broker.close()


def test_signed_cash_activity_reaches_strategy_without_mutating_cash(history):
    broker, _, rows = history
    rows["userFunding"] = [row(1, "funding", coin="ETH", usdc="-0.2"), row(2, "funding", coin="BTC", usdc="0.1")]
    rows["userNonFundingLedgerUpdates"] = [
        row(3, usdc="25"),
        row(4, "withdraw", usdc="5", fee="0"),
        row(5, "internalTransfer", usdc="2", fee="0", user=WALLET, destination=OTHER),
        row(6, "subAccountTransfer", usdc="3", user=OTHER, destination=WALLET),
    ]
    sync = broker.get_cash_event_sync(since=START)
    assert sync.state == "complete" and sync.account_id == f"testnet:{WALLET}:perp"
    assert sync.mode == "paper" and all(event.currency == "USDC" for event in sync.events)
    assert [event.amount for event in sync.events] == [-0.2, 0.1, 25, -5, -2, 3]
    assert [event.is_external_cash_flow for event in sync.events] == [False, False, True, True, True, True]
    assert [e.event_id for e in broker.get_cash_events(since=START)] == [e.event_id for e in sync.events]
    dummy = _cloud_update_dummy(broker.get_cash_events)
    dummy.broker = broker
    broker.get_cash_event_sync = Mock(return_value=sync)
    assert _Strategy._collect_cash_events_for_cloud(dummy) == list(sync.events)
    assert dummy._cash_event_initial_history_loaded is True
    assert dummy.get_cash() == 80 and dummy._portfolio_value == 100


@pytest.mark.parametrize(
    "delta",
    [
        {"type": "new-ledger-type", "usdc": "1"},
        {"type": "deposit", "usdc": "NaN"},
        {"type": "deposit", "usdc": "1e999"},
        {"type": "deposit", "usdc": True},
        {"type": "deposit", "usdc": "-1"},
        {"type": "withdraw", "usdc": "5", "fee": "1"},
        {"type": "withdraw", "usdc": "5"},
        {"type": "deposit", "usdc": "5", "token": "USDT"},
        {"type": "deposit", "usdc": "5", "nativeTokenFee": "1"},
        {"type": "accountClassTransfer", "usdc": "5", "toPerp": True},
        {"type": "spotTransfer", "token": "USDC", "amount": "5"},
        {"type": "internalTransfer", "usdc": "1", "fee": "0", "user": OTHER, "destination": OTHER},
    ],
)
def test_unresolved_capital_preserves_known_events_without_certifying_history(history, delta):
    broker, _, rows = history
    rows["userNonFundingLedgerUpdates"] = [row(1, usdc="10"), {**row(2), "delta": delta}]
    sync = broker.get_cash_event_sync(since=START)
    assert sync.state == "partial" and len(sync.events) == 1
    assert sync.to_dict(producer_kind="external_bot", batch_index=0, batch_size=50)["covered_intervals"] == []
    with pytest.raises(MetaMaskError, match="CASH_HISTORY_INCOMPLETE"):
        broker.get_cash_events(since=START)
    dummy = _cloud_update_dummy(broker.get_cash_events)
    dummy.broker = broker
    broker.get_cash_event_sync = Mock(return_value=sync)
    assert _Strategy._collect_cash_events_for_cloud(dummy) == list(sync.events)
    assert not getattr(dummy, "_cash_event_initial_history_loaded", False)


def test_checked_empty_window_differs_from_failed_read_and_unsupported_spot(history):
    broker, venue, _ = history
    sync = broker.get_cash_event_sync(since=START)
    assert sync.state == "complete" and sync.events == ()
    venue.session.request.side_effect = requests.ConnectionError("offline")
    sync = broker.get_cash_event_sync(since=START)
    assert sync.state == "unavailable" and sync.events == ()
    assert not sync.to_dict(producer_kind="external_bot", batch_index=0, batch_size=50)["pagination_exhausted"]
    broker.mode = "spot"
    assert broker.get_cash_event_sync(since=START).state == "unsupported"


@pytest.mark.parametrize("since,limit", [(START.replace(tzinfo=None), 100), (START, True), (START, 0)])
def test_invalid_cash_history_requests_fail_before_provider_reads(history, since, limit):
    broker, venue, _ = history
    with pytest.raises(ValueError):
        broker.get_cash_event_sync(since=since, limit=limit)
    venue.session.request.assert_not_called()


def test_limit_and_identity_failure_never_claim_complete_history(history):
    broker, _, rows = history
    rows["userNonFundingLedgerUpdates"] = [row(1, usdc="1"), row(2, usdc="2")]
    sync = broker.get_cash_event_sync(since=START, limit=1)
    assert sync.state == "partial" and len(sync.events) == 1
    assert sync.events[0].amount == 2
    broker.client.verify.side_effect = MetaMaskError("IDENTITY_MISMATCH", "Wrong wallet")
    assert broker.get_cash_event_sync(since=START).state == "unavailable"


def test_cash_history_boundaries_ids_and_amounts_ignore_decimal_context(history):
    broker, venue, rows = history
    rows["userNonFundingLedgerUpdates"] = [row(1, "withdraw", usdc="1.234567", fee="0")]
    with localcontext() as context:
        context.prec = 1
        first = broker.get_cash_event_sync(since=START + dt.timedelta(microseconds=500))
    assert first.events[0].amount == -1.234567
    assert venue.session.request.call_args.kwargs["json"]["endTime"] == int(first.end.timestamp() * 1000) - 1
    rows["userNonFundingLedgerUpdates"] = [row(0, usdc="1")]
    assert broker.get_cash_event_sync(since=START + dt.timedelta(microseconds=500)).events == ()
    rows["userNonFundingLedgerUpdates"] = [row(1, "withdraw", usdc="1.234567", fee="0")]
    broker.client.network = "mainnet"
    broker.client._venue = HyperliquidVenue("mainnet", session=venue.session)
    broker.client.verify.return_value = {"wallet": WALLET, "network": "mainnet"}
    # Network identity is part of event identity even when ledger hashes match.
    second = broker.get_cash_event_sync(since=START)
    assert second.mode == "live" and second.events[0].event_id != first.events[0].event_id


def test_fractional_start_filters_unsupported_rows_before_classification(history):
    broker, _, rows = history
    rows["userNonFundingLedgerUpdates"] = [row(0, "accountClassTransfer", usdc="1", toPerp=True)]
    since = START + dt.timedelta(microseconds=500)
    sync = broker.get_cash_event_sync(since=since)
    assert sync.state == "complete" and sync.events == ()
    rows["userNonFundingLedgerUpdates"].append(row(1, "accountClassTransfer", usdc="1", toPerp=True))
    assert broker.get_cash_event_sync(since=since).state == "partial"


def test_ledger_pagination_keeps_same_millisecond_tail_and_scopes_funding_coins(history):
    broker, venue, _ = history
    pages = [
        [row(i, "funding", coin="BTC", usdc="0.1") for i in range(1, 501)],
        [row(500, "funding", coin="BTC", usdc="0.1"), row(500, "funding", coin="ETH", usdc="0.2")],
    ]
    venue.info = Mock(side_effect=pages)
    rows = venue.ledger(WALLET, "userFunding", STAMP, STAMP + 1000)
    assert len(rows) == 501
    assert venue.info.call_args_list[1].args[0]["startTime"] == STAMP + 500
    venue.info = Mock(return_value=[row(1, "funding", coin="BTC", usdc="0.1")] * 500)
    with pytest.raises(MetaMaskError, match="CASH_HISTORY_INCOMPLETE"):
        venue.ledger(WALLET, "userFunding", STAMP, STAMP + 1000)
    venue.info = Mock(return_value=[row(1, usdc="1"), row(1, usdc="2")])
    with pytest.raises(MetaMaskError, match="CASH_HISTORY_INCOMPLETE"):
        venue.ledger(WALLET, "userNonFundingLedgerUpdates", STAMP, STAMP + 1000)


@pytest.mark.parametrize("kind", ["internalTransfer", "subAccountTransfer"])
def test_transfer_case_only_replays_keep_identity_and_one_cash_event(history, kind):
    from eth_utils import to_checksum_address

    broker, venue, rows = history
    source, destination = "0x" + "ab" * 20, "0x" + "cd" * 20
    broker.client.config["WALLET_ADDRESS"] = source
    broker.client.verify.return_value = {"wallet": source, "network": "testnet"}
    original = row(1, kind, usdc="12", fee="0", user=source, destination=destination)
    original["hash"] = "0x" + "ab" * 32
    alternate = {
        **original,
        "hash": "0x" + original["hash"][2:].upper(),
        "delta": {
            **original["delta"],
            "user": to_checksum_address(source),
            "destination": to_checksum_address(destination),
        },
    }
    venue.info = Mock(return_value=[])

    def read(payload):
        return [] if payload["type"] == "userFunding" else rows["userNonFundingLedgerUpdates"]

    venue.info.side_effect = read
    rows["userNonFundingLedgerUpdates"] = [original]
    first = broker.get_cash_event_sync(since=START)
    rows["userNonFundingLedgerUpdates"] = [alternate]
    second = broker.get_cash_event_sync(since=START)
    assert first.state == second.state == "complete"
    assert first.events[0].event_id == second.events[0].event_id
    rows["userNonFundingLedgerUpdates"] = [original, alternate]
    combined = broker.get_cash_event_sync(since=START)
    assert combined.state == "complete" and len(combined.events) == 1
    assert sum(event.amount for event in combined.events) == -12
    for change in ({"usdc": "13"}, {"fee": "1"}):
        rows["userNonFundingLedgerUpdates"] = [original, {**alternate, "delta": {**alternate["delta"], **change}}]
        assert broker.get_cash_event_sync(since=START).state == "unavailable"
    assert alternate["delta"]["user"] == to_checksum_address(source)


def test_vault_address_casing_cannot_hide_conflicting_ledger_evidence(history):
    from eth_utils import to_checksum_address

    _, venue, _ = history
    vault = "0x" + "de" * 20
    original = row(1, "vaultDeposit", usdc="12", vault=vault)
    alternate = {**original, "delta": {**original["delta"], "vault": to_checksum_address(vault)}}
    venue.info = Mock(return_value=[original, alternate])
    assert len(venue.ledger(WALLET, "userNonFundingLedgerUpdates", STAMP, STAMP + 1000)) == 1
    venue.info.return_value = [original, {**alternate, "delta": {**alternate["delta"], "usdc": "13"}}]
    with pytest.raises(MetaMaskError, match="CASH_HISTORY_INCOMPLETE"):
        venue.ledger(WALLET, "userNonFundingLedgerUpdates", STAMP, STAMP + 1000)


@pytest.mark.parametrize("mutation", [{"time": True}, {"hash": "bad"}, {"time": STAMP - 1}, {"delta": None}])
def test_bad_ledger_identity_cannot_certify_a_window(history, mutation):
    broker, _, rows = history
    rows["userNonFundingLedgerUpdates"] = [{**row(1, usdc="1"), **mutation}]
    assert broker.get_cash_event_sync(since=START).state == "unavailable"
