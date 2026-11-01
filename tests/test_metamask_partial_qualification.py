"""Offline orchestration controls; no provider calls or trading evidence."""

import importlib.util
import json
from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.tools.metamask.venue import HyperliquidVenue

SCRIPT = Path(__file__).parents[1] / "scripts/qualify_metamask_partial.py"
spec = importlib.util.spec_from_file_location("metamask_partial_qualification", SCRIPT)
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


def configs():
    return [
        {
            "NETWORK": "testnet",
            "WALLET_ADDRESS": "0x" + str(i + 1) * 40,
            "AGENT_ID": f"agent-{i}",
            "STATE_FILE": "same-state",
            "SESSION_FILE": "same-session",
        }
        for i in range(2)
    ]


@pytest.mark.parametrize("mutation", ["mainnet", "same_wallet", "different_owner", "spot"])
def test_hard_guards_reject_unsafe_configurations(mutation):
    values = configs()
    if mutation == "mainnet":
        values[1]["NETWORK"] = "mainnet"
    elif mutation == "same_wallet":
        values[1]["WALLET_ADDRESS"] = values[0]["WALLET_ADDRESS"]
    elif mutation == "different_owner":
        values[1]["STATE_FILE"] = "other-state"
    else:
        values[1]["MODE"] = "spot"
    with pytest.raises(RuntimeError):
        qualification.validate_configs(values)


def test_authority_must_match_approved_caps_and_have_time_for_safe_cleanup():
    binding = {
        "network": "testnet",
        "enabled": True,
        "maxOrderUsd": "25",
        "maxDailyUsd": "300",
        "maxLeverage": 1,
        "maxSlippageBps": 50,
        "markets": ["BTC", "ETH"],
        "expiresAt": 900_000,
    }
    qualification.validate_authority(binding, 0, 100_000)
    for changes in ({"maxLeverage": 2}, {"maxDailyUsd": "301"}, {"expiresAt": 200_000}, {"enabled": False}):
        with pytest.raises(RuntimeError):
            qualification.validate_authority({**binding, **changes}, 0, 100_000)


def test_timed_out_signature_hold_never_releases_and_worker_finishes():
    gate = qualification.SignatureGate("offline", Mock(), timeout=0.02)
    job = SimpleNamespace(state="SIGNED", request_id="offline-job")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(gate.hold, job)
        with pytest.raises(qualification.MetaMaskError, match="QUALIFICATION_ABORTED"):
            future.result(timeout=1)
    assert future.done()


def test_abort_joins_ready_worker_without_returning_signed_result():
    gate = qualification.SignatureGate("offline", Mock(), timeout=1)
    job = SimpleNamespace(state="SIGNED", request_id="offline-job")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(gate.hold, job)
        qualification.await_gate(gate, future, timeout=1)
        gate.abort()
        with pytest.raises(qualification.MetaMaskError, match="QUALIFICATION_ABORTED"):
            future.result(timeout=1)
    assert future.done()


def test_preparation_timeout_aborts_gate_before_any_later_signature_can_release():
    gate = qualification.SignatureGate("offline", Mock(), timeout=1)
    pending = Future()
    with pytest.raises(TimeoutError):
        qualification.await_gate(gate, pending, timeout=0.01)
    with pytest.raises(qualification.MetaMaskError, match="QUALIFICATION_ABORTED"):
        gate.hold(SimpleNamespace(state="SIGNED", request_id="late-job"))


@pytest.fixture
def public_book_clock(monkeypatch):
    clock = SimpleNamespace(elapsed=0.0, sleeps=[])

    def sleep(seconds):
        clock.sleeps.append(seconds)
        clock.elapsed += seconds

    monkeypatch.setattr(qualification.time, "monotonic", lambda: clock.elapsed)
    monkeypatch.setattr(qualification.time, "time", lambda: 1_000 + clock.elapsed)
    monkeypatch.setattr(qualification.time, "sleep", sleep)
    return clock


def public_venue(clock, spreads):
    # The eligibility phase receives only a public info reader and local normalizer.
    venue = Mock(spec=["info", "normalize_price"])
    values = iter(spreads)

    def info(query):
        assert query == {"type": "l2Book", "coin": "BTC"}
        bid, ask = next(values)
        return {
            "coin": "BTC",
            "time": int((1_000 + clock.elapsed) * 1_000),
            "levels": [[{"px": bid}], [{"px": ask}]],
        }

    venue.info.side_effect = info
    venue.normalize_price.side_effect = HyperliquidVenue.normalize_price
    return venue


def test_public_book_wait_accepts_only_normalized_price_with_five_ticks_each_side(public_book_clock):
    venue = public_venue(public_book_clock, [("77352", "77353"), ("77352", "77361"), ("77352", "77363")])
    bid, ask, price, tick = qualification.wait_for_eligible_book(venue, SimpleNamespace(size_decimals=5))
    # The nine-tick spread has an inside midpoint but insufficient room after rounding.
    assert (bid, ask, price, tick) == tuple(map(Decimal, ("77352", "77363", "77358", "1")))
    assert venue.info.call_count == 3
    assert public_book_clock.sleeps == [1, 1]


def test_public_book_wait_times_out_without_freezing_an_ineligible_intent(public_book_clock):
    venue = public_venue(public_book_clock, [("77352", "77353")] * 3)
    with pytest.raises(TimeoutError, match="five ticks"):
        qualification.wait_for_eligible_book(venue, SimpleNamespace(size_decimals=5), timeout=2.5)
    assert public_book_clock.elapsed == 2.5
    assert public_book_clock.sleeps == [1, 1, 0.5]
    assert venue.info.call_count == 3


def test_public_book_wait_does_not_accept_a_response_arriving_after_deadline(public_book_clock):
    venue = public_venue(public_book_clock, [("77352", "77415")])
    read = venue.info.side_effect

    def late_info(query):
        public_book_clock.elapsed = 61
        return read(query)

    venue.info.side_effect = late_info
    with pytest.raises(TimeoutError, match="five ticks"):
        qualification.wait_for_eligible_book(venue, SimpleNamespace(size_decimals=5))
    assert venue.info.call_count == 1
    assert public_book_clock.sleeps == []


def operation(state="canceled", filled="0.00015"):
    return {
        "id": "owned-maker",
        "state": state,
        "filledQuantity": filled,
        "request": {"symbol": "BTC", "quantity": "0.00030", "side": "buy"},
    }


@pytest.mark.parametrize("state", ["unknown", "open", "awaiting_approval"])
def test_even_zero_fills_cannot_hide_an_unresolved_order(state):
    with pytest.raises(RuntimeError, match="Unresolved"):
        qualification.owned_residual([operation(state, "0")], [], [], {"owned-maker"})


def test_cleanup_requires_exact_ownership_and_matching_authoritative_position():
    owned = {"owned-maker"}
    position = [{"symbol": "BTC", "quantity": "0.00015"}]
    assert qualification.owned_residual([operation()], position, [], owned) == Decimal("0.00015")
    for operations, positions, orders in (
        ([operation()], [{"symbol": "BTC", "quantity": "0.00030"}], []),
        ([operation()], position, [{"oid": 777}]),
        ([{**operation(), "id": "unowned"}], position, []),
    ):
        with pytest.raises(RuntimeError):
            qualification.owned_residual(operations, positions, orders, owned)


def test_cleanup_does_not_cancel_foreign_orders_or_close_unowned_position():
    broker = Mock()
    broker.client.config = {"AGENT_ID": "agent", "WALLET_ADDRESS": "0x" + "1" * 40}
    broker.client.journal.get.return_value = operation()
    broker.client.get_order.return_value = operation()
    broker.client.account.return_value = {"positions": [{"symbol": "BTC", "quantity": "0.00030"}]}
    broker.client.venue.info.return_value = []
    item = SimpleNamespace(identifier="owned-maker")
    with pytest.raises(RuntimeError, match="Authoritative position differs"):
        qualification.cleanup(broker, [item], "owned-cleanup", Mock(), 0)
    broker.cancel_order.assert_called_once_with(item)
    broker.submit_order.assert_not_called()


def test_evidence_is_private_durable_and_contains_no_signature_material(tmp_path):
    path = tmp_path / "evidence.json"
    evidence = qualification.Evidence(path, "offline")
    evidence.stage("prepared", ids=["owned-maker"])
    assert json.loads(path.read_text())["stages"][0]["ids"] == ["owned-maker"]
    assert path.stat().st_mode & 0o077 == 0
    assert "signature" not in path.read_text()
    assert not list(tmp_path.glob(".partial-*"))
