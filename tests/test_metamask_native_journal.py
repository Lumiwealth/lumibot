"""Durable wallet binding, reservation and crash fencing for native execution."""

import json
import sqlite3
from decimal import localcontext

import pytest

from lumibot.tools.metamask.journal import Journal, sqlite_busy
from lumibot.tools.metamask.provider import MetaMaskError

NOW = 2_000_000
WALLET = "0x" + "1" * 40


def test_python_310_lock_errors_are_distinguished_from_storage_failures():
    assert sqlite_busy(sqlite3.OperationalError("database is locked"))
    assert sqlite_busy(sqlite3.OperationalError("database table is locked: owner"))
    assert not sqlite_busy(sqlite3.OperationalError("disk I/O error"))


def binding():
    return {
        "id": "agent",
        "wallet": WALLET,
        "network": "testnet",
        "enabled": True,
        "expiresAt": NOW + 86_400_000,
        "maxOrderUsd": "25",
        "maxDailyUsd": "50",
        "maxLeverage": 1,
        "maxSlippageBps": 50,
        "markets": ["BTC", "ETH"],
        "chains": [421614],
        "assets": [],
        "policyDigest": "a" * 64,
    }


def request(identifier="one"):
    return {
        "kind": "perp",
        "id": identifier,
        "symbol": "BTC",
        "side": "buy",
        "quantity": "0.001",
        "orderType": "limit",
        "limitPrice": "20000",
        "leverage": 1,
        "reduceOnly": False,
        "tif": "gtc",
    }


def _reserve(store, request, usd, **kwargs):
    return store.reserve("agent", request, usd, expected_binding=store.binding("agent"), **kwargs)


@pytest.fixture
def journal(tmp_path):
    instance = Journal(tmp_path / "broker.sqlite")
    instance.bind(binding())
    yield instance
    instance.close()


def test_same_request_id_reuses_reservation_and_different_content_is_rejected(journal):
    first, created = _reserve(journal, request(), "20", now=NOW)
    second, created_again = _reserve(journal, request(), "20", now=NOW + 1)
    assert created is True and created_again is False
    assert second == first
    with pytest.raises(MetaMaskError, match="ID_CONFLICT"):
        _reserve(journal, {**request(), "quantity": "0.002"}, "20", now=NOW + 2)
    assert len(journal.operations("agent")) == 1


def test_daily_limit_cannot_round_down_in_strategy_decimal_context(journal):
    journal.bind({**binding(), "maxOrderUsd": "100", "maxDailyUsd": "100"})
    with localcontext() as context:
        context.prec = 6
        context.Emax = 1
        first, _ = _reserve(journal, request(), "99.9999", now=NOW)
        journal.save({**first, "state": "canceled"})
        with pytest.raises(MetaMaskError, match="DAILY_LIMIT"):
            _reserve(journal, request("two"), "0.0002", now=NOW)


def test_fee_overrun_requires_exact_owner_acknowledgment_without_losing_fill(journal):
    first, _ = _reserve(journal, request(), "20", now=NOW)
    overrun = {"reservedAtomic": "1", "actualAtomic": "2", "chainId": 1}
    journal.save(
        {
            **first,
            "state": "filled",
            "filledQuantity": "0.001",
            "budgetReviewRequired": True,
            "feeBudgetOverrun": overrun,
            "feeAdjustedReservationUsd": "21",
        }
    )
    journal.bind({**binding(), "maxDailyUsd": "1000"})
    with pytest.raises(MetaMaskError, match="FEE_BUDGET_REVIEW_REQUIRED"):
        _reserve(journal, request("two"), "1", now=NOW + 1)
    with pytest.raises(MetaMaskError, match="REVIEW_CHANGED"):
        journal.acknowledge_fee_overrun("agent", "one", {**overrun, "actualAtomic": "1"})
    reviewed = journal.acknowledge_fee_overrun("agent", "one", overrun)
    assert reviewed["state"] == "filled" and reviewed["filledQuantity"] == "0.001"
    assert reviewed["feeBudgetOverrun"] == overrun and reviewed["budgetReviewRequired"] is False
    assert _reserve(journal, request("two"), "1", now=NOW + 2)[1] is True


def test_reviewed_fee_overrun_stays_charged_to_daily_usage(journal):
    first, _ = _reserve(journal, request(), "20", now=NOW)
    overrun = {"reservedAtomic": "1", "actualAtomic": "2", "chainId": 1}
    journal.save(
        {
            **first,
            "state": "rejected",
            "hashes": ["chain-receipt"],
            "budgetReviewRequired": True,
            "feeBudgetOverrun": overrun,
            "feeAdjustedReservationUsd": "49.5",
        }
    )
    journal.acknowledge_fee_overrun("agent", "one", overrun)
    with pytest.raises(MetaMaskError, match="DAILY_LIMIT"):
        _reserve(journal, request("two"), "1", now=NOW + 1)
    row = journal.get("agent", "one")
    with pytest.raises(MetaMaskError, match="RESERVATION_REGRESSION"):
        journal.save({**row, "feeAdjustedReservationUsd": "20"})


def test_unknown_effect_blocks_other_orders_across_restart(journal, tmp_path):
    operation, _ = _reserve(journal, request(), "20", now=NOW)
    journal.save({**operation, "state": "unknown"})
    journal.close()
    restored = Journal(tmp_path / "broker.sqlite")
    try:
        with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
            _reserve(restored, request("two"), "20", now=NOW + 1)
        assert restored.get("agent", "one")["state"] == "unknown"
    finally:
        restored.close()


@pytest.mark.parametrize("state", ["canceled", "rejected"])
def test_terminal_reconciliation_error_fences_new_spending_until_resolved(journal, state):
    first, _ = _reserve(journal, request(), "20", now=NOW)
    unresolved = {**first, "state": state, "reconciliationErrorCode": "FILL_HISTORY_INCOMPLETE"}
    journal.save(unresolved)
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        _reserve(journal, request("two"), "1", now=NOW + 1)
    assert journal.get("agent", "two") is None
    journal.save({**unresolved, "reconciliationErrorCode": None})
    assert _reserve(journal, request("two"), "1", now=NOW + 2)[1] is True


def test_open_orders_retain_budget_even_outside_rolling_day(journal):
    operation, _ = _reserve(journal, request(), "25", now=NOW)
    journal.save({**operation, "state": "open"})
    late = NOW + 86_400_001
    journal.bind({**binding(), "expiresAt": late + 1000})
    second, _ = _reserve(journal, request("two"), "25", now=late)
    journal.save({**second, "state": "open"})
    with pytest.raises(MetaMaskError, match="DAILY_LIMIT"):
        _reserve(journal, request("three"), "1", now=late)


def test_known_rejection_releases_budget_but_cancellation_retains_daily_reservation(journal):
    first, _ = _reserve(journal, request(), "25", now=NOW)
    journal.save({**first, "state": "rejected"})
    second, _ = _reserve(journal, request("two"), "25", now=NOW + 1)
    journal.save({**second, "state": "canceled"})
    third, _ = _reserve(journal, request("three"), "25", now=NOW + 2)
    journal.save({**third, "state": "canceled"})
    with pytest.raises(MetaMaskError, match="DAILY_LIMIT"):
        _reserve(journal, request("four"), "1", now=NOW + 3)


def test_expiry_revocation_and_wallet_change_never_create_an_order(journal):
    with pytest.raises(MetaMaskError, match="WALLET_IMMUTABLE"):
        journal.bind({**binding(), "wallet": "0x" + "2" * 40})
    journal.bind({**binding(), "enabled": False})
    with pytest.raises(MetaMaskError, match="REVOKED"):
        _reserve(journal, request(), "1", now=NOW)
    journal.bind(binding())
    with pytest.raises(MetaMaskError, match="REVOKED"):
        _reserve(journal, request(), "1", now=NOW + 86_400_001)
    assert journal.operations("agent") == []


def test_existing_node_owner_lock_fences_native_execution(tmp_path):
    path = tmp_path / "broker.sqlite"
    owner = sqlite3.connect(str(path) + ".owner.sqlite")
    owner.executescript("PRAGMA journal_mode=DELETE; CREATE TABLE owner(id INTEGER); BEGIN EXCLUSIVE;")
    try:
        with pytest.raises(MetaMaskError, match="STATE_IN_USE"):
            Journal(path)
    finally:
        owner.close()
    native = Journal(path)
    native.close()


def test_legacy_pending_tables_and_json_are_preserved_without_implicit_replay(tmp_path):
    path = tmp_path / "broker.sqlite"
    db = sqlite3.connect(path)
    records = {
        "policy_approvals": ("pending-policy", WALLET, json.dumps({"id": "pending-policy", "state": "pending"})),
        "native_funding": (
            "agent",
            "pending-gas",
            WALLET,
            json.dumps({"id": "pending-gas", "state": "unknown", "nonce": "4"}),
        ),
        "collateral_transfers": (
            "agent",
            "pending-collateral",
            WALLET,
            json.dumps({"id": "pending-collateral", "state": "awaiting_approval"}),
        ),
    }
    db.executescript(
        "CREATE TABLE policy_approvals(id TEXT PRIMARY KEY,wallet TEXT,data TEXT);"
        "CREATE TABLE native_funding(agent TEXT,id TEXT,wallet TEXT,data TEXT,PRIMARY KEY(agent,id));"
        "CREATE TABLE collateral_transfers(agent TEXT,id TEXT,wallet TEXT,data TEXT,PRIMARY KEY(agent,id));"
    )
    for table, values in records.items():
        db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in values)})", values)
    db.commit()
    db.close()
    journal = Journal(path)
    try:
        journal.bind(binding())
        with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
            _reserve(journal, request(), "1", now=NOW)
        for table, values in records.items():
            assert journal.db.execute(f"SELECT data FROM {table}").fetchone()[0] == values[-1]
    finally:
        journal.close()


def test_effect_identity_is_saved_before_a_mutation_and_nonce_does_not_rewind(journal):
    operation, _ = _reserve(journal, request(), "20", now=NOW)
    first_nonce = journal.next_nonce(now=NOW)
    assert journal.next_nonce(now=NOW - 100) == first_nonce + 1
    effect = {
        "requestId": "provider-request",
        "origin": "lumibot",
        "nonce": first_nonce,
        "action": {"type": "cancel", "cancels": []},
        "state": "prepared",
    }
    journal.save_effect(operation, "order", effect)
    assert journal.effect("agent", "one", "order") == effect
    with pytest.raises(MetaMaskError, match="EFFECT_IMMUTABLE"):
        journal.save_effect(operation, "order", {**effect, "requestId": "different"})


def test_binding_change_during_price_estimation_cannot_authorize_stale_intent(journal):
    expected = journal.binding("agent")
    journal.bind({**binding(), "policyDigest": "b" * 64})
    with pytest.raises(MetaMaskError, match="POLICY_CHANGED"):
        journal.reserve("agent", request(), "20", expected_binding=expected, now=NOW)
    assert journal.operations("agent") == []


def test_execution_snapshot_cannot_lower_existing_reservation(journal):
    operation, _ = _reserve(journal, request(), "20", now=NOW)
    with pytest.raises(MetaMaskError, match="RESERVATION_IMMUTABLE"):
        journal.save({**operation, "reservedUsd": "1", "state": "open"})
    assert journal.get("agent", "one")["reservedUsd"] == "20"


def test_hardlinked_journal_cannot_create_another_execution_owner(tmp_path):
    import os

    path = tmp_path / "original.sqlite"
    sqlite3.connect(path).close()
    alias = tmp_path / "alias.sqlite"
    os.link(path, alias)
    with pytest.raises(MetaMaskError, match="UNSAFE_STATE"):
        Journal(alias)
    assert not (tmp_path / "alias.sqlite.owner.sqlite").exists()


def test_stale_reconciliation_does_not_overwrite_newer_cancellation(journal):
    operation, _ = _reserve(journal, request(), "20", now=NOW)
    canceled = {**operation, "state": "canceled", "updatedAt": NOW + 2}
    journal.save(canceled)
    result = journal.replace_if_unchanged(operation, {**operation, "state": "open", "updatedAt": NOW + 1})
    assert result == canceled
    assert journal.get("agent", "one") == canceled


def test_negative_initial_fill_cannot_enter_journal(journal):
    operation = {
        "agent": "agent",
        "id": "one",
        "request": request(),
        "createdAt": NOW,
        "state": "open",
        "reservedUsd": "20",
        "filledQuantity": "-1",
    }
    with pytest.raises(MetaMaskError, match="INVALID_AMOUNT"):
        journal.save(operation)
    assert journal.operations("agent") == []
