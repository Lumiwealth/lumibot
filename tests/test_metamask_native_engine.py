"""Wallet execution checkpoints and recovery, with offline ECDSA signatures."""

import time
from decimal import Decimal, localcontext
from unittest.mock import Mock

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

from lumibot.tools.metamask.engine import ExecutionEngine
from lumibot.tools.metamask.journal import Journal
from lumibot.tools.metamask.provider import Credentials, GuardPolicy, MetaMaskError, SignatureJob
from lumibot.tools.metamask.venue import HyperliquidVenue, Market

SIGNER = Account.from_key("0x" + "01" * 32)
WALLET = SIGNER.address.lower()


def intent(**values):
    return {
        "kind": "perp",
        "id": "one",
        "symbol": "BTC",
        "side": "buy",
        "quantity": "0.001",
        "orderType": "limit",
        "limitPrice": "20000",
        "leverage": 1,
        "reduceOnly": False,
        "tif": "gtc",
        **values,
    }


@pytest.fixture
def engine(tmp_path):
    journal = Journal(tmp_path / "broker.sqlite")
    journal.bind(
        {
            "id": "agent",
            "wallet": WALLET,
            "network": "testnet",
            "enabled": True,
            "expiresAt": int(time.time() * 1000) + 86400000,
            "policyDigest": "a" * 64,
            "markets": ["BTC"],
            "chains": [421614],
            "assets": [],
            "maxOrderUsd": "25",
            "maxDailyUsd": "100",
            "maxLeverage": 1,
            "maxSlippageBps": 50,
        }
    )
    provider = Mock()
    provider.credentials.return_value = Credentials("project", "access")
    provider.policy.return_value = GuardPolicy("policy", "a" * 64)
    venue = Mock(spec=HyperliquidVenue)
    venue.network = "testnet"
    venue.market.return_value = Market("BTC", 0, 5, 40)
    venue.price.return_value = Decimal("20000")
    venue.fee_rate.return_value = Decimal("0.00045")
    venue.order_action.side_effect = lambda *args, **kw: {
        "type": "order",
        "orders": [
            {"a": 0, "b": True, "p": "20000", "s": "0.001", "r": args[6], "t": {"limit": {"tif": "Gtc"}}, "c": args[7]}
        ],
        "grouping": "na",
    }
    instance = ExecutionEngine(journal, provider, venue, "agent")

    def sign(request_id, wallet, network, data, action):
        step = "leverage" if action["type"] == "updateLeverage" else "order"
        saved = journal.effect("agent", "one", step)
        assert saved["requestId"] == request_id and saved["typedData"] == data
        signature = "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x")
        return SignatureJob(request_id, wallet, "SIGNED", signature)

    provider.submit_signature.side_effect = sign

    def submit(action, nonce, signature, expiry):
        step = "leverage" if action["type"] == "updateLeverage" else "order"
        assert journal.effect("agent", "one", step)["phase"] == "venue"
        assert journal.get("agent", "one")["state"] == "unknown"
        return (
            {"status": "ok", "response": {"type": "default"}}
            if step == "leverage"
            else {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 42}}]}}}
        )

    venue.submit_action.side_effect = submit
    venue.fills.return_value = []

    def status(wallet, reference):
        plan = journal.effect("agent", "one", "plan")
        return {
            "status": "order",
            "order": {
                "status": "open",
                "order": {
                    "coin": "BTC",
                    "side": "B",
                    "origSz": "0.001",
                    "sz": "0.001",
                    "oid": 42,
                    "cloid": plan["clientOrderId"],
                    "reduceOnly": False,
                },
            },
        }

    venue.order_status.side_effect = status
    # Ordinary fixture observations agree; regressions below supply independently stale snapshots.
    venue.frontend_open_orders.side_effect = lambda wallet: [venue.order_status(wallet, 42)["order"]["order"]]
    yield instance
    journal.close()


def test_checkpoint_before_each_external_mutation_and_duplicate_id_never_resends(engine):
    result = engine.submit(intent())
    assert result["state"] == "open"
    assert engine.submit(intent())["state"] == "open"
    assert engine.provider.submit_signature.call_count == 2
    assert engine.venue.submit_action.call_count == 2
    with pytest.raises(MetaMaskError, match="ID_CONFLICT"):
        engine.submit(intent(quantity="0.0005"))


def test_normal_mark_movement_during_signing_fits_reviewed_reservation(engine):
    engine.venue.price.side_effect = [Decimal("20000"), Decimal("20010"), Decimal("20010")]
    result = engine.submit(intent())
    assert result["state"] == "open"
    assert engine.venue.submit_action.call_count == 2
    assert engine.journal.effect("agent", "one", "plan")["action"]["orders"][0]["p"] == "20000"


def test_mark_movement_beyond_reviewed_reservation_never_releases_signature(engine):
    engine.venue.price.side_effect = [Decimal("20000"), Decimal("20200")]
    assert engine.submit(intent())["state"] == "rejected"
    engine.venue.submit_action.assert_not_called()


@pytest.mark.parametrize("restart", [False, True])
def test_expired_uncertain_cancel_can_retry_only_cancel(engine, monkeypatch, restart):
    engine.submit(intent())
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, network, data, action: SignatureJob(
        request_id,
        wallet,
        "SIGNED",
        "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x"),
    )
    engine.venue.submit_action.side_effect = MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost cancel response")
    assert engine.cancel("one")["state"] == "open"
    first = engine.journal.effect("agent", "one", "cancel-42-1")
    assert engine.cancel("one")["state"] == "open"
    assert engine.venue.submit_action.call_count == 3
    monkeypatch.setattr(time, "time", lambda: (first["expiresAfter"] + 1000) / 1000)
    subject = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent") if restart else engine
    assert subject.cancel("one")["state"] == "open"
    assert engine.venue.submit_action.call_count == 4
    second = engine.journal.effect("agent", "one", "cancel-42-2")
    assert second["nonce"] > first["nonce"] and second["requestId"] != first["requestId"]
    assert [call.args[0]["type"] for call in engine.venue.submit_action.call_args_list] == [
        "updateLeverage",
        "order",
        "cancel",
        "cancel",
    ]


@pytest.mark.parametrize("state", ["unknown", "canceled"])
def test_expired_cancel_is_not_retried_without_confirmed_live_order(engine, monkeypatch, state):
    engine.submit(intent())
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, network, data, action: SignatureJob(
        request_id,
        wallet,
        "SIGNED",
        "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x"),
    )
    engine.venue.submit_action.side_effect = MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost cancel response")
    engine.cancel("one")
    first = engine.journal.effect("agent", "one", "cancel-42-1")
    monkeypatch.setattr(time, "time", lambda: (first["expiresAfter"] + 1000) / 1000)
    original = engine.venue.order_status.side_effect

    def status(*args):
        if state == "unknown":
            raise MetaMaskError("VENUE_UNAVAILABLE", "Read timed out")
        result = original(*args)
        result["order"]["status"] = state
        return result

    engine.venue.order_status.side_effect = status
    assert engine.cancel("one")["state"] == state
    assert engine.venue.submit_action.call_count == 3
    assert engine.journal.effect("agent", "one", "cancel-42-2") is None


def test_signature_uncertainty_polls_original_id_without_another_post(engine):
    engine.provider.submit_signature.side_effect = MetaMaskError("REQUEST_OUTCOME_UNKNOWN", "Lost response")
    assert engine.submit(intent())["state"] == "unknown"
    effect = engine.journal.effect("agent", "one", "leverage")
    engine.provider.signature.return_value = SignatureJob(effect["requestId"], WALLET, "SIGNING")
    assert engine.order("one")["state"] == "unknown"
    assert engine.provider.submit_signature.call_count == 1
    engine.venue.submit_action.assert_not_called()


def test_provider_shield_failure_remains_durable_and_does_not_submit_or_retry(engine):
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "FAILED", failure_code="SHIELD_SCAN_FAILED"
    )
    result = engine.submit(intent())
    assert result["state"] == "rejected" and "SHIELD_SCAN_FAILED" in result["message"]
    assert engine.journal.effect("agent", "one", "leverage")["failureCode"] == "SHIELD_SCAN_FAILED"
    assert engine.order("one") == result
    assert engine.provider.submit_signature.call_count == 1
    engine.venue.submit_action.assert_not_called()


def test_invalid_provider_request_is_rejected_without_polling_missing_job(engine):
    engine.provider.submit_signature.side_effect = MetaMaskError("REQUEST_REJECTED", "Invalid signing payload")
    result = engine.submit(intent())
    assert result["state"] == "rejected"
    assert engine.order("one") == result
    assert engine.provider.submit_signature.call_count == 1
    engine.provider.signature.assert_not_called()
    engine.venue.submit_action.assert_not_called()


def test_actual_venue_order_rejection_is_durable_and_never_replayed(engine):
    original = engine.venue.submit_action.side_effect
    reason = "Post only order would have immediately matched"

    def rejected(action, *args):
        if action["type"] == "order":
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"error": reason}]}}}
        return original(action, *args)

    engine.venue.submit_action.side_effect = rejected
    result = engine.submit(intent())
    assert result["state"] == "rejected" and result["rejectionReason"] == reason
    effect = engine.journal.effect("agent", "one", "order")
    assert effect["state"] == "rejected" and effect["response"]["response"]["data"]["statuses"] == [{"error": reason}]
    assert engine.order("one") == result
    assert engine.venue.submit_action.call_count == 2


def test_revocation_after_provider_signing_prevents_release(engine):
    original = engine.provider.submit_signature.side_effect

    def revoke(*args):
        result = original(*args)
        engine.journal.bind({**engine.journal.binding("agent"), "enabled": False})
        return result

    engine.provider.submit_signature.side_effect = revoke
    assert engine.submit(intent())["state"] == "rejected"
    engine.venue.submit_action.assert_not_called()


@pytest.mark.parametrize("read", ["price", "fee_rate"])
def test_revocation_during_final_venue_validation_prevents_release(engine, read):
    original = engine.provider.submit_signature.side_effect

    def sign(*args):
        result = original(*args)

        def revoke(*_):
            engine.journal.bind({**engine.journal.binding("agent"), "enabled": False})
            return Decimal("20000") if read == "price" else Decimal("0.00045")

        getattr(engine.venue, read).side_effect = revoke
        return result

    engine.provider.submit_signature.side_effect = sign
    result = engine.submit(intent())
    assert result["state"] == "rejected"
    assert engine.journal.effect("agent", "one", "leverage")["phase"] == "provider"
    engine.venue.submit_action.assert_not_called()


def test_venue_uncertainty_reconciles_client_id_without_resubmission(engine):
    original = engine.venue.submit_action.side_effect

    def lost(action, *args):
        if action["type"] == "order":
            raise MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost response")
        return original(action, *args)

    engine.venue.submit_action.side_effect = lost
    assert engine.submit(intent())["state"] == "unknown"
    assert engine.order("one")["state"] == "open"
    assert engine.venue.submit_action.call_count == 2


def test_restart_does_not_release_old_unused_signature(engine):
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "AWAITING_MFA"
    )
    assert engine.submit(intent())["state"] == "awaiting_approval"
    effect = engine.journal.effect("agent", "one", "leverage")
    engine.provider.signature.return_value = SignatureJob(effect["requestId"], WALLET, "DENIED")
    restarted = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent")
    assert restarted.order("one")["state"] == "rejected"
    engine.venue.submit_action.assert_not_called()


def test_fills_are_cumulative_and_terminal_requires_complete_ledger(engine):
    engine.submit(intent())
    original = engine.venue.order_status.side_effect

    def filled(*args):
        result = original(*args)
        result["order"]["status"] = "filled"
        return result

    engine.venue.order_status.side_effect = filled
    assert engine.order("one")["state"] == "unknown"
    engine.venue.fills.return_value = [
        {
            "tid": 7,
            "oid": 42,
            "coin": "BTC",
            "side": "B",
            "sz": "0.001",
            "px": "19999",
            "fee": "0.01",
            "feeToken": "USDC",
            "time": int(time.time() * 1000),
        }
    ]
    result = engine.order("one")
    assert result["state"] == "filled" and result["filledQuantity"] == "0.001"
    assert result["averagePrice"] == "19999" and result["fees"] == [{"asset": "USDC", "amount": "0.01"}]


def partial_fill(size="0.0004"):
    return {
        "tid": 71,
        "oid": 42,
        "coin": "BTC",
        "side": "B",
        "sz": size,
        "px": "19999",
        "fee": "0.001",
        "feeToken": "USDC",
        "time": int(time.time() * 1000),
    }


def test_open_status_size_is_a_snapshot_live_remainder_proves_partial_execution(engine):
    engine.submit(intent())
    live = dict(engine.venue.order_status(WALLET, 42)["order"]["order"], sz="0.0006")
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = [live]
    engine.venue.fills.return_value = [partial_fill()]
    result = engine.order("one")
    assert result["state"] == "partial_fill" and result["filledQuantity"] == "0.0004"
    assert engine.venue.order_status(WALLET, 42)["order"]["order"]["sz"] == "0.001"
    assert result["fees"] == [{"asset": "USDC", "amount": "0.001"}]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "foreign_cloid",
        "foreign_coin",
        "foreign_side",
        "foreign_size",
        "foreign_reduce_only",
        "negative_remaining",
        "zero_remaining",
        "excess_remaining",
        "lagging_remaining",
    ],
)
def test_open_live_snapshot_must_match_identity_and_complete_fill_ledger(engine, mutation):
    engine.submit(intent())
    live = dict(engine.venue.order_status(WALLET, 42)["order"]["order"], sz="0.0006")
    rows = [live]
    if mutation == "missing":
        rows = []
    elif mutation == "duplicate":
        rows = [live, dict(live)]
    else:
        field, value = {
            "foreign_cloid": ("cloid", "0x" + "f" * 32),
            "foreign_coin": ("coin", "ETH"),
            "foreign_side": ("side", "A"),
            "foreign_size": ("origSz", "0.002"),
            "foreign_reduce_only": ("reduceOnly", True),
            "negative_remaining": ("sz", "-0.0001"),
            "zero_remaining": ("sz", "0"),
            "excess_remaining": ("sz", "0.002"),
            "lagging_remaining": ("sz", "0.001"),
        }[mutation]
        live[field] = value
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = rows
    engine.venue.fills.return_value = [partial_fill()]
    result = engine.order("one")
    assert result["state"] == "unknown" and result["filledQuantity"] == "0"
    assert engine.venue.submit_action.call_count == 2


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("terminal", ["canceled", "rejected"])
def test_closed_order_refreshes_late_terminal_fill_without_mutation_or_replay(engine, restart, terminal):
    engine.submit(intent())
    snapshot = engine.venue.order_status(WALLET, 42)
    snapshot["order"]["status"] = terminal
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = snapshot
    assert engine.order("one")["state"] == terminal
    engine.provider.reset_mock()
    engine.venue.submit_action.reset_mock()
    engine.venue.frontend_open_orders.reset_mock()
    snapshot["order"]["order"]["sz"] = "0.0006"
    engine.venue.fills.return_value = [partial_fill()]
    subject = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent") if restart else engine
    result = subject.order("one")
    assert result["state"] == terminal and result["filledQuantity"] == "0.0004"
    assert result["averagePrice"] == "19999" and result["fees"] == [{"asset": "USDC", "amount": "0.001"}]
    assert subject.order("one")["filledQuantity"] == "0.0004"
    assert len(engine.journal.effect("agent", "one", "fills")["items"]) == 1
    engine.provider.submit_signature.assert_not_called()
    engine.provider.signature.assert_not_called()
    engine.venue.submit_action.assert_not_called()
    engine.venue.frontend_open_orders.assert_not_called()


def test_terminal_released_order_without_recorded_venue_id_refreshes_by_client_id_without_replay(engine):
    engine.submit(intent())
    operation = engine.journal.get("agent", "one")
    operation.pop("providerOrderId")
    operation["state"] = "canceled"
    engine.journal.save(operation)
    effect = engine.journal.effect("agent", "one", "order")
    engine.journal.save_effect(operation, "order", {**effect, "state": "released"})
    snapshot = engine.venue.order_status(WALLET, 42)
    snapshot["order"]["status"] = "canceled"
    snapshot["order"]["order"]["sz"] = "0.0006"
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = snapshot
    engine.venue.fills.return_value = [partial_fill()]
    engine.provider.reset_mock()
    engine.venue.submit_action.reset_mock()
    restored = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent")
    result = restored.order("one")
    assert result["state"] == "canceled" and result["filledQuantity"] == "0.0004"
    assert result["providerOrderId"] == "42"
    engine.venue.order_status.assert_called_with(WALLET, engine.journal.effect("agent", "one", "plan")["clientOrderId"])
    engine.provider.submit_signature.assert_not_called()
    engine.provider.signature.assert_not_called()
    engine.venue.submit_action.assert_not_called()


@pytest.mark.parametrize("contradiction", ["open", "filled", "missing", "incomplete_fills", "read_error"])
def test_terminal_reconciliation_failure_never_reopens_or_erases_known_fills(engine, contradiction):
    engine.submit(intent())
    snapshot = engine.venue.order_status(WALLET, 42)
    snapshot["order"]["status"] = "canceled"
    snapshot["order"]["order"]["sz"] = "0.0006"
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = snapshot
    engine.venue.fills.return_value = [partial_fill()]
    assert engine.order("one")["state"] == "canceled"
    if contradiction in {"open", "filled"}:
        snapshot["order"]["status"] = contradiction
    elif contradiction == "missing":
        engine.venue.order_status.return_value = {"status": "unknownOid"}
    elif contradiction == "incomplete_fills":
        snapshot["order"]["order"]["sz"] = "0.0002"
    else:
        engine.venue.order_status.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "Read timed out")
    result = engine.order("one")
    assert result["state"] == "canceled" and result["filledQuantity"] == "0.0004"
    assert result.get("reconciliationErrorCode")
    assert result["fees"] == [{"asset": "USDC", "amount": "0.001"}]
    assert engine.venue.submit_action.call_count == 2


def test_reduce_only_does_not_mutate_leverage(engine):
    original = engine.venue.order_status.side_effect

    def status(*args):
        result = original(*args)
        result["order"]["order"]["reduceOnly"] = True
        return result

    engine.venue.order_status.side_effect = status
    assert engine.submit(intent(reduceOnly=True))["state"] == "open"
    assert engine.venue.submit_action.call_count == 1
    assert engine.venue.submit_action.call_args.args[0]["type"] == "order"


def test_shared_journal_serializes_multiple_engine_instances(engine):
    second = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent")
    assert second._execution is engine._execution
    with engine._execution:
        with pytest.raises(MetaMaskError, match="EXECUTION_IN_PROGRESS"):
            second.submit(intent())
    assert engine.journal.operations("agent") == []


def test_restart_does_not_release_budget_for_unknown_leverage(engine):
    engine.venue.submit_action.side_effect = MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost response")
    assert engine.submit(intent())["state"] == "unknown"
    restarted = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent")
    assert restarted.order("one")["state"] == "unknown"
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        restarted.submit(intent(id="two"))
    assert engine.venue.submit_action.call_count == 1


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("value", [1, 3])
@pytest.mark.parametrize("method", ["order", "cancel"])
def test_expired_uncertain_leverage_is_reconciled_without_order_replay(engine, monkeypatch, restart, value, method):
    engine.venue.submit_action.side_effect = MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost leverage response")
    engine.submit(intent())
    effect = engine.journal.effect("agent", "one", "leverage")
    engine.venue.leverage.return_value = {"type": "cross", "value": value}
    subject = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent") if restart else engine
    assert subject.order("one")["state"] == "unknown"
    engine.venue.leverage.assert_not_called()
    monkeypatch.setattr(time, "time", lambda: (effect["expiresAfter"] + 1000) / 1000)
    result = getattr(subject, method)("one")
    assert result["state"] == "rejected"
    assert result["filledQuantity"] == "0"
    receipt = engine.journal.effect("agent", "one", "leverage")
    assert receipt["observedLeverage"] == {"type": "cross", "value": value}
    assert receipt["state"] == "reconciled"
    engine.venue.leverage.assert_called_once_with(WALLET, "BTC")
    assert subject.submit(intent())["state"] == "rejected"
    assert engine.venue.submit_action.call_count == 1
    assert engine.provider.submit_signature.call_count == 1
    assert engine.journal.effect("agent", "one", "order") is None
    # Reconciliation frees only the unsubmitted order's reservation, allowing a fresh intent.
    new, created = engine.journal.reserve(
        "agent", intent(id="two"), "20", expected_binding=engine.journal.binding("agent")
    )
    assert created and new["state"] == "prepared"


def test_uncertain_leverage_keeps_hold_when_recovery_read_fails(engine, monkeypatch):
    engine.venue.submit_action.side_effect = MetaMaskError("VENUE_OUTCOME_UNKNOWN", "Lost leverage response")
    engine.submit(intent())
    effect = engine.journal.effect("agent", "one", "leverage")
    monkeypatch.setattr(time, "time", lambda: (effect["expiresAfter"] + 1000) / 1000)
    engine.venue.leverage.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "Read timed out")
    assert engine.order("one")["state"] == "unknown"
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        engine.submit(intent(id="two"))
    assert engine.venue.submit_action.call_count == 1


def test_perp_reservation_is_not_rounded_by_strategy_decimal_context(engine):
    engine.venue.fee_rate.return_value = Decimal("0.00045123456789")
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        result = engine.submit(intent())
    # The reviewed 50-bps mark cushion covers movement while managed signing waits.
    assert Decimal(result["reservedUsd"]) == Decimal("20.10906981481458900")


def test_cumulative_fill_values_and_fees_ignore_strategy_decimal_context(engine):
    engine.submit(intent())
    original = engine.venue.order_status.side_effect

    def filled(*args):
        result = original(*args)
        result["order"]["status"] = "filled"
        return result

    engine.venue.order_status.side_effect = filled
    engine.venue.fills.return_value = [
        {
            "tid": tid,
            "oid": 42,
            "coin": "BTC",
            "side": "B",
            "sz": size,
            "px": price,
            "fee": fee,
            "feeToken": "USDC",
            "time": int(time.time() * 1000),
        }
        for tid, size, price, fee in (
            (1, "0.0004", "19999.1", "0.000123456789"),
            (2, "0.0006", "20000.2", "0.000987654321"),
        )
    ]
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        result = engine.order("one")
    assert result["state"] == "filled"
    assert Decimal(result["averagePrice"]) == Decimal("19999.76")
    assert result["fees"] == [{"asset": "USDC", "amount": "0.001111111110"}]


def test_legacy_partial_fill_reconciles_recorded_venue_id(engine):
    result = engine.submit(intent())
    engine.journal.db.execute("DELETE FROM native_effects WHERE agent='agent' AND operation='one'")
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = {
        "status": "order",
        "order": {
            "status": "marginCanceled",
            "order": {"coin": "BTC", "side": "B", "origSz": "0.001", "sz": "0.0006", "oid": 42, "reduceOnly": False},
        },
    }
    engine.venue.fills.return_value = [
        {
            "tid": 1,
            "oid": 42,
            "coin": "BTC",
            "side": "B",
            "sz": "0.0004",
            "px": "19999",
            "fee": "0.001",
            "feeToken": "USDC",
            "time": int(time.time() * 1000),
        }
    ]
    restored = ExecutionEngine(engine.journal, engine.provider, engine.venue, "agent")
    assert result["providerOrderId"] == "42"
    result = restored.order("one")
    assert result["state"] == "canceled" and result["filledQuantity"] == "0.0004"
    assert result["venueStatus"] == "marginCanceled"


def test_cancel_denial_never_writes_original_order_as_rejected(engine):
    engine.submit(intent())
    original_save = engine.journal.save
    writes = []

    def save(value):
        writes.append(value.copy())
        return original_save(value)

    engine.journal.save = save
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "DENIED"
    )
    result = engine.cancel("one")
    assert result["state"] == "open" and result["cancelState"] == "rejected"
    assert all(row["state"] == "open" for row in writes)
    assert all(row.get("signatureOnly") is False for row in writes)
    first = engine.provider.submit_signature.call_args.args[0]
    engine.cancel("one")
    assert engine.provider.submit_signature.call_args.args[0] != first


def test_cancel_pending_intent_never_advances_it_into_a_trade(engine):
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "AWAITING_MFA"
    )
    engine.submit(intent())
    assert engine.cancel("one")["state"] == "canceled"
    assert engine.order("one")["state"] == "canceled"
    engine.venue.submit_action.assert_not_called()


def test_read_failure_after_venue_acceptance_returns_unknown_not_order_error(engine):
    engine.venue.order_status.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "Read timed out")
    result = engine.submit(intent())
    assert result["state"] == "unknown" and result["providerOrderId"] == "42"
    assert result["lastErrorCode"] == "VENUE_UNAVAILABLE" and result["filledQuantity"] == "0"
    assert engine.venue.submit_action.call_count == 2
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        engine.submit(intent(id="two"))


def test_malformed_acknowledgement_after_release_retains_unknown_and_recovers(engine):
    original = engine.venue.submit_action.side_effect

    def malformed(action, *args):
        if action["type"] == "order":
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": None}}]}}}
        return original(action, *args)

    engine.venue.submit_action.side_effect = malformed
    result = engine.submit(intent())
    assert result["state"] == "unknown" and result["lastErrorCode"] == "INVALID_INTENT"
    result = engine.order("one")
    assert result["state"] == "open" and result["lastErrorCode"] is None
    assert engine.venue.submit_action.call_count == 2


def test_malformed_cancel_acknowledgement_does_not_reject_live_order(engine):
    engine.submit(intent())

    def sign(request_id, wallet, network, data, action):
        signature = "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x")
        return SignatureJob(request_id, wallet, "SIGNED", signature)

    engine.provider.submit_signature.side_effect = sign
    engine.venue.submit_action.side_effect = None
    engine.venue.submit_action.return_value = {"status": "ok", "response": {"type": "cancel", "data": None}}
    result = engine.cancel("one")
    assert result["state"] == "open" and result["cancelState"] == "unknown"
    assert result["cancelErrorCode"] == "INVALID_RESPONSE"


def seed_closed_orders(engine, count=20):
    base = engine.submit(intent())
    for index in range(count):
        identifier = "one" if index == 0 else f"closed-{index}"
        operation = {
            **base,
            "id": identifier,
            "request": intent(id=identifier),
            "state": "canceled",
            "providerOrderId": str(42 + index),
        }
        engine.journal.save(operation)
        engine.journal.save_effect(operation, "fills", {"wallet": WALLET, "network": "testnet", "items": []})

    def status(wallet, reference):
        operation = next(row for row in engine.journal.operations("agent") if row["providerOrderId"] == str(reference))
        plan = engine.journal.effect("agent", operation["id"], "plan")
        return {
            "status": "order",
            "order": {
                "status": "canceled",
                "order": {
                    "coin": "BTC",
                    "side": "B",
                    "origSz": "0.001",
                    "sz": "0.001",
                    "oid": int(reference),
                    "cloid": plan["clientOrderId"] if plan else None,
                    "reduceOnly": False,
                },
            },
        }

    engine.venue.order_status.side_effect = status
    engine.venue.reset_mock()
    engine.provider.reset_mock()


def test_polling_twenty_unchanged_closed_orders_uses_one_fill_read_and_no_status_reads(engine):
    seed_closed_orders(engine)
    results = engine.orders()
    assert len(results) == 20 and all(row["state"] == "canceled" for row in results)
    assert engine.venue.fills.call_count == 1
    engine.venue.order_status.assert_not_called()
    engine.venue.submit_action.assert_not_called()
    engine.provider.submit_signature.assert_not_called()


def test_terminal_batch_discovery_refreshes_only_order_with_new_fill(engine):
    seed_closed_orders(engine)
    original = engine.venue.order_status.side_effect

    def status(*args):
        result = original(*args)
        if args[1] == 49:
            result["order"]["order"]["sz"] = "0.0006"
        return result

    engine.venue.order_status.side_effect = status
    engine.venue.fills.return_value = [{**partial_fill(), "oid": 49}]
    results = engine.orders()
    assert next(row for row in results if row["providerOrderId"] == "49")["filledQuantity"] == "0.0004"
    engine.venue.order_status.assert_called_once_with(WALLET, 49)
    # The same request-local ledger now supplies discovery and the selected reconciliation.
    assert engine.venue.fills.call_count == 1
    engine.venue.reset_mock()
    engine.orders()
    assert engine.venue.fills.call_count == 1
    engine.venue.order_status.assert_not_called()
    engine.venue.submit_action.assert_not_called()


def test_batch_discovery_returns_concurrently_updated_terminal_receipt(engine):
    seed_closed_orders(engine, count=1)

    def discover(*args):
        fill = partial_fill()
        operation = engine.journal.get("agent", "one")
        engine.journal.save_effect(operation, "fills", {"wallet": WALLET, "network": "testnet", "items": [fill]})
        engine.journal.save({**operation, "filledQuantity": "0.0004", "averagePrice": "19999"})
        return [fill]

    engine.venue.fills.side_effect = discover
    result = engine.orders()[0]
    assert result == engine.journal.get("agent", "one")
    assert result["filledQuantity"] == "0.0004"
    engine.venue.order_status.assert_not_called()


def test_terminal_batch_discovery_never_ignores_changed_existing_fill_record(engine):
    seed_closed_orders(engine)
    operation = engine.journal.get("agent", "one")
    engine.journal.save_effect(operation, "fills", {"wallet": WALLET, "network": "testnet", "items": [partial_fill()]})
    engine.venue.fills.return_value = [{**partial_fill(), "fee": "0.002"}]
    result = next(row for row in engine.orders() if row["id"] == "one")
    assert result["state"] == "canceled" and result["reconciliationErrorCode"] == "FILL_REGRESSION"
    engine.venue.order_status.assert_called_once_with(WALLET, 42)


def test_terminal_batch_read_failure_falls_back_and_durably_blocks_new_spending(engine):
    seed_closed_orders(engine)
    engine.venue.fills.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "Read timed out")
    results = engine.orders()
    assert all(row["state"] == "canceled" and row["reconciliationErrorCode"] == "VENUE_UNAVAILABLE" for row in results)
    assert engine.venue.order_status.call_count == 20
    assert engine.venue.fills.call_count == 21
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        engine.journal.reserve("agent", intent(id="blocked"), "1", expected_binding=engine.journal.binding("agent"))


def test_terminal_reconciliation_error_forces_refresh_even_without_new_batch_fills(engine):
    seed_closed_orders(engine)
    operation = engine.journal.get("agent", "one")
    engine.journal.save({**operation, "reconciliationErrorCode": "VENUE_UNAVAILABLE"})
    result = next(row for row in engine.orders() if row["id"] == "one")
    assert result["state"] == "canceled" and result["reconciliationErrorCode"] is None
    engine.venue.order_status.assert_called_once_with(WALLET, 42)


def test_twelve_active_orders_share_fresh_snapshots_with_measured_request_count_reduction(engine):
    seed_closed_orders(engine, count=12)
    for operation in engine.journal.operations("agent"):
        engine.journal.save({**operation, "state": "open"})
    original = engine.venue.order_status.side_effect

    def status(*args):
        result = original(*args)
        result["order"]["status"] = "open"
        return result

    engine.venue.order_status.side_effect = status
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = [status(WALLET, oid)["order"]["order"] for oid in range(42, 54)]
    baseline = [engine.order(row["id"]) for row in engine.journal.operations("agent")]
    assert all(row["state"] == "open" for row in baseline)
    assert (
        engine.venue.frontend_open_orders.call_count,
        engine.venue.fills.call_count,
        engine.venue.order_status.call_count,
    ) == (12, 12, 12)
    engine.venue.reset_mock()
    results = engine.orders()
    assert all(row["state"] == "open" for row in results)
    assert (
        engine.venue.frontend_open_orders.call_count,
        engine.venue.fills.call_count,
        engine.venue.order_status.call_count,
    ) == (1, 1, 12)
    engine.venue.submit_action.assert_not_called()
    engine.provider.submit_signature.assert_not_called()


def test_sweep_snapshot_is_not_used_after_a_new_order_mutation(engine):
    sign = engine.provider.submit_signature.side_effect
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "AWAITING_MFA"
    )
    pending = engine.submit(intent())
    engine.journal.save(
        {**pending, "id": "closed", "request": intent(id="closed"), "state": "canceled", "providerOrderId": "99"}
    )
    effect = engine.journal.effect("agent", "one", "leverage")
    signature = "0x" + SIGNER.sign_message(
        encode_typed_data(full_message=effect["typedData"])
    ).signature.hex().removeprefix("0x")
    engine.provider.signature.return_value = SignatureJob(effect["requestId"], WALLET, "SIGNED", signature)
    engine.provider.submit_signature.side_effect = sign
    live = dict(engine.venue.order_status(WALLET, 42)["order"]["order"], sz="0.0006")
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = [live]
    engine.venue.fills.side_effect = [[], [partial_fill()]]
    results = engine.orders()
    result = next(row for row in results if row["id"] == "one")
    assert result["state"] == "partial_fill" and result["filledQuantity"] == "0.0004"
    assert engine.venue.fills.call_count == 2
    assert engine.venue.submit_action.call_count == 2


def test_sweep_snapshot_is_not_used_after_a_pending_cancel_mutation(engine):
    engine.submit(intent())
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, *_: SignatureJob(
        request_id, wallet, "AWAITING_MFA"
    )
    assert engine.cancel("one")["state"] == "open"
    effect = engine.journal.effect("agent", "one", "cancel-42-1")
    signature = "0x" + SIGNER.sign_message(
        encode_typed_data(full_message=effect["typedData"])
    ).signature.hex().removeprefix("0x")
    engine.provider.signature.return_value = SignatureJob(effect["requestId"], WALLET, "SIGNED", signature)
    snapshot = engine.venue.order_status(WALLET, 42)
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = [dict(snapshot["order"]["order"])]
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = snapshot

    def cancel(*args):
        snapshot["order"]["status"] = "canceled"
        snapshot["order"]["order"]["sz"] = "0.0006"
        engine.venue.fills.return_value = [partial_fill()]
        return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}

    engine.venue.submit_action.side_effect = cancel
    engine.venue.reset_mock()
    result = engine.orders()[0]
    assert result["state"] == "canceled" and result["filledQuantity"] == "0.0004"
    assert engine.venue.fills.call_count == 2
    engine.venue.submit_action.assert_called_once()
    assert engine.venue.submit_action.call_args.args[0]["type"] == "cancel"
