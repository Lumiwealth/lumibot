"""Wallet execution checkpoints and recovery, with offline ECDSA signatures."""

import time
from decimal import Decimal
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


def test_signature_uncertainty_polls_original_id_without_another_post(engine):
    engine.provider.submit_signature.side_effect = MetaMaskError("REQUEST_OUTCOME_UNKNOWN", "Lost response")
    assert engine.submit(intent())["state"] == "unknown"
    effect = engine.journal.effect("agent", "one", "leverage")
    engine.provider.signature.return_value = SignatureJob(effect["requestId"], WALLET, "SIGNING")
    assert engine.order("one")["state"] == "unknown"
    assert engine.provider.submit_signature.call_count == 1
    engine.venue.submit_action.assert_not_called()


def test_revocation_after_provider_signing_prevents_release(engine):
    original = engine.provider.submit_signature.side_effect

    def revoke(*args):
        result = original(*args)
        engine.journal.bind({**engine.journal.binding("agent"), "enabled": False})
        return result

    engine.provider.submit_signature.side_effect = revoke
    assert engine.submit(intent())["state"] == "rejected"
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
