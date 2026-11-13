"""Native client dispatch and real broker polling, with offline venue/provider transports."""

from contextlib import closing
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from eth_account.messages import encode_typed_data

from lumibot.brokers import MetaMask
from lumibot.tools.metamask.journal import Journal
from lumibot.tools.metamask.provider import GuardPolicy, MetaMaskError, SignatureJob
from lumibot.tools.metamask_client import MetaMaskClient
from tests.test_metamask_native_engine import SIGNER, WALLET, intent, partial_fill, seed_closed_orders
from tests.test_metamask_native_engine import engine as engine


@pytest.fixture
def native_broker(engine):
    broker = MetaMask(
        {"NETWORK": "testnet", "AGENT_ID": "agent", "WALLET_ADDRESS": WALLET},
        connect_stream=False,
        start_orders_thread=False,
    )
    # Keep the real client/engine/broker dispatch; replace only remote transports.
    broker.client._engine = engine
    broker.client._journal = engine.journal
    broker.client._provider = engine.provider
    broker.client._venue = engine.venue
    yield broker
    broker._stop_event.set()


def active_orders(engine, count=12):
    seed_closed_orders(engine, count=count)
    for row in engine.journal.operations("agent"):
        engine.journal.save({**row, "state": "open"})
    original = engine.venue.order_status.side_effect

    def status(*args):
        result = original(*args)
        result["order"]["status"] = "open"
        return result

    engine.venue.order_status.side_effect = status
    engine.venue.frontend_open_orders.side_effect = None
    engine.venue.frontend_open_orders.return_value = [
        status(WALLET, oid)["order"]["order"] for oid in range(42, 42 + count)
    ]


def restore_orders(broker, engine):
    for row in engine.journal.operations("agent"):
        broker._parse_broker_order(row, "native-polling")


def test_real_broker_polling_batches_twelve_active_orders(native_broker, engine):
    active_orders(engine)
    restore_orders(native_broker, engine)
    engine.orders = Mock(wraps=engine.orders)
    native_broker._process_trade_event = Mock(wraps=native_broker._process_trade_event)
    before = engine.journal.operations("agent")
    native_broker.do_polling()
    assert (
        engine.venue.frontend_open_orders.call_count,
        engine.venue.fills.call_count,
        engine.venue.order_status.call_count,
    ) == (1, 1, 12)
    engine.orders.assert_called_once_with()
    native_broker._process_trade_event.assert_not_called()
    engine.venue.submit_action.assert_not_called()
    engine.provider.submit_signature.assert_not_called()
    assert [row["reservedUsd"] for row in engine.journal.operations("agent")] == [row["reservedUsd"] for row in before]


def test_real_broker_polling_discovers_twenty_closed_orders_in_one_read(native_broker, engine):
    seed_closed_orders(engine)
    restore_orders(native_broker, engine)
    native_broker._process_trade_event = Mock(wraps=native_broker._process_trade_event)
    native_broker.do_polling()
    assert engine.venue.fills.call_count == 1
    engine.venue.order_status.assert_not_called()
    engine.venue.frontend_open_orders.assert_not_called()
    native_broker._process_trade_event.assert_not_called()
    engine.venue.submit_action.assert_not_called()
    engine.provider.submit_signature.assert_not_called()


def test_real_client_polling_delivers_new_partial_fill_once(native_broker, engine):
    active_orders(engine, count=1)
    restore_orders(native_broker, engine)
    subscriber = SimpleNamespace(
        name="native-polling",
        add_event=Mock(),
        NEW_ORDER="new",
        PARTIALLY_FILLED_ORDER="partial_fill",
        FILLED_ORDER="fill",
        CANCELED_ORDER="canceled",
    )
    native_broker._add_subscriber(subscriber)
    engine.venue.frontend_open_orders.return_value[0]["sz"] = "0.0006"
    engine.venue.fills.return_value = [partial_fill()]
    native_broker.do_polling()
    native_broker.do_polling()
    subscriber.add_event.assert_called_once()
    assert subscriber.add_event.call_args.args[0] == "partial_fill"
    assert Decimal(str(subscriber.add_event.call_args.args[1]["quantity"])) == Decimal("0.0004")
    assert engine.venue.fills.call_count == 2
    assert engine.venue.frontend_open_orders.call_count == 2
    engine.venue.submit_action.assert_not_called()


def test_client_recovers_unknown_venue_order_without_replaying_and_preserves_not_found(native_broker, engine):
    active_orders(engine, count=1)
    operation = engine.journal.get("agent", "one")
    engine.journal.save({**operation, "state": "unknown"})
    result = native_broker.client.orders()[0]
    assert result["state"] == "open" and result["providerOrderId"] == "42"
    with pytest.raises(MetaMaskError, match="NOT_FOUND"):
        native_broker.client.get_order("missing")
    engine.venue.submit_action.assert_not_called()
    engine.provider.submit_signature.assert_not_called()


@pytest.mark.parametrize("legacy_effects", [False, True])
@pytest.mark.parametrize("registry", ["duplicate", "unapproved_chain"])
def test_upgrade_legacy_registry_retains_exact_order_cancel(native_broker, engine, legacy_effects, registry):
    engine.submit(intent())
    token = {"symbol": "TOKEN", "chainId": 421614, "address": "0x" + "2" * 40, "decimals": 6}
    assets = [token, {**token, "symbol": "ALIAS"}] if registry == "duplicate" else [{**token, "chainId": 1}]
    saved = {**engine.journal.binding("agent"), "assets": assets}
    engine.journal.bind(saved)
    venue_status = engine.venue.order_status(WALLET, 42)
    engine.venue.order_status.side_effect = None
    engine.venue.order_status.return_value = venue_status
    if legacy_effects:
        engine.journal.db.execute("DELETE FROM native_effects WHERE agent='agent' AND operation='one'")
    client = native_broker.client
    client._engine = None  # Exercise startup verification, not an already initialized engine.
    assert client.get_order("one")["state"] == "open"
    assert client.orders()[0]["providerOrderId"] == "42"
    before = engine.journal.get("agent", "one")
    with pytest.raises(MetaMaskError, match="INVALID_BINDING"):
        client.submit(intent(id="new"))
    with pytest.raises(MetaMaskError, match="INVALID_BINDING"):
        client.authorize(saved)
    from lumibot.tools.metamask.evm import EvmWallet

    with closing(EvmWallet(engine.journal, engine.provider, client.config)) as evm:
        with pytest.raises(MetaMaskError, match="INVALID_BINDING"):
            evm.account()
    assert engine.journal.get("agent", "new") is None
    engine.provider.submit_signature.side_effect = lambda request_id, wallet, network, data, action: SignatureJob(
        request_id,
        wallet,
        "SIGNED",
        "0x" + SIGNER.sign_message(encode_typed_data(full_message=data)).signature.hex().removeprefix("0x"),
    )

    def cancel(action, *args):
        assert action == {"type": "cancel", "cancels": [{"a": 0, "o": 42}]}
        venue_status["order"]["status"] = "canceled"
        return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}

    engine.venue.submit_action.side_effect = cancel
    result = client.cancel("one")
    assert result["state"] == "canceled" and result["providerOrderId"] == "42"
    assert result["reservedUsd"] == before["reservedUsd"] and result["filledQuantity"] == "0"
    assert engine.journal.binding("agent") == saved
    assert engine.provider.submit_signature.call_count == 3  # Two entry steps, one exact cancellation.
    assert engine.venue.submit_action.call_count == 3


@pytest.mark.parametrize(
    "changed,error",
    [
        ("wallet", "IDENTITY_MISMATCH"),
        ("network", "IDENTITY_MISMATCH"),
        ("projectId", "PROJECT_MISMATCH"),
        ("policy", "POLICY_CHANGED"),
    ],
)
def test_legacy_registry_does_not_bypass_cancel_authority(native_broker, engine, changed, error):
    engine.submit(intent())
    token = {"symbol": "TOKEN", "chainId": 421614, "address": "0x" + "2" * 40, "decimals": 6}
    saved = {**engine.journal.binding("agent"), "assets": [token, {**token, "symbol": "ALIAS"}]}
    if changed == "policy":
        engine.provider.policy.return_value = GuardPolicy("policy", "b" * 64)
    elif changed == "projectId":
        saved[changed] = "other"
    elif changed == "wallet":
        native_broker.client.config["WALLET_ADDRESS"] = "0x" + "3" * 40
    else:
        native_broker.client.network = "mainnet"
    engine.journal.bind(saved)
    native_broker.client._engine = None
    engine.provider.submit_signature.reset_mock()
    engine.venue.submit_action.reset_mock()
    with pytest.raises(MetaMaskError, match=error):
        native_broker.client.cancel("one")
    engine.provider.submit_signature.assert_not_called()
    engine.venue.submit_action.assert_not_called()


def mixed_journal(engine):
    seed_closed_orders(engine, count=1)
    base = engine.journal.get("agent", "one")
    for identifier, kind in (("swap-one", "swap"), ("perp-two", "perp"), ("swap-two", "swap"), ("legacy", None)):
        request = intent(id=identifier)
        if kind is None:
            request.pop("kind")
        else:
            request["kind"] = kind
        row = {**base, "id": identifier, "request": request, "providerOrderId": "43" if kind == "perp" else None}
        if kind == "swap":
            row.update(state="unknown", providerOrderId="999", createdAt=1)
        engine.journal.save(row)


def test_client_preserves_mixed_journal_order_and_legacy_dispatch(native_broker, engine):
    mixed_journal(engine)
    client = native_broker.client
    client._evm = Mock()
    client._evm.order.side_effect = lambda identifier: {
        **engine.journal.get("agent", identifier),
        "message": "EVM reconciled",
    }
    engine.orders = Mock(wraps=engine.orders)
    engine.order = Mock(wraps=engine.order)
    results = client.orders()
    assert [row["id"] for row in results] == ["one", "swap-one", "perp-two", "swap-two", "legacy"]
    assert [row["message"] for row in results if row["request"].get("kind") == "swap"] == [
        "EVM reconciled",
        "EVM reconciled",
    ]
    assert [call.args[0] for call in client._evm.order.call_args_list] == ["swap-one", "swap-two"]
    engine.orders.assert_called_once_with()
    assert all(call.args[0] not in {"swap-one", "swap-two"} for call in engine.order.call_args_list)
    assert any(call.args[0] == "legacy" for call in engine.order.call_args_list)
    engine.venue.frontend_open_orders.assert_not_called()
    engine.venue.submit_action.assert_not_called()


def test_engine_bulk_discovery_excludes_swap_rows_and_their_history_window(engine):
    mixed_journal(engine)
    results = engine.orders()
    assert [row["id"] for row in results] == ["one", "perp-two", "legacy"]
    engine.venue.frontend_open_orders.assert_not_called()
    assert engine.venue.fills.call_args.args[1] == engine.journal.get("agent", "one")["createdAt"] - 5000
    engine.venue.submit_action.assert_not_called()


@pytest.mark.parametrize("mode", ["spot", "perp"])
def test_swap_only_and_empty_journals_do_not_initialize_hyperliquid(tmp_path, mode):
    client = MetaMaskClient({"NETWORK": "testnet", "AGENT_ID": "agent", "MODE": mode})
    with closing(Journal(tmp_path / "only-swaps.sqlite")) as journal:
        client._journal = journal
        assert client.orders() == []
        row = {
            "id": "swap",
            "agent": "agent",
            "request": {"kind": "swap", "id": "swap"},
            "createdAt": 1,
            "updatedAt": 1,
            "state": "unknown",
            "reservedUsd": "10",
            "filledQuantity": "0",
        }
        journal.save(row)
        client._evm = Mock()
        client._evm.order.return_value = row
        assert client.orders() == [row]
        client._evm.order.assert_called_once_with("swap")
        assert client._engine is None and client._venue is None and client._provider is None


def test_client_batch_read_failure_keeps_budget_guard_and_never_replays(native_broker, engine):
    seed_closed_orders(engine, count=2)
    engine.venue.fills.side_effect = MetaMaskError("VENUE_UNAVAILABLE", "Offline failure")
    results = native_broker.client.orders()
    assert all(row["state"] == "canceled" and row["reconciliationErrorCode"] == "VENUE_UNAVAILABLE" for row in results)
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        engine.journal.reserve("agent", intent(id="blocked"), "1", expected_binding=engine.journal.binding("agent"))
    engine.provider.submit_signature.assert_not_called()
    engine.venue.submit_action.assert_not_called()
