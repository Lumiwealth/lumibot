"""Ordinary strategy decisions; live Strategy/Trader proof is separate."""

from decimal import Decimal
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order, Position
from lumibot.example_strategies.metamask_perpetual import MetaMaskPerpetualRoundTrip
from scripts.qualify_metamask_strategy import stop_strategy


@pytest.fixture
def strategy():
    broker = MetaMask(
        {"NETWORK": "testnet", "WALLET_ADDRESS": "0x" + "1" * 40, "AGENT_ID": "example-agent"},
        connect_stream=False,
        start_orders_thread=False,
    )
    broker.client = Mock()
    broker.client.account.return_value = {"cash": "100", "positionsValue": "0", "equity": "100", "positions": []}
    instance = MetaMaskPerpetualRoundTrip(
        broker=broker,
        name="ordinary example with spaces",
        quote_asset=Asset("USDC", asset_type="crypto"),
        benchmark_asset=None,
        analyze_backtest=False,
    )
    instance.initialize()
    instance.get_orders = Mock(return_value=[])
    instance.get_positions = Mock(return_value=[])
    instance.get_position = Mock(return_value=None)
    instance.get_last_price = Mock(return_value=Decimal("2300"))
    instance.get_cash = Mock(return_value=Decimal("100"))
    instance.submit_order = Mock()
    yield instance
    broker.cleanup_streams()


def observed_order(strategy, *, exit_number=None, quantity="0.005", filled="0.005", status="fill", certainty=None):
    closing = exit_number is not None
    order = Order(
        strategy.name,
        strategy.asset,
        Decimal(quantity),
        "sell" if closing else "buy",
        quote=strategy.quote_asset,
        identifier=f"{strategy.exit_prefix}{exit_number}" if closing else strategy.entry_id,
        order_type="market",
        time_in_force="ioc",
        reduce_only=closing,
    )
    if Decimal(filled):
        order.add_transaction(Decimal("2300"), Decimal(filled))
    order.status = status
    order.submission_outcome = {
        "broker_state_certainty": certainty or ("confirmed_rejected" if status == "error" else "confirmed_submitted"),
        "reconciliation_required": certainty == "unknown",
    }
    return order


def set_position(strategy, quantity):
    position = Position(strategy.name, strategy.asset, Decimal(quantity)) if Decimal(quantity) else None
    strategy.get_positions.return_value = [position] if position else []
    strategy.get_position.return_value = position


def test_one_entry_is_recorded_before_submission_and_queue_delay_never_creates_another(strategy):
    def submit(order):
        assert strategy.vars.entry_requested
        assert strategy.vars.pending_id == order.identifier == strategy.entry_id

    strategy.submit_order.side_effect = submit
    strategy.on_trading_iteration()
    strategy.on_trading_iteration()
    strategy.on_trading_iteration()
    strategy.submit_order.assert_called_once()
    opening = strategy.submit_order.call_args.args[0]
    assert opening.quantity == Decimal("0.005") and opening.asset.asset_type == "crypto_future"
    assert not opening.reduce_only and opening.time_in_force == "ioc"


def test_entry_uses_inherited_strategy_validation_and_public_submission(strategy):
    del strategy.submit_order
    strategy.broker.submit_order = Mock(side_effect=lambda order, **kwargs: order)
    strategy.on_trading_iteration()
    strategy.broker.submit_order.assert_called_once()
    opening = strategy.broker.submit_order.call_args.args[0]
    assert opening.identifier == strategy.entry_id and opening.quantity == Decimal("0.005")


@pytest.mark.parametrize("status", ["submitted", "partial_fill", "unknown", "canceled"])
def test_unresolved_entry_never_creates_a_replacement_or_exit(strategy, status):
    order = observed_order(strategy, filled="0.0002", status=status, certainty="unknown")
    strategy.get_orders.return_value = [order]
    set_position(strategy, "0.0002")
    strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()
    assert not strategy.vars.completed


@pytest.mark.parametrize("filled,status", [("0.005", "fill"), ("0.0002", "canceled")])
def test_terminal_entry_closes_only_actual_cumulative_execution(strategy, filled, status):
    strategy.get_orders.return_value = [observed_order(strategy, filled=filled, status=status)]
    set_position(strategy, filled)
    strategy.on_trading_iteration()
    closing = strategy.submit_order.call_args.args[0]
    assert closing.quantity == Decimal(filled) and closing.reduce_only
    assert closing.side == "sell" and closing.identifier == f"{strategy.exit_prefix}1"
    assert strategy.vars.pending_id == closing.identifier


def test_known_partial_exit_creates_a_new_identity_for_only_the_verified_residual(strategy):
    entry = observed_order(strategy, filled="0.0002", status="canceled")
    first_exit = observed_order(strategy, exit_number=1, quantity="0.0002", filled="0.0001", status="canceled")
    strategy.get_orders.return_value = [first_exit, entry]
    set_position(strategy, "0.0001")
    strategy.on_trading_iteration()
    closing = strategy.submit_order.call_args.args[0]
    assert closing.quantity == Decimal("0.0001")
    assert closing.identifier == f"{strategy.exit_prefix}2" and closing.reduce_only


def test_rejected_entry_is_not_reported_as_a_successful_round_trip(strategy):
    strategy.get_orders.return_value = [observed_order(strategy, filled="0", status="error")]
    strategy.on_trading_iteration()
    assert strategy.vars.completed and strategy.vars.result == "entry_not_filled"
    strategy.submit_order.assert_not_called()


def test_zero_fill_rejected_exit_is_reported_without_automatic_replacement(strategy):
    strategy.get_orders.return_value = [
        observed_order(strategy),
        observed_order(strategy, exit_number=1, filled="0", status="error"),
    ]
    set_position(strategy, "0.005")
    with pytest.raises(RuntimeError, match="Exit did not execute"):
        strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()


def test_restart_recovers_finished_round_trip_without_reentering(strategy):
    entry, closing = observed_order(strategy), observed_order(strategy, exit_number=1)
    entry_id, exit_prefix = strategy.entry_id, strategy.exit_prefix
    strategy.initialize()
    assert strategy.entry_id == entry_id and strategy.exit_prefix == exit_prefix
    strategy.get_orders.return_value = [closing, entry]
    strategy.on_trading_iteration()
    strategy.on_trading_iteration()
    assert strategy.vars.completed and strategy.vars.result == "completed"
    strategy.submit_order.assert_not_called()


def test_restart_waits_for_existing_unsettled_exit_without_submitting_another(strategy):
    entry = observed_order(strategy)
    closing = observed_order(strategy, exit_number=1, filled="0", status="open")
    strategy.initialize()
    assert strategy.vars.pending_id is None
    strategy.get_orders.return_value = [entry, closing]
    set_position(strategy, "0.005")
    strategy.on_trading_iteration()
    strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()
    assert not strategy.vars.completed


def test_completed_round_trip_continues_reconciling_late_entry_fills_without_reentry(strategy):
    entry = observed_order(strategy, filled="0.0002", status="canceled")
    closing = observed_order(strategy, exit_number=1, quantity="0.0002", filled="0.0002")
    snapshots = [
        {
            "id": str(item.identifier),
            "request": {
                "kind": "perp",
                "symbol": "ETH",
                "quantity": str(item.quantity),
                "side": str(item.side),
                "orderType": "market",
                "leverage": 1,
                "tif": "ioc",
                "reduceOnly": item.reduce_only,
            },
            "state": state,
            "filledQuantity": "0.0002",
            "averagePrice": "2300",
        }
        for item, state in ((entry, "canceled"), (closing, "filled"))
    ]
    # Exercise native snapshots through the real broker/Strategy refresh, not injected transactions.
    del strategy.get_orders
    strategy.broker.client.orders.return_value = snapshots
    strategy.on_trading_iteration()
    assert strategy.vars.completed
    strategy.submit_order.assert_not_called()
    tracked_entry = strategy.broker.get_tracked_order(entry.identifier)
    assert tracked_entry is not entry and tracked_entry.is_canceled()
    snapshots[0] = {**snapshots[0], "filledQuantity": "0.0003"}
    set_position(strategy, "0.0001")
    strategy.on_trading_iteration()
    assert not strategy.vars.completed
    assert tracked_entry.is_canceled()
    assert sum(Decimal(str(fill.quantity)) for fill in tracked_entry.transactions) == Decimal("0.0003")
    extra_close = strategy.submit_order.call_args.args[0]
    assert extra_close.quantity == Decimal("0.0001")
    assert extra_close.identifier == f"{strategy.exit_prefix}2" and extra_close.reduce_only


def test_completed_round_trip_does_not_hide_an_unexplained_later_position(strategy):
    strategy.get_orders.return_value = [observed_order(strategy), observed_order(strategy, exit_number=1)]
    strategy.on_trading_iteration()
    assert strategy.vars.completed
    set_position(strategy, "0.001")
    with pytest.raises(RuntimeError, match="exact residual fills"):
        strategy.on_trading_iteration()
    assert not strategy.vars.completed
    strategy.submit_order.assert_not_called()


def test_changed_parameters_cannot_adopt_or_close_a_different_existing_intent(strategy):
    entry = observed_order(strategy)
    strategy.parameters["quantity"] = "0.006"
    strategy.initialize()
    strategy.get_orders.return_value = [entry]
    set_position(strategy, "0.005")
    with pytest.raises(RuntimeError, match="conflicts with the strategy parameters"):
        strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()


@pytest.mark.parametrize("quantity", ["0.0049", "-0.005"])
def test_position_mismatch_is_never_liquidated_or_claimed_complete(strategy, quantity):
    strategy.get_orders.return_value = [observed_order(strategy)]
    set_position(strategy, quantity)
    with pytest.raises(RuntimeError, match="exact residual fills"):
        strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()


def test_unrelated_position_blocks_both_new_entry_and_completion(strategy):
    foreign = Position(strategy.name, Asset("BTC", asset_type="crypto_future"), Decimal("0.001"))
    strategy.get_positions.return_value = [foreign]
    with pytest.raises(RuntimeError, match="flat dedicated wallet"):
        strategy.on_trading_iteration()
    strategy.get_orders.return_value = [observed_order(strategy), observed_order(strategy, exit_number=1)]
    with pytest.raises(RuntimeError, match="unrelated position"):
        strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()


def test_missing_exit_generation_is_not_adopted(strategy):
    strategy.get_orders.return_value = [observed_order(strategy), observed_order(strategy, exit_number=2)]
    with pytest.raises(RuntimeError, match="exit history is missing"):
        strategy.on_trading_iteration()
    strategy.submit_order.assert_not_called()


def test_completion_requires_a_fresh_cash_read(strategy):
    strategy.get_orders.return_value = [observed_order(strategy), observed_order(strategy, exit_number=1)]
    strategy.get_cash.return_value = None
    with pytest.raises(RuntimeError, match="Fresh cash"):
        strategy.on_trading_iteration()
    assert not strategy.vars.completed


@pytest.mark.parametrize("quantity", ["0", "-1", "NaN", "Infinity"])
def test_quantity_validation_prevents_any_entry(strategy, quantity):
    strategy.parameters["quantity"] = quantity
    with pytest.raises(ValueError, match="finite, positive"):
        strategy.initialize()
    strategy.submit_order.assert_not_called()


def test_shutdown_waits_for_user_iteration_before_allowing_cleanup():
    release, idle, finished = Event(), Event(), Event()

    def iteration():
        release.wait(5)
        idle.set()

    worker = Thread(target=iteration)
    worker.start()
    trader, broker, evidence = Mock(), Mock(), Mock()
    broker._orders_queue.join.side_effect = lambda: finished.set() if idle.is_set() else None
    owner = Thread(target=stop_strategy, args=(trader, SimpleNamespace(_executor=worker), broker, idle, evidence))
    try:
        owner.start()
        assert not finished.wait(0.05)
        release.set()
        owner.join(2)
        assert not owner.is_alive() and finished.is_set()
        assert not worker.is_alive()
    finally:
        release.set()
        worker.join(2)
        owner.join(2)


def test_shutdown_diagnostic_failure_never_bypasses_producer_barrier():
    idle, evidence, broker = Mock(), Mock(), Mock()
    idle.wait.side_effect = [False, True]
    evidence.stage.side_effect = OSError("offline evidence-write failure")
    worker = Mock()
    worker.is_alive.return_value = False
    failure = stop_strategy(Mock(), SimpleNamespace(_executor=worker), broker, idle, evidence)
    assert isinstance(failure, OSError)
    assert idle.wait.call_count == 2
    broker._orders_queue.join.assert_called_once()
