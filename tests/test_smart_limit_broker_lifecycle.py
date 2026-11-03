"""Exercise native SmartLimit and Alpaca observations without a live account."""

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus
from alpaca.trading.models import Order as ProviderOrder
from requests import HTTPError, Response

from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset, Order, SmartLimitConfig
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


def _response(number=1, *, status="new", filled="0", quantity="3", price=None, **fields):
    at = datetime(2027, 1, 4, tzinfo=timezone.utc)
    # Actual alpaca-py response model, with neutral synthetic values.
    return ProviderOrder(
        id=UUID(int=number), client_order_id=f"neutral-{number}", asset_id=UUID(int=1000),
        created_at=at, updated_at=at, submitted_at=at, symbol="SYN", asset_class="us_equity",
        qty=quantity, filled_qty=filled, filled_avg_price=price, order_class="simple",
        type="limit", side="buy", time_in_force="day", status=status, extended_hours=False,
        limit_price="100", **fields,
    )


class _Strategy(Strategy):
    def __init__(self, broker):
        self.broker = broker
        self._name = "neutral"
        self.logger = MagicMock()

    def log_message(self, *args, **kwargs):
        pass

    def get_quote(self, *args, **kwargs):
        return SimpleNamespace(bid=99, ask=101)


def _harness():
    client = MagicMock(spec=TradingClient)
    broker = Alpaca(
        {"PAPER": True}, trading_client=client,
        data_source=AlpacaData({"OAUTH_TOKEN": "synthetic-provider-token"}),
        connect_stream=False, start_orders_thread=False,
    )
    strategy = _Strategy(broker)
    executor = StrategyExecutor(strategy)
    broker._add_subscriber(SimpleNamespace(
        name="neutral", FILLED_ORDER=broker.FILLED_ORDER, CANCELED_ORDER=broker.CANCELED_ORDER,
        PARTIALLY_FILLED_ORDER=broker.PARTIALLY_FILLED_ORDER, ERROR_ORDER=broker.ERROR_ORDER, add_event=MagicMock(),
    ))
    return broker, client, executor


def _working_order(broker, number=1, *, quantity=3):
    config = SmartLimitConfig(preset="fast", step_seconds=1, final_hold_seconds=2)
    order = Order(
        "neutral", Asset("SYN"), quantity, "buy", identifier=UUID(int=number),
        order_type=Order.OrderType.SMART_LIMIT, smart_limit=config, limit_price=100,
    )
    broker._process_new_order(order)
    order._smart_limit_state = {
        "created_at": 0, "step_index": 0, "steps": config.get_step_count(),
        "step_seconds": 1, "final_hold_seconds": 2,
    }
    return order


@pytest.mark.parametrize("status,filled,price", [
    (OrderStatus.FILLED, "3", "101"),
    (OrderStatus.CANCELED, "0", None),
    (OrderStatus.CANCELED, "1", "101"),
    (OrderStatus.EXPIRED, "0", None),
    (OrderStatus.REJECTED, "0", None),
])
def test_modify_reconciles_provider_terminal_status_instead_of_replacing(status, filled, price):
    broker, client, _ = _harness()
    order = _working_order(broker)
    client.get_order_by_id.return_value = _response(status=status, filled=filled, price=price)

    broker.modify_order(order, limit_price=101)

    client.replace_order_by_id.assert_not_called()
    assert order._closed_event.is_set()
    assert not order.is_active()
    assert sum(t.quantity for t in order.transactions) == int(filled)
    assert broker.get_active_tracked_orders("neutral") == []


def test_scheduled_drain_reconciles_six_confirmed_cancellations_without_stream(monkeypatch):
    broker, client, executor = _harness()
    orders = [_working_order(broker, number) for number in range(1, 7)]
    canceled = set()
    client.cancel_order_by_id.side_effect = lambda identifier: canceled.add(str(identifier))
    client.get_order_by_id.side_effect = lambda identifier, **kwargs: _response(
        int(UUID(str(identifier))), status="canceled" if str(identifier) in canceled else "new",
    )
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 5)

    # Only the server response changes. No stream, local status assignment, or
    # direct test-side reconciliation completes these orders for the executor.
    executor._process_run_once_work()
    executor._process_run_once_work()

    assert len(canceled) == 6
    assert executor._scheduled_pending_work() == {}
    assert all(order._closed_event.is_set() for order in orders)


def test_fill_winning_reprice_race_is_not_resubmitted(monkeypatch):
    broker, client, executor = _harness()
    order = _working_order(broker)
    current = [_response()]
    client.get_order_by_id.side_effect = lambda *args, **kwargs: current[0]

    def rejected_replace(**kwargs):
        current[0] = _response(status="filled", filled="3", price="101")
        raise RuntimeError("order filled before replacement reached venue")

    client.replace_order_by_id.side_effect = rejected_replace
    rejected = Response()
    rejected.status_code = 422
    client.cancel_order_by_id.side_effect = APIError(
        '{"code":42210000,"message":"order is already filled"}', HTTPError(response=rejected),
    )
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 1.1)

    executor._process_smart_limit_orders()
    executor._process_smart_limit_orders()

    client.submit_order.assert_not_called()
    assert order.is_filled()
    assert sum(t.quantity for t in order.transactions) == 3
    # Live position quantities are refreshed from the broker separately, not
    # fabricated by this order-only provider fixture.
    assert [o for o in broker.get_all_orders() if o.is_filled()] == [order]


def test_ambiguous_modify_and_cancel_do_not_create_second_order(monkeypatch):
    broker, client, executor = _harness()
    _working_order(broker)
    client.get_order_by_id.return_value = _response(status="pending_cancel")
    client.replace_order_by_id.side_effect = RuntimeError("replace response lost")
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 1.1)

    executor._process_smart_limit_orders()
    executor._process_smart_limit_orders()

    client.submit_order.assert_not_called()
    assert executor._scheduled_pending_work()


@pytest.mark.parametrize("late_fill", [False, True])
def test_pending_cancel_is_refreshed_without_resubmission(monkeypatch, late_fill):
    broker, client, executor = _harness()
    order = _working_order(broker)
    observed = broker._parse_broker_order(_response(status="pending_cancel"), "neutral")
    broker._apply_direct_order_observation(order, observed)
    client.get_order_by_id.return_value = _response(
        status="filled" if late_fill else "canceled", filled="3" if late_fill else "0",
        price="101" if late_fill else None,
    )
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 5)
    executor._process_run_once_work()
    assert executor._scheduled_pending_work() == {}
    assert order._closed_event.is_set()
    client.submit_order.assert_not_called()
    client.cancel_order_by_id.assert_not_called()


@pytest.mark.parametrize("old_replaced", [False, True])
def test_confirmed_partial_cancel_submits_only_remainder_once(monkeypatch, old_replaced):
    broker, client, executor = _harness()
    order = _working_order(broker)
    order.custom_params = {"client_order_id": "original-client-id"}
    order._smart_limit_state["final_hold_seconds"] = 999
    canceled = [False]
    clock = [1.1]

    def read(identifier, **kwargs):
        if str(identifier) == str(UUID(int=9)):
            return _response(9, quantity="2")
        if old_replaced and str(identifier) == str(UUID(int=1)):
            return _response(status="replaced", replaced_by=UUID(int=2))
        return _response(
            2 if old_replaced else 1, status="canceled" if canceled[0] else "partially_filled",
            filled="1", price="101",
        )

    client.get_order_by_id.side_effect = read
    client.replace_order_by_id.side_effect = RuntimeError("replace response unavailable")
    client.cancel_order_by_id.side_effect = lambda identifier: None
    def submit(**kwargs):
        # Later ordinary native modifications work; only the original order's
        # replace attempt failed. Responses retain their exact requested IDs.
        client.replace_order_by_id.side_effect = None
        client.replace_order_by_id.return_value = _response(9, quantity="2")
        return _response(9, quantity="2")
    client.submit_order.side_effect = submit
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: clock[0])
    executor._process_smart_limit_orders()
    assert order.is_active()
    client.submit_order.assert_not_called()
    canceled[0] = True
    clock[0] = 7
    executor._process_smart_limit_orders()
    executor._process_smart_limit_orders()

    client.submit_order.assert_called_once()
    payload = client.submit_order.call_args.kwargs["order_data"].to_request_fields()
    assert Decimal(payload["qty"]) == Decimal("2")
    assert payload["client_order_id"] != "original-client-id"
    assert order.quantity == 3
    assert sum(t.quantity for t in order.transactions) == 1
    replacement = broker.get_tracked_order(UUID(int=9))
    assert replacement is not order
    assert replacement.quantity == 2
    assert replacement.transactions == []
    assert replacement.order_type == Order.OrderType.SMART_LIMIT
    assert executor._smart_limit_pending_replacements == {}


def test_unreadable_replacement_chain_never_authorizes_new_purchase(monkeypatch):
    broker, client, executor = _harness()
    _working_order(broker)
    client.get_order_by_id.side_effect = lambda identifier, **kwargs: (
        _response(status="replaced", replaced_by=UUID(int=2))
        if str(identifier) == str(UUID(int=1)) else None
    )
    client.replace_order_by_id.side_effect = RuntimeError("replace outcome unknown")
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 1.1)
    executor._process_smart_limit_orders()
    client.submit_order.assert_not_called()
    assert executor._scheduled_pending_work()


def test_cancellation_observation_with_partial_fill_keeps_one_execution_receipt(monkeypatch):
    broker, client, executor = _harness()
    order = _working_order(broker)
    client.get_order_by_id.return_value = _response(status="canceled", filled="1", price="101")
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: 5)
    executor._process_run_once_work()
    executor._process_run_once_work()
    assert order.is_canceled()
    assert sum(t.quantity for t in order.transactions) == 1
    assert executor._scheduled_pending_work() == {}
    client.submit_order.assert_not_called()


def test_uncertain_remainder_submission_is_not_retried_with_fresh_identity(monkeypatch):
    broker, client, executor = _harness()
    order = _working_order(broker)
    order._smart_limit_state["final_hold_seconds"] = 999
    current = [_response()]
    client.get_order_by_id.side_effect = lambda *args, **kwargs: current[0]
    client.replace_order_by_id.side_effect = RuntimeError("modify unsupported")
    client.cancel_order_by_id.side_effect = lambda identifier: current.__setitem__(0, _response(status="canceled"))
    client.submit_order.side_effect = TimeoutError("submission response lost")
    clock = [1.1]
    monkeypatch.setattr("lumibot.strategies.strategy_executor.time.monotonic", lambda: clock[0])
    executor._process_smart_limit_orders()
    clock[0] = 7
    executor._process_smart_limit_orders()
    client.submit_order.assert_called_once()
    assert executor._scheduled_pending_work()["smart_limit_replacements"] == 1
