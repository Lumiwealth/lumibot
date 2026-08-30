"""Opt-in paper test for IBKR REST BRACKET, OTO, and OCO order lifecycles.

This module is excluded from ordinary CI by the existing ``apitest`` marker.
It must never be run without the test-scoped paper-account fixture below.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field

import pytest

from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST
from lumibot.entities import Asset, Order
from tests.ibkr_rest_paper_order_safety import ibkr_rest_paper_order_data_source


pytestmark = [pytest.mark.apitest, pytest.mark.ibkr]

_STRATEGY_NAME = "ibkr_rest_advanced_orders_paper_apitest"
_POLL_INTERVAL_SECONDS = 0.5
_VISIBLE_TIMEOUT_SECONDS = 20.0
_CANCEL_TIMEOUT_SECONDS = 30.0
_TERMINAL_STATUSES = {
    "apicancelled",
    "cancelled",
    "canceled",
    "expired",
    "inactive",
    "rejected",
}
_FILLED_STATUSES = {"fill", "filled"}


@dataclass
class _ScenarioRecord:
    name: str
    expected_native_count: int
    acknowledged_ids: list[str] = field(default_factory=list)
    response_entry_count: int = 0
    cleanup_attempted_ids: set[str] = field(default_factory=set)
    confirmation_count_before: int = 0
    cancellation_count_before: int = 0
    polling_outcome: str = "not_reached"
    cancellation_outcome: str = "not_reached"

    @property
    def acceptance_outcome(self) -> str:
        acknowledged_count = len(self.acknowledged_ids)
        if acknowledged_count == 0:
            return "no_order_accepted"
        if acknowledged_count == self.expected_native_count:
            return "package_fully_accepted"
        if acknowledged_count < self.expected_native_count:
            return "package_partially_acknowledged"
        return "unexpected_acknowledgement_count"

    def sanitized_diagnostic(self) -> str:
        return (
            f"acceptance={self.acceptance_outcome}, "
            f"expected_native={self.expected_native_count}, "
            f"response_entries={self.response_entry_count}, "
            f"acknowledged_unique={len(self.acknowledged_ids)}, "
            f"polling={self.polling_outcome}, "
            f"cancellation={self.cancellation_outcome}, "
            f"cleanup_attempted_unique={len(self.cleanup_attempted_ids)}"
        )


class _BrokerTrafficProbe:
    """Record only sanitized lifecycle facts while delegating every real call."""

    def __init__(self, data_source, monkeypatch):
        self.data_source = data_source
        self.confirmation_count = 0
        self.cancellation_targets: list[str] = []
        self.current_scenario: _ScenarioRecord | None = None

        # Keep every network operation bounded. These wrappers still use the
        # configured real transport and do not mock an IBKR response.
        data_source.request_timeout = min(float(data_source.request_timeout), 5.0)

        original_get = data_source.get_from_endpoint
        original_post = data_source.post_to_endpoint
        original_delete = data_source.delete_to_endpoint
        original_delete_order = data_source.delete_order

        def bounded_get(
            url,
            description="",
            silent=False,
            allow_fail=True,
            max_retries=None,
        ):
            return original_get(
                url,
                description=description,
                silent=silent,
                allow_fail=allow_fail,
                max_retries=0 if max_retries is None else max_retries,
            )

        def bounded_post(
            url,
            json,
            description="",
            silent=False,
            allow_fail=True,
            max_retries=None,
        ):
            if description == "Confirming Order":
                self.confirmation_count += 1
            response = original_post(
                url,
                json,
                description=description,
                silent=silent,
                allow_fail=allow_fail,
                max_retries=0 if max_retries is None else max_retries,
            )
            if description == "Executing order" and self.current_scenario is not None:
                entries = response if isinstance(response, list) else [response]
                self.current_scenario.response_entry_count = len(entries)
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    order_id = entry.get("order_id")
                    if isinstance(order_id, bool) or not isinstance(order_id, (str, int)):
                        continue
                    normalized = str(order_id).strip()
                    if normalized:
                        # Record immediately after the real acknowledgement,
                        # before the broker maps or validates the package.
                        if normalized not in self.current_scenario.acknowledged_ids:
                            self.current_scenario.acknowledged_ids.append(normalized)
            return response

        def bounded_delete(
            url,
            description="",
            silent=False,
            allow_fail=True,
            max_retries=None,
        ):
            return original_delete(
                url,
                description=description,
                silent=silent,
                allow_fail=allow_fail,
                max_retries=0 if max_retries is None else max_retries,
            )

        def delete_order_and_record(order):
            self.cancellation_targets.append(str(order.identifier))
            return original_delete_order(order)

        monkeypatch.setattr(data_source, "get_from_endpoint", bounded_get)
        monkeypatch.setattr(data_source, "post_to_endpoint", bounded_post)
        monkeypatch.setattr(data_source, "delete_to_endpoint", bounded_delete)
        monkeypatch.setattr(data_source, "delete_order", delete_order_and_record)

    def begin(self, name: str, expected_native_count: int) -> _ScenarioRecord:
        record = _ScenarioRecord(
            name=name,
            expected_native_count=expected_native_count,
            confirmation_count_before=self.confirmation_count,
            cancellation_count_before=len(self.cancellation_targets),
        )
        self.current_scenario = record
        return record

    def finish(self) -> None:
        self.current_scenario = None

    def confirmation_occurred(self, record: _ScenarioRecord) -> bool:
        return self.confirmation_count > record.confirmation_count_before

    def scenario_cancellation_targets(self, record: _ScenarioRecord) -> list[str]:
        return self.cancellation_targets[record.cancellation_count_before :]


def _status(payload) -> str | None:
    if isinstance(payload, list) and payload:
        payload = payload[0]
    if not isinstance(payload, dict):
        return None
    value = payload.get("status")
    if value is None:
        value = payload.get("order_status", payload.get("orderStatus"))
    return str(value).strip().lower().replace("_", "").replace(" ", "") if value else None


def _order_id(payload) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("orderId", payload.get("order_id"))
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    normalized = str(value).strip()
    return normalized or None


def _poll_account_orders(data_source) -> tuple[bool, dict[str, str | None]]:
    """Poll the configured account once without retaining or logging raw data."""
    data_source.get_from_endpoint(
        f"{data_source.base_url}/iserver/account/orders?force=true",
        description="Refreshing paper order test state",
        silent=True,
        max_retries=0,
    )
    payload = data_source.get_from_endpoint(
        f"{data_source.base_url}/iserver/account/orders?accountId={data_source.account_id}",
        description="Polling paper order test state",
        silent=True,
        max_retries=0,
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("orders"), list):
        return False, {}

    statuses: dict[str, str | None] = {}
    for raw_order in payload["orders"]:
        order_id = _order_id(raw_order)
        if order_id is not None:
            statuses[order_id] = _status(raw_order)
        if isinstance(raw_order, dict) and isinstance(raw_order.get("leg"), list):
            for raw_leg in raw_order["leg"]:
                leg_id = _order_id(raw_leg)
                if leg_id is not None:
                    statuses[leg_id] = _status(raw_leg)
    return True, statuses


def _poll_order_status(data_source, order_id: str) -> tuple[bool, str | None]:
    payload = data_source.get_from_endpoint(
        f"{data_source.base_url}/iserver/account/order/status/{order_id}",
        description="Retrieving paper order test state",
        silent=True,
        max_retries=0,
    )
    if payload is None or (
        isinstance(payload, dict) and ("error" in payload or "message" in payload)
    ):
        return False, None
    status = _status(payload)
    returned_order_id = _order_id(payload)
    if returned_order_id is not None and returned_order_id != order_id:
        return False, None
    return status is not None, status


def _wait_until_native_tickets_are_retrievable(
    data_source,
    expected_ids: list[str],
) -> None:
    deadline = time.monotonic() + _VISIBLE_TIMEOUT_SECONDS
    missing = set(expected_ids)
    while missing and time.monotonic() < deadline:
        account_poll_succeeded, visible = _poll_account_orders(data_source)
        if account_poll_succeeded:
            missing.difference_update(visible)

        for order_id in list(missing):
            retrieved, _ = _poll_order_status(data_source, order_id)
            if retrieved:
                missing.remove(order_id)

        if missing:
            time.sleep(_POLL_INTERVAL_SECONDS)

    if missing:
        pytest.fail(
            f"IBKR did not expose {len(missing)} of {len(expected_ids)} acknowledged native tickets "
            f"within {_VISIBLE_TIMEOUT_SECONDS:g} seconds"
        )


def _wait_until_orders_are_not_working(data_source, order_ids: list[str]) -> None:
    deadline = time.monotonic() + _CANCEL_TIMEOUT_SECONDS
    pending = set(order_ids)
    filled: set[str] = set()

    while pending and time.monotonic() < deadline:
        account_poll_succeeded, visible = _poll_account_orders(data_source)
        for order_id in list(pending):
            visible_status = visible.get(order_id)
            if visible_status in _FILLED_STATUSES:
                filled.add(order_id)
                pending.remove(order_id)
                continue
            if visible_status in _TERMINAL_STATUSES:
                pending.remove(order_id)
                continue
            if account_poll_succeeded and order_id not in visible:
                pending.remove(order_id)
                continue

            retrieved, retrieved_status = _poll_order_status(data_source, order_id)
            if retrieved_status in _FILLED_STATUSES:
                filled.add(order_id)
                pending.remove(order_id)
            elif retrieved_status in _TERMINAL_STATUSES:
                pending.remove(order_id)
            elif not retrieved and account_poll_succeeded and order_id not in visible:
                pending.remove(order_id)

        if pending:
            time.sleep(_POLL_INTERVAL_SECONDS)

    if filled:
        pytest.fail(f"{len(filled)} supposedly non-marketable paper orders filled during cleanup")
    if pending:
        pytest.fail(
            f"{len(pending)} acknowledged paper orders were still working after "
            f"{_CANCEL_TIMEOUT_SECONDS:g} seconds"
        )


def _assert_acknowledgement_and_tracking(
    broker: InteractiveBrokersREST,
    record: _ScenarioRecord,
    package_order: Order,
    native_orders: list[Order],
) -> list[str]:
    expected_count = len(native_orders)
    if record.response_entry_count != expected_count:
        pytest.fail(
            f"IBKR {record.name} response cardinality mismatch: expected {expected_count} "
            f"native acknowledgement entries, received {record.response_entry_count}"
        )
    if len(record.acknowledged_ids) != expected_count:
        pytest.fail(
            f"IBKR {record.name} acknowledged {len(record.acknowledged_ids)} unique native IDs "
            f"but expected {expected_count}"
        )

    native_ids = [str(order.identifier) for order in native_orders]
    assert len(set(native_ids)) == expected_count, (
        f"IBKR {record.name} native tickets did not receive distinct broker IDs"
    )
    assert Counter(record.acknowledged_ids) == Counter(native_ids), (
        f"IBKR {record.name} acknowledgement mapping did not cover every native ticket"
    )
    assert all(order.was_transmitted() for order in native_orders)
    assert package_order.status != Order.OrderStatus.ERROR, (
        f"IBKR {record.name} package failed during submission or acknowledgement mapping"
    )

    tracked = broker.get_all_orders()
    expected_local_orders = [package_order, *package_order.child_orders]
    assert all(sum(candidate is expected for candidate in tracked) == 1 for expected in expected_local_orders), (
        f"IBKR {record.name} created a duplicate or omitted LumiBot order"
    )
    assert all(sum(str(candidate.identifier) == order_id for candidate in tracked) == 1 for order_id in native_ids), (
        f"IBKR {record.name} created duplicate tracked broker IDs"
    )
    return native_ids


def _cleanup_scenario(
    broker: InteractiveBrokersREST,
    data_source,
    probe: _BrokerTrafficProbe,
    record: _ScenarioRecord,
) -> list[str]:
    cleanup_errors = []
    cancellation_start = len(probe.cancellation_targets)
    # The acknowledgement ledger is ordered and deduplicated at capture time.
    # Repeating cancellation here is intentional: cleanup must be idempotent
    # even when the scenario already canceled through a package parent.
    for order_id in record.acknowledged_ids:
        try:
            broker.cancel_order(Order(strategy=_STRATEGY_NAME, identifier=order_id))
        except Exception as exc:
            cleanup_errors.append(type(exc).__name__)

    attempted = probe.cancellation_targets[cancellation_start:]
    record.cleanup_attempted_ids.update(
        order_id for order_id in attempted if order_id in record.acknowledged_ids
    )
    missing_attempts = Counter(record.acknowledged_ids) - Counter(attempted)
    if missing_attempts:
        cleanup_errors.append(
            f"no broker cancellation attempt for {sum(missing_attempts.values())} acknowledged IDs"
        )

    if record.acknowledged_ids:
        try:
            _wait_until_orders_are_not_working(data_source, record.acknowledged_ids)
        except BaseException as exc:
            cleanup_errors.append(str(exc))
    return cleanup_errors


def _run_scenario(
    *,
    name: str,
    broker: InteractiveBrokersREST,
    data_source,
    probe: _BrokerTrafficProbe,
    package_order: Order,
    expected_native_orders,
    assertions,
    cancellation_assertions,
    record_property,
) -> None:
    native_orders = expected_native_orders(package_order)
    record = probe.begin(name, expected_native_count=len(native_orders))
    scenario_error: BaseException | None = None
    try:
        result = broker.submit_order(package_order)
        assert result is package_order
        native_orders = expected_native_orders(package_order)
        native_ids = _assert_acknowledgement_and_tracking(
            broker,
            record,
            package_order,
            native_orders,
        )
        assertions(package_order, native_orders, native_ids, record)
        record.polling_outcome = "in_progress"
        try:
            _wait_until_native_tickets_are_retrievable(data_source, native_ids)
        except BaseException:
            record.polling_outcome = "failed"
            raise
        record.polling_outcome = "all_native_tickets_retrievable"
        assert all(
            broker.get_order(order_id) is native_order
            for order_id, native_order in zip(native_ids, native_orders)
        ), f"IBKR {name} lifecycle polling lost or duplicated a tracked native order"
        cancellation_assertions(package_order, native_orders, native_ids, record)
    except BaseException as exc:
        scenario_error = exc
    finally:
        cleanup_errors = _cleanup_scenario(broker, data_source, probe, record)
        probe.finish()
        record_property(
            f"ibkr_{name.lower()}_confirmation_path",
            probe.confirmation_occurred(record),
        )
        record_property(f"ibkr_{name.lower()}_acceptance", record.acceptance_outcome)
        record_property(
            f"ibkr_{name.lower()}_acknowledged_unique",
            len(record.acknowledged_ids),
        )
        record_property(
            f"ibkr_{name.lower()}_cleanup_attempted_unique",
            len(record.cleanup_attempted_ids),
        )
        record_property(f"ibkr_{name.lower()}_polling", record.polling_outcome)

    if scenario_error is not None:
        scenario_error.add_note("Sanitized IBKR paper diagnostic: " + record.sanitized_diagnostic())
        if cleanup_errors:
            scenario_error.add_note("Cleanup: " + "; ".join(cleanup_errors))
        raise scenario_error
    if cleanup_errors:
        pytest.fail(
            f"IBKR {name} cleanup failed ({record.sanitized_diagnostic()}): "
            f"{'; '.join(cleanup_errors)}"
        )


def test_ibkr_rest_advanced_orders_on_verified_paper_account(
    ibkr_rest_paper_order_data_source,
    monkeypatch,
    record_property,
    request,
):
    """Exercise real broker submission, polling, tracking, and cleanup on paper."""
    from lumibot.credentials import INTERACTIVE_BROKERS_REST_CONFIG

    data_source = ibkr_rest_paper_order_data_source
    probe = _BrokerTrafficProbe(data_source, monkeypatch)
    broker = InteractiveBrokersREST(
        dict(INTERACTIVE_BROKERS_REST_CONFIG),
        data_source=data_source,
        connect_stream=False,
    )
    broker._strategy_name = _STRATEGY_NAME

    def stop_broker_threads():
        broker._stop_event.set()
        if broker._orders_thread is not None:
            broker._orders_thread.join(timeout=1)

    request.addfinalizer(stop_broker_threads)

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    reference_price = broker.get_last_price(asset, exchange="SMART")
    if reference_price is None or float(reference_price) <= 0:
        pytest.skip("IBKR paper gateway did not provide a usable current SPY reference price")

    reference_price = float(reference_price)
    lower_limit = round(reference_price * 0.95, 2)
    lower_stop = round(reference_price * 0.90, 2)
    upper_trigger = round(reference_price * 1.05, 2)

    def assert_bracket(parent, native_orders, native_ids, _record):
        assert len(parent.child_orders) == 2
        assert native_orders == [parent, *parent.child_orders]
        assert all(child.parent_identifier == parent.identifier for child in parent.child_orders)
        assert parent.identifier == native_ids[0]

    def assert_oto(parent, native_orders, native_ids, _record):
        assert len(parent.child_orders) == 1
        assert native_orders == [parent, parent.child_orders[0]]
        assert parent.child_orders[0].parent_identifier == parent.identifier
        assert parent.identifier == native_ids[0]

    def assert_oco(parent, native_orders, native_ids, record):
        local_parent_id = str(parent.identifier)
        assert native_orders == parent.child_orders
        assert len(native_orders) == 2
        assert all(child.side == Order.OrderSide.BUY for child in native_orders)
        assert not parent.was_transmitted()
        assert local_parent_id not in native_ids
        assert all(child.parent_identifier == local_parent_id for child in native_orders)

    def assert_exact_cancellation_targets(start, expected_ids, label):
        contacted_ids = probe.cancellation_targets[start:]
        assert Counter(contacted_ids) == Counter(expected_ids), (
            f"IBKR {label} cancellation contacted an unexpected set of native IDs"
        )

    def cancel_bracket(parent, native_orders, native_ids, record):
        record.cancellation_outcome = "in_progress"
        direct_child = native_orders[-1]
        direct_child_id = native_ids[-1]

        # Explicit cancellation must reach IBKR regardless of local workflow
        # state, and repeating the exact child cancellation must remain safe.
        for local_status in (Order.OrderStatus.CANCELLING, Order.OrderStatus.CANCELED):
            direct_child.status = local_status
            cancellation_start = len(probe.cancellation_targets)
            broker.cancel_order(direct_child)
            assert_exact_cancellation_targets(
                cancellation_start,
                [direct_child_id],
                f"BRACKET direct child in {local_status.value}",
            )

        cancellation_start = len(probe.cancellation_targets)
        broker.cancel_order(parent)
        assert_exact_cancellation_targets(
            cancellation_start,
            native_ids,
            "BRACKET parent",
        )
        _wait_until_orders_are_not_working(data_source, native_ids)
        record.cancellation_outcome = "all_native_tickets_inactive"

    def cancel_oto(parent, _native_orders, native_ids, record):
        record.cancellation_outcome = "in_progress"
        # Two parent-level attempts prove both local terminal-ish states still
        # contact every broker-backed member and that cancellation is idempotent.
        for local_status in (Order.OrderStatus.CANCELLING, Order.OrderStatus.CANCELED):
            parent.status = local_status
            cancellation_start = len(probe.cancellation_targets)
            broker.cancel_order(parent)
            assert_exact_cancellation_targets(
                cancellation_start,
                native_ids,
                f"OTO parent in {local_status.value}",
            )
        _wait_until_orders_are_not_working(data_source, native_ids)
        record.cancellation_outcome = "all_native_tickets_inactive"

    def cancel_oco(parent, _native_orders, native_ids, record):
        local_parent_id = str(parent.identifier)
        record.cancellation_outcome = "in_progress"
        for local_status in (Order.OrderStatus.CANCELLING, Order.OrderStatus.CANCELED):
            parent.status = local_status
            cancellation_start = len(probe.cancellation_targets)
            broker.cancel_order(parent)
            parent_cancel_targets = probe.cancellation_targets[cancellation_start:]
            assert Counter(parent_cancel_targets) == Counter(native_ids)
            assert local_parent_id not in parent_cancel_targets
        assert local_parent_id not in probe.scenario_cancellation_targets(record)
        _wait_until_orders_are_not_working(data_source, native_ids)
        record.cancellation_outcome = "all_native_tickets_inactive"

    try:
        _run_scenario(
            name="BRACKET",
            broker=broker,
            data_source=data_source,
            probe=probe,
            package_order=Order(
                strategy=_STRATEGY_NAME,
                asset=asset,
                quantity=1,
                side=Order.OrderSide.BUY,
                limit_price=lower_limit,
                order_type=Order.OrderType.LIMIT,
                order_class=Order.OrderClass.BRACKET,
                secondary_limit_price=upper_trigger,
                secondary_stop_price=lower_stop,
                time_in_force="day",
                exchange="SMART",
            ),
            expected_native_orders=lambda parent: [parent, *parent.child_orders],
            assertions=assert_bracket,
            cancellation_assertions=cancel_bracket,
            record_property=record_property,
        )

        _run_scenario(
            name="OTO",
            broker=broker,
            data_source=data_source,
            probe=probe,
            package_order=Order(
                strategy=_STRATEGY_NAME,
                asset=asset,
                quantity=1,
                side=Order.OrderSide.BUY,
                limit_price=lower_limit,
                order_type=Order.OrderType.LIMIT,
                order_class=Order.OrderClass.OTO,
                secondary_limit_price=upper_trigger,
                time_in_force="day",
                exchange="SMART",
            ),
            expected_native_orders=lambda parent: [parent, *parent.child_orders],
            assertions=assert_oto,
            cancellation_assertions=cancel_oto,
            record_property=record_property,
        )

        _run_scenario(
            name="OCO",
            broker=broker,
            data_source=data_source,
            probe=probe,
            package_order=Order(
                strategy=_STRATEGY_NAME,
                asset=asset,
                quantity=1,
                side=Order.OrderSide.BUY,
                limit_price=lower_limit,
                stop_price=upper_trigger,
                order_class=Order.OrderClass.OCO,
                time_in_force="day",
                exchange="SMART",
            ),
            expected_native_orders=lambda parent: list(parent.child_orders),
            assertions=assert_oco,
            cancellation_assertions=cancel_oco,
            record_property=record_property,
        )
    finally:
        stop_broker_threads()
