"""Explicit demo-only execution proof using normal Strategy methods.

Requires dedicated demo keys and KALSHI_TEST_MARKET pointing to a funded,
two-sided demo market with at least three contracts at its displayed top level.
The test never deposits/transfers funds and cleans up only its own orders and
the initially flat selected market. It makes real demo API writes.
"""

import json
import os
import time
import uuid
from decimal import Decimal

import pytest

from lumibot.brokers import Kalshi
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi]


@pytest.fixture
def native_demo():
    ticker = os.getenv("KALSHI_TEST_MARKET")
    key_id = os.getenv("KALSHI_API_KEY_ID")
    path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
    if not ticker or not key_id or not path:
        pytest.skip("Dedicated demo credentials and KALSHI_TEST_MARKET are required")
    client = KalshiClient(
        {"IS_DEMO": True, "BASE_URL": KalshiClient.DEMO_URL, "API_KEY_ID": key_id, "PRIVATE_KEY_PATH": path}
    )
    broker = Kalshi(client=client, connect_stream=False)
    strategy = Strategy(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
    owned = []
    try:
        assert not [p for p in strategy.get_positions() if p.asset.symbol == ticker]
        yield strategy, broker, ticker, owned
    finally:
        try:
            for order in owned:
                row = broker._pull_broker_order(order.identifier) if order.was_transmitted() else None
                if row and row["status"] == "resting":
                    strategy.cancel_order(order)
            while any(
                (broker._pull_broker_order(order.identifier) or {}).get("status") == "resting"
                for order in owned if order.was_transmitted()
            ):
                time.sleep(0.5)
            # The configured market is dedicated to this qualification and was
            # verified flat before any order. Close only its test inventory.
            for position in [p for p in strategy.get_positions() if p.asset.symbol == ticker]:
                closing = strategy.submit_order(
                    strategy.create_order(
                        position.asset, position.quantity, "sell", order_type="market", time_in_force="ioc"
                    )
                )
                while strategy.get_order(closing.identifier).is_active():
                    time.sleep(0.5)
            assert not [p for p in strategy.get_positions() if p.asset.symbol == ticker], (
                "Native demo cleanup remains incomplete"
            )
        finally:
            broker.close()
            client.close()


def test_demo_normal_batch_keeps_two_orders_and_outcomes_independent(native_demo, tmp_path):
    strategy, broker, ticker, owned = native_demo
    yes, no = (Asset(ticker, "prediction_contract", outcome=outcome) for outcome in ("yes", "no"))
    assert all(strategy.get_quote(asset).ask > 0.02 for asset in (yes, no))
    owned.extend(
        strategy.create_order(asset, Decimal(".25"), "buy", limit_price=0.01, time_in_force="gtc")
        for asset in (yes, no)
    )
    submitted = strategy.submit_orders(owned)
    assert len(submitted) == 2 and len({order.identifier for order in submitted}) == 2
    while True:
        orders = strategy.get_orders(
            identifiers=[order.identifier for order in owned], broker_refresh=True, broker_refresh_ttl_seconds=0
        )
        if len(orders) == 2 and all(order.status == Order.OrderStatus.NEW for order in orders):
            break
        time.sleep(0.5)
    assert {order.asset.outcome for order in orders} == {"yes", "no"}
    assert all(order.quantity == Decimal(".25") and order.filled_quantity == 0 for order in orders)
    (tmp_path / "native-two-orders.json").write_text(json.dumps([order.to_dict() for order in orders], default=str))
    for order in orders:
        strategy.cancel_order(order)
    while any(strategy.get_order(order.identifier).is_active() for order in orders):
        time.sleep(.5)


def test_demo_real_ioc_partial_fill_reports_only_executed_quantity(native_demo, tmp_path):
    strategy, broker, ticker, owned = native_demo
    asset = Asset(ticker, "prediction_contract", outcome="yes")
    quote = strategy.get_quote(asset)
    assert quote.ask is not None and quote.ask_size is not None and 0 < quote.ask_size <= 10
    quantity = Decimal(str(quote.ask_size)) + Decimal(".25")
    order = strategy.create_order(asset, quantity, "buy", limit_price=quote.ask, time_in_force="ioc")
    owned.append(order)
    strategy.submit_order(order)
    while True:
        current = strategy.get_order(order.identifier, broker_refresh=True, broker_refresh_ttl_seconds=0)
        if current is not None and not current.is_active():
            break
        time.sleep(0.5)
    filled = Decimal(broker._state(current)["fill_count"])
    (tmp_path / "native-ioc-partial.json").write_text(json.dumps(current.to_dict(), default=str))
    assert Decimal("0") < filled < quantity, (
        "The actual book did not produce a partial fill; retain the observed result"
    )
    assert current.is_canceled()
    assert current.filled_quantity == filled
    assert current.remaining_quantity == 0
    position = strategy.get_position(asset, broker_refresh=True, broker_refresh_ttl_seconds=0)
    assert position is not None and Decimal(str(position.quantity)) == filled


def test_demo_fresh_strategy_recovers_owned_order_and_delivers_one_fill(native_demo, tmp_path):
    original, _, ticker, owned = native_demo
    asset = Asset(ticker, "prediction_contract", outcome="no")
    order = original.create_order(asset, Decimal(".25"), "buy", limit_price=.01, time_in_force="gtc")
    owned.append(original.submit_order(order))
    while original.get_order(order.identifier).status != Order.OrderStatus.NEW:
        time.sleep(.5)

    class RestartedStrategy(Strategy):
        def initialize(self):
            self.fill_callbacks = []

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.fill_callbacks.append({"id": order.identifier, "outcome": order.asset.outcome, "quantity": quantity})

    # A fresh adapter and executor have no in-memory order/tag checkpoints.
    client = KalshiClient({"IS_DEMO": True, "BASE_URL": KalshiClient.DEMO_URL,
                           "API_KEY_ID": os.environ["KALSHI_API_KEY_ID"],
                           "PRIVATE_KEY_PATH": os.environ["KALSHI_PRIVATE_KEY_PATH"]})
    broker = Kalshi(client=client, connect_stream=False)
    try:
        restarted = RestartedStrategy(broker=broker, name=original.name, analyze_backtest=False,
                                      synchronize_broker_on_start=False)
        restarted._executor._initialize()
        recovered = restarted.get_order(order.identifier)
        assert recovered.asset == asset and recovered.side == "buy"
        assert broker.order_belongs_to_local_strategy(recovered, restarted.name)
        assert not restarted.fill_callbacks
        ask = restarted.get_quote(asset).ask
        assert ask is not None and 0 < ask < 1
        restarted.modify_order(recovered, limit_price=ask)
        while not restarted.get_order(order.identifier).is_filled():
            time.sleep(.5)
        restarted._executor.process_queue(skip_first_iteration_events=False)
        assert len(restarted.fill_callbacks) == 1
        assert restarted.fill_callbacks[0]["id"] == order.identifier
        assert restarted.fill_callbacks[0]["outcome"] == "no"
        assert Decimal(str(restarted.fill_callbacks[0]["quantity"])) == Decimal(".25")
        assert Decimal(str(restarted.get_position(asset).quantity)) == Decimal(".25")
        restarted.get_orders()
        restarted._executor.process_queue(skip_first_iteration_events=False)
        assert len(restarted.fill_callbacks) == 1
        (tmp_path / "native-restarted-strategy.json").write_text(json.dumps({
            "order": recovered.to_dict(), "callbacks": restarted.fill_callbacks,
            "cash": restarted.get_cash()}, default=str))
    finally:
        broker.close()
        client.close()


def test_demo_lost_ack_and_cold_retry_keep_one_native_order(native_demo, tmp_path):
    strategy, broker, ticker, owned = native_demo
    asset = Asset(ticker, "prediction_contract", outcome="yes")
    assert strategy.get_quote(asset).ask > .02
    caller_id = str(uuid.uuid4())
    order = strategy.create_order(asset, Decimal(".25"), "buy", limit_price=.01,
                                  custom_params={"client_order_id": caller_id})
    owned.append(order)
    actual_request = broker._client.request
    accepted = {}
    writes = []

    def lose_first_ack(method, path, **kwargs):
        response = actual_request(method, path, **kwargs)
        if method == "POST" and path == "/portfolio/events/orders":
            writes.append(response["order_id"])
            accepted.update(response)
            # The exchange really accepted the order. Only its transport
            # acknowledgement is lost; no exchange result is fabricated.
            raise KalshiAPIError("Injected connection loss after the real exchange accepted the order")
        return response

    broker._client.request = lose_first_ack
    try:
        try:
            strategy.submit_order(order)
        except KalshiAPIError:
            pass
        assert len(writes) == 1
        identifier = accepted["order_id"]
        while strategy.get_order(identifier) is None:
            time.sleep(.5)
        try:
            strategy.submit_order(order)
        except ValueError:
            pass
        assert len(writes) == 1, "The same attempted Order must not be posted twice"
    finally:
        broker._client.request = actual_request

    client = KalshiClient({"IS_DEMO": True, "BASE_URL": KalshiClient.DEMO_URL,
                           "API_KEY_ID": os.environ["KALSHI_API_KEY_ID"],
                           "PRIVATE_KEY_PATH": os.environ["KALSHI_PRIVATE_KEY_PATH"]})
    fresh = Kalshi(client=client, connect_stream=False)
    try:
        restarted = Strategy(broker=fresh, name=strategy.name, analyze_backtest=False,
                             synchronize_broker_on_start=False)
        retry = restarted.create_order(asset, Decimal(".25"), "buy", limit_price=.01,
                                        custom_params={"client_order_id": caller_id})
        try:
            restarted.submit_order(retry)
        except KalshiAPIError as error:
            assert error.status_code == 409
        while restarted.get_order(identifier) is None:
            time.sleep(.5)
        wire = broker._state(order)["client_order_id"]
        matches = [row for row in restarted.get_orders()
                   if getattr(row, "_raw", {}).get("client_order_id") == wire]
        assert len(matches) == 1 and matches[0].identifier == identifier
        restarted.cancel_order(matches[0])
        while restarted.get_order(identifier).is_active():
            time.sleep(.5)
        (tmp_path / "native-lost-ack-retry.json").write_text(json.dumps({
            "orderId": identifier, "sameInstanceCreateCount": len(writes),
            "coldRetryOrderCount": len(matches), "finalOrder": restarted.get_order(identifier).to_dict()}, default=str))
    finally:
        fresh.close()
        client.close()


@pytest.mark.parametrize("unit", [Decimal("1"), Decimal("0.25")], ids=["whole", "fractional"])
def test_demo_selected_contract_orders_positions_and_netting(tmp_path, unit):
    ticker = os.environ.get("KALSHI_TEST_MARKET")
    key_id = os.environ.get("KALSHI_API_KEY_ID")
    key, path = os.environ.get("KALSHI_PRIVATE_KEY"), os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if not ticker or not key_id or not (key or path):
        pytest.skip("Dedicated demo credentials and KALSHI_TEST_MARKET are required for demo writes")
    # Explicit provider host, independent of inherited production/custom URLs.
    client = KalshiClient(
        {
            "IS_DEMO": True,
            "BASE_URL": KalshiClient.DEMO_URL,
            "API_KEY_ID": key_id,
            "PRIVATE_KEY": key,
            "PRIVATE_KEY_PATH": path,
        }
    )
    records = []
    original = client.request

    def record(method, endpoint, **kwargs):
        entry = {"method": method, "path": endpoint, "request": kwargs}
        records.append(entry)
        wire = tmp_path / "kalshi-demo-wire.json"
        wire.write_text(json.dumps(records, indent=2, default=str))
        try:
            result = original(method, endpoint, **kwargs)
            entry["response"] = result
            return result
        except Exception as exc:
            entry["error"] = str(exc)
            raise
        finally:
            wire.write_text(json.dumps(records, indent=2, default=str))

    client.request = record

    class DemoContractCheck(Strategy):
        def initialize(self):
            self.callback_fills = []

        def on_trading_iteration(self):
            pass

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.callback_fills.append(
                {
                    "outcome": order.asset.outcome,
                    "side": str(order.side),
                    "quantity": quantity,
                    "price": price,
                    "position_quantity": position.quantity,
                    "positions": [
                        (p.asset.outcome, p.quantity)
                        for p in self.get_positions(broker_refresh=False)
                        if p.asset.symbol == ticker
                    ],
                }
            )

    broker = Kalshi(client=client, connect_stream=False)
    strategy = DemoContractCheck(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
    strategy._executor._initialize()
    yes, no = (Asset(ticker, "prediction_contract", outcome=side) for side in ("yes", "no"))
    owned = []
    flat_verified = False
    summary = {}

    def observed(order, predicate):
        # Eventual consistency is a functional wait, with no elapsed acceptance
        # threshold. Actual provider errors/interruption remain visible errors.
        while True:
            current = strategy.get_order(order.identifier)
            if current is not None and current.status == Order.OrderStatus.ERROR:
                raise AssertionError(current.error_message)
            if current is not None and predicate(current):
                return current
            time.sleep(0.25)

    def inventory(outcome, quantity):
        positions = [p for p in strategy.get_positions() if p.asset.symbol == ticker]
        actual = [(p.asset.outcome, p.quantity) for p in positions]
        assert actual == ([(outcome, quantity)] if quantity else [])
        selected = strategy.get_position(yes if outcome == "yes" else no)
        assert (selected.quantity if selected else 0) == quantity
        return actual

    def execute(asset, quantity, action, market=False):
        quote = strategy.get_quote(asset)
        price = quote.ask if action == "buy" else quote.bid
        assert price is not None, "Demo book has no executable liquidity"
        order = strategy.create_order(
            asset,
            quantity,
            action,
            time_in_force="ioc",
            **({"order_type": "market"} if market else {"limit_price": price}),
        )
        owned.append(order)
        assert strategy.submit_order(order) is order
        observed(order, lambda current: current.status in (Order.OrderStatus.FILLED, Order.OrderStatus.CANCELED))
        assert Decimal(broker._state(order)["fill_count"]) == Decimal(quantity), (
            "Demo liquidity did not fill the requested test size"
        )
        strategy._executor.process_queue(skip_first_iteration_events=False)
        assert order.asset == asset and order.side == action
        return order

    try:
        assert not [p for p in strategy.get_positions() if p.asset.symbol == ticker], (
            "Selected demo market must start flat"
        )
        flat_verified = True
        resting = strategy.create_order(yes, 1, "buy", limit_price=0.01, time_in_force="gtc")
        owned.append(resting)
        strategy.submit_order(resting)
        # Immediate amend exercises the real write/read lag observed in demo.
        strategy.modify_order(resting, limit_price=0.02)
        observed(resting, lambda current: current._raw.get("yes_price_dollars") == "0.0200")
        strategy.cancel_order(resting)
        observed(resting, lambda current: current.status == Order.OrderStatus.CANCELED)
        assert resting in strategy.get_orders(identifiers=[resting.identifier])

        execute(yes, 3 * unit, "buy")
        inventory("yes", 3 * unit)
        strategy.update_broker_balances()
        summary["open_balances"] = broker._get_balances_at_broker(strategy.quote_asset, strategy)
        summary["open_positions"] = [p.to_dict() for p in strategy.get_positions() if p.asset.symbol == ticker]
        execute(yes, unit, "sell")
        inventory("yes", 2 * unit)
        execute(no, unit, "buy")
        inventory("yes", unit)
        assert strategy.get_position(no) is None
        assert strategy.callback_fills[-1]["position_quantity"] == 0
        assert strategy.callback_fills[-1]["positions"] == [("yes", unit)]
        execute(no, 2 * unit, "buy")
        inventory("no", unit)
        execute(no, unit, "sell", market=True)
        inventory("no", 0)
        assert len(strategy.callback_fills) == 5
        for _ in range(2):
            strategy.get_orders()
        strategy._executor.process_queue(skip_first_iteration_events=False)
        assert len(strategy.callback_fills) == 5, "Repeated reconciliation duplicated fill callbacks"
        strategy.update_broker_balances()
        summary.update(
            callbacks=strategy.callback_fills,
            final_balances=broker._get_balances_at_broker(strategy.quote_asset, strategy),
            orders=[order.to_dict() for order in owned],
        )
    finally:
        try:
            for order in owned:
                if order.was_transmitted() and order.is_active():
                    strategy.cancel_order(order)
            if flat_verified:
                for position in [p for p in strategy.get_positions() if p.asset.symbol == ticker]:
                    cleanup = strategy.create_order(
                        position.asset, position.quantity, "sell", order_type="market", time_in_force="ioc"
                    )
                    strategy.submit_order(cleanup)
                    observed(
                        cleanup,
                        lambda current: current.status in (Order.OrderStatus.FILLED, Order.OrderStatus.CANCELED),
                    )
                assert not [p for p in strategy.get_positions() if p.asset.symbol == ticker], (
                    "Demo cleanup is incomplete"
                )
        finally:
            summary["callbacks"] = strategy.callback_fills
            (tmp_path / "kalshi-demo-summary.json").write_text(json.dumps(summary, indent=2, default=str))
            broker.close()
            client.close()
