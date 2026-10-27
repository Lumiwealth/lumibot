"""Explicit demo-only execution proof using normal Strategy methods.

Requires dedicated demo keys and KALSHI_TEST_MARKET pointing to a funded,
two-sided demo market with at least three contracts at its displayed top level.
The test never deposits/transfers funds and cleans up only its own orders and
the initially flat selected market. It makes real demo API writes.
"""

import json
import os
import time
from decimal import Decimal

import pytest

from lumibot.brokers import Kalshi
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.tools.kalshi_client import KalshiClient

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi]


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
