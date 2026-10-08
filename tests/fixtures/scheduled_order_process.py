"""Subprocess worker for the scheduled order boundary contract (no network)."""

import json
import os
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID


def main():
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    helpers = runpy.run_path(str(root / "tests/test_strategy_live_order_accessors.py"))
    from lumibot.entities import Asset, Order

    broker_path, output_path = map(Path, sys.argv[2:4])

    class FileBroker(helpers["_LiveBroker"]):
        def __init__(self):
            super().__init__()
            # The accessor test helper simulates an already-running broker.
            # A real new Broker starts in startup reconciliation mode.
            self._first_iteration = True

        def _submit_order(self, order):
            order.identifier = UUID("383d1a74-79ec-4e58-a35e-b1df71833cfa")
            order.status = Order.OrderStatus.OPEN
            broker_path.write_text(json.dumps(order.to_dict(), default=str))
            self._process_new_order(order)
            return order

        def _pull_broker_all_orders(self):
            return [Order.from_dict(json.loads(broker_path.read_text()))] if broker_path.exists() else []

        def _pull_broker_order(self, identifier):
            return next(
                (
                    order
                    for order in self._pull_broker_all_orders()
                    if self.identifiers_equal(order.identifier, identifier)
                ),
                None,
            )

    # No broker/HTTP implementation can escape the fixture process.
    def reject_network(*args, **kwargs):
        raise AssertionError("Unexpected external network request in scheduled-order fixture")

    with patch("requests.sessions.Session.send", reject_network):
        broker = FileBroker()
        strategy = helpers["_AccessorStrategy"](broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})
        if sys.argv[1] == "submit":
            strategy.submit_order(strategy.create_order(Asset("SPY"), 3, "buy", order_type="limit", limit_price=100))
        else:
            broker.sync_orders(strategy)
            broker.sync_orders(strategy)  # Duplicate broker observations must not duplicate orders.
        payloads = []
        strategy.lumiwealth_api_key = "synthetic-listener-test"
        with patch(
            "lumibot.strategies._strategy.requests.post",
            lambda *args, **kwargs: payloads.append(json.loads(kwargs["data"])) or SimpleNamespace(status_code=200),
        ):
            assert strategy.send_update_to_cloud() is True
        output_path.write_text(json.dumps({"pid": os.getpid(), "payload": payloads[-1]}, default=str))


if __name__ == "__main__":
    main()
