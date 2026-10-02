"""Native run_once / SmartLimit / callback proof in a fresh process, no network."""

import json
import os
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def main():
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    helpers = runpy.run_path(str(root / "tests/test_strategy_live_order_accessors.py"))
    from lumibot.entities import SmartLimitConfig

    class CompletionBroker(helpers["_LiveBroker"]):
        def __init__(self):
            super().__init__()
            self.market = "24/7"
            self.closed = False
            self.reprices = []
            self.submitted_types = []

        def _submit_order(self, order):
            self.submitted_types.append(str(order.order_type))
            order.identifier = "native-scheduled-smart"
            self.broker_orders.append(order)
            self._process_trade_event(order, self.NEW_ORDER)
            return order

        def _modify_order(self, order, limit_price=None, stop_price=None):
            self.reprices.append(limit_price)
            order.avg_fill_price = limit_price  # Broker response metadata, as in live adapters.
            self._process_trade_event(order, self.FILLED_ORDER, price=limit_price, filled_quantity=order.quantity)
            return order

        def _close_connection(self):
            self.closed = True
            self.cleanup_streams()

    class CompletionStrategy(helpers["_AccessorStrategy"]):
        def get_datetime(self):
            return datetime.now(timezone.utc)

        def get_quote(self, *args, **kwargs):
            return SimpleNamespace(bid=99.0, ask=101.0)

        def on_trading_iteration(self):
            self.submit_order(self.create_order(
                "SPY", 1, "buy",
                smart_limit=SmartLimitConfig(preset="fast", step_seconds=1, final_hold_seconds=1),
            ))

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.vars.fill_seen = True
            self.vars.fill_price = price

        def on_strategy_end(self):
            assert self.vars.fill_seen, "The end hook must run after SmartLimit and fill callbacks finish"
            assert not self.broker.closed

    def reject_network(*args, **kwargs):
        raise AssertionError("Unexpected external network request in SmartLimit completion fixture")

    payloads = []
    with patch("requests.sessions.Session.send", reject_network):
        broker = CompletionBroker()
        strategy = CompletionStrategy(broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})
        strategy.lumiwealth_api_key = "synthetic-listener-test"
        with patch(
            "lumibot.strategies._strategy.requests.post",
            lambda *args, **kwargs: payloads.append(json.loads(kwargs["data"])) or SimpleNamespace(status_code=200),
        ):
            result = strategy._executor.run_once()
        Path(sys.argv[1]).write_text(json.dumps({
            "pid": os.getpid(), "completed": result, "closed": broker.closed,
            "reprices": broker.reprices, "submitted_types": broker.submitted_types,
            "fill_seen": getattr(strategy.vars, "fill_seen", False), "payload": payloads[-1] if payloads else None,
            "exception": str(strategy._executor.exception),
        }, default=str))


if __name__ == "__main__":
    main()
