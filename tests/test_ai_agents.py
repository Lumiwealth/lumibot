from __future__ import annotations

from typing import Any, Dict, List

from lumibot.ai.manager import AgentManager


class DummyBroker:
    IS_BACKTESTING_BROKER = True

    def __init__(self):
        self._orders_queue = type("Q", (), {"queue": []})()

    def is_market_open(self) -> bool:
        return True


class DummyOrder:
    def __init__(self, asset, quantity, side, order_type="market", limit_price=None, stop_price=None):
        self.asset = asset
        self.quantity = quantity
        self.side = side
        self.order_type = order_type
        self.limit_price = limit_price
        self.stop_price = stop_price
        self.identifier = f"{asset.symbol}-{side}-{quantity}"

    def is_parent(self):
        return False

    @property
    def child_orders(self):
        return []


class DummyAsset:
    def __init__(self, symbol):
        self.symbol = symbol
        self.asset_type = "stock"
        self.multiplier = 1

    def __str__(self):
        return self.symbol


class DummyStrategy:
    def __init__(self, is_backtesting=True):
        self.is_backtesting = is_backtesting
        self.broker = DummyBroker()
        self._dt = None
        self.submitted: List[DummyOrder] = []
        self._cron_jobs: List[str] = []

    # Methods used by AgentManager/Runner
    def get_datetime(self):
        import datetime as _dt
        return self._dt or _dt.datetime.now()

    def get_cash(self) -> float:
        return 100000.0

    def get_portfolio_value(self) -> float:
        return 100000.0

    def get_positions(self):
        return []

    def get_orders(self):
        return []

    def get_last_price(self, asset, quote=None):
        return 100.0

    def create_order(self, asset, quantity, side, order_type="market", limit_price=None, stop_price=None, quote=None):
        # accept symbol string too
        if not hasattr(asset, "symbol"):
            asset = DummyAsset(str(asset))
        return DummyOrder(asset, quantity, side, order_type, limit_price, stop_price)

    def submit_order(self, order):
        self.submitted.append(order)

    def cancel_order(self, order):
        pass

    def log_message(self, *args, **kwargs):
        pass

    def register_cron_callback(self, cron_schedule: str, callback) -> str:
        # In live mode this would schedule; we just record it for assertions
        self._cron_jobs.append(cron_schedule)
        # Call the callback immediately to simulate a trigger
        callback()
        return f"job_{len(self._cron_jobs)}"


class FakeRunner:
    def __init__(self, strategy):
        self.strategy = strategy

    def tick(self, handle) -> Dict[str, Any]:
        return {
            "actions": [
                {"type": "trade", "symbol": "SPY", "side": "BUY", "qty": 1, "order_type": "market"}
            ],
            "notes": "test",
            "confidence": 0.9,
        }


class FakeRunnerCrypto:
    def __init__(self, strategy):
        self.strategy = strategy

    def tick(self, handle) -> Dict[str, Any]:
        return {
            "actions": [
                {
                    "type": "trade",
                    "symbol": "BTC",
                    "asset_type": "crypto",
                    "side": "BUY",
                    "qty": 0.1,
                    "order_type": "market",
                    "quote": {"symbol": "USDT", "asset_type": "crypto"},
                }
            ],
            "notes": "crypto test",
            "confidence": 0.9,
        }


class FakeRunnerFuture:
    def __init__(self, strategy):
        self.strategy = strategy

    def tick(self, handle) -> Dict[str, Any]:
        return {
            "actions": [
                {
                    "type": "trade",
                    "symbol": "ES",
                    "asset_type": "future",
                    "expiration": "2025-12-19",
                    "side": "BUY",
                    "qty": 1,
                    "order_type": "market",
                }
            ],
            "notes": "future test",
            "confidence": 0.9,
        }


def test_backtest_agent_executes_trade_on_iteration():
    strat = DummyStrategy(is_backtesting=True)
    agents = AgentManager(strat, runner_factory=lambda s: FakeRunner(s))
    strat._agents = agents  # expose through Strategy-like property

    agents.create(name="test", prompt="do it", cadence="1S", allow_trading=True)

    # Backtest flow: tick via on_iteration and drain on strategy thread
    agents.on_iteration()
    agents.drain_pending()

    assert len(strat.submitted) == 1
    assert strat.submitted[0].asset.symbol == "SPY"


def test_live_agent_registers_cron_and_executes_trade_immediately():
    strat = DummyStrategy(is_backtesting=False)
    agents = AgentManager(strat, runner_factory=lambda s: FakeRunner(s))
    strat._agents = agents

    agents.create(name="live", prompt="go", cadence="5m", allow_trading=True)
    # Drain pending decisions to execute on strategy thread
    agents.drain_pending()

    # Our DummyStrategy.register_cron_callback triggers callback immediately
    # so we expect one submitted order already
    assert len(strat.submitted) == 1
    # Cron should be every 5 minutes
    assert any(cron == "*/5 * * * *" for cron in strat._cron_jobs)


def test_allow_trading_false_does_not_submit_orders():
    strat = DummyStrategy(is_backtesting=True)
    agents = AgentManager(strat, runner_factory=lambda s: FakeRunner(s))
    strat._agents = agents

    agents.create(name="no_trade", prompt="noop", cadence="1S", allow_trading=False)

    agents.on_iteration()
    agents.drain_pending()

    assert len(strat.submitted) == 0


def test_trade_crypto_pair_executes():
    strat = DummyStrategy(is_backtesting=True)
    agents = AgentManager(strat, runner_factory=lambda s: FakeRunnerCrypto(s))
    strat._agents = agents

    agents.create(name="crypto", prompt="go", cadence="1S", allow_trading=True)
    agents.on_iteration()
    agents.drain_pending()

    assert len(strat.submitted) == 1
    assert strat.submitted[0].asset.symbol == "BTC"


def test_trade_future_with_expiration_executes():
    strat = DummyStrategy(is_backtesting=True)
    agents = AgentManager(strat, runner_factory=lambda s: FakeRunnerFuture(s))
    strat._agents = agents

    agents.create(name="future", prompt="go", cadence="1S", allow_trading=True)
    agents.on_iteration()
    agents.drain_pending()

    assert len(strat.submitted) == 1
    assert strat.submitted[0].asset.symbol == "ES"
